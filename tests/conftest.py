from urllib.parse import parse_qs, urlparse

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from coreledger.api.main import app
from coreledger.client_sdk import (
    generate_pkce_pair,
    generate_signing_keypair,
    register_client_jwk,
    sign_request_body,
)
from coreledger.db.models import Account
from coreledger.db.session import SessionLocal

TEST_CLIENT_ID = "coreledger-pytest-client"
TEST_REDIRECT_URI = "https://example.test/callback"  # never actually dereferenced by tests
# Deliberately distinct from "coreledger-test-client" (the id tied to the
# real mTLS cert, used by scripts/register_test_client.py for manual/live
# demos) — sharing one client_id meant pytest's session fixture and the
# demo script kept overwriting each other's registered public key in the DB.


@pytest.fixture(autouse=True)
def clean_tables():
    """Truncate ledger tables before every test so tests don't see each
    other's data. Runs against the real Postgres started by docker-compose —
    this project doesn't mock the database. Deliberately leaves clients/
    used_jtis/authorization_codes/tokens alone — those are registration
    state, not per-test ledger data."""
    with SessionLocal() as session:
        session.execute(text("TRUNCATE ledger_entries, transactions, accounts CASCADE"))
        session.commit()
    yield


@pytest.fixture(scope="module")
def client():
    # TestClient must be used as a context manager for FastAPI's lifespan
    # (startup/shutdown) to actually run — that's what creates app.state.producer.
    # base_url is https:// because Authlib's OAuth2Request rejects insecure
    # transport by design (a real check, not something to disable) — in
    # production this traffic is always HTTPS via nginx; TestClient bypasses
    # nginx entirely, so the URL scheme has to say so explicitly instead.
    with TestClient(app, base_url="https://testserver") as c:
        yield c


@pytest.fixture
def two_accounts():
    with SessionLocal() as session:
        checking = Account(name="Checking", currency="USD")
        savings = Account(name="Savings", currency="USD")
        session.add_all([checking, savings])
        session.commit()
        session.refresh(checking)
        session.refresh(savings)
        return checking.id, savings.id


@pytest.fixture(scope="session")
def signing_key():
    """The test suite plays the role of a registered client: generates its
    own throwaway keypair and registers the public half, same as
    scripts/register_test_client.py does for manual/live demos."""
    key = generate_signing_keypair()
    with SessionLocal() as session:
        register_client_jwk(
            session,
            client_id=TEST_CLIENT_ID,
            public_jwk=key.as_dict(private=False),
            redirect_uris=[TEST_REDIRECT_URI],
        )
    return key


@pytest.fixture
def sign(signing_key):
    def _sign(body: bytes) -> str:
        return sign_request_body(body, signing_key, TEST_CLIENT_ID)

    return _sign


@pytest.fixture
def access_token(client, signing_key):
    """Drives the actual PKCE authorize+token flow against the running app
    (not a shortcut) to get a valid Bearer token for tests that need one."""
    verifier, challenge = generate_pkce_pair()
    authorize_params = {
        "response_type": "code",
        "client_id": TEST_CLIENT_ID,
        "redirect_uri": TEST_REDIRECT_URI,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }

    consent = client.post(
        "/oauth/authorize",
        data={**authorize_params, "confirm": "true"},
        follow_redirects=False,
    )
    assert consent.status_code == 302, consent.text
    code = parse_qs(urlparse(consent.headers["location"]).query)["code"][0]

    token_resp = client.post(
        "/oauth/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": TEST_REDIRECT_URI,
            "client_id": TEST_CLIENT_ID,
            "code_verifier": verifier,
        },
    )
    assert token_resp.status_code == 200, token_resp.text
    return token_resp.json()["access_token"]
