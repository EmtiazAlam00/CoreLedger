from urllib.parse import parse_qs, urlparse

from coreledger.client_sdk import generate_pkce_pair
from tests.conftest import TEST_CLIENT_ID, TEST_REDIRECT_URI


def _authorize_params(challenge: str) -> dict:
    return {
        "response_type": "code",
        "client_id": TEST_CLIENT_ID,
        "redirect_uri": TEST_REDIRECT_URI,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }


def test_full_pkce_flow_issues_working_access_token(client, signing_key):
    """This mirrors what the access_token fixture does — kept as an explicit,
    readable test of the happy path rather than only ever using it indirectly."""
    verifier, challenge = generate_pkce_pair()

    consent = client.get("/oauth/authorize", params=_authorize_params(challenge))
    assert consent.status_code == 200
    assert TEST_CLIENT_ID in consent.text

    approve = client.post(
        "/oauth/authorize", data={**_authorize_params(challenge), "confirm": "true"}, follow_redirects=False
    )
    assert approve.status_code == 302
    code = parse_qs(urlparse(approve.headers["location"]).query)["code"][0]

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
    assert token_resp.status_code == 200
    body = token_resp.json()
    assert body["token_type"] == "Bearer"
    assert body["access_token"]


def test_denied_consent_does_not_issue_code(client, signing_key):
    _, challenge = generate_pkce_pair()

    deny = client.post(
        "/oauth/authorize", data={**_authorize_params(challenge), "confirm": "false"}, follow_redirects=False
    )

    assert deny.status_code == 302
    assert "error=access_denied" in deny.headers["location"]


def test_wrong_code_verifier_is_rejected(client, signing_key):
    """The core PKCE guarantee: possessing the authorization code alone
    isn't enough — you need the verifier that matches the challenge sent at
    the start of the flow, e.g. if the code was intercepted in transit."""
    verifier, challenge = generate_pkce_pair()

    approve = client.post(
        "/oauth/authorize", data={**_authorize_params(challenge), "confirm": "true"}, follow_redirects=False
    )
    code = parse_qs(urlparse(approve.headers["location"]).query)["code"][0]

    wrong_verifier, _ = generate_pkce_pair()
    token_resp = client.post(
        "/oauth/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": TEST_REDIRECT_URI,
            "client_id": TEST_CLIENT_ID,
            "code_verifier": wrong_verifier,
        },
    )

    assert token_resp.status_code == 400
    assert token_resp.json()["error"] == "invalid_grant"


def test_authorization_code_is_single_use(client, signing_key):
    verifier, challenge = generate_pkce_pair()

    approve = client.post(
        "/oauth/authorize", data={**_authorize_params(challenge), "confirm": "true"}, follow_redirects=False
    )
    code = parse_qs(urlparse(approve.headers["location"]).query)["code"][0]

    token_data = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": TEST_REDIRECT_URI,
        "client_id": TEST_CLIENT_ID,
        "code_verifier": verifier,
    }
    first = client.post("/oauth/token", data=token_data)
    second = client.post("/oauth/token", data=token_data)

    assert first.status_code == 200
    assert second.status_code == 400
    assert second.json()["error"] == "invalid_grant"


def test_unknown_client_id_is_rejected(client):
    _, challenge = generate_pkce_pair()
    params = _authorize_params(challenge)
    params["client_id"] = "some-client-that-was-never-registered"

    response = client.get("/oauth/authorize", params=params)

    assert response.status_code == 400
