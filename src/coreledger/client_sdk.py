"""What a real client integrating with CoreLedger's payment-initiation API
would use to sign its requests and drive the PKCE consent flow. Used by
demo/registration scripts and by the test suite, which plays the role of a
client."""

import base64
import hashlib
import secrets
import time
import uuid

from joserfc import jws
from joserfc.jwk import RSAKey
from joserfc.jws import JWSRegistry
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from coreledger.db.models import Client

# strict_check_header=False: jti/iat are custom protected-header claims, not
# part of the base JWS spec's registered header set. This mirrors how UK
# Open Banking's detached-JWS signing actually carries iat/jti in the header
# rather than in a signed JSON body field.
JWS_REGISTRY = JWSRegistry(algorithms=["RS256"], strict_check_header=False)


def generate_signing_keypair() -> RSAKey:
    return RSAKey.generate_key(2048, private=True)


def sign_request_body(body: bytes, private_key: RSAKey, client_id: str) -> str:
    """Returns a detached-content JWS: 'header..signature'. The caller sends
    this as the X-JWS-Signature header alongside the plain JSON body — the
    body itself stays directly parseable, but its bytes are exactly what the
    signature covers."""
    protected = {
        "alg": "RS256",
        "kid": client_id,
        "jti": str(uuid.uuid4()),
        "iat": int(time.time()),
    }
    full_compact = jws.serialize_compact(protected, body, private_key, registry=JWS_REGISTRY)
    return jws.detach_compact_content(full_compact)


def generate_pkce_pair() -> tuple[str, str]:
    """Returns (code_verifier, code_challenge) for the S256 method — the
    verifier stays with the client and is only revealed at the token
    endpoint; the challenge is what gets sent up front at the authorize step."""
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest()).rstrip(b"=").decode()
    return verifier, challenge


def register_client_jwk(
    session: Session, *, client_id: str, public_jwk: dict, redirect_uris: list[str] | None = None
) -> None:
    """Upserts a client's public JWK and OAuth redirect_uris — shared by the
    one-off registration script and the test suite, which registers its own
    throwaway key."""
    redirect_uris = redirect_uris or []
    stmt = (
        pg_insert(Client)
        .values(client_id=client_id, public_jwk=public_jwk, redirect_uris=redirect_uris)
        .on_conflict_do_update(
            index_elements=["client_id"],
            set_={"public_jwk": public_jwk, "redirect_uris": redirect_uris},
        )
    )
    session.execute(stmt)
    session.commit()
