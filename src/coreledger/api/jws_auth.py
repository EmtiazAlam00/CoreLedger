import time

from fastapi import Depends, Header, HTTPException, Request
from joserfc import jws
from joserfc.errors import JoseError
from joserfc.jwk import RSAKey
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from coreledger.api.deps import get_db
from coreledger.client_sdk import JWS_REGISTRY
from coreledger.db.models import Client, UsedJti

MAX_CLOCK_SKEW_SECONDS = 300


def _client_id_from_dn(dn: str) -> str:
    # nginx's $ssl_client_s_dn renders our certs (subj "/CN=...", no other
    # fields) as "CN=coreledger-test-client".
    for part in dn.split(","):
        part = part.strip()
        if part.startswith("CN="):
            return part.removeprefix("CN=")
    raise HTTPException(status_code=401, detail=f"no CN found in client DN: {dn}")


async def require_valid_jws(
    request: Request,
    x_ssl_client_dn: str = Header(alias="X-SSL-Client-DN"),
    x_jws_signature: str = Header(alias="X-JWS-Signature"),
    db: Session = Depends(get_db),
) -> None:
    client_id = _client_id_from_dn(x_ssl_client_dn)

    client = db.get(Client, client_id)
    if client is None:
        raise HTTPException(status_code=401, detail=f"unknown client {client_id}")

    body = await request.body()
    public_key = RSAKey.import_key(client.public_jwk)

    try:
        result = jws.deserialize_compact(
            x_jws_signature, public_key, registry=JWS_REGISTRY, payload=body
        )
    except JoseError as e:
        raise HTTPException(status_code=401, detail=f"invalid JWS signature: {e}") from e

    jti = result.protected.get("jti")
    iat = result.protected.get("iat")
    if not jti or iat is None:
        raise HTTPException(status_code=401, detail="JWS header missing jti/iat")

    if abs(time.time() - iat) > MAX_CLOCK_SKEW_SECONDS:
        raise HTTPException(status_code=401, detail="JWS iat outside allowed window")

    # Same ON CONFLICT DO NOTHING dedup pattern as the ledger's idempotency
    # check (step 3) — the replay guard is exactly this insert failing.
    stmt = (
        pg_insert(UsedJti)
        .values(jti=jti)
        .on_conflict_do_nothing(index_elements=["jti"])
        .returning(UsedJti.jti)
    )
    row = db.execute(stmt).first()
    if row is None:
        raise HTTPException(status_code=401, detail="JWS replay detected: jti already used")
    db.commit()
