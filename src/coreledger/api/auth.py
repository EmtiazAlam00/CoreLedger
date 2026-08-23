from datetime import datetime

from fastapi import Depends, Header, HTTPException
from sqlalchemy.orm import Session

from coreledger.api.deps import get_db
from coreledger.db.models import Token


def require_mtls_client(
    x_ssl_client_verify: str | None = Header(default=None, alias="X-SSL-Client-Verify"),
) -> str:
    """Requires the request to have passed nginx's mTLS client-certificate
    check. nginx only proxies to us at all once verification has already
    succeeded — with ssl_verify_client on, an unauthenticated client fails at
    the TLS handshake and never reaches this code. This check exists for the
    path nginx doesn't protect: a request sent directly to :8000, bypassing
    nginx (and therefore mTLS) entirely, arrives with no such header and is
    rejected here instead of silently succeeding.
    """
    if x_ssl_client_verify != "SUCCESS":
        raise HTTPException(status_code=401, detail="mTLS client certificate required")
    return x_ssl_client_verify


def require_access_token(
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> Token:
    """Requires a valid, unexpired Bearer token issued by the PKCE consent
    flow (/oauth/authorize + /oauth/token) — proof the resource owner
    actually consented to this client initiating payments, on top of mTLS
    (who the client is) and JWS (this exact payload, unmodified)."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Bearer access token required")

    access_token = authorization.removeprefix("Bearer ")
    token = db.get(Token, access_token)
    if token is None:
        raise HTTPException(status_code=401, detail="invalid access token")
    if token.expires_at < datetime.utcnow():
        raise HTTPException(status_code=401, detail="access token expired")
    return token
