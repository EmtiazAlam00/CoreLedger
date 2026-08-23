from authlib.oauth2 import OAuth2Error
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from sqlalchemy.orm import Session

from coreledger.api.deps import get_db
from coreledger.oauth.requests import FastAPIOAuth2Request
from coreledger.oauth.server import DEMO_USER_ID, CoreLedgerAuthorizationServer

router = APIRouter(prefix="/oauth", tags=["oauth"])


def _response(status_code: int, payload, headers) -> Response:
    headers_dict = dict(headers)
    if isinstance(payload, dict):
        return JSONResponse(content=payload, status_code=status_code, headers=headers_dict)
    return Response(content=payload or "", status_code=status_code, headers=headers_dict)


def _error_page(error: OAuth2Error) -> HTMLResponse:
    return HTMLResponse(
        f"<h1>Authorization error</h1><p>{error.error}: {error.description}</p>",
        status_code=error.status_code,
    )


@router.get("/authorize")
async def authorize_prompt(request: Request, db: Session = Depends(get_db)) -> Response:
    """Shows the consent screen. A real bank would authenticate the user
    here first — this demo skips login and consents as a fixed demo user,
    but still requires an explicit human click through a real form post,
    not a silent auto-approval."""
    params = dict(request.query_params)
    oauth_req = FastAPIOAuth2Request(
        method="GET", uri=str(request.url), headers=dict(request.headers), args=params, form={}
    )

    server = CoreLedgerAuthorizationServer(db)
    try:
        grant = server.get_consent_grant(request=oauth_req, end_user=None)
    except OAuth2Error as error:
        return _error_page(error)

    client = grant.request.client
    scope = grant.request.payload.scope or "(no scope requested)"
    hidden_fields = "".join(f'<input type="hidden" name="{k}" value="{v}">' for k, v in params.items())

    return HTMLResponse(f"""
    <h1>CoreLedger authorization request</h1>
    <p><strong>{client.client_id}</strong> is requesting access to initiate payments.</p>
    <p>Scope: {scope}</p>
    <form method="post" action="/oauth/authorize">
        {hidden_fields}
        <button type="submit" name="confirm" value="true">Approve</button>
        <button type="submit" name="confirm" value="false">Deny</button>
    </form>
    """)


@router.post("/authorize")
async def authorize_decision(request: Request, db: Session = Depends(get_db)) -> Response:
    form = dict((await request.form()))
    confirm = form.pop("confirm", "false") == "true"

    oauth_req = FastAPIOAuth2Request(
        method="POST", uri=str(request.url), headers=dict(request.headers), args={}, form=form
    )

    server = CoreLedgerAuthorizationServer(db)
    grant_user = DEMO_USER_ID if confirm else None
    status_code, payload, headers = server.create_authorization_response(
        oauth_req, grant_user=grant_user
    )
    return _response(status_code, payload, headers)


@router.post("/token")
async def token(request: Request, db: Session = Depends(get_db)) -> Response:
    form = dict(await request.form())
    oauth_req = FastAPIOAuth2Request(
        method="POST", uri=str(request.url), headers=dict(request.headers), args={}, form=form
    )

    server = CoreLedgerAuthorizationServer(db)
    status_code, payload, headers = server.create_token_response(oauth_req)
    return _response(status_code, payload, headers)
