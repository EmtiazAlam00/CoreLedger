"""The PKCE authorization-code flow. There's no real user/login system here
— DEMO_USER_ID stands in for "the resource owner consented"; a real bank
would authenticate an actual customer at the consent step instead."""

import secrets
from datetime import datetime, timedelta

from authlib.oauth2.rfc6749 import AuthorizationServer as _AuthorizationServer
from authlib.oauth2.rfc6749.grants import AuthorizationCodeGrant as _AuthorizationCodeGrant
from authlib.oauth2.rfc7636 import CodeChallenge
from sqlalchemy.orm import Session

from coreledger.db.models import AuthorizationCode, Client, Token

AUTH_CODE_TTL_SECONDS = 600  # RFC 6749's recommended max lifetime
ACCESS_TOKEN_TTL_SECONDS = 3600
DEMO_USER_ID = "demo-user"


class CoreLedgerAuthorizationCodeGrant(_AuthorizationCodeGrant):
    TOKEN_ENDPOINT_AUTH_METHODS = ["none"]  # public client, PKCE-only

    def save_authorization_code(self, code: str, request) -> AuthorizationCode:
        item = AuthorizationCode(
            code=code,
            client_id=request.client.client_id,
            redirect_uri=request.payload.redirect_uri or "",
            scope=request.payload.scope or "",
            code_challenge=request.payload.data.get("code_challenge"),
            code_challenge_method=request.payload.data.get("code_challenge_method"),
            user_id=DEMO_USER_ID,
        )
        self.server.db.add(item)
        self.server.db.commit()
        return item

    def query_authorization_code(self, code: str, client: Client) -> AuthorizationCode | None:
        item = self.server.db.get(AuthorizationCode, code)
        if item is None or item.client_id != client.client_id:
            return None
        if datetime.utcnow() - item.created_at > timedelta(seconds=AUTH_CODE_TTL_SECONDS):
            return None  # expired — treated as if it never existed
        return item

    def delete_authorization_code(self, authorization_code: AuthorizationCode) -> None:
        # A code is single-use: deleting it here (rather than only marking it
        # used) is what makes a second exchange attempt fail with the same
        # "invalid code" error as one that was never valid.
        self.server.db.delete(authorization_code)
        self.server.db.commit()

    def authenticate_user(self, authorization_code: AuthorizationCode) -> str:
        return authorization_code.user_id


class CoreLedgerAuthorizationServer(_AuthorizationServer):
    """Constructed fresh per request (cheap — just registers grants) so its
    self.db is always the current request's session, never shared mutable
    state across concurrent requests."""

    def __init__(self, db: Session):
        super().__init__()
        self.db = db
        self.register_grant(CoreLedgerAuthorizationCodeGrant, [CodeChallenge(required=True)])

    def query_client(self, client_id: str) -> Client | None:
        return self.db.get(Client, client_id)

    def create_oauth2_request(self, request):
        return request  # already a FastAPIOAuth2Request built by the caller

    def create_json_request(self, request):
        return request

    def handle_response(self, status_code: int, payload, headers):
        return status_code, payload, headers

    def send_signal(self, name, *args, **kwargs):
        pass  # no signal system (Flask's integration wires this to blinker) — not needed here

    def generate_token(
        self,
        client,
        grant_type,
        user=None,
        scope=None,
        expires_in=None,
        include_refresh_token=True,
    ) -> dict:
        return {
            "access_token": secrets.token_urlsafe(32),
            "token_type": "Bearer",
            "expires_in": ACCESS_TOKEN_TTL_SECONDS,
            "scope": scope or "",
        }

    def save_token(self, token: dict, request) -> None:
        item = Token(
            access_token=token["access_token"],
            client_id=request.client.client_id,
            scope=token.get("scope", ""),
            user_id=request.user,
            expires_at=datetime.utcnow() + timedelta(seconds=token["expires_in"]),
        )
        self.db.add(item)
        self.db.commit()
