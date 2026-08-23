from authlib.oauth2.rfc6749 import OAuth2Payload, OAuth2Request


class SimpleOAuth2Payload(OAuth2Payload):
    def __init__(self, data: dict):
        self._data = data

    @property
    def data(self) -> dict:
        return self._data

    @property
    def datalist(self) -> dict:
        return {k: [v] for k, v in self._data.items()}


class FastAPIOAuth2Request(OAuth2Request):
    """Adapts plain dicts into what Authlib's core (framework-agnostic)
    AuthorizationServer expects. Authlib ships Flask/Django adapters but not
    a FastAPI one, so route handlers parse the ASGI request themselves
    (async, since Starlette's form/query parsing is async) and hand this
    class already-materialized dicts — Authlib's core below this point is
    entirely synchronous."""

    def __init__(self, method: str, uri: str, headers: dict, args: dict, form: dict):
        super().__init__(method=method, uri=uri, headers=headers)
        self._args = args
        self._form = form
        self.payload = SimpleOAuth2Payload({**args, **form})

    @property
    def args(self) -> dict:
        return self._args

    @property
    def form(self) -> dict:
        return self._form
