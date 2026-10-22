"""ASGI composition: FastMCP owns MCP/OAuth; Starlette adds the application API."""

import json
import secrets
from contextlib import asynccontextmanager
from urllib.parse import urlsplit

from pydantic import ValidationError
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from .auth import PersonalOAuth
from .bridge import DotsBridge
from .config import Settings
from .errors import BridgeError
from .mcp import create_mcp
from .models import MessageInput, ReceivedAcknowledgement, ReceivedQuery

MAX_REQUEST_BYTES = 300 * 1024


class RequestGuard:
    """Bound all request bodies and reject cross-origin application API requests."""

    def __init__(self, app, *, allowed_origins: set[str]):
        self.app = app
        self.allowed_origins = allowed_origins

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = dict(scope["headers"])
        origin = headers.get(b"origin", b"").decode("latin-1")
        if scope["path"].startswith("/api/") and origin and origin not in self.allowed_origins:
            await JSONResponse({"code": "origin_rejected", "message": "Origin not allowed."}, 403)(
                scope, receive, send
            )
            return
        try:
            declared_length = int(headers.get(b"content-length", b"0"))
        except ValueError:
            declared_length = MAX_REQUEST_BYTES + 1
        if declared_length > MAX_REQUEST_BYTES or declared_length < 0:
            await JSONResponse(
                {"code": "body_too_large", "message": "Request body is too large."}, 413
            )(scope, receive, send)
            return
        # Read before routing so chunked requests cannot bypass the body cap or
        # make a protocol handler partially mutate state before an overflow.
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            body.extend(message.get("body", b""))
            if len(body) > MAX_REQUEST_BYTES:
                await JSONResponse(
                    {"code": "body_too_large", "message": "Request body is too large."}, 413
                )(scope, receive, send)
                return
            if not message.get("more_body", False):
                break
        consumed = False

        async def replay():
            nonlocal consumed
            if not consumed:
                consumed = True
                return {"type": "http.request", "body": bytes(body), "more_body": False}
            return await receive()

        async def secured_send(message):
            if message["type"] == "http.response.start":
                message["headers"] = [
                    *message.get("headers", []),
                    (b"cache-control", b"no-store"),
                    (b"x-content-type-options", b"nosniff"),
                ]
            await send(message)

        await self.app(scope, replay, secured_send)


def create_app(
    settings: Settings, *, bridge: DotsBridge | None = None, start_worker: bool = True
) -> Starlette:
    """Create a standalone ASGI server, suitable for uvicorn or in-process use.

    The lifespan must run to start durable delivery. Run one uvicorn worker;
    SQLite leases recover an interrupted delivery after at most 30 seconds.
    """
    bridge = bridge or DotsBridge(settings)
    auth = PersonalOAuth(
        db_path=settings.db_path,
        public_base_url=settings.public_base_url,
        owner_key=settings.owner_key,
        redirect_uri=settings.redirect_uri,
        client_id=settings.client_id,
        owner_user_id=settings.owner_user_id,
    )
    auth.on_revoke = bridge.revoke_subscriptions
    mcp = create_mcp(bridge, auth)
    public = urlsplit(settings.public_base_url)
    local_origins = {
        f"http://127.0.0.1:{settings.port}",
        f"http://localhost:{settings.port}",
        f"http://[::1]:{settings.port}",
    }
    allowed_hosts = list({public.hostname, "127.0.0.1", "localhost", "::1"})
    mcp_app = mcp.http_app(
        path="/mcp",
        json_response=True,
        stateless_http=True,
        allowed_hosts=allowed_hosts,
        allowed_origins=[settings.public_base_url, *local_origins],
    )

    def authorize(request: Request):
        scheme, _, token = request.headers.get("authorization", "").partition(" ")
        if scheme.lower() != "bearer" or not secrets.compare_digest(
            token.encode(), settings.api_key.encode()
        ):
            raise BridgeError(
                "unauthorized", "Invalid or missing application API credentials.", 401
            )

    async def health(request):
        return JSONResponse({"status": "ok"})

    async def subscriptions(request: Request):
        authorize(request)
        return JSONResponse({"subscriptions": await bridge.list_subscriptions()})

    async def submit(request: Request):
        authorize(request)
        payload = await parse_json(request)
        try:
            message = MessageInput.model_validate(payload)
        except ValidationError:
            raise BridgeError(
                "invalid_message", "Invalid message fields; check text, channel and metadata.", 422
            ) from None
        return JSONResponse(await bridge.submit(message), status_code=202)

    async def parse_json(request: Request):
        if request.headers.get("content-type", "").split(";")[0].strip() != "application/json":
            raise BridgeError("invalid_content_type", "Expected application/json.", 415)
        try:
            return json.loads(await request.body(), parse_constant=lambda value: _invalid_json())
        except (ValueError, UnicodeError, RecursionError):
            raise BridgeError("invalid_json", "Expected a valid JSON request body.", 400) from None

    async def get_message(request: Request):
        authorize(request)
        return JSONResponse(await bridge.get_message(request.path_params["request_id"]))

    def query_parameters(request, allowed):
        query = dict(request.query_params)
        if set(query) - allowed or len(query) != len(request.query_params.multi_items()):
            raise BridgeError(
                "invalid_received_query", "Invalid or repeated query parameters.", 422
            )
        return query

    async def response_messages(request: Request):
        authorize(request)
        query = query_parameters(request, {"after", "limit"})
        try:
            validated = ReceivedQuery(
                after=int(query.get("after", 0)), limit=int(query.get("limit", 100))
            )
        except (ValueError, ValidationError):
            raise BridgeError("invalid_received_query", "Invalid after or limit.", 422) from None
        return JSONResponse(
            {
                "messages": await bridge.list_response_messages(
                    request.path_params["request_id"], after=validated.after, limit=validated.limit
                )
            }
        )

    async def received(request: Request):
        authorize(request)
        query = query_parameters(request, {"after", "limit", "channel"})
        try:
            after = int(query.get("after", 0))
            limit = int(query.get("limit", 100))
        except ValueError:
            raise BridgeError(
                "invalid_received_query", "after and limit must be integers.", 422
            ) from None
        return JSONResponse(
            {
                "messages": await bridge.list_received(
                    after=after,
                    limit=limit,
                    channel=query.get("channel"),
                )
            }
        )

    async def consumer_cursor(request: Request):
        authorize(request)
        query = query_parameters(request, {"channel"})
        cursor = await bridge.get_consumer_cursor(
            request.path_params["consumer_id"],
            channel=query.get("channel"),
        )
        return JSONResponse({"cursor": cursor})

    async def acknowledge(request: Request):
        authorize(request)
        payload = await parse_json(request)
        try:
            acknowledgement = ReceivedAcknowledgement.model_validate(payload)
        except ValidationError:
            raise BridgeError(
                "invalid_received_cursor", "Invalid sequence or channel.", 422
            ) from None
        cursor = await bridge.acknowledge_received(
            request.path_params["consumer_id"],
            **acknowledgement.model_dump(),
        )
        return JSONResponse({"cursor": cursor})

    async def delete_subscription(request: Request):
        authorize(request)
        await bridge.initialize()
        await bridge.store.delete_subscription(
            request.path_params["subscription_id"], bridge.owner_user_id
        )
        return JSONResponse({})

    async def bridge_error(request, exc: BridgeError):
        headers = {"WWW-Authenticate": "Bearer"} if exc.status_code == 401 else None
        return JSONResponse(
            {"code": exc.code, "message": exc.message}, exc.status_code, headers=headers
        )

    @asynccontextmanager
    async def lifespan(app):
        await bridge.initialize()
        async with mcp_app.router.lifespan_context(mcp_app):
            try:
                if start_worker:
                    await bridge.start()
                yield
            finally:
                await bridge.close()

    app = Starlette(
        routes=[
            Route("/health", health),
            Route("/api/subscriptions", subscriptions),
            Route("/api/subscriptions/{subscription_id}", delete_subscription, methods=["DELETE"]),
            Route("/api/messages", submit, methods=["POST"]),
            Route("/api/messages/{request_id}", get_message),
            Route("/api/messages/{request_id}/responses", response_messages),
            Route("/api/received", received),
            Route("/api/consumers/{consumer_id}", consumer_cursor),
            Route("/api/consumers/{consumer_id}/ack", acknowledge, methods=["POST"]),
            Mount("/", app=mcp_app),
        ],
        lifespan=lifespan,
        exception_handlers={BridgeError: bridge_error},
        middleware=[
            Middleware(TrustedHostMiddleware, allowed_hosts=allowed_hosts),
            Middleware(RequestGuard, allowed_origins={settings.public_base_url, *local_origins}),
        ],
    )
    app.state.bridge = bridge
    app.state.auth = auth
    app.state.mcp = mcp
    return app


def _invalid_json():
    raise ValueError("Non-finite JSON number")
