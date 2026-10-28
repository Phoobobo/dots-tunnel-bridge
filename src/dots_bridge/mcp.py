"""FastMCP tools and the small adapter for ChatGPT's draft Events methods.

FastMCP and the MCP SDK own JSON-RPC, HTTP, protocol negotiation, validation,
and authentication. The draft webhook Events API is not part of the SDK yet.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated, Any, Literal, Protocol

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.server.auth import AuthProvider, require_scopes
from fastmcp.server.dependencies import get_access_token
from fastmcp.server.extensions import MethodBinding, ServerExtension
from mcp.server.context import CallNext, HandlerResult, ServerRequestContext
from mcp.shared.exceptions import MCPError
from mcp_types import INVALID_PARAMS, PaginatedRequestParams, RequestParams
from pydantic import BaseModel, ConfigDict, Field

from .errors import BridgeError

PROTOCOL_VERSION = "2026-07-28"


class BridgeBackend(Protocol):
    """The bridge operations exposed to an authenticated dot."""

    owner_user_id: str

    async def list_events(self) -> list[dict[str, Any]]: ...

    async def subscribe(self, params: dict[str, Any], owner: str) -> dict[str, Any]: ...

    async def unsubscribe(self, params: dict[str, Any], owner: str) -> dict[str, Any]: ...

    async def reply_to_app(self, request_id: str, text: str) -> dict[str, Any]: ...

    async def report_progress(
        self, request_id: str, text: str, *, idempotency_key: str
    ) -> dict[str, Any]: ...

    async def cancel_request(self, request_id: str, *, superseded_by: str) -> dict[str, Any]: ...

    async def send_to_app(
        self,
        text: str,
        *,
        channel: str = "default",
        metadata: dict[str, Any] | None = None,
        idempotency_key: str,
    ) -> dict[str, Any]: ...

    async def get_message(self, request_id: str) -> dict[str, Any]: ...


class _WebhookDestination(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["webhook"]
    url: str


class _WebhookSubscription(_WebhookDestination):
    secret: str


class _SubscribeParams(RequestParams):
    name: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    delivery: _WebhookSubscription
    cursor: str | None = None
    ttl_ms: Annotated[int, Field(gt=0, strict=True)] | None = Field(default=None, alias="ttlMs")
    max_age_ms: Annotated[int, Field(ge=0, strict=True)] | None = Field(
        default=None, alias="maxAgeMs"
    )


class _UnsubscribeParams(RequestParams):
    name: str
    arguments: dict[str, Any] = Field(default_factory=dict)
    delivery: _WebhookDestination


class _EventsExtension(ServerExtension):
    identifier = "io.dotsbridge/webhook-events"

    def __init__(self, bridge: BridgeBackend, *, authenticated: bool) -> None:
        self._bridge = bridge
        self._authenticated = authenticated

    def methods(self) -> Sequence[MethodBinding]:
        versions = frozenset({PROTOCOL_VERSION})
        return (
            MethodBinding("events/list", PaginatedRequestParams, self._list, versions),
            MethodBinding("events/subscribe", _SubscribeParams, self._subscribe, versions),
            MethodBinding("events/unsubscribe", _UnsubscribeParams, self._unsubscribe, versions),
        )

    def _check_access(self) -> None:
        if not self._authenticated:
            return
        token = get_access_token()
        if token is None or "events:read" not in token.scopes:
            raise MCPError(
                code=-32003,
                message="The events:read OAuth scope is required.",
                data={"reason": "insufficient_scope"},
            )

    async def _list(
        self, ctx: ServerRequestContext, params: PaginatedRequestParams
    ) -> dict[str, Any]:
        self._check_access()
        if params.cursor is not None:
            raise MCPError(INVALID_PARAMS, "This event catalog has no further pages.")
        return {"events": await self._bridge.list_events()}

    async def _subscribe(
        self, ctx: ServerRequestContext, params: _SubscribeParams
    ) -> dict[str, Any]:
        self._check_access()
        # Preserve omitted ttlMs separately from an explicit null; both have
        # distinct meanings in the Events draft.
        raw = params.model_dump(by_alias=True, mode="json", exclude_unset=True)
        raw.pop("_meta", None)
        raw.setdefault("arguments", {})
        try:
            return await self._bridge.subscribe(raw, owner=self._bridge.owner_user_id)
        except BridgeError as exc:
            raise MCPError(exc.rpc_code, exc.message, data={"reason": exc.code}) from None

    async def _unsubscribe(
        self, ctx: ServerRequestContext, params: _UnsubscribeParams
    ) -> dict[str, Any]:
        self._check_access()
        raw = params.model_dump(by_alias=True, mode="json", exclude_unset=True)
        raw.pop("_meta", None)
        raw.setdefault("arguments", {})
        try:
            return await self._bridge.unsubscribe(raw, owner=self._bridge.owner_user_id)
        except BridgeError as exc:
            raise MCPError(exc.rpc_code, exc.message, data={"reason": exc.code}) from None


async def _advertise_events(ctx: ServerRequestContext, call_next: CallNext) -> HandlerResult:
    """Preserve draft Events and OpenAI tool metadata after SDK serialization.

    MCP SDK 2.x drops unknown ServerCapabilities keys, including ``events``
    (upstream issue #3640). This SDK middleware seam is the only private
    FastMCP access used below; tools and HTTP remain entirely library-owned.
    Tool securitySchemes is also absent from the SDK model; publish the OpenAI
    top-level field alongside its back-compat _meta mirror. Remove these shims
    when the SDK preserves both fields itself.
    """
    result = await call_next(ctx)
    if ctx.method in {"server/discover", "tools/list"}:
        if isinstance(result, BaseModel):
            result = result.model_dump(by_alias=True, mode="json", exclude_none=True)
        if isinstance(result, dict):
            if ctx.method == "server/discover":
                result = {
                    **result,
                    "capabilities": {**result.get("capabilities", {}), "events": {}},
                }
            else:
                tools = []
                for tool in result.get("tools", []):
                    schemes = tool.get("_meta", {}).get("securitySchemes")
                    tools.append({**tool, "securitySchemes": schemes} if schemes else tool)
                result = {**result, "tools": tools}
    return result


def _security_meta(scope: str) -> dict[str, Any]:
    return {"securitySchemes": [{"type": "oauth2", "scopes": [scope]}]}


def create_mcp(
    bridge: BridgeBackend, auth: AuthProvider | None, *, no_auth: bool = False
) -> FastMCP:
    """Build the MCP endpoint used by dots, with draft webhook Events support."""
    mcp = FastMCP(
        "dots-bridge",
        version="0.6.0",
        auth=auth,
        instructions=(
            "This bridge connects its owner's application to this dot. Subscribe to "
            "app.message.created only when the owner asks. Each event contains a "
            "request_id and application text. When the owner has requested replies "
            "through this bridge, call reply_to_app with that request_id and your "
            "final response. get_message retrieves the original request. Only one "
            "final reply is stored per request; retry the same reply if needed. "
            "You may call report_progress with the request_id and plain text to "
            "send intermediate updates. Each new update needs its own unique "
            "idempotency_key; reuse that key only when retrying the same update. "
            "Progress does not complete the request: always use reply_to_app for "
            "the final response. "
            "Only when a later request clearly replaces an unfinished earlier request, "
            "call cancel_request with the obsolete request_id and the newer request's ID "
            "as superseded_by. Never cancel independent requests. Normal completion, "
            "including a conclusion that no action is needed, still requires reply_to_app. "
            "Always send a final reply for the latest active request. Cancellation closes "
            "the bridge request; it does not forcibly interrupt this dot's execution. "
            "When the owner authorizes proactive messages, call send_to_app to "
            "send application messages without a prior request or subscription. "
            "Use reply_to_app for responses to application requests so they remain "
            "correlated. Give each new proactive message a unique idempotency_key "
            "and reuse that key when retrying the same message. "
            "Continue to apply the owner's normal approval requirements."
        ),
    )
    read_auth = require_scopes("events:read") if auth is not None else None
    reply_auth = require_scopes("messages:reply") if auth is not None else None

    def _meta(scope: str) -> dict[str, Any]:
        # Local patch: advertise noauth when OAuth is disabled.
        if no_auth:
            return {"securitySchemes": [{"type": "noauth"}]}
        return _security_meta(scope)

    @mcp.tool(
        annotations={"readOnlyHint": True, "openWorldHint": False},
        auth=read_auth,
        meta=_meta("events:read"),
    )
    async def get_current_user() -> dict[str, str]:
        """Return the connected application owner's ID."""
        return {"user_id": bridge.owner_user_id}

    @mcp.tool(
        annotations={"readOnlyHint": True, "openWorldHint": False},
        auth=read_auth,
        meta=_meta("events:read"),
    )
    async def get_message(
        request_id: Annotated[str, Field(min_length=1, max_length=200)],
    ) -> dict[str, Any]:
        """Read a bridge request by request_id, including its text and metadata."""
        try:
            return await bridge.get_message(request_id)
        except BridgeError as exc:
            raise ToolError(f"{exc.code}: {exc.message}") from None

    @mcp.tool(
        annotations={
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
        auth=reply_auth,
        meta=_meta("messages:reply"),
    )
    async def reply_to_app(
        request_id: Annotated[str, Field(min_length=1, max_length=200)],
        text: Annotated[str, Field(min_length=1, max_length=64000)],
    ) -> dict[str, Any]:
        """Send one final text reply to the application request identified by request_id.

        Repeating the same request_id and text succeeds without duplicating the
        reply. A different second final reply for the same request is rejected.
        """
        try:
            return await bridge.reply_to_app(request_id, text)
        except BridgeError as exc:
            raise ToolError(f"{exc.code}: {exc.message}") from None

    @mcp.tool(
        annotations={
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
        auth=reply_auth,
        meta=_meta("messages:reply"),
    )
    async def report_progress(
        request_id: Annotated[str, Field(min_length=1, max_length=200)],
        text: Annotated[str, Field(min_length=1, max_length=64000)],
        idempotency_key: Annotated[
            str,
            Field(
                min_length=1,
                max_length=128,
                description=(
                    "A unique key for this progress update. Reuse it on retries of the same "
                    "update and choose a different key for each new update."
                ),
            ),
        ],
    ) -> dict[str, Any]:
        """Send a plain-text intermediate update for an application request.

        This does not complete the request or replace its final reply. Call
        reply_to_app with the same request_id when the final response is ready.
        Retrying the same request_id, idempotency_key and text does not duplicate
        the update; reusing the key with different text is rejected.
        """
        try:
            return await bridge.report_progress(request_id, text, idempotency_key=idempotency_key)
        except BridgeError as exc:
            raise ToolError(f"{exc.code}: {exc.message}") from None

    @mcp.tool(
        annotations={
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
        auth=reply_auth,
        meta=_meta("messages:reply"),
    )
    async def cancel_request(
        request_id: Annotated[str, Field(min_length=1, max_length=200)],
        superseded_by: Annotated[str, Field(min_length=1, max_length=200)],
    ) -> dict[str, Any]:
        """Cancel an obsolete unfinished request clearly replaced by a newer request.

        superseded_by must name a real, newer request for the same channel and
        subscription. Do not cancel independent requests. For normal completion
        or a conclusion that nothing needs doing, use reply_to_app instead.
        Always provide a final reply for the latest active request. Repeating the
        same request_id and superseded_by is idempotent; another replacement is
        rejected. This marks the bridge request as cancelled and does not forcibly
        interrupt dots execution.
        """
        try:
            return await bridge.cancel_request(request_id, superseded_by=superseded_by)
        except BridgeError as exc:
            raise ToolError(f"{exc.code}: {exc.message}") from None

    @mcp.tool(
        annotations={
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
        auth=reply_auth,
        meta=_meta("messages:reply"),
    )
    async def send_to_app(
        text: Annotated[str, Field(min_length=1, max_length=64000)],
        idempotency_key: Annotated[
            str,
            Field(
                min_length=1,
                max_length=128,
                description=(
                    "A stable unique key for this logical message. Reuse it when retrying "
                    "the same message and choose a different key for every new message."
                ),
            ),
        ],
        channel: Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[\w.-]+$")] = (
            "default"
        ),
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Send an owner-authorized proactive message to the application.

        No prior application request or Events subscription is needed. Use
        reply_to_app instead for a response to an existing request. Retrying
        the same idempotency_key and message succeeds without duplicating it;
        reusing the key with different content is rejected.
        """
        try:
            return await bridge.send_to_app(
                text,
                channel=channel,
                metadata=metadata,
                idempotency_key=idempotency_key,
            )
        except BridgeError as exc:
            raise ToolError(f"{exc.code}: {exc.message}") from None

    mcp.add_extension(_EventsExtension(bridge, authenticated=auth is not None))
    # FastMCP's public extension API registers the three draft methods. Its SDK
    # server middleware must add the top-level capability after wire validation.
    mcp._mcp_server.middleware.insert(0, _advertise_events)
    return mcp
