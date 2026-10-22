"""Exercise actual HTTP wire responses through FastMCP and the MCP SDK."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
from fastmcp.server.auth import StaticTokenVerifier

from dots_bridge.errors import BridgeError
from dots_bridge.mcp import PROTOCOL_VERSION, create_mcp


class Backend:
    owner_user_id = "alice"

    def __init__(self):
        self.calls = []
        self.pushes = {}

    async def list_events(self):
        return [{"name": "app.message.created", "delivery": ["webhook"]}]

    async def subscribe(self, params, owner):
        self.calls.append(("subscribe", params, owner))
        if params["name"] != "app.message.created":
            raise BridgeError("event_not_found", "Unknown event.")
        return {"id": "sub_test", "refreshBefore": None, "cursor": None, "truncated": False}

    async def unsubscribe(self, params, owner):
        self.calls.append(("unsubscribe", params, owner))
        return {}

    async def get_message(self, request_id):
        return {"request_id": request_id, "text": "Hello", "metadata": {"source": "test"}}

    async def reply_to_app(self, request_id, text):
        self.calls.append(("reply", request_id, text))
        if request_id == "missing":
            raise BridgeError("message_not_found", "Unknown request.", 404)
        return {"request_id": request_id, "text": text}

    async def send_to_app(self, text, *, channel="default", metadata=None, idempotency_key):
        self.calls.append(("send", text, channel, metadata, idempotency_key))
        payload = {"text": text, "channel": channel, "metadata": metadata or {}}
        if idempotency_key in self.pushes:
            if self.pushes[idempotency_key] != payload:
                raise BridgeError(
                    "idempotency_conflict", "This key was used with different content.", 409
                )
        else:
            self.pushes[idempotency_key] = payload
        return {"id": idempotency_key, **payload}


@asynccontextmanager
async def rpc_client(backend: Backend, *, auth=None, token=None):
    mcp = create_mcp(backend, auth)
    app = mcp.http_app(
        path="/mcp", json_response=True, stateless_http=True, allowed_hosts=["testserver"]
    )
    headers = {"Accept": "application/json, text/event-stream"}
    if token is not None:
        headers["Authorization"] = "Bearer " + token
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
            headers=headers,
        ) as client:

            async def call(method: str, params: dict[str, Any] | None = None):
                payload = {
                    **(params or {}),
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
                        "io.modelcontextprotocol/clientCapabilities": {},
                    },
                }
                request_headers = {
                    "MCP-Protocol-Version": PROTOCOL_VERSION,
                    "Mcp-Method": method,
                }
                if "name" in payload:
                    request_headers["Mcp-Name"] = payload["name"]
                return await client.post(
                    "/mcp",
                    headers=request_headers,
                    json={"jsonrpc": "2.0", "id": "test", "method": method, "params": payload},
                )

            yield call


@pytest.mark.asyncio
async def test_modern_discovery_and_tools_survive_sdk_wire_validation():
    async with rpc_client(Backend()) as call:
        response = await call("server/discover")
        assert response.status_code == 200
        discovery = response.json()["result"]
        assert discovery["supportedVersions"] == [PROTOCOL_VERSION]
        assert discovery["capabilities"]["events"] == {}
        assert "tools" in discovery["capabilities"]
        assert discovery["resultType"] == "complete"
        assert discovery["_meta"]["io.modelcontextprotocol/serverInfo"]["name"] == "dots-bridge"

        tools = (await call("tools/list")).json()["result"]["tools"]
        assert {tool["name"] for tool in tools} == {
            "get_current_user",
            "get_message",
            "reply_to_app",
            "report_progress",
            "cancel_request",
            "send_to_app",
        }
        reply = next(tool for tool in tools if tool["name"] == "reply_to_app")
        assert reply["annotations"]["idempotentHint"] is True
        assert reply["_meta"]["securitySchemes"][0]["scopes"] == ["messages:reply"]
        assert reply["securitySchemes"] == reply["_meta"]["securitySchemes"]
        push = next(tool for tool in tools if tool["name"] == "send_to_app")
        assert push["annotations"]["idempotentHint"] is True
        assert push["annotations"]["readOnlyHint"] is False
        assert push["_meta"]["securitySchemes"][0]["scopes"] == ["messages:reply"]
        assert push["securitySchemes"] == push["_meta"]["securitySchemes"]
        assert set(push["inputSchema"]["required"]) == {"text", "idempotency_key"}
        result = (await call("tools/call", {"name": "get_current_user", "arguments": {}})).json()[
            "result"
        ]
        assert result["structuredContent"] == {"user_id": "alice"}


@pytest.mark.asyncio
async def test_events_lifecycle_uses_extension_handlers_and_preserves_ttl_semantics():
    backend = Backend()
    async with rpc_client(backend) as call:
        events = (await call("events/list")).json()["result"]
        assert events["events"][0]["name"] == "app.message.created"
        params = {
            "name": "app.message.created",
            "arguments": {"channel": "chat"},
            "delivery": {
                "mode": "webhook",
                "url": "https://example.com/hook",
                "secret": "whsec_test",
            },
        }
        result = (await call("events/subscribe", params)).json()["result"]
        assert result["id"] == "sub_test"
        assert backend.calls[-1] == ("subscribe", params, "alice")
        await call("events/subscribe", {**params, "ttlMs": None})
        assert "ttlMs" in backend.calls[-1][1]
        assert backend.calls[-1][1]["ttlMs"] is None

        unsubscribe = {**params, "delivery": {"mode": "webhook", "url": "https://example.com/hook"}}
        result = (await call("events/unsubscribe", unsubscribe)).json()["result"]
        assert result["resultType"] == "complete"
        assert backend.calls[-1] == ("unsubscribe", unsubscribe, "alice")

        invalid = (await call("events/subscribe", {**params, "name": "unknown"})).json()["error"]
        assert invalid["code"] == -32602
        assert invalid["data"]["reason"] == "event_not_found"


@pytest.mark.asyncio
async def test_reply_tools_call_backend_and_return_safe_errors():
    backend = Backend()
    async with rpc_client(backend) as call:
        result = (
            await call("tools/call", {"name": "get_message", "arguments": {"request_id": "req_1"}})
        ).json()["result"]
        assert result["structuredContent"]["request_id"] == "req_1"
        result = (
            await call(
                "tools/call",
                {"name": "reply_to_app", "arguments": {"request_id": "req_1", "text": "Done"}},
            )
        ).json()["result"]
        assert result["structuredContent"]["text"] == "Done"
        assert backend.calls[-1] == ("reply", "req_1", "Done")
        result = (
            await call(
                "tools/call",
                {"name": "reply_to_app", "arguments": {"request_id": "missing", "text": "Done"}},
            )
        ).json()["result"]
        assert result["isError"] is True
        assert "message_not_found" in result["content"][0]["text"]


@pytest.mark.asyncio
async def test_push_tool_needs_no_request_or_subscription_and_preserves_metadata():
    backend = Backend()
    async with rpc_client(backend) as call:
        result = (
            await call(
                "tools/call",
                {
                    "name": "send_to_app",
                    "arguments": {"text": "A reminder", "idempotency_key": "reminder_1"},
                },
            )
        ).json()["result"]
        assert result["structuredContent"]["text"] == "A reminder"
        assert backend.calls == [("send", "A reminder", "default", None, "reminder_1")]

        arguments = {
            "text": "Job finished",
            "channel": "jobs.1",
            "metadata": {"job_id": 42, "results": ["ok", None]},
            "idempotency_key": "job_42",
        }
        first = (await call("tools/call", {"name": "send_to_app", "arguments": arguments})).json()[
            "result"
        ]
        repeated = (
            await call("tools/call", {"name": "send_to_app", "arguments": arguments})
        ).json()["result"]
        assert repeated == first
        assert len(backend.pushes) == 2
        assert first["structuredContent"]["metadata"] == arguments["metadata"]
        assert backend.calls[-1] == (
            "send",
            "Job finished",
            "jobs.1",
            arguments["metadata"],
            "job_42",
        )

        conflict = (
            await call(
                "tools/call",
                {"name": "send_to_app", "arguments": {**arguments, "text": "Changed"}},
            )
        ).json()["result"]
        assert conflict["isError"] is True
        assert "idempotency_conflict" in conflict["content"][0]["text"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "arguments",
    [
        {"text": "Hello"},
        {"text": "", "idempotency_key": "key"},
        {"text": "x" * 64001, "idempotency_key": "key"},
        {"text": "Hello", "idempotency_key": ""},
        {"text": "Hello", "idempotency_key": "x" * 129},
        {"text": "Hello", "idempotency_key": "key", "channel": ""},
        {"text": "Hello", "idempotency_key": "key", "channel": "x" * 129},
        {"text": "Hello", "idempotency_key": "key", "channel": "bad/channel"},
        {"text": "Hello", "idempotency_key": "key", "metadata": ["not an object"]},
    ],
)
async def test_push_tool_validates_arguments_before_backend_calls(arguments):
    backend = Backend()
    async with rpc_client(backend) as call:
        result = (await call("tools/call", {"name": "send_to_app", "arguments": arguments})).json()
        assert "error" in result or result.get("result", {}).get("isError") is True
        assert backend.calls == []


@pytest.mark.asyncio
async def test_authenticated_http_enforces_event_and_reply_scopes():
    auth = StaticTokenVerifier(
        tokens={
            "read-token": {"client_id": "test", "scopes": ["events:read"]},
            "reply-token": {"client_id": "test", "scopes": ["messages:reply"]},
        },
    )
    async with rpc_client(Backend(), auth=auth) as call:
        assert (await call("events/list")).status_code == 401
    backend = Backend()
    async with rpc_client(backend, auth=auth, token="read-token") as call:
        assert "events" in (await call("events/list")).json()["result"]
        tools = (await call("tools/list")).json()["result"]["tools"]
        assert not {"reply_to_app", "send_to_app"} & {tool["name"] for tool in tools}
        for name, arguments in (
            ("reply_to_app", {"request_id": "req_1", "text": "Done"}),
            ("send_to_app", {"text": "A reminder", "idempotency_key": "reminder_1"}),
        ):
            result = (await call("tools/call", {"name": name, "arguments": arguments})).json()
            assert "error" in result or result.get("result", {}).get("isError") is True
        assert backend.calls == []
    backend = Backend()
    async with rpc_client(backend, auth=auth, token="reply-token") as call:
        error = (await call("events/list")).json()["error"]
        assert error["data"]["reason"] == "insufficient_scope"
        tools = (await call("tools/list")).json()["result"]["tools"]
        assert {"reply_to_app", "send_to_app"} <= {tool["name"] for tool in tools}
        result = (
            await call(
                "tools/call",
                {
                    "name": "send_to_app",
                    "arguments": {"text": "A reminder", "idempotency_key": "reminder_1"},
                },
            )
        ).json()["result"]
        assert result["structuredContent"]["text"] == "A reminder"
        assert backend.calls == [("send", "A reminder", "default", None, "reminder_1")]
