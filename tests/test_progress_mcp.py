"""Report progress through actual FastMCP wire validation and OAuth scopes."""

from __future__ import annotations

from contextlib import asynccontextmanager

import httpx
import pytest
from fastmcp.server.auth import StaticTokenVerifier

from dots_bridge import BridgeError
from dots_bridge.mcp import PROTOCOL_VERSION, create_mcp


class Backend:
    owner_user_id = "owner"

    def __init__(self):
        self.calls = []
        self.updates = {}

    async def report_progress(self, request_id, text, *, idempotency_key):
        self.calls.append((request_id, text, idempotency_key))
        if request_id == "missing":
            raise BridgeError("message_not_found", "No matching message.", 404)
        if request_id == "completed":
            raise BridgeError("reply_completed", "The final reply is already stored.", 409)
        key = (request_id, idempotency_key)
        if key in self.updates and self.updates[key]["text"] != text:
            raise BridgeError("idempotency_conflict", "Key already used with different text.", 409)
        return self.updates.setdefault(
            key,
            {
                "id": idempotency_key,
                "request_id": request_id,
                "kind": "progress",
                "text": text,
                "channel": "chat",
                "metadata": {},
                "sequence": len(self.updates) + 1,
                "received_at": "2026-10-22T00:00:00Z",
            },
        )


@asynccontextmanager
async def rpc_client(backend, *, auth=None, token=None):
    mcp = create_mcp(backend, auth)
    app = mcp.http_app(
        path="/mcp", json_response=True, stateless_http=True, allowed_hosts=["testserver"]
    )
    headers = {"Accept": "application/json, text/event-stream"}
    if token is not None:
        headers["Authorization"] = "Bearer " + token
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver", headers=headers
        ) as client:

            async def call(method, params=None):
                params = {
                    **(params or {}),
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
                        "io.modelcontextprotocol/clientCapabilities": {},
                    },
                }
                request_headers = {"MCP-Protocol-Version": PROTOCOL_VERSION, "Mcp-Method": method}
                if "name" in params:
                    request_headers["Mcp-Name"] = params["name"]
                return await client.post(
                    "/mcp",
                    headers=request_headers,
                    json={"jsonrpc": "2.0", "id": "progress", "method": method, "params": params},
                )

            yield call


def arguments(**changes):
    return {
        "request_id": "req_1",
        "text": "検索しています。",
        "idempotency_key": "step-1",
        **changes,
    }


async def test_progress_tool_metadata_and_instructions_describe_plain_text_and_final_reply():
    backend = Backend()
    async with rpc_client(backend) as call:
        tools = (await call("tools/list")).json()["result"]["tools"]
        tool = next(tool for tool in tools if tool["name"] == "report_progress")
        assert tool["annotations"]["readOnlyHint"] is False
        assert tool["annotations"]["destructiveHint"] is False
        assert tool["annotations"]["idempotentHint"] is True
        assert tool["securitySchemes"] == tool["_meta"]["securitySchemes"]
        assert tool["securitySchemes"][0]["scopes"] == ["messages:reply"]
        assert set(tool["inputSchema"]["required"]) == {"request_id", "text", "idempotency_key"}
        assert "does not complete" in tool["description"]
        assert "reply_to_app" in tool["description"]
    instructions = create_mcp(backend, None).instructions
    assert "plain text" in instructions
    assert "report_progress" in instructions
    assert "always use reply_to_app" in instructions


async def test_progress_tool_forwards_plain_text_and_stable_key_without_finalizing():
    backend = Backend()
    async with rpc_client(backend) as call:
        payload = {"name": "report_progress", "arguments": arguments()}
        first = (await call("tools/call", payload)).json()["result"]
        repeated = (await call("tools/call", payload)).json()["result"]
        assert first == repeated
        assert first["structuredContent"]["kind"] == "progress"
        assert first["structuredContent"]["text"] == "検索しています。"
        assert backend.calls == [("req_1", "検索しています。", "step-1")] * 2
        conflict = (
            await call(
                "tools/call", {"name": "report_progress", "arguments": arguments(text="別の文章")}
            )
        ).json()["result"]
        assert conflict["isError"] is True
        assert "idempotency_conflict" in conflict["content"][0]["text"]
        assert len(backend.updates) == 1


@pytest.mark.parametrize(
    "request_id,code", [("missing", "message_not_found"), ("completed", "reply_completed")]
)
async def test_progress_backend_errors_are_safe_tool_errors(request_id, code):
    async with rpc_client(Backend()) as call:
        result = (
            await call(
                "tools/call",
                {"name": "report_progress", "arguments": arguments(request_id=request_id)},
            )
        ).json()["result"]
        assert result["isError"] is True
        assert code in result["content"][0]["text"]


@pytest.mark.parametrize(
    "invalid",
    [
        {"request_id": "req_1", "text": "Missing key"},
        arguments(request_id=""),
        arguments(request_id="x" * 201),
        arguments(text=""),
        arguments(text="x" * 64001),
        arguments(idempotency_key=""),
        arguments(idempotency_key="x" * 129),
    ],
)
async def test_progress_tool_rejects_invalid_arguments_before_backend_call(invalid):
    backend = Backend()
    async with rpc_client(backend) as call:
        result = (
            await call("tools/call", {"name": "report_progress", "arguments": invalid})
        ).json()
        assert "error" in result or result.get("result", {}).get("isError") is True
        assert backend.calls == []


async def test_progress_tool_requires_reply_scope_and_stays_hidden_from_read_only_tokens():
    auth = StaticTokenVerifier(
        tokens={
            "read": {"client_id": "test", "scopes": ["events:read"]},
            "reply": {"client_id": "test", "scopes": ["messages:reply"]},
        }
    )
    backend = Backend()
    async with rpc_client(backend, auth=auth, token="read") as call:
        tools = (await call("tools/list")).json()["result"]["tools"]
        assert "report_progress" not in {tool["name"] for tool in tools}
        result = (
            await call("tools/call", {"name": "report_progress", "arguments": arguments()})
        ).json()
        assert "error" in result or result.get("result", {}).get("isError") is True
        assert backend.calls == []
    async with rpc_client(backend, auth=auth, token="reply") as call:
        result = (
            await call("tools/call", {"name": "report_progress", "arguments": arguments()})
        ).json()["result"]
        assert result["structuredContent"]["kind"] == "progress"
        assert backend.calls == [("req_1", "検索しています。", "step-1")]
