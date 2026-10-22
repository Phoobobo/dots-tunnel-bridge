"""Expose explicit semantic replacement through authenticated MCP tools."""

import pytest
from fastmcp.server.auth import StaticTokenVerifier
from test_mcp import rpc_client

from dots_bridge import BridgeError
from dots_bridge.mcp import create_mcp


class Backend:
    owner_user_id = "owner"

    def __init__(self):
        self.calls = []

    async def cancel_request(self, request_id, *, superseded_by):
        self.calls.append((request_id, superseded_by))
        if request_id == "completed":
            raise BridgeError("reply_completed", "This request already has a final reply.", 409)
        return {
            "sequence": 1,
            "id": "cancel_one",
            "kind": "cancel",
            "request_id": request_id,
            "channel": "chat",
            "text": "",
            "metadata": {},
            "received_at": "2026-10-22T00:00:00Z",
            "superseded_by": superseded_by,
        }


async def test_cancel_tool_discovery_and_instructions_limit_semantic_replacement():
    backend = Backend()
    async with rpc_client(backend) as call:
        tools = (await call("tools/list")).json()["result"]["tools"]
        tool = next(tool for tool in tools if tool["name"] == "cancel_request")
    assert set(tool["inputSchema"]["required"]) == {"request_id", "superseded_by"}
    assert tool["securitySchemes"] == tool["_meta"]["securitySchemes"]
    assert tool["securitySchemes"][0]["scopes"] == ["messages:reply"]
    assert tool["annotations"]["idempotentHint"] is True
    assert tool["annotations"]["readOnlyHint"] is False
    assert "independent requests" in tool["description"]
    assert "reply_to_app" in tool["description"]
    assert "does not forcibly" in tool["description"]
    mcp = create_mcp(backend, None)
    assert mcp.version == "0.6.0"
    assert "Never cancel independent requests" in mcp.instructions
    assert "latest active request" in mcp.instructions
    assert "no action is needed" in mcp.instructions


async def test_cancel_tool_forwards_replacement_and_serializes_safe_errors():
    backend = Backend()
    async with rpc_client(backend) as call:
        result = (
            await call(
                "tools/call",
                {
                    "name": "cancel_request",
                    "arguments": {"request_id": "old", "superseded_by": "new"},
                },
            )
        ).json()["result"]
        assert result["structuredContent"]["kind"] == "cancel"
        assert result["structuredContent"]["superseded_by"] == "new"
        failed = (
            await call(
                "tools/call",
                {
                    "name": "cancel_request",
                    "arguments": {"request_id": "completed", "superseded_by": "new"},
                },
            )
        ).json()["result"]
        assert failed["isError"] is True
        assert "reply_completed" in failed["content"][0]["text"]
    assert backend.calls == [("old", "new"), ("completed", "new")]


@pytest.mark.parametrize(
    "arguments",
    [
        {"request_id": "old"},
        {"request_id": "", "superseded_by": "new"},
        {"request_id": "old", "superseded_by": ""},
        {"request_id": "old", "superseded_by": "x" * 201},
    ],
)
async def test_cancel_tool_rejects_invalid_arguments_before_backend_call(arguments):
    backend = Backend()
    async with rpc_client(backend) as call:
        response = (
            await call("tools/call", {"name": "cancel_request", "arguments": arguments})
        ).json()
    assert "error" in response or response.get("result", {}).get("isError") is True
    assert backend.calls == []


async def test_cancel_tool_requires_reply_scope():
    auth = StaticTokenVerifier(
        tokens={
            "read": {"client_id": "test", "scopes": ["events:read"]},
            "reply": {"client_id": "test", "scopes": ["messages:reply"]},
        }
    )
    backend = Backend()
    async with rpc_client(backend, auth=auth, token="read") as call:
        tools = (await call("tools/list")).json()["result"]["tools"]
        assert "cancel_request" not in {tool["name"] for tool in tools}
        failed = (
            await call(
                "tools/call",
                {
                    "name": "cancel_request",
                    "arguments": {"request_id": "old", "superseded_by": "new"},
                },
            )
        ).json()
        assert "error" in failed or failed.get("result", {}).get("isError") is True
        assert backend.calls == []
    async with rpc_client(backend, auth=auth, token="reply") as call:
        success = (
            await call(
                "tools/call",
                {
                    "name": "cancel_request",
                    "arguments": {"request_id": "old", "superseded_by": "new"},
                },
            )
        ).json()["result"]
        assert success["structuredContent"]["kind"] == "cancel"
