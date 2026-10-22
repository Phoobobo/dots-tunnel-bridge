"""Cancellation crosses real MCP, SQLite, HTTP client, and connector boundaries."""

import asyncio
import json
import time

import httpx
import pytest
from cryptography.fernet import Fernet

from dots_bridge import AsyncBridgeClient, DotsBridge, Settings, create_app
from dots_bridge.connectors import create_openai_connector
from dots_bridge.mcp import PROTOCOL_VERSION, create_mcp

KEY = "cancel-integration-application-key-" + "a" * 32


class ObservedBridge(DotsBridge):
    def __init__(self, settings):
        super().__init__(settings)
        self.submitted = asyncio.Queue()

    async def submit(self, message):
        result = await super().submit(message)
        await self.submitted.put(result["request_id"])
        return result


@pytest.mark.parametrize("stream", [False, True])
async def test_mcp_supersession_closes_old_openai_waiter_and_completes_successor(tmp_path, stream):
    settings = Settings(
        owner_key="owner-" + "b" * 32,
        api_key=KEY,
        storage_key=Fernet.generate_key().decode(),
        db_path=tmp_path / "bridge.sqlite3",
    )
    bridge = ObservedBridge(settings)
    await bridge.initialize()
    await bridge.store.save_subscription(
        {
            "id": "sub_integration",
            "owner": "owner",
            "channel": "default",
            "destination": {"url": "https://unused.example", "secret": "unused"},
            "expires_at": time.time() + 3600,
            "verified_at": time.time(),
        }
    )
    core = create_app(settings, bridge=bridge, start_worker=False)
    mcp = create_mcp(bridge, None).http_app(
        path="/mcp", json_response=True, stateless_http=True, allowed_hosts=["mcp.test"]
    )
    async with core.router.lifespan_context(core), mcp.router.lifespan_context(mcp):
        async with (
            AsyncBridgeClient(
                "http://127.0.0.1:8765", api_key=KEY, transport=httpx.ASGITransport(app=core)
            ) as client,
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=mcp),
                base_url="http://mcp.test",
                headers={"Accept": "application/json, text/event-stream"},
            ) as rpc,
        ):
            connector = create_openai_connector(api_key=KEY, client=client, reply_timeout=5)
            async with connector.router.lifespan_context(connector):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=connector),
                    base_url="http://connector.test",
                    headers={"Authorization": "Bearer " + KEY},
                ) as http:

                    async def tool(name, **arguments):
                        response = await rpc.post(
                            "/mcp",
                            headers={
                                "MCP-Protocol-Version": PROTOCOL_VERSION,
                                "Mcp-Method": "tools/call",
                                "Mcp-Name": name,
                            },
                            json={
                                "jsonrpc": "2.0",
                                "id": "integration",
                                "method": "tools/call",
                                "params": {
                                    "name": name,
                                    "arguments": arguments,
                                    "_meta": {
                                        "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
                                        "io.modelcontextprotocol/clientCapabilities": {},
                                    },
                                },
                            },
                        )
                        assert response.status_code == 200
                        return response.json()["result"]

                    async def ask(text):
                        return await http.post(
                            "/v1/chat/completions",
                            json={
                                "model": "dots",
                                "messages": [{"role": "user", "content": text}],
                                "stream": stream,
                            },
                        )

                    tasks = []
                    try:
                        async with asyncio.timeout(6):
                            tasks.append(asyncio.create_task(ask("古い調査")))
                            old_id = await bridge.submitted.get()
                            assert not (
                                await tool(
                                    "report_progress",
                                    request_id=old_id,
                                    text="調査中",
                                    idempotency_key="step-1",
                                )
                            ).get("isError")
                            tasks.append(asyncio.create_task(ask("代わりに新しい調査")))
                            new_id = await bridge.submitted.get()
                            cancelled = await tool(
                                "cancel_request", request_id=old_id, superseded_by=new_id
                            )
                            assert not cancelled.get("isError"), cancelled
                            assert cancelled["structuredContent"]["kind"] == "cancel"
                            assert cancelled == await tool(
                                "cancel_request", request_id=old_id, superseded_by=new_id
                            )
                            assert not (
                                await tool(
                                    "reply_to_app", request_id=new_id, text="新しい要求の最終回答"
                                )
                            ).get("isError")
                            old, new = await asyncio.gather(*tasks)
                            assert old.status_code == new.status_code == 200
                            if stream:
                                frames = [
                                    line[6:]
                                    for line in old.text.splitlines()
                                    if line.startswith("data: ")
                                ]
                                payloads = [
                                    json.loads(frame) for frame in frames if frame != "[DONE]"
                                ]
                                notice = next(
                                    payload["dots_bridge"]
                                    for payload in payloads
                                    if payload.get("dots_bridge", {}).get("kind") == "cancel"
                                )
                                assert frames[-1] == "[DONE]"
                                assert "新しい要求の最終回答" in new.text
                            else:
                                notice = old.json()["dots_bridge"]
                                assert old.json()["choices"][0]["message"]["content"] == ""
                                assert (
                                    new.json()["choices"][0]["message"]["content"]
                                    == "新しい要求の最終回答"
                                )
                            assert notice["request_id"] == old_id
                            assert notice["superseded_by"] == new_id
                            state = await client.get_message(old_id)
                            assert state["reply_status"] == "cancelled"
                            assert state["superseded_by"] == new_id
                            late = await tool("reply_to_app", request_id=old_id, text="遅れた回答")
                            assert late.get("isError") is True
                            assert "message_inactive" in late["content"][0]["text"]
                    finally:
                        for task in tasks:
                            task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
