"""End-to-end HTTP/OAuth/MCP coverage without a live dots account."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
from contextlib import asynccontextmanager
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from cryptography.fernet import Fernet

from dots_bridge import DotsBridge, Settings, create_app
from dots_bridge.app import MAX_REQUEST_BYTES
from dots_bridge.mcp import PROTOCOL_VERSION

BASE = "https://bridge.example"
CALLBACK = "https://chatgpt.example/callback"
OWNER_KEY = "test-owner-key-" + "a" * 40
API_KEY = "test-application-key-" + "b" * 40
VERIFIER = "test-verifier-" + "c" * 48
CHALLENGE = (
    base64.urlsafe_b64encode(hashlib.sha256(VERIFIER.encode()).digest()).decode().rstrip("=")
)
WEBHOOK_KEY = b"test-webhook-key-for-integration!"
WEBHOOK_SECRET = "whsec_" + base64.b64encode(WEBHOOK_KEY).decode()


class Webhook:
    """A public receiver that verifies the exact delivered Standard Webhooks bytes."""

    def __init__(self):
        self.verifications = []
        self.events = []
        self.received = asyncio.Event()

    async def __call__(self, url, body, headers):
        assert url == "https://hooks.example/ingress"
        message = (
            headers["webhook-id"].encode()
            + b"."
            + headers["webhook-timestamp"].encode()
            + b"."
            + body
        )
        expected = "v1," + base64.b64encode(hmac.digest(WEBHOOK_KEY, message, "sha256")).decode()
        assert hmac.compare_digest(headers["webhook-signature"], expected)
        payload = json.loads(body)
        if payload.get("type") == "verification":
            self.verifications.append(payload)
            return httpx.Response(200, json={"challenge": payload["challenge"]})
        self.events.append((payload, headers))
        self.received.set()
        return httpx.Response(202)


@asynccontextmanager
async def running_app(tmp_path, *, start_worker=False):
    settings = Settings(
        public_base_url=BASE,
        redirect_uri=CALLBACK,
        owner_key=OWNER_KEY,
        api_key=API_KEY,
        storage_key=Fernet.generate_key().decode(),
        db_path=tmp_path / "data" / "bridge.db",
        worker_interval_seconds=0.01,
    )
    webhook = Webhook()
    bridge = DotsBridge(settings, webhook_post=webhook)
    app = create_app(settings, bridge=bridge, start_worker=start_worker)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=BASE,
            follow_redirects=False,
        ) as client:
            yield client, app, bridge, webhook


async def login(client, *, scope="events:read messages:reply"):
    response = await client.get(
        "/authorize",
        params={
            "client_id": "dots-bridge",
            "redirect_uri": CALLBACK,
            "response_type": "code",
            "resource": BASE + "/mcp",
            "scope": scope,
            "code_challenge": CHALLENGE,
            "code_challenge_method": "S256",
            "state": "test-state",
        },
    )
    assert response.status_code == 302, response.text
    location = response.headers["location"]
    request_id = parse_qs(urlsplit(location).query)["request_id"][0]
    assert (await client.get(location)).status_code == 200
    response = await client.post(
        "/consent",
        data={"request_id": request_id, "owner_key": OWNER_KEY},
        headers={"Origin": BASE},
    )
    assert response.status_code == 302, response.text
    params = parse_qs(urlsplit(response.headers["location"]).query)
    assert params["state"] == ["test-state"]
    response = await client.post(
        "/token",
        data={
            "client_id": "dots-bridge",
            "grant_type": "authorization_code",
            "code": params["code"][0],
            "code_verifier": VERIFIER,
            "redirect_uri": CALLBACK,
            "resource": BASE + "/mcp",
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def bearer(token):
    return {"Authorization": "Bearer " + token}


async def rpc(client, token, method, params=None):
    params = {
        **(params or {}),
        "_meta": {
            "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
            "io.modelcontextprotocol/clientCapabilities": {},
        },
    }
    headers = {
        **bearer(token),
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": PROTOCOL_VERSION,
        "Mcp-Method": method,
    }
    if "name" in params:
        headers["Mcp-Name"] = params["name"]
    return await client.post(
        "/mcp",
        headers=headers,
        json={"jsonrpc": "2.0", "id": "integration", "method": method, "params": params},
    )


async def subscribe(client, token):
    response = await rpc(
        client,
        token,
        "events/subscribe",
        {
            "name": "app.message.created",
            "arguments": {"channel": "chat"},
            "delivery": {
                "mode": "webhook",
                "url": "https://hooks.example/ingress",
                "secret": WEBHOOK_SECRET,
            },
        },
    )
    assert response.status_code == 200, response.text
    assert "error" not in response.json(), response.text
    return response.json()["result"]["id"]


@pytest.mark.asyncio
async def test_oauth_events_delivery_and_mcp_reply_through_real_http_app(tmp_path):
    async with running_app(tmp_path) as (client, app, bridge, webhook):
        tokens = await login(client)
        token = tokens["access_token"]
        discovery = await rpc(client, token, "server/discover")
        assert discovery.json()["result"]["capabilities"]["events"] == {}
        subscription_id = await subscribe(client, token)
        assert len(webhook.verifications) == 1
        response = await client.post(
            "/api/messages",
            headers=bearer(API_KEY),
            json={
                "text": "日本語のメッセージ",
                "channel": "chat",
                "metadata": {"source": "integration"},
                "idempotency_key": "one-request",
            },
        )
        assert response.status_code == 202, response.text
        message = response.json()
        assert message["subscription_id"] == subscription_id
        assert message["delivery_status"] == "queued"
        assert await bridge.process_next() is True
        assert await bridge.process_next() is False
        event, headers = webhook.events[0]
        assert event["eventId"] == message["event_id"] == headers["webhook-id"]
        assert event["data"] == {
            "request_id": message["request_id"],
            "text": "日本語のメッセージ",
            "channel": "chat",
            "metadata": {"source": "integration"},
        }
        response = await rpc(
            client,
            token,
            "tools/call",
            {
                "name": "reply_to_app",
                "arguments": {
                    "request_id": message["request_id"],
                    "text": "dotsからの応答です。",
                },
            },
        )
        assert response.status_code == 200, response.text
        result = response.json()["result"]
        assert result.get("isError") is not True, response.text
        assert result["structuredContent"]["reply"] == "dotsからの応答です。"
        response = await client.get(
            "/api/messages/" + message["request_id"], headers=bearer(API_KEY)
        )
        assert response.status_code == 200
        assert response.json()["reply"] == "dotsからの応答です。"
        assert response.json()["reply_status"] == "replied"
        assert response.json()["delivery_status"] == "accepted"
        assert response.json()["attempts"] == 1
        assert bridge._worker is None
        assert app.state.auth is not None


@pytest.mark.asyncio
async def test_application_owner_and_oauth_credentials_have_separate_roles(tmp_path):
    async with running_app(tmp_path) as (client, app, bridge, webhook):
        token = (await login(client))["access_token"]
        for wrong in (OWNER_KEY, token, "invalid"):
            response = await client.get("/api/subscriptions", headers=bearer(wrong))
            assert response.status_code == 401
            assert response.headers["www-authenticate"] == "Bearer"
        assert (await client.get("/api/subscriptions", headers=bearer(API_KEY))).status_code == 200
        for wrong in (OWNER_KEY, API_KEY, "invalid"):
            assert (await rpc(client, wrong, "events/list")).status_code == 401
        assert (await rpc(client, token, "events/list")).status_code == 200
        assert app.state.auth.check_owner_key(OWNER_KEY)
        assert not app.state.auth.check_owner_key(API_KEY)
        assert not app.state.auth.check_owner_key(token)
        assert not webhook.events


@pytest.mark.asyncio
async def test_application_guards_origin_host_body_and_finite_json(tmp_path):
    async with running_app(tmp_path) as (client, app, bridge, webhook):
        headers = bearer(API_KEY)
        response = await client.get("/api/subscriptions", headers={**headers, "Origin": "null"})
        assert response.status_code == 403 and response.json()["code"] == "origin_rejected"
        response = await client.get(
            "/api/subscriptions",
            headers={**headers, "Origin": "https://evil.example"},
        )
        assert response.status_code == 403
        assert (
            await client.get("/api/subscriptions", headers={**headers, "Origin": BASE})
        ).status_code == 200
        assert (await client.get("/health", headers={"Host": "evil.example"})).status_code == 400
        response = await client.post("/api/messages", headers=headers, content="text")
        assert response.status_code == 415
        json_headers = {**headers, "Content-Type": "application/json"}
        for invalid in (
            '{"text":"hello","metadata":{"x":NaN}}',
            '{"text":"hello","metadata":{"x":Infinity}}',
        ):
            response = await client.post("/api/messages", headers=json_headers, content=invalid)
            assert response.status_code == 400 and response.json()["code"] == "invalid_json"
        response = await client.post(
            "/api/messages", headers=json_headers, content='{"text":"x","metadata":{"x":1e999}}'
        )
        assert response.status_code in (400, 422)
        response = await client.post("/api/messages", headers=json_headers, content='{"text":')
        assert response.status_code == 400
        response = await client.post("/api/messages", headers=headers, json={"text": " "})
        assert response.status_code == 422
        response = await client.post(
            "/api/messages",
            headers={**json_headers, "Content-Length": str(MAX_REQUEST_BYTES + 1)},
            content="{}",
        )
        assert response.status_code == 413 and response.json()["code"] == "body_too_large"

        async def chunks():
            yield b"x" * (MAX_REQUEST_BYTES // 2)
            yield b"x" * (MAX_REQUEST_BYTES // 2 + 1)

        response = await client.post("/api/messages", headers=json_headers, content=chunks())
        assert response.status_code == 413
        assert not webhook.events


@pytest.mark.asyncio
async def test_sdk_revocation_deletes_subscriptions_and_cancels_queued_delivery(tmp_path):
    async with running_app(tmp_path) as (client, app, bridge, webhook):
        tokens = await login(client)
        await subscribe(client, tokens["access_token"])
        response = await client.post(
            "/api/messages",
            headers=bearer(API_KEY),
            json={"text": "waiting", "channel": "chat"},
        )
        assert response.status_code == 202
        request_id = response.json()["request_id"]
        response = await client.post(
            "/revoke",
            data={
                "client_id": "dots-bridge",
                "token": tokens["refresh_token"],
                "token_type_hint": "refresh_token",
            },
        )
        assert response.status_code == 200, response.text
        assert (await rpc(client, tokens["access_token"], "events/list")).status_code == 401
        subscriptions = await client.get("/api/subscriptions", headers=bearer(API_KEY))
        assert subscriptions.json() == {"subscriptions": []}
        message = await client.get("/api/messages/" + request_id, headers=bearer(API_KEY))
        assert message.json()["delivery_status"] == "cancelled"
        assert message.json()["error"] == "authorization_revoked"
        assert await bridge.process_next() is False
        assert not webhook.events


@pytest.mark.asyncio
async def test_lifespan_worker_delivers_without_manual_processing(tmp_path):
    async with running_app(tmp_path, start_worker=True) as (client, app, bridge, webhook):
        token = (await login(client))["access_token"]
        await subscribe(client, token)
        response = await client.post(
            "/api/messages",
            headers=bearer(API_KEY),
            json={"text": "automatic", "channel": "chat"},
        )
        assert response.status_code == 202
        await asyncio.wait_for(webhook.received.wait(), timeout=2)
        assert webhook.events[0][0]["data"]["text"] == "automatic"
        assert bridge._worker is not None
    assert bridge._worker is None


@pytest.mark.asyncio
async def test_real_oauth_narrowed_tokens_enforce_each_mcp_action_scope(tmp_path):
    async with running_app(tmp_path) as (client, app, bridge, webhook):
        full_token = (await login(client))["access_token"]
        await subscribe(client, full_token)
        response = await client.post(
            "/api/messages",
            headers=bearer(API_KEY),
            json={"text": "Scoped permissions", "channel": "chat"},
        )
        request_id = response.json()["request_id"]
        assert await bridge.process_next() is True
        read_token = (await login(client, scope="events:read"))["access_token"]
        assert (await rpc(client, read_token, "events/list")).json()["result"]["events"]
        response = await rpc(client, read_token, "tools/list")
        names = {tool["name"] for tool in response.json()["result"]["tools"]}
        assert names == {"get_current_user", "get_message"}
        response = await rpc(
            client,
            read_token,
            "tools/call",
            {
                "name": "get_message",
                "arguments": {"request_id": request_id},
            },
        )
        assert response.json()["result"]["structuredContent"]["text"] == "Scoped permissions"
        response = await rpc(
            client,
            read_token,
            "tools/call",
            {
                "name": "reply_to_app",
                "arguments": {"request_id": request_id, "text": "Forbidden"},
            },
        )
        denied = response.json()
        assert "error" in denied or denied.get("result", {}).get("isError") is True
        assert (await bridge.get_message(request_id))["reply"] is None

        reply_token = (await login(client, scope="messages:reply"))["access_token"]
        response = await rpc(client, reply_token, "events/list")
        assert response.json()["error"]["data"]["reason"] == "insufficient_scope"
        response = await rpc(client, reply_token, "tools/list")
        assert {tool["name"] for tool in response.json()["result"]["tools"]} == {"reply_to_app"}
        response = await rpc(
            client,
            reply_token,
            "tools/call",
            {
                "name": "reply_to_app",
                "arguments": {"request_id": request_id, "text": "Allowed"},
            },
        )
        assert response.json()["result"]["structuredContent"]["reply"] == "Allowed"
        assert (await bridge.get_message(request_id))["reply"] == "Allowed"


@pytest.mark.asyncio
@pytest.mark.parametrize("replay_endpoint", ["/token", "/revoke"])
async def test_rotated_refresh_replay_revokes_successor_and_cancels_delivery(
    tmp_path, replay_endpoint
):
    async with running_app(tmp_path) as (client, app, bridge, webhook):
        tokens = await login(client)
        await subscribe(client, tokens["access_token"])
        response = await client.post(
            "/api/messages",
            headers=bearer(API_KEY),
            json={"text": "Must not deliver after revocation", "channel": "chat"},
        )
        request_id = response.json()["request_id"]
        response = await client.post(
            "/token",
            data={
                "client_id": "dots-bridge",
                "grant_type": "refresh_token",
                "refresh_token": tokens["refresh_token"],
            },
        )
        assert response.status_code == 200
        successor = response.json()
        if replay_endpoint == "/token":
            response = await client.post(
                "/token",
                data={
                    "client_id": "dots-bridge",
                    "grant_type": "refresh_token",
                    "refresh_token": tokens["refresh_token"],
                },
            )
            assert response.json()["error"] == "invalid_grant"
        else:
            response = await client.post(
                "/revoke",
                data={
                    "client_id": "dots-bridge",
                    "token": tokens["refresh_token"],
                    "token_type_hint": "refresh_token",
                },
            )
            assert response.status_code == 200
        assert (await rpc(client, successor["access_token"], "events/list")).status_code == 401
        assert (await client.get("/api/subscriptions", headers=bearer(API_KEY))).json() == {
            "subscriptions": [],
        }
        message = await bridge.get_message(request_id)
        assert message["delivery_status"] == "cancelled"
        assert message["error"] == "authorization_revoked"
        assert await bridge.process_next() is False
        assert not webhook.events
