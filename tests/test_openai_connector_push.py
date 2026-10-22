"""Exercise opt-in WebSocket pushes through the bridge's public HTTP client."""

from __future__ import annotations

import asyncio
import json
import threading
from contextlib import asynccontextmanager, contextmanager
from urllib.parse import unquote

import httpx
import pytest
from cryptography.fernet import Fernet
from starlette.testclient import TestClient

from dots_bridge import AsyncBridgeClient, DotsBridge, Settings, create_app
from dots_bridge.connectors import create_openai_connector

KEY = "test-push-connector-application-key"
AUTH = {"Authorization": "Bearer " + KEY}


def receipt(sequence, *, kind="push", channel="work", text=None):
    return {
        "id": f"received_{sequence}",
        "sequence": sequence,
        "kind": kind,
        "request_id": f"req_old_{sequence}" if kind == "reply" else None,
        "text": text or f"Message {sequence} 🌏",
        "channel": channel,
        "metadata": {"source": "test", "values": [sequence, None]},
        "received_at": "2026-10-22T00:00:00Z",
    }


class BridgeHTTP:
    """A durable fake upstream; the connector accesses it only through HTTP."""

    def __init__(self, receipts=()):
        self.receipts = list(receipts)
        self.calls = []
        self.sent = []
        self.consumers = {}
        self.acknowledgements = []
        self.cursor_failures = 0
        self.poll_failures = 0
        self.block_poll = False
        self.poll_started = threading.Event()
        self.poll_cancelled = threading.Event()
        self.reply_started = threading.Event()
        self.reply_cancelled = threading.Event()
        self.reply_release = None

    async def handle(self, request):
        assert request.headers["authorization"] == "Bearer " + KEY
        path = unquote(request.url.path)
        self.calls.append((request.method, path, dict(request.url.params)))
        if request.method == "POST" and path == "/api/messages":
            payload = json.loads(request.content)
            self.sent.append(payload)
            return httpx.Response(202, json={"request_id": f"req_{len(self.sent)}"})
        if request.method == "GET" and path.startswith("/api/messages/"):
            request_id = path.rsplit("/", 1)[1]
            text = self.sent[int(request_id.removeprefix("req_")) - 1]["text"]
            if text == "blocked":
                self.reply_release = asyncio.Event()
                self.reply_started.set()
                try:
                    await self.reply_release.wait()
                except asyncio.CancelledError:
                    self.reply_cancelled.set()
                    raise
            return httpx.Response(
                200,
                json={
                    "request_id": request_id,
                    "reply_status": "replied",
                    "reply": "Reply: " + text,
                    "delivery_status": "accepted",
                },
            )
        if path == "/api/received" and request.method == "GET":
            self.poll_started.set()
            if self.poll_failures:
                self.poll_failures -= 1
                return httpx.Response(503, json={"code": "temporary", "message": "Try again"})
            if self.block_poll:
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    self.poll_cancelled.set()
                    raise
            after = int(request.url.params.get("after", 0))
            channel = request.url.params.get("channel")
            return httpx.Response(
                200,
                json={
                    "messages": [
                        item
                        for item in self.receipts
                        if item["sequence"] > after and item["channel"] == channel
                    ][: int(request.url.params.get("limit", 100))]
                },
            )
        if path.startswith("/api/consumers/"):
            consumer_id = path.removeprefix("/api/consumers/").removesuffix("/ack")
            if request.method == "GET":
                if self.cursor_failures:
                    self.cursor_failures -= 1
                    return httpx.Response(
                        503, json={"code": "temporary", "message": "Temporary cursor failure"}
                    )
                channel = request.url.params.get("channel")
                state = self.consumers.setdefault(consumer_id, {"cursor": 0, "channel": channel})
                assert state["channel"] == channel
                return httpx.Response(200, json={"cursor": state["cursor"]})
            if request.method == "POST" and path.endswith("/ack"):
                payload = json.loads(request.content)
                state = self.consumers[consumer_id]
                assert state["channel"] == payload["channel"]
                state["cursor"] = max(state["cursor"], payload["sequence"])
                self.acknowledgements.append((consumer_id, payload))
                return httpx.Response(200, json={"cursor": state["cursor"]})
        raise AssertionError(f"Unexpected bridge endpoint: {request.method} {path}")

    async def release_reply(self):
        self.reply_release.set()


@contextmanager
def connector(upstream=None, *, channel="work"):
    upstream = upstream or BridgeHTTP()
    bridge = AsyncBridgeClient(
        "http://bridge.test", api_key=KEY, transport=httpx.MockTransport(upstream.handle)
    )
    app = create_openai_connector(api_key=KEY, client=bridge, channel=channel)
    with TestClient(app) as http:
        try:
            yield http, upstream
        finally:
            http.portal.call(bridge.aclose)
    assert not bridge.receiving


def receive_event(socket):
    # Bound the test transport's receive so missing events fail instead of hanging.
    async def bounded_receive():
        async with asyncio.timeout(3):
            return await socket._send_rx.receive()

    message = socket.portal.call(bounded_receive)
    assert message["type"] == "websocket.send", message
    return json.loads(message["text"])


def subscribe(socket, consumer_id="personal-agent"):
    socket.send_json({"type": "dots.subscribe", "consumer_id": consumer_id})
    return receive_event(socket)


def acknowledge(socket, message_id):
    socket.send_json({"type": "dots.push.ack", "message_id": message_id})
    return receive_event(socket)


def receive_response(socket):
    events = []
    for _ in range(12):
        event = receive_event(socket)
        events.append(event)
        if event["type"] == "response.completed":
            return events
    raise AssertionError("Standard response did not complete")


def test_standard_websocket_client_does_not_opt_into_received_messages():
    with connector(BridgeHTTP([receipt(1)])) as (http, upstream):
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            socket.send_json({"type": "response.create", "input": "standard"})
            events = receive_response(socket)
            assert all(event["type"].startswith("response.") for event in events)
            assert events[-1]["response"]["output"][0]["content"][0]["text"] == "Reply: standard"
        assert all(path.startswith("/api/messages") for _, path, _ in upstream.calls)
        assert upstream.consumers == {}


def test_subscribe_precedes_full_push_and_uses_configured_channel_and_consumer_namespace():
    expected = receipt(1)
    with connector(BridgeHTTP([expected, receipt(2, channel="other")])) as (http, upstream):
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            assert subscribe(socket) == {
                "type": "dots.subscribed",
                "consumer_id": "personal-agent",
                "channel": "work",
                "cursor": 0,
            }
            assert receive_event(socket) == {
                "type": "dots.push",
                "consumer_id": "personal-agent",
                "message": expected,
            }
            assert upstream.consumers["openai-ws:personal-agent"]["cursor"] == 0
            assert upstream.acknowledgements == []
            assert acknowledge(socket, expected["id"]) == {
                "type": "dots.push.acked",
                "consumer_id": "personal-agent",
                "message_id": expected["id"],
                "sequence": 1,
            }
            assert upstream.consumers["openai-ws:personal-agent"]["cursor"] == 1
        assert all(
            query["channel"] == "work"
            for _, path, query in upstream.calls
            if path == "/api/received" or path.startswith("/api/consumers/") and query
        )
        assert "personal-agent" not in upstream.consumers


def test_push_subscription_and_ack_remain_live_while_standard_reply_waits():
    with connector(BridgeHTTP([receipt(1)])) as (http, upstream):
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            socket.send_json({"type": "response.create", "input": "blocked", "stream_id": "lane"})
            starts = [receive_event(socket), receive_event(socket)]
            assert [event["type"] for event in starts] == [
                "response.created",
                "response.in_progress",
            ]
            assert upstream.reply_started.wait(1)
            assert subscribe(socket)["type"] == "dots.subscribed"
            push = receive_event(socket)
            assert push["type"] == "dots.push"
            assert acknowledge(socket, push["message"]["id"])["type"] == "dots.push.acked"
            http.portal.call(upstream.release_reply)
            completed = starts + receive_response(socket)
            assert len(completed) == 9
            assert [event["sequence_number"] for event in completed] == list(range(9))
            assert all(event["stream_id"] == "lane" for event in completed)
            assert completed[-1]["response"]["output"][0]["content"][0]["text"] == "Reply: blocked"


def test_unacked_push_replays_then_explicit_ack_advances_durable_consumer_across_reconnects():
    with connector(BridgeHTTP([receipt(1), receipt(2)])) as (http, upstream):
        with http.websocket_connect("/v1/responses", headers=AUTH) as first:
            assert subscribe(first)["cursor"] == 0
            initial = receive_event(first)
        assert upstream.acknowledgements == []
        with http.websocket_connect("/v1/responses", headers=AUTH) as second:
            assert subscribe(second)["cursor"] == 0
            assert receive_event(second) == initial
            ack = acknowledge(second, initial["message"]["id"])
            following = receive_event(second)
            assert ack["type"] == "dots.push.acked"
            assert following["type"] == "dots.push"
            assert following["message"]["id"] == "received_2"
            # A repeated last ACK confirms success without dismissing the next pending push.
            assert acknowledge(second, initial["message"]["id"]) == ack
            assert acknowledge(second, following["message"]["id"])["sequence"] == 2
        with http.websocket_connect("/v1/responses", headers=AUTH) as third:
            assert subscribe(third)["cursor"] == 2
            third.send_json({"type": "response.create", "input": "barrier"})
            assert all(event["type"].startswith("response.") for event in receive_response(third))
        assert upstream.consumers["openai-ws:personal-agent"]["cursor"] == 2
        assert [payload["sequence"] for _, payload in upstream.acknowledgements] == [1, 2]


def test_replies_are_skipped_in_order_without_advancing_past_unacknowledged_push():
    items = [receipt(1, kind="reply"), receipt(2), receipt(3, kind="reply"), receipt(4)]
    with connector(BridgeHTTP(items)) as (http, upstream):
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            assert subscribe(socket)["type"] == "dots.subscribed"
            push = receive_event(socket)
            assert push["message"]["id"] == "received_2"
            assert upstream.consumers["openai-ws:personal-agent"]["cursor"] == 1
            socket.send_json({"type": "response.create", "input": "barrier"})
            assert all(event["type"].startswith("response.") for event in receive_response(socket))
            assert [payload["sequence"] for _, payload in upstream.acknowledgements] == [1]
            assert acknowledge(socket, push["message"]["id"])["sequence"] == 2
            following = receive_event(socket)
            assert following["message"]["id"] == "received_4"
            assert upstream.consumers["openai-ws:personal-agent"]["cursor"] == 3


def test_unknown_ack_returns_400_without_advancing_cursor_or_dismissing_pending_message():
    with connector(BridgeHTTP([receipt(1)])) as (http, upstream):
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            assert subscribe(socket)["type"] == "dots.subscribed"
            push = receive_event(socket)
            error = acknowledge(socket, "never-sent")
            assert error["type"] == "error"
            assert error["status"] == 400
            assert error["error"]["code"] == "invalid_push_ack"
            assert upstream.acknowledgements == []
            assert acknowledge(socket, push["message"]["id"])["type"] == "dots.push.acked"


def test_same_consumer_cannot_be_claimed_by_two_sockets_and_is_released_on_disconnect():
    with connector(BridgeHTTP([receipt(1)])) as (http, _):
        with http.websocket_connect("/v1/responses", headers=AUTH) as first:
            assert subscribe(first)["type"] == "dots.subscribed"
            assert receive_event(first)["type"] == "dots.push"
            with http.websocket_connect("/v1/responses", headers=AUTH) as second:
                error = subscribe(second)
                assert error["type"] == "error"
                assert error["status"] == 409
                assert error["error"]["code"] == "push_consumer_conflict"
                assert subscribe(second, "another-agent")["type"] == "dots.subscribed"
                assert receive_event(second)["type"] == "dots.push"
        with http.websocket_connect("/v1/responses", headers=AUTH) as reconnected:
            assert subscribe(reconnected)["type"] == "dots.subscribed"


@pytest.mark.parametrize("consumer_id", [None, "", "bad/consumer", "x" * 119, 123])
def test_invalid_consumer_is_rejected_before_upstream_access_and_socket_can_retry(consumer_id):
    with connector() as (http, upstream):
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            error = subscribe(socket, consumer_id)
            assert error["type"] == "error"
            assert error["status"] == 400
            assert upstream.calls == []
            assert subscribe(socket)["type"] == "dots.subscribed"


def test_initial_cursor_failure_reports_error_and_allows_same_socket_retry():
    upstream = BridgeHTTP([receipt(1)])
    upstream.cursor_failures = 1
    with connector(upstream) as (http, _):
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            error = subscribe(socket)
            assert error["type"] == "error"
            assert error["status"] == 502
            assert error["error"]["code"] == "temporary"
            assert subscribe(socket)["type"] == "dots.subscribed"
            assert receive_event(socket)["type"] == "dots.push"


def test_temporary_poll_failure_is_retried_without_acknowledging_or_losing_push():
    upstream = BridgeHTTP([receipt(1)])
    upstream.poll_failures = 1
    with connector(upstream) as (http, _):
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            assert subscribe(socket)["type"] == "dots.subscribed"
            push = receive_event(socket)
            assert push["type"] == "dots.push"
            assert push["message"]["id"] == "received_1"
            assert upstream.acknowledgements == []
            assert sum(path == "/api/received" for _, path, _ in upstream.calls) == 2


def test_disconnect_cancels_push_poll_and_standard_reply_and_releases_consumer():
    upstream = BridgeHTTP()
    upstream.block_poll = True
    with connector(upstream) as (http, _):
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            assert subscribe(socket)["type"] == "dots.subscribed"
            assert upstream.poll_started.wait(1)
            socket.send_json({"type": "response.create", "input": "blocked"})
            receive_event(socket)
            receive_event(socket)
            assert upstream.reply_started.wait(1)
        assert upstream.poll_cancelled.wait(1)
        assert upstream.reply_cancelled.wait(1)
        assert upstream.acknowledgements == []
        with http.websocket_connect("/v1/responses", headers=AUTH) as reconnected:
            assert subscribe(reconnected)["type"] == "dots.subscribed"


@contextmanager
def real_connector(settings):
    backend = DotsBridge(settings)
    bridge_app = create_app(settings, bridge=backend, start_worker=False)
    client = AsyncBridgeClient(
        settings.public_base_url,
        api_key=settings.api_key,
        transport=httpx.ASGITransport(app=bridge_app),
    )
    app = create_openai_connector(api_key=settings.api_key, client=client, channel="work")
    connector_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(application):
        try:
            async with bridge_app.router.lifespan_context(bridge_app):
                async with connector_lifespan(application):
                    yield
        finally:
            await client.aclose()

    app.router.lifespan_context = lifespan
    with TestClient(app) as http:
        yield http, backend


def test_real_bridge_http_and_sqlite_keep_acknowledgement_across_connector_restart(tmp_path):
    settings = Settings(
        public_base_url="https://bridge.test",
        owner_key="owner-" + "a" * 32,
        api_key=KEY,
        storage_key=Fernet.generate_key().decode(),
        db_path=tmp_path / "bridge.sqlite3",
    )

    async def push(backend):
        return await backend.send_to_app(
            "依頼なしの自発通知 🌏",
            channel="work",
            idempotency_key="sqlite-push-1",
            metadata={"source": "real-bridge", "nested": [1, None]},
        )

    with real_connector(settings) as (http, backend):
        expected = http.portal.call(push, backend)
        with http.websocket_connect("/v1/responses", headers=AUTH) as first:
            assert subscribe(first)["cursor"] == 0
            assert receive_event(first)["message"] == expected
        with http.websocket_connect("/v1/responses", headers=AUTH) as second:
            assert subscribe(second)["cursor"] == 0
            assert receive_event(second)["message"] == expected
            assert acknowledge(second, expected["id"])["sequence"] == expected["sequence"]
        assert (
            http.portal.call(backend.get_consumer_cursor, "openai-ws:personal-agent", "work")
            == expected["sequence"]
        )
        assert http.portal.call(backend.get_consumer_cursor, "personal-agent", "work") == 0
    with real_connector(settings) as (http, backend):
        with http.websocket_connect("/v1/responses", headers=AUTH) as restarted:
            assert subscribe(restarted)["cursor"] == expected["sequence"]
        assert http.portal.call(backend.list_received) == [expected]
