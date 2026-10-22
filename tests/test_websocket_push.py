"""Durable WebSocket push delivery against the bridge's existing receipt API."""

import asyncio
import json
from contextlib import asynccontextmanager

import httpx
import pytest

from dots_bridge import AsyncBridgeClient, BridgeError
from dots_bridge.connectors._openai_protocol import ProtocolError
from dots_bridge.connectors._websocket_push import WebSocketPushSession

KEY = "push-session-application-key"
CHANNEL = "work"
CONSUMER = "openai-ws:desktop"


def receipt(sequence, *, kind="push", channel=CHANNEL):
    return {
        "sequence": sequence,
        "id": f"recv_{sequence}",
        "kind": kind,
        "request_id": f"req_{sequence}" if kind == "reply" else None,
        "channel": channel,
        "text": f"Message {sequence}",
        "metadata": {"source": "test"},
        "received_at": "2026-10-22T00:00:00Z",
    }


class ReceiptAPI:
    """Persist cursors across simulated socket sessions, without a real database."""

    def __init__(self, messages=()):
        self.messages = list(messages)
        self.cursors = {}
        self.fetches = []
        self.acks = []
        self.fetch_failures = []
        self.ack_failures = []
        self.cursor_failures = []
        self.fetch_entered = asyncio.Event()
        self.fetch_cancelled = asyncio.Event()
        self.fetch_release = None

    @staticmethod
    def fail(failures):
        if failures:
            error = failures.pop(0)
            if isinstance(error, Exception):
                raise error
            return error
        return None

    async def handle(self, request):
        assert request.headers["authorization"] == "Bearer " + KEY
        if request.url.path == "/api/received":
            after = int(request.url.params["after"])
            limit = int(request.url.params["limit"])
            channel = request.url.params.get("channel")
            self.fetches.append((after, limit, channel))
            self.fetch_entered.set()
            if self.fetch_release is not None:
                try:
                    await self.fetch_release.wait()
                except asyncio.CancelledError:
                    self.fetch_cancelled.set()
                    raise
            if failure := self.fail(self.fetch_failures):
                return failure
            return httpx.Response(
                200,
                json={
                    "messages": [
                        item
                        for item in self.messages
                        if item["sequence"] > after
                        and (channel is None or item["channel"] == channel)
                    ][:limit]
                },
            )
        if request.url.path.startswith("/api/consumers/"):
            consumer = request.url.path.split("/")[3]
            if request.method == "POST":
                payload = json.loads(request.content)
                key = (consumer, payload.get("channel"))
                self.acks.append((consumer, payload["sequence"], payload.get("channel")))
                if failure := self.fail(self.ack_failures):
                    return failure
                self.cursors[key] = max(self.cursors.get(key, 0), payload["sequence"])
            else:
                key = (consumer, request.url.params.get("channel"))
                if failure := self.fail(self.cursor_failures):
                    return failure
            return httpx.Response(200, json={"cursor": self.cursors.get(key, 0)})
        raise AssertionError(f"Unexpected bridge request: {request.method} {request.url.path}")


class EventSink:
    def __init__(self):
        self.events = []
        self.queue = asyncio.Queue()

    async def send(self, event):
        self.events.append(event)
        self.queue.put_nowait(event)

    async def next(self):
        return await asyncio.wait_for(self.queue.get(), 1)


@asynccontextmanager
async def push_session(api=None, *, sink=None, registry=None):
    api = api if api is not None else ReceiptAPI()
    sink = sink if sink is not None else EventSink()
    registry = registry if registry is not None else set()
    async with AsyncBridgeClient(
        "http://bridge.test", api_key=KEY, transport=httpx.MockTransport(api.handle)
    ) as client:
        session = WebSocketPushSession(
            client,
            channel=CHANNEL,
            send_event=sink.send,
            consumer_registry=registry,
            poll_interval=0.001,
        )
        try:
            yield session, sink, api, registry
        finally:
            await session.close()


async def wait_until(predicate):
    async with asyncio.timeout(2):
        while not predicate():  # noqa: ASYNC110 - Observe independently running delivery.
            await asyncio.sleep(0.001)


async def subscribe(session, sink, consumer_id="desktop", *, cursor=0):
    await session.subscribe(consumer_id)
    assert await sink.next() == {
        "type": "dots.subscribed",
        "consumer_id": consumer_id,
        "channel": CHANNEL,
        "cursor": cursor,
    }


async def test_push_requires_manual_ack_before_any_later_receipt_is_consumed():
    api = ReceiptAPI([receipt(1), receipt(2, kind="reply"), receipt(3)])
    async with push_session(api) as (session, sink, _, _):
        await subscribe(session, sink)
        assert await sink.next() == {
            "type": "dots.push",
            "consumer_id": "desktop",
            "message": receipt(1),
        }
        await asyncio.sleep(0.02)
        assert api.cursors == {}
        assert api.acks == []
        assert len(sink.events) == 2

        await session.acknowledge("recv_1")
        assert await sink.next() == {
            "type": "dots.push.acked",
            "consumer_id": "desktop",
            "message_id": "recv_1",
            "sequence": 1,
        }
        assert (await sink.next())["message"] == receipt(3)
        assert api.cursors == {(CONSUMER, CHANNEL): 2}
        assert api.acks == [(CONSUMER, 1, CHANNEL), (CONSUMER, 2, CHANNEL)]
        await session.acknowledge("recv_1")
        assert (await sink.next())["message_id"] == "recv_1"
        assert api.cursors == {(CONSUMER, CHANNEL): 2}
        assert api.acks == [(CONSUMER, 1, CHANNEL), (CONSUMER, 2, CHANNEL)]
        await session.acknowledge("recv_3")
        assert (await sink.next())["sequence"] == 3
        assert api.cursors == {(CONSUMER, CHANNEL): 3}


async def test_request_progress_is_skipped_by_push_consumer_without_becoming_a_push():
    progress = {**receipt(1, kind="progress"), "request_id": "req_work"}
    api = ReceiptAPI([progress, receipt(2)])
    async with push_session(api) as (session, sink, _, _):
        await subscribe(session, sink)
        pushed = await sink.next()
        assert pushed["type"] == "dots.push"
        assert pushed["message"]["sequence"] == 2
        assert pushed["message"]["request_id"] is None
        assert api.cursors[(CONSUMER, CHANNEL)] == 1
        await session.acknowledge(pushed["message"]["id"])
        assert (await sink.next())["type"] == "dots.push.acked"
        assert api.cursors[(CONSUMER, CHANNEL)] == 2


async def test_reply_receipts_are_skipped_using_only_the_push_consumers_cursor():
    api = ReceiptAPI(
        [receipt(1, kind="reply"), receipt(2, channel="elsewhere"), receipt(3, kind="reply")]
    )
    api.cursors[("desktop", CHANNEL)] = 99
    async with push_session(api) as (session, sink, _, _):
        await subscribe(session, sink)
        await wait_until(lambda: api.cursors.get((CONSUMER, CHANNEL)) == 3)
        assert [event["type"] for event in sink.events] == ["dots.subscribed"]
        assert api.cursors[("desktop", CHANNEL)] == 99
        assert all(channel == CHANNEL for _, _, channel in api.fetches)
        assert api.acks == [(CONSUMER, 1, CHANNEL), (CONSUMER, 3, CHANNEL)]


async def test_reconnect_replays_unacked_push_then_resumes_after_persisted_ack():
    api = ReceiptAPI([receipt(1), receipt(2)])
    registry = set()
    async with push_session(api, registry=registry) as (session, sink, _, _):
        await subscribe(session, sink)
        assert (await sink.next())["message"]["id"] == "recv_1"
    assert registry == set()
    assert api.cursors == {}

    async with push_session(api, registry=registry) as (session, sink, _, _):
        await subscribe(session, sink)
        assert (await sink.next())["message"]["id"] == "recv_1"
        await session.acknowledge("recv_1")
        assert (await sink.next())["type"] == "dots.push.acked"
        assert (await sink.next())["message"]["id"] == "recv_2"

    async with push_session(api, registry=registry) as (session, sink, _, _):
        await subscribe(session, sink, cursor=1)
        assert (await sink.next())["message"]["id"] == "recv_2"


async def test_same_subscription_is_idempotent_and_consumer_cannot_be_stolen():
    registry = set()
    async with push_session(registry=registry) as (first, first_sink, _, _):
        await subscribe(first, first_sink)
        task = first.task
        await first.subscribe("desktop")
        assert first.task is task
        with pytest.raises(ProtocolError) as changed:
            await first.subscribe("other-desktop")
        assert changed.value.status_code == 409

        async with push_session(registry=registry) as (second, second_sink, _, _):
            with pytest.raises(ProtocolError) as collision:
                await second.subscribe("desktop")
            assert collision.value.status_code == 409
            assert second_sink.events == []
            await first.close()
            await subscribe(second, second_sink)


async def test_failed_initial_cursor_load_releases_consumer_and_allows_retry_with_another_id():
    api = ReceiptAPI()
    api.cursor_failures = [httpx.Response(503, json={"error": {"code": "unavailable"}})]
    async with push_session(api) as (session, sink, _, registry):
        with pytest.raises(BridgeError) as error:
            await session.subscribe("desktop")
        assert error.value.status_code == 503
        assert sink.events == []
        assert registry == set()
        assert session.task is None or session.task.done()
        await subscribe(session, sink, "another-app")


async def test_failed_durable_ack_keeps_pending_push_until_ack_is_retried():
    api = ReceiptAPI([receipt(1), receipt(2)])
    api.ack_failures = [httpx.Response(503, json={"error": {"code": "unavailable"}})]
    async with push_session(api) as (session, sink, _, _):
        await subscribe(session, sink)
        await sink.next()
        with pytest.raises(BridgeError) as error:
            await session.acknowledge("recv_1")
        assert error.value.status_code == 503
        assert api.cursors == {}
        await asyncio.sleep(0.02)
        assert len(sink.events) == 2
        await session.acknowledge("recv_1")
        assert (await sink.next())["type"] == "dots.push.acked"
        assert (await sink.next())["message"]["id"] == "recv_2"
        assert api.acks == [(CONSUMER, 1, CHANNEL), (CONSUMER, 1, CHANNEL)]


async def test_ack_confirmation_send_failure_does_not_release_next_push_before_retry():
    class FailingAckSink(EventSink):
        def __init__(self):
            super().__init__()
            self.ack_started = asyncio.Event()
            self.ack_release = asyncio.Event()
            self.failed = False

        async def send(self, event):
            if event["type"] == "dots.push.acked" and not self.failed:
                self.failed = True
                self.ack_started.set()
                await self.ack_release.wait()
                raise OSError("socket unavailable")
            await super().send(event)

    api = ReceiptAPI([receipt(1), receipt(2)])
    sink = FailingAckSink()
    async with push_session(api, sink=sink) as (session, _, _, _):
        await subscribe(session, sink)
        await sink.next()
        acknowledgement = asyncio.create_task(session.acknowledge("recv_1"))
        try:
            await asyncio.wait_for(sink.ack_started.wait(), 1)
            assert api.cursors == {(CONSUMER, CHANNEL): 1}
            await asyncio.sleep(0.02)
            assert len(sink.events) == 2
            sink.ack_release.set()
            with pytest.raises(OSError, match="socket unavailable"):
                await acknowledgement
            await asyncio.sleep(0.02)
            assert len(sink.events) == 2
            await session.acknowledge("recv_1")
            assert (await sink.next())["message_id"] == "recv_1"
            assert (await sink.next())["message"]["id"] == "recv_2"
        finally:
            sink.ack_release.set()
            if not acknowledgement.done():
                acknowledgement.cancel()
            await asyncio.gather(acknowledgement, return_exceptions=True)


@pytest.mark.parametrize("consumer_id", [None, True, 1, "", "has space", "slash/id", "x" * 119])
async def test_invalid_public_consumer_ids_are_rejected_without_claiming_registry(consumer_id):
    async with push_session() as (session, sink, api, registry):
        with pytest.raises(ProtocolError) as error:
            await session.subscribe(consumer_id)
        assert error.value.status_code == 400
        assert sink.events == []
        assert api.fetches == []
        assert registry == set()


async def test_longest_valid_public_consumer_id_fits_namespaced_bridge_id():
    consumer_id = "a" * 118
    api = ReceiptAPI([receipt(1)])
    async with push_session(api) as (session, sink, _, _):
        await subscribe(session, sink, consumer_id)
        await sink.next()
        await session.acknowledge("recv_1")
        assert api.acks == [("openai-ws:" + consumer_id, 1, CHANNEL)]


@pytest.mark.parametrize("message_id", [None, True, 1, "", "unknown", "recv_2"])
async def test_malformed_or_unknown_ack_cannot_advance_past_pending_push(message_id):
    api = ReceiptAPI([receipt(1), receipt(2)])
    async with push_session(api) as (session, sink, _, _):
        await subscribe(session, sink)
        await sink.next()
        with pytest.raises(ProtocolError) as error:
            await session.acknowledge(message_id)
        assert error.value.status_code == 400
        assert api.cursors == {}
        assert api.acks == []
        assert len(sink.events) == 2


async def test_ack_before_subscribe_is_rejected():
    async with push_session() as (session, sink, api, _):
        with pytest.raises(ProtocolError) as error:
            await session.acknowledge("recv_1")
        assert error.value.status_code == 400
        assert api.acks == []
        assert sink.events == []


async def test_transient_receipt_fetch_errors_retry_without_claiming_delivery():
    api = ReceiptAPI([receipt(1)])
    api.fetch_failures = [
        httpx.ConnectError("private upstream address"),
        httpx.Response(503, json={"error": {"code": "unavailable", "message": "private"}}),
    ]
    async with push_session(api) as (session, sink, _, _):
        await subscribe(session, sink)
        assert (await sink.next())["message"] == receipt(1)
        assert len(api.fetches) == 3
        assert api.cursors == {}
        assert session.error is None


async def test_permanent_receipt_error_reports_safe_error_and_stops_polling():
    api = ReceiptAPI()
    api.fetch_failures = [
        httpx.Response(
            401,
            json={"error": {"code": "unauthorized", "message": "Bridge rejected credentials."}},
        )
    ]
    async with push_session(api) as (session, sink, _, registry):
        await subscribe(session, sink)
        event = await sink.next()
        assert event["type"] == "error"
        assert event["status"] == 502
        assert event["error"]["type"] == "server_error"
        assert event["error"]["code"] == "unauthorized"
        await wait_until(lambda: session.task is None or session.task.done())
        assert session.error is not None
        assert len(api.fetches) == 1
        assert registry == set()
        api.messages.append(receipt(1))
        await subscribe(session, sink)
        assert (await sink.next())["message"] == receipt(1)
        assert session.error is None


@pytest.mark.parametrize(
    "failure",
    [
        RuntimeError("private-secret"),
        httpx.UnsupportedProtocol("private-secret"),
        httpx.LocalProtocolError("private-secret"),
        httpx.TooManyRedirects("private-secret"),
    ],
)
async def test_permanent_network_or_unexpected_failure_reports_safe_error_and_stops(failure):
    api = ReceiptAPI()
    api.fetch_failures = [failure]
    async with push_session(api) as (session, sink, _, registry):
        await subscribe(session, sink)
        event = await sink.next()
        assert event["type"] == "error"
        assert event["status"] == 502
        assert event["error"]["code"] == "push_reception_failed"
        assert "private-secret" not in json.dumps(event)
        await wait_until(lambda: session.task is None or session.task.done())
        assert registry == set()
        assert len(api.fetches) == 1


async def test_failed_push_send_stops_without_ack_and_same_consumer_can_retry():
    class FailingPushSink(EventSink):
        failed = False

        async def send(self, event):
            if event["type"] == "dots.push" and not self.failed:
                self.failed = True
                raise OSError("private socket details")
            await super().send(event)

    api = ReceiptAPI([receipt(1)])
    sink = FailingPushSink()
    async with push_session(api, sink=sink) as (session, _, _, registry):
        await subscribe(session, sink)
        event = await sink.next()
        assert event["type"] == "error"
        assert "private socket details" not in json.dumps(event)
        await wait_until(lambda: session.task is None or session.task.done())
        assert registry == set()
        assert api.cursors == {}
        assert api.acks == []
        await subscribe(session, sink)
        assert (await sink.next())["message"] == receipt(1)


async def test_close_cancels_and_awaits_inflight_poll_before_releasing_consumer():
    api = ReceiptAPI()
    api.fetch_release = asyncio.Event()
    async with push_session(api) as (session, sink, _, registry):
        await subscribe(session, sink)
        await asyncio.wait_for(api.fetch_entered.wait(), 1)
        task = session.task
        assert registry
        await session.close()
        assert api.fetch_cancelled.is_set()
        assert task.done()
        assert registry == set()
        assert api.cursors == {}
