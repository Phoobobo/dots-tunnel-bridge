import asyncio
import json
import threading

import httpx
import pytest

from dots_bridge import AsyncBridgeClient, BridgeClient, BridgeError, ReceivedMessage
from dots_bridge.receiver import ReceivedDispatcher

KEY = "application-key"


def receipt(sequence=1, *, kind="reply", channel="default"):
    return {
        "sequence": sequence,
        "id": f"recv_{sequence}",
        "kind": kind,
        "request_id": f"req_{sequence}" if kind == "reply" else None,
        "channel": channel,
        "text": f"Message {sequence}",
        "metadata": {"source": "test"},
        "received_at": "2026-10-21T00:00:00Z",
    }


class ReceiveAPI:
    def __init__(self, messages=()):
        self.messages = list(messages)
        self.cursors = {}
        self.seen = []
        self.acknowledged = asyncio.Event()

    def handle(self, request):
        assert request.headers["authorization"] == "Bearer " + KEY
        self.seen.append((request.method, request.url.path))
        if request.url.path == "/api/received":
            after = int(request.url.params["after"])
            limit = int(request.url.params["limit"])
            channel = request.url.params.get("channel")
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
                self.cursors[key] = max(self.cursors.get(key, 0), payload["sequence"])
                self.acknowledged.set()
            else:
                key = (consumer, request.url.params.get("channel"))
            return httpx.Response(200, json={"cursor": self.cursors.get(key, 0)})
        if request.url.path == "/api/messages":
            return httpx.Response(202, json={"request_id": "req_new", "reply": None})
        raise AssertionError(f"Unexpected route: {request.url.path}")


async def wait_until(predicate):
    async with asyncio.timeout(2):
        while not predicate():  # noqa: ASYNC110 - Observe background task state within a deadline.
            await asyncio.sleep(0.001)


async def test_receiving_is_independent_from_sending_and_includes_unsolicited_push():
    api = ReceiveAPI([receipt(1), receipt(2, kind="push")])
    api.messages.reverse()  # Delivery is processed by sequence, even if the server reorders it.
    entered = asyncio.Event()
    release = asyncio.Event()
    received = []
    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:

        @client.on_received
        async def on_received(message):
            assert isinstance(message, ReceivedMessage)
            received.append(message)
            if message.sequence == 1:
                entered.set()
                await release.wait()

        await client.start_receiving(consumer_id="app", poll_interval=0.001)
        await asyncio.wait_for(entered.wait(), 1)
        assert client.receiving
        # Sending queues independently while a callback is still working.
        assert (await client.send_message("Hello"))["request_id"] == "req_new"
        assert api.cursors == {}
        release.set()
        await wait_until(lambda: api.cursors.get(("app", None)) == 2)
        assert [(item.kind, item.request_id) for item in received] == [
            ("reply", "req_1"),
            ("push", None),
        ]
    assert not client.receiving
    assert client.receiver_error is None


async def test_failed_handler_retries_all_callbacks_without_advancing_cursor(caplog):
    api = ReceiveAPI([receipt()])
    calls = []
    failed = asyncio.Event()
    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:

        @client.on_received
        async def first(message):
            calls.append(("first", message.sequence))

        @client.on_received
        async def second(message):
            calls.append(("second", message.sequence))
            if not failed.is_set():
                failed.set()
                raise RuntimeError("private-message-content-secret")

        await client.start_receiving(consumer_id="app", poll_interval=0.001)
        await asyncio.wait_for(failed.wait(), 1)
        assert api.cursors == {}
        await asyncio.wait_for(api.acknowledged.wait(), 1)
        assert calls == [("first", 1), ("second", 1), ("first", 1), ("second", 1)]
        assert api.cursors == {("app", None): 1}
        assert "RuntimeError" in caplog.text
        assert "private-message-content-secret" not in caplog.text


async def test_sync_callback_runs_outside_event_loop():
    api = ReceiveAPI([receipt()])
    main_thread = threading.get_ident()
    thread_ids = []
    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:

        @client.on_received
        def callback(message):
            thread_ids.append(threading.get_ident())
            assert message.text == "Message 1"

        await client.start_receiving(poll_interval=0.001)
        await asyncio.wait_for(api.acknowledged.wait(), 1)
    assert len(thread_ids) == 1
    assert thread_ids[0] != main_thread


async def test_consumer_resume_channel_filter_and_independent_consumers():
    api = ReceiveAPI(
        [receipt(1, channel="chat"), receipt(2, channel="other"), receipt(3, channel="chat")]
    )
    seen = []
    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:
        client.on_received(lambda message: seen.append(message.sequence))
        await client.start_receiving(consumer_id="app", channel="chat", poll_interval=0.001)
        await wait_until(lambda: api.cursors.get(("app", "chat")) == 3)
        await client.stop_receiving()
        assert seen == [1, 3]
        await client.start_receiving(consumer_id="app", channel="chat", poll_interval=0.001)
        await asyncio.sleep(0.01)
        await client.stop_receiving()
        assert seen == [1, 3]
        await client.start_receiving(consumer_id="other-app", channel="chat", poll_interval=0.001)
        await wait_until(lambda: api.cursors.get(("other-app", "chat")) == 3)
        assert seen == [1, 3, 1, 3]


async def test_start_is_idempotent_and_requires_handlers():
    api = ReceiveAPI()
    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:
        with pytest.raises(RuntimeError, match="on_received"):
            await client.start_receiving()
        assert api.seen == []
        client.on_received(lambda message: None)
        await asyncio.gather(client.start_receiving(), client.start_receiving())
        assert api.seen.count(("GET", "/api/consumers/default")) == 1
        with pytest.raises(RuntimeError, match="Stop receiving"):
            await client.start_receiving(channel="chat")


async def test_start_auth_error_is_raised_before_background_task():
    transport = httpx.MockTransport(
        lambda request: httpx.Response(401, json={"code": "unauthorized", "message": "Denied"})
    )
    async with AsyncBridgeClient(api_key=KEY, transport=transport) as client:
        client.on_received(lambda message: None)
        with pytest.raises(BridgeError) as error:
            await client.start_receiving()
        assert error.value.status_code == 401
        assert not client.receiving
        assert client.receiver_error is error.value


async def test_background_auth_error_stops_and_is_visible(caplog):
    def handle(request):
        if request.url.path == "/api/received":
            return httpx.Response(403, json={"message": "secret-error-message"})
        return httpx.Response(200, json={"cursor": 0})

    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        client.on_received(lambda message: None)
        await client.start_receiving(poll_interval=0.001)
        await wait_until(lambda: not client.receiving)
        assert isinstance(client.receiver_error, BridgeError)
        assert client.receiver_error.status_code == 403
        assert "secret-error-message" not in caplog.text


async def test_transient_fetch_and_ack_errors_retry_without_losing_receipt():
    api = ReceiveAPI([receipt()])
    fetches = 0
    ack_attempts = 0
    calls = []

    def handle(request):
        nonlocal fetches, ack_attempts
        if request.url.path == "/api/received":
            fetches += 1
            if fetches == 1:
                raise httpx.ConnectError("private-address", request=request)
        if request.method == "POST":
            ack_attempts += 1
            if ack_attempts == 1:
                return httpx.Response(503, json={"message": "temporary"})
        return api.handle(request)

    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        client.on_received(lambda message: calls.append(message.sequence))
        await client.start_receiving(poll_interval=0.001)
        await asyncio.wait_for(api.acknowledged.wait(), 1)
        assert calls == [1, 1]
        assert api.cursors == {("default", None): 1}
        assert client.receiver_error is None


async def test_stop_cancels_pending_handler_without_ack_and_aclose_stops_receiving():
    api = ReceiveAPI([receipt()])
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    client = AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle))

    @client.on_received
    async def callback(message):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    await client.start_receiving(poll_interval=0.001)
    await asyncio.wait_for(entered.wait(), 1)
    await client.aclose()
    assert cancelled.is_set()
    assert not client.receiving
    assert api.cursors == {}
    await client.stop_receiving()  # Idempotent shutdown.


async def test_callback_can_stop_receiver_after_acknowledging_current_message():
    api = ReceiveAPI([receipt(1), receipt(2)])
    calls = []
    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:

        @client.on_received
        async def callback(message):
            calls.append(message.sequence)
            await client.stop_receiving()

        await client.start_receiving(poll_interval=0.001)
        await wait_until(lambda: not client.receiving)
        assert calls == [1]
        assert api.cursors == {("default", None): 1}
        assert client.receiver_error is None


@pytest.mark.parametrize("cleanup_method", ["stop_receiving", "aclose"])
async def test_cancelled_callback_can_stop_or_close_client_during_external_stop(cleanup_method):
    api = ReceiveAPI([receipt()])
    entered = asyncio.Event()
    cleaned_up = asyncio.Event()
    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:

        @client.on_received
        async def callback(message):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                await getattr(client, cleanup_method)()
                cleaned_up.set()

        await client.start_receiving(poll_interval=0.001)
        await asyncio.wait_for(entered.wait(), 1)
        async with asyncio.timeout(0.5):
            await client.stop_receiving()
        assert cleaned_up.is_set()
        assert not client.receiving
        assert client.receiver_error is None
        assert api.cursors == {}


async def test_start_during_stop_is_rejected_and_concurrent_stops_finish_cleanup_once():
    api = ReceiveAPI([receipt()])
    entered = asyncio.Event()
    cleanup_entered = asyncio.Event()
    cleanup_release = asyncio.Event()
    cleaned_up = asyncio.Event()
    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:

        @client.on_received
        async def callback(message):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleanup_entered.set()
                await cleanup_release.wait()
                await client.stop_receiving()
                cleaned_up.set()

        await client.start_receiving(poll_interval=0.001)
        await asyncio.wait_for(entered.wait(), 1)
        stop = asyncio.create_task(client.stop_receiving())
        await asyncio.wait_for(cleanup_entered.wait(), 1)
        try:
            with pytest.raises(RuntimeError, match="stopping"):
                await client.start_receiving(poll_interval=0.001)
            second_stop = asyncio.create_task(client.stop_receiving())
            await asyncio.sleep(0)
            assert not cleaned_up.is_set()
        finally:
            cleanup_release.set()
        async with asyncio.timeout(0.5):
            await asyncio.gather(stop, second_stop)
        assert cleaned_up.is_set()
        assert not client.receiving
        assert api.cursors == {}


async def test_cancelling_stop_waiter_preserves_callback_cleanup_and_propagates_cancel():
    api = ReceiveAPI([receipt()])
    entered = asyncio.Event()
    cleanup_entered = asyncio.Event()
    cleanup_release = asyncio.Event()
    cleaned_up = asyncio.Event()
    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:

        @client.on_received
        async def callback(message):
            entered.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleanup_entered.set()
                await cleanup_release.wait()
                cleaned_up.set()

        await client.start_receiving(poll_interval=0.001)
        await asyncio.wait_for(entered.wait(), 1)
        stop = asyncio.create_task(client.stop_receiving())
        await asyncio.wait_for(cleanup_entered.wait(), 1)
        stop.cancel()
        try:
            async with asyncio.timeout(0.5):
                with pytest.raises(asyncio.CancelledError):
                    await stop
            assert client.receiving
            assert not cleaned_up.is_set()
        finally:
            cleanup_release.set()
        async with asyncio.timeout(0.5):
            await client.stop_receiving()
        assert cleaned_up.is_set()
        assert not client.receiving


async def test_dispatcher_notify_wakes_long_poll():
    messages = []
    polled = asyncio.Event()
    processed = asyncio.Event()

    async def get_cursor(**kwargs):
        return 0

    async def list_received(**kwargs):
        polled.set()
        return messages.copy()

    async def acknowledge(**kwargs):
        processed.set()
        return kwargs["sequence"]

    dispatcher = ReceivedDispatcher(
        list_received=list_received, get_cursor=get_cursor, acknowledge=acknowledge
    )
    dispatcher.on_received(lambda message: None)
    await dispatcher.start(poll_interval=60)
    try:
        await asyncio.wait_for(polled.wait(), 1)
        messages.append(receipt())
        dispatcher.notify()
        await asyncio.wait_for(processed.wait(), 1)
    finally:
        await dispatcher.stop()


@pytest.mark.parametrize(
    "invalid", [{}, {"messages": None}, {"messages": [receipt() | {"sequence": "1"}]}]
)
async def test_invalid_received_response_stops_receiver(invalid):
    def handle(request):
        return httpx.Response(
            200, json=invalid if request.url.path == "/api/received" else {"cursor": 0}
        )

    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        client.on_received(lambda message: None)
        await client.start_receiving(poll_interval=0.001)
        await wait_until(lambda: not client.receiving)
        assert isinstance(client.receiver_error, BridgeError)
        assert client.receiver_error.code == "invalid_response"


def test_manual_sync_receive_read_and_ack():
    api = ReceiveAPI([receipt(1), receipt(2, kind="push", channel="chat")])
    with BridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:
        assert client.get_consumer_cursor("manual", channel="chat") == 0
        received = client.list_received(after=1, limit=10, channel="chat")
        assert len(received) == 1
        assert isinstance(received[0], ReceivedMessage)
        assert received[0].kind == "push"
        assert client.acknowledge_received("manual", received[0].sequence, channel="chat") == 2
        assert client.get_consumer_cursor("manual", channel="chat") == 2


@pytest.mark.parametrize("invalid", [0, -1, True, float("nan"), float("inf")])
async def test_invalid_poll_interval_fails_before_network(invalid):
    def handle(request):
        pytest.fail("Invalid receive options must not create HTTP requests")

    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        client.on_received(lambda message: None)
        with pytest.raises(ValueError):
            await client.start_receiving(poll_interval=invalid)


async def test_manual_async_receive_read_and_ack():
    api = ReceiveAPI([receipt()])
    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:
        assert await client.get_consumer_cursor("manual") == 0
        assert (await client.list_received())[0].sequence == 1
        assert await client.acknowledge_received("manual", 1) == 1
        assert await client.get_consumer_cursor("manual") == 1
