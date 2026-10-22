"""Request-scoped streaming of durable progress receipts and a final reply."""

import asyncio

import httpx
import pytest

from dots_bridge import (
    AsyncBridgeClient,
    BridgeClient,
    BridgeError,
    ReceivedMessage,
    ReplyTimeoutError,
)

KEY = "response-stream-application-key"


def receipt(sequence, *, request_id="req_a", kind="progress"):
    return {
        "sequence": sequence,
        "id": f"received_{sequence}",
        "kind": kind,
        "request_id": request_id,
        "channel": "work",
        "text": f"{kind}: {sequence}",
        "metadata": {"source": "test"},
        "received_at": "2026-10-22T00:00:00Z",
    }


class ResponseAPI:
    def __init__(self, messages=(), *, status="pending", delivery="accepted"):
        self.messages = list(messages)
        self.status = status
        self.delivery = delivery
        self.seen = []

    def handle(self, request):
        assert request.headers["authorization"] == "Bearer " + KEY
        assert request.method == "GET", "Streaming must never enqueue or acknowledge a message"
        self.seen.append((request.url.path, dict(request.url.params)))
        parts = request.url.path.split("/")
        assert parts[1:3] == ["api", "messages"], "A request stream must not access global receipts"
        request_id = parts[3]
        if parts[-1] == "responses":
            after = int(request.url.params["after"])
            limit = int(request.url.params["limit"])
            messages = [
                message
                for message in self.messages
                if message["request_id"] == request_id and message["sequence"] > after
            ]
            return httpx.Response(200, json={"messages": messages[:limit]})
        assert len(parts) == 4
        return httpx.Response(
            200,
            json={
                "request_id": request_id,
                "reply_status": self.status,
                "delivery_status": self.delivery,
                "reply": "final text" if self.status == "replied" else None,
            },
        )


async def collect(client, request_id="req_a", **options):
    return [message async for message in client.iter_responses(request_id, **options)]


def test_sync_response_list_scopes_request_and_supports_pagination():
    api = ResponseAPI(
        [receipt(1), receipt(2, request_id="req_other"), receipt(4), receipt(8, kind="reply")]
    )
    with BridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:
        messages = client.list_response_messages("req_a", after=1, limit=2)
    assert all(isinstance(message, ReceivedMessage) for message in messages)
    assert [(message.sequence, message.kind) for message in messages] == [
        (4, "progress"),
        (8, "reply"),
    ]
    assert api.seen == [("/api/messages/req_a/responses", {"after": "1", "limit": "2"})]


async def test_async_response_list_returns_progress_and_final_in_sequence_order():
    def handle(request):
        assert request.url.path == "/api/messages/req_a/responses"
        return httpx.Response(200, json={"messages": [receipt(4, kind="reply"), receipt(1)]})

    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        messages = await client.list_response_messages("req_a")
    assert [message.sequence for message in messages] == [1, 4]


@pytest.mark.parametrize("sync", [False, True])
@pytest.mark.parametrize(
    "bad_message", [receipt(1, request_id="req_other"), receipt(1, kind="push")]
)
async def test_response_list_rejects_other_requests_and_unrelated_receipt_kinds(sync, bad_message):
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json={"messages": [bad_message]})
    )
    if sync:
        with BridgeClient(api_key=KEY, transport=transport) as client:
            with pytest.raises(BridgeError) as error:
                client.list_response_messages("req_a")
    else:
        async with AsyncBridgeClient(api_key=KEY, transport=transport) as client:
            with pytest.raises(BridgeError) as error:
                await client.list_response_messages("req_a")
    assert error.value.code == "invalid_response"


async def test_fast_final_replays_all_progress_pages_before_stopping():
    progress = [receipt(sequence) for sequence in range(1, 502, 2)]
    unrelated = [receipt(sequence, request_id="req_b") for sequence in range(2, 503, 2)]
    final = receipt(503, kind="reply")
    api = ResponseAPI(
        sorted(progress + unrelated + [final], key=lambda message: message["sequence"]),
        status="replied",
    )
    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:
        messages = await collect(client, timeout=1)
    assert [message.sequence for message in messages] == [
        message["sequence"] for message in progress
    ] + [503]
    assert all(message.request_id == "req_a" for message in messages)
    assert [message.kind for message in messages].count("reply") == 1
    assert api.seen == [
        ("/api/messages/req_a/responses", {"after": "0", "limit": "100"}),
        ("/api/messages/req_a/responses", {"after": "199", "limit": "100"}),
        ("/api/messages/req_a/responses", {"after": "399", "limit": "100"}),
    ]


async def test_overlapping_and_reordered_receipts_are_yielded_once_by_sequence():
    pages = []

    def handle(request):
        if request.url.path.endswith("/responses"):
            after = int(request.url.params["after"])
            pages.append(after)
            messages = (
                [receipt(3), receipt(1), receipt(1)]
                if after == 0
                else [receipt(3), receipt(7, kind="reply"), receipt(5), receipt(1)]
            )
            return httpx.Response(200, json={"messages": messages})
        return httpx.Response(200, json={"reply_status": "pending", "delivery_status": "accepted"})

    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        messages = await collect(client, timeout=1, poll_interval=0.001)
    assert [message.sequence for message in messages] == [1, 3, 5, 7]
    assert pages == [0, 3]


@pytest.mark.parametrize("race_progress_count", [0, 150])
async def test_final_commit_between_page_and_status_is_refetched_with_all_racing_progress(
    race_progress_count,
):
    api = ResponseAPI([receipt(1)])
    raced = False

    def handle(request):
        nonlocal raced
        if not request.url.path.endswith("/responses") and not raced:
            raced = True
            api.messages.extend(receipt(sequence) for sequence in range(2, race_progress_count + 2))
            api.messages.append(receipt(race_progress_count + 2, kind="reply"))
            api.status = "replied"
        return api.handle(request)

    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        messages = await collect(client, timeout=1)
    assert [message.sequence for message in messages] == list(range(1, race_progress_count + 3))
    assert messages[-1].kind == "reply"
    assert sum(path == "/api/messages/req_a" for path, _ in api.seen) == 1


async def test_final_status_without_its_receipt_fails_after_one_confirmation_pass():
    api = ResponseAPI([receipt(1)], status="replied")
    received = []
    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:
        with pytest.raises(BridgeError) as error:
            async for message in client.iter_responses("req_a", timeout=1):
                received.append(message)
    assert error.value.code == "invalid_response"
    assert [message.sequence for message in received] == [1]
    assert [path for path, _ in api.seen] == [
        "/api/messages/req_a/responses",
        "/api/messages/req_a",
        "/api/messages/req_a/responses",
    ]


async def test_replied_status_with_invalid_reply_text_fails_instead_of_polling_forever():
    calls = []

    def handle(request):
        calls.append(request.url.path)
        body = (
            {"messages": []}
            if request.url.path.endswith("/responses")
            else {
                "reply_status": "replied",
                "reply": None,
            }
        )
        return httpx.Response(200, json=body)

    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(BridgeError) as error:
            await collect(client, timeout=1)
    assert error.value.code == "invalid_response"
    assert calls == ["/api/messages/req_a/responses", "/api/messages/req_a"]


@pytest.mark.parametrize(
    "status, delivery, code",
    [
        ("expired", "accepted", "reply_expired"),
        ("cancelled", "accepted", "reply_cancelled"),
        ("pending", "failed", "delivery_failed"),
        ("pending", "cancelled", "delivery_cancelled"),
    ],
)
async def test_terminal_failure_after_progress_is_reported_without_resending(
    status, delivery, code
):
    api = ResponseAPI([receipt(1)], status=status, delivery=delivery)
    received = []
    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:
        with pytest.raises(BridgeError) as error:
            async for message in client.iter_responses("req_a", timeout=1):
                received.append(message)
    assert error.value.code == code
    assert [message.kind for message in received] == ["progress"]


async def test_parallel_request_streams_do_not_block_or_cross_correlate():
    api = ResponseAPI(
        [
            receipt(1),
            receipt(2, request_id="req_b"),
            receipt(3, kind="reply"),
            receipt(4, request_id="req_b", kind="reply"),
        ]
    )
    entered = asyncio.Event()
    release = asyncio.Event()

    async def handle(request):
        if request.url.path.startswith("/api/messages/req_a/"):
            entered.set()
            await release.wait()
        return api.handle(request)

    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        first = asyncio.create_task(collect(client, "req_a", timeout=1))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            second = await collect(client, "req_b", timeout=1)
            assert [(message.request_id, message.sequence) for message in second] == [
                ("req_b", 2),
                ("req_b", 4),
            ]
            assert not first.done()
        finally:
            release.set()
        first_result = await first
    assert [(message.request_id, message.sequence) for message in first_result] == [
        ("req_a", 1),
        ("req_a", 3),
    ]


async def test_timeout_does_not_reset_while_progress_keeps_arriving():
    sequence = 0
    received = []

    async def handle(request):
        nonlocal sequence
        assert request.method == "GET"
        if request.url.path.endswith("/responses"):
            await asyncio.sleep(0.005)
            sequence += 1
            return httpx.Response(200, json={"messages": [receipt(sequence)]})
        return httpx.Response(200, json={"reply_status": "pending", "delivery_status": "accepted"})

    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        async with asyncio.timeout(0.3):
            with pytest.raises(ReplyTimeoutError) as error:
                async for message in client.iter_responses(
                    "req_a", timeout=0.04, poll_interval=0.001
                ):
                    received.append(message)
    assert received
    assert error.value.request_id == "req_a"


@pytest.mark.parametrize("kind", ["progress", "reply"])
async def test_timeout_never_cancels_callers_work_between_generator_yields(kind):
    api = ResponseAPI([receipt(1, kind=kind)])
    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:
        responses = client.iter_responses("req_a", timeout=0.02, poll_interval=0.001)
        try:
            assert (await anext(responses)).kind == kind
            await asyncio.sleep(0.04)  # No timeout context may remain armed across yield.
            if kind == "reply":
                with pytest.raises(StopAsyncIteration):
                    await anext(responses)
            else:
                with pytest.raises(ReplyTimeoutError):
                    await anext(responses)
        finally:
            await responses.aclose()
    assert len(api.seen) == 1


@pytest.mark.parametrize("cancel", [False, True])
async def test_timeout_and_caller_cancellation_interrupt_inflight_response_http_request(cancel):
    entered = asyncio.Event()
    cancelled = asyncio.Event()
    calls = []

    async def handle(request):
        calls.append((request.method, request.url.path))
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        task = asyncio.create_task(collect(client, timeout=60 if cancel else 0.01))
        await asyncio.wait_for(entered.wait(), 1)
        if cancel:
            task.cancel()
        with pytest.raises(asyncio.CancelledError if cancel else ReplyTimeoutError):
            await task
    assert cancelled.is_set()
    assert calls == [("GET", "/api/messages/req_a/responses")]


@pytest.mark.parametrize(
    "options",
    [
        {"timeout": 0},
        {"timeout": True},
        {"timeout": float("inf")},
        {"poll_interval": 0},
        {"poll_interval": float("nan")},
    ],
)
async def test_invalid_stream_settings_fail_before_http(options):
    def unexpected(request):
        pytest.fail("Invalid options must not perform an HTTP request")

    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(unexpected)) as client:
        with pytest.raises(ValueError):
            await collect(client, **options)


@pytest.mark.parametrize("options", [{"after": True}, {"after": -1}, {"limit": 0}, {"limit": 101}])
def test_invalid_response_page_options_fail_before_http(options):
    def unexpected(request):
        pytest.fail("Invalid options must not perform an HTTP request")

    with BridgeClient(api_key=KEY, transport=httpx.MockTransport(unexpected)) as client:
        with pytest.raises(ValueError):
            client.list_response_messages("req_a", **options)


async def test_wait_for_reply_remains_final_only_and_does_not_poll_receipts():
    api = ResponseAPI([receipt(1), receipt(2, kind="reply")], status="replied")
    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(api.handle)) as client:
        final = await client.wait_for_reply("req_a", timeout=1)
    assert final["reply"] == "final text"
    assert api.seen == [("/api/messages/req_a", {})]
