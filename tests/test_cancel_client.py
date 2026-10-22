"""Explicit request cancellation terminates waits and durable response streams."""

import json

import httpx
import pytest

from dots_bridge import AsyncBridgeClient, BridgeClient, BridgeError, RequestCancelledError

KEY = "cancel-client-application-key"


def cancellation_status(**changes):
    return {
        "request_id": "req_old",
        "reply_status": "cancelled",
        "reply": None,
        "delivery_status": "accepted",
        "superseded_by": "req_new",
        "cancelled_at": "2026-10-22T00:00:00Z",
        **changes,
    }


def receipt(sequence, *, kind="progress"):
    return {
        "sequence": sequence,
        "id": f"message_{sequence}",
        "kind": kind,
        "request_id": "req_old",
        "channel": "work",
        "text": "" if kind == "cancel" else "途中経過",
        "metadata": {},
        "received_at": "2026-10-22T00:00:00Z",
        **({"superseded_by": "req_new"} if kind == "cancel" else {}),
    }


@pytest.mark.parametrize("sync", [False, True])
async def test_wait_for_reply_raises_identifiable_cancellation_without_polling_again(sync):
    calls = []

    def handle(request):
        calls.append(request.url.path)
        return httpx.Response(200, json=cancellation_status())

    transport = httpx.MockTransport(handle)
    with pytest.raises(RequestCancelledError) as failure:
        if sync:
            with BridgeClient(api_key=KEY, transport=transport) as client:
                client.wait_for_reply("req_old", timeout=30)
        else:
            async with AsyncBridgeClient(api_key=KEY, transport=transport) as client:
                await client.wait_for_reply("req_old", timeout=30)
    error = failure.value
    assert isinstance(error, BridgeError)
    assert error.request_id == "req_old"
    assert error.superseded_by == "req_new"
    assert error.code == "request_cancelled"
    assert error.status_code == 409
    assert calls == ["/api/messages/req_old"]


@pytest.mark.parametrize("sync", [False, True])
async def test_ask_propagates_request_cancellation_without_sending_again(sync):
    calls = []

    def handle(request):
        calls.append((request.method, request.url.path))
        if request.method == "POST":
            assert json.loads(request.content)["text"] == "question"
            return httpx.Response(202, json={"request_id": "req_old"})
        return httpx.Response(200, json=cancellation_status())

    transport = httpx.MockTransport(handle)
    with pytest.raises(RequestCancelledError):
        if sync:
            with BridgeClient(api_key=KEY, transport=transport) as client:
                client.ask("question", timeout=30)
        else:
            async with AsyncBridgeClient(api_key=KEY, transport=transport) as client:
                await client.ask("question", timeout=30)
    assert calls == [("POST", "/api/messages"), ("GET", "/api/messages/req_old")]


@pytest.mark.parametrize("sync", [False, True])
async def test_subscription_revocation_retains_legacy_reply_cancelled_error(sync):
    status = {"request_id": "req_old", "reply_status": "cancelled", "delivery_status": "cancelled"}
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=status))
    with pytest.raises(BridgeError) as failure:
        if sync:
            with BridgeClient(api_key=KEY, transport=transport) as client:
                client.wait_for_reply("req_old")
        else:
            async with AsyncBridgeClient(api_key=KEY, transport=transport) as client:
                await client.wait_for_reply("req_old")
    assert not isinstance(failure.value, RequestCancelledError)
    assert failure.value.code == "reply_cancelled"


@pytest.mark.parametrize("sync", [False, True])
async def test_request_response_list_accepts_cancel_receipt_and_superseding_id(sync):
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json={"messages": [receipt(2, kind="cancel")]})
    )
    if sync:
        with BridgeClient(api_key=KEY, transport=transport) as client:
            messages = client.list_response_messages("req_old")
    else:
        async with AsyncBridgeClient(api_key=KEY, transport=transport) as client:
            messages = await client.list_response_messages("req_old")
    assert len(messages) == 1
    assert messages[0].kind == "cancel"
    assert messages[0].text == ""
    assert messages[0].superseded_by == "req_new"


async def test_iterator_yields_progress_then_cancel_once_and_never_reads_final_status():
    calls = []

    def handle(request):
        calls.append(request.url.path)
        return httpx.Response(200, json={"messages": [receipt(1), receipt(2, kind="cancel")]})

    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        messages = [message async for message in client.iter_responses("req_old", timeout=30)]
    assert [message.kind for message in messages] == ["progress", "cancel"]
    assert calls == ["/api/messages/req_old/responses"]


@pytest.mark.parametrize("racing_progress", [0, 150])
async def test_cancel_commit_between_page_and_status_is_refetched_with_all_progress(
    racing_progress,
):
    messages = [receipt(1)]
    paths = []
    cancelled = False

    def handle(request):
        nonlocal cancelled
        paths.append(request.url.path)
        if request.url.path.endswith("/responses"):
            after = int(request.url.params["after"])
            return httpx.Response(
                200,
                json={
                    "messages": [message for message in messages if message["sequence"] > after][
                        :100
                    ]
                },
            )
        assert not cancelled
        cancelled = True
        messages.extend(receipt(sequence) for sequence in range(2, racing_progress + 2))
        messages.append(receipt(racing_progress + 2, kind="cancel"))
        return httpx.Response(200, json=cancellation_status())

    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        result = [message async for message in client.iter_responses("req_old", timeout=1)]
    assert [message.sequence for message in result] == list(range(1, racing_progress + 3))
    assert result[-1].kind == "cancel"
    assert sum(path == "/api/messages/req_old" for path in paths) == 1


async def test_cancel_status_without_receipt_is_a_bounded_protocol_failure():
    paths = []

    def handle(request):
        paths.append(request.url.path)
        body = (
            {"messages": [receipt(1)]}
            if request.url.path.endswith("/responses")
            else cancellation_status()
        )
        return httpx.Response(200, json=body)

    received = []
    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(BridgeError) as failure:
            async for message in client.iter_responses("req_old", timeout=1):
                received.append(message)
    assert failure.value.code == "invalid_response"
    assert [message.kind for message in received] == ["progress"]
    assert paths == [
        "/api/messages/req_old/responses",
        "/api/messages/req_old",
        "/api/messages/req_old/responses",
    ]
