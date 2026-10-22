"""Exercise the compatibility connector without an OpenAI API or live dots."""

import asyncio
import hashlib
import json
import threading
from contextlib import asynccontextmanager

import httpx
import pytest
from starlette.requests import ClientDisconnect
from starlette.testclient import TestClient, WebSocketDenialResponse

from dots_bridge import AsyncBridgeClient, BridgeError, ReceivedMessage, ReplyTimeoutError
from dots_bridge.connectors import create_openai_connector

KEY = "test-connector-application-key"
AUTH = {"Authorization": "Bearer " + KEY}


def chat_payload(text="こんにちは", **options):
    return {"model": "caller-model", "messages": [{"role": "user", "content": text}], **options}


class UpstreamHTTP:
    """Only the existing enqueue and request-ID reply endpoints are available."""

    def __init__(self):
        self.sent = []
        self.requests = []
        self.messages = {}

    async def handle(self, request):
        assert request.headers["authorization"] == "Bearer " + KEY
        self.requests.append((request.method, request.url.path))
        if request.method == "POST" and request.url.path == "/api/messages":
            payload = json.loads(request.content)
            self.sent.append(payload)
            request_id = f"req_{len(self.sent)}"
            self.messages[request_id] = payload["text"]
            return httpx.Response(202, json={"request_id": request_id})
        if request.method == "GET" and request.url.path.startswith("/api/messages/"):
            request_id = request.url.path.split("/")[3]
            if request.url.path.endswith("/responses"):
                return httpx.Response(
                    200,
                    json={
                        "messages": [
                            {
                                "sequence": 1,
                                "id": "received_" + request_id,
                                "kind": "reply",
                                "request_id": request_id,
                                "channel": "default",
                                "text": "返信: " + self.messages[request_id],
                                "metadata": {},
                                "received_at": "2026-10-22T00:00:00Z",
                            }
                        ]
                    },
                )
            return httpx.Response(
                200,
                json={
                    "request_id": request_id,
                    "delivery_status": "accepted",
                    "reply_status": "replied",
                    "reply": "返信: " + self.messages[request_id],
                },
            )
        raise AssertionError("Connector must not access subscriptions or received consumer cursors")


@asynccontextmanager
async def http_connector(upstream=None, *, handler=None, **options):
    upstream = upstream or UpstreamHTTP()
    async with AsyncBridgeClient(
        "http://bridge.test", api_key=KEY, transport=httpx.MockTransport(handler or upstream.handle)
    ) as bridge:
        app = create_openai_connector(api_key=KEY, client=bridge, **options)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://connector.test",
                headers=AUTH,
            ) as http:
                yield http, bridge, upstream
        assert not bridge.receiving


class FakeBridge:
    """Controlled reply timing for socket ordering and cancellation checks."""

    def __init__(self, *, blocked=(), failures=None):
        self.sent = []
        self.waiting = []
        self.messages = {}
        self.blocked = set(blocked)
        self.failures = failures or {}
        self.releases = {}
        self.started = threading.Event()
        self.cancelled = threading.Event()
        self.closed = False

    async def send_message(self, text, **options):
        self.sent.append((text, options))
        request_id = f"req_{len(self.sent)}"
        self.messages[request_id] = text
        return {"request_id": request_id}

    async def wait_for_reply(self, request_id, *, timeout):  # noqa: ASYNC109 - Client interface.
        self.waiting.append(request_id)
        text = self.messages[request_id]
        if text in self.blocked:
            release = asyncio.Event()
            self.releases[request_id] = release
            self.started.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                self.cancelled.set()
                raise
        if error := self.failures.get(text):
            if error == "timeout":
                raise ReplyTimeoutError(request_id)
            raise error
        return {"reply": "返信: " + text}

    async def release(self, request_id):
        self.releases[request_id].set()

    async def iter_responses(self, request_id, *, timeout):  # noqa: ASYNC109
        result = await self.wait_for_reply(request_id, timeout=timeout)
        yield ReceivedMessage(
            sequence=1,
            id="received_" + request_id,
            kind="reply",
            request_id=request_id,
            channel="default",
            text=result["reply"],
            metadata={},
            received_at="2026-10-22T00:00:00Z",
        )

    async def aclose(self):
        self.closed = True

    def __getattr__(self, name):
        if "receiv" in name or "consumer" in name or "subscription" in name:
            raise AssertionError("Connector must not consume or acknowledge push receipts")
        raise AttributeError(name)


def receive_response(socket):
    events = []
    while len(events) < 12:
        event = socket.receive_json()
        events.append(event)
        if event["type"] in {"response.completed", "response.failed"}:
            return events
    raise AssertionError("Response did not reach a terminal event")


def assert_completed(events, text, *, stream_id=None):
    assert [event["type"] for event in events] == [
        "response.created",
        "response.in_progress",
        "response.output_item.added",
        "response.content_part.added",
        "response.output_text.delta",
        "response.output_text.done",
        "response.content_part.done",
        "response.output_item.done",
        "response.completed",
    ]
    assert [event["sequence_number"] for event in events] == list(range(9))
    assert all(event.get("stream_id") == stream_id for event in events)
    response = events[-1]["response"]
    assert response["id"] == events[0]["response"]["id"]
    assert response["status"] == "completed"
    assert response["output"][0]["role"] == "assistant"
    assert response["output"][0]["content"][0]["text"] == text
    assert events[4]["delta"] == text
    assert events[5]["text"] == text


async def test_http_json_forwards_only_latest_user_text_and_configured_routing():
    upstream = UpstreamHTTP()
    payload = chat_payload(
        messages=[
            {"role": "system", "content": "private system instructions"},
            {"role": "user", "content": "old user text"},
            {"role": "assistant", "content": "old assistant reply"},
            {
                "role": "user",
                "content": [{"type": "text", "text": "最新"}, {"type": "text", "text": "続き"}],
            },
        ],
        temperature=0.1,
        top_p=0.2,
        max_tokens=1,
        previous_response_id="ignored-response",
    )
    async with http_connector(upstream, channel="work", subscription_id="sub_work") as (http, _, _):
        response = await http.post("/v1/chat/completions", json=payload)
    assert response.status_code == 200
    assert response.headers["x-dots-request-id"] == "req_1"
    result = response.json()
    assert result["object"] == "chat.completion"
    assert result["model"] == "caller-model"
    assert result["choices"][0]["message"] == {"role": "assistant", "content": "返信: 最新\n続き"}
    assert result["choices"][0]["finish_reason"] == "stop"
    assert "usage" not in result  # The bridge does not expose token counts.
    assert upstream.sent == [
        {
            "text": "最新\n続き",
            "channel": "work",
            "subscription_id": "sub_work",
            "metadata": {
                "connector": "openai",
                "protocol": "chat.completions",
                "model": "caller-model",
            },
        }
    ]


async def test_http_sse_contains_complete_unicode_reply_and_done_without_consuming_pushes():
    async with http_connector() as (http, _, upstream):
        response = await http.post(
            "/v1/chat/completions",
            json=chat_payload(
                "複数行\nこんにちは🌏", stream=True, stream_options={"include_usage": True}
            ),
        )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    frames = [line[6:] for line in response.text.splitlines() if line.startswith("data: ")]
    assert frames[-1] == "[DONE]"
    chunks = [json.loads(frame) for frame in frames[:-1]]
    assert chunks[0]["choices"][0]["delta"]["role"] == "assistant"
    assert chunks[1]["choices"][0]["delta"]["content"] == "返信: 複数行\nこんにちは🌏"
    assert chunks[2]["choices"][0]["finish_reason"] == "stop"
    assert chunks[3]["choices"] == []
    assert chunks[3]["usage"] is None
    assert {chunk["id"] for chunk in chunks} == {"chatcmpl-req_1"}
    assert len(upstream.sent) == 1
    assert upstream.requests == [
        ("POST", "/api/messages"),
        ("GET", "/api/messages/req_1/responses"),
    ]


async def test_models_and_bearer_auth_reject_before_any_upstream_call():
    async with http_connector(model="configured-dots") as (http, _, upstream):
        models = await http.get("/v1/models")
        assert models.json()["data"][0]["id"] == "configured-dots"
        assert (
            await http.get("/v1/models", headers={"Authorization": "bearer " + KEY})
        ).status_code == 200
        for headers, status in [
            ({"Authorization": ""}, 401),
            ({"Authorization": "Bearer wrong"}, 401),
            ({"Origin": "https://other.test"}, 403),
        ]:
            response = await http.post("/v1/chat/completions", headers=headers, json=chat_payload())
            assert response.status_code == status
            assert "error" in response.json()
            if status == 401:
                assert response.headers["www-authenticate"] == "Bearer"
        assert (await http.get("/v1/models", headers={"Authorization": ""})).status_code == 401
        assert upstream.requests == []


@pytest.mark.parametrize(
    "payload",
    [
        {"messages": [{"role": "system", "content": "no user"}]},
        chat_payload(""),
        chat_payload("a" * 16001),
        chat_payload(
            messages=[
                {"role": "user", "content": "old valid text"},
                {"role": "user", "content": [{"type": "image_url", "image_url": {"url": "x"}}]},
            ]
        ),
        chat_payload(stream="true"),
    ],
)
async def test_invalid_latest_user_or_stream_never_enqueues(payload):
    async with http_connector() as (http, _, upstream):
        response = await http.post("/v1/chat/completions", json=payload)
        assert response.status_code == 400
        assert response.json()["error"]["type"] == "invalid_request_error"
        assert upstream.sent == []


@pytest.mark.parametrize(
    "raw, status", [("[]", 400), ("{broken", 400), ('{"x":NaN}', 400), ("x" * 310000, 413)]
)
async def test_invalid_or_oversized_json_never_enqueues(raw, status):
    async with http_connector() as (http, _, upstream):
        response = await http.post("/v1/chat/completions", content=raw)
        assert response.status_code == status
        assert upstream.sent == []


@pytest.mark.parametrize("payload", [chat_payload("\ud800"), chat_payload(model="\udfff")])
async def test_invalid_unicode_is_rejected_before_upstream_encoding(payload):
    async with http_connector() as (http, _, upstream):
        response = await http.post("/v1/chat/completions", content=json.dumps(payload))
        assert response.status_code == 400
        assert upstream.sent == []


async def test_http_parallel_requests_correlate_replies_that_finish_in_reverse_order():
    upstream = UpstreamHTTP()
    first_enqueued = asyncio.Event()
    release_first = asyncio.Event()

    async def handle(request):
        if request.method == "POST":
            response = await upstream.handle(request)
            if upstream.messages[response.json()["request_id"]] == "first":
                first_enqueued.set()
            return response
        request_id = request.url.path.rsplit("/", 1)[1]
        if upstream.messages[request_id] == "first":
            await release_first.wait()
        return await upstream.handle(request)

    async with http_connector(upstream, handler=handle) as (http, _, _):
        first = asyncio.create_task(http.post("/v1/chat/completions", json=chat_payload("first")))
        try:
            async with asyncio.timeout(1):
                await first_enqueued.wait()
                second = await http.post("/v1/chat/completions", json=chat_payload("second"))
            assert second.json()["choices"][0]["message"]["content"] == "返信: second"
            assert not first.done()
        finally:
            release_first.set()
        async with asyncio.timeout(1):
            first_result = await first
        assert first_result.json()["choices"][0]["message"]["content"] == "返信: first"
        assert first_result.headers["x-dots-request-id"] != second.headers["x-dots-request-id"]
    assert len(upstream.sent) == 2


async def test_http_idempotency_key_is_stable_and_namespaced_upstream():
    async with http_connector() as (http, _, upstream):
        for _ in range(2):
            response = await http.post(
                "/v1/chat/completions", json=chat_payload(), headers={"Idempotency-Key": "retry-1"}
            )
            assert response.status_code == 200
    keys = [message["idempotency_key"] for message in upstream.sent]
    assert keys == ["openai:" + hashlib.sha256(b"retry-1").hexdigest()] * 2


@pytest.mark.parametrize("stream", [False, True])
async def test_reply_timeout_preserves_request_id_and_never_resends(stream):
    upstream = UpstreamHTTP()

    async def pending(request):
        if request.method == "GET":
            await asyncio.Event().wait()
        return await upstream.handle(request)

    async with http_connector(upstream, handler=pending, reply_timeout=0.01) as (http, _, _):
        response = await http.post("/v1/chat/completions", json=chat_payload(stream=stream))
    if stream:
        assert response.status_code == 200
        frames = [line[6:] for line in response.text.splitlines() if line.startswith("data: ")]
        assert frames[-1] == "[DONE]"
        error = json.loads(frames[-2])["error"]
    else:
        assert response.status_code == 504
        assert response.headers["x-should-retry"] == "false"
        error = response.json()["error"]
    assert error["code"] == "bridge_reply_timeout"
    assert error["request_id"] == "req_1"
    assert response.headers["x-dots-request-id"] == "req_1"
    assert len(upstream.sent) == 1


@pytest.mark.parametrize(
    "failure, expected_status, expected_code",
    [
        (BridgeError("no_subscription", "No active subscription", 409), 409, "no_subscription"),
        (BridgeError("unauthorized", "Bridge authentication failed", 401), 502, "unauthorized"),
        (httpx.ConnectError("private-address-and-token"), 502, "bridge_unavailable"),
    ],
)
async def test_upstream_errors_are_openai_envelopes_without_sensitive_network_details(
    failure, expected_status, expected_code
):
    bridge = FakeBridge(failures={"fail": failure})
    app = create_openai_connector(api_key=KEY, client=bridge)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://connector.test", headers=AUTH
    ) as http:
        response = await http.post("/v1/chat/completions", json=chat_payload("fail"))
    assert response.status_code == expected_status
    assert response.headers["x-should-retry"] == "false"
    assert response.json()["error"]["code"] == expected_code
    assert "private-address-and-token" not in response.text
    assert len(bridge.sent) == 1
    assert not bridge.closed


@pytest.mark.parametrize("stream", [False, True])
async def test_http_disconnect_cancels_pending_reply_without_resending(stream):
    bridge = FakeBridge(blocked={"pending"})
    app = create_openai_connector(api_key=KEY, client=bridge)
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/v1/chat/completions",
        "raw_path": b"/v1/chat/completions",
        "query_string": b"",
        "root_path": "",
        "server": ("connector.test", 80),
        "client": ("127.0.0.1", 12345),
        "headers": [(b"authorization", ("Bearer " + KEY).encode()), (b"host", b"connector.test")],
    }
    body = json.dumps(chat_payload("pending", stream=stream)).encode()
    supplied = False
    responses = []

    async def receive():
        nonlocal supplied
        if not supplied:
            supplied = True
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message):
        responses.append(message)
        if stream and message["type"] == "http.response.body":
            # This opening role chunk arrives while dots still has no reply.
            assert message["body"].startswith(b"data: ")
            assert await asyncio.to_thread(bridge.started.wait, 1)
            raise OSError("The downstream connection was closed")

    async with asyncio.timeout(1.5):
        if stream:
            with pytest.raises(ClientDisconnect):
                await app(scope, receive, send)
        else:
            await app(scope, receive, send)
            assert responses[0]["status"] == 499
        assert await asyncio.to_thread(bridge.cancelled.wait, 0.5)
    assert [text for text, _ in bridge.sent] == ["pending"]


def test_websocket_turns_are_fifo_and_echo_stream_ids_while_ignoring_history():
    bridge = FakeBridge(blocked={"first"})
    app = create_openai_connector(
        api_key=KEY, client=bridge, channel="work", subscription_id="sub_1"
    )
    with TestClient(app) as http:
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            socket.send_json({"type": "response.create", "input": "first", "stream_id": "a"})
            first_start = [socket.receive_json(), socket.receive_json()]
            assert [event["type"] for event in first_start] == [
                "response.created",
                "response.in_progress",
            ]
            assert bridge.started.wait(1)
            socket.send_json(
                {
                    "type": "response.create",
                    "stream_id": "b",
                    "previous_response_id": "ignored-id",
                    "instructions": "ignored system text",
                    "temperature": 0.1,
                    "input": [
                        {"role": "user", "content": "old text"},
                        {"role": "assistant", "content": "old reply"},
                        {"role": "user", "content": [{"type": "input_text", "text": "second"}]},
                    ],
                }
            )
            http.portal.call(bridge.release, "req_1")
            first = first_start + receive_response(socket)
            second = receive_response(socket)
            assert_completed(first, "返信: first", stream_id="a")
            assert_completed(second, "返信: second", stream_id="b")
            assert first[-1]["response"]["id"] != second[-1]["response"]["id"]
            assert second[-1]["response"]["metadata"]["dots_bridge_request_id"] == "req_2"
        assert [text for text, _ in bridge.sent] == ["first", "second"]
        assert all(
            options["channel"] == "work" and options["subscription_id"] == "sub_1"
            for _, options in bridge.sent
        )
        assert "ignored" not in json.dumps(bridge.sent)
    assert not bridge.closed  # Injected clients remain caller-owned.


def test_websocket_connections_do_not_block_each_other():
    bridge = FakeBridge(blocked={"slow"})
    with TestClient(create_openai_connector(api_key=KEY, client=bridge)) as http:
        with http.websocket_connect("/v1/responses", headers=AUTH) as first:
            first.send_json({"type": "response.create", "input": "slow"})
            starts = [first.receive_json(), first.receive_json()]
            assert bridge.started.wait(1)
            with http.websocket_connect("/v1/responses", headers=AUTH) as second:
                second.send_json({"type": "response.create", "input": "fast"})
                assert_completed(receive_response(second), "返信: fast")
            http.portal.call(bridge.release, "req_1")
            assert_completed(starts + receive_response(first), "返信: slow")
    assert len(bridge.sent) == 2


@pytest.mark.parametrize(
    "invalid",
    ["not-json", "[]", '{"type":"response.cancel"}', '{"type":"response.create","input":""}'],
)
def test_websocket_invalid_event_reports_error_and_connection_remains_usable(invalid):
    bridge = FakeBridge()
    with TestClient(create_openai_connector(api_key=KEY, client=bridge)) as http:
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            socket.send_text(invalid)
            assert socket.receive_json()["type"] == "error"
            socket.send_json({"type": "response.create", "input": "valid"})
            assert_completed(receive_response(socket), "返信: valid")
    assert [text for text, _ in bridge.sent] == ["valid"]


def test_websocket_validation_error_echoes_valid_stream_id_and_accepts_256_char_lane():
    bridge = FakeBridge()
    stream_id = "lane_" + "a" * 251
    with TestClient(create_openai_connector(api_key=KEY, client=bridge)) as http:
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            socket.send_json({"type": "response.create", "input": "", "stream_id": stream_id})
            error = socket.receive_json()
            assert error["type"] == "error"
            assert error["status"] == 400
            assert error["stream_id"] == stream_id
            socket.send_json({"type": "response.create", "input": "valid", "stream_id": stream_id})
            assert_completed(receive_response(socket), "返信: valid", stream_id=stream_id)
    assert [text for text, _ in bridge.sent] == ["valid"]


def test_websocket_failed_response_does_not_stop_subsequent_turn():
    bridge = FakeBridge(failures={"timeout": "timeout"})
    with TestClient(create_openai_connector(api_key=KEY, client=bridge)) as http:
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            socket.send_json({"type": "response.create", "input": "timeout", "stream_id": "failed"})
            events = receive_response(socket)
            assert [event["type"] for event in events] == [
                "response.created",
                "response.in_progress",
                "response.failed",
            ]
            assert all(event["stream_id"] == "failed" for event in events)
            failed = events[-1]["response"]
            assert failed["status"] == "failed"
            assert failed["metadata"]["dots_bridge_request_id"] == "req_1"
            socket.send_json({"type": "response.create", "input": "next"})
            assert_completed(receive_response(socket), "返信: next")
    assert len(bridge.sent) == 2


def test_websocket_disconnect_stops_waiting_for_already_enqueued_requests():
    bridge = FakeBridge(blocked={"pending"})
    with TestClient(create_openai_connector(api_key=KEY, client=bridge)) as http:
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            socket.send_json({"type": "response.create", "input": "pending"})
            socket.receive_json()
            socket.receive_json()
            assert bridge.started.wait(1)
            socket.send_json({"type": "response.create", "input": "queued"})
        assert bridge.cancelled.wait(1)
        assert [text for text, _ in bridge.sent] == ["pending", "queued"]


@pytest.mark.parametrize(
    "headers, status",
    [
        ({}, 401),
        ({"Authorization": "Bearer wrong"}, 401),
        (AUTH | {"Origin": "https://other.test"}, 403),
    ],
)
def test_websocket_invalid_auth_or_origin_is_denied_before_enqueue(headers, status):
    bridge = FakeBridge()
    with TestClient(create_openai_connector(api_key=KEY, client=bridge)) as http:
        with pytest.raises(WebSocketDenialResponse) as denied:
            with http.websocket_connect("/v1/responses", headers=headers):
                pytest.fail("Invalid websocket should not be accepted")
        assert denied.value.status_code == status
    assert bridge.sent == []


def test_websocket_queue_is_bounded_without_enqueuing_rejected_messages_upstream():
    bridge = FakeBridge(blocked={"pending"})
    with TestClient(create_openai_connector(api_key=KEY, client=bridge)) as http:
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            socket.send_json({"type": "response.create", "input": "pending"})
            socket.receive_json()
            socket.receive_json()
            assert bridge.started.wait(1)
            for index in range(18):
                socket.send_json({"type": "response.create", "input": f"queued-{index}"})
            errors = [socket.receive_json(), socket.receive_json()]
            assert all(event["type"] == "error" for event in errors)
            assert all(event["error"]["code"] == "rate_limit_exceeded" for event in errors)
        assert bridge.cancelled.wait(1)
    assert [text for text, _ in bridge.sent] == [
        "pending",
        *(f"queued-{index}" for index in range(16)),
    ]
