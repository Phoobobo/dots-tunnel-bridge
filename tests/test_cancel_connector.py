"""Cancellation is a normal connector completion, including interrupting WS turns."""

import asyncio
import json
import threading
from contextlib import asynccontextmanager, contextmanager
from urllib.parse import unquote

import httpx
import pytest
from starlette.testclient import TestClient

from dots_bridge import AsyncBridgeClient, ReceivedMessage
from dots_bridge.connectors import create_openai_connector
from dots_bridge.connectors._openai_protocol import ResponseEventStream

KEY = "cancel-connector-application-key"
AUTH = {"Authorization": "Bearer " + KEY}


def receipt(request_id, sequence, kind, *, superseded_by=None):
    return {
        "id": f"message_{sequence}",
        "sequence": sequence,
        "kind": kind,
        "request_id": request_id,
        "text": "" if kind == "cancel" else f"{kind} text",
        "channel": "work",
        "metadata": {},
        "received_at": "2026-10-22T00:00:00Z",
        **({"superseded_by": superseded_by} if kind == "cancel" else {}),
    }


class Upstream:
    def __init__(self, *, interrupt=False):
        self.interrupt = interrupt
        self.sent = []
        self.receipts = []
        self.calls = []
        self.consumers = {}
        self.acks = []
        self.second_enqueued = threading.Event()

    async def handle(self, request):
        assert request.headers["authorization"] == "Bearer " + KEY
        path = unquote(request.url.path)
        self.calls.append((request.method, path))
        if path == "/api/messages" and request.method == "POST":
            self.sent.append(json.loads(request.content))
            request_id = f"req_{len(self.sent)}"
            if request_id == "req_1":
                self.receipts.append(receipt(request_id, 1, "progress"))
                if not self.interrupt:
                    self.receipts.append(receipt(request_id, 2, "cancel", superseded_by="req_new"))
            else:
                self.receipts.append(receipt("req_1", 2, "cancel", superseded_by=request_id))
                self.receipts.append(receipt(request_id, 3, "reply"))
                self.second_enqueued.set()
            return httpx.Response(202, json={"request_id": request_id})
        if path.startswith("/api/messages/") and request.method == "GET":
            request_id = path.split("/")[3]
            messages = [message for message in self.receipts if message["request_id"] == request_id]
            if path.endswith("/responses"):
                after = int(request.url.params["after"])
                return httpx.Response(
                    200, json={"messages": [item for item in messages if item["sequence"] > after]}
                )
            terminal = next(
                (item for item in messages if item["kind"] in {"reply", "cancel"}), None
            )
            return httpx.Response(
                200,
                json={
                    "request_id": request_id,
                    "reply": terminal["text"] if terminal and terminal["kind"] == "reply" else None,
                    "reply_status": "cancelled"
                    if terminal and terminal["kind"] == "cancel"
                    else "replied"
                    if terminal
                    else "pending",
                    "delivery_status": "accepted",
                    **(
                        {
                            "superseded_by": terminal["superseded_by"],
                            "cancelled_at": terminal["received_at"],
                        }
                        if terminal and terminal["kind"] == "cancel"
                        else {}
                    ),
                },
            )
        if path == "/api/received":
            after = int(request.url.params["after"])
            return httpx.Response(
                200, json={"messages": [item for item in self.receipts if item["sequence"] > after]}
            )
        if path.startswith("/api/consumers/"):
            consumer_id = path.removeprefix("/api/consumers/").removesuffix("/ack")
            if request.method == "GET":
                return httpx.Response(
                    200, json={"cursor": self.consumers.setdefault(consumer_id, 0)}
                )
            sequence = json.loads(request.content)["sequence"]
            self.consumers[consumer_id] = sequence
            self.acks.append(sequence)
            return httpx.Response(200, json={"cursor": sequence})
        raise AssertionError((request.method, path))


@asynccontextmanager
async def http_connector():
    upstream = Upstream()
    async with AsyncBridgeClient(
        "http://bridge.test", api_key=KEY, transport=httpx.MockTransport(upstream.handle)
    ) as bridge:
        app = create_openai_connector(api_key=KEY, client=bridge, channel="work")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://connector.test", headers=AUTH
        ) as client:
            yield client, upstream


@contextmanager
def websocket_connector(upstream):
    bridge = AsyncBridgeClient(
        "http://bridge.test", api_key=KEY, transport=httpx.MockTransport(upstream.handle)
    )
    with TestClient(create_openai_connector(api_key=KEY, client=bridge, channel="work")) as client:
        try:
            yield client
        finally:
            client.portal.call(bridge.aclose)


def receive(socket):
    async def bounded():
        async with asyncio.timeout(2):
            return await socket._send_rx.receive()

    event = socket.portal.call(bounded)
    assert event["type"] == "websocket.send", event
    return json.loads(event["text"])


def terminal(socket):
    events = []
    for _ in range(12):
        event = receive(socket)
        events.append(event)
        if event["type"] in {"response.completed", "response.failed"}:
            return events
    raise AssertionError("Missing terminal response")


async def test_rest_cancel_is_empty_success_with_explicit_metadata_instead_of_api_error():
    async with http_connector() as (client, upstream):
        response = await client.post(
            "/v1/chat/completions", json={"messages": [{"role": "user", "content": "question"}]}
        )
    assert response.status_code == 200
    body = response.json()
    assert body["choices"][0]["message"] == {"role": "assistant", "content": ""}
    assert body["choices"][0]["finish_reason"] == "stop"
    assert body["dots_bridge"] == {
        "kind": "cancel",
        "request_id": "req_1",
        "superseded_by": "req_new",
    }
    assert response.headers["x-dots-request-id"] == "req_1"
    assert "error" not in body
    assert len(upstream.sent) == 1


async def test_sse_cancel_has_empty_delta_and_normal_finish_without_error_or_extra_text():
    async with http_connector() as (client, upstream):
        response = await client.post(
            "/v1/chat/completions",
            json={"messages": [{"role": "user", "content": "question"}], "stream": True},
        )
    data = [line[6:] for line in response.text.splitlines() if line.startswith("data: ")]
    assert data[-1] == "[DONE]"
    chunks = [json.loads(frame) for frame in data[:-1]]
    assert [chunk.get("dots_bridge", {}).get("kind") for chunk in chunks] == [
        None,
        "progress",
        "cancel",
        None,
    ]
    cancel = chunks[2]
    assert cancel["choices"][0]["delta"]["content"] == ""
    assert cancel["dots_bridge"] == {
        "kind": "cancel",
        "request_id": "req_1",
        "message_id": "message_2",
        "sequence": 2,
        "superseded_by": "req_new",
    }
    assert chunks[-1]["choices"][0]["finish_reason"] == "stop"
    assert all("error" not in chunk for chunk in chunks)
    assert "cancel text" not in response.text
    assert len(upstream.sent) == 1


def test_second_ws_request_is_enqueued_before_first_cancels_with_fifo_output():
    upstream = Upstream(interrupt=True)
    with websocket_connector(upstream) as client:
        with client.websocket_connect("/v1/responses", headers=AUTH) as socket:
            socket.send_json({"type": "response.create", "input": "A", "stream_id": "a"})
            early = [receive(socket) for _ in range(8)]
            assert early[-1]["type"] == "response.output_item.done"
            assert early[-1]["item"]["phase"] == "commentary"
            socket.send_json({"type": "response.create", "input": "B", "stream_id": "b"})
            assert upstream.second_enqueued.wait(1), (
                "B cannot cancel A unless B is enqueued immediately"
            )
            cancelled = receive(socket)
            assert cancelled["type"] == "response.completed"
            assert cancelled["stream_id"] == "a"
            assert cancelled["sequence_number"] == 8
            assert cancelled["response"]["status"] == "cancelled"
            assert cancelled["response"]["output"] == []
            assert cancelled["response"]["metadata"] == {
                "dots_bridge_kind": "cancel",
                "dots_bridge_request_id": "req_1",
                "dots_bridge_superseded_by": "req_2",
            }
            following = terminal(socket)
            assert len(following) == 9
            assert following[0]["type"] == "response.created"
            assert following[0]["sequence_number"] == 0
            assert all(event["stream_id"] == "b" for event in following)
            assert following[-1]["response"]["status"] == "completed"
            assert following[-1]["response"]["output"][0]["content"][0]["text"] == "reply text"
            assert not any(event["type"] == "error" for event in early + [cancelled] + following)
        assert [message["text"] for message in upstream.sent] == ["A", "B"]


def test_cancel_receipts_are_skipped_and_acked_by_the_dedicated_push_consumer():
    upstream = Upstream()
    upstream.receipts = [
        receipt("req_old", 1, "progress"),
        receipt("req_old", 2, "cancel", superseded_by="req_new"),
        receipt(None, 3, "push"),
    ]
    with websocket_connector(upstream) as client:
        with client.websocket_connect("/v1/responses", headers=AUTH) as socket:
            socket.send_json({"type": "dots.subscribe", "consumer_id": "desktop"})
            assert receive(socket)["type"] == "dots.subscribed"
            push = receive(socket)
            assert push["type"] == "dots.push"
            assert push["message"]["kind"] == "push"
            assert upstream.acks == [1, 2]
            socket.send_json({"type": "dots.push.ack", "message_id": "message_3"})
            assert receive(socket)["type"] == "dots.push.acked"
            assert upstream.acks == [1, 2, 3]


def test_cancel_response_clears_progress_state_and_validates_against_local_sdk_schema():
    event_stream = ResponseEventStream(
        response_id="resp_cancel", created=123, model="dots", stream_id="a"
    )
    event_stream.start()
    event_stream.message("Old progress", final=False)
    events = event_stream.cancel(
        metadata={
            "dots_bridge_kind": "cancel",
            "dots_bridge_request_id": "req_old",
            "dots_bridge_superseded_by": "req_new",
        }
    )
    assert len(events) == 1
    event = events[0]
    assert event["sequence_number"] == 8
    assert event["response"]["status"] == "cancelled"
    assert event["response"]["output"] == []
    assert event["response"]["error"] is None
    with pytest.raises(ValueError, match="already finished"):
        event_stream.message("Late answer", final=True)
    sdk = pytest.importorskip("openai.types.responses.response_completed_event")
    assert sdk.ResponseCompletedEvent.model_validate(event).response.status == "cancelled"


def test_websocket_queue_wait_uses_the_remaining_budget_from_request_acceptance():
    class BudgetBridge:
        def __init__(self):
            self.sent = []
            self.timeouts = {}
            self.release = None
            self.second_enqueued = threading.Event()

        async def send_message(self, text, **options):
            self.sent.append(text)
            if len(self.sent) == 2:
                self.second_enqueued.set()
            return {"request_id": f"req_{len(self.sent)}"}

        async def iter_responses(self, request_id, *, timeout):  # noqa: ASYNC109
            self.timeouts[request_id] = timeout
            if request_id == "req_1":
                self.release = asyncio.Event()
                await self.release.wait()
            yield ReceivedMessage.model_validate(receipt(request_id, 1, "reply"))

        async def release_first(self):
            await asyncio.sleep(0.04)
            self.release.set()

    bridge = BudgetBridge()
    with TestClient(create_openai_connector(api_key=KEY, client=bridge, reply_timeout=1)) as client:
        with client.websocket_connect("/v1/responses", headers=AUTH) as socket:
            socket.send_json({"type": "response.create", "input": "A"})
            receive(socket)
            receive(socket)
            socket.send_json({"type": "response.create", "input": "B"})
            assert bridge.second_enqueued.wait(1)
            client.portal.call(bridge.release_first)
            assert terminal(socket)[-1]["response"]["status"] == "completed"
            assert terminal(socket)[-1]["response"]["status"] == "completed"
    assert 0 < bridge.timeouts["req_2"] < 0.98
