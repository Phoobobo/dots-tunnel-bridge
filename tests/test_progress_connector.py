"""Request progress reaches OpenAI-compatible streams before the final reply."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager, contextmanager
from urllib.parse import unquote

import httpx
import pytest
from starlette.testclient import TestClient

from dots_bridge import AsyncBridgeClient
from dots_bridge.connectors import create_openai_connector

KEY = "test-progress-connector-application-key"
AUTH = {"Authorization": "Bearer " + KEY}
PROGRESS = ["資料を検索しています。🌏", "候補が見つかりました。"]
FINAL = "これが最終的な回答です。"
MESSAGE_EVENT_TYPES = [
    "response.output_item.added",
    "response.content_part.added",
    "response.output_text.delta",
    "response.output_text.done",
    "response.content_part.done",
    "response.output_item.done",
]


def record(request_id, sequence, text, *, kind="progress"):
    return {
        "id": f"received_{sequence}",
        "sequence": sequence,
        "kind": kind,
        "request_id": request_id,
        "channel": "work",
        "text": text,
        "metadata": {"source": "test"},
        "received_at": "2026-10-22T00:00:00Z",
    }


class ProgressHTTP:
    """Serve real AsyncBridgeClient requests with controlled final availability."""

    def __init__(self, *, final_available=True, contaminate=False, push=False):
        self.final_available = final_available
        self.contaminate = contaminate
        self.push = push
        self.sent = []
        self.calls = []
        self.consumers = {}
        self.acknowledgements = []

    def responses(self, request_id):
        ordinal = int(request_id.removeprefix("req_"))
        base = ordinal * 100
        messages = [
            record(request_id, base + index, text) for index, text in enumerate(PROGRESS, 1)
        ]
        if self.final_available and self.sent[ordinal - 1]["text"] != "fail":
            messages.append(record(request_id, base + 99, FINAL, kind="reply"))
        return messages

    async def handle(self, request):
        assert request.headers["authorization"] == "Bearer " + KEY
        path = unquote(request.url.path)
        self.calls.append((request.method, path, dict(request.url.params)))
        if request.method == "POST" and path == "/api/messages":
            self.sent.append(json.loads(request.content))
            return httpx.Response(202, json={"request_id": f"req_{len(self.sent)}"})
        if request.method == "GET" and path.startswith("/api/messages/"):
            request_id = path.split("/")[3]
            if path.endswith("/responses"):
                after = int(request.url.params.get("after", 0))
                messages = [item for item in self.responses(request_id) if item["sequence"] > after]
                if self.contaminate:
                    messages.append(record("another-request", 9999, "Do not leak another request"))
                return httpx.Response(200, json={"messages": messages})
            failed = self.sent[int(request_id.removeprefix("req_")) - 1]["text"] == "fail"
            status = "expired" if failed else "replied" if self.final_available else "pending"
            return httpx.Response(
                200,
                json={
                    "request_id": request_id,
                    "reply_status": status,
                    "reply": FINAL if status == "replied" else None,
                    "delivery_status": "accepted",
                },
            )
        if path == "/api/received" and request.method == "GET":
            after = int(request.url.params.get("after", 0))
            messages = [
                item
                for ordinal in range(1, len(self.sent) + 1)
                for item in self.responses(f"req_{ordinal}")
            ]
            if self.push:
                messages.append(record(None, 150, "自発的な通知", kind="push"))
            return httpx.Response(
                200,
                json={
                    "messages": sorted(
                        (item for item in messages if item["sequence"] > after),
                        key=lambda item: item["sequence"],
                    )
                },
            )
        if path.startswith("/api/consumers/"):
            consumer_id = path.removeprefix("/api/consumers/").removesuffix("/ack")
            if request.method == "GET":
                return httpx.Response(
                    200, json={"cursor": self.consumers.setdefault(consumer_id, 0)}
                )
            payload = json.loads(request.content)
            self.consumers[consumer_id] = payload["sequence"]
            self.acknowledgements.append(payload["sequence"])
            return httpx.Response(200, json={"cursor": payload["sequence"]})
        raise AssertionError(f"Unexpected endpoint: {request.method} {path}")


@asynccontextmanager
async def http_connector(upstream=None):
    upstream = upstream or ProgressHTTP()
    async with AsyncBridgeClient(
        "http://bridge.test", api_key=KEY, transport=httpx.MockTransport(upstream.handle)
    ) as bridge:
        app = create_openai_connector(api_key=KEY, client=bridge, channel="work", reply_timeout=2)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://connector.test",
                headers=AUTH,
            ) as http:
                yield http, upstream, app


@contextmanager
def websocket_connector(upstream=None):
    upstream = upstream or ProgressHTTP()
    bridge = AsyncBridgeClient(
        "http://bridge.test", api_key=KEY, transport=httpx.MockTransport(upstream.handle)
    )
    app = create_openai_connector(api_key=KEY, client=bridge, channel="work", reply_timeout=3)
    with TestClient(app) as http:
        try:
            yield http, upstream
        finally:
            http.portal.call(bridge.aclose)


def chat_payload(text="question", *, stream=False):
    return {"model": "dots", "messages": [{"role": "user", "content": text}], "stream": stream}


def frames(text):
    return [line.removeprefix("data: ") for line in text.splitlines() if line.startswith("data: ")]


def receive_event(socket):
    # Bound the test transport so a missing partial update cannot hang pytest.
    async def receive():
        async with asyncio.timeout(3):
            return await socket._send_rx.receive()

    message = socket.portal.call(receive)
    assert message["type"] == "websocket.send", message
    return json.loads(message["text"])


def receive_until_terminal(socket):
    result = []
    for _ in range(30):
        event = receive_event(socket)
        result.append(event)
        if event["type"] in {"response.completed", "response.failed"}:
            return result
    raise AssertionError("No terminal response event")


def assert_message_group(events, *, text, index, phase):
    assert [event["type"] for event in events] == MESSAGE_EVENT_TYPES
    assert all(event["output_index"] == index for event in events)
    added, part_added, delta, text_done, part_done, item_done = events
    assert added["item"]["phase"] == item_done["item"]["phase"] == phase
    assert added["item"]["status"] == "in_progress"
    assert added["item"]["content"] == []
    assert item_done["item"]["status"] == "completed"
    item_id = item_done["item"]["id"]
    assert added["item"]["id"] == item_id
    assert all(event["item_id"] == item_id for event in events[1:-1])
    assert part_added["part"]["text"] == ""
    assert delta["delta"] == text_done["text"] == part_done["part"]["text"] == text
    assert item_done["item"]["content"][0]["text"] == text
    return item_done["item"]


async def test_non_streaming_rest_returns_only_final_and_does_not_poll_progress_endpoint():
    async with http_connector() as (http, upstream, _):
        response = await http.post("/v1/chat/completions", json=chat_payload())
        assert response.status_code == 200
        assert response.json()["choices"][0]["message"]["content"] == FINAL
        assert all(not path.endswith("/responses") for _, path, _ in upstream.calls)
        assert "資料を検索" not in response.text
        assert len(upstream.sent) == 1


async def test_sse_streams_ordered_progress_and_final_with_metadata_and_finishes_only_once():
    async with http_connector() as (http, upstream, _):
        response = await http.post("/v1/chat/completions", json=chat_payload(stream=True))
    data = frames(response.text)
    assert data[-1] == "[DONE]"
    assert data.count("[DONE]") == 1
    chunks = [json.loads(frame) for frame in data[:-1]]
    assert chunks[0]["choices"][0]["delta"] == {"role": "assistant", "content": ""}
    content = chunks[1:-1]
    assert [chunk["choices"][0]["delta"]["content"] for chunk in content] == [
        *(text + "\n\n" for text in PROGRESS),
        FINAL,
    ]
    assert [chunk["dots_bridge"]["kind"] for chunk in content] == ["progress", "progress", "reply"]
    assert [chunk["dots_bridge"] for chunk in content] == [
        {
            "kind": kind,
            "request_id": "req_1",
            "message_id": f"received_{sequence}",
            "sequence": sequence,
        }
        for kind, sequence in [("progress", 101), ("progress", 102), ("reply", 199)]
    ]
    assert all(chunk["choices"][0]["finish_reason"] is None for chunk in chunks[:-1])
    assert chunks[-1]["choices"][0]["finish_reason"] == "stop"
    assert {chunk["id"] for chunk in chunks} == {"chatcmpl-req_1"}
    assert len(upstream.sent) == 1
    assert not upstream.consumers


async def test_sse_progress_is_sent_while_final_reply_is_still_unavailable():
    upstream = ProgressHTTP(final_available=False)
    async with http_connector(upstream) as (_, _, app):
        body = json.dumps(chat_payload(stream=True)).encode()
        supplied = False
        sent = []
        progress_seen = asyncio.Event()

        async def receive():
            nonlocal supplied
            if not supplied:
                supplied = True
                return {"type": "http.request", "body": body, "more_body": False}
            await asyncio.Event().wait()

        async def send(message):
            sent.append(message)
            if b'"kind":"progress"' in message.get("body", b""):
                progress_seen.set()

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
            "headers": [
                (b"authorization", ("Bearer " + KEY).encode()),
                (b"host", b"connector.test"),
            ],
        }
        task = asyncio.create_task(app(scope, receive, send))
        try:
            async with asyncio.timeout(1):
                await progress_seen.wait()
            assert not task.done()
            assert not upstream.final_available
            assert b"[DONE]" not in b"".join(message.get("body", b"") for message in sent)
            upstream.final_available = True
            async with asyncio.timeout(2):
                await task
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        result = b"".join(message.get("body", b"") for message in sent).decode()
        assert frames(result)[-1] == "[DONE]"
        assert FINAL in result


async def test_sse_failure_after_progress_keeps_updates_without_a_successful_finish():
    async with http_connector() as (http, upstream, _):
        response = await http.post("/v1/chat/completions", json=chat_payload("fail", stream=True))
    data = frames(response.text)
    assert data[-1] == "[DONE]"
    events = [json.loads(frame) for frame in data[:-1]]
    assert [event["dots_bridge"]["kind"] for event in events if "dots_bridge" in event] == [
        "progress",
        "progress",
    ]
    assert events[-1]["error"]["code"] == "reply_expired"
    assert events[-1]["error"]["request_id"] == "req_1"
    assert all(
        event["choices"][0]["finish_reason"] is None for event in events if "choices" in event
    )
    assert len(upstream.sent) == 1


def test_websocket_delivers_progress_before_final_with_one_continuous_response_lifecycle():
    with websocket_connector(ProgressHTTP(final_available=False)) as (http, upstream):
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            socket.send_json({"type": "response.create", "input": "question", "stream_id": "lane"})
            early = [receive_event(socket) for _ in range(14)]
            assert [event["type"] for event in early[:2]] == [
                "response.created",
                "response.in_progress",
            ]
            items = [
                assert_message_group(
                    early[2 + index * 6 : 8 + index * 6], text=text, index=index, phase="commentary"
                )
                for index, text in enumerate(PROGRESS)
            ]
            assert not upstream.final_available
            assert all(event["type"] != "response.completed" for event in early)
            upstream.final_available = True
            final = receive_until_terminal(socket)
            assert len(final) == 7
            items.append(assert_message_group(final[:6], text=FINAL, index=2, phase="final_answer"))
            events = early + final
            assert [event["sequence_number"] for event in events] == list(range(21))
            assert all(event["stream_id"] == "lane" for event in events)
            assert len({item["id"] for item in items}) == 3
            completed = final[-1]["response"]
            assert completed["id"] == early[0]["response"]["id"] == early[1]["response"]["id"]
            assert completed["status"] == "completed"
            assert completed["output"] == items
            assert completed["metadata"]["dots_bridge_request_id"] == "req_1"
            assert sum(event["type"] == "response.completed" for event in events) == 1
        assert len(upstream.sent) == 1
        assert not upstream.consumers


def test_websocket_failure_retains_completed_commentary_and_allows_next_request():
    with websocket_connector() as (http, upstream):
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            socket.send_json(
                {"type": "response.create", "input": "fail", "stream_id": "failed-lane"}
            )
            events = receive_until_terminal(socket)
            assert len(events) == 15
            assert [event["sequence_number"] for event in events] == list(range(15))
            items = [
                assert_message_group(
                    events[2 + index * 6 : 8 + index * 6],
                    text=text,
                    index=index,
                    phase="commentary",
                )
                for index, text in enumerate(PROGRESS)
            ]
            failed = events[-1]
            assert failed["type"] == "response.failed"
            assert failed["response"]["status"] == "failed"
            assert failed["response"]["output"] == items
            assert failed["response"]["metadata"]["dots_bridge_request_id"] == "req_1"
            assert "expired" in failed["response"]["error"]["message"]
            socket.send_json({"type": "response.create", "input": "recovered"})
            recovered = receive_until_terminal(socket)
            assert recovered[-1]["type"] == "response.completed"
            assert recovered[0]["sequence_number"] == 0
            assert recovered[-1]["response"]["id"] != failed["response"]["id"]
            assert len(upstream.sent) == 2


@pytest.mark.parametrize("websocket", [False, True])
def test_foreign_request_in_response_page_is_rejected_without_leaking_its_text(websocket):
    upstream = ProgressHTTP(contaminate=True)
    with websocket_connector(upstream) as (http, _):
        if websocket:
            with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
                socket.send_json({"type": "response.create", "input": "question"})
                events = receive_until_terminal(socket)
                assert events[-1]["type"] == "response.failed"
                assert events[-1]["response"]["output"] == []
                content = json.dumps(events)
        else:
            response = http.post(
                "/v1/chat/completions", headers=AUTH, json=chat_payload(stream=True)
            )
            events = [json.loads(frame) for frame in frames(response.text) if frame != "[DONE]"]
            assert events[-1]["error"]["code"] == "invalid_response"
            content = response.text
        assert "Do not leak another request" not in content
        assert len(upstream.sent) == 1


def test_progress_and_unsolicited_pushes_use_separate_websocket_event_families():
    with websocket_connector(ProgressHTTP(final_available=False, push=True)) as (http, upstream):
        with http.websocket_connect("/v1/responses", headers=AUTH) as socket:
            socket.send_json({"type": "response.create", "input": "question"})
            starts_and_progress = [receive_event(socket) for _ in range(14)]
            assert all(event["type"].startswith("response.") for event in starts_and_progress)
            socket.send_json({"type": "dots.subscribe", "consumer_id": "personal-agent"})
            assert receive_event(socket)["type"] == "dots.subscribed"
            push = receive_event(socket)
            assert push["type"] == "dots.push"
            assert push["message"]["kind"] == "push"
            assert push["message"]["request_id"] is None
            assert push["message"]["text"] == "自発的な通知"
            assert upstream.acknowledgements == [101, 102]
            socket.send_json({"type": "dots.push.ack", "message_id": push["message"]["id"]})
            assert receive_event(socket)["type"] == "dots.push.acked"
            upstream.final_available = True
            final = receive_until_terminal(socket)
            assert [event["type"] for event in final] == [
                *MESSAGE_EVENT_TYPES,
                "response.completed",
            ]
            assert [item["phase"] for item in final[-1]["response"]["output"]] == [
                "commentary",
                "commentary",
                "final_answer",
            ]
            assert "自発的な通知" not in json.dumps(final, ensure_ascii=False)
