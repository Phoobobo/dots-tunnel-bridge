"""Interactive official-SDK example behavior, with no SDK or network dependency."""

import asyncio
import importlib.util
import json
import re
import sys
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "openai_client.py"
spec = importlib.util.spec_from_file_location("dots_bridge_openai_client_example", EXAMPLE)
assert spec is not None and spec.loader is not None
example = importlib.util.module_from_spec(spec)
spec.loader.exec_module(example)


class ScriptedSession:
    def __init__(self, *responses):
        self.responses = deque(responses)
        self.prompt_count = 0

    async def prompt_async(self, prompt):
        self.prompt_count += 1
        response = self.responses.popleft()
        if isinstance(response, BaseException):
            raise response
        return response


class PendingSession:
    def __init__(self):
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def prompt_async(self, prompt):
        self.started.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.cancelled.set()


def chunk(text=None, *, kind=None, finish=None, choices=True, request_id=None):
    metadata = {"kind": kind} if kind else {}
    if request_id is not None:
        metadata["request_id"] = request_id
    return SimpleNamespace(
        choices=[SimpleNamespace(delta=SimpleNamespace(content=text), finish_reason=finish)]
        if choices
        else [],
        dots_bridge=metadata or None,
    )


class APIError(Exception):
    """The SDK exception attributes used by the example, without importing OpenAI."""

    def __init__(self, message, *, body=None, code=None, status_code=None, request_id=None):
        super().__init__(message)
        self.message = message
        self.body = body
        self.code = code
        self.status_code = status_code
        self.request_id = request_id


class FakeStream:
    def __init__(self, chunks, *, headers=None):
        self.chunks = deque(chunks)
        self.entered = False
        self.closed = False
        self.response = SimpleNamespace(headers=httpx.Headers(headers or {}))

    async def __aenter__(self):
        self.entered = True
        return self

    async def __aexit__(self, *args):
        self.closed = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self.chunks:
            raise StopAsyncIteration
        item = self.chunks.popleft()
        if isinstance(item, Exception):
            raise item
        return item


class FakeCompletions:
    def __init__(self, *, failures=None, blocked=(), stream_overrides=None):
        self.requests = []
        self.streams = []
        self.failures = failures or {}
        self.blocked = set(blocked)
        self.stream_overrides = stream_overrides or {}
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def create(self, **payload):
        self.requests.append(payload)
        text = payload["messages"][-1]["content"]
        if text in self.blocked:
            self.started.set()
            await self.release.wait()
        if error := self.failures.get(text):
            raise error
        if payload.get("stream"):
            if text in self.stream_overrides:
                stream = FakeStream(**self.stream_overrides[text])
                self.streams.append(stream)
                return stream
            chunks = [
                chunk(""),
                chunk("進捗: " + text + "\n\n", kind="progress"),
                chunk("返信: " + text, kind="reply"),
                chunk(finish="stop"),
                chunk(choices=False),
            ]
            if text == "途中で失敗":
                chunks[2:] = [RuntimeError("private-stream-failure-detail")]
            stream = FakeStream(chunks)
            self.streams.append(stream)
            return stream
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="返信: " + text))]
        )


def response_events(text, *, response_id, stream_id=None):
    output = []
    events = [
        {"type": "response.created", "response": {"id": response_id, "status": "in_progress"}}
    ]
    for index, (phase, value) in enumerate(
        [("commentary", "進捗: " + text), ("final_answer", "返信: " + text)]
    ):
        item = {
            "id": f"msg_{response_id}_{index}",
            "type": "message",
            "role": "assistant",
            "phase": phase,
            "status": "completed",
            "content": [{"type": "output_text", "text": value}],
        }
        events.extend(
            [
                {
                    "type": "response.output_item.added",
                    "output_index": index,
                    "item": {**item, "status": "in_progress", "content": []},
                },
                {
                    "type": "response.output_text.delta",
                    "output_index": index,
                    "item_id": item["id"],
                    "content_index": 0,
                    "delta": value,
                },
                {
                    "type": "response.output_text.done",
                    "output_index": index,
                    "item_id": item["id"],
                    "content_index": 0,
                    "text": value,
                },
                {"type": "response.output_item.done", "output_index": index, "item": item},
            ]
        )
        output.append(item)
    events.append(
        {
            "type": "response.completed",
            "response": {
                "id": response_id,
                "status": "completed",
                "output": output,
            },
        }
    )
    return [{**event, **({"stream_id": stream_id} if stream_id else {})} for event in events]


class FakeConnection:
    def __init__(
        self,
        *,
        auto_reply=True,
        failed=(),
        request_errors=(),
        subscription_error=False,
        event_overrides=None,
    ):
        self.response = SimpleNamespace(create=self.create_response)
        self.incoming = asyncio.Queue()
        self.sent = []
        self.requests = []
        self.auto_reply = auto_reply
        self.failed = set(failed)
        self.request_errors = set(request_errors)
        self.subscription_error = subscription_error
        self.event_overrides = event_overrides or {}
        self.consumer_id = None
        self.connected = asyncio.Event()
        self.closed = False
        self.active_receivers = 0
        self.max_receivers = 0
        self.reader_cancelled = False

    async def __aenter__(self):
        self.connected.set()
        return self

    async def __aexit__(self, *args):
        self.closed = True

    async def send(self, event):
        self.sent.append(event)
        if event["type"] == "dots.subscribe":
            self.consumer_id = event["consumer_id"]
            if self.subscription_error:
                self.publish(
                    {"type": "error", "status": 409, "error": {"code": "push_consumer_conflict"}}
                )
                return
            self.publish(
                {
                    "type": "dots.subscribed",
                    "consumer_id": event["consumer_id"],
                    "channel": "default",
                    "cursor": 0,
                }
            )

    async def create_response(self, **payload):
        self.requests.append(payload)
        text = payload["input"]
        response_id = f"resp_{len(self.requests)}"
        if text in self.event_overrides:
            self.publish(
                *[
                    {**event, "stream_id": payload["stream_id"]}
                    for event in self.event_overrides[text]
                ]
            )
        elif text in self.request_errors:
            self.publish(
                {
                    "type": "error",
                    "status": 400,
                    "stream_id": payload["stream_id"],
                    "error": {"code": "invalid_request_error"},
                }
            )
        elif text in self.failed:
            self.publish(
                {
                    "type": "response.failed",
                    "response": {
                        "id": response_id,
                        "status": "failed",
                        "error": {"code": "server_error", "message": "request failed"},
                    },
                }
            )
        elif self.auto_reply:
            self.publish(
                *response_events(text, response_id=response_id, stream_id=payload.get("stream_id"))
            )

    def publish(self, *events):
        for event in events:
            self.incoming.put_nowait(
                event if isinstance(event, (Exception, bytes)) else json.dumps(event).encode()
            )

    def push(self, text="入力中のお知らせ", *, message_id="push_1"):
        self.publish(
            {
                "type": "dots.push",
                "consumer_id": self.consumer_id or "example-openai",
                "message": {
                    "id": message_id,
                    "sequence": 101,
                    "kind": "push",
                    "request_id": None,
                    "channel": "default",
                    "text": text,
                    "metadata": {},
                    "received_at": "2026-10-22T00:00:00Z",
                },
            }
        )

    async def recv_bytes(self):
        self.active_receivers += 1
        self.max_receivers = max(self.max_receivers, self.active_receivers)
        try:
            event = await self.incoming.get()
            if isinstance(event, Exception):
                raise event
            return event
        except asyncio.CancelledError:
            self.reader_cancelled = True
            raise
        finally:
            self.active_receivers -= 1


class FakeResponses:
    def __init__(self, connection):
        self.connection = connection
        self.connects = []

    def connect(self, **options):
        self.connects.append(options)
        return self.connection


class FakeClient:
    def __init__(self, *, connection=None, **completion_options):
        self.chat = SimpleNamespace(completions=FakeCompletions(**completion_options))
        self.responses = FakeResponses(connection or FakeConnection())


async def wait_until(predicate):
    async with asyncio.timeout(1):
        while not predicate():  # noqa: ASYNC110 - Observe the independently running prompt or socket.
            await asyncio.sleep(0.001)


@asynccontextmanager
async def running_chat(client, session, **options):
    task = asyncio.create_task(example.run_chat(client, session, **options))
    try:
        yield task
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.fixture
def terminal():
    prompt_toolkit = pytest.importorskip("prompt_toolkit")
    from prompt_toolkit.input.defaults import create_pipe_input
    from prompt_toolkit.output import DummyOutput

    with create_pipe_input() as pipe:
        yield pipe, prompt_toolkit.PromptSession(input=pipe, output=DummyOutput())


async def test_chat_has_no_implicit_initial_send_and_keeps_latest_user_only(capsys):
    client = FakeClient()
    args = example.parser().parse_args([])
    assert args.text is None
    session = ScriptedSession("", "こんにちは", "次も教えて", "/exit")
    await example.run_chat(client, session)
    assert client.chat.completions.requests == [
        {"model": "dots", "messages": [{"role": "user", "content": "こんにちは"}]},
        {"model": "dots", "messages": [{"role": "user", "content": "次も教えて"}]},
    ]
    output = capsys.readouterr().out
    assert "dots> 返信: こんにちは" in output
    assert "dots> 返信: 次も教えて" in output


@pytest.mark.parametrize("mode", ["chat", "stream", "websocket"])
async def test_initial_text_is_sent_once_then_interactive_input_continues(mode):
    client = FakeClient()
    await example.run_chat(client, ScriptedSession("続き", "/quit"), initial_text="最初", mode=mode)
    if mode == "websocket":
        assert [request["input"] for request in client.responses.connection.requests] == [
            "最初",
            "続き",
        ]
    else:
        assert [request["messages"] for request in client.chat.completions.requests] == [
            [{"role": "user", "content": "最初"}],
            [{"role": "user", "content": "続き"}],
        ]


async def test_rest_turn_waits_for_reply_before_requesting_next_input():
    client = FakeClient(blocked={"first"})
    session = ScriptedSession("first", "second", "/exit")
    async with running_chat(client, session) as task:
        await asyncio.wait_for(client.chat.completions.started.wait(), 1)
        assert session.prompt_count == 1
        assert len(client.chat.completions.requests) == 1
        client.chat.completions.release.set()
        async with asyncio.timeout(1):
            await task
    assert len(client.chat.completions.requests) == 2


async def test_rest_error_reports_safe_type_and_continues_without_resending(capsys):
    client = FakeClient(failures={"失敗": RuntimeError("private-request-details")})
    await example.run_chat(client, ScriptedSession("失敗", "次", "/exit"))
    assert [request["messages"][0]["content"] for request in client.chat.completions.requests] == [
        "失敗",
        "次",
    ]
    output = capsys.readouterr().out
    assert "RuntimeError" in output
    assert "private-request-details" not in output
    assert "dots> 返信: 次" in output
    assert re.search(r"elapsed=\d+(?:\.\d+)?s", output)


@pytest.mark.parametrize("mode", ["chat", "stream"])
@pytest.mark.parametrize("wrapped", [False, True])
async def test_sdk_error_details_explain_failure_and_keep_bridge_request_id(mode, wrapped, capsys):
    details = {
        "code": "bridge_reply_timeout",
        "message": "Timed out waiting for dots; the request remains running.",
        "request_id": "req_diagnostic_body",
    }
    error = APIError(
        "SDK request failed",
        body={"error": details} if wrapped else details,
        status_code=504 if mode == "chat" else None,
    )
    if mode == "chat":
        client = FakeClient(failures={"失敗": error})
    else:
        client = FakeClient(
            stream_overrides={
                "失敗": {
                    "chunks": [
                        chunk("まだ作業中です\n\n", kind="progress"),
                        error,
                    ]
                }
            }
        )
    await example.run_chat(client, ScriptedSession("失敗", "次", "/exit"), mode=mode)
    output = capsys.readouterr().out
    assert "APIError" in output
    assert details["code"] in output
    assert details["message"] in output
    assert details["request_id"] in output
    assert re.search(r"elapsed=\d+(?:\.\d+)?s", output)
    if mode == "chat":
        assert "504" in output
    assert "dots> 返信: 次" in output
    assert len(client.chat.completions.requests) == 2
    assert all(stream.closed for stream in client.chat.completions.streams)


async def test_sdk_error_attributes_are_used_when_no_structured_body_is_available(capsys):
    error = APIError(
        "The connector is temporarily rate limited.",
        code="rate_limit_exceeded",
        status_code=429,
        request_id="req_sdk_attribute",
    )
    client = FakeClient(failures={"失敗": error})
    await example.run_chat(client, ScriptedSession("失敗", "/exit"))
    output = capsys.readouterr().out
    assert error.message in output
    assert "rate_limit_exceeded" in output
    assert "429" in output
    assert "req_sdk_attribute" in output
    assert re.search(r"elapsed=\d+(?:\.\d+)?s", output)


async def test_stream_error_uses_progress_request_id_when_body_has_none(capsys):
    error = APIError(
        "SDK interrupted",
        body={
            "code": "bridge_unavailable",
            "message": "The bridge connection was interrupted.",
        },
    )
    client = FakeClient(
        stream_overrides={
            "失敗": {
                "chunks": [
                    chunk(
                        "作業を続けています\n\n", kind="progress", request_id="req_from_progress"
                    ),
                    error,
                ]
            }
        }
    )
    await example.run_chat(client, ScriptedSession("失敗", "/exit"), mode="stream")
    output = capsys.readouterr().out
    assert "progress> 作業を続けています" in output
    assert "bridge_unavailable" in output
    assert "req_from_progress" in output
    assert client.chat.completions.streams[0].closed


async def test_stream_failure_before_first_chunk_uses_response_header_request_id(capsys):
    error = APIError(
        "SDK interrupted",
        body={
            "code": "bridge_unavailable",
            "message": "The stream failed before its first chunk.",
        },
    )
    client = FakeClient(
        stream_overrides={
            "失敗": {
                "chunks": [error],
                "headers": {"X-Dots-Request-ID": "req_before_first_chunk"},
            }
        }
    )
    await example.run_chat(client, ScriptedSession("失敗", "/exit"), mode="stream")
    output = capsys.readouterr().out
    assert "bridge_unavailable" in output
    assert "req_before_first_chunk" in output
    assert re.search(r"elapsed=\d+(?:\.\d+)?s", output)
    assert client.chat.completions.streams[0].closed


async def test_elapsed_includes_wait_after_progress_without_modifying_asyncio_clock(
    monkeypatch, capsys
):
    clock = SimpleNamespace(now=100.0)
    # Replace the example's module binding, not time.monotonic globally: the
    # event loop must keep using its real clock for cancellation and scheduling.
    monkeypatch.setattr(example, "time", SimpleNamespace(monotonic=lambda: clock.now))
    error = APIError(
        "SDK error",
        body={
            "code": "bridge_reply_timeout",
            "message": "Waiting for dots reached the deadline.",
        },
    )

    class TimedStream(FakeStream):
        async def __anext__(self):
            try:
                return await super().__anext__()
            except APIError:
                clock.now = 220.0
                raise

    stream = TimedStream([chunk("進捗です\n\n", kind="progress"), error])
    client = FakeClient()

    async def create(**payload):
        client.chat.completions.requests.append(payload)
        return stream

    client.chat.completions.create = create
    await example.run_chat(client, ScriptedSession("失敗", "/exit"), mode="stream")
    output = capsys.readouterr().out
    assert "elapsed=120.0s" in output
    assert "bridge_reply_timeout" in output
    assert stream.closed
    assert len(client.chat.completions.requests) == 1


async def test_stream_labels_progress_and_reply_and_closes_each_turn(capsys):
    client = FakeClient()
    await example.run_chat(client, ScriptedSession("first", "second", "/exit"), mode="stream")
    output = capsys.readouterr().out
    assert "progress> 進捗: first" in output
    assert "dots> 返信: first" in output
    assert "dots> 返信: second" in output
    assert all(stream.entered and stream.closed for stream in client.chat.completions.streams)
    assert [request["messages"] for request in client.chat.completions.requests] == [
        [{"role": "user", "content": "first"}],
        [{"role": "user", "content": "second"}],
    ]
    assert all(request["stream"] is True for request in client.chat.completions.requests)


async def test_stream_error_closes_stream_and_next_turn_can_succeed(capsys):
    client = FakeClient()
    await example.run_chat(client, ScriptedSession("途中で失敗", "次", "/exit"), mode="stream")
    assert len(client.chat.completions.requests) == 2
    assert all(stream.closed for stream in client.chat.completions.streams)
    output = capsys.readouterr().out
    assert "RuntimeError" in output
    assert "private-stream-failure-detail" not in output
    assert "dots> 返信: 次" in output


@pytest.mark.parametrize("exit_input", ["/exit", "/quit", EOFError(), KeyboardInterrupt()])
@pytest.mark.parametrize("mode", ["chat", "stream"])
async def test_rest_exit_eof_and_ctrl_c_finish_without_sending(exit_input, mode):
    client = FakeClient()
    async with asyncio.timeout(1):
        await example.run_chat(client, ScriptedSession(exit_input), mode=mode)
    assert client.chat.completions.requests == []


async def test_caller_cancellation_is_not_swallowed_as_a_turn_error():
    client = FakeClient(blocked={"pending"})
    async with running_chat(client, ScriptedSession("pending")) as task:
        await asyncio.wait_for(client.chat.completions.started.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert len(client.chat.completions.requests) == 1


async def test_websocket_keeps_one_connection_and_one_reader_for_multiple_turns(capsys):
    client = FakeClient()
    connection = client.responses.connection
    async with asyncio.timeout(1):
        await example.run_chat(
            client, ScriptedSession("first", "second", "/exit"), mode="websocket"
        )
    assert client.responses.connects == [{"max_retries": 0}]
    assert connection.sent == [{"type": "dots.subscribe", "consumer_id": "example-openai"}]
    assert [(request["model"], request["input"]) for request in connection.requests] == [
        ("dots", "first"),
        ("dots", "second"),
    ]
    assert len({request["stream_id"] for request in connection.requests}) == 2
    assert connection.max_receivers == 1
    assert connection.active_receivers == 0
    assert connection.reader_cancelled
    assert connection.closed
    output = capsys.readouterr().out
    assert "progress> 進捗: first" in output
    assert "dots> 返信: first" in output
    assert "dots> 返信: second" in output


async def test_websocket_waits_for_completion_but_receives_push_during_pending_turn(capsys):
    connection = FakeConnection(auto_reply=False)
    client = FakeClient(connection=connection)
    session = ScriptedSession("first", "second", "/exit")
    async with running_chat(client, session, mode="websocket", consumer_id="my-app") as task:
        await wait_until(lambda: len(connection.requests) == 1)
        assert session.prompt_count == 1
        connection.push("処理中のお知らせ")
        await wait_until(lambda: any(event["type"] == "dots.push.ack" for event in connection.sent))
        assert session.prompt_count == 1
        connection.publish(*response_events("first", response_id="resp_1"))
        await wait_until(lambda: len(connection.requests) == 2)
        assert session.prompt_count == 2
        connection.publish(*response_events("second", response_id="resp_2"))
        async with asyncio.timeout(1):
            await task
    assert connection.sent[0] == {"type": "dots.subscribe", "consumer_id": "my-app"}
    assert {"type": "dots.push.ack", "message_id": "push_1"} in connection.sent
    assert "push> 処理中のお知らせ" in capsys.readouterr().out


async def test_websocket_failed_request_reports_error_and_accepts_next_turn(capsys):
    connection = FakeConnection(failed={"失敗"})
    client = FakeClient(connection=connection)
    async with asyncio.timeout(1):
        await example.run_chat(client, ScriptedSession("失敗", "次", "/exit"), mode="websocket")
    assert [request["input"] for request in connection.requests] == ["失敗", "次"]
    output = capsys.readouterr().out
    assert "system>" in output
    assert "dots> 返信: 次" in output
    assert connection.closed


async def test_websocket_correlated_request_error_is_not_a_subscription_failure(capsys):
    connection = FakeConnection(request_errors={"invalid"})
    client = FakeClient(connection=connection)
    async with asyncio.timeout(1):
        await example.run_chat(
            client, ScriptedSession("invalid", "next", "/exit"), mode="websocket"
        )
    assert [request["input"] for request in connection.requests] == ["invalid", "next"]
    output = capsys.readouterr().out
    assert "system>" in output
    assert "dots> 返信: next" in output
    assert len(client.responses.connects) == 1


@pytest.mark.parametrize(
    "event, expected",
    [
        (
            {
                "type": "response.failed",
                "response": {
                    "status": "failed",
                    "error": {
                        "code": "bridge_reply_timeout",
                        "message": "Dots did not reply before the deadline.",
                    },
                    "metadata": {"dots_bridge_request_id": "req_ws_failed"},
                },
            },
            ["bridge_reply_timeout", "Dots did not reply before the deadline.", "req_ws_failed"],
        ),
        pytest.param(
            {
                "type": "response.failed",
                "response": {
                    "status": "failed",
                    "error": {
                        "code": "server_error",
                        "message": "Dots did not reply before the metadata deadline.",
                    },
                    "metadata": {
                        "dots_bridge_error_code": "bridge_reply_timeout",
                        "dots_bridge_request_id": "req_ws_metadata",
                    },
                },
            },
            [
                "code=bridge_reply_timeout",
                "request_id=req_ws_metadata",
                "Dots did not reply before the metadata deadline.",
            ],
            id="sdk-compatible-server-error-with-bridge-cause",
        ),
        (
            {
                "type": "response.incomplete",
                "response": {
                    "status": "incomplete",
                    "incomplete_details": {"reason": "max_output_tokens"},
                    "metadata": {"dots_bridge_request_id": "req_ws_incomplete"},
                },
            },
            ["max_output_tokens", "req_ws_incomplete"],
        ),
        (
            {
                "type": "error",
                "status": 400,
                "error": {
                    "code": "invalid_request_error",
                    "message": "Only text input is supported.",
                    "request_id": "req_ws_error",
                },
            },
            ["invalid_request_error", "Only text input is supported.", "req_ws_error"],
        ),
    ],
)
async def test_websocket_failure_diagnostics_keep_structured_cause_and_request_id(
    event, expected, capsys
):
    connection = FakeConnection(event_overrides={"失敗": [event]})
    client = FakeClient(connection=connection)
    async with asyncio.timeout(1):
        await example.run_chat(client, ScriptedSession("失敗", "次", "/exit"), mode="websocket")
    output = capsys.readouterr().out
    for value in expected:
        assert value in output
    assert "dots> 返信: 次" in output
    assert [request["input"] for request in connection.requests] == ["失敗", "次"]


async def test_websocket_request_timeout_is_configurable_and_does_not_resend():
    connection = FakeConnection(auto_reply=False)
    client = FakeClient(connection=connection)
    async with asyncio.timeout(1):
        with pytest.raises(TimeoutError):
            await example.run_chat(
                client, ScriptedSession("pending"), mode="websocket", timeout=0.01
            )
    assert len(connection.requests) == 1
    assert connection.closed
    assert connection.active_receivers == 0


def test_parser_defaults_to_long_running_client_timeout_and_allows_override():
    assert example.parser().parse_args([]).timeout == 660
    assert example.parser().parse_args(["--timeout", "900"]).timeout == 900


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf"])
def test_parser_rejects_unbounded_or_nonpositive_client_timeout(value):
    with pytest.raises(SystemExit) as error:
        example.parser().parse_args(["--timeout", value])
    assert error.value.code == 2


async def test_websocket_subscription_failure_stops_before_initial_send_and_prompt():
    connection = FakeConnection(subscription_error=True)
    client = FakeClient(connection=connection)
    session = ScriptedSession("never prompted")
    async with asyncio.timeout(1):
        with pytest.raises(RuntimeError):
            await example.run_chat(client, session, mode="websocket", initial_text="never sent")
    assert session.prompt_count == 0
    assert connection.requests == []
    assert connection.closed
    assert connection.active_receivers == 0


async def test_websocket_push_preserves_real_partially_typed_prompt_and_acks(terminal, capsys):
    patch_stdout = pytest.importorskip("prompt_toolkit.patch_stdout").patch_stdout
    from prompt_toolkit.application.current import create_app_session
    from prompt_toolkit.output.defaults import create_output

    pipe, session = terminal
    client = FakeClient()
    connection = client.responses.connection
    # Isolate prompt_toolkit's cached output from captures closed by earlier tests.
    with create_app_session(output=create_output(stdout=sys.stdout)), patch_stdout():
        async with running_chat(client, session, mode="websocket") as task:
            await wait_until(lambda: session.app.is_running)
            pipe.send_text("入力途中")
            await wait_until(lambda: session.default_buffer.text == "入力途中")
            connection.push()
            await wait_until(
                lambda: any(event["type"] == "dots.push.ack" for event in connection.sent)
            )
            assert session.default_buffer.text == "入力途中"
            pipe.send_text("の続き\r")
            await wait_until(lambda: len(connection.requests) == 1)
            assert connection.requests[0]["input"] == "入力途中の続き"
            await wait_until(lambda: session.app.is_running and not session.default_buffer.text)
            pipe.send_text("/exit\r")
            async with asyncio.timeout(1):
                await task
    assert "push> 入力中のお知らせ" in capsys.readouterr().out
    assert connection.closed
    assert connection.active_receivers == 0


@pytest.mark.parametrize(
    "incoming, error_type",
    [(ConnectionError("private-socket-details"), ConnectionError), (b"not-json", ValueError)],
)
async def test_websocket_terminal_reader_failure_cancels_pending_input_without_reconnecting(
    incoming, error_type
):
    client = FakeClient()
    connection = client.responses.connection
    session = PendingSession()
    async with running_chat(client, session, mode="websocket") as task:
        await asyncio.wait_for(session.started.wait(), 1)
        connection.publish(incoming)
        async with asyncio.timeout(1):
            with pytest.raises(error_type):
                await task
    assert session.cancelled.is_set()
    assert connection.closed
    assert connection.active_receivers == 0
    assert connection.requests == []
    assert client.responses.connects == [{"max_retries": 0}]
