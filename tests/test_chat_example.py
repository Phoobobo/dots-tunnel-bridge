"""Exercise the terminal example without contacting a live dots instance."""

import asyncio
import importlib.util
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest

from dots_bridge import BridgeError, ReceivedMessage

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "chat.py"
spec = importlib.util.spec_from_file_location("dots_bridge_chat_example", EXAMPLE)
assert spec is not None and spec.loader is not None
chat = importlib.util.module_from_spec(spec)
spec.loader.exec_module(chat)


class FakeBridge:
    def __init__(self, *, auto_reply=False, send_errors=None):
        self.receiving = False
        self.receiver_error = None
        self.started = asyncio.Event()
        self.stop_count = 0
        self.sent = []
        self.handlers = []
        self.auto_reply = auto_reply
        self.send_errors = send_errors or {}
        self.sequence = 0

    def on_received(self, handler):
        self.handlers.append(handler)
        return handler

    async def start_receiving(self, **kwargs):
        self.receiving = True
        self.started.set()

    async def stop_receiving(self):
        self.receiving = False
        self.stop_count += 1

    async def send_message(self, text, **kwargs):
        self.sent.append((text, kwargs))
        if error := self.send_errors.get(text):
            raise error
        request_id = f"req_{len(self.sent)}"
        if self.auto_reply:
            await self.deliver(f"返信: {text}", request_id=request_id)
        return {"request_id": request_id}

    async def deliver(self, text, *, request_id=None):
        self.sequence += 1
        message = ReceivedMessage(
            sequence=self.sequence,
            id=f"recv_{self.sequence}",
            kind="reply" if request_id else "push",
            request_id=request_id,
            channel="default",
            text=text,
            metadata={},
            received_at="2026-10-22T00:00:00Z",
        )
        for handler in self.handlers:
            await handler(message)


class ScriptedSession:
    def __init__(self, *responses):
        self.responses = deque(responses)

    async def prompt_async(self, prompt):
        response = self.responses.popleft()
        if isinstance(response, BaseException):
            raise response
        return response


async def wait_until(predicate):
    async with asyncio.timeout(1):
        while not predicate():  # noqa: ASYNC110 - Wait for actual prompt/receiver state transitions.
            await asyncio.sleep(0.001)


@asynccontextmanager
async def running_chat(bridge, session, **kwargs):
    task = asyncio.create_task(chat.run_chat(bridge, session, **kwargs))
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
        session = prompt_toolkit.PromptSession(input=pipe, output=DummyOutput())
        yield pipe, session


async def test_chat_handles_multiple_sends_and_replies_in_one_process(capsys):
    bridge = FakeBridge(auto_reply=True)
    session = ScriptedSession("", "こんにちは", "もう少し教えて", "/exit")
    async with asyncio.timeout(1):
        await chat.run_chat(bridge, session, channel="chat", subscription_id="sub_chat")
    assert [text for text, _ in bridge.sent] == ["こんにちは", "もう少し教えて"]
    assert all(
        options == {"channel": "chat", "subscription_id": "sub_chat"} for _, options in bridge.sent
    )
    output = capsys.readouterr().out
    assert "dots> 返信: こんにちは" in output
    assert "dots> 返信: もう少し教えて" in output
    assert bridge.stop_count == 1
    assert not bridge.receiving


async def test_push_preserves_partially_typed_prompt_and_next_submission(terminal, capsys):
    patch_stdout = pytest.importorskip("prompt_toolkit.patch_stdout").patch_stdout
    pipe, session = terminal
    bridge = FakeBridge()
    with patch_stdout():
        async with running_chat(bridge, session) as task:
            await wait_until(lambda: session.app.is_running)
            pipe.send_text("入力途中")
            await wait_until(lambda: session.default_buffer.text == "入力途中")
            await bridge.deliver("先にお知らせします")
            assert session.default_buffer.text == "入力途中"
            assert not task.done()
            pipe.send_text("の続き\r")
            await wait_until(lambda: len(bridge.sent) == 1)
            assert bridge.sent[0][0] == "入力途中の続き"
            await wait_until(lambda: session.app.is_running and not session.default_buffer.text)
            pipe.send_text("/exit\r")
            async with asyncio.timeout(1):
                await task
    assert "dots> 先にお知らせします" in capsys.readouterr().out
    assert bridge.stop_count == 1
    assert not session.app.is_running


@pytest.mark.parametrize("exit_input", ["/exit\r", "\x04", "\x03"])
async def test_exit_eof_and_ctrl_c_stop_real_async_prompt_cleanly(terminal, exit_input):
    pipe, session = terminal
    bridge = FakeBridge()
    async with running_chat(bridge, session) as task:
        await wait_until(lambda: session.app.is_running)
        pipe.send_text(exit_input)
        async with asyncio.timeout(1):
            await task
    assert bridge.stop_count == 1
    assert not bridge.receiving
    assert not session.app.is_running
    assert bridge.sent == []


async def test_send_errors_do_not_resend_and_allow_following_input(capsys):
    bridge = FakeBridge(
        auto_reply=True,
        send_errors={
            "拒否される送信": BridgeError("no_subscription", "No subscription", 409),
            "結果不明の送信": httpx.ReadTimeout("private-request-detail"),
        },
    )
    session = ScriptedSession("拒否される送信", "結果不明の送信", "次の送信", "/exit")
    async with asyncio.timeout(1):
        await chat.run_chat(bridge, session)
    assert [text for text, _ in bridge.sent] == ["拒否される送信", "結果不明の送信", "次の送信"]
    output = capsys.readouterr().out
    assert "no_subscription" in output
    assert "ReadTimeout" in output
    assert "private-request-detail" not in output
    assert "dots> 返信: 次の送信" in output
    assert bridge.stop_count == 1


async def test_fatal_receiver_error_cancels_pending_real_prompt_and_stops_receiver(terminal):
    _, session = terminal
    bridge = FakeBridge()
    error = BridgeError("unauthorized", "API key revoked", 401)
    async with running_chat(bridge, session) as task:
        await wait_until(lambda: session.app.is_running)
        bridge.receiver_error = error
        bridge.receiving = False
        async with asyncio.timeout(0.5):
            with pytest.raises(BridgeError) as caught:
                await task
        assert caught.value is error
    assert bridge.stop_count == 1
    assert not session.app.is_running
    assert bridge.sent == []
