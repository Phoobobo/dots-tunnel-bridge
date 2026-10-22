"""Exercise the terminal example without contacting a live dots instance."""

import asyncio
import importlib.util
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
from cryptography.fernet import Fernet

from dots_bridge import BridgeError, DotsBridge, ReceivedMessage, Settings

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


async def test_direct_chat_silently_acknowledges_cancel_and_keeps_next_reply_visible(capsys):
    class CancellingBridge(FakeBridge):
        async def send_message(self, text, **options):
            self.sent.append((text, options))
            if text == "A":
                message = ReceivedMessage(
                    sequence=1,
                    id="cancel_a",
                    kind="cancel",
                    request_id="req_a",
                    channel="default",
                    text="",
                    metadata={},
                    received_at="2026-10-22T00:00:00Z",
                    superseded_by="req_b",
                )
                for handler in self.handlers:
                    await handler(message)
            else:
                await self.deliver("Bの返信", request_id="req_b")
            return {"request_id": "req_a" if text == "A" else "req_b"}

    bridge = CancellingBridge()
    await chat.run_chat(bridge, ScriptedSession("A", "B", "/exit"))
    output = capsys.readouterr().out
    assert "dots> Bの返信" in output
    assert output.count("dots> ") == 1
    assert "system>" not in output
    assert [text for text, _ in bridge.sent] == ["A", "B"]
    assert bridge.stop_count == 1


async def test_chat_continues_send_example_checkpoint_without_replaying_processed_messages(
    tmp_path,
    capsys,
):
    settings = Settings(
        owner_key="owner-" + "a" * 32,
        api_key="app-" + "b" * 32,
        storage_key=Fernet.generate_key().decode(),
        db_path=tmp_path / "bridge.sqlite3",
    )
    bridge = DotsBridge(settings)
    previous = await bridge.send_to_app("送信テストで処理済み", idempotency_key="old")
    # This persisted checkpoint was written by the send_message.py demo.
    await bridge.acknowledge_received("example-app", previous["sequence"], channel="default")
    options = chat.parser().parse_args([])
    new_message = None

    class Session:
        async def prompt_async(self, prompt):
            nonlocal new_message
            new_message = await bridge.send_to_app("チャットで新規受信", idempotency_key="new")
            async with asyncio.timeout(1):
                # Observe successful callback acknowledgement in the real SQLite store.
                while True:  # noqa: ASYNC110 - Wait for the durable receive cursor.
                    cursor = await bridge.get_consumer_cursor(options.consumer_id, "default")
                    if cursor >= new_message["sequence"]:
                        return "/exit"
                    await asyncio.sleep(0.005)

    try:
        await chat.run_chat(bridge, Session(), consumer_id=options.consumer_id)
        output = capsys.readouterr().out
        assert "dots> 送信テストで処理済み" not in output
        assert "dots> チャットで新規受信" in output
        assert await bridge.get_consumer_cursor("example-app", "default") == new_message["sequence"]
    finally:
        await bridge.close()


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
