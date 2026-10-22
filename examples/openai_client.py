"""Chat with dots through the standalone connector and official OpenAI SDK.

Run ``dots-bridge connector openai`` in another terminal first.
Install ``pip install 'openai[realtime]>=3.8,<4' 'prompt-toolkit>=3.0.43,<4'``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from contextlib import suppress
from pathlib import Path

from dots_bridge import Settings

ROOT = Path(__file__).resolve().parents[1]


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description=__doc__)
    command.add_argument("text", nargs="?", help="Send this first, then continue chatting")
    command.add_argument("--config", type=Path, default=ROOT / "bridge.toml")
    command.add_argument("--url", default="http://127.0.0.1:8766/v1")
    command.add_argument("--mode", choices=["chat", "stream", "websocket"], default="chat")
    command.add_argument(
        "--consumer-id",
        default="example-openai",
        help="WebSocket push checkpoint ID (reuse when reconnecting)",
    )
    return command


async def _inputs(session, initial_text):
    if initial_text is not None:
        print(f"user> {initial_text}", flush=True)
    while True:
        if initial_text is not None:
            text, initial_text = initial_text, None
        else:
            try:
                text = await session.prompt_async("user> ")
            except (EOFError, KeyboardInterrupt):
                return
        if text.strip() in {"/exit", "/quit"}:
            return
        if text.strip():
            yield text


class _TextOutput:
    """Print deltas as they arrive, with one label per progress/final message."""

    def __init__(self):
        self.key = None
        self.newline = True

    def write(self, text, *, key, label):
        if not text:
            return
        if key != self.key:
            self.finish()
            print(f"{label}> ", end="", flush=True)
            self.key = key
        print(text, end="", flush=True)
        self.newline = text.endswith("\n")

    def finish(self):
        if self.key is not None and not self.newline:
            print(flush=True)
        self.key = None
        self.newline = True


async def _http_chat(client, session, *, stream, initial_text):
    output = _TextOutput()
    async for text in _inputs(session, initial_text):
        try:
            # The dot owns the conversation context; only send the latest input.
            arguments = {"model": "dots", "messages": [{"role": "user", "content": text}]}
            if stream:
                async with await client.chat.completions.create(**arguments, stream=True) as chunks:
                    async for chunk in chunks:
                        if not chunk.choices:
                            continue
                        metadata = getattr(chunk, "dots_bridge", None) or {}
                        kind = metadata.get("kind", "reply")
                        output.write(
                            chunk.choices[0].delta.content or "",
                            key=metadata.get("message_id") or kind,
                            label="progress" if kind == "progress" else "dots",
                        )
            else:
                result = await client.chat.completions.create(**arguments)
                print(f"dots> {result.choices[0].message.content or ''}", flush=True)
        except Exception as error:
            output.finish()
            # A timeout may happen after enqueueing. Leave retries to the user.
            print(f"system> 送受信に失敗しました（{type(error).__name__}）。", flush=True)
        finally:
            output.finish()


async def _websocket_chat(client, session, *, initial_text, consumer_id):
    async with client.responses.connect(max_retries=0) as connection:
        # Use one raw reader: dots.* are connector extensions, outside SDK event types.
        await connection.send({"type": "dots.subscribe", "consumer_id": consumer_id})
        ready = asyncio.Event()
        completed = asyncio.Event()
        output = _TextOutput()
        phases = {}
        pending_stream = None

        async def receive():
            while True:
                event = json.loads(await connection.recv_bytes())
                event_type = event["type"]
                if event_type == "dots.subscribed":
                    ready.set()
                elif event_type == "dots.push":
                    message = event["message"]
                    output.finish()
                    print(f"push> {message['text']}", flush=True)
                    # Display is this demo's processing step. In an application,
                    # acknowledge only after your own ingestion has succeeded.
                    await connection.send({"type": "dots.push.ack", "message_id": message["id"]})
                elif event_type == "response.output_item.added":
                    item = event["item"]
                    phases[item["id"]] = item.get("phase")
                elif event_type == "response.output_text.delta":
                    item_id = event["item_id"]
                    output.write(
                        event["delta"],
                        key=item_id,
                        label="progress" if phases.get(item_id) == "commentary" else "dots",
                    )
                elif event_type == "response.output_text.done":
                    output.finish()
                elif event_type in {"response.completed", "response.failed", "response.incomplete"}:
                    output.finish()
                    phases.clear()
                    if event_type != "response.completed":
                        print(f"system> {event_type}", flush=True)
                    completed.set()
                elif event_type == "error":
                    # Correlate request errors separately from push/subscription failures.
                    if pending_stream is None or event.get("stream_id") != pending_stream:
                        raise RuntimeError("WebSocketの購読・受信に失敗しました。")
                    output.finish()
                    phases.clear()
                    print("system> 要求を完了できませんでした（error）。", flush=True)
                    completed.set()

        async def send():
            nonlocal pending_stream
            async with asyncio.timeout(30):
                await ready.wait()
            turn = 0
            async for text in _inputs(session, initial_text):
                turn += 1
                pending_stream = f"turn_{turn}"
                completed.clear()
                await connection.response.create(model="dots", input=text, stream_id=pending_stream)
                async with asyncio.timeout(180):
                    await completed.wait()
                pending_stream = None

        tasks = [asyncio.create_task(receive()), asyncio.create_task(send())]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in tasks:
                if task in done:
                    await task
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            output.finish()


async def run_chat(
    client,
    session,
    *,
    mode="chat",
    initial_text=None,
    consumer_id="example-openai",
):
    print("Enterで送信、/exit・/quit・Ctrl+C・Ctrl+Dで終了します。", flush=True)
    if mode == "websocket":
        print("自発pushも受信します。このサンプルで未処理のpushから再開します。", flush=True)
        await _websocket_chat(client, session, initial_text=initial_text, consumer_id=consumer_id)
    else:
        await _http_chat(client, session, stream=mode == "stream", initial_text=initial_text)


async def main(args):
    try:
        from openai import AsyncOpenAI
        from prompt_toolkit import PromptSession
        from prompt_toolkit.patch_stdout import patch_stdout
    except ImportError:
        raise ImportError(
            "依存を追加してください: python -m pip install "
            "'openai[realtime]>=3.8,<4' 'prompt-toolkit>=3.0.43,<4'"
        ) from None
    settings = Settings.from_file(args.config)
    session = PromptSession()
    # Keep the input buffer intact while a push arrives, until all tasks stop.
    with patch_stdout():
        async with AsyncOpenAI(
            api_key=settings.api_key,
            base_url=args.url,
            timeout=180,
            max_retries=0,
        ) as client:
            await run_chat(
                client,
                session,
                mode=args.mode,
                initial_text=args.text,
                consumer_id=args.consumer_id,
            )


def entrypoint():
    args = parser().parse_args()
    try:
        asyncio.run(main(args))
    except (KeyboardInterrupt, EOFError):
        pass
    except ImportError as error:
        print(f"system> {error}", flush=True)
        return 1
    except Exception as error:
        print(f"system> チャットを終了しました（{type(error).__name__}）。", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    with suppress(BrokenPipeError):
        raise SystemExit(entrypoint())
