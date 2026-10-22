"""Chat with dots through the standalone connector and official OpenAI SDK.

Run ``dots-bridge connector openai`` in another terminal first.
Install ``pip install 'openai[realtime]>=3.8,<4' 'prompt-toolkit>=3.0.43,<4'``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from contextlib import suppress
from pathlib import Path

from dots_bridge import Settings

ROOT = Path(__file__).resolve().parents[1]


def _timeout(value):
    try:
        seconds = float(value)
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError
    except ValueError:
        raise argparse.ArgumentTypeError("Timeout must be a positive finite number") from None
    return seconds


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description=__doc__)
    command.add_argument("text", nargs="?", help="Send this first, then continue chatting")
    command.add_argument("--config", type=Path, default=ROOT / "bridge.toml")
    command.add_argument("--url", default="http://127.0.0.1:8766/v1")
    command.add_argument("--mode", choices=["chat", "stream", "websocket"], default="chat")
    command.add_argument(
        "--interruptible",
        action="store_true",
        help="Accept another input while waiting for earlier replies",
    )
    command.add_argument(
        "--timeout", type=_timeout, default=660.0, help="Client timeout seconds (default: 660)"
    )
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

    def discard(self):
        self.key = None
        self.newline = True


def _field(value):
    # Keep diagnostics to selected scalar fields, on one terminal line.
    if isinstance(value, (str, int)) and not isinstance(value, bool):
        return " ".join(str(value).split())[:2000]
    return None


def _request_id(response):
    return _field(getattr(response, "headers", {}).get("X-Dots-Request-ID"))


def _report_error(kind, *, details=None, elapsed=None, request_id=None, status_code=None):
    details = details if isinstance(details, dict) else {}
    code = _field(details.get("code"))
    request_id = _field(details.get("request_id")) or _field(request_id)
    fields = [kind]
    if code:
        fields.append(f"code={code}")
    if status_code is not None:
        fields.append(f"HTTP {status_code}")
    if elapsed is not None:
        fields.append(f"elapsed={elapsed:.1f}s")
    if request_id:
        fields.append(f"request_id={request_id}")
    print(f"system> 送受信に失敗しました（{', '.join(fields)}）。", flush=True)
    if message := _field(details.get("message")):
        print(f"system> {message}", flush=True)
    if code == "bridge_reply_timeout":
        print(
            "system> コネクターの返信待ちがタイムアウトしました。依頼は取り消されていません。"
            "待機時間はコネクターの --reply-timeout で延長できます。",
            flush=True,
        )


def _report_exception(error, *, elapsed=None, request_id=None):
    body = getattr(error, "body", None)
    details = body.get("error", body) if isinstance(body, dict) else {}
    details = dict(details) if isinstance(details, dict) else {}
    if not details.get("code"):
        details["code"] = getattr(error, "code", None)
    # Transport errors have a public SDK message but no response body.
    if body is None:
        details["message"] = getattr(error, "message", None)
    request_id = (
        _request_id(getattr(error, "response", None))
        or request_id
        or _field(getattr(error, "request_id", None))
    )
    _report_error(
        type(error).__name__,
        details=details,
        elapsed=elapsed,
        request_id=request_id,
        status_code=getattr(error, "status_code", None),
    )


def _report_event_error(event, *, elapsed=None):
    response = event.get("response", {})
    details = event.get("error") or response.get("error") or {}
    metadata = response.get("metadata") or {}
    if bridge_code := _field(metadata.get("dots_bridge_error_code")):
        # ResponseError.code uses standard Responses values. The connector keeps
        # its more specific cause in response metadata for compatible clients.
        details = {**details, "code": bridge_code}
    if not details and (reason := _field((response.get("incomplete_details") or {}).get("reason"))):
        details = {"code": reason, "message": "Response incomplete: " + reason}
    _report_error(
        event["type"],
        details=details,
        elapsed=elapsed,
        request_id=metadata.get("dots_bridge_request_id"),
        status_code=event.get("status"),
    )


async def _http_turn(client, text, *, stream):
    output = _TextOutput()
    started = time.monotonic()
    request_id = None
    cancelled = False
    try:
        # The dot owns the conversation context; only send the latest input.
        arguments = {"model": "dots", "messages": [{"role": "user", "content": text}]}
        if stream:
            async with await client.chat.completions.create(**arguments, stream=True) as chunks:
                request_id = _request_id(getattr(chunks, "response", None))
                async for chunk in chunks:
                    metadata = getattr(chunk, "dots_bridge", None) or {}
                    request_id = _field(metadata.get("request_id")) or request_id
                    kind = metadata.get("kind", "reply")
                    if kind == "cancel":
                        cancelled = True
                        output.discard()
                        break
                    if not chunk.choices:
                        continue
                    output.write(
                        chunk.choices[0].delta.content or "",
                        key=metadata.get("message_id") or kind,
                        label="progress" if kind == "progress" else "dots",
                    )
        else:
            result = await client.chat.completions.create(**arguments)
            if (getattr(result, "dots_bridge", None) or {}).get("kind") == "cancel":
                cancelled = True
            else:
                print(f"dots> {result.choices[0].message.content or ''}", flush=True)
    except Exception as error:
        if not cancelled:
            output.finish()
            # A timeout may happen after enqueueing. Leave retries to the user.
            _report_exception(error, elapsed=time.monotonic() - started, request_id=request_id)
    finally:
        if cancelled:
            output.discard()
        else:
            output.finish()


async def _http_chat(client, session, *, stream, initial_text, interruptible):
    tasks = set()
    try:
        async for text in _inputs(session, initial_text):
            if not interruptible:
                await _http_turn(client, text, stream=stream)
                continue
            task = asyncio.create_task(_http_turn(client, text, stream=stream))
            tasks.add(task)
            task.add_done_callback(tasks.discard)
            # Start the request even when an injected input source is already ready.
            await asyncio.sleep(0)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


class _PendingResponse:
    def __init__(self):
        self.completed = asyncio.Event()
        self.output = _TextOutput()
        self.phases = {}
        self.started = time.monotonic()
        self.cancelled = False


async def _websocket_chat(
    client, session, *, initial_text, consumer_id, reply_timeout, interruptible
):
    async with client.responses.connect(max_retries=0) as connection:
        # Use one raw reader: dots.* are connector extensions, outside SDK event types.
        await connection.send({"type": "dots.subscribe", "consumer_id": consumer_id})
        ready = asyncio.Event()
        pending = {}
        retired = set()
        waiters = set()

        def response_for(event):
            stream_id = event.get("stream_id")
            if stream_id in pending:
                return pending[stream_id]
            # Serial mode also accepts older connectors without lane metadata.
            if stream_id is None and not interruptible and len(pending) == 1:
                return next(iter(pending.values()))
            return None

        async def receive():
            while True:
                event = json.loads(await connection.recv_bytes())
                event_type = event["type"]
                if event_type == "dots.subscribed":
                    ready.set()
                    continue
                elif event_type == "dots.push":
                    message = event["message"]
                    for response in pending.values():
                        response.output.finish()
                    print(f"push> {message['text']}", flush=True)
                    # Display is this demo's processing step. In an application,
                    # acknowledge only after your own ingestion has succeeded.
                    await connection.send({"type": "dots.push.ack", "message_id": message["id"]})
                    continue
                else:
                    if event.get("stream_id") in retired:
                        continue
                    response = response_for(event)
                    if response is not None and response.completed.is_set():
                        continue
                    if event_type == "error":
                        if response is not None:
                            response.output.finish()
                        _report_event_error(
                            event,
                            elapsed=time.monotonic() - response.started if response else None,
                        )
                        if response is None:
                            raise RuntimeError("WebSocketの購読・受信に失敗しました。")
                        response.phases.clear()
                        response.completed.set()
                        continue
                    if response is None:
                        continue
                if event_type == "response.output_item.added":
                    item = event["item"]
                    response.phases[item["id"]] = item.get("phase")
                elif event_type == "response.output_text.delta":
                    item_id = event["item_id"]
                    response.output.write(
                        event["delta"],
                        key=item_id,
                        label="progress"
                        if response.phases.get(item_id) == "commentary"
                        else "dots",
                    )
                elif event_type == "response.output_text.done":
                    response.output.finish()
                elif event_type in {"response.completed", "response.failed", "response.incomplete"}:
                    wire_response = event.get("response") or {}
                    metadata = wire_response.get("metadata") or {}
                    response.cancelled = (
                        metadata.get("dots_bridge_kind") == "cancel"
                        or wire_response.get("status") == "cancelled"
                    )
                    if response.cancelled:
                        response.output.discard()
                    else:
                        response.output.finish()
                    response.phases.clear()
                    if not response.cancelled and event_type != "response.completed":
                        _report_event_error(event, elapsed=time.monotonic() - response.started)
                    response.completed.set()

        async def wait_response(stream_id, response):
            try:
                async with asyncio.timeout(reply_timeout):
                    await response.completed.wait()
            except TimeoutError:
                response.output.finish()
                _report_error(
                    "TimeoutError",
                    details={
                        "code": "client_reply_timeout",
                        "message": "WebSocketの応答待ち期限を超えました。"
                        "--timeout で延長できます。依頼は取り消されていません。",
                    },
                    elapsed=time.monotonic() - response.started,
                )
                if not interruptible:
                    raise
            finally:
                pending.pop(stream_id, None)
                retired.add(stream_id)
                if response.cancelled:
                    response.output.discard()
                else:
                    response.output.finish()

        async def send():
            async with asyncio.timeout(30):
                await ready.wait()
            turn = 0
            async for text in _inputs(session, initial_text):
                turn += 1
                stream_id = f"turn_{turn}"
                response = _PendingResponse()
                pending[stream_id] = response
                try:
                    await connection.response.create(model="dots", input=text, stream_id=stream_id)
                except BaseException:
                    pending.pop(stream_id, None)
                    response.output.discard()
                    raise
                if interruptible:
                    waiter = asyncio.create_task(wait_response(stream_id, response))
                    waiters.add(waiter)
                    waiter.add_done_callback(waiters.discard)
                else:
                    await wait_response(stream_id, response)

        tasks = [asyncio.create_task(receive()), asyncio.create_task(send())]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in tasks:
                if task in done:
                    await task
        finally:
            cleanup = [*tasks, *waiters]
            for task in cleanup:
                task.cancel()
            await asyncio.gather(*cleanup, return_exceptions=True)
            for response in pending.values():
                response.output.discard()
                response.phases.clear()
            pending.clear()
            retired.clear()


async def run_chat(
    client,
    session,
    *,
    mode="chat",
    initial_text=None,
    consumer_id="example-openai",
    timeout=660,  # noqa: ASYNC109 - Forwarded to the WebSocket turn deadline.
    interruptible=False,
):
    print("Enterで送信、/exit・/quit・Ctrl+C・Ctrl+Dで終了します。", flush=True)
    if mode == "websocket":
        print("自発pushも受信します。このサンプルで未処理のpushから再開します。", flush=True)
        await _websocket_chat(
            client,
            session,
            initial_text=initial_text,
            consumer_id=consumer_id,
            reply_timeout=timeout,
            interruptible=interruptible,
        )
    else:
        await _http_chat(
            client,
            session,
            stream=mode == "stream",
            initial_text=initial_text,
            interruptible=interruptible,
        )


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
            timeout=args.timeout,
            max_retries=0,
        ) as client:
            await run_chat(
                client,
                session,
                mode=args.mode,
                initial_text=args.text,
                consumer_id=args.consumer_id,
                timeout=args.timeout,
                interruptible=args.interruptible,
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
        _report_exception(error)
        return 1
    return 0


if __name__ == "__main__":
    with suppress(BrokenPipeError):
        raise SystemExit(entrypoint())
