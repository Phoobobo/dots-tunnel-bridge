"""Interactive chat: install '.[chat]', then run python examples/chat.py."""

from __future__ import annotations

import argparse
import asyncio
from contextlib import suppress
from pathlib import Path

import httpx
from pydantic import ValidationError

from dots_bridge import AsyncBridgeClient, BridgeError, ReceivedMessage, Settings

ROOT = Path(__file__).resolve().parents[1]


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description="Chat with your connected dots.")
    command.add_argument("--config", type=Path, default=ROOT / "bridge.toml")
    command.add_argument("--url", help="Override the bridge HTTP URL")
    command.add_argument("--channel", default="default")
    command.add_argument(
        "--subscription-id", help="Select a dot when several subscribe to a channel"
    )
    command.add_argument("--consumer-id", default="example-chat")
    return command


def local_url(settings: Settings) -> str:
    host = settings.host
    if host in {"0.0.0.0", "::", "[::]"}:
        host = "127.0.0.1"
    return str(httpx.URL(scheme="http", host=host.strip("[]"), port=settings.port))


async def read_and_send(bridge, session, *, channel, subscription_id):
    while True:
        try:
            text = await session.prompt_async("user> ")
        except (EOFError, KeyboardInterrupt):
            return
        if text.strip() in {"/exit", "/quit"}:
            return
        if not text.strip():
            continue
        try:
            await bridge.send_message(text, channel=channel, subscription_id=subscription_id)
        except BridgeError as error:
            print(f"system> {error.code}: {error.message}", flush=True)
        except ValidationError:
            print("system> メッセージは1〜16000文字で入力してください。", flush=True)
        except httpx.RequestError as error:
            # A timeout can happen after enqueueing. Never resend automatically.
            print(f"system> 送信結果を確認できませんでした（{type(error).__name__}）。", flush=True)


async def watch_receiver(bridge):
    while bridge.receiving:  # noqa: ASYNC110 - Observe the existing client's receiver state.
        await asyncio.sleep(0.1)
    if bridge.receiver_error is not None:
        raise bridge.receiver_error


async def run_chat(
    bridge,
    session,
    *,
    channel="default",
    subscription_id=None,
    consumer_id="example-chat",
):
    @bridge.on_received
    async def received(message: ReceivedMessage):
        print(f"dots> {message.text}", flush=True)

    print("Enterで送信、/exit・Ctrl+C・Ctrl+Dで終了します。", flush=True)
    print("保存済みの未処理メッセージも受信します。", flush=True)
    await bridge.start_receiving(consumer_id=consumer_id, channel=channel)
    tasks = [
        asyncio.create_task(
            read_and_send(
                bridge,
                session,
                channel=channel,
                subscription_id=subscription_id,
            )
        ),
        asyncio.create_task(watch_receiver(bridge)),
    ]
    try:
        completed, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        # Surface terminal receive errors instead of leaving a disconnected prompt.
        for task in completed:
            await task
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await bridge.stop_receiving()


async def main(args):
    try:
        from prompt_toolkit import PromptSession
        from prompt_toolkit.patch_stdout import patch_stdout
    except ImportError:
        raise RuntimeError(
            f"チャット用依存を追加してください: python -m pip install -e '{ROOT}[chat]'"
        ) from None
    settings = Settings.from_file(args.config)
    session = PromptSession()
    # Keep output patched until the receiver has shut down, so pushes preserve
    # the current input buffer and redraw the user prompt safely.
    with patch_stdout():
        async with AsyncBridgeClient(
            args.url or local_url(settings), api_key=settings.api_key
        ) as bridge:
            await run_chat(
                bridge,
                session,
                channel=args.channel,
                subscription_id=args.subscription_id,
                consumer_id=args.consumer_id,
            )


def entrypoint():
    args = parser().parse_args()
    try:
        asyncio.run(main(args))
    except (KeyboardInterrupt, EOFError):
        pass
    except BridgeError as error:
        print(f"system> {error.code}: {error.message}")
        return 1
    except (OSError, ValueError, RuntimeError, httpx.HTTPError) as error:
        print(f"system> {error}")
        return 1
    return 0


if __name__ == "__main__":
    with suppress(BrokenPipeError):
        raise SystemExit(entrypoint())
