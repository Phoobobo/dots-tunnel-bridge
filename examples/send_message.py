"""Send independently and handle replies/pushes: python examples/send_message.py"""

import asyncio

from dots_bridge import AsyncBridgeClient, ReceivedMessage, Settings


async def main():
    settings = Settings.from_file("bridge.toml")
    async with AsyncBridgeClient("http://127.0.0.1:8765", api_key=settings.api_key) as bridge:

        @bridge.on_received
        async def received(message: ReceivedMessage):
            # Replace this with your application logic. id is stable across retries.
            print(message.kind, message.request_id, message.text)

        await bridge.start_receiving(consumer_id="example-app", channel="default")
        message = await bridge.send_message("接続テストです。受信したと返信してください。")
        print("queued:", message["request_id"])
        # The application can perform other work here. Keep its event loop alive.
        await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
