"""Handle dots messages inside the server process.

Run: uvicorn examples.embedded:app --host 127.0.0.1 --port 8765 --no-access-log
"""

from dots_bridge import DotsBridge, ReceivedMessage, Settings, create_app

settings = Settings.from_file("bridge.toml")
bridge = DotsBridge(settings)


@bridge.on_received
async def received(message: ReceivedMessage):
    print(message.kind, message.request_id, message.text)


# Starting the app lifespan also starts registered callbacks; shutdown stops them.
app = create_app(settings, bridge=bridge)
