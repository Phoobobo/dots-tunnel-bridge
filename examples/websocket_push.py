"""Receive spontaneous dots pushes and Responses replies on one WebSocket.

Start ``dots-bridge connector openai`` in another terminal.
Use the same consumer ID when reconnecting to resume unacknowledged pushes.
"""

import argparse
import asyncio
import json
from pathlib import Path

from websockets.asyncio.client import connect

from dots_bridge import Settings

ROOT = Path(__file__).resolve().parents[1]


async def receive(args, api_key):
    async with connect(
        args.url,
        additional_headers={"Authorization": "Bearer " + api_key},
        max_size=1024 * 1024,
    ) as socket:
        await socket.send(json.dumps({"type": "dots.subscribe", "consumer_id": args.consumer_id}))
        async for raw in socket:
            event = json.loads(raw)
            event_type = event.get("type")
            if event_type == "dots.subscribed":
                print(f"Subscribed: {event['consumer_id']} (cursor={event['cursor']})", flush=True)
                if args.message:
                    await socket.send(
                        json.dumps(
                            {
                                "type": "response.create",
                                "model": "dots",
                                "input": args.message,
                            },
                            ensure_ascii=False,
                        )
                    )
            elif event_type == "dots.push":
                message = event["message"]
                # Replace this print with durable ingestion into your agent.
                # Deduplicate side effects by message['id'] before acknowledging.
                print(f"\npush> {message['text']}", flush=True)
                await socket.send(
                    json.dumps(
                        {
                            "type": "dots.push.ack",
                            "message_id": message["id"],
                        }
                    )
                )
            elif event_type == "response.output_text.delta":
                print(event["delta"], end="", flush=True)
            elif event_type == "response.output_text.done":
                print(flush=True)
            elif event_type in {"error", "response.failed", "response.incomplete"}:
                raise RuntimeError(json.dumps(event, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=str(ROOT / "bridge.toml"))
    parser.add_argument("--url", default="ws://127.0.0.1:8766/v1/responses")
    parser.add_argument("--consumer-id", default="personal-agent")
    parser.add_argument("--message", help="Also send one request after subscribing")
    args = parser.parse_args()
    settings = Settings.from_file(args.config)
    try:
        asyncio.run(receive(args, settings.api_key))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
