"""Call the standalone connector with the official OpenAI Python SDK.

Run ``dots-bridge openai --config bridge.toml`` in another terminal first.
Install the caller's SDK with ``pip install 'openai[realtime]>=3.8,<4'``.
"""

import argparse
from pathlib import Path

from dots_bridge import Settings

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("text", nargs="?", default="こんにちは！接続テストです。")
    parser.add_argument("--config", default=str(ROOT / "bridge.toml"))
    parser.add_argument("--url", default="http://127.0.0.1:8766/v1")
    parser.add_argument("--mode", choices=["chat", "stream", "websocket"], default="chat")
    args = parser.parse_args()
    try:
        from openai import OpenAI
    except ImportError:
        parser.error("Install the SDK first: pip install 'openai[realtime]>=3.8,<4'")
    settings = Settings.from_file(args.config)
    with OpenAI(
        api_key=settings.api_key,
        base_url=args.url,
        timeout=180,
        max_retries=0,
    ) as client:
        if args.mode == "websocket":
            with client.responses.connect(max_retries=0) as connection:
                connection.response.create(model="dots", input=args.text)
                for event in connection:
                    if event.type == "response.output_text.delta":
                        print(event.delta, end="", flush=True)
                    elif event.type == "response.completed":
                        print()
                        break
                    elif event.type in {"error", "response.failed", "response.incomplete"}:
                        raise RuntimeError(event.to_json())
        elif args.mode == "stream":
            with client.chat.completions.create(
                model="dots", messages=[{"role": "user", "content": args.text}], stream=True
            ) as stream:
                for chunk in stream:
                    if chunk.choices:
                        print(chunk.choices[0].delta.content or "", end="", flush=True)
            print()
        else:
            result = client.chat.completions.create(
                model="dots", messages=[{"role": "user", "content": args.text}]
            )
            print(result.choices[0].message.content)


if __name__ == "__main__":
    main()
