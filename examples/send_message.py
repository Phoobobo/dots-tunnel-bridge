"""Run from the repository root after connecting a dot: python examples/send_message.py"""

from dots_bridge import BridgeClient, Settings

settings = Settings.from_file("bridge.toml")
with BridgeClient("http://127.0.0.1:8765", api_key=settings.api_key) as client:
    message = client.send_message("接続テストです。受信したと返信してください。")
    print("request_id:", message["request_id"])
    result = client.wait_for_reply(message["request_id"], timeout=120)
    print(result["reply"])
