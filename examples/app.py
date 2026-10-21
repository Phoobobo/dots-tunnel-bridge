"""Run: uvicorn examples.app:app --host 127.0.0.1 --port 8765 --no-access-log"""

from dots_bridge import Settings, create_app

app = create_app(Settings.from_file("bridge.toml"))
