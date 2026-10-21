import json
import os
import stat

import pytest

from dots_bridge import cli
from dots_bridge.client import ReplyTimeoutError
from dots_bridge.config import Settings, write_config


@pytest.fixture
def config(tmp_path):
    return write_config(tmp_path / "bridge.toml", "https://bridge.example.com")


def test_init_creates_private_file_without_printing_keys(tmp_path, capsys):
    path = tmp_path / "bridge.toml"
    assert (
        cli.main(["init", "--config", str(path), "--public-url", "https://bridge.example.com"]) == 0
    )
    settings = Settings.from_file(path)
    output = capsys.readouterr()
    assert str(path) in output.out
    assert all(
        key not in output.out + output.err
        for key in (
            settings.owner_key,
            settings.api_key,
            settings.storage_key,
        )
    )
    if os.name != "nt":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    before = path.read_bytes()
    assert (
        cli.main(["init", "--config", str(path), "--public-url", "https://bridge.example.com"]) == 1
    )
    assert path.read_bytes() == before


def test_send_wait_prints_request_id_then_reply_and_passes_arguments(config, monkeypatch, capsys):
    seen = []

    class Client:
        def __init__(self, url, *, api_key):
            seen.append((url, api_key))

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def send_message(self, text, **kwargs):
            seen.append((text, kwargs))
            return {"request_id": "req_test", "delivery_status": "queued"}

        def wait_for_reply(self, request_id, **kwargs):
            seen.append((request_id, kwargs))
            return {"request_id": request_id, "reply": "Done"}

    monkeypatch.setattr(cli, "BridgeClient", Client)
    assert (
        cli.main(
            [
                "--config",
                str(config),
                "send",
                "Hello",
                "--url",
                "https://bridge.example.com",
                "--channel",
                "chat",
                "--subscription-id",
                "sub_test",
                "--idempotency-key",
                "once",
                "--wait",
                "--timeout",
                "20",
            ]
        )
        == 0
    )
    output = capsys.readouterr().out
    assert '"request_id": "req_test"' in output
    assert '"reply": "Done"' in output
    assert seen == [
        ("https://bridge.example.com", Settings.from_file(config).api_key),
        ("Hello", {"channel": "chat", "subscription_id": "sub_test", "idempotency_key": "once"}),
        ("req_test", {"timeout": 20.0}),
    ]


def test_cli_timeout_preserves_request_id(config, monkeypatch, capsys):
    class Client:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def send_message(self, *args, **kwargs):
            return {"request_id": "req_resume"}

        def wait_for_reply(self, *args, **kwargs):
            raise ReplyTimeoutError("req_resume")

    monkeypatch.setattr(cli, "BridgeClient", Client)
    assert cli.main(["send", "Hello", "--config", str(config), "--wait"]) == 3
    output = capsys.readouterr()
    assert "req_resume" in output.out
    assert "req_resume" in output.err


def test_serve_starts_one_worker_without_access_logs(config, monkeypatch):
    import uvicorn

    import dots_bridge.app

    sentinel = object()
    seen = []
    monkeypatch.setattr(dots_bridge.app, "create_app", lambda settings: sentinel)
    monkeypatch.setattr(uvicorn, "run", lambda app, **kwargs: seen.append((app, kwargs)))
    assert cli.main(["serve", "--config", str(config)]) == 0
    assert seen == [
        (
            sentinel,
            {
                "host": "127.0.0.1",
                "port": 8765,
                "workers": 1,
                "access_log": False,
            },
        )
    ]


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        ("0.0.0.0", "http://127.0.0.1:8765"),
        ("::", "http://127.0.0.1:8765"),
        ("::1", "http://[::1]:8765"),
    ],
)
def test_local_url_handles_wildcard_and_ipv6(config, host, expected):
    settings = Settings.from_file(config).model_copy(update={"host": host})
    assert cli._local_url(settings) == expected


def test_config_validation_never_prints_secret_values(config, capsys):
    invalid_key = "secret-too-short"
    content = config.read_text()
    settings = Settings.from_file(config)
    config.write_text(content.replace(settings.owner_key, invalid_key))
    assert cli.main(["subscriptions", "--config", str(config)]) == 2
    output = capsys.readouterr()
    assert "owner_key" in output.err
    assert invalid_key not in output.out + output.err


def test_prune_dispatch_uses_requested_retention(config, monkeypatch, capsys):
    seen = []

    async def maintenance(settings, command, days):
        seen.append((command, days))
        return {"deleted_messages": 5}

    monkeypatch.setattr(cli, "_maintenance", maintenance)
    assert cli.main(["prune", "--config", str(config), "--days", "7"]) == 0
    assert seen == [("prune", 7)]
    assert json.loads(capsys.readouterr().out) == {"deleted_messages": 5}
