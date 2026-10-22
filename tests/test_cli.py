import json
import os
import stat
import sys
from types import ModuleType

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


@pytest.fixture
def connector_server(monkeypatch):
    import uvicorn

    sentinel = object()
    seen = []
    module = ModuleType("dots_bridge.connectors.openai")

    def create_connector(bridge_url, **kwargs):
        seen.append(("factory", bridge_url, kwargs))
        return sentinel

    module.create_openai_connector = create_connector
    monkeypatch.setitem(sys.modules, "dots_bridge.connectors.openai", module)
    monkeypatch.setattr(
        uvicorn, "run", lambda app, **kwargs: seen.append(("run", app, kwargs))
    )
    return sentinel, seen


def test_openai_connector_defaults_are_separate_from_bridge_server(config, connector_server):
    sentinel, seen = connector_server
    assert cli.main(["connector", "openai", "--config", str(config)]) == 0
    assert seen == [
        (
            "factory",
            "http://127.0.0.1:8765",
            {
                "api_key": Settings.from_file(config).api_key,
                "channel": "default",
                "subscription_id": None,
                "reply_timeout": 120.0,
            },
        ),
        (
            "run",
            sentinel,
            {
                "host": "127.0.0.1",
                "port": 8766,
                "workers": 1,
                "access_log": False,
                "ws_max_size": 300 * 1024,
            },
        ),
    ]


@pytest.mark.parametrize("option_position", ["root", "connector", "openai"])
def test_openai_connector_passes_explicit_arguments(config, connector_server, option_position):
    sentinel, seen = connector_server
    connection_arguments = ["--config", str(config), "--url", "http://127.0.0.1:9000"]
    arguments = []
    if option_position == "root":
        arguments += connection_arguments
    arguments += ["connector"]
    if option_position == "connector":
        arguments += connection_arguments
    arguments += ["openai"]
    if option_position == "openai":
        arguments += connection_arguments
    arguments += [
        "--host", "0.0.0.0",
        "--port", "9001",
        "--channel", "chat",
        "--subscription-id", "sub_test",
        "--reply-timeout", "17.5",
    ]
    assert cli.main(arguments) == 0
    assert seen == [
        (
            "factory",
            "http://127.0.0.1:9000",
            {
                "api_key": Settings.from_file(config).api_key,
                "channel": "chat",
                "subscription_id": "sub_test",
                "reply_timeout": 17.5,
            },
        ),
        (
            "run",
            sentinel,
            {
                "host": "0.0.0.0",
                "port": 9001,
                "workers": 1,
                "access_log": False,
                "ws_max_size": 300 * 1024,
            },
        ),
    ]


def test_openai_connector_default_bridge_url_uses_bridge_configuration(
    config, connector_server, monkeypatch
):
    settings = Settings.from_file(config).model_copy(update={"host": "::", "port": 9000})
    monkeypatch.setattr(Settings, "from_file", lambda path: settings)
    assert cli.main(["connector", "openai", "--config", str(config)]) == 0
    assert connector_server[1][0][1] == "http://127.0.0.1:9000"
    assert connector_server[1][1][2]["port"] == 8766


@pytest.mark.parametrize("port", ["0", "-1", "65536", "invalid", "1.5"])
def test_openai_connector_rejects_invalid_ports_before_loading_configuration(port, monkeypatch):
    def fail_load(path):
        raise AssertionError("Configuration should not be loaded")

    monkeypatch.setattr(Settings, "from_file", fail_load)
    with pytest.raises(SystemExit) as caught:
        cli.main(["connector", "openai", "--port", port])
    assert caught.value.code == 2


@pytest.mark.parametrize("port", [1, 65535])
def test_openai_connector_accepts_port_boundaries(port):
    assert cli._parser().parse_args(["connector", "openai", "--port", str(port)]).port == port


@pytest.mark.parametrize("duration", ["0", "-1", "nan", "inf"])
def test_openai_connector_rejects_invalid_reply_timeout(duration):
    with pytest.raises(SystemExit) as caught:
        cli.main(["connector", "openai", "--reply-timeout", duration])
    assert caught.value.code == 2


def test_openai_connector_loads_default_configuration_from_working_directory(
    config, connector_server, monkeypatch
):
    monkeypatch.chdir(config.parent)
    assert cli.main(["connector", "openai"]) == 0
    assert connector_server[1][0][2]["api_key"] == Settings.from_file(config).api_key


@pytest.mark.parametrize("arguments", [["connector"], ["connector", "unknown"], ["openai"]])
def test_connector_requires_a_known_nested_command_before_loading_configuration(
    arguments, monkeypatch
):
    def fail_load(path):
        raise AssertionError("Configuration should not be loaded")

    monkeypatch.setattr(Settings, "from_file", fail_load)
    with pytest.raises(SystemExit) as caught:
        cli.main(arguments)
    assert caught.value.code == 2


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
