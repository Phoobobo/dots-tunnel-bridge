"""Connector diagnostics explain outcomes using identifiers, never request content."""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from io import StringIO

import httpx
import pytest
from starlette.testclient import TestClient

from dots_bridge import BridgeError, ReceivedMessage, ReplyTimeoutError, cli
from dots_bridge.connectors import create_openai_connector

LOGGER = "dots_bridge.connectors.openai"
KEY = "private-authentication-key-do-not-log"
TEXT = "private user message do not log"
PROGRESS = "private progress text do not log"
FINAL = "private final answer do not log"
PRIVATE_URL = "https://private-upstream.example/path?token=secret"
AUTH = {"Authorization": "Bearer " + KEY}


class Bridge:
    def __init__(self, *, failure=None, enqueue_failure=None):
        self.failure = failure
        self.enqueue_failure = enqueue_failure

    async def send_message(self, text, **options):
        if self.enqueue_failure is not None:
            raise self.enqueue_failure
        return {"request_id": "req_diagnostic"}

    async def wait_for_reply(self, request_id, **options):
        if self.failure is not None:
            raise self.failure
        return {"reply": FINAL}

    async def iter_responses(self, request_id, **options):
        for sequence, kind, text in [(1, "progress", PROGRESS), (2, "reply", FINAL)]:
            if kind == "reply" and self.failure is not None:
                raise self.failure
            yield ReceivedMessage(
                id=f"message_{sequence}",
                sequence=sequence,
                kind=kind,
                request_id=request_id,
                channel="private-channel",
                text=text,
                metadata={"private": "metadata-do-not-log"},
                received_at="2026-10-22T00:00:00Z",
            )


@asynccontextmanager
async def http_connector(bridge=None, *, reply_timeout=17.5):
    app = create_openai_connector(
        api_key=KEY, client=bridge or Bridge(), reply_timeout=reply_timeout
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://connector.test", headers=AUTH
    ) as client:
        yield client


def payload(*, stream=False):
    return {"messages": [{"role": "user", "content": TEXT}], "stream": stream}


def records(caplog):
    return [record for record in caplog.records if record.name == LOGGER]


def assert_safe_diagnostics(caplog, *, protocol, timeout=17.5):
    result = records(caplog)
    assert result
    for record in result:
        assert record.protocol == protocol
        assert record.reply_timeout == timeout
        assert isinstance(record.elapsed, float)
        assert record.elapsed >= 0
        assert record.exc_info is None
        assert "elapsed=" in record.getMessage()
        assert "reply_timeout=" in record.getMessage()
    logs = repr([record.__dict__ for record in result])
    for private in [
        KEY,
        TEXT,
        PROGRESS,
        FINAL,
        PRIVATE_URL,
        "metadata-do-not-log",
        "private-channel",
    ]:
        assert private not in logs
    return result


@pytest.mark.parametrize("stream", [False, True])
async def test_success_lifecycle_has_request_id_elapsed_and_timeout_without_content(caplog, stream):
    with caplog.at_level(logging.INFO, logger=LOGGER):
        async with http_connector() as client:
            response = await client.post("/v1/chat/completions", json=payload(stream=stream))
    assert response.status_code == 200
    protocol = "chat.completions.stream" if stream else "chat.completions"
    result = assert_safe_diagnostics(caplog, protocol=protocol)
    assert [record.event for record in result] == (
        ["request_accepted", "progress_received", "final_received"]
        if stream
        else ["request_accepted", "final_received"]
    )
    assert all(record.request_id == "req_diagnostic" for record in result)
    assert all(record.levelno == logging.INFO for record in result)


@pytest.mark.parametrize("stream", [False, True])
async def test_timeout_log_identifies_deadline_and_preserves_wire_error(caplog, stream):
    with caplog.at_level(logging.INFO, logger=LOGGER):
        async with http_connector(Bridge(failure=ReplyTimeoutError("req_diagnostic"))) as client:
            response = await client.post("/v1/chat/completions", json=payload(stream=stream))
    if stream:
        assert response.status_code == 200
        data = [line[6:] for line in response.text.splitlines() if line.startswith("data: ")]
        assert json.loads(data[-2])["error"]["code"] == "bridge_reply_timeout"
    else:
        assert response.status_code == 504
        assert response.json()["error"]["code"] == "bridge_reply_timeout"
    protocol = "chat.completions.stream" if stream else "chat.completions"
    result = assert_safe_diagnostics(caplog, protocol=protocol)
    assert [record.event for record in result] == (
        ["request_accepted", "progress_received", "request_error"]
        if stream
        else ["request_accepted", "request_error"]
    )
    error = result[-1]
    assert error.levelno == logging.WARNING
    assert error.code == "bridge_reply_timeout"
    assert error.error_type == "ReplyTimeoutError"
    assert error.request_id == "req_diagnostic"
    assert "bridge_reply_timeout" in error.getMessage()


async def test_network_failure_log_uses_safe_code_instead_of_exception_or_upstream_url(caplog):
    bridge = Bridge(enqueue_failure=httpx.ConnectError(PRIVATE_URL))
    with caplog.at_level(logging.INFO, logger=LOGGER):
        async with http_connector(bridge) as client:
            response = await client.post("/v1/chat/completions", json=payload())
    assert response.status_code == 502
    result = assert_safe_diagnostics(caplog, protocol="chat.completions")
    assert len(result) == 1
    assert result[0].code == "bridge_unavailable"
    assert result[0].error_type == "ConnectError"
    assert result[0].request_id == "-"


async def test_upstream_read_timeout_is_distinguishable_from_dots_reply_deadline(caplog):
    with caplog.at_level(logging.INFO, logger=LOGGER):
        async with http_connector(Bridge(failure=httpx.ReadTimeout(PRIVATE_URL))) as client:
            response = await client.post("/v1/chat/completions", json=payload(stream=True))
    assert response.status_code == 200
    result = assert_safe_diagnostics(caplog, protocol="chat.completions.stream")
    assert result[-1].code == "bridge_unavailable"
    assert result[-1].error_type == "ReadTimeout"
    assert result[-1].request_id == "req_diagnostic"


async def test_upstream_error_code_cannot_inject_url_or_extra_lines_into_diagnostics(caplog):
    bridge = Bridge(enqueue_failure=BridgeError(PRIVATE_URL + "\ninjected", TEXT, 409))
    with caplog.at_level(logging.INFO, logger=LOGGER):
        async with http_connector(bridge) as client:
            response = await client.post("/v1/chat/completions", json=payload())
    assert response.status_code == 409
    result = assert_safe_diagnostics(caplog, protocol="chat.completions")
    assert result[0].code == "invalid"
    assert "injected" not in result[0].getMessage()


async def test_authentication_failure_is_logged_without_credentials(caplog):
    with caplog.at_level(logging.INFO, logger=LOGGER):
        async with http_connector() as client:
            response = await client.post(
                "/v1/chat/completions",
                headers={"Authorization": "Bearer wrong-private-key"},
                json=payload(),
            )
    assert response.status_code == 401
    result = assert_safe_diagnostics(caplog, protocol="chat.completions")
    assert result[0].code == "invalid_api_key"
    assert "wrong-private-key" not in result[0].getMessage()


def receive_event(socket):
    async def receive():
        async with asyncio.timeout(2):
            return await socket._send_rx.receive()

    message = socket.portal.call(receive)
    assert message["type"] == "websocket.send", message
    return json.loads(message["text"])


@pytest.mark.parametrize("timeout", [False, True])
def test_websocket_lifecycle_preserves_timeout_code_after_progress(caplog, timeout):
    bridge = Bridge(failure=ReplyTimeoutError("req_diagnostic") if timeout else None)
    app = create_openai_connector(api_key=KEY, client=bridge, reply_timeout=9.0)
    with caplog.at_level(logging.INFO, logger=LOGGER):
        with TestClient(app) as client:
            with client.websocket_connect("/v1/responses", headers=AUTH) as socket:
                socket.send_json({"type": "response.create", "input": TEXT, "stream_id": "turn"})
                events = []
                for _ in range(16):
                    event = receive_event(socket)
                    events.append(event)
                    if event["type"] in {"response.completed", "response.failed"}:
                        break
                expected = "response.failed" if timeout else "response.completed"
                assert events[-1]["type"] == expected
                if timeout:
                    assert events[-1]["response"]["error"]["code"] == "server_error"
                    assert (
                        events[-1]["response"]["metadata"]["dots_bridge_error_code"]
                        == "bridge_reply_timeout"
                    )
                    assert (
                        events[-1]["response"]["metadata"]["dots_bridge_request_id"]
                        == "req_diagnostic"
                    )
    result = assert_safe_diagnostics(caplog, protocol="responses.websocket", timeout=9.0)
    assert [record.event for record in result] == [
        "request_accepted",
        "progress_received",
        "request_error" if timeout else "final_received",
    ]
    assert all(record.request_id == "req_diagnostic" for record in result)
    if timeout:
        assert result[-1].code == "bridge_reply_timeout"


def test_cli_logging_configuration_shows_info_by_default_without_touching_root(monkeypatch):
    root = logging.getLogger()
    logger = logging.getLogger(LOGGER)
    monkeypatch.setattr(root, "handlers", [])
    monkeypatch.setattr(logger, "handlers", [])
    monkeypatch.setattr(logger, "level", logging.NOTSET)
    stream = StringIO()
    monkeypatch.setattr("sys.stderr", stream)
    original_root_level = root.level
    try:
        cli._configure_connector_logging()
        cli._configure_connector_logging()
        assert logger.level == logging.INFO
        assert len(logger.handlers) == 1
        logger.info("safe-lifecycle")
        assert stream.getvalue().count("safe-lifecycle") == 1
        assert root.handlers == []
        assert root.level == original_root_level
    finally:
        for handler in logger.handlers:
            handler.close()


def test_cli_logging_configuration_uses_an_existing_application_handler(monkeypatch):
    logger = logging.getLogger(LOGGER)
    root = logging.getLogger()
    handler = logging.StreamHandler(StringIO())
    monkeypatch.setattr(logger, "handlers", [])
    monkeypatch.setattr(logger, "level", logging.NOTSET)
    monkeypatch.setattr(root, "handlers", [handler])
    cli._configure_connector_logging()
    assert logger.handlers == []
    assert logger.level == logging.INFO
