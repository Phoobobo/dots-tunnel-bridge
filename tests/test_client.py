import asyncio
import json
import time
from types import SimpleNamespace

import httpx
import pytest

import dots_bridge.client as client_module
from dots_bridge import AsyncBridgeClient, BridgeClient, BridgeError, ReplyTimeoutError

KEY = "application-key"


def message(*, replied=False, delivery="accepted", reply_status=None):
    return {
        "request_id": "req_test",
        "delivery_status": delivery,
        "reply_status": reply_status or ("replied" if replied else "pending"),
        "reply": "Hello from dots" if replied else None,
    }


def test_sync_ask_posts_once_and_passes_application_fields():
    seen = []

    def handle(request):
        assert request.headers["authorization"] == "Bearer " + KEY
        seen.append((request.method, request.url.path))
        if request.url.path == "/api/subscriptions":
            return httpx.Response(200, json={"subscriptions": [{"id": "sub_1", "channel": "chat"}]})
        if request.method == "POST":
            assert json.loads(request.content) == {
                "text": "Hello",
                "channel": "chat",
                "subscription_id": "sub_1",
                "metadata": {"source": "test"},
                "idempotency_key": "once",
            }
            return httpx.Response(202, json=message(delivery="queued"))
        return httpx.Response(200, json=message(replied=True))

    with BridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        assert client.list_subscriptions()[0]["id"] == "sub_1"
        assert (
            client.ask(
                "Hello",
                channel="chat",
                subscription_id="sub_1",
                metadata={"source": "test"},
                idempotency_key="once",
            )
            == "Hello from dots"
        )
    assert seen.count(("POST", "/api/messages")) == 1


class FakeClock:
    now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, duration):
        self.now += duration


def test_sync_timeout_exposes_request_id_and_can_resume_without_resending(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(client_module, "time", clock)
    posts = []
    replied = False

    def handle(request):
        if request.method == "POST":
            posts.append(request)
            return httpx.Response(202, json=message(delivery="queued"))
        return httpx.Response(200, json=message(replied=replied))

    with BridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ReplyTimeoutError) as error:
            client.ask("Hello", timeout=1.1, poll_interval=0.5)
        assert isinstance(error.value, TimeoutError)
        assert error.value.request_id == "req_test"
        assert clock.now == 1.1
        replied = True
        result = client.wait_for_reply(error.value.request_id)
        assert result["reply"] == "Hello from dots"
    assert len(posts) == 1


def test_sync_reply_received_after_deadline_reports_timeout():
    def slow_reply(request):
        time.sleep(0.05)
        return httpx.Response(200, json=message(replied=True))

    with BridgeClient(api_key=KEY, transport=httpx.MockTransport(slow_reply)) as client:
        with pytest.raises(ReplyTimeoutError) as error:
            client.wait_for_reply("req_test", timeout=0.01)
    assert error.value.request_id == "req_test"


@pytest.mark.asyncio
async def test_async_ask_and_timeout_use_the_same_resume_contract(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(client_module, "time", clock)

    async def sleep(duration):
        clock.sleep(duration)

    monkeypatch.setattr(
        client_module, "asyncio", SimpleNamespace(sleep=sleep, timeout=asyncio.timeout)
    )
    posts = []
    replied = False

    async def handle(request):
        assert request.headers["authorization"] == "Bearer " + KEY
        if request.method == "POST":
            posts.append(request)
            return httpx.Response(202, json=message(delivery="queued"))
        if request.url.path == "/api/subscriptions":
            return httpx.Response(200, json={"subscriptions": []})
        return httpx.Response(200, json=message(replied=replied))

    async with AsyncBridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        assert await client.list_subscriptions() == []
        with pytest.raises(ReplyTimeoutError) as error:
            await client.ask("Hello", timeout=0.9, poll_interval=0.5)
        assert error.value.request_id == "req_test"
        replied = True
        assert (await client.wait_for_reply("req_test"))["reply"] == "Hello from dots"
        assert await client.ask("Hello again") == "Hello from dots"
        assert (await client.get_message("req_test"))["reply_status"] == "replied"
    assert len(posts) == 2


@pytest.mark.parametrize("status", [401, 404, 409, 422, 503])
def test_http_errors_preserve_safe_bridge_code_and_message(status):
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            status, json={"code": "example_error", "message": "An actionable explanation."}
        )
    )
    with BridgeClient(api_key=KEY, transport=transport) as client:
        with pytest.raises(BridgeError) as error:
            client.get_message("req_test")
    assert error.value.code == "example_error"
    assert error.value.message == "An actionable explanation."
    assert error.value.status_code == status


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        ({"delivery": "failed"}, "delivery_failed"),
        ({"delivery": "cancelled"}, "delivery_cancelled"),
        ({"reply_status": "expired"}, "reply_expired"),
        ({"reply_status": "cancelled"}, "reply_cancelled"),
    ],
)
def test_wait_reports_terminal_states(state, expected):
    transport = httpx.MockTransport(lambda request: httpx.Response(200, json=message(**state)))
    with BridgeClient(api_key=KEY, transport=transport) as client:
        with pytest.raises(BridgeError) as error:
            client.wait_for_reply("req_test")
    assert error.value.code == expected


@pytest.mark.parametrize("bad", [0, -1, float("nan"), float("inf"), True])
def test_invalid_wait_settings_fail_before_sending(bad):
    def handle(request):
        pytest.fail("Invalid wait settings must not create a request")

    with BridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ValueError):
            client.ask("Hello", timeout=bad)
        with pytest.raises(ValueError):
            client.ask("Hello", poll_interval=bad)


def test_redirects_are_not_followed_and_invalid_json_is_safe():
    seen = []

    def handle(request):
        seen.append(request)
        return httpx.Response(302, headers={"Location": "https://other.example.com/api/messages"})

    with BridgeClient(api_key=KEY, transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(BridgeError) as error:
            client.get_message("req_test")
        assert error.value.code == "http_302"
    assert len(seen) == 1

    transport = httpx.MockTransport(lambda request: httpx.Response(200, text="Not JSON"))
    with BridgeClient(api_key=KEY, transport=transport) as client:
        with pytest.raises(BridgeError, match="invalid JSON"):
            client.get_message("req_test")
