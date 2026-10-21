import asyncio

import pytest
from cryptography.fernet import Fernet

from dots_bridge import BridgeError, DotsBridge, ReceivedMessage, Settings, create_app


@pytest.fixture
def settings(tmp_path):
    return Settings(
        owner_key="owner-" + "a" * 32,
        api_key="app-" + "b" * 32,
        storage_key=Fernet.generate_key().decode(),
        db_path=tmp_path / "bridge.sqlite3",
    )


async def test_embedded_decorator_starts_with_app_lifespan_and_wakes_on_push(settings):
    bridge = DotsBridge(settings)
    entered = asyncio.Event()
    unblock = asyncio.Event()
    seen = []

    @bridge.on_received
    async def receive(message: ReceivedMessage):
        seen.append(message)
        entered.set()
        await unblock.wait()

    app = create_app(settings, bridge=bridge)
    async with app.router.lifespan_context(app):
        assert bridge.receiving
        # send_to_app returns after persistence; the callback can keep processing independently.
        pushed = await asyncio.wait_for(
            bridge.send_to_app("通知", idempotency_key="push-1"),
            1,
        )
        await asyncio.wait_for(entered.wait(), 1)
        assert seen[0].id == pushed["id"]
        assert seen[0].kind == "push"
        assert await bridge.get_consumer_cursor("in-process") == 0
        unblock.set()
        async with asyncio.timeout(1):
            # Callback completion precedes the durable acknowledgement.
            while True:  # noqa: ASYNC110 - Poll persisted state.
                if await bridge.get_consumer_cursor("in-process") >= pushed["sequence"]:
                    break
                await asyncio.sleep(0.01)
    assert not bridge.receiving
    assert bridge._worker is None


async def test_unsolicited_push_does_not_require_subscription_and_is_idempotent(settings):
    bridge = DotsBridge(settings)
    one = await bridge.send_to_app("通知", channel="news", idempotency_key="push-1")
    assert (await bridge.send_to_app("通知", channel="news", idempotency_key="push-1")) == one
    assert one["request_id"] is None
    assert len(await bridge.list_received()) == 1
    with pytest.raises(BridgeError) as caught:
        await bridge.send_to_app("変更した通知", channel="news", idempotency_key="push-1")
    assert caught.value.code == "idempotency_conflict"
    await bridge.close()


@pytest.mark.parametrize(
    "options",
    [
        {"text": " "},
        {"text": "\ud800"},
        {"text": "x" * 64001},
        {"channel": "bad/channel"},
        {"idempotency_key": ""},
        {"metadata": {"bad": float("nan")}},
        {"metadata": {"bad": object()}},
    ],
)
async def test_invalid_push_fails_before_storage(settings, options):
    bridge = DotsBridge(settings)
    with pytest.raises(BridgeError) as caught:
        await bridge.send_to_app(**{"text": "hello", "idempotency_key": "push-1", **options})
    assert caught.value.code == "invalid_push"
    assert await bridge.list_received() == []
    await bridge.close()


async def test_push_rejects_payload_over_256k(settings):
    bridge = DotsBridge(settings)
    with pytest.raises(BridgeError) as caught:
        await bridge.send_to_app("通知", idempotency_key="large", metadata={"large": "a" * 262144})
    assert caught.value.status_code == 413
    await bridge.close()
