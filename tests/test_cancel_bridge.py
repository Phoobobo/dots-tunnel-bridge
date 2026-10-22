"""Cancellation validates replacement IDs and wakes ordinary application receivers."""

import asyncio
import time

import pytest
from cryptography.fernet import Fernet

from dots_bridge import BridgeError, DotsBridge, Settings


@pytest.fixture
async def bridge(tmp_path):
    result = DotsBridge(
        Settings(
            owner_key="owner-" + "a" * 32,
            api_key="app-" + "b" * 32,
            storage_key=Fernet.generate_key().decode(),
            db_path=tmp_path / "bridge.sqlite3",
        )
    )
    await result.initialize()
    now = time.time()
    await result.store.save_subscription(
        {
            "id": "sub_chat",
            "owner": result.owner_user_id,
            "channel": "chat",
            "destination": {"url": "https://callback.example"},
            "expires_at": now + 1000,
            "verified_at": now,
        }
    )
    yield result
    await result.close()


async def test_cancellation_exposes_terminal_receipt_and_wakes_embedded_receiver(bridge):
    old = await bridge.send_message("old request", channel="chat", metadata={"source": "app"})
    new = await bridge.send_message("replacement request", channel="chat")
    assert (await bridge.get_message(old["request_id"]))["reply_status"] == "pending"
    received = []
    ready = asyncio.Event()

    @bridge.on_received
    async def on_received(message):
        received.append(message)
        ready.set()

    await bridge.start_receiving(consumer_id="cancel-test", poll_interval=60)
    cancelled = await bridge.cancel_request(old["request_id"], superseded_by=new["request_id"])
    await asyncio.wait_for(ready.wait(), 1)
    assert received[0].model_dump() == cancelled
    assert received[0].kind == "cancel"
    assert received[0].text == ""
    assert received[0].superseded_by == new["request_id"]
    assert received[0].metadata == {"source": "app"}
    assert (
        await bridge.cancel_request(old["request_id"], superseded_by=new["request_id"]) == cancelled
    )
    status = await bridge.get_message(old["request_id"])
    assert status["reply_status"] == "cancelled"
    assert status["superseded_by"] == new["request_id"]
    assert status["cancelled_at"] == cancelled["received_at"]
    assert (await bridge.get_message(new["request_id"]))["reply_status"] == "pending"
    await bridge.reply_to_app(new["request_id"], "new final")


async def test_new_requests_do_not_implicitly_cancel_independent_requests(bridge):
    old = await bridge.send_message("independent first task", channel="chat")
    new = await bridge.send_message("independent second task", channel="chat")
    assert (await bridge.get_message(old["request_id"]))["reply_status"] == "pending"
    await bridge.reply_to_app(old["request_id"], "first final")
    await bridge.reply_to_app(new["request_id"], "second final")
    assert [message["kind"] for message in await bridge.list_received()] == ["reply", "reply"]


async def test_subscription_cancellation_retains_legacy_status_without_superseded_by(bridge):
    old = await bridge.send_message("request", channel="chat")
    await bridge.store.delete_subscription("sub_chat", bridge.owner_user_id)
    status = await bridge.get_message(old["request_id"])
    assert status["reply_status"] == "cancelled"
    assert "superseded_by" not in status
    assert "cancelled_at" not in status
    assert await bridge.list_response_messages(old["request_id"]) == []


@pytest.mark.parametrize(
    "fields",
    [
        {"request_id": ""},
        {"request_id": " "},
        {"request_id": "x" * 201},
        {"request_id": "\ud800"},
        {"request_id": None},
        {"superseded_by": ""},
        {"superseded_by": " "},
        {"superseded_by": "x" * 201},
        {"superseded_by": "\ud800"},
        {"superseded_by": True},
    ],
)
async def test_cancellation_rejects_invalid_ids_before_storage(bridge, fields):
    with pytest.raises(BridgeError) as caught:
        await bridge.cancel_request(**{"request_id": "old", "superseded_by": "new", **fields})
    assert caught.value.code == "invalid_cancellation"
    assert caught.value.status_code == 422
    assert await bridge.list_received() == []
