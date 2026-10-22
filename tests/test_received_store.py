import asyncio
import json
import time
from uuid import UUID

import pytest
from cryptography.fernet import Fernet

from dots_bridge.errors import BridgeError
from dots_bridge.models import iso8601
from dots_bridge.store import Store

STORAGE_KEY = Fernet.generate_key().decode()


@pytest.fixture
async def store(tmp_path):
    result = Store(tmp_path / "bridge.sqlite3", STORAGE_KEY)
    await result.initialize()
    return result


async def add_request(store, request_id="request-1", *, channel="default", metadata=None):
    now = time.time()
    await store.save_subscription(
        {
            "id": "subscription-1",
            "owner": "owner-1",
            "channel": channel,
            "destination": {"url": "https://callback.example"},
            "expires_at": now + 1000,
            "verified_at": now,
        }
    )
    return await store.insert_message(
        {
            "request_id": request_id,
            "event_id": "event-" + request_id,
            "subscription_id": "subscription-1",
            "channel": channel,
            "text": "request text",
            "metadata": json.dumps(metadata or {}),
            "created_at": now,
            "expires_at": now + 1000,
            "next_attempt_at": now,
            "idempotency_key": None,
            "fingerprint": "request-fingerprint",
            "event": "{}",
        }
    )


async def push(store, key="push-1", *, channel="default", text="hello", metadata=None):
    return await store.save_push(
        text=text,
        channel=channel,
        metadata=metadata or {},
        idempotency_key=key,
        fingerprint=json.dumps([text, channel, metadata], sort_keys=True),
    )


async def test_reply_is_appended_once_with_original_request_context(store):
    await add_request(store, channel="chat-1", metadata={"avatar": "ミント"})
    original = await store.save_reply("request-1", "reply text")
    repeated = await store.save_reply("request-1", "reply text")
    assert original == repeated
    received = await store.list_received()
    assert len(received) == 1
    assert received[0] == {
        "sequence": 1,
        "id": received[0]["id"],
        "kind": "reply",
        "request_id": "request-1",
        "channel": "chat-1",
        "text": "reply text",
        "metadata": {"avatar": "ミント"},
        "received_at": iso8601(original["replied_at"]),
    }
    UUID(received[0]["id"])
    with pytest.raises(BridgeError, match="different final reply"):
        await store.save_reply("request-1", "different")
    assert await store.list_received() == received


async def test_concurrent_identical_replies_append_only_one_receipt(store):
    await add_request(store)
    results = await asyncio.gather(*(store.save_reply("request-1", "hello") for _ in range(8)))
    assert all(result == results[0] for result in results)
    assert len(await store.list_received()) == 1


async def test_reply_and_receipt_are_saved_atomically(store, monkeypatch):
    await add_request(store)

    def fail_append(db, row):
        raise RuntimeError("append failed")

    monkeypatch.setattr(store, "_append_reply", fail_append)
    with pytest.raises(RuntimeError, match="append failed"):
        await store.save_reply("request-1", "hello")
    assert (await store.message("request-1"))["reply"] is None
    assert await store.list_received() == []


async def test_push_requires_no_subscription_or_pending_request(store):
    received = await push(store, metadata={"source": "dots"})
    assert received["kind"] == "push"
    assert received["request_id"] is None
    assert received["metadata"] == {"source": "dots"}
    UUID(received["id"])
    assert await store.list_received() == [received]


async def test_concurrent_idempotent_pushes_share_original_receipt(store):
    received = await asyncio.gather(*(push(store) for _ in range(8)))
    assert all(item == received[0] for item in received)
    assert await store.list_received() == [received[0]]


async def test_push_key_conflict_does_not_append(store):
    original = await push(store)
    with pytest.raises(BridgeError) as caught:
        await push(store, text="different")
    assert caught.value.code == "idempotency_conflict"
    assert caught.value.status_code == 409
    assert await store.list_received() == [original]


async def test_stream_orders_replies_and_pushes_with_channel_pagination(store):
    first = await push(store, "push-1", channel="one")
    await add_request(store, channel="two")
    await store.save_reply("request-1", "reply")
    third = await push(store, "push-2", channel="one")
    all_received = await store.list_received()
    assert [item["kind"] for item in all_received] == ["push", "reply", "push"]
    assert await store.list_received(limit=1) == [first]
    assert await store.list_received(after=first["sequence"], limit=1) == [all_received[1]]
    assert await store.list_received(channel="one") == [first, third]
    assert await store.list_received(channel="one", after=first["sequence"]) == [third]
    assert await store.list_received(after=third["sequence"]) == []
    assert await store.latest_received_sequence() == third["sequence"]
    assert await store.latest_received_sequence(channel="two") == all_received[1]["sequence"]
    assert await store.latest_received_sequence(channel="missing") == 0


async def test_consumer_checkpoint_survives_restart_and_advances_monotonically(store):
    first = await push(store, "push-1")
    second = await push(store, "push-2")
    assert await store.get_consumer_cursor("avatar") == 0
    assert await store.acknowledge_received("avatar", second["sequence"]) == second["sequence"]
    assert await store.acknowledge_received("avatar", first["sequence"]) == second["sequence"]
    reopened = Store(store.path, STORAGE_KEY)
    await reopened.initialize()
    assert await reopened.get_consumer_cursor("avatar") == second["sequence"]
    assert await reopened.get_consumer_cursor("another-app") == 0


@pytest.mark.parametrize("original,changed", [(None, "chat"), ("chat", None), ("one", "two")])
async def test_consumer_channel_filter_cannot_change(store, original, changed):
    assert await store.get_consumer_cursor("avatar", original) == 0
    for operation in (
        store.get_consumer_cursor("avatar", changed),
        store.acknowledge_received("avatar", 0, changed),
    ):
        with pytest.raises(BridgeError) as caught:
            await operation
        assert caught.value.code == "consumer_channel_conflict"
        assert caught.value.status_code == 409


async def test_acknowledgement_rejects_future_or_wrong_channel_sequence(store):
    received = await push(store, channel="one")
    for sequence, channel in [(999, None), (received["sequence"], "two")]:
        with pytest.raises(BridgeError) as caught:
            await store.acknowledge_received("avatar", sequence, channel)
        assert caught.value.code == "invalid_received_cursor"
    assert await store.get_consumer_cursor("avatar") == 0
    assert await store.acknowledge_received("channel-one", received["sequence"], "one") == 1


@pytest.mark.parametrize("sequence", [-1, True, 1.5, "1"])
async def test_acknowledgement_rejects_invalid_sequence_types(store, sequence):
    with pytest.raises(BridgeError) as caught:
        await store.acknowledge_received("avatar", sequence)
    assert caught.value.code == "invalid_received_cursor"


@pytest.mark.parametrize("schema_version", [b"1", b"2"])
async def test_migration_backfills_replies_once_in_deterministic_order(store, schema_version):
    for request_id in ("b", "c", "a"):
        await add_request(store, request_id, metadata={"request": request_id})

    def legacy_database(db):
        db.execute("DROP TABLE received_messages")
        db.execute("DROP TABLE received_consumers")
        if schema_version == b"1":
            db.execute("ALTER TABLE messages DROP COLUMN reply_cancelled")
        db.execute("UPDATE bridge_meta SET value=? WHERE key='schema'", (schema_version,))
        db.execute(
            "UPDATE messages SET reply='legacy', replied_at=100 WHERE request_id IN ('a','b')"
        )
        db.execute("UPDATE messages SET reply='latest', replied_at=200 WHERE request_id='c'")

    await store.run(legacy_database)
    store._ready = False
    await store.initialize()
    migrated = await store.list_received()
    assert [item["request_id"] for item in migrated] == ["a", "b", "c"]
    assert [item["received_at"] for item in migrated] == [iso8601(100), iso8601(100), iso8601(200)]
    assert migrated[0]["metadata"] == {"request": "a"}
    assert (
        await store.run(
            lambda db: db.execute("SELECT value FROM bridge_meta WHERE key='schema'").fetchone()[0]
        )
        == b"4"
    )
    store._ready = False
    await store.initialize()
    assert await store.list_received() == migrated


async def test_pruning_requests_preserves_receipts_and_checkpoints(store):
    await add_request(store)
    await store.save_reply("request-1", "hello")
    receipt = (await store.list_received())[0]
    await store.acknowledge_received("avatar", receipt["sequence"])
    await store.run(
        lambda db: db.execute("UPDATE messages SET delivery_status='delivered', expires_at=0")
    )
    assert await store.prune(time.time()) == 1
    assert await store.list_received() == [receipt]
    assert await store.get_consumer_cursor("avatar") == receipt["sequence"]
