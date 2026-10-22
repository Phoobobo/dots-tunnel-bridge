"""Explicit request replacement is durable and atomic with final replies."""

import asyncio
import json
import time

import pytest
from cryptography.fernet import Fernet

from dots_bridge.errors import BridgeError
from dots_bridge.models import CancellationInput, ReceivedMessage
from dots_bridge.store import Store, public_message

STORAGE_KEY = Fernet.generate_key().decode()
OLD_RECEIVED_SCHEMA = """
CREATE TABLE received_messages (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('reply', 'progress', 'push')), request_id TEXT,
    channel TEXT NOT NULL, text TEXT NOT NULL, metadata TEXT NOT NULL,
    received_at REAL NOT NULL, idempotency_key TEXT, fingerprint TEXT NOT NULL
)
"""


@pytest.fixture
async def store(tmp_path):
    result = Store(tmp_path / "bridge.sqlite3", STORAGE_KEY)
    await result.initialize()
    return result


async def add_request(
    store, request_id, *, channel="chat", subscription="sub_chat", created_at=None, metadata=None
):
    now = time.time()
    await store.save_subscription(
        {
            "id": subscription,
            "owner": "owner",
            "channel": channel,
            "destination": {"url": "https://callback.example"},
            "expires_at": now + 1000,
            "verified_at": now,
        }
    )
    await store.insert_message(
        {
            "request_id": request_id,
            "event_id": "event_" + request_id,
            "subscription_id": subscription,
            "channel": channel,
            "text": "question",
            "metadata": json.dumps(metadata or {}, ensure_ascii=False),
            "created_at": now if created_at is None else created_at,
            "expires_at": now + 1000,
            "next_attempt_at": now,
            "idempotency_key": None,
            "fingerprint": "request-fingerprint",
            "event": "{}",
        }
    )


async def pair(store):
    await add_request(store, "old", created_at=100, metadata={"source": "アプリ"})
    await add_request(store, "new", created_at=101)


async def test_cancellation_persists_context_and_preserves_normal_public_shapes(store):
    await pair(store)
    progress = await store.save_progress("old", "step", idempotency_key="one")
    normal = public_message(await store.message("new"))
    assert "superseded_by" not in normal
    assert "cancelled_at" not in normal
    assert ReceivedMessage.model_validate(progress).model_dump() == progress
    cancelled = await store.cancel_request("old", superseded_by="new")
    assert cancelled["kind"] == "cancel"
    assert cancelled["text"] == ""
    assert cancelled["request_id"] == "old"
    assert cancelled["superseded_by"] == "new"
    assert cancelled["channel"] == "chat"
    assert cancelled["metadata"] == {"source": "アプリ"}
    assert ReceivedMessage.model_validate(cancelled).model_dump() == cancelled
    assert await store.list_response_messages("old") == [progress, cancelled]
    message = public_message(await store.message("old"))
    assert message["reply_status"] == "cancelled"
    assert message["reply"] is None
    assert message["superseded_by"] == "new"
    assert message["cancelled_at"] == cancelled["received_at"]
    assert message["delivery_status"] == "cancelled"
    assert public_message(await store.message("new")) == normal


async def test_cancelled_requests_block_new_progress_and_final_but_allow_stable_retries(store):
    await pair(store)
    progress = await store.save_progress("old", "step", idempotency_key="one")
    cancelled = await store.cancel_request("old", superseded_by="new")
    await store.run(lambda db: db.execute("UPDATE messages SET expires_at=0"))
    assert await store.cancel_request("old", superseded_by="new") == cancelled
    assert await store.save_progress("old", "step", idempotency_key="one") == progress
    for operation in (
        store.save_progress("old", "new step", idempotency_key="two"),
        store.save_reply("old", "late final"),
    ):
        with pytest.raises(BridgeError) as caught:
            await operation
        assert caught.value.code == "message_inactive"
        assert caught.value.status_code == 409
    assert await store.list_response_messages("old") == [progress, cancelled]


async def test_concurrent_cancel_retries_create_one_terminal_receipt(store):
    await pair(store)
    receipts = await asyncio.gather(
        *(store.cancel_request("old", superseded_by="new") for _ in range(8))
    )
    assert all(receipt == receipts[0] for receipt in receipts)
    assert await store.list_response_messages("old") == [receipts[0]]


async def test_different_replacement_conflicts_without_overwriting_original(store):
    await pair(store)
    await add_request(store, "newer", created_at=102)
    original = await store.cancel_request("old", superseded_by="new")
    with pytest.raises(BridgeError) as caught:
        await store.cancel_request("old", superseded_by="newer")
    assert caught.value.code == "cancellation_conflict"
    assert await store.list_response_messages("old") == [original]


@pytest.mark.parametrize(
    ("old", "replacement", "code", "status"),
    [
        ("missing", "new", "message_not_found", 404),
        ("old", "missing", "replacement_not_found", 404),
        ("old", "old", "invalid_replacement", 409),
        ("new", "old", "invalid_replacement", 409),
    ],
)
async def test_cancellation_requires_a_real_distinct_newer_request(
    store, old, replacement, code, status
):
    await pair(store)
    with pytest.raises(BridgeError) as caught:
        await store.cancel_request(old, superseded_by=replacement)
    assert caught.value.code == code
    assert caught.value.status_code == status
    assert await store.list_received() == []


async def test_equal_timestamps_use_insertion_order_to_identify_newer_request(store):
    await add_request(store, "first", created_at=100)
    await add_request(store, "second", created_at=100)
    with pytest.raises(BridgeError) as caught:
        await store.cancel_request("second", superseded_by="first")
    assert caught.value.code == "invalid_replacement"
    assert (await store.cancel_request("first", superseded_by="second"))[
        "superseded_by"
    ] == "second"


@pytest.mark.parametrize("changes", [{"channel": "other"}, {"subscription": "sub_other"}])
async def test_replacement_must_match_channel_and_subscription(store, changes):
    await add_request(store, "old", created_at=100)
    await add_request(store, "new", created_at=101, **changes)
    with pytest.raises(BridgeError) as caught:
        await store.cancel_request("old", superseded_by="new")
    assert caught.value.code == "invalid_replacement"


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE messages SET expires_at=0 WHERE request_id='new'",
        "UPDATE messages SET reply_cancelled=1 WHERE request_id='new'",
        "UPDATE messages SET delivery_status='failed' WHERE request_id='new'",
        "UPDATE messages SET delivery_status='cancelled' WHERE request_id='new'",
        "UPDATE messages SET superseded_by='another' WHERE request_id='new'",
    ],
)
async def test_replacement_must_still_be_active(store, sql):
    await pair(store)
    await store.run(lambda db: db.execute(sql))
    with pytest.raises(BridgeError) as caught:
        await store.cancel_request("old", superseded_by="new")
    assert caught.value.code == "invalid_replacement"
    assert await store.list_response_messages("old") == []


async def test_replacement_may_already_have_a_final_reply(store):
    await pair(store)
    final = await store.save_reply("new", "new answer")
    assert (await store.cancel_request("old", superseded_by="new"))["kind"] == "cancel"
    assert await store.save_reply("new", "new answer") == final


async def test_finished_request_cannot_be_cancelled(store):
    await pair(store)
    original = await store.save_reply("old", "final")
    with pytest.raises(BridgeError) as caught:
        await store.cancel_request("old", superseded_by="new")
    assert caught.value.code == "reply_completed"
    assert await store.save_reply("old", "final") == original
    assert [receipt["kind"] for receipt in await store.list_response_messages("old")] == ["reply"]


async def test_final_and_cancel_race_commit_exactly_one_terminal_state(store):
    await pair(store)
    final, cancelled = await asyncio.gather(
        store.save_reply("old", "final"),
        store.cancel_request("old", superseded_by="new"),
        return_exceptions=True,
    )
    assert isinstance(final, BridgeError) != isinstance(cancelled, BridgeError)
    if isinstance(final, BridgeError):
        assert final.code == "message_inactive"
    else:
        assert cancelled.code == "reply_completed"
    receipts = await store.list_response_messages("old")
    assert len(receipts) == 1
    assert receipts[0]["kind"] in {"reply", "cancel"}


async def test_receipt_insert_failure_rolls_back_cancellation_state(store):
    await pair(store)
    await store.run(
        lambda db: db.execute(
            "CREATE TRIGGER reject_cancel BEFORE INSERT ON received_messages "
            "WHEN NEW.kind='cancel' BEGIN SELECT RAISE(ABORT, 'cancel rejected'); END"
        )
    )
    with pytest.raises(BridgeError) as caught:
        await store.cancel_request("old", superseded_by="new")
    assert caught.value.code == "storage_error"
    old = await store.message("old")
    assert old["superseded_by"] is None
    assert old["cancelled_at"] is None
    assert old["reply_cancelled"] == 0
    assert old["delivery_status"] == "queued"
    assert await store.list_response_messages("old") == []


async def downgrade_to_schema_four(store):
    def downgrade(db):
        db.execute("BEGIN IMMEDIATE")
        db.execute("ALTER TABLE received_messages RENAME TO received_messages_five")
        db.execute(OLD_RECEIVED_SCHEMA)
        db.execute(
            "INSERT INTO received_messages SELECT sequence, id, kind, request_id, channel, "
            "text, metadata, received_at, idempotency_key, fingerprint FROM received_messages_five"
        )
        db.execute("DROP TABLE received_messages_five")
        db.execute("ALTER TABLE messages DROP COLUMN superseded_by")
        db.execute("ALTER TABLE messages DROP COLUMN cancelled_at")
        db.execute("UPDATE sqlite_sequence SET seq=100 WHERE name='received_messages'")
        db.execute("UPDATE bridge_meta SET value=? WHERE key='schema'", (b"4",))

    await store.run(downgrade)
    store._ready = False


async def test_schema_four_migration_preserves_receipts_cursors_and_creation_order(store):
    await pair(store)
    progress = await store.save_progress("old", "step", idempotency_key="one")
    await store.save_reply("new", "new final")
    before = await store.list_received()
    await store.acknowledge_received("consumer", before[-1]["sequence"], channel="chat")
    await downgrade_to_schema_four(store)
    await store.initialize()
    assert await store.list_received() == before
    assert await store.get_consumer_cursor("consumer", channel="chat") == before[-1]["sequence"]
    cancelled = await store.cancel_request("old", superseded_by="new")
    assert cancelled["sequence"] == 101
    assert await store.list_response_messages("old") == [progress, cancelled]
    assert (
        await store.run(
            lambda db: db.execute("SELECT value FROM bridge_meta WHERE key='schema'").fetchone()[0]
        )
        == b"5"
    )


async def test_schema_five_migration_failure_rolls_back_added_message_columns(store, monkeypatch):
    await pair(store)
    await store.save_progress("old", "step", idempotency_key="one")
    before = await store.list_received()
    await downgrade_to_schema_four(store)
    original = store._migrate_received_messages

    def fail(db):
        original(db)
        raise RuntimeError("interrupted")

    with monkeypatch.context() as patch:
        patch.setattr(store, "_migrate_received_messages", fail)
        with pytest.raises(RuntimeError, match="interrupted"):
            await store.initialize()
    columns = await store.run(
        lambda db: [row[1] for row in db.execute("PRAGMA table_info(messages)")]
    )
    assert "superseded_by" not in columns
    assert "cancelled_at" not in columns
    assert await store.list_received() == before
    assert (
        await store.run(
            lambda db: db.execute("SELECT value FROM bridge_meta WHERE key='schema'").fetchone()[0]
        )
        == b"4"
    )
    await store.initialize()
    assert (await store.cancel_request("old", superseded_by="new"))["kind"] == "cancel"


@pytest.mark.parametrize("field", ["request_id", "superseded_by"])
@pytest.mark.parametrize("value", ["", " ", "x" * 201, "\ud800", None, True])
def test_cancellation_input_rejects_invalid_request_ids(field, value):
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        CancellationInput.model_validate(
            {"request_id": "old", "superseded_by": "new", field: value}
        )
