import asyncio
import json
import time
from uuid import UUID

import pytest
from cryptography.fernet import Fernet
from pydantic import ValidationError

from dots_bridge.errors import BridgeError
from dots_bridge.models import ProgressInput, ReceivedMessage
from dots_bridge.store import Store

STORAGE_KEY = Fernet.generate_key().decode()
LEGACY_RECEIVED_SCHEMA = """
CREATE TABLE received_messages (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('reply', 'push')), request_id TEXT UNIQUE,
    channel TEXT NOT NULL, text TEXT NOT NULL, metadata TEXT NOT NULL,
    received_at REAL NOT NULL, idempotency_key TEXT UNIQUE, fingerprint TEXT NOT NULL
)
"""


@pytest.fixture
async def store(tmp_path):
    result = Store(tmp_path / "bridge.sqlite3", STORAGE_KEY)
    await result.initialize()
    return result


async def add_request(store, request_id="req_one", *, channel="chat", metadata=None):
    now = time.time()
    await store.save_subscription(
        {
            "id": "sub_one",
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
            "subscription_id": "sub_one",
            "channel": channel,
            "text": "question",
            "metadata": json.dumps(metadata or {}, ensure_ascii=False),
            "created_at": now,
            "expires_at": now + 1000,
            "next_attempt_at": now,
            "idempotency_key": None,
            "fingerprint": "request-fingerprint",
            "event": "{}",
        }
    )


async def add_push(store, key="key"):
    return await store.save_push(
        text="push", channel="chat", metadata={}, idempotency_key=key, fingerprint="push"
    )


async def schema_version(store):
    return await store.run(
        lambda db: db.execute("SELECT value FROM bridge_meta WHERE key='schema'").fetchone()[0]
    )


async def make_schema_three(store):
    def downgrade(db):
        db.execute("BEGIN IMMEDIATE")
        sequence = db.execute(
            "SELECT seq FROM sqlite_sequence WHERE name='received_messages'"
        ).fetchone()
        db.execute("ALTER TABLE received_messages RENAME TO received_messages_four")
        db.execute(LEGACY_RECEIVED_SCHEMA)
        db.execute(
            "INSERT INTO received_messages SELECT sequence, id, kind, request_id, channel, "
            "text, metadata, received_at, idempotency_key, fingerprint FROM received_messages_four"
        )
        db.execute("DROP TABLE received_messages_four")
        db.execute("CREATE INDEX received_messages_channel ON received_messages(channel, sequence)")
        if sequence:
            db.execute(
                "UPDATE sqlite_sequence SET seq=? WHERE name='received_messages'", (sequence[0],)
            )
        db.execute("UPDATE bridge_meta SET value=? WHERE key='schema'", (b"3",))

    await store.run(downgrade)
    store._ready = False


async def test_progress_preserves_context_and_leaves_final_reply_pending(store):
    await add_request(store, metadata={"avatar": "ミント"})
    progress = await store.save_progress("req_one", "調査中です", idempotency_key="step-1")
    UUID(progress["id"])
    assert ReceivedMessage.model_validate(progress).kind == "progress"
    assert progress["request_id"] == "req_one"
    assert progress["channel"] == "chat"
    assert progress["metadata"] == {"avatar": "ミント"}
    assert (await store.message("req_one"))["reply"] is None
    assert await store.list_response_messages("req_one") == [progress]
    assert await store.list_received() == [progress]
    assert await schema_version(store) == b"5"


async def test_progress_and_final_replay_in_order_with_request_pagination(store):
    await add_request(store)
    await add_request(store, "req_other")
    first = await store.save_progress("req_one", "one", idempotency_key="1")
    await add_push(store)
    await store.save_progress("req_other", "another request", idempotency_key="1")
    second = await store.save_progress("req_one", "two", idempotency_key="2")
    final = await store.save_reply("req_one", "done")
    messages = await store.list_response_messages("req_one")
    assert [item["kind"] for item in messages] == ["progress", "progress", "reply"]
    assert [item["text"] for item in messages] == ["one", "two", "done"]
    assert messages[:2] == [first, second]
    assert await store.list_response_messages("req_one", limit=1) == [first]
    assert await store.list_response_messages("req_one", after=first["sequence"], limit=1) == [
        second
    ]
    assert await store.list_response_messages("req_one", after=second["sequence"]) == [messages[-1]]
    assert await store.save_reply("req_one", "done") == final
    assert await store.list_response_messages("req_one") == messages


async def test_progress_idempotency_is_scoped_to_request_and_separate_from_push(store):
    await add_request(store)
    await add_request(store, "req_other")
    first = await store.save_progress("req_one", "one", idempotency_key="shared")
    assert await store.save_progress("req_one", "one", idempotency_key="shared") == first
    second = await store.save_progress("req_other", "two", idempotency_key="shared")
    push = await add_push(store, "shared")
    assert second["id"] != first["id"] != push["id"]
    assert await add_push(store, "shared") == push
    with pytest.raises(BridgeError) as caught:
        await store.save_progress("req_one", "changed", idempotency_key="shared")
    assert caught.value.code == "idempotency_conflict"
    assert caught.value.status_code == 409
    assert len(await store.list_received()) == 3


async def test_same_text_with_different_progress_keys_represents_distinct_steps(store):
    await add_request(store)
    first = await store.save_progress("req_one", "same", idempotency_key="one")
    second = await store.save_progress("req_one", "same", idempotency_key="two")
    assert first["id"] != second["id"]
    assert second["sequence"] > first["sequence"]


async def test_saved_progress_retry_succeeds_after_final_but_new_progress_is_closed(store):
    await add_request(store)
    progress = await store.save_progress("req_one", "step", idempotency_key="one")
    await store.save_reply("req_one", "done")
    await store.run(lambda db: db.execute("UPDATE messages SET expires_at=0"))
    assert await store.save_progress("req_one", "step", idempotency_key="one") == progress
    with pytest.raises(BridgeError) as conflict:
        await store.save_progress("req_one", "changed", idempotency_key="one")
    assert conflict.value.code == "idempotency_conflict"
    with pytest.raises(BridgeError) as closed:
        await store.save_progress("req_one", "new step", idempotency_key="two")
    assert closed.value.code == "reply_completed"
    assert closed.value.status_code == 409
    assert len(await store.list_response_messages("req_one")) == 2


@pytest.mark.parametrize(
    ("sql", "code", "status"),
    [
        ("UPDATE messages SET expires_at=0", "reply_expired", 410),
        ("DELETE FROM subscriptions", "message_inactive", 409),
        ("UPDATE subscriptions SET expires_at=0", "message_inactive", 409),
        ("UPDATE messages SET reply_cancelled=1", "message_inactive", 409),
        ("UPDATE messages SET delivery_status='failed'", "message_inactive", 409),
        ("UPDATE messages SET delivery_status='cancelled'", "message_inactive", 409),
    ],
)
async def test_new_progress_rejects_expired_or_inactive_requests(store, sql, code, status):
    await add_request(store)
    existing = await store.save_progress("req_one", "existing", idempotency_key="existing")
    await store.run(lambda db: db.execute(sql))
    assert await store.save_progress("req_one", "existing", idempotency_key="existing") == existing
    with pytest.raises(BridgeError) as caught:
        await store.save_progress("req_one", "late", idempotency_key="new")
    assert caught.value.code == code
    assert caught.value.status_code == status
    assert await store.list_response_messages("req_one") == [existing]


async def test_progress_and_response_listing_reject_missing_or_pruned_requests(store):
    with pytest.raises(BridgeError) as missing:
        await store.save_progress("missing", "step", idempotency_key="one")
    assert missing.value.status_code == 404
    await add_request(store)
    progress = await store.save_progress("req_one", "step", idempotency_key="one")
    await store.run(
        lambda db: db.execute("UPDATE messages SET delivery_status='accepted', expires_at=0")
    )
    assert await store.prune(time.time()) == 1
    with pytest.raises(BridgeError) as pruned:
        await store.list_response_messages("req_one")
    assert pruned.value.code == "message_not_found"
    assert pruned.value.status_code == 404
    assert await store.list_received() == [progress]


async def test_concurrent_progress_retries_append_once(store):
    await add_request(store)
    results = await asyncio.gather(
        *(store.save_progress("req_one", "step", idempotency_key="one") for _ in range(8))
    )
    assert all(result == results[0] for result in results)
    assert await store.list_response_messages("req_one") == [results[0]]


async def test_final_and_progress_race_never_appends_progress_after_final(store):
    await add_request(store)
    results = await asyncio.gather(
        store.save_reply("req_one", "final"),
        *(
            store.save_progress("req_one", f"step {index}", idempotency_key=str(index))
            for index in range(8)
        ),
        return_exceptions=True,
    )
    assert not isinstance(results[0], BaseException)
    for result in results[1:]:
        if isinstance(result, BaseException):
            assert isinstance(result, BridgeError)
            assert result.code == "reply_completed"
    responses = await store.list_response_messages("req_one")
    assert responses[-1]["kind"] == "reply"
    assert len([item for item in responses if item["kind"] == "reply"]) == 1


async def test_schema_three_migration_preserves_receipts_cursors_and_sequence_highwater(store):
    await add_request(store)
    pushed = await add_push(store)
    await store.save_reply("req_one", "old final")
    before = await store.list_received()
    await store.acknowledge_received("consumer", before[-1]["sequence"], channel="chat")
    await store.run(
        lambda db: db.execute("UPDATE sqlite_sequence SET seq=100 WHERE name='received_messages'")
    )
    await make_schema_three(store)
    await store.initialize()
    assert await schema_version(store) == b"5"
    assert await store.list_received() == before
    assert await store.get_consumer_cursor("consumer", channel="chat") == before[-1]["sequence"]
    assert await add_push(store) == pushed
    await add_request(store, "req_new")
    progress = await store.save_progress("req_new", "migrated step", idempotency_key="key")
    assert progress["sequence"] == 101
    await store.save_reply("req_new", "new final")
    assert [item["kind"] for item in await store.list_response_messages("req_new")] == [
        "progress",
        "reply",
    ]


async def test_schema_three_empty_stream_keeps_autoincrement_highwater(store):
    await add_request(store)
    pushed = await add_push(store)
    await store.acknowledge_received("consumer", pushed["sequence"])
    await store.run(lambda db: db.execute("DELETE FROM received_messages"))
    await make_schema_three(store)
    await store.initialize()
    assert await store.list_received() == []
    assert await store.get_consumer_cursor("consumer") == pushed["sequence"]
    progress = await store.save_progress("req_one", "step", idempotency_key="one")
    assert progress["sequence"] > pushed["sequence"]


async def test_wrong_storage_key_does_not_migrate_legacy_receipts(store):
    await add_request(store)
    await store.save_reply("req_one", "old final")
    before = await store.list_received()
    await make_schema_three(store)
    wrong_key = Store(store.path, Fernet.generate_key().decode())
    with pytest.raises(BridgeError) as caught:
        await wrong_key.initialize()
    assert caught.value.code == "storage_key_mismatch"
    assert await schema_version(store) == b"3"
    assert await store.list_received() == before


@pytest.mark.parametrize("version", [b"1", b"2"])
async def test_pre_stream_migration_backfills_final_replies_into_current_schema(store, version):
    await add_request(store)
    await store.save_reply("req_one", "old final")

    def downgrade(db):
        db.execute("DROP TABLE received_messages")
        db.execute("DROP TABLE received_consumers")
        if version == b"1":
            db.execute("ALTER TABLE messages DROP COLUMN reply_cancelled")
        db.execute("UPDATE bridge_meta SET value=? WHERE key='schema'", (version,))

    await store.run(downgrade)
    store._ready = False
    await store.initialize()
    assert await schema_version(store) == b"5"
    responses = await store.list_response_messages("req_one")
    assert len(responses) == 1
    assert responses[0]["kind"] == "reply"
    assert responses[0]["text"] == "old final"
    await add_request(store, "req_new")
    await store.save_progress("req_new", "step", idempotency_key="one")
    await store.save_reply("req_new", "done")


async def test_migration_failure_rolls_back_table_rebuild_and_schema_version(store, monkeypatch):
    await add_request(store)
    await store.save_reply("req_one", "old final")
    await store.acknowledge_received("consumer", 1, channel="chat")
    before = await store.list_received()
    await make_schema_three(store)
    migrate = store._migrate_received_messages

    def fail_after_rebuild(db):
        migrate(db)
        raise RuntimeError("interrupted migration")

    with monkeypatch.context() as patch:
        patch.setattr(store, "_migrate_received_messages", fail_after_rebuild)
        with pytest.raises(RuntimeError, match="interrupted migration"):
            await store.initialize()
    assert not store._ready
    assert await schema_version(store) == b"3"
    assert await store.list_received() == before
    assert await store.get_consumer_cursor("consumer", channel="chat") == 1
    old_sql = await store.run(
        lambda db: db.execute(
            "SELECT sql FROM sqlite_master WHERE name='received_messages'"
        ).fetchone()[0]
    )
    assert "request_id TEXT UNIQUE" in old_sql
    assert (
        await store.run(
            lambda db: db.execute(
                "SELECT 1 FROM sqlite_master WHERE name='received_messages_legacy'"
            ).fetchone()
        )
        is None
    )
    await store.initialize()
    assert await schema_version(store) == b"5"
    assert await store.list_received() == before


async def test_restart_backfills_missing_final_even_if_progress_already_exists(store):
    await add_request(store)
    progress = await store.save_progress("req_one", "step", idempotency_key="one")
    await store.run(
        lambda db: db.execute(
            "UPDATE messages SET reply='final', replied_at=? WHERE request_id='req_one'",
            (time.time(),),
        )
    )
    store._ready = False
    await store.initialize()
    expected = await store.list_response_messages("req_one")
    assert expected[0] == progress
    assert expected[1]["kind"] == "reply"
    store._ready = False
    await store.initialize()
    assert await store.list_response_messages("req_one") == expected


@pytest.mark.parametrize(
    "fields",
    [
        {"request_id": " "},
        {"request_id": "x" * 201},
        {"text": " "},
        {"text": "x" * 64001},
        {"text": "\ud800"},
        {"idempotency_key": " "},
        {"idempotency_key": "x" * 129},
        {"idempotency_key": "\ud800"},
        {"request_id": "\ud800"},
        {"extra": True},
        {"text": 123},
    ],
)
def test_progress_input_rejects_invalid_fields(fields):
    with pytest.raises(ValidationError):
        ProgressInput.model_validate(
            {"request_id": "req", "text": "step", "idempotency_key": "one"} | fields
        )
