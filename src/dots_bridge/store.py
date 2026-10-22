"""Short SQLite transactions; network calls never run inside a transaction."""

import asyncio
import hashlib
import json
import os
import sqlite3
import time
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any, TypeVar
from uuid import uuid4

from cryptography.fernet import Fernet, InvalidToken

from .errors import BridgeError
from .models import iso8601

T = TypeVar("T")

RECEIVED_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS received_messages (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL,
    kind TEXT NOT NULL CHECK(kind IN ('reply', 'progress', 'push')), request_id TEXT,
    channel TEXT NOT NULL, text TEXT NOT NULL, metadata TEXT NOT NULL,
    received_at REAL NOT NULL, idempotency_key TEXT, fingerprint TEXT NOT NULL
);
"""

RECEIVED_INDEX_SCHEMA = """
CREATE INDEX IF NOT EXISTS received_messages_channel ON received_messages(channel, sequence);
CREATE INDEX IF NOT EXISTS received_messages_request ON received_messages(request_id, sequence);
CREATE UNIQUE INDEX IF NOT EXISTS received_messages_reply ON received_messages(request_id)
    WHERE kind='reply';
CREATE UNIQUE INDEX IF NOT EXISTS received_messages_push_key ON received_messages(idempotency_key)
    WHERE kind='push';
CREATE UNIQUE INDEX IF NOT EXISTS received_messages_progress_key
    ON received_messages(request_id, idempotency_key) WHERE kind='progress';
"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS bridge_meta (key TEXT PRIMARY KEY, value BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS subscriptions (
    id TEXT PRIMARY KEY, owner TEXT NOT NULL, channel TEXT NOT NULL,
    destination BLOB NOT NULL, expires_at REAL NOT NULL, verified_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
    request_id TEXT PRIMARY KEY, event_id TEXT NOT NULL UNIQUE,
    subscription_id TEXT NOT NULL, channel TEXT NOT NULL, text TEXT NOT NULL,
    metadata TEXT NOT NULL, created_at REAL NOT NULL, expires_at REAL NOT NULL,
    delivery_status TEXT NOT NULL DEFAULT 'queued', attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL, lease_until REAL, status_code INTEGER, error TEXT,
    reply TEXT, replied_at REAL, reply_cancelled INTEGER NOT NULL DEFAULT 0,
    idempotency_key TEXT UNIQUE, fingerprint TEXT NOT NULL,
    event TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS messages_pending ON messages(delivery_status, next_attempt_at);
CREATE TABLE IF NOT EXISTS received_consumers (
    consumer_id TEXT PRIMARY KEY, channel TEXT, cursor INTEGER NOT NULL DEFAULT 0
);
"""


class Store:
    def __init__(self, path: Path, storage_key: str):
        self.path = Path(path)
        self.cipher = Fernet(storage_key.encode())
        self._ready = False
        self._lock = asyncio.Lock()

    def _execute(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.path.is_symlink():
            raise BridgeError("storage_error", "Database must not be a symbolic link.", 500)
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        os.close(fd)
        with closing(sqlite3.connect(self.path, timeout=10)) as db, db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA busy_timeout=10000")
            return operation(db)

    async def run(self, operation: Callable[[sqlite3.Connection], T]) -> T:
        try:
            return await asyncio.to_thread(self._execute, operation)
        except (sqlite3.Error, OSError):
            raise BridgeError(
                "storage_error", "Could not access the bridge database.", 500
            ) from None

    @staticmethod
    def _enable_wal(db: sqlite3.Connection) -> None:
        # Concurrent first opens can fail the journal-mode lock upgrade without
        # waiting for busy_timeout. Retry only this idempotent setup statement.
        deadline = time.monotonic() + 1
        while True:
            try:
                db.execute("PRAGMA journal_mode=WAL")
                return
            except sqlite3.OperationalError as error:
                if getattr(error, "sqlite_errorcode", None) != sqlite3.SQLITE_BUSY:
                    raise
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise
                time.sleep(min(0.01, remaining))
                if time.monotonic() >= deadline:
                    raise

    @staticmethod
    def _migrate_received_messages(db: sqlite3.Connection) -> None:
        # Rebuilding removes schema 3's request-wide uniqueness while preserving
        # all public receipt identities and the AUTOINCREMENT high-water mark.
        sequence = db.execute(
            "SELECT seq FROM sqlite_sequence WHERE name='received_messages'"
        ).fetchone()
        db.execute("ALTER TABLE received_messages RENAME TO received_messages_legacy")
        db.execute(RECEIVED_TABLE_SCHEMA)
        db.execute("INSERT INTO received_messages SELECT * FROM received_messages_legacy")
        db.execute("DROP TABLE received_messages_legacy")
        if sequence:
            updated = db.execute(
                "UPDATE sqlite_sequence SET seq=MAX(seq, ?) WHERE name='received_messages'",
                (sequence[0],),
            )
            if not updated.rowcount:
                db.execute(
                    "INSERT INTO sqlite_sequence (name, seq) VALUES ('received_messages', ?)",
                    (sequence[0],),
                )

    async def initialize(self):
        async with self._lock:
            if self._ready:
                return

            def setup(db):
                self._enable_wal(db)
                db.execute("BEGIN IMMEDIATE")
                had_received = db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='received_messages'"
                ).fetchone()
                # executescript commits an existing transaction, so execute the
                # static schema statements individually to keep migrations atomic.
                for statement in SCHEMA.split(";"):
                    if statement.strip():
                        db.execute(statement)
                version = db.execute("SELECT value FROM bridge_meta WHERE key='schema'").fetchone()
                if version and version[0] not in (b"1", b"2", b"3", b"4"):
                    raise BridgeError(
                        "storage_version", "Unsupported database schema version.", 500
                    )
                if version and version[0] == b"1":
                    columns = {row[1] for row in db.execute("PRAGMA table_info(messages)")}
                    if "reply_cancelled" not in columns:
                        db.execute(
                            "ALTER TABLE messages ADD COLUMN "
                            "reply_cancelled INTEGER NOT NULL DEFAULT 0"
                        )
                check = db.execute("SELECT value FROM bridge_meta WHERE key='key_check'").fetchone()
                if check:
                    try:
                        if self.cipher.decrypt(check[0]) != b"dots-bridge":
                            raise InvalidToken
                    except InvalidToken:
                        raise BridgeError(
                            "storage_key_mismatch",
                            "Database storage key does not match configuration.",
                            500,
                        ) from None
                else:
                    db.execute(
                        "INSERT INTO bridge_meta VALUES ('key_check', ?)",
                        (self.cipher.encrypt(b"dots-bridge"),),
                    )
                if had_received and (not version or version[0] != b"4"):
                    self._migrate_received_messages(db)
                else:
                    db.execute(RECEIVED_TABLE_SCHEMA)
                for statement in RECEIVED_INDEX_SCHEMA.split(";"):
                    if statement.strip():
                        db.execute(statement)
                # A final reply is backfilled even when progress for the same
                # request already exists. The partial unique index prevents repeats.
                replies = db.execute(
                    "SELECT * FROM messages WHERE reply IS NOT NULL AND request_id NOT IN "
                    "(SELECT request_id FROM received_messages "
                    "WHERE kind='reply' AND request_id IS NOT NULL) "
                    "ORDER BY COALESCE(replied_at, created_at), request_id"
                ).fetchall()
                for reply in replies:
                    self._append_reply(db, reply)
                db.execute(
                    "INSERT INTO bridge_meta VALUES ('schema', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (b"4",),
                )

            await self.run(setup)
            self._ready = True

    def destination(self, subscription: dict) -> dict:
        try:
            return json.loads(self.cipher.decrypt(subscription["destination"]))
        except (InvalidToken, ValueError, KeyError, TypeError):
            raise BridgeError(
                "storage_error", "Could not decrypt a stored subscription.", 500
            ) from None

    async def subscription(self, subscription_id: str) -> dict | None:
        def query(db):
            row = db.execute(
                "SELECT * FROM subscriptions WHERE id=?", (subscription_id,)
            ).fetchone()
            return dict(row) if row else None

        return await self.run(query)

    async def subscriptions(self, owner: str) -> list[dict]:
        return await self.run(
            lambda db: [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM subscriptions WHERE owner=? AND expires_at>? ORDER BY id",
                    (owner, time.time()),
                )
            ]
        )

    async def save_subscription(self, sub: dict):
        encrypted = self.cipher.encrypt(json.dumps(sub["destination"]).encode())

        def save(db):
            db.execute("BEGIN IMMEDIATE")
            previous = db.execute(
                "SELECT expires_at FROM subscriptions WHERE id=?", (sub["id"],)
            ).fetchone()
            if previous and previous["expires_at"] <= time.time():
                # A new subscription after a lapse must not revive old requests
                # merely because its deterministic subscription ID is the same.
                self._cancel_requests(db, sub["id"], "subscription_expired")
            db.execute(
                "INSERT INTO subscriptions VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET destination=excluded.destination, "
                "expires_at=excluded.expires_at, verified_at=excluded.verified_at",
                (
                    sub["id"],
                    sub["owner"],
                    sub["channel"],
                    encrypted,
                    sub["expires_at"],
                    sub["verified_at"],
                ),
            )

        await self.run(save)

    @staticmethod
    def _cancel_requests(db, subscription_id: str, reason: str):
        db.execute(
            "UPDATE messages SET reply_cancelled=1 WHERE subscription_id=? AND reply IS NULL",
            (subscription_id,),
        )
        db.execute(
            "UPDATE messages SET delivery_status='cancelled', error=? "
            "WHERE subscription_id=? AND delivery_status IN ('queued','delivering')",
            (reason, subscription_id),
        )

    async def delete_subscription(self, subscription_id: str, owner: str):
        def delete(db):
            db.execute("BEGIN IMMEDIATE")
            exists = db.execute(
                "SELECT id FROM subscriptions WHERE id=? AND owner=?", (subscription_id, owner)
            ).fetchone()
            if not exists:
                return
            self._cancel_requests(db, subscription_id, "subscription_removed")
            db.execute("DELETE FROM subscriptions WHERE id=? AND owner=?", (subscription_id, owner))

        await self.run(delete)

    async def revoke_subscriptions(self, owner: str):
        def revoke(db):
            db.execute("BEGIN IMMEDIATE")
            subscriptions = list(db.execute("SELECT id FROM subscriptions WHERE owner=?", (owner,)))
            for subscription in subscriptions:
                self._cancel_requests(db, subscription["id"], "authorization_revoked")
            db.execute("DELETE FROM subscriptions WHERE owner=?", (owner,))

        await self.run(revoke)

    async def by_idempotency_key(self, key: str) -> dict | None:
        def query(db):
            row = db.execute("SELECT * FROM messages WHERE idempotency_key=?", (key,)).fetchone()
            return dict(row) if row else None

        return await self.run(query)

    async def insert_message(self, message: dict) -> dict:
        def insert(db):
            # Serialize idempotency lookup and insert against other submitters.
            db.execute("BEGIN IMMEDIATE")
            if message["idempotency_key"]:
                row = db.execute(
                    "SELECT * FROM messages WHERE idempotency_key=?", (message["idempotency_key"],)
                ).fetchone()
                if row:
                    return dict(row)
            subscription = db.execute(
                "SELECT id FROM subscriptions WHERE id=? AND expires_at>?",
                (message["subscription_id"], time.time()),
            ).fetchone()
            if not subscription:
                raise BridgeError(
                    "no_subscription", "The selected dots subscription is no longer active.", 409
                )
            columns = ",".join(message)
            placeholders = ",".join("?" for _ in message)
            db.execute(
                f"INSERT INTO messages ({columns}) VALUES ({placeholders})", tuple(message.values())
            )
            return dict(
                db.execute(
                    "SELECT * FROM messages WHERE request_id=?", (message["request_id"],)
                ).fetchone()
            )

        return await self.run(insert)

    async def message(self, request_id: str) -> dict:
        def query(db):
            row = db.execute("SELECT * FROM messages WHERE request_id=?", (request_id,)).fetchone()
            if not row:
                raise BridgeError("message_not_found", "No matching message.", 404)
            return dict(row)

        return await self.run(query)

    async def save_reply(self, request_id: str, text: str) -> dict:
        def save(db):
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM messages WHERE request_id=?", (request_id,)).fetchone()
            if not row:
                raise BridgeError("message_not_found", "No matching message.", 404)
            if row["reply"] is not None:
                if row["reply"] != text:
                    raise BridgeError(
                        "reply_conflict", "A different final reply is already stored.", 409
                    )
                return dict(row)
            self._require_active_request(db, row)
            db.execute(
                "UPDATE messages SET reply=?, replied_at=? WHERE request_id=?",
                (text, time.time(), request_id),
            )
            saved = db.execute(
                "SELECT * FROM messages WHERE request_id=?", (request_id,)
            ).fetchone()
            self._append_reply(db, saved)
            return dict(saved)

        return await self.run(save)

    @staticmethod
    def _require_active_request(db: sqlite3.Connection, row: sqlite3.Row) -> None:
        if row["expires_at"] <= time.time():
            raise BridgeError("reply_expired", "The reply deadline has passed.", 410)
        sub = db.execute(
            "SELECT id FROM subscriptions WHERE id=? AND expires_at>?",
            (row["subscription_id"], time.time()),
        ).fetchone()
        if not sub or row["reply_cancelled"] or row["delivery_status"] in {"failed", "cancelled"}:
            raise BridgeError("message_inactive", "This request is no longer active.", 409)

    async def save_progress(self, request_id: str, text: str, *, idempotency_key: str) -> dict:
        def save(db):
            db.execute("BEGIN IMMEDIATE")
            request = db.execute(
                "SELECT * FROM messages WHERE request_id=?", (request_id,)
            ).fetchone()
            if not request:
                raise BridgeError("message_not_found", "No matching message.", 404)
            existing = db.execute(
                "SELECT * FROM received_messages "
                "WHERE kind='progress' AND request_id=? AND idempotency_key=?",
                (request_id, idempotency_key),
            ).fetchone()
            if existing:
                if existing["text"] != text:
                    raise BridgeError(
                        "idempotency_conflict",
                        "This idempotency key was used with different input.",
                        409,
                    )
                return public_received(dict(existing))
            if request["reply"] is not None:
                raise BridgeError("reply_completed", "This request already has a final reply.", 409)
            self._require_active_request(db, request)
            message_id = str(uuid4())
            db.execute(
                "INSERT INTO received_messages "
                "(id, kind, request_id, channel, text, metadata, received_at, "
                "idempotency_key, fingerprint) VALUES (?, 'progress', ?, ?, ?, ?, ?, ?, ?)",
                (
                    message_id,
                    request_id,
                    request["channel"],
                    text,
                    request["metadata"],
                    time.time(),
                    idempotency_key,
                    hashlib.sha256(text.encode("utf-8")).hexdigest(),
                ),
            )
            return public_received(
                dict(
                    db.execute(
                        "SELECT * FROM received_messages WHERE id=?", (message_id,)
                    ).fetchone()
                )
            )

        return await self.run(save)

    @staticmethod
    def _append_reply(db: sqlite3.Connection, row: sqlite3.Row):
        db.execute(
            "INSERT INTO received_messages "
            "(id, kind, request_id, channel, text, metadata, received_at, fingerprint) "
            "VALUES (?, 'reply', ?, ?, ?, ?, ?, ?)",
            (
                str(uuid4()),
                row["request_id"],
                row["channel"],
                row["reply"],
                row["metadata"],
                row["replied_at"] if row["replied_at"] is not None else row["created_at"],
                row["fingerprint"],
            ),
        )

    async def save_push(
        self,
        *,
        text: str,
        channel: str,
        metadata: dict,
        idempotency_key: str,
        fingerprint: str,
    ) -> dict:
        def save(db):
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM received_messages WHERE kind='push' AND idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
            if row:
                if row["fingerprint"] != fingerprint:
                    raise BridgeError(
                        "idempotency_conflict",
                        "This idempotency key was already used with different message content.",
                        409,
                    )
                return public_received(dict(row))
            message_id = str(uuid4())
            db.execute(
                "INSERT INTO received_messages "
                "(id, kind, channel, text, metadata, received_at, idempotency_key, fingerprint) "
                "VALUES (?, 'push', ?, ?, ?, ?, ?, ?)",
                (
                    message_id,
                    channel,
                    text,
                    json.dumps(metadata, ensure_ascii=False),
                    time.time(),
                    idempotency_key,
                    fingerprint,
                ),
            )
            return public_received(
                dict(
                    db.execute(
                        "SELECT * FROM received_messages WHERE id=?", (message_id,)
                    ).fetchone()
                )
            )

        return await self.run(save)

    async def list_received(
        self, *, after: int = 0, limit: int = 100, channel: str | None = None
    ) -> list[dict]:
        def query(db):
            channel_clause = " AND channel=?" if channel is not None else ""
            parameters = (after, channel, limit) if channel is not None else (after, limit)
            return [
                public_received(dict(row))
                for row in db.execute(
                    "SELECT * FROM received_messages WHERE sequence>?"
                    + channel_clause
                    + " ORDER BY sequence LIMIT ?",
                    parameters,
                )
            ]

        return await self.run(query)

    async def list_response_messages(
        self, request_id: str, *, after: int = 0, limit: int = 100
    ) -> list[dict]:
        def query(db):
            db.execute("BEGIN")
            if not db.execute(
                "SELECT 1 FROM messages WHERE request_id=?", (request_id,)
            ).fetchone():
                raise BridgeError("message_not_found", "No matching message.", 404)
            return [
                public_received(dict(row))
                for row in db.execute(
                    "SELECT * FROM received_messages WHERE request_id=? "
                    "AND kind IN ('progress', 'reply') AND sequence>? ORDER BY sequence LIMIT ?",
                    (request_id, after, limit),
                )
            ]

        return await self.run(query)

    async def latest_received_sequence(self, *, channel: str | None = None) -> int:
        def query(db):
            if channel is None:
                row = db.execute("SELECT MAX(sequence) FROM received_messages").fetchone()
            else:
                row = db.execute(
                    "SELECT MAX(sequence) FROM received_messages WHERE channel=?", (channel,)
                ).fetchone()
            return row[0] or 0

        return await self.run(query)

    @staticmethod
    def _consumer_cursor(db: sqlite3.Connection, consumer_id: str, channel: str | None) -> int:
        row = db.execute(
            "SELECT channel, cursor FROM received_consumers WHERE consumer_id=?", (consumer_id,)
        ).fetchone()
        if row:
            if row["channel"] != channel:
                raise BridgeError(
                    "consumer_channel_conflict",
                    "This consumer ID is already bound to a different channel filter.",
                    409,
                )
            return row["cursor"]
        db.execute(
            "INSERT INTO received_consumers (consumer_id, channel) VALUES (?, ?)",
            (consumer_id, channel),
        )
        return 0

    async def get_consumer_cursor(self, consumer_id: str, channel: str | None = None) -> int:
        def query(db):
            db.execute("BEGIN IMMEDIATE")
            return self._consumer_cursor(db, consumer_id, channel)

        return await self.run(query)

    async def acknowledge_received(
        self, consumer_id: str, sequence: int, channel: str | None = None
    ) -> int:
        def acknowledge(db):
            db.execute("BEGIN IMMEDIATE")
            cursor = self._consumer_cursor(db, consumer_id, channel)
            if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
                raise BridgeError("invalid_received_cursor", "Invalid received message sequence.")
            if sequence:
                row = db.execute(
                    "SELECT channel FROM received_messages WHERE sequence=?", (sequence,)
                ).fetchone()
                if not row or (channel is not None and row["channel"] != channel):
                    raise BridgeError(
                        "invalid_received_cursor", "No matching received message at this sequence."
                    )
            if sequence > cursor:
                db.execute(
                    "UPDATE received_consumers SET cursor=? WHERE consumer_id=?",
                    (sequence, consumer_id),
                )
                return sequence
            return cursor

        return await self.run(acknowledge)

    async def claim(self, max_attempts: int) -> dict | None:
        def claim(db):
            db.execute("BEGIN IMMEDIATE")
            now = time.time()
            db.execute(
                "UPDATE messages SET delivery_status='failed', error='delivery_expired' "
                "WHERE delivery_status IN ('queued','delivering') AND expires_at<=?",
                (now,),
            )
            db.execute(
                "UPDATE messages SET delivery_status='failed', error='attempts_exhausted' "
                "WHERE delivery_status='delivering' AND lease_until<=? AND attempts>=?",
                (now, max_attempts),
            )
            db.execute(
                "UPDATE messages SET delivery_status='cancelled', error='subscription_inactive' "
                "WHERE delivery_status IN ('queued','delivering') AND subscription_id NOT IN "
                "(SELECT id FROM subscriptions WHERE expires_at>?)",
                (now,),
            )
            row = db.execute(
                "SELECT * FROM messages WHERE attempts<? AND expires_at>? AND "
                "((delivery_status='queued' AND next_attempt_at<=?) OR "
                "(delivery_status='delivering' AND lease_until<=?)) ORDER BY created_at LIMIT 1",
                (max_attempts, now, now, now),
            ).fetchone()
            if not row:
                return None
            db.execute(
                "UPDATE messages SET delivery_status='delivering', attempts=attempts+1, "
                "lease_until=? WHERE request_id=?",
                (now + 30, row["request_id"]),
            )
            return dict(
                db.execute(
                    "SELECT * FROM messages WHERE request_id=?", (row["request_id"],)
                ).fetchone()
            )

        return await self.run(claim)

    async def delivery_result(
        self,
        request_id: str,
        status: str,
        status_code: int | None,
        error: str | None,
        next_attempt_at: float,
    ):
        await self.run(
            lambda db: db.execute(
                "UPDATE messages SET delivery_status=?, status_code=?, error=?, next_attempt_at=?, "
                "lease_until=NULL WHERE request_id=? AND delivery_status='delivering'",
                (status, status_code, error, next_attempt_at, request_id),
            )
        )

    async def prune(self, older_than: float) -> int:
        def prune(db):
            return db.execute(
                "DELETE FROM messages WHERE expires_at<? "
                "AND delivery_status NOT IN ('queued','delivering')",
                (older_than,),
            ).rowcount

        return await self.run(prune)


def public_message(row: dict) -> dict[str, Any]:
    replied = row["reply"] is not None
    return {
        "request_id": row["request_id"],
        "event_id": row["event_id"],
        "subscription_id": row["subscription_id"],
        "channel": row["channel"],
        "text": row["text"],
        "metadata": json.loads(row["metadata"]),
        "created_at": iso8601(row["created_at"]),
        "expires_at": iso8601(row["expires_at"]),
        "delivery_status": row["delivery_status"],
        "attempts": row["attempts"],
        "status_code": row["status_code"],
        "error": row["error"],
        "reply_status": "replied"
        if replied
        else (
            "cancelled"
            if row["reply_cancelled"]
            else ("expired" if row["expires_at"] <= time.time() else "pending")
        ),
        "reply": row["reply"],
        "replied_at": iso8601(row["replied_at"]) if row["replied_at"] else None,
    }


def public_received(row: dict) -> dict[str, Any]:
    return {
        "sequence": row["sequence"],
        "id": row["id"],
        "kind": row["kind"],
        "request_id": row["request_id"],
        "channel": row["channel"],
        "text": row["text"],
        "metadata": json.loads(row["metadata"]),
        "received_at": iso8601(row["received_at"]),
    }
