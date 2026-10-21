"""Short SQLite transactions; network calls never run inside a transaction."""

import asyncio
import json
import os
import sqlite3
import time
from collections.abc import Callable
from contextlib import closing
from pathlib import Path
from typing import Any, TypeVar

from cryptography.fernet import Fernet, InvalidToken

from .errors import BridgeError
from .models import iso8601

T = TypeVar("T")

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

    async def initialize(self):
        async with self._lock:
            if self._ready:
                return

            def setup(db):
                db.execute("PRAGMA journal_mode=WAL")
                db.executescript(SCHEMA)
                db.execute("BEGIN IMMEDIATE")
                version = db.execute("SELECT value FROM bridge_meta WHERE key='schema'").fetchone()
                if version and version[0] not in (b"1", b"2"):
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
                db.execute(
                    "INSERT INTO bridge_meta VALUES ('schema', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (b"2",),
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
            if row["expires_at"] <= time.time():
                raise BridgeError("reply_expired", "The reply deadline has passed.", 410)
            sub = db.execute(
                "SELECT id FROM subscriptions WHERE id=? AND expires_at>?",
                (row["subscription_id"], time.time()),
            ).fetchone()
            if (
                not sub
                or row["reply_cancelled"]
                or row["delivery_status"] in {"failed", "cancelled"}
            ):
                raise BridgeError("message_inactive", "This request is no longer active.", 409)
            db.execute(
                "UPDATE messages SET reply=?, replied_at=? WHERE request_id=?",
                (text, time.time(), request_id),
            )
            return dict(
                db.execute("SELECT * FROM messages WHERE request_id=?", (request_id,)).fetchone()
            )

        return await self.run(save)

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
