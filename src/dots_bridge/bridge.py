import asyncio
import hashlib
import hmac
import json
import logging
import re
import secrets
import time
from contextlib import suppress
from copy import deepcopy
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from uuid import uuid4

import httpx
from pydantic import ValidationError

from .config import Settings
from .errors import BridgeError
from .models import (
    EVENT,
    EVENT_NAME,
    CancellationInput,
    MessageInput,
    ProgressInput,
    PushInput,
    ReceivedAcknowledgement,
    ReceivedConsumer,
    ReceivedQuery,
    iso8601,
)
from .receiver import ReceivedDispatcher
from .store import Store, public_message
from .webhooks import post_webhook, signed_body, signing_key, validate_url

logger = logging.getLogger(__name__)


class DotsBridge:
    """Persistent single-owner bridge. Use create_app() for HTTP and MCP access."""

    def __init__(self, settings: Settings, *, webhook_post=post_webhook):
        self.settings = settings
        self.owner_user_id = settings.owner_user_id
        self.store = Store(settings.db_path, settings.storage_key)
        self.webhook_post = webhook_post
        self._worker: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self._has_received_handlers = False
        self._receiver = ReceivedDispatcher(
            list_received=self.list_received,
            get_cursor=self.get_consumer_cursor,
            acknowledge=self.acknowledge_received,
        )

    async def initialize(self):
        await self.store.initialize()

    async def start(self):
        await self.initialize()
        if self._worker is None:
            self._worker = asyncio.create_task(self._run_worker(), name="dots-bridge-delivery")
        if self._has_received_handlers and not self.receiving:
            await self.start_receiving()

    async def close(self):
        await self.stop_receiving()
        if self._worker is not None:
            self._worker.cancel()
            with suppress(asyncio.CancelledError):
                await self._worker
            self._worker = None

    def on_received(self, handler):
        """Register a sync or async handler for replies and unsolicited messages.

        Register before the application's lifespan starts to start receiving
        automatically, or call start_receiving() explicitly.
        """
        registered = self._receiver.on_received(handler)
        self._has_received_handlers = True
        return registered

    @property
    def receiving(self) -> bool:
        return self._receiver.running

    @property
    def receiver_error(self) -> Exception | None:
        return self._receiver.error

    async def start_receiving(
        self,
        *,
        consumer_id: str = "in-process",
        channel: str | None = None,
        poll_interval: float = 0.5,
    ):
        """Start background callbacks, resuming this consumer's persisted cursor."""
        await self.initialize()
        await self._receiver.start(
            consumer_id=consumer_id,
            channel=channel,
            poll_interval=poll_interval,
        )

    async def stop_receiving(self):
        await self._receiver.stop()

    async def list_received(
        self,
        *,
        after: int = 0,
        limit: int = 100,
        channel: str | None = None,
    ) -> list[dict]:
        try:
            query = ReceivedQuery(after=after, limit=limit, channel=channel)
        except ValidationError:
            raise BridgeError(
                "invalid_received_query", "Invalid after, limit or channel.", 422
            ) from None
        await self.initialize()
        return await self.store.list_received(**query.model_dump())

    async def list_response_messages(
        self, request_id: str, *, after: int = 0, limit: int = 100
    ) -> list[dict]:
        """Read this request's progress updates and final reply in receipt order."""
        try:
            query = ReceivedQuery(after=after, limit=limit)
        except ValidationError:
            raise BridgeError("invalid_received_query", "Invalid after or limit.", 422) from None
        if not isinstance(request_id, str) or not request_id.strip() or len(request_id) > 200:
            raise BridgeError("invalid_request_id", "Expected a valid request_id.", 422)
        try:
            request_id.encode("utf-8")
        except UnicodeError:
            raise BridgeError("invalid_request_id", "Expected a valid request_id.", 422) from None
        await self.initialize()
        return await self.store.list_response_messages(
            request_id, after=query.after, limit=query.limit
        )

    async def get_consumer_cursor(self, consumer_id: str, channel: str | None = None) -> int:
        self._validate_consumer(consumer_id, channel)
        await self.initialize()
        return await self.store.get_consumer_cursor(consumer_id, channel)

    async def acknowledge_received(
        self,
        consumer_id: str,
        sequence: int,
        channel: str | None = None,
    ) -> int:
        self._validate_consumer(consumer_id, channel)
        try:
            ReceivedAcknowledgement(sequence=sequence, channel=channel)
        except ValidationError:
            raise BridgeError(
                "invalid_received_cursor", "Invalid received message sequence.", 422
            ) from None
        await self.initialize()
        return await self.store.acknowledge_received(consumer_id, sequence, channel)

    @staticmethod
    def _validate_consumer(consumer_id, channel):
        try:
            ReceivedConsumer(consumer_id=consumer_id, channel=channel)
        except ValidationError:
            raise BridgeError("invalid_consumer", "Invalid consumer_id or channel.", 422) from None

    async def list_events(self) -> list[dict]:
        return [deepcopy(EVENT)]

    def _owner(self, owner: str):
        if owner != self.owner_user_id:
            raise BridgeError("forbidden", "This connection does not own the subscription.", 403)

    def _subscription_args(self, params: dict, owner: str) -> tuple[str, str, dict]:
        self._owner(owner)
        args = params.get("arguments", {})
        delivery = params.get("delivery")
        if (
            params.get("name") != EVENT_NAME
            or not isinstance(args, dict)
            or set(args) - {"channel"}
            or not isinstance(delivery, dict)
            or delivery.get("mode") != "webhook"
            or not isinstance(delivery.get("url"), str)
        ):
            raise BridgeError(
                "invalid_subscription", "Expected app.message.created with webhook delivery."
            )
        channel = args.get("channel", "default")
        if (
            not isinstance(channel, str)
            or not 1 <= len(channel) <= 128
            or not re.fullmatch(r"[\w.-]+", channel)
        ):
            raise BridgeError(
                "invalid_channel", "channel must contain 1–128 letters, digits, dots or hyphens."
            )
        try:
            validate_url(delivery["url"])
        except ValueError:
            raise BridgeError(
                "invalid_callback", "Callback must be an HTTPS URL on port 443."
            ) from None
        identity = json.dumps(
            [owner, delivery["url"], EVENT_NAME, {"channel": channel}],
            sort_keys=True,
            separators=(",", ":"),
        )
        sub_id = "sub_" + hashlib.sha256(identity.encode()).hexdigest()
        return sub_id, channel, delivery

    async def subscribe(self, params: dict, owner: str) -> dict:
        await self.initialize()
        sub_id, channel, delivery = self._subscription_args(params, owner)
        if params.get("cursor") is not None:
            raise BridgeError("replay_unsupported", "This event does not support cursor replay.")
        secret = delivery.get("secret")
        try:
            signing_key(secret)
        except ValueError:
            raise BridgeError("invalid_secret", "Invalid webhook signing secret.") from None
        default_ttl = self.settings.subscription_ttl_seconds * 1000
        ttl = params.get("ttlMs", default_ttl)
        if ttl is not None and (type(ttl) is not int or ttl <= 0):
            raise BridgeError("invalid_ttl", "ttlMs must be a positive integer or null.")
        now = time.time()
        expires_at = now + min(ttl if ttl is not None else default_ttl, default_ttl) / 1000
        existing = await self.store.subscription(sub_id)
        old = self.store.destination(existing) if existing else {}
        verified_at = existing["verified_at"] if existing else 0
        if not (old.get("secret") == secret and verified_at > now - 300):
            challenge = secrets.token_urlsafe(32)
            body, headers = signed_body(
                sub_id,
                secret,
                "verify_" + uuid4().hex,
                {"type": "verification", "challenge": challenge},
            )
            try:
                response = await self.webhook_post(delivery["url"], body, headers)
                echoed = response.json().get("challenge") if response.is_success else None
                if not isinstance(echoed, str) or not hmac.compare_digest(
                    echoed.encode(), challenge.encode()
                ):
                    raise ValueError("challenge_failed")
            except (httpx.HTTPError, OSError, TimeoutError, ValueError, AttributeError):
                raise BridgeError(
                    "challenge_failed", "Callback verification failed.", 400, -32015
                ) from None
            verified_at = now
        destination = {"url": delivery["url"], "secret": secret}
        if old.get("secret") and old["secret"] != secret:
            destination.update(old_secret=old["secret"], old_secret_until=now + 300)
        elif old.get("old_secret_until", 0) > now:
            destination.update(
                old_secret=old["old_secret"], old_secret_until=old["old_secret_until"]
            )
        await self.store.save_subscription(
            {
                "id": sub_id,
                "owner": owner,
                "channel": channel,
                "destination": destination,
                "expires_at": expires_at,
                "verified_at": verified_at,
            }
        )
        return {
            "id": sub_id,
            "refreshBefore": iso8601(expires_at),
            "cursor": None,
            "truncated": False,
        }

    async def unsubscribe(self, params: dict, owner: str) -> dict:
        await self.initialize()
        sub_id, _, _ = self._subscription_args(params, owner)
        await self.store.delete_subscription(sub_id, owner)
        return {}

    async def revoke_subscriptions(self):
        await self.initialize()
        await self.store.revoke_subscriptions(self.owner_user_id)

    async def list_subscriptions(self) -> list[dict]:
        await self.initialize()
        rows = await self.store.subscriptions(self.owner_user_id)
        return [
            {"id": row["id"], "channel": row["channel"], "expires_at": iso8601(row["expires_at"])}
            for row in rows
        ]

    async def send_message(
        self,
        text: str,
        *,
        channel: str = "default",
        subscription_id: str | None = None,
        metadata: dict | None = None,
        idempotency_key: str | None = None,
    ) -> dict:
        message = MessageInput(
            text=text,
            channel=channel,
            subscription_id=subscription_id,
            metadata=metadata if metadata is not None else {},
            idempotency_key=idempotency_key,
        )
        return await self.submit(message)

    async def submit(self, message: MessageInput) -> dict:
        await self.initialize()
        try:
            canonical = json.dumps(
                message.model_dump(exclude={"idempotency_key"}),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            canonical_bytes = canonical.encode("utf-8")
        except (TypeError, ValueError, RecursionError):
            raise BridgeError(
                "invalid_metadata", "metadata must contain finite JSON values.", 422
            ) from None
        fingerprint = hashlib.sha256(canonical_bytes).hexdigest()
        if message.idempotency_key:
            existing = await self.store.by_idempotency_key(message.idempotency_key)
            if existing:
                return self._check_idempotency(existing, fingerprint)
        subscriptions = await self.store.subscriptions(self.owner_user_id)
        matches = [
            s
            for s in subscriptions
            if s["channel"] == message.channel
            and (message.subscription_id is None or s["id"] == message.subscription_id)
        ]
        if not matches:
            raise BridgeError(
                "no_subscription", "Create an active dots subscription for this channel first.", 409
            )
        if len(matches) > 1:
            raise BridgeError(
                "subscription_required", "Multiple dots match; supply subscription_id.", 409
            )
        now = time.time()
        request_id, event_id = "req_" + uuid4().hex, "evt_" + uuid4().hex
        event = {
            "eventId": event_id,
            "name": EVENT_NAME,
            "timestamp": iso8601(now),
            "cursor": None,
            "data": {
                "request_id": request_id,
                "text": message.text,
                "channel": message.channel,
                "metadata": message.metadata,
            },
        }
        event_json = json.dumps(event, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        if len(event_json.encode()) > 262144:
            raise BridgeError("message_too_large", "Serialized event exceeds 256 KiB.", 413)
        row = await self.store.insert_message(
            {
                "request_id": request_id,
                "event_id": event_id,
                "subscription_id": matches[0]["id"],
                "channel": message.channel,
                "text": message.text,
                "metadata": json.dumps(message.metadata, ensure_ascii=False, allow_nan=False),
                "created_at": now,
                "expires_at": now + self.settings.reply_timeout_seconds,
                "next_attempt_at": now,
                "idempotency_key": message.idempotency_key,
                "fingerprint": fingerprint,
                "event": event_json,
            }
        )
        result = self._check_idempotency(row, fingerprint)
        self._wake.set()
        return result

    @staticmethod
    def _check_idempotency(row: dict, fingerprint: str) -> dict:
        if row["fingerprint"] != fingerprint:
            raise BridgeError(
                "idempotency_conflict", "This idempotency key was used with different input.", 409
            )
        return public_message(row)

    async def get_message(self, request_id: str) -> dict:
        await self.initialize()
        return public_message(await self.store.message(request_id))

    async def reply_to_app(self, request_id: str, text: str) -> dict:
        if not isinstance(text, str) or not text.strip() or len(text) > 64000:
            raise BridgeError("invalid_reply", "Reply must contain 1–64000 characters.", 422)
        try:
            text.encode("utf-8")
        except UnicodeError:
            raise BridgeError(
                "invalid_reply", "Reply must contain valid Unicode text.", 422
            ) from None
        await self.initialize()
        row = await self.store.save_reply(request_id, text)
        self._receiver.notify()
        return {
            "request_id": row["request_id"],
            "reply": row["reply"],
            "replied_at": iso8601(row["replied_at"]),
        }

    async def report_progress(self, request_id: str, text: str, *, idempotency_key: str) -> dict:
        """Persist a plain-text update without completing the application request."""
        try:
            progress = ProgressInput(
                request_id=request_id, text=text, idempotency_key=idempotency_key
            )
        except ValidationError:
            raise BridgeError(
                "invalid_progress",
                "Expected valid request_id, progress text and idempotency_key.",
                422,
            ) from None
        await self.initialize()
        result = await self.store.save_progress(
            progress.request_id, progress.text, idempotency_key=progress.idempotency_key
        )
        self._receiver.notify()
        return result

    async def cancel_request(self, request_id: str, *, superseded_by: str) -> dict:
        """Close an obsolete request in favor of a newer request, without interrupting dots."""
        try:
            cancellation = CancellationInput(request_id=request_id, superseded_by=superseded_by)
        except ValidationError:
            raise BridgeError(
                "invalid_cancellation",
                "Expected valid request_id and superseded_by request IDs.",
                422,
            ) from None
        await self.initialize()
        result = await self.store.cancel_request(
            cancellation.request_id, superseded_by=cancellation.superseded_by
        )
        self._receiver.notify()
        return result

    async def send_to_app(
        self,
        text: str,
        *,
        idempotency_key: str,
        channel: str = "default",
        metadata: dict | None = None,
    ) -> dict:
        """Persist an unsolicited dots message independently of request/reply state."""
        try:
            message = PushInput(
                text=text,
                channel=channel,
                metadata=metadata if metadata is not None else {},
                idempotency_key=idempotency_key,
            )
            canonical = json.dumps(
                {"text": message.text, "channel": message.channel, "metadata": message.metadata},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        except (ValidationError, TypeError, ValueError, RecursionError):
            raise BridgeError(
                "invalid_push",
                "Expected valid text, channel, JSON metadata and idempotency_key.",
                422,
            ) from None
        if len(canonical) > 262144:
            raise BridgeError("message_too_large", "Serialized message exceeds 256 KiB.", 413)
        await self.initialize()
        result = await self.store.save_push(
            text=message.text,
            channel=message.channel,
            metadata=message.metadata,
            idempotency_key=message.idempotency_key,
            fingerprint=hashlib.sha256(canonical).hexdigest(),
        )
        self._receiver.notify()
        return result

    async def process_next(self) -> bool:
        """Deliver one due outbox entry. Also useful for deterministic worker tests."""
        await self.initialize()
        row = await self.store.claim(self.settings.max_delivery_attempts)
        if row is None:
            return False
        sub = await self.store.subscription(row["subscription_id"])
        if not sub or sub["expires_at"] <= time.time():
            await self.store.delivery_result(
                row["request_id"], "cancelled", None, "subscription_inactive", time.time()
            )
            return True
        destination = self.store.destination(sub)
        old_secret = (
            destination.get("old_secret")
            if destination.get("old_secret_until", 0) > time.time()
            else None
        )
        status_code = None
        retry_after = None
        try:
            body, headers = signed_body(
                sub["id"],
                destination["secret"],
                row["event_id"],
                json.loads(row["event"]),
                old_secret=old_secret,
            )
            response = await self.webhook_post(destination["url"], body, headers)
            status_code = response.status_code
            if response.is_success:
                await self.store.delivery_result(
                    row["request_id"], "accepted", status_code, None, time.time()
                )
                return True
            transient = status_code in {408, 425, 429} or status_code >= 500
            retry_after = response.headers.get("retry-after")
            error = f"webhook_http_{status_code}"
        except (httpx.HTTPError, OSError, TimeoutError):
            transient, error = True, "webhook_unavailable"
        except ValueError:
            transient, error = False, "invalid_webhook_destination"
        retry = transient and row["attempts"] < self.settings.max_delivery_attempts
        delay = min(300, self.settings.retry_base_seconds * 2 ** (row["attempts"] - 1))
        if retry_after:
            try:
                try:
                    wait = float(retry_after)
                except ValueError:
                    wait = (parsedate_to_datetime(retry_after) - datetime.now(UTC)).total_seconds()
                delay = max(delay, min(3600, max(0, wait)))
            except (ValueError, TypeError, OverflowError):
                pass
        await self.store.delivery_result(
            row["request_id"],
            "queued" if retry else "failed",
            status_code,
            error,
            time.time() + delay,
        )
        if status_code == 410:
            await self.store.delete_subscription(sub["id"], self.owner_user_id)
        return True

    async def _run_worker(self):
        while True:
            self._wake.clear()
            try:
                if await self.process_next():
                    continue
            except BridgeError as exc:
                logger.error("Delivery worker error: %s", exc.code)
            except Exception as exc:
                # Keep the durable queue alive if an injected delivery adapter
                # fails unexpectedly. Its lease will expire and be retried.
                # Avoid logging exception text, which may contain callback URLs.
                logger.error("Unexpected delivery worker error: %s", type(exc).__name__)
            try:
                await asyncio.wait_for(self._wake.wait(), self.settings.worker_interval_seconds)
            except TimeoutError:
                pass
