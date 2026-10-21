import asyncio
import base64
import json
from collections import deque
from datetime import datetime

import httpx
import pytest
from cryptography.fernet import Fernet

from dots_bridge.bridge import DotsBridge
from dots_bridge.config import Settings
from dots_bridge.errors import BridgeError
from dots_bridge.models import EVENT_NAME


def signing_secret(byte=b"s"):
    return "whsec_" + base64.b64encode(byte * 32).decode()


def subscription_params(*, channel="default", url="https://callback.example/dots", secret=None):
    return {
        "name": EVENT_NAME,
        "arguments": {"channel": channel},
        "delivery": {"mode": "webhook", "url": url, "secret": secret or signing_secret()},
    }


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


class Clock:
    def __init__(self):
        self.now = 1800000000.0

    def advance(self, seconds):
        self.now += seconds


class Callback:
    def __init__(self):
        self.calls = []
        self.verification_outcomes = deque()
        self.outcomes = deque()

    @property
    def verifications(self):
        return [call for call in self.calls if call["payload"].get("type") == "verification"]

    @property
    def events(self):
        return [call for call in self.calls if call["payload"].get("type") != "verification"]

    async def __call__(self, url, body, headers):
        payload = json.loads(body)
        self.calls.append({"url": url, "body": body, "headers": headers, "payload": payload})
        verification = payload.get("type") == "verification"
        outcomes = self.verification_outcomes if verification else self.outcomes
        if outcomes:
            outcome = outcomes.popleft()
            if isinstance(outcome, Exception):
                raise outcome
            if isinstance(outcome, int):
                return httpx.Response(outcome)
            return outcome
        return (
            httpx.Response(200, json={"challenge": payload["challenge"]})
            if verification
            else httpx.Response(202)
        )


@pytest.fixture
def clock(monkeypatch):
    clock = Clock()
    # bridge/store/webhooks all use the shared time module. Advancing this clock
    # exercises deadlines and retries without real sleeps or external services.
    monkeypatch.setattr("dots_bridge.bridge.time.time", lambda: clock.now)
    return clock


@pytest.fixture
def settings(tmp_path):
    return Settings(
        owner_key="owner-" + "a" * 32,
        api_key="app-" + "b" * 32,
        storage_key=Fernet.generate_key().decode(),
        db_path=tmp_path / "data" / "bridge.sqlite3",
        retry_base_seconds=1,
        max_delivery_attempts=3,
    )


@pytest.fixture
def callback():
    return Callback()


@pytest.fixture
async def bridge(settings, callback, clock):
    bridge = DotsBridge(settings, webhook_post=callback)
    await bridge.initialize()
    yield bridge
    await bridge.close()


async def subscribe(bridge, **kwargs):
    return await bridge.subscribe(subscription_params(**kwargs), bridge.owner_user_id)


@pytest.mark.parametrize(
    "failure",
    [
        httpx.Response(200, json={"challenge": "different"}),
        httpx.Response(200, content=b"not json"),
        httpx.Response(503),
        httpx.ConnectError("unavailable"),
    ],
)
async def test_failed_verification_does_not_create_subscription(bridge, callback, failure):
    callback.verification_outcomes.append(failure)
    with pytest.raises(BridgeError) as caught:
        await subscribe(bridge)
    assert caught.value.code == "challenge_failed"
    assert caught.value.rpc_code == -32015
    assert await bridge.list_subscriptions() == []


async def test_subscription_refresh_is_idempotent_and_verification_cache_expires(
    bridge, callback, clock
):
    first = await subscribe(bridge)
    clock.advance(100)
    refreshed = await subscribe(bridge)
    assert refreshed["id"] == first["id"]
    assert timestamp(refreshed["refreshBefore"]) == timestamp(first["refreshBefore"]) + 100
    assert len(await bridge.list_subscriptions()) == 1
    assert len(callback.verifications) == 1
    clock.advance(201)
    assert (await subscribe(bridge))["id"] == first["id"]
    assert len(callback.verifications) == 2


@pytest.mark.parametrize("ttl", [None, 1000, 864000000])
async def test_subscription_ttl_null_and_maximum(bridge, clock, ttl):
    params = subscription_params()
    params["ttlMs"] = ttl
    result = await bridge.subscribe(params, bridge.owner_user_id)
    expected = 1 if ttl == 1000 else bridge.settings.subscription_ttl_seconds
    assert timestamp(result["refreshBefore"]) == clock.now + expected
    assert result["cursor"] is None
    assert result["truncated"] is False


@pytest.mark.parametrize("ttl", [0, -1, True, "1000", 1.5])
async def test_subscription_rejects_nonpositive_or_noninteger_ttl(bridge, callback, ttl):
    params = {**subscription_params(), "ttlMs": ttl}
    with pytest.raises(BridgeError) as caught:
        await bridge.subscribe(params, bridge.owner_user_id)
    assert caught.value.code == "invalid_ttl"
    assert not callback.calls


async def test_null_cursor_uses_cache_but_replay_cursor_is_rejected(bridge, callback):
    first = await subscribe(bridge)
    refreshed = await bridge.subscribe(
        {**subscription_params(), "cursor": None}, bridge.owner_user_id
    )
    assert refreshed["id"] == first["id"]
    assert len(callback.verifications) == 1
    with pytest.raises(BridgeError) as caught:
        await bridge.subscribe(
            {**subscription_params(), "cursor": "old-event"}, bridge.owner_user_id
        )
    assert caught.value.code == "replay_unsupported"


async def test_secret_rotation_challenges_new_key_and_temporarily_signs_with_both(
    bridge, callback, clock
):
    first = await subscribe(bridge)
    clock.advance(10)
    rotated = await subscribe(bridge, secret=signing_secret(b"n"))
    assert first["id"] == rotated["id"]
    assert len(callback.verifications) == 2
    await bridge.send_message("during rotation")
    assert await bridge.process_next()
    assert len(callback.events[-1]["headers"]["webhook-signature"].split()) == 2
    clock.advance(10)
    await subscribe(bridge, secret=signing_secret(b"n"))
    assert len(callback.verifications) == 2
    await bridge.send_message("same key refresh retains grace period")
    assert await bridge.process_next()
    assert len(callback.events[-1]["headers"]["webhook-signature"].split()) == 2
    clock.advance(291)
    await bridge.send_message("after rotation")
    assert await bridge.process_next()
    assert len(callback.events[-1]["headers"]["webhook-signature"].split()) == 1


async def test_failed_rotation_preserves_previous_working_subscription(bridge, callback):
    first = await subscribe(bridge)
    callback.verification_outcomes.append(httpx.Response(200, json={"challenge": "wrong"}))
    with pytest.raises(BridgeError):
        await subscribe(bridge, secret=signing_secret(b"n"))
    stored = await bridge.store.subscription(first["id"])
    assert bridge.store.destination(stored)["secret"] == signing_secret()


async def test_owner_mismatch_cannot_subscribe_or_unsubscribe(bridge):
    first = await subscribe(bridge)
    for method in (bridge.subscribe, bridge.unsubscribe):
        with pytest.raises(BridgeError) as caught:
            await method(subscription_params(), "another-user")
        assert caught.value.code == "forbidden"
        assert caught.value.status_code == 403
    assert (await bridge.list_subscriptions())[0]["id"] == first["id"]


async def test_send_requires_an_active_subscription(bridge, clock):
    with pytest.raises(BridgeError) as caught:
        await bridge.send_message("hello")
    assert caught.value.code == "no_subscription"
    await subscribe(bridge)
    clock.advance(bridge.settings.subscription_ttl_seconds + 1)
    with pytest.raises(BridgeError) as caught:
        await bridge.send_message("late")
    assert caught.value.code == "no_subscription"


async def test_multiple_subscriptions_require_target_and_channels_filter_destinations(bridge):
    first = await subscribe(bridge)
    await subscribe(bridge, url="https://callback.example/second")
    other_channel = await subscribe(bridge, channel="avatar", url="https://callback.example/avatar")
    with pytest.raises(BridgeError) as caught:
        await bridge.send_message("ambiguous")
    assert caught.value.code == "subscription_required"
    explicit = await bridge.send_message("targeted", subscription_id=first["id"])
    assert explicit["subscription_id"] == first["id"]
    filtered = await bridge.send_message("avatar message", channel="avatar")
    assert filtered["subscription_id"] == other_channel["id"]
    with pytest.raises(BridgeError) as caught:
        await bridge.send_message("mismatched", channel="avatar", subscription_id=first["id"])
    assert caught.value.code == "no_subscription"


async def test_event_correlates_request_metadata_and_webhook_receipt_is_separate_from_reply(
    bridge, callback
):
    subscription = await subscribe(bridge)
    metadata = {"conversation": "abc", "sequence": 7, "nested": {"ok": True}}
    sent = await bridge.send_message("こんにちは", metadata=metadata)
    assert sent["delivery_status"] == "queued"
    assert sent["reply_status"] == "pending"
    assert sent["attempts"] == 0
    assert not callback.events
    assert await bridge.process_next()
    event = callback.events[0]
    assert event["payload"]["eventId"] == sent["event_id"]
    assert event["payload"]["name"] == EVENT_NAME
    assert event["payload"]["data"] == {
        "request_id": sent["request_id"],
        "text": "こんにちは",
        "channel": "default",
        "metadata": metadata,
    }
    assert event["headers"]["webhook-id"] == sent["event_id"]
    assert event["headers"]["X-MCP-Subscription-Id"] == subscription["id"]
    accepted = await bridge.get_message(sent["request_id"])
    assert accepted["delivery_status"] == "accepted"
    assert accepted["status_code"] == 202
    assert accepted["reply_status"] == "pending"
    assert accepted["reply"] is None


@pytest.mark.parametrize("first_outcome", [503, 429, httpx.ReadTimeout("timed out")])
async def test_retry_keeps_event_and_id_but_refreshes_signature_timestamp(
    bridge, callback, clock, first_outcome
):
    await subscribe(bridge)
    callback.outcomes.append(first_outcome)
    sent = await bridge.send_message("retry me")
    assert await bridge.process_next()
    failed_attempt = await bridge.get_message(sent["request_id"])
    assert failed_attempt["delivery_status"] == "queued"
    assert failed_attempt["attempts"] == 1
    assert not await bridge.process_next()
    clock.advance(2)
    assert await bridge.process_next()
    first, second = callback.events
    assert first["body"] == second["body"]
    assert first["headers"]["webhook-id"] == second["headers"]["webhook-id"] == sent["event_id"]
    assert first["headers"]["webhook-timestamp"] != second["headers"]["webhook-timestamp"]
    assert first["headers"]["webhook-signature"] != second["headers"]["webhook-signature"]
    final = await bridge.get_message(sent["request_id"])
    assert final["delivery_status"] == "accepted"
    assert final["attempts"] == 2
    assert not await bridge.process_next()


async def test_retry_after_delays_next_attempt(bridge, callback, clock):
    await subscribe(bridge)
    callback.outcomes.append(httpx.Response(429, headers={"Retry-After": "10"}))
    sent = await bridge.send_message("rate limited")
    assert await bridge.process_next()
    clock.advance(9)
    assert not await bridge.process_next()
    clock.advance(1)
    assert await bridge.process_next()
    assert (await bridge.get_message(sent["request_id"]))["delivery_status"] == "accepted"


async def test_retry_limit_is_terminal(bridge, callback, clock):
    await subscribe(bridge)
    callback.outcomes.extend([503] * 3)
    sent = await bridge.send_message("keep failing")
    for _ in range(3):
        assert await bridge.process_next()
        clock.advance(10)
    final = await bridge.get_message(sent["request_id"])
    assert final["delivery_status"] == "failed"
    assert final["attempts"] == 3
    assert final["error"] == "webhook_http_503"
    assert not await bridge.process_next()


@pytest.mark.parametrize("status", [410, 413])
async def test_410_and_413_are_never_retried(bridge, callback, clock, status):
    await subscribe(bridge)
    callback.outcomes.append(status)
    sent = await bridge.send_message("permanent rejection")
    assert await bridge.process_next()
    clock.advance(100)
    assert not await bridge.process_next()
    final = await bridge.get_message(sent["request_id"])
    assert final["delivery_status"] == "failed"
    assert final["attempts"] == 1
    assert final["status_code"] == status
    assert len(callback.events) == 1
    assert bool(await bridge.list_subscriptions()) is (status != 410)


async def test_410_removes_subscription_and_cancels_its_other_pending_events(bridge, callback):
    await subscribe(bridge)
    callback.outcomes.append(410)
    first = await bridge.send_message("gone")
    second = await bridge.send_message("also pending")
    assert await bridge.process_next()
    assert (await bridge.get_message(first["request_id"]))["delivery_status"] == "failed"
    assert (await bridge.get_message(second["request_id"]))["delivery_status"] == "cancelled"
    assert not await bridge.process_next()


async def test_restart_resumes_persisted_queue_and_keeps_subscription_secret_private(
    bridge, callback
):
    subscription = await subscribe(bridge)
    sent = await bridge.send_message("survives restart", metadata={"source": "test"})
    stored = await bridge.store.subscription(subscription["id"])
    assert b"callback.example" not in stored["destination"]
    assert signing_secret().encode() not in stored["destination"]
    restarted = DotsBridge(bridge.settings, webhook_post=callback)
    await restarted.initialize()
    assert await restarted.process_next()
    result = await restarted.get_message(sent["request_id"])
    assert result["event_id"] == sent["event_id"]
    assert result["metadata"] == {"source": "test"}
    assert result["delivery_status"] == "accepted"


async def test_restart_reclaims_abandoned_delivery_after_lease_expires(bridge, callback, clock):
    await subscribe(bridge)
    sent = await bridge.send_message("crashed while delivering")
    claimed = await bridge.store.claim(bridge.settings.max_delivery_attempts)
    assert claimed["request_id"] == sent["request_id"]
    restarted = DotsBridge(bridge.settings, webhook_post=callback)
    assert not await restarted.process_next()
    clock.advance(31)
    assert await restarted.process_next()
    result = await restarted.get_message(sent["request_id"])
    assert result["delivery_status"] == "accepted"
    assert result["attempts"] == 2
    assert callback.events[0]["payload"]["eventId"] == sent["event_id"]


async def test_idempotency_duplicate_and_conflict(bridge):
    await subscribe(bridge)
    first = await bridge.send_message(
        "once", metadata={"a": 1, "b": 2}, idempotency_key="client-request"
    )
    duplicate = await bridge.send_message(
        "once", metadata={"b": 2, "a": 1}, idempotency_key="client-request"
    )
    assert duplicate == first
    with pytest.raises(BridgeError) as caught:
        await bridge.send_message("different", idempotency_key="client-request")
    assert caught.value.code == "idempotency_conflict"
    assert caught.value.status_code == 409


async def test_concurrent_idempotent_submissions_create_one_outbox_entry(bridge, callback):
    await subscribe(bridge)
    results = await asyncio.gather(
        *(bridge.send_message("once concurrently", idempotency_key="shared-key") for _ in range(8))
    )
    assert len({result["request_id"] for result in results}) == 1
    assert await bridge.process_next()
    assert not await bridge.process_next()
    assert len(callback.events) == 1


async def test_concurrent_conflicting_idempotency_submissions_have_one_winner(bridge):
    await subscribe(bridge)
    results = await asyncio.gather(
        bridge.send_message("one", idempotency_key="shared-key"),
        bridge.send_message("two", idempotency_key="shared-key"),
        return_exceptions=True,
    )
    successes = [result for result in results if isinstance(result, dict)]
    errors = [result for result in results if isinstance(result, BridgeError)]
    assert len(successes) == len(errors) == 1
    assert errors[0].code == "idempotency_conflict"


async def test_reply_is_correlated_persistent_and_idempotent(bridge, clock):
    await subscribe(bridge)
    sent = await bridge.send_message("question")
    assert await bridge.process_next()
    first = await bridge.reply_to_app(sent["request_id"], "答えです")
    clock.advance(2)
    duplicate = await bridge.reply_to_app(sent["request_id"], "答えです")
    assert duplicate == first
    assert first["request_id"] == sent["request_id"]
    with pytest.raises(BridgeError) as caught:
        await bridge.reply_to_app(sent["request_id"], "different answer")
    assert caught.value.code == "reply_conflict"
    restarted = DotsBridge(bridge.settings)
    result = await restarted.get_message(sent["request_id"])
    assert result["reply"] == "答えです"
    assert result["reply_status"] == "replied"
    assert result["replied_at"] == first["replied_at"]


async def test_concurrent_duplicate_replies_store_one_final_result(bridge):
    await subscribe(bridge)
    sent = await bridge.send_message("question")
    results = await asyncio.gather(
        *(bridge.reply_to_app(sent["request_id"], "same answer") for _ in range(6))
    )
    assert all(result == results[0] for result in results)


async def test_reply_unknown_expired_and_invalid_text(bridge, clock):
    with pytest.raises(BridgeError) as caught:
        await bridge.reply_to_app("req_unknown", "answer")
    assert caught.value.code == "message_not_found"
    await subscribe(bridge)
    sent = await bridge.send_message("question")
    with pytest.raises(BridgeError) as caught:
        await bridge.reply_to_app(sent["request_id"], "   ")
    assert caught.value.code == "invalid_reply"
    clock.advance(bridge.settings.reply_timeout_seconds + 1)
    with pytest.raises(BridgeError) as caught:
        await bridge.reply_to_app(sent["request_id"], "late answer")
    assert caught.value.code == "reply_expired"
    assert caught.value.status_code == 410
    assert (await bridge.get_message(sent["request_id"]))["reply_status"] == "expired"


@pytest.mark.parametrize("deliver_first", [False, True])
async def test_unsubscribe_blocks_replies_and_cancels_pending_delivery(
    bridge, callback, deliver_first
):
    await subscribe(bridge)
    sent = await bridge.send_message("question")
    if deliver_first:
        assert await bridge.process_next()
    assert await bridge.unsubscribe(subscription_params(), bridge.owner_user_id) == {}
    assert await bridge.unsubscribe(subscription_params(), bridge.owner_user_id) == {}
    result = await bridge.get_message(sent["request_id"])
    assert result["delivery_status"] == ("accepted" if deliver_first else "cancelled")
    with pytest.raises(BridgeError) as caught:
        await bridge.reply_to_app(sent["request_id"], "answer")
    assert caught.value.code == "message_inactive"
    assert not await bridge.process_next()
    assert len(callback.events) == int(deliver_first)


async def test_unsubscribe_during_inflight_delivery_cannot_be_overwritten_by_receipt(
    bridge, callback
):
    await subscribe(bridge)
    sent = await bridge.send_message("in flight")
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_callback(url, body, headers):
        entered.set()
        await release.wait()
        return httpx.Response(202)

    bridge.webhook_post = slow_callback
    delivery = asyncio.create_task(bridge.process_next())
    await asyncio.wait_for(entered.wait(), 2)
    try:
        await bridge.unsubscribe(subscription_params(), bridge.owner_user_id)
    finally:
        release.set()
        await delivery
    assert (await bridge.get_message(sent["request_id"]))["delivery_status"] == "cancelled"


async def test_authorization_revocation_removes_all_subscriptions_and_blocks_pending_replies(
    bridge,
):
    await subscribe(bridge)
    await subscribe(bridge, channel="avatar", url="https://callback.example/avatar")
    first = await bridge.send_message("one")
    assert await bridge.process_next()
    second = await bridge.send_message("two", channel="avatar")
    await bridge.revoke_subscriptions()
    assert await bridge.list_subscriptions() == []
    assert (await bridge.get_message(first["request_id"]))["delivery_status"] == "accepted"
    pending = await bridge.get_message(second["request_id"])
    assert pending["delivery_status"] == "cancelled"
    assert pending["error"] == "authorization_revoked"
    assert not await bridge.process_next()
    for request in (first, second):
        with pytest.raises(BridgeError) as caught:
            await bridge.reply_to_app(request["request_id"], "answer")
        assert caught.value.code == "message_inactive"


async def test_expired_outbox_entry_is_never_delivered(bridge, callback, clock):
    await subscribe(bridge)
    sent = await bridge.send_message("too late to deliver")
    clock.advance(bridge.settings.reply_timeout_seconds + 1)
    assert not await bridge.process_next()
    result = await bridge.get_message(sent["request_id"])
    assert result["delivery_status"] == "failed"
    assert result["error"] == "delivery_expired"
    assert not callback.events


async def test_storage_key_mismatch_fails_before_using_persisted_data(bridge):
    settings = bridge.settings.model_copy(update={"storage_key": Fernet.generate_key().decode()})
    restarted = DotsBridge(settings)
    with pytest.raises(BridgeError) as caught:
        await restarted.initialize()
    assert caught.value.code == "storage_key_mismatch"


@pytest.mark.parametrize("cancel", ["unsubscribe", "revoke"])
async def test_resubscription_cannot_reactivate_old_accepted_requests(bridge, cancel):
    first = await subscribe(bridge)
    sent = await bridge.send_message("old generation")
    assert await bridge.process_next()
    if cancel == "unsubscribe":
        await bridge.unsubscribe(subscription_params(), bridge.owner_user_id)
    else:
        await bridge.revoke_subscriptions()
    assert (await subscribe(bridge))["id"] == first["id"]
    with pytest.raises(BridgeError) as caught:
        await bridge.reply_to_app(sent["request_id"], "stale answer")
    assert caught.value.code == "message_inactive"
    old = await bridge.get_message(sent["request_id"])
    assert old["delivery_status"] == "accepted"
    assert old["reply_status"] == "cancelled"
    current = await bridge.send_message("new generation")
    assert (await bridge.reply_to_app(current["request_id"], "current answer"))[
        "reply"
    ] == "current answer"


async def test_resubscription_after_expiry_cannot_revive_old_delivery_or_reply(bridge, clock):
    params = {**subscription_params(), "ttlMs": 1000}
    first = await bridge.subscribe(params, bridge.owner_user_id)
    accepted = await bridge.send_message("old accepted")
    assert await bridge.process_next()
    queued = await bridge.send_message("old queued")
    clock.advance(2)
    assert (await subscribe(bridge))["id"] == first["id"]
    assert not await bridge.process_next()
    assert (await bridge.get_message(queued["request_id"]))["delivery_status"] == "cancelled"
    for sent in (accepted, queued):
        assert (await bridge.get_message(sent["request_id"]))["reply_status"] == "cancelled"
        with pytest.raises(BridgeError) as caught:
            await bridge.reply_to_app(sent["request_id"], "stale answer")
        assert caught.value.code == "message_inactive"


async def test_normal_subscription_refresh_preserves_pending_requests(bridge, clock):
    await subscribe(bridge)
    sent = await bridge.send_message("still current")
    assert await bridge.process_next()
    clock.advance(301)
    await subscribe(bridge)
    result = await bridge.reply_to_app(sent["request_id"], "answer after refresh")
    assert result["reply"] == "answer after refresh"


async def test_removal_during_submission_cannot_create_orphaned_outbox_entry(bridge, monkeypatch):
    await subscribe(bridge)
    insert = bridge.store.insert_message

    async def remove_before_insert(message):
        await bridge.unsubscribe(subscription_params(), bridge.owner_user_id)
        return await insert(message)

    monkeypatch.setattr(bridge.store, "insert_message", remove_before_insert)
    with pytest.raises(BridgeError) as caught:
        await bridge.send_message("racing removal")
    assert caught.value.code == "no_subscription"
    count = await bridge.store.run(
        lambda db: db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    )
    assert count == 0


@pytest.mark.parametrize(
    "metadata",
    [
        {"bad": float("nan")},
        {"bad": float("inf")},
        {"bad": b"bytes"},
        {"bad": object()},
        {"bad": "\ud800"},
    ],
)
async def test_python_metadata_rejects_nonfinite_nonjson_and_invalid_unicode(bridge, metadata):
    await subscribe(bridge)
    with pytest.raises(BridgeError) as caught:
        await bridge.send_message("metadata validation", metadata=metadata)
    assert caught.value.code == "invalid_metadata"
    assert caught.value.status_code == 422


async def test_circular_python_metadata_is_rejected(bridge):
    metadata = {}
    metadata["self"] = metadata
    with pytest.raises(BridgeError) as caught:
        await bridge.send_message("circular metadata", metadata=metadata)
    assert caught.value.code == "invalid_metadata"


async def test_oversized_metadata_event_is_rejected_before_enqueue(bridge):
    await subscribe(bridge)
    with pytest.raises(BridgeError) as caught:
        await bridge.send_message("large metadata", metadata={"text": "あ" * 90000})
    assert caught.value.code == "message_too_large"
    assert caught.value.status_code == 413
    assert not await bridge.process_next()


async def test_python_reply_rejects_invalid_unicode(bridge):
    await subscribe(bridge)
    sent = await bridge.send_message("question")
    with pytest.raises(BridgeError) as caught:
        await bridge.reply_to_app(sent["request_id"], "\ud800")
    assert caught.value.code == "invalid_reply"
    assert caught.value.status_code == 422


async def test_worker_survives_unexpected_adapter_failure_and_recovers_its_lease(
    bridge, callback, clock, caplog
):
    await subscribe(bridge)
    sent = await bridge.send_message("worker resilience")
    failed = asyncio.Event()
    receipt = asyncio.Event()
    bridge.settings.worker_interval_seconds = 0.001
    delivery_result = bridge.store.delivery_result

    async def record_receipt(*args):
        await delivery_result(*args)
        receipt.set()

    bridge.store.delivery_result = record_receipt

    async def flaky_adapter(url, body, headers):
        if not failed.is_set():
            failed.set()
            raise RuntimeError("sensitive callback details must not be logged")
        return await callback(url, body, headers)

    bridge.webhook_post = flaky_adapter
    await bridge.start()
    try:
        await asyncio.wait_for(failed.wait(), 2)
        clock.advance(31)
        await asyncio.wait_for(receipt.wait(), 2)
        assert not bridge._worker.done()
        assert (await bridge.get_message(sent["request_id"]))["attempts"] == 2
    finally:
        await bridge.close()
    assert "RuntimeError" in caplog.text
    assert "sensitive callback details" not in caplog.text


async def test_schema_one_is_migrated_without_losing_queued_messages(bridge, callback):
    await subscribe(bridge)
    sent = await bridge.send_message("old schema data")

    def make_schema_one(db):
        db.execute("ALTER TABLE messages DROP COLUMN reply_cancelled")
        db.execute("UPDATE bridge_meta SET value=? WHERE key='schema'", (b"1",))

    await bridge.store.run(make_schema_one)
    restarted = DotsBridge(bridge.settings, webhook_post=callback)
    assert await restarted.process_next()
    result = await restarted.get_message(sent["request_id"])
    assert result["delivery_status"] == "accepted"
    assert result["reply_status"] == "pending"


async def test_concurrent_initialization_of_same_database_is_safe(settings, callback, clock):
    bridges = [DotsBridge(settings, webhook_post=callback) for _ in range(6)]
    await asyncio.gather(*(bridge.initialize() for bridge in bridges))
    assert all(bridge.store._ready for bridge in bridges)
