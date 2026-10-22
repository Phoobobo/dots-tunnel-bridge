"""Progress updates remain durable request messages without finalizing replies."""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager

import httpx
import pytest
from cryptography.fernet import Fernet

from dots_bridge import BridgeError, DotsBridge, Settings, create_app


@pytest.fixture
async def bridge(tmp_path):
    settings = Settings(
        owner_key="owner-" + "a" * 32,
        api_key="app-" + "b" * 32,
        storage_key=Fernet.generate_key().decode(),
        db_path=tmp_path / "bridge.sqlite3",
    )
    result = DotsBridge(settings)
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
    try:
        yield result
    finally:
        await result.close()


async def request(bridge, text="調査してください", *, metadata=None):
    return await bridge.send_message(text, channel="chat", metadata=metadata or {"source": "app"})


@asynccontextmanager
async def http_bridge(bridge):
    app = create_app(bridge.settings, bridge=bridge, start_worker=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url=bridge.settings.public_base_url,
            headers={"Authorization": "Bearer " + bridge.settings.api_key},
        ) as client:
            yield client


async def test_progress_preserves_request_context_and_requires_separate_final_reply(bridge):
    message = await request(bridge, metadata={"avatar": "ミント", "nested": [1, None]})
    request_id = message["request_id"]
    first = await bridge.report_progress(
        request_id, "まず資料を調べています。", idempotency_key="step-1"
    )
    assert first["kind"] == "progress"
    assert first["request_id"] == request_id
    assert first["channel"] == "chat"
    assert first["metadata"] == message["metadata"]
    assert first["text"] == "まず資料を調べています。"
    assert (
        await bridge.report_progress(
            request_id, "まず資料を調べています。", idempotency_key="step-1"
        )
        == first
    )
    pending = await bridge.get_message(request_id)
    assert pending["reply_status"] == "pending"
    assert pending["reply"] is None

    second = await bridge.report_progress(
        request_id, "資料が見つかりました。", idempotency_key="step-2"
    )
    final = await bridge.reply_to_app(request_id, "最終的な調査結果です。")
    assert final["reply"] == "最終的な調査結果です。"
    responses = await bridge.list_response_messages(request_id)
    assert responses[:2] == [first, second]
    assert [item["kind"] for item in responses] == ["progress", "progress", "reply"]
    assert responses[-1]["text"] == final["reply"]
    assert [item["sequence"] for item in responses] == sorted(
        item["sequence"] for item in responses
    )
    assert (await bridge.get_message(request_id))["reply"] == final["reply"]
    # Retries remain idempotent even after the separate final reply is saved.
    assert (
        await bridge.report_progress(
            request_id, "まず資料を調べています。", idempotency_key="step-1"
        )
        == first
    )
    with pytest.raises(BridgeError) as error:
        await bridge.report_progress(request_id, "余分な途中報告", idempotency_key="step-3")
    assert error.value.code == "reply_completed"
    assert error.value.status_code == 409


async def test_progress_conflicts_and_missing_requests_do_not_append_receipts(bridge):
    message = await request(bridge)
    first = await bridge.report_progress(message["request_id"], "途中報告", idempotency_key="step")
    with pytest.raises(BridgeError) as error:
        await bridge.report_progress(message["request_id"], "異なる内容", idempotency_key="step")
    assert error.value.code == "idempotency_conflict"
    with pytest.raises(BridgeError) as error:
        await bridge.report_progress("missing-request", "途中報告", idempotency_key="step")
    assert error.value.code == "message_not_found"
    assert error.value.status_code == 404
    assert await bridge.list_received() == [first]


@pytest.mark.parametrize(
    "changes",
    [
        {"request_id": None},
        {"request_id": " "},
        {"request_id": "x" * 201},
        {"request_id": "\ud800"},
        {"text": None},
        {"text": " \n\t"},
        {"text": "x" * 64001},
        {"text": "bad \udfff"},
        {"idempotency_key": None},
        {"idempotency_key": " "},
        {"idempotency_key": "x" * 129},
        {"idempotency_key": "\ud800"},
    ],
)
async def test_invalid_progress_is_safe_and_does_not_touch_storage(bridge, changes):
    with pytest.raises(BridgeError) as error:
        await bridge.report_progress(
            **{"request_id": "missing", "text": "hello", "idempotency_key": "key", **changes}
        )
    assert error.value.code == "invalid_progress"
    assert error.value.status_code == 422
    assert await bridge.list_received() == []


async def test_response_query_is_request_scoped_and_paginates_by_global_sequence(bridge):
    one = await request(bridge)
    other = await request(bridge, "別の依頼")
    first = await bridge.report_progress(one["request_id"], "開始", idempotency_key="one")
    await bridge.report_progress(other["request_id"], "別依頼の経過", idempotency_key="two")
    await bridge.send_to_app("独立した通知", channel="chat", idempotency_key="push")
    second = await bridge.report_progress(one["request_id"], "続き", idempotency_key="three")
    await bridge.reply_to_app(one["request_id"], "完了")
    responses = await bridge.list_response_messages(one["request_id"])
    assert [item["text"] for item in responses] == ["開始", "続き", "完了"]
    assert await bridge.list_response_messages(one["request_id"], limit=1) == [first]
    assert await bridge.list_response_messages(
        one["request_id"], after=first["sequence"], limit=1
    ) == [second]
    assert (
        await bridge.list_response_messages(one["request_id"], after=responses[-1]["sequence"])
        == []
    )


@pytest.mark.parametrize(
    "options",
    [
        {"after": True},
        {"after": "0"},
        {"after": -1},
        {"after": 1.0},
        {"after": 2**63},
        {"limit": False},
        {"limit": "1"},
        {"limit": 0},
        {"limit": 101},
    ],
)
async def test_bridge_response_query_validates_limits_strictly(bridge, options):
    with pytest.raises(BridgeError) as error:
        await bridge.list_response_messages("request", **options)
    assert error.value.code == "invalid_received_query"
    assert error.value.status_code == 422


async def test_report_progress_wakes_an_idle_embedded_receiver(bridge, monkeypatch):
    message = await request(bridge)
    idle = asyncio.Event()
    received = asyncio.Event()
    seen = []
    original = bridge.store.list_received

    async def list_received(**options):
        result = await original(**options)
        if not result:
            idle.set()
        return result

    monkeypatch.setattr(bridge.store, "list_received", list_received)

    @bridge.on_received
    async def callback(item):
        seen.append(item)
        received.set()

    await bridge.start_receiving(consumer_id="progress-app", channel="chat", poll_interval=60)
    async with asyncio.timeout(1):
        await idle.wait()
    progress = await bridge.report_progress(
        message["request_id"], "途中の通知", idempotency_key="wake"
    )
    async with asyncio.timeout(1):
        await received.wait()
    assert len(seen) == 1
    assert seen[0].model_dump() == progress
    assert (await bridge.get_message(message["request_id"]))["reply"] is None


async def test_http_response_messages_require_auth_and_keep_final_status_separate(bridge):
    message = await request(bridge)
    request_id = message["request_id"]
    progress = await bridge.report_progress(
        request_id, "検索しています。", idempotency_key="http-step"
    )
    path = f"/api/messages/{request_id}/responses"
    async with http_bridge(bridge) as client:
        unauthenticated = await client.get(path, headers={"Authorization": ""})
        assert unauthenticated.status_code == 401
        assert unauthenticated.headers["www-authenticate"] == "Bearer"
        response = await client.get(path)
        assert response.status_code == 200
        assert response.json() == {"messages": [progress]}
        assert (await client.get(f"/api/messages/{request_id}")).json()["reply"] is None
        await bridge.reply_to_app(request_id, "最終回答")
        assert (await client.get(path, params={"after": progress["sequence"], "limit": 1})).json()[
            "messages"
        ][0]["kind"] == "reply"
        assert (await client.get(f"/api/messages/{request_id}")).json()["reply"] == "最終回答"
        missing = await client.get("/api/messages/missing/responses")
        assert missing.status_code == 404
        assert missing.json()["code"] == "message_not_found"


@pytest.mark.parametrize(
    "query",
    [
        "after=-1",
        "after=1.5",
        "after=9223372036854775808",
        "limit=0",
        "limit=101",
        "limit=no",
        "channel=chat",
        "after=0&after=1",
        "limit=1&limit=2",
        "unknown=value",
    ],
)
async def test_http_response_queries_reject_bad_repeated_and_unsupported_fields(bridge, query):
    async with http_bridge(bridge) as client:
        response = await client.get("/api/messages/request/responses?" + query)
        assert response.status_code == 422
        assert response.json()["code"] == "invalid_received_query"
