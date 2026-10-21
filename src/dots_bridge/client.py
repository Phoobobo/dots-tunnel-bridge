"""HTTP clients for applications sending requests to dots through the bridge."""

from __future__ import annotations

import asyncio
import math
import time
from typing import Any
from urllib.parse import quote

import httpx

from .errors import BridgeError
from .models import MessageInput, ReceivedMessage
from .receiver import ReceivedDispatcher, ReceivedHandler, _cursor, _received


class ReplyTimeoutError(TimeoutError):
    """Waiting ended; ``request_id`` can be used to resume without resending."""

    def __init__(self, request_id: str) -> None:
        self.request_id = request_id
        super().__init__(
            f"Timed out waiting for dots reply to {request_id}; resume with this request_id."
        )


def _positive(value: float, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a positive finite number")
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return float(value)


def _message_path(request_id: str) -> str:
    if not isinstance(request_id, str) or not request_id or request_id in {".", ".."}:
        raise ValueError("request_id must be a nonempty message ID")
    return "/api/messages/" + quote(request_id, safe="")


def _consumer_path(consumer_id: str) -> str:
    if not isinstance(consumer_id, str) or not consumer_id or consumer_id in {".", ".."}:
        raise ValueError("consumer_id must be a nonempty consumer ID")
    return "/api/consumers/" + quote(consumer_id, safe="")


def _received_params(after: int, limit: int, channel: str | None) -> dict[str, Any]:
    if isinstance(after, bool) or not isinstance(after, int) or after < 0:
        raise ValueError("after must be a nonnegative integer")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
        raise ValueError("limit must be an integer between 1 and 100")
    result: dict[str, Any] = {"after": after, "limit": limit}
    result.update(_channel_params(channel))
    return result


def _channel_params(channel: str | None) -> dict[str, str]:
    if channel is None:
        return {}
    if not isinstance(channel, str) or not channel:
        raise ValueError("channel must be a nonempty string or None")
    return {"channel": channel}


def _ack_payload(sequence: int, channel: str | None) -> dict[str, Any]:
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 0:
        raise ValueError("sequence must be a nonnegative integer")
    _channel_params(channel)
    return {"sequence": sequence, "channel": channel}


def _decode(response: httpx.Response) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError:
        body = None
    if not response.is_success:
        error = body.get("error", body) if isinstance(body, dict) else {}
        error = error if isinstance(error, dict) else {}
        code = error.get("code")
        message = error.get("message")
        raise BridgeError(
            code if isinstance(code, str) else f"http_{response.status_code}",
            message
            if isinstance(message, str)
            else f"Bridge request failed: HTTP {response.status_code}.",
            status_code=response.status_code,
        )
    if not isinstance(body, dict):
        raise BridgeError("invalid_response", "Bridge returned an invalid JSON response.", 502)
    return body


def _subscriptions(body: dict[str, Any]) -> list[dict[str, Any]]:
    result = body.get("subscriptions")
    if not isinstance(result, list) or any(not isinstance(item, dict) for item in result):
        raise BridgeError("invalid_response", "Bridge returned an invalid subscription list.", 502)
    return result


def _has_reply(message: dict[str, Any]) -> bool:
    if message.get("reply_status") == "replied" and isinstance(message.get("reply"), str):
        return True
    if message.get("reply_status") == "expired":
        raise BridgeError("reply_expired", "This request's reply window has expired.", 410)
    if message.get("reply_status") == "cancelled":
        raise BridgeError("reply_cancelled", "Replies to this request have been cancelled.", 409)
    delivery = message.get("delivery_status")
    if delivery in {"failed", "cancelled"}:
        raise BridgeError(
            f"delivery_{delivery}",
            f"Delivery to dots is {delivery}; inspect this request before retrying.",
            409,
        )
    return False


def _payload(
    text: str,
    channel: str,
    subscription_id: str | None,
    metadata: dict[str, Any] | None,
    idempotency_key: str | None,
) -> dict[str, Any]:
    return MessageInput(
        text=text,
        channel=channel,
        subscription_id=subscription_id,
        metadata=metadata if metadata is not None else {},
        idempotency_key=idempotency_key,
    ).model_dump(mode="json", exclude_none=True)


class BridgeClient:
    """Synchronous application client. Use as a context manager to close HTTP resources."""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8765",
        *,
        api_key: str,
        timeout: float = 10,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._timeout = _positive(timeout, "timeout")
        self._client = httpx.Client(
            base_url=base_url,
            headers={"Authorization": "Bearer " + api_key},
            timeout=self._timeout,
            follow_redirects=False,
            transport=transport,
        )

    def __enter__(self) -> BridgeClient:
        self._client.__enter__()
        return self

    def __exit__(self, *args: Any) -> None:
        self._client.__exit__(*args)

    def close(self) -> None:
        self._client.close()

    def list_subscriptions(self) -> list[dict[str, Any]]:
        return _subscriptions(_decode(self._client.get("/api/subscriptions")))

    def list_received(
        self, *, after: int = 0, limit: int = 100, channel: str | None = None
    ) -> list[ReceivedMessage]:
        """Read replies and pushes after a sequence, without acknowledging them."""
        body = _decode(
            self._client.get("/api/received", params=_received_params(after, limit, channel))
        )
        return _received(body.get("messages"))

    def get_consumer_cursor(self, consumer_id: str, channel: str | None = None) -> int:
        """Return this consumer's last acknowledged receipt sequence."""
        body = _decode(
            self._client.get(_consumer_path(consumer_id), params=_channel_params(channel))
        )
        return _cursor(body.get("cursor"))

    def acknowledge_received(
        self, consumer_id: str, sequence: int, channel: str | None = None
    ) -> int:
        """Advance one consumer's cursor after successful processing."""
        body = _decode(
            self._client.post(
                _consumer_path(consumer_id) + "/ack", json=_ack_payload(sequence, channel)
            )
        )
        return _cursor(body.get("cursor"))

    def send_message(
        self,
        text: str,
        *,
        channel: str = "default",
        subscription_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Queue one message and return its status, including the request_id."""
        return _decode(
            self._client.post(
                "/api/messages",
                json=_payload(text, channel, subscription_id, metadata, idempotency_key),
            )
        )

    def get_message(self, request_id: str) -> dict[str, Any]:
        """Fetch delivery and reply status for an existing request."""
        return _decode(self._client.get(_message_path(request_id)))

    def wait_for_reply(
        self, request_id: str, *, timeout: float = 120, poll_interval: float = 0.5
    ) -> dict[str, Any]:
        """Return the full replied message without resending on timeout.

        ``timeout`` is the polling budget. HTTPX applies the remaining budget to
        each network operation; synchronous I/O has no hard wall cancellation.
        Replies received after the polling deadline raise ReplyTimeoutError.
        """
        deadline = time.monotonic() + _positive(timeout, "timeout")
        interval = _positive(poll_interval, "poll_interval")
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ReplyTimeoutError(request_id)
            try:
                message = _decode(
                    self._client.get(
                        _message_path(request_id), timeout=min(self._timeout, remaining)
                    )
                )
            except httpx.TimeoutException:
                if time.monotonic() >= deadline:
                    raise ReplyTimeoutError(request_id) from None
                raise
            if time.monotonic() >= deadline:
                raise ReplyTimeoutError(request_id)
            if _has_reply(message):
                return message
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ReplyTimeoutError(request_id)
            time.sleep(min(interval, remaining))

    def ask(
        self,
        text: str,
        *,
        timeout: float = 120,
        poll_interval: float = 0.5,
        channel: str = "default",
        subscription_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> str:
        """Send once, wait for the final reply, and return its text."""
        _positive(timeout, "timeout")
        _positive(poll_interval, "poll_interval")
        message = self.send_message(
            text,
            channel=channel,
            subscription_id=subscription_id,
            metadata=metadata,
            idempotency_key=idempotency_key,
        )
        result = self.wait_for_reply(
            message["request_id"], timeout=timeout, poll_interval=poll_interval
        )
        return result["reply"]


class AsyncBridgeClient:
    """Async client supporting independent sending and background receive callbacks."""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8765",
        *,
        api_key: str,
        timeout: float = 10,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._timeout = _positive(timeout, "timeout")
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": "Bearer " + api_key},
            timeout=self._timeout,
            follow_redirects=False,
            transport=transport,
        )
        self._receiver = ReceivedDispatcher(
            list_received=self.list_received,
            get_cursor=self.get_consumer_cursor,
            acknowledge=self.acknowledge_received,
        )

    async def __aenter__(self) -> AsyncBridgeClient:
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop_receiving()
        await self._client.__aexit__(*args)

    async def aclose(self) -> None:
        await self.stop_receiving()
        await self._client.aclose()

    async def list_subscriptions(self) -> list[dict[str, Any]]:
        return _subscriptions(_decode(await self._client.get("/api/subscriptions")))

    @property
    def receiving(self) -> bool:
        """Whether the background receiver is running."""
        return self._receiver.running

    @property
    def receiver_error(self) -> Exception | None:
        """The terminal background error, if reception stopped unexpectedly."""
        return self._receiver.error

    def on_received(self, handler: ReceivedHandler) -> ReceivedHandler:
        """Register a callback for both replies and unsolicited messages from dots."""
        return self._receiver.on_received(handler)

    async def start_receiving(
        self,
        *,
        consumer_id: str = "default",
        channel: str | None = None,
        poll_interval: float = 0.5,
    ) -> None:
        """Start background dispatch, resuming this consumer's durable cursor.

        Callback delivery is at least once: all callbacks must succeed before a
        receipt is acknowledged. Failed callbacks are retried with backoff.
        """
        await self._receiver.start(
            consumer_id=consumer_id, channel=channel, poll_interval=poll_interval
        )

    async def stop_receiving(self) -> None:
        """Cancel and await background reception; leave pending receipts unacked."""
        await self._receiver.stop()

    async def list_received(
        self, *, after: int = 0, limit: int = 100, channel: str | None = None
    ) -> list[ReceivedMessage]:
        """Read replies and pushes after a sequence, without acknowledging them."""
        body = _decode(
            await self._client.get("/api/received", params=_received_params(after, limit, channel))
        )
        return _received(body.get("messages"))

    async def get_consumer_cursor(self, consumer_id: str, channel: str | None = None) -> int:
        """Return this consumer's last acknowledged receipt sequence."""
        body = _decode(
            await self._client.get(_consumer_path(consumer_id), params=_channel_params(channel))
        )
        return _cursor(body.get("cursor"))

    async def acknowledge_received(
        self, consumer_id: str, sequence: int, channel: str | None = None
    ) -> int:
        """Advance one consumer's cursor after successful processing."""
        body = _decode(
            await self._client.post(
                _consumer_path(consumer_id) + "/ack", json=_ack_payload(sequence, channel)
            )
        )
        return _cursor(body.get("cursor"))

    async def send_message(
        self,
        text: str,
        *,
        channel: str = "default",
        subscription_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Queue one message and return its status, including the request_id."""
        return _decode(
            await self._client.post(
                "/api/messages",
                json=_payload(text, channel, subscription_id, metadata, idempotency_key),
            )
        )

    async def get_message(self, request_id: str) -> dict[str, Any]:
        """Fetch delivery and reply status for an existing request."""
        return _decode(await self._client.get(_message_path(request_id)))

    async def wait_for_reply(
        self,
        request_id: str,
        *,
        timeout: float = 120,  # noqa: ASYNC109 - Match the sync API; bounded by asyncio.timeout.
        poll_interval: float = 0.5,
    ) -> dict[str, Any]:
        """Return the full replied message; a timeout never sends another message."""
        duration = _positive(timeout, "timeout")
        deadline = time.monotonic() + duration
        interval = _positive(poll_interval, "poll_interval")
        try:
            async with asyncio.timeout(duration):
                return await self._poll_reply(request_id, deadline, interval)
        except TimeoutError:
            raise ReplyTimeoutError(request_id) from None

    async def _poll_reply(
        self, request_id: str, deadline: float, interval: float
    ) -> dict[str, Any]:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ReplyTimeoutError(request_id)
            try:
                message = _decode(
                    await self._client.get(
                        _message_path(request_id), timeout=min(self._timeout, remaining)
                    )
                )
            except httpx.TimeoutException:
                if time.monotonic() >= deadline:
                    raise ReplyTimeoutError(request_id) from None
                raise
            if _has_reply(message):
                return message
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ReplyTimeoutError(request_id)
            await asyncio.sleep(min(interval, remaining))

    async def ask(
        self,
        text: str,
        *,
        timeout: float = 120,  # noqa: ASYNC109 - Forwarded to wait_for_reply's bounded deadline.
        poll_interval: float = 0.5,
        channel: str = "default",
        subscription_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> str:
        """Send once, wait for the final reply, and return its text."""
        _positive(timeout, "timeout")
        _positive(poll_interval, "poll_interval")
        message = await self.send_message(
            text,
            channel=channel,
            subscription_id=subscription_id,
            metadata=metadata,
            idempotency_key=idempotency_key,
        )
        result = await self.wait_for_reply(
            message["request_id"], timeout=timeout, poll_interval=poll_interval
        )
        return result["reply"]
