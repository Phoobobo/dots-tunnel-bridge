"""Opt-in, acknowledged push delivery on a connector WebSocket connection."""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
from pydantic import ValidationError

from ..client import AsyncBridgeClient
from ..errors import BridgeError
from ..models import ReceivedConsumer, ReceivedMessage
from ._openai_protocol import ProtocolError

logger = logging.getLogger(__name__)
CONSUMER_PREFIX = "openai-ws:"


def _retryable(error: Exception) -> bool:
    if isinstance(error, BridgeError):
        return error.code != "invalid_response" and (
            error.status_code in {408, 425, 429} or error.status_code >= 500
        )
    if isinstance(
        error, (httpx.UnsupportedProtocol, httpx.LocalProtocolError, httpx.TooManyRedirects)
    ):
        return False
    return isinstance(error, httpx.RequestError)


def _cursor(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 2**63 - 1:
        raise BridgeError("invalid_response", "Bridge returned an invalid push cursor.", 502)
    return value


def _failure_event(error: Exception) -> dict[str, Any]:
    if isinstance(error, BridgeError):
        status = error.status_code if error.status_code in {400, 404, 409, 410, 413, 422} else 502
        if status == 422:
            status = 400
        problem = {
            "message": error.message,
            "type": "server_error" if status >= 500 else "invalid_request_error",
            "param": None,
            "code": error.code,
        }
    else:
        status = 502
        problem = {
            "message": "Push reception stopped. Subscribe again to resume delivery.",
            "type": "server_error",
            "param": None,
            "code": "push_reception_failed",
        }
    return {"type": "error", "status": status, "error": problem}


class _PushSendFailed(Exception):
    def __init__(self, error: Exception) -> None:
        self.error = error


class WebSocketPushSession:
    """Deliver one durable push at a time, advancing only on a client ACK.

    The push consumer is independent of normal Responses requests and other
    bridge applications. A completed WebSocket send is not an acknowledgement.
    Use a stable public consumer ID when reconnecting to recover unacked pushes.
    """

    def __init__(
        self,
        client: AsyncBridgeClient,
        *,
        channel: str,
        send_event: Callable[[dict[str, Any]], Awaitable[None]],
        consumer_registry: set[str],
        poll_interval: float = 0.5,
    ) -> None:
        if not isinstance(channel, str) or not channel:
            raise ValueError("channel must be a nonempty string")
        ReceivedConsumer(consumer_id="validate", channel=channel)
        if (
            isinstance(poll_interval, bool)
            or not isinstance(poll_interval, (int, float))
            or not math.isfinite(poll_interval)
            or poll_interval <= 0
        ):
            raise ValueError("poll_interval must be a positive finite number")
        self._client = client
        self._channel = channel
        self._send_event = send_event
        self._registry = consumer_registry
        self._poll_interval = float(poll_interval)
        self._consumer_id: str | None = None
        self._registry_key: str | None = None
        self._registered = False
        self._cursor = 0
        self._task: asyncio.Task[None] | None = None
        self._error: Exception | None = None
        self._pending: ReceivedMessage | None = None
        self._pending_announced = False
        self._pending_persisted = False
        self._last_acked: ReceivedMessage | None = None
        self._acknowledged = asyncio.Event()
        self._lock = asyncio.Lock()
        self._closed = False

    @property
    def task(self) -> asyncio.Task[None] | None:
        return self._task

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def error(self) -> Exception | None:
        return self._error

    def _release_registry(self) -> None:
        if self._registered and self._registry_key is not None:
            self._registry.discard(self._registry_key)
            self._registered = False

    async def subscribe(self, consumer_id: str) -> None:
        try:
            ReceivedConsumer(consumer_id=consumer_id, channel=self._channel)
            if len(consumer_id) > 118:
                raise ValueError("consumer_id too long")
        except (ValidationError, ValueError):
            raise ProtocolError(
                "consumer_id must contain 1 to 118 letters, digits, underscores, "
                "hyphens, periods, or colons.",
                param="consumer_id",
                code="invalid_push_consumer",
            ) from None
        async with self._lock:
            if self._closed:
                raise ProtocolError("Push session is closed.", status_code=409)
            if self._consumer_id is not None and self._consumer_id != consumer_id:
                raise ProtocolError(
                    "This connection is already subscribed with another consumer ID.",
                    status_code=409,
                    code="push_consumer_conflict",
                )
            if self.running:
                await self._send_event(self._subscribed_event(consumer_id))
                return
            registry_key = CONSUMER_PREFIX + consumer_id
            if registry_key in self._registry:
                raise ProtocolError(
                    "This push consumer already has an active connection.",
                    status_code=409,
                    code="push_consumer_conflict",
                )
            self._registry.add(registry_key)
            self._registry_key = registry_key
            self._registered = True
            try:
                cursor = _cursor(
                    await self._client.get_consumer_cursor(registry_key, channel=self._channel)
                )
                self._cursor = cursor
                await self._send_event(self._subscribed_event(consumer_id))
                if self._closed:
                    raise ProtocolError("Push session is closed.", status_code=409)
            except BaseException:
                self._release_registry()
                raise
            self._consumer_id = consumer_id
            self._error = None
            self._pending = None
            self._pending_announced = False
            self._pending_persisted = False
            self._acknowledged.clear()
            self._task = asyncio.create_task(self._run(), name="dots-bridge-openai-push")

    def _subscribed_event(self, consumer_id: str) -> dict[str, Any]:
        return {
            "type": "dots.subscribed",
            "consumer_id": consumer_id,
            "channel": self._channel,
            "cursor": self._cursor,
        }

    def _acked_event(self, message: ReceivedMessage) -> dict[str, Any]:
        return {
            "type": "dots.push.acked",
            "consumer_id": self._consumer_id,
            "message_id": message.id,
            "sequence": message.sequence,
        }

    async def acknowledge(self, message_id: str) -> None:
        async with self._lock:
            if self._closed or not isinstance(message_id, str) or not message_id:
                raise ProtocolError("Unknown push message_id.", code="invalid_push_ack")
            if self._last_acked is not None and self._last_acked.id == message_id:
                await self._send_event(self._acked_event(self._last_acked))
                return
            message = self._pending
            if message is None or not self._pending_announced or message.id != message_id:
                raise ProtocolError("Unknown push message_id.", code="invalid_push_ack")
            if not self._pending_persisted:
                await self._advance(message.sequence)
                self._pending_persisted = True
            # Keep pending until this notification succeeds, even after a durable
            # ACK. Retrying the same ACK can finish the interrupted notification.
            await self._send_event(self._acked_event(message))
            self._last_acked = message
            self._pending = None
            self._pending_announced = False
            self._pending_persisted = False
            self._acknowledged.set()

    async def _advance(self, sequence: int) -> None:
        assert self._registry_key is not None
        cursor = _cursor(
            await self._client.acknowledge_received(
                self._registry_key, sequence, channel=self._channel
            )
        )
        if cursor != sequence:
            raise BridgeError(
                "invalid_response", "Bridge returned an unexpected push consumer cursor.", 502
            )
        self._cursor = cursor

    async def close(self) -> None:
        self._closed = True
        self._acknowledged.set()
        task = self._task
        if task is None:
            self._release_registry()
            return
        if task is asyncio.current_task():
            return  # _run's finally releases this task's reservation.
        task.cancel()
        try:
            await asyncio.gather(task, return_exceptions=True)
        finally:
            if task.done():
                self._release_registry()

    async def _messages(self) -> list[ReceivedMessage]:
        messages = await self._client.list_received(
            after=self._cursor, limit=100, channel=self._channel
        )
        try:
            if not isinstance(messages, list):
                raise ValueError("Expected a received list")
            result = [ReceivedMessage.model_validate(message) for message in messages]
        except (ValidationError, ValueError):
            raise BridgeError(
                "invalid_response", "Bridge returned invalid received messages.", 502
            ) from None
        if any(
            message.channel != self._channel
            or (message.kind == "push" and message.request_id is not None)
            for message in result
        ):
            raise BridgeError("invalid_response", "Bridge returned an invalid push message.", 502)
        return sorted(result, key=lambda message: message.sequence)

    async def _batch(self) -> bool:
        previous_cursor = self._cursor
        messages = await self._messages()
        for message in messages:
            if self._closed:
                return False
            if message.sequence <= self._cursor:
                continue
            if message.kind in {"progress", "reply"}:
                async with self._lock:
                    # Receipt order plus one pending push prevents a reply ACK
                    # from skipping any unacknowledged push in this consumer.
                    await self._advance(message.sequence)
                continue
            async with self._lock:
                self._pending = message
                self._pending_announced = False
                self._pending_persisted = False
                self._acknowledged.clear()
                try:
                    await self._send_event(
                        {
                            "type": "dots.push",
                            "consumer_id": self._consumer_id,
                            "message": message.model_dump(mode="json"),
                        }
                    )
                except Exception as error:
                    raise _PushSendFailed(error) from None
                self._pending_announced = True
            await self._acknowledged.wait()
            self._acknowledged.clear()
        return len(messages) == 100 and self._cursor != previous_cursor

    async def _run(self) -> None:
        failures = 0
        try:
            while not self._closed:
                try:
                    more = await self._batch()
                except _PushSendFailed as error:
                    raise error.error from None
                except Exception as error:
                    if not _retryable(error):
                        raise
                    logger.warning(
                        "WebSocket push poll failed (%s); retrying", type(error).__name__
                    )
                    await asyncio.sleep(
                        min(max(self._poll_interval, 0.05) * 2 ** min(failures, 10), 30.0)
                    )
                    failures += 1
                    continue
                failures = 0
                if not more:
                    await asyncio.sleep(self._poll_interval)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._error = error
            logger.error("WebSocket push reception stopped (%s)", type(error).__name__)
            try:
                await self._send_event(_failure_event(error))
            except Exception as send_error:
                logger.warning("Push error notification failed (%s)", type(send_error).__name__)
        finally:
            self._release_registry()
