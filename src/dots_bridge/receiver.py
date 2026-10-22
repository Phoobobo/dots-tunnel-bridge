"""Durable, ordered dispatch of replies and push messages from dots."""

from __future__ import annotations

import asyncio
import inspect
import logging
import math
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
from pydantic import ValidationError

from .errors import BridgeError
from .models import ReceivedMessage

logger = logging.getLogger(__name__)
ReceivedHandler = Callable[[ReceivedMessage], Any]


def _retryable(error: Exception) -> bool:
    if isinstance(error, BridgeError):
        return error.code != "invalid_response" and (
            error.status_code in {408, 425, 429} or error.status_code >= 500
        )
    return isinstance(error, httpx.RequestError)


def _cursor(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise BridgeError("invalid_response", "Bridge returned an invalid receive cursor.", 502)
    return value


def _received(value: Any) -> list[ReceivedMessage]:
    if not isinstance(value, list):
        raise BridgeError("invalid_response", "Bridge returned an invalid received list.", 502)
    try:
        return [ReceivedMessage.model_validate(item) for item in value]
    except ValidationError:
        raise BridgeError(
            "invalid_response", "Bridge returned an invalid received message.", 502
        ) from None


class ReceivedDispatcher:
    """Run callbacks before acknowledging durable receipts.

    All callbacks run in registration order. If one fails, the whole receipt is
    retried, so callbacks must tolerate duplicate delivery. Synchronous callbacks
    run in a worker thread; cancellation cannot interrupt a running sync function.
    Each concurrently running application should use its own consumer ID.
    """

    def __init__(
        self,
        *,
        list_received: Callable[..., Awaitable[list[ReceivedMessage | dict[str, Any]]]],
        get_cursor: Callable[..., Awaitable[int]],
        acknowledge: Callable[..., Awaitable[int]],
    ) -> None:
        self._list_received = list_received
        self._get_cursor = get_cursor
        self._acknowledge = acknowledge
        self._handlers: list[ReceivedHandler] = []
        self._task: asyncio.Task[None] | None = None
        self._stopping_task: asyncio.Task[None] | None = None
        self._options: tuple[str, str | None, float] | None = None
        self._error: Exception | None = None
        self._lock = asyncio.Lock()
        self._wake = asyncio.Event()
        self._stop_requested = False

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    @property
    def error(self) -> Exception | None:
        """The terminal background error, cleared by the next successful start."""
        return self._error

    def on_received(self, handler: ReceivedHandler) -> ReceivedHandler:
        """Register a sync or async callback, also usable as a decorator."""
        if not callable(handler):
            raise TypeError("on_received requires a callable")
        self._handlers.append(handler)
        return handler

    def notify(self) -> None:
        """Wake an idle poll when a receipt is created in the same event loop."""
        self._wake.set()

    async def start(
        self,
        *,
        consumer_id: str = "default",
        channel: str | None = None,
        poll_interval: float = 0.5,
    ) -> None:
        if not isinstance(consumer_id, str) or not consumer_id or consumer_id in {".", ".."}:
            raise ValueError("consumer_id must be a nonempty consumer ID")
        if channel is not None and (not isinstance(channel, str) or not channel):
            raise ValueError("channel must be a nonempty string or None")
        if (
            isinstance(poll_interval, bool)
            or not isinstance(poll_interval, (int, float))
            or not math.isfinite(poll_interval)
            or poll_interval <= 0
        ):
            raise ValueError("poll_interval must be a positive finite number")
        options = (consumer_id, channel, float(poll_interval))
        async with self._lock:
            if self.running:
                if self._stop_requested:
                    raise RuntimeError(
                        "Reception is stopping; await stop_receiving() before restart"
                    )
                if options != self._options:
                    raise RuntimeError("Stop receiving before changing receive options")
                return
            if not self._handlers:
                raise RuntimeError("Register an on_received callback before starting reception")
            # Fetch before spawning so bad credentials/configuration fail at startup.
            try:
                cursor = _cursor(await self._get_cursor(consumer_id=consumer_id, channel=channel))
            except Exception as error:
                self._error = error
                raise
            self._error = None
            self._options = options
            self._stop_requested = False
            self._stopping_task = None
            self._wake.clear()
            self._task = asyncio.create_task(
                self._run(cursor, consumer_id, channel, float(poll_interval)),
                name="dots-bridge-receiver",
            )

    async def stop(self) -> None:
        async with self._lock:
            task = self._task
            if task is None:
                return
            self._stop_requested = True
            self._wake.set()
            if task is asyncio.current_task():
                # A callback may stop reception after the current receipt is acked.
                return
            if self._stopping_task is not task:
                self._stopping_task = task
                task.cancel()
        # Callback cancellation cleanup may call stop/aclose itself. Never hold
        # the lifecycle lock while waiting for that callback to finish.
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            current = asyncio.current_task()
            if current is not None and current.cancelling():
                raise
        finally:
            async with self._lock:
                if self._task is task and task.done():
                    self._task = None
                    self._stopping_task = None

    async def _wait(self, delay: float) -> None:
        try:
            async with asyncio.timeout(delay):
                await self._wake.wait()
        except TimeoutError:
            pass
        self._wake.clear()

    async def _retry(self, error: Exception, failures: int, interval: float) -> int:
        # Exception messages may contain request bodies, tokens, or user content.
        logger.warning("Dots receive operation failed (%s); retrying", type(error).__name__)
        await self._wait(min(max(interval, 0.05) * 2 ** min(failures, 10), 30.0))
        return failures + 1

    async def _call_handlers(self, message: ReceivedMessage) -> None:
        # Registration during a callback only affects subsequent attempts/messages.
        for handler in tuple(self._handlers):
            if inspect.iscoroutinefunction(handler):
                result = handler(message)
            else:
                result = await asyncio.to_thread(handler, message)
            if inspect.isawaitable(result):
                await result

    async def _run(
        self, cursor: int, consumer_id: str, channel: str | None, interval: float
    ) -> None:
        failures = 0
        try:
            while not self._stop_requested:
                try:
                    messages = _received(
                        await self._list_received(after=cursor, limit=100, channel=channel)
                    )
                except Exception as error:
                    if not _retryable(error):
                        raise
                    failures = await self._retry(error, failures, interval)
                    continue
                retry = False
                previous_cursor = cursor
                for message in sorted(messages, key=lambda item: item.sequence):
                    if message.sequence <= cursor:
                        continue
                    if channel is not None and message.channel != channel:
                        raise BridgeError(
                            "invalid_response",
                            "Bridge returned a message for another channel.",
                            502,
                        )
                    try:
                        await self._call_handlers(message)
                    except Exception as error:
                        failures = await self._retry(error, failures, interval)
                        retry = True
                        break
                    try:
                        acknowledged = _cursor(
                            await self._acknowledge(
                                consumer_id=consumer_id, sequence=message.sequence, channel=channel
                            )
                        )
                        if acknowledged < message.sequence:
                            raise BridgeError(
                                "invalid_response", "Bridge did not acknowledge the message.", 502
                            )
                        cursor = acknowledged
                    except Exception as error:
                        if not _retryable(error):
                            raise
                        failures = await self._retry(error, failures, interval)
                        retry = True
                        break
                    failures = 0
                    if self._stop_requested:
                        return
                if not retry:
                    failures = 0
                    # Full batches may have more immediately available receipts.
                    if len(messages) < 100 or cursor == previous_cursor:
                        await self._wait(interval)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._error = error
            logger.error("Dots reception stopped (%s)", type(error).__name__)
