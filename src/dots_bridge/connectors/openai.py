"""Text-only OpenAI-compatible endpoints backed by the bridge's public HTTP client.

This is a separate ASGI application. It does not mount or operate the MCP server,
open its database, or call the OpenAI API.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import urlsplit

import httpx
from anyio import CancelScope
from starlette.applications import Starlette
from starlette.requests import HTTPConnection, Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route, WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect

from ..client import AsyncBridgeClient, ReplyTimeoutError, _positive
from ..errors import BridgeError
from ..models import MessageInput
from ._openai_protocol import (
    ProtocolError,
    chat_chunks,
    chat_completion,
    latest_chat_text,
    latest_response_text,
    response_finish_events,
    response_object,
    response_start_events,
)
from ._websocket_push import WebSocketPushSession

MAX_REQUEST_BYTES = 300 * 1024
MAX_QUEUED_RESPONSES = 16
HEARTBEAT_SECONDS = 15.0


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _reject_constant(value: str) -> None:
    raise ValueError("Non-finite JSON number")


def _parse_json(raw: str | bytes) -> dict[str, Any]:
    if len(raw.encode() if isinstance(raw, str) else raw) > MAX_REQUEST_BYTES:
        raise ProtocolError("Request body exceeds 300 KiB.", status_code=413)
    try:
        result = json.loads(raw, parse_constant=_reject_constant)
    except (ValueError, UnicodeError, RecursionError):
        raise ProtocolError("Expected a valid JSON object.") from None
    if not isinstance(result, dict):
        raise ProtocolError("Expected a JSON object.")
    return result


async def _read_json(request: Request) -> dict[str, Any]:
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > MAX_REQUEST_BYTES:
            raise ProtocolError("Request body exceeds 300 KiB.", status_code=413)
    return _parse_json(bytes(body))


def _authorize(connection: HTTPConnection, api_key: str) -> None:
    scheme, _, token = connection.headers.get("authorization", "").partition(" ")
    if scheme.lower() != "bearer" or not secrets.compare_digest(token.encode(), api_key.encode()):
        raise ProtocolError("Invalid API key.", status_code=401, code="invalid_api_key")
    origin = connection.headers.get("origin")
    if origin is not None:
        expected_scheme = "https" if connection.url.scheme in {"https", "wss"} else "http"
        expected = f"{expected_scheme}://{connection.url.netloc}"
        if origin.rstrip("/") != expected:
            raise ProtocolError("Cross-origin requests are not allowed.", status_code=403)


def _model(payload: dict[str, Any], default: str) -> str:
    model = payload.get("model", default)
    if not isinstance(model, str) or not model.strip() or len(model) > 128:
        raise ProtocolError(
            "model must be a nonempty string of up to 128 characters.", param="model"
        )
    try:
        model.encode("utf-8")
    except UnicodeError:
        raise ProtocolError("model must contain valid Unicode text.", param="model") from None
    return model


def _options(payload: dict[str, Any], *, websocket: bool = False) -> None:
    if "n" in payload and (type(payload["n"]) is not int or payload["n"] != 1):
        raise ProtocolError("Only n=1 is supported.", param="n")
    for key in ("tools", "functions"):
        if payload.get(key) not in (None, []):
            raise ProtocolError("Client-supplied tools are not supported.", param=key)
    for key in ("tool_choice", "function_call"):
        if payload.get(key) not in (None, "none", "auto"):
            raise ProtocolError("Client-side tool calling is not supported.", param=key)
    if payload.get("modalities") not in (None, ["text"]):
        raise ProtocolError("Only text output is supported.", param="modalities")
    if payload.get("audio") is not None:
        raise ProtocolError("Audio output is not supported.", param="audio")
    output_format = payload.get("response_format")
    if output_format not in (None, {"type": "text"}):
        raise ProtocolError("Structured output is not supported.", param="response_format")
    text_options = payload.get("text")
    if text_options is not None:
        if not isinstance(text_options, dict):
            raise ProtocolError("text must be an object.", param="text")
        if text_options.get("format") not in (None, {"type": "text"}):
            raise ProtocolError("Structured output is not supported.", param="text.format")
    if websocket and payload.get("background") not in (None, False):
        raise ProtocolError("background responses are not supported.", param="background")
    if websocket and payload.get("generate") not in (None, True):
        raise ProtocolError("Warmup without generation is not supported.", param="generate")


def _idempotency_key(connection: HTTPConnection) -> str | None:
    key = connection.headers.get("idempotency-key")
    if key is None:
        return None
    if not key.strip() or len(key) > 128:
        raise ProtocolError("Idempotency-Key must contain 1 to 128 characters.")
    return "openai:" + hashlib.sha256(key.encode()).hexdigest()


def _error(error: Exception, request_id: str | None = None) -> tuple[int, dict[str, Any]]:
    if isinstance(error, ProtocolError):
        result = error.as_dict()
        status = error.status_code
        if status == 401:
            result["type"] = "authentication_error"
    elif isinstance(error, ReplyTimeoutError):
        status = 504
        request_id = error.request_id
        result = {
            "message": "Timed out waiting for dots. The request remains queued or running; "
            "use request_id to inspect it before sending again.",
            "type": "server_error",
            "param": None,
            "code": "bridge_reply_timeout",
        }
    elif isinstance(error, BridgeError):
        # Authentication here belongs to the upstream bridge, not this API caller.
        status = (
            error.status_code if error.status_code in {400, 404, 409, 410, 413, 422, 429} else 502
        )
        if status == 422:
            status = 400
        result = {
            "message": error.message,
            "type": "invalid_request_error" if status < 500 else "server_error",
            "param": None,
            "code": error.code,
        }
    else:
        status = 502
        result = {
            "message": "The bridge could not be reached. Inspect its status before retrying.",
            "type": "server_error",
            "param": None,
            "code": "bridge_unavailable",
        }
    if request_id is not None:
        result["request_id"] = request_id
    return status, {"error": result}


def _error_response(error: Exception, request_id: str | None = None) -> JSONResponse:
    status, body = _error(error, request_id)
    # The upstream may already have persisted the request. Official SDKs honor
    # this header instead of retrying a timeout and enqueueing a second job.
    headers = {"x-should-retry": "false"}
    if status == 401:
        headers["WWW-Authenticate"] = "Bearer"
    if body["error"].get("request_id"):
        headers["X-Dots-Request-ID"] = body["error"]["request_id"]
    return JSONResponse(body, status_code=status, headers=headers)


async def _cancel(task: asyncio.Task) -> None:
    if not task.done():
        task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def create_openai_connector(
    bridge_url: str = "http://127.0.0.1:8765",
    *,
    api_key: str,
    channel: str = "default",
    subscription_id: str | None = None,
    reply_timeout: float = 120,
    model: str = "dots",
    client: AsyncBridgeClient | None = None,
) -> Starlette:
    """Create a separately served OpenAI-compatible bridge client.

    ``api_key`` authenticates this connector and its upstream bridge requests.
    A supplied client remains owned by its caller. Replies are read by request
    ID. Opt-in WebSocket push uses a separate, explicitly acknowledged consumer.
    """
    if not isinstance(api_key, str) or not api_key.strip():
        raise ValueError("api_key must be a nonempty string")
    parsed_url = urlsplit(bridge_url)
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.hostname:
        raise ValueError("bridge_url must be an HTTP or HTTPS URL")
    if parsed_url.username or parsed_url.password or parsed_url.query or parsed_url.fragment:
        raise ValueError("bridge_url must not contain credentials, a query, or a fragment")
    reply_timeout = _positive(reply_timeout, "reply_timeout")
    MessageInput(text="validate", channel=channel, subscription_id=subscription_id)
    _model({"model": model}, "dots")
    bridge_client = client if client is not None else AsyncBridgeClient(bridge_url, api_key=api_key)
    push_consumers: set[str] = set()

    @asynccontextmanager
    async def lifespan(app):
        try:
            yield
        finally:
            if client is None:
                await bridge_client.aclose()

    async def health(request: Request):
        return JSONResponse({"status": "ok", "service": "dots-bridge-openai"})

    async def models(request: Request):
        _authorize(request, api_key)
        return JSONResponse(
            {
                "object": "list",
                "data": [{"id": model, "object": "model", "created": 0, "owned_by": "dots-bridge"}],
            }
        )

    async def enqueue(text: str, requested_model: str, protocol: str, key: str | None = None):
        result = await bridge_client.send_message(
            text,
            channel=channel,
            subscription_id=subscription_id,
            metadata={"connector": "openai", "protocol": protocol, "model": requested_model},
            idempotency_key=key,
        )
        request_id = result.get("request_id")
        if not isinstance(request_id, str) or not request_id:
            raise BridgeError("invalid_response", "Bridge did not return a request ID.", 502)
        return request_id

    async def reply(request_id: str) -> str:
        result = await bridge_client.wait_for_reply(request_id, timeout=reply_timeout)
        text = result.get("reply")
        if not isinstance(text, str):
            raise BridgeError("invalid_response", "Bridge did not return a text reply.", 502)
        return text

    async def completions(request: Request):
        request_id = None
        try:
            _authorize(request, api_key)
            payload = await _read_json(request)
            _options(payload)
            requested_model = _model(payload, model)
            text = latest_chat_text(payload)
            stream = payload.get("stream", False)
            if not isinstance(stream, bool):
                raise ProtocolError("stream must be a boolean.", param="stream")
            stream_options = payload.get("stream_options")
            if stream_options is not None and not isinstance(stream_options, dict):
                raise ProtocolError("stream_options must be an object.", param="stream_options")
            request_id = await enqueue(
                text, requested_model, "chat.completions", _idempotency_key(request)
            )
            completion_id = "chatcmpl-" + request_id
            created = int(time.time())
            if stream:
                return StreamingResponse(
                    completion_stream(
                        request_id,
                        completion_id,
                        created,
                        requested_model,
                        bool(stream_options and stream_options.get("include_usage")),
                    ),
                    media_type="text/event-stream",
                    headers={
                        "Cache-Control": "no-cache",
                        "X-Accel-Buffering": "no",
                        "X-Dots-Request-ID": request_id,
                    },
                )
            task = asyncio.create_task(reply(request_id))
            try:
                while not task.done():
                    await asyncio.wait({task}, timeout=0.5)
                    if await request.is_disconnected():
                        return JSONResponse({}, status_code=499)
                result = await task
            finally:
                await _cancel(task)
            return JSONResponse(
                chat_completion(
                    result, completion_id=completion_id, created=created, model=requested_model
                ),
                headers={"X-Dots-Request-ID": request_id},
            )
        except (ProtocolError, BridgeError, ReplyTimeoutError, httpx.HTTPError) as error:
            return _error_response(error, request_id)

    async def completion_stream(request_id, completion_id, created, requested_model, include_usage):
        task = asyncio.create_task(reply(request_id))
        try:
            opening = chat_chunks(
                "", completion_id=completion_id, created=created, model=requested_model
            )[0]
            yield "data: " + _json(opening) + "\n\n"
            while not task.done():
                await asyncio.wait({task}, timeout=HEARTBEAT_SECONDS)
                if not task.done():
                    yield ": keep-alive\n\n"
            text = await task
            for chunk in chat_chunks(
                text, completion_id=completion_id, created=created, model=requested_model
            )[1:]:
                yield "data: " + _json(chunk) + "\n\n"
            if include_usage:
                yield "data: " + _json({**opening, "choices": [], "usage": None}) + "\n\n"
        except (BridgeError, ReplyTimeoutError, httpx.HTTPError) as error:
            yield "data: " + _json(_error(error, request_id)[1]) + "\n\n"
        finally:
            await _cancel(task)
        yield "data: [DONE]\n\n"

    async def responses(socket: WebSocket):
        try:
            _authorize(socket, api_key)
        except ProtocolError as error:
            if "websocket.http.response" in socket.scope.get("extensions", {}):
                await socket.send_denial_response(_error_response(error))
            else:
                await socket.close(code=1008)
            return
        await socket.accept()
        queue: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUED_RESPONSES)
        send_lock = asyncio.Lock()

        async def send_event(event):
            async with send_lock:
                await socket.send_text(_json(event))

        push_session = WebSocketPushSession(
            bridge_client,
            channel=channel,
            send_event=send_event,
            consumer_registry=push_consumers,
        )

        async def run_response(payload):
            requested_model = _model(payload, model)
            response_id = "resp_" + uuid.uuid4().hex
            created = int(time.time())
            stream_id = payload.get("stream_id")
            kwargs = {
                "response_id": response_id,
                "created": created,
                "model": requested_model,
                "stream_id": stream_id,
            }
            for event in response_start_events(**kwargs):
                await send_event(event)
            request_id = None
            try:
                request_id = await enqueue(
                    latest_response_text(payload), requested_model, "responses.websocket"
                )
                text = await reply(request_id)
                for event in response_finish_events(
                    text, **kwargs, metadata={"dots_bridge_request_id": request_id}
                ):
                    await send_event(event)
            except (BridgeError, ReplyTimeoutError, httpx.HTTPError) as error:
                _, body = _error(error, request_id)
                failed = response_object(
                    response_id=response_id,
                    created=created,
                    model=requested_model,
                    status="failed",
                    error={"code": "server_error", "message": body["error"]["message"]},
                    metadata={"dots_bridge_request_id": request_id} if request_id else {},
                )
                event = {"type": "response.failed", "sequence_number": 2, "response": failed}
                if stream_id is not None:
                    event["stream_id"] = stream_id
                await send_event(event)

        async def worker():
            while True:
                payload = await queue.get()
                await run_response(payload)

        worker_task = asyncio.create_task(worker(), name="dots-bridge-openai-websocket")
        receive_task = None
        try:
            while True:
                receive_task = asyncio.create_task(socket.receive())
                done, _ = await asyncio.wait(
                    {receive_task, worker_task}, return_when=asyncio.FIRST_COMPLETED
                )
                if worker_task in done:
                    await worker_task
                message = await receive_task
                if message["type"] == "websocket.disconnect":
                    break
                payload = None
                try:
                    if message.get("text") is None:
                        raise ProtocolError("Send JSON as a text WebSocket message.")
                    payload = _parse_json(message["text"])
                    event_type = payload.get("type")
                    if event_type == "dots.subscribe":
                        await push_session.subscribe(payload.get("consumer_id"))
                        continue
                    if event_type == "dots.push.ack":
                        await push_session.acknowledge(payload.get("message_id"))
                        continue
                    if event_type != "response.create":
                        raise ProtocolError(
                            "Supported events are response.create, dots.subscribe, "
                            "and dots.push.ack.",
                            param="type",
                        )
                    _options(payload, websocket=True)
                    _model(payload, model)
                    latest_response_text(payload)
                    stream_id = payload.get("stream_id")
                    if stream_id is not None and (
                        not isinstance(stream_id, str)
                        or re.fullmatch(r"[A-Za-z0-9_.-]{1,256}", stream_id) is None
                    ):
                        raise ProtocolError(
                            "stream_id must contain 1 to 256 letters, digits, underscores, "
                            "hyphens, or periods.",
                            param="stream_id",
                        )
                    try:
                        queue.put_nowait(payload)
                    except asyncio.QueueFull:
                        raise ProtocolError(
                            "Too many queued responses on this connection.",
                            status_code=429,
                            code="rate_limit_exceeded",
                        ) from None
                except (ProtocolError, BridgeError, httpx.HTTPError) as error:
                    status, body = _error(error)
                    event = {"type": "error", "status": status, **body}
                    if isinstance(payload, dict):
                        failed_stream = payload.get("stream_id")
                        if isinstance(failed_stream, str) and re.fullmatch(
                            r"[A-Za-z0-9_.-]{1,256}", failed_stream
                        ):
                            event["stream_id"] = failed_stream
                    await send_event(event)
        except (WebSocketDisconnect, OSError):
            pass
        finally:
            tasks = [worker_task]
            if receive_task is not None:
                tasks.append(receive_task)
            # Cancel every child before awaiting: ASGI disconnect may cancel
            # this scope repeatedly, otherwise one wait can leave work alive.
            for task in tasks:
                task.cancel()
            with CancelScope(shield=True):
                await push_session.close()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def protocol_error(request, error):
        return _error_response(error)

    app = Starlette(
        routes=[
            Route("/health", health),
            Route("/v1/models", models),
            Route("/v1/chat/completions", completions, methods=["POST"]),
            WebSocketRoute("/v1/responses", responses),
        ],
        lifespan=lifespan,
        exception_handlers={ProtocolError: protocol_error},
    )
    app.state.bridge_client = bridge_client
    return app
