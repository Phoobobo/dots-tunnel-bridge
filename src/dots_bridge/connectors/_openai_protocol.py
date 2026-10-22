"""Pure text handling and wire objects for the OpenAI-compatible connector.

The bridge receives only the latest user message. It does not replay the
caller's conversation history or forward the caller's system instructions.
Streaming objects describe one completed bridge reply, not token generation.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any


class ProtocolError(ValueError):
    """A safe request error that can be serialized on either HTTP or WebSocket."""

    def __init__(
        self,
        message: str,
        *,
        param: str | None = None,
        status_code: int = 400,
        code: str = "invalid_request_error",
        error_type: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.param = param
        self.status_code = status_code
        self.code = code
        self.error_type = error_type or {
            401: "authentication_error",
            403: "permission_error",
            429: "rate_limit_error",
        }.get(status_code, "server_error" if status_code >= 500 else "invalid_request_error")

    def as_dict(self) -> dict[str, Any]:
        """Return the inner OpenAI error object, without its transport envelope."""
        return {
            "message": self.message,
            "type": self.error_type,
            "param": self.param,
            "code": self.code,
        }


def _request(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ProtocolError("Request body must be a JSON object.")


def _text(content: Any, param: str) -> str:
    if isinstance(content, str):
        text = content
    elif isinstance(content, list):
        pieces = []
        for index, part in enumerate(content):
            part_param = f"{param}[{index}]"
            if not isinstance(part, dict):
                raise ProtocolError("Each content part must be a text object.", param=part_param)
            if part.get("type") not in ("text", "input_text"):
                raise ProtocolError(
                    "Only text content is supported; image, audio and file inputs are unsupported.",
                    param=f"{part_param}.type",
                )
            value = part.get("text")
            if not isinstance(value, str):
                raise ProtocolError("Text content must be a string.", param=f"{part_param}.text")
            pieces.append(value)
        text = "\n".join(pieces)
    else:
        raise ProtocolError("Content must be a string or a list of text objects.", param=param)
    if not text.strip():
        raise ProtocolError("The latest user message must contain non-empty text.", param=param)
    if len(text) > 16000:
        raise ProtocolError(
            "The latest user message must not exceed 16000 characters.", param=param
        )
    try:
        text.encode("utf-8")
    except UnicodeError:
        raise ProtocolError(
            "The latest user message must contain valid Unicode text.", param=param
        ) from None
    return text


def latest_chat_text(payload: dict[str, Any]) -> str:
    """Extract exactly the latest user message from a Chat Completions request."""
    _request(payload)
    messages = payload.get("messages")
    if not isinstance(messages, list) or not messages:
        raise ProtocolError("messages must be a non-empty array.", param="messages")
    for index in range(len(messages) - 1, -1, -1):
        message = messages[index]
        if isinstance(message, dict) and message.get("role") == "user":
            return _text(message.get("content"), f"messages[{index}].content")
    raise ProtocolError("messages must contain a user message.", param="messages")


def latest_response_text(payload: dict[str, Any]) -> str:
    """Extract a string input or exactly the latest Responses user message."""
    _request(payload)
    inputs = payload.get("input")
    if isinstance(inputs, str):
        return _text(inputs, "input")
    if not isinstance(inputs, list) or not inputs:
        raise ProtocolError("input must be text or a non-empty message array.", param="input")
    for index in range(len(inputs) - 1, -1, -1):
        message = inputs[index]
        if isinstance(message, dict) and message.get("role") == "user":
            if message.get("type", "message") != "message":
                raise ProtocolError(
                    "The latest user input must be a message.", param=f"input[{index}].type"
                )
            return _text(message.get("content"), f"input[{index}].content")
    raise ProtocolError("input must contain a user message.", param="input")


def chat_completion(text: str, *, completion_id: str, created: int, model: str) -> dict[str, Any]:
    """Serialize a final text reply without inventing unavailable token usage."""
    return {
        "id": completion_id,
        "object": "chat.completion",
        "created": created,
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": text},
                "finish_reason": "stop",
                "logprobs": None,
            }
        ],
    }


def chat_chunks(text: str, *, completion_id: str, created: int, model: str) -> list[dict[str, Any]]:
    """Serialize assistant-role, whole-reply and stop chunks for an SSE stream."""
    base = {
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
    }
    return [
        {
            **base,
            "choices": [
                {"index": 0, "delta": delta, "finish_reason": finish_reason, "logprobs": None}
            ],
        }
        for delta, finish_reason in (
            ({"role": "assistant", "content": ""}, None),
            ({"content": text}, None),
            ({}, "stop"),
        )
    ]


def _part(text: str) -> dict[str, Any]:
    return {"type": "output_text", "text": text, "annotations": [], "logprobs": []}


def _item(response_id: str, *, text: str | None, status: str) -> dict[str, Any]:
    return {
        "id": f"msg_{response_id}",
        "type": "message",
        "role": "assistant",
        "status": status,
        "content": [] if text is None else [_part(text)],
    }


def response_object(
    *,
    response_id: str,
    created: int,
    model: str,
    status: str = "in_progress",
    text: str | None = None,
    error: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Serialize a Responses object with optional final text or a failure error."""
    item_status = status if status in {"in_progress", "completed"} else "incomplete"
    return {
        "id": response_id,
        "object": "response",
        "created_at": created,
        "status": status,
        "access_programs": None,
        "error": deepcopy(error),
        "incomplete_details": None,
        "instructions": None,
        "model": model,
        "output": [] if text is None else [_item(response_id, text=text, status=item_status)],
        "parallel_tool_calls": False,
        "tool_choice": "none",
        "tools": [],
        "temperature": None,
        "top_p": None,
        "text": {"format": {"type": "text"}},
        "store": False,
        "usage": None,
        "metadata": deepcopy(metadata) if metadata is not None else {},
    }


def _events(
    events: list[dict[str, Any]], *, first_sequence: int, stream_id: str | None
) -> list[dict[str, Any]]:
    return [
        {
            **event,
            "sequence_number": number,
            **({"stream_id": stream_id} if stream_id is not None else {}),
        }
        for number, event in enumerate(events, start=first_sequence)
    ]


def response_start_events(
    *,
    response_id: str,
    created: int,
    model: str,
    stream_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Return sequence 0–1 immediately, before waiting for the bridge reply."""
    return _events(
        [
            {
                "type": event_type,
                "response": response_object(
                    response_id=response_id, created=created, model=model, metadata=metadata
                ),
            }
            for event_type in ("response.created", "response.in_progress")
        ],
        first_sequence=0,
        stream_id=stream_id,
    )


def response_finish_events(
    text: str,
    *,
    response_id: str,
    created: int,
    model: str,
    stream_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Return sequence 2–8 for a complete text reply, following start events."""
    item_index = {"output_index": 0}
    content_index = {**item_index, "item_id": f"msg_{response_id}", "content_index": 0}
    return _events(
        [
            {
                "type": "response.output_item.added",
                **item_index,
                "item": _item(response_id, text=None, status="in_progress"),
            },
            {"type": "response.content_part.added", **content_index, "part": _part("")},
            {
                "type": "response.output_text.delta",
                **content_index,
                "delta": text,
                "logprobs": [],
            },
            {
                "type": "response.output_text.done",
                **content_index,
                "text": text,
                "logprobs": [],
            },
            {"type": "response.content_part.done", **content_index, "part": _part(text)},
            {
                "type": "response.output_item.done",
                **item_index,
                "item": _item(response_id, text=text, status="completed"),
            },
            {
                "type": "response.completed",
                "response": response_object(
                    response_id=response_id,
                    created=created,
                    model=model,
                    status="completed",
                    text=text,
                    metadata=metadata,
                ),
            },
        ],
        first_sequence=2,
        stream_id=stream_id,
    )


def response_events(
    text: str,
    *,
    response_id: str,
    created: int,
    model: str,
    stream_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Return the complete, ordered Responses event lifecycle for one reply."""
    arguments = {
        "response_id": response_id,
        "created": created,
        "model": model,
        "stream_id": stream_id,
        "metadata": metadata,
    }
    return response_start_events(**arguments) + response_finish_events(text, **arguments)
