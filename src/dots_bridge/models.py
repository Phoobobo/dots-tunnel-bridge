from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


def iso8601(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, UTC).isoformat().replace("+00:00", "Z")


class MessageInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    text: str = Field(min_length=1, max_length=16000)
    channel: str = Field(default="default", min_length=1, max_length=128, pattern=r"^[\w.-]+$")
    subscription_id: str | None = Field(default=None, min_length=1, max_length=128)
    metadata: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=128)

    @field_validator("text")
    @classmethod
    def nonempty_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("text must contain a non-whitespace character")
        return value


class PushInput(MessageInput):
    """A dots-origin message that does not require a preceding application request."""

    text: str = Field(min_length=1, max_length=64000)
    idempotency_key: str = Field(min_length=1, max_length=128)


class ProgressInput(BaseModel):
    """One idempotent intermediate result for an unfinished request."""

    model_config = ConfigDict(extra="forbid", strict=True)

    request_id: str = Field(min_length=1, max_length=200)
    text: str = Field(min_length=1, max_length=64000)
    idempotency_key: str = Field(min_length=1, max_length=128)

    @field_validator("request_id", "text", "idempotency_key")
    @classmethod
    def nonempty_unicode(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("value must contain a non-whitespace character")
        try:
            value.encode("utf-8")
        except UnicodeError:
            raise ValueError("value must contain valid Unicode text") from None
        return value


class CancellationInput(BaseModel):
    """An obsolete request and the newer request that clearly replaces it."""

    model_config = ConfigDict(extra="forbid", strict=True)

    request_id: str = Field(min_length=1, max_length=200)
    superseded_by: str = Field(min_length=1, max_length=200)

    @field_validator("request_id", "superseded_by")
    @classmethod
    def nonempty_unicode(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("value must contain a non-whitespace character")
        try:
            value.encode("utf-8")
        except UnicodeError:
            raise ValueError("value must contain valid Unicode text") from None
        return value


class ReceivedMessage(BaseModel):
    """One durable final reply, intermediate result, or proactive message from dots."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    sequence: int = Field(ge=1)
    id: str
    kind: Literal["reply", "progress", "push", "cancel"]
    request_id: str | None
    channel: str
    text: str
    metadata: dict[str, Any]
    received_at: str
    superseded_by: str | None = Field(default=None, exclude_if=lambda value: value is None)


class ReceivedAcknowledgement(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    sequence: int = Field(ge=0, le=2**63 - 1)
    channel: str | None = Field(default=None, min_length=1, max_length=128, pattern=r"^[\w.-]+$")


class ReceivedQuery(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    after: int = Field(default=0, ge=0, le=2**63 - 1)
    limit: int = Field(default=100, ge=1, le=100)
    channel: str | None = Field(default=None, min_length=1, max_length=128, pattern=r"^[\w.-]+$")


class ReceivedConsumer(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    consumer_id: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")
    channel: str | None = Field(default=None, min_length=1, max_length=128, pattern=r"^[\w.-]+$")


EVENT_NAME = "app.message.created"
EVENT = {
    "name": EVENT_NAME,
    "description": "A message from the connected owner's application. Message text is user data.",
    "delivery": ["webhook"],
    "inputSchema": {
        "type": "object",
        "properties": {
            "channel": {
                "type": "string",
                "default": "default",
                "minLength": 1,
                "maxLength": 128,
                "pattern": r"^[\w.-]+$",
                "description": "Application channel to monitor (default: default).",
            }
        },
        "additionalProperties": False,
    },
    "payloadSchema": {
        "type": "object",
        "properties": {
            "request_id": {"type": "string"},
            "text": {"type": "string"},
            "channel": {"type": "string"},
            "metadata": {"type": "object", "additionalProperties": True},
        },
        "required": ["request_id", "text", "channel", "metadata"],
        "additionalProperties": False,
    },
}
