from datetime import UTC, datetime
from typing import Any

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
