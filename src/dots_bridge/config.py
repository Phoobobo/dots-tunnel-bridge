import json
import os
import secrets
import tomllib
from pathlib import Path
from urllib.parse import urlsplit

from cryptography.fernet import Fernet
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class Settings(BaseModel):
    """File-based single-owner configuration; secrets may be overridden by environment."""

    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True)

    public_base_url: str = "http://127.0.0.1:8765"
    redirect_uri: str | None = None
    client_id: str = "dots-bridge"
    owner_user_id: str = "owner"
    owner_key: str = Field(min_length=32, repr=False)
    api_key: str = Field(min_length=32, repr=False)
    storage_key: str = Field(repr=False)
    db_path: Path = Path("data/bridge.sqlite3")
    host: str = "127.0.0.1"
    port: int = Field(default=8765, ge=1, le=65535, strict=True)
    subscription_ttl_seconds: int = Field(default=86400, ge=1, le=604800, strict=True)
    reply_timeout_seconds: int = Field(default=3600, ge=1, le=604800, strict=True)
    max_delivery_attempts: int = Field(default=5, ge=1, le=20, strict=True)
    retry_base_seconds: float = Field(default=1.0, gt=0, le=3600)
    worker_interval_seconds: float = Field(default=0.5, gt=0, le=60)

    @field_validator("public_base_url")
    @classmethod
    def public_origin(cls, value: str) -> str:
        value = value.rstrip("/")
        parsed = urlsplit(value)
        local = parsed.scheme == "http" and parsed.hostname in {"localhost", "127.0.0.1", "::1"}
        if (
            not parsed.hostname
            or (parsed.scheme != "https" and not local)
            or parsed.path
            or parsed.query
            or parsed.fragment
            or parsed.username is not None
            or parsed.password is not None
            or any(c.isspace() for c in value)
        ):
            raise ValueError("public_base_url must be an HTTPS origin (HTTP only on loopback)")
        _ = parsed.port
        return value

    @field_validator("redirect_uri", mode="before")
    @classmethod
    def empty_redirect(cls, value):
        return value or None

    @field_validator("storage_key")
    @classmethod
    def valid_storage_key(cls, value: str) -> str:
        Fernet(value.encode())
        return value

    @field_validator("owner_user_id", "client_id")
    @classmethod
    def nonempty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("value must not be empty")
        return value

    @model_validator(mode="after")
    def distinct_keys(self):
        if self.owner_key == self.api_key:
            raise ValueError("Use separate owner_key and api_key values")
        return self

    @classmethod
    def from_file(cls, path: str | Path = "bridge.toml") -> "Settings":
        path = Path(path).expanduser().resolve()
        with path.open("rb") as source:
            values = tomllib.load(source)
        for name in ("owner_key", "api_key", "storage_key"):
            if override := os.environ.get("DOTS_BRIDGE_" + name.upper()):
                values[name] = override
        settings = cls.model_validate(values)
        if not settings.db_path.is_absolute():
            settings.db_path = path.parent / settings.db_path
        return settings


def write_config(path: str | Path, public_base_url: str) -> Path:
    """Create a new config with independent keys, refusing to overwrite any file."""
    path = Path(path).expanduser().resolve()
    settings = Settings(
        public_base_url=public_base_url,
        owner_key=secrets.token_urlsafe(32),
        api_key=secrets.token_urlsafe(32),
        storage_key=Fernet.generate_key().decode(),
    )
    values = settings.model_dump(mode="json")
    values["redirect_uri"] = ""
    content = (
        "# Keep this file private. Restart the server after changing settings.\n"
        "# Set redirect_uri to the exact callback shown by your plugin registration.\n\n"
        + "\n".join(
            f"{key} = {json.dumps(value, ensure_ascii=False)}" for key, value in values.items()
        )
        + "\n"
    )
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, "w") as output:
        output.write(content)
    return path
