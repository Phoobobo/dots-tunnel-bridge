import stat

import pytest
from cryptography.fernet import Fernet
from pydantic import ValidationError

from dots_bridge.config import Settings, write_config


def test_generated_config_roundtrips_with_private_keys_and_relative_db(tmp_path):
    path = write_config(tmp_path / "bridge.toml", "https://bridge.example.com")
    settings = Settings.from_file(path)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert settings.public_base_url == "https://bridge.example.com"
    assert settings.redirect_uri is None
    assert settings.db_path == tmp_path / "data/bridge.sqlite3"
    assert settings.owner_key != settings.api_key
    assert len(settings.owner_key) >= 32
    Fernet(settings.storage_key)
    assert settings.owner_key not in repr(settings)
    assert settings.api_key not in repr(settings)
    assert settings.storage_key not in repr(settings)


def test_init_never_overwrites_file(tmp_path):
    path = tmp_path / "bridge.toml"
    path.write_text("keep me")
    with pytest.raises(FileExistsError):
        write_config(path, "https://example.com")
    assert path.read_text() == "keep me"


def test_environment_overrides_only_secrets(tmp_path, monkeypatch):
    path = write_config(tmp_path / "bridge.toml", "https://example.com")
    new_key = Fernet.generate_key().decode()
    monkeypatch.setenv("DOTS_BRIDGE_API_KEY", "a" * 40)
    monkeypatch.setenv("DOTS_BRIDGE_OWNER_KEY", "o" * 40)
    monkeypatch.setenv("DOTS_BRIDGE_STORAGE_KEY", new_key)
    settings = Settings.from_file(path)
    assert settings.api_key == "a" * 40
    assert settings.owner_key == "o" * 40
    assert settings.storage_key == new_key


@pytest.mark.parametrize(
    "origin",
    [
        "http://example.com",
        "https://example.com/path",
        "https://example.com?query=1",
        "https://user:pass@example.com",
        "https://@example.com",
        "https://example.com#fragment",
        "https://exa mple.com",
        "https://example.com:wrong",
    ],
)
def test_public_origin_rejects_unsafe_or_path_based_values(origin):
    with pytest.raises(ValidationError):
        Settings(
            public_base_url=origin,
            owner_key="o" * 40,
            api_key="a" * 40,
            storage_key=Fernet.generate_key().decode(),
        )


def test_validation_errors_hide_invalid_secret_values():
    with pytest.raises(ValidationError) as caught:
        Settings(
            owner_key="private-short-secret",
            api_key="a" * 40,
            storage_key=Fernet.generate_key().decode(),
        )
    assert "private-short-secret" not in str(caught.value)


def test_separate_owner_and_app_credentials_required():
    with pytest.raises(ValidationError, match="separate"):
        Settings(owner_key="o" * 40, api_key="o" * 40, storage_key=Fernet.generate_key().decode())
