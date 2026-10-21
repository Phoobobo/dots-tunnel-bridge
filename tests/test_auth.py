"""Exercise the real SDK OAuth routes, with persistent provider state."""

import asyncio
import base64
import hashlib
import sqlite3
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from dots_bridge.auth import PersonalOAuth

BASE = "https://bridge.example"
CALLBACK = "https://chatgpt.example/oauth/callback?plugin=dots"
OWNER_KEY = "test-owner-key-" + "a" * 40
VERIFIER = "test-verifier-" + "b" * 48
CHALLENGE = (
    base64.urlsafe_b64encode(hashlib.sha256(VERIFIER.encode()).digest()).decode().rstrip("=")
)


def provider(db_path: Path, **overrides) -> PersonalOAuth:
    settings = dict(
        db_path=db_path,
        public_base_url=BASE,
        owner_key=OWNER_KEY,
        redirect_uri=CALLBACK,
    )
    settings.update(overrides)
    return PersonalOAuth(**settings)


def http(auth: PersonalOAuth) -> TestClient:
    return TestClient(
        Starlette(routes=auth.get_routes("/mcp")), base_url=BASE, follow_redirects=False
    )


def authorization(client: TestClient, **overrides):
    params = dict(
        client_id="dots-bridge",
        redirect_uri=CALLBACK,
        response_type="code",
        resource=f"{BASE}/mcp",
        scope="events:read messages:reply",
        code_challenge=CHALLENGE,
        code_challenge_method="S256",
        state="test-state",
    )
    params.update(overrides)
    return client.get("/authorize", params=params)


def consent(client: TestClient, **overrides) -> str:
    response = authorization(client, **overrides)
    assert response.status_code == 302
    location = response.headers["location"]
    assert urlsplit(location).path == "/consent"
    request_id = parse_qs(urlsplit(location).query)["request_id"][0]
    response = client.get(location)
    assert response.status_code == 200
    assert (
        "form-action 'self' https://chatgpt.example" in response.headers["content-security-policy"]
    )
    assert response.headers["referrer-policy"] == "same-origin"
    response = client.post(
        "/consent",
        data={"request_id": request_id, "owner_key": OWNER_KEY},
        headers={"Origin": BASE},
    )
    assert response.status_code == 302
    callback = urlsplit(response.headers["location"])
    assert callback.scheme == "https" and callback.netloc == "chatgpt.example"
    params = parse_qs(callback.query)
    assert params["state"] == ["test-state"]
    return params["code"][0]


def exchange(client: TestClient, code: str, **overrides):
    data = dict(
        grant_type="authorization_code",
        client_id="dots-bridge",
        code=code,
        code_verifier=VERIFIER,
        redirect_uri=CALLBACK,
        resource=f"{BASE}/mcp",
    )
    data.update(overrides)
    return client.post("/token", data=data)


def refresh(client: TestClient, token: str, **overrides):
    data = dict(grant_type="refresh_token", client_id="dots-bridge", refresh_token=token)
    data.update(overrides)
    return client.post("/token", data=data)


def test_complete_flow_restart_refresh_and_revoke(tmp_path):
    path = tmp_path / "bridge.sqlite3"
    original = provider(path)
    with http(original) as client:
        code = consent(client)

    # Pending code state, access tokens and refresh tokens survive restarts.
    restarted = provider(path)
    with http(restarted) as client:
        response = exchange(client, code)
        assert response.status_code == 200, response.text
        tokens = response.json()
        assert tokens["token_type"] == "Bearer" and tokens["expires_in"] == 3600
        assert set(tokens["scope"].split()) == set(PersonalOAuth.SCOPES)
        assert exchange(client, code).json()["error"] == "invalid_grant"
        access = asyncio.run(restarted.verify_token(tokens["access_token"]))
        assert access.subject == "owner" and access.resource == f"{BASE}/mcp"

    final = provider(path)
    with http(final) as client:
        assert asyncio.run(final.verify_token(tokens["access_token"])) is not None
        response = refresh(client, tokens["refresh_token"])
        assert response.status_code == 200, response.text
        rotated = response.json()
        assert rotated["access_token"] != tokens["access_token"]
        assert rotated["refresh_token"] != tokens["refresh_token"]
        assert asyncio.run(final.verify_token(tokens["access_token"])) is None
        assert asyncio.run(final.verify_token(rotated["access_token"])) is not None
        called = []

        async def on_revoke():
            called.append(True)

        final.on_revoke = on_revoke
        response = client.post(
            "/revoke",
            data={"client_id": "dots-bridge", "token": rotated["access_token"]},
        )
        assert response.status_code == 200
        assert called == [True]
        assert asyncio.run(final.verify_token(rotated["access_token"])) is None
        assert refresh(client, rotated["refresh_token"]).json()["error"] == "invalid_grant"

    stored = path.read_bytes()
    for secret in (
        OWNER_KEY,
        code,
        tokens["access_token"],
        tokens["refresh_token"],
        rotated["access_token"],
        rotated["refresh_token"],
    ):
        assert secret.encode() not in stored


def test_refresh_reuse_revokes_successor(tmp_path):
    auth = provider(tmp_path / "auth.db")
    with http(auth) as client:
        tokens = exchange(client, consent(client)).json()
        rotated = refresh(client, tokens["refresh_token"]).json()
        assert refresh(client, tokens["refresh_token"]).json()["error"] == "invalid_grant"
        assert asyncio.run(auth.verify_token(rotated["access_token"])) is None
        assert refresh(client, rotated["refresh_token"]).json()["error"] == "invalid_grant"


def test_sdk_pkce_redirect_and_scope_validation(tmp_path):
    auth = provider(tmp_path / "auth.db")
    with http(auth) as client:
        assert authorization(client, redirect_uri="https://evil.example/").status_code == 400
        assert authorization(client, client_id="someone-else").status_code == 400
        response = authorization(client, scope="unknown")
        assert parse_qs(urlsplit(response.headers["location"]).query)["error"] == ["invalid_scope"]
        response = authorization(client, resource="https://evil.example/mcp")
        assert parse_qs(urlsplit(response.headers["location"]).query)["error"] == ["invalid_target"]
        response = authorization(client, code_challenge_method="plain")
        assert response.status_code == 302
        assert "error=" in response.headers["location"]
        code = consent(client)
        assert exchange(client, code, code_verifier="wrong").json()["error"] == "invalid_grant"
        assert (
            exchange(client, code, redirect_uri="https://evil.example/").json()["error"]
            == "invalid_request"
        )
        tokens = exchange(client, code).json()
        assert (
            refresh(client, tokens["refresh_token"], scope="unknown").json()["error"]
            == "invalid_scope"
        )
        narrowed = refresh(client, tokens["refresh_token"], scope="events:read").json()
        assert narrowed["scope"] == "events:read"


def test_consent_origin_cookie_and_expiration(tmp_path):
    auth = provider(tmp_path / "auth.db")
    with http(auth) as client:
        response = authorization(client)
        location = response.headers["location"]
        request_id = parse_qs(urlsplit(location).query)["request_id"][0]
        assert client.get(location).status_code == 200
        form = {"request_id": request_id, "owner_key": OWNER_KEY}
        assert client.post("/consent", data=form).status_code == 403
        assert client.post("/consent", data=form, headers={"Origin": "null"}).status_code == 403
        assert (
            client.post(
                "/consent", data=form, headers={"Origin": "https://evil.example"}
            ).status_code
            == 403
        )
        assert (
            client.post(
                "/consent", data={**form, "owner_key": "wrong"}, headers={"Origin": BASE}
            ).status_code
            == 403
        )
        client.cookies.clear()
        assert client.post("/consent", data=form, headers={"Origin": BASE}).status_code == 403
        assert client.get(location).status_code == 200
        with sqlite3.connect(auth.db_path) as db:
            db.execute("UPDATE oauth_pending SET expires_at = 0")
        assert client.post("/consent", data=form, headers={"Origin": BASE}).status_code == 403


def test_metadata_static_client_and_audience(tmp_path):
    path = tmp_path / "auth.db"
    auth = provider(path)
    with http(auth) as client:
        metadata = client.get("/.well-known/oauth-authorization-server").json()
        assert metadata["token_endpoint_auth_methods_supported"] == ["none"]
        assert metadata["scopes_supported"] == PersonalOAuth.SCOPES
        assert "registration_endpoint" not in metadata
        assert client.post("/register", json={}).status_code == 404
        resource = client.get("/.well-known/oauth-protected-resource/mcp").json()
        assert resource["resource"] == f"{BASE}/mcp"
        tokens = exchange(client, consent(client)).json()
    different_resource = provider(path, public_base_url="https://another.example")
    assert asyncio.run(different_resource.verify_token(tokens["access_token"])) is None
    different_owner = provider(path, owner_user_id="another-owner")
    assert asyncio.run(different_owner.verify_token(tokens["access_token"])) is None


def test_revoke_all_clears_pending_codes_and_tokens(tmp_path):
    auth = provider(tmp_path / "auth.db")
    with http(auth) as client:
        tokens = exchange(client, consent(client)).json()
        code = consent(client)
        authorization(client)
        calls = []

        async def on_revoke():
            calls.append(True)

        auth.on_revoke = on_revoke
        asyncio.run(auth.revoke_all())
        assert calls == [True]
        assert asyncio.run(auth.verify_token(tokens["access_token"])) is None
        assert exchange(client, code).json()["error"] == "invalid_grant"
        assert refresh(client, tokens["refresh_token"]).json()["error"] == "invalid_grant"
        with sqlite3.connect(auth.db_path) as db:
            assert db.execute("SELECT count(*) FROM oauth_pending").fetchone()[0] == 0


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com",
        "https://user:password@example.com",
        "https://example.com/path",
        "https://example.com?x=1",
    ],
)
def test_rejects_unsafe_base_urls(tmp_path, url):
    with pytest.raises(ValueError):
        provider(tmp_path / "auth.db", public_base_url=url)


def test_owner_key_and_loopback_configuration(tmp_path):
    auth = provider(tmp_path / "auth.db", public_base_url="http://127.0.0.1:8000")
    assert auth.check_owner_key(OWNER_KEY)
    assert not auth.check_owner_key("wrong")
    with pytest.raises(ValueError):
        provider(tmp_path / "auth.db", owner_key="short")


@pytest.mark.parametrize(
    "changed",
    [
        {"client_id": "changed-client"},
        {"owner_user_id": "changed-owner"},
        {"public_base_url": "https://changed.example"},
        {"redirect_uri": "https://changed.example/callback"},
    ],
)
def test_codes_and_pending_consent_bound_to_original_configuration(tmp_path, changed):
    path = tmp_path / "auth.db"
    original = provider(path)
    with http(original) as client:
        code = consent(client)
        pending = authorization(client).headers["location"]
    changed_auth = provider(path, **changed)
    with http(changed_auth) as client:
        response = exchange(client, code, client_id=changed_auth.client_id)
        assert response.status_code in (400, 401)
        assert response.json()["error"] == "invalid_grant"
        assert client.get(pending).status_code == 403


def test_private_database_creation_and_symlink_rejection(tmp_path):
    path = tmp_path / "private" / "auth.db"
    provider(path)
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    target = tmp_path / "target"
    target.write_text("must not modify")
    link = tmp_path / "linked.db"
    link.symlink_to(target)
    with pytest.raises(ValueError, match="symbolic link"):
        provider(link)
    assert target.read_text() == "must not modify"
