"""Single-owner OAuth using FastMCP's OAuth protocol endpoints.

Only opaque token digests are persisted.  This provider is deliberately not a
general identity service: every consent belongs to the configured local owner.
"""

from __future__ import annotations

import asyncio
import hashlib
import html
import json
import os
import re
import secrets
import sqlite3
import time
from collections.abc import Awaitable, Callable
from contextlib import closing
from pathlib import Path
from typing import TypeVar
from urllib.parse import parse_qs, urlencode, urlsplit

from fastmcp.server.auth import OAuthProvider
from mcp.server.auth.provider import (
    AccessToken,
    AuthorizationCode,
    AuthorizationParams,
    AuthorizeError,
    RefreshToken,
    TokenError,
)
from mcp.server.auth.routes import build_metadata
from mcp.server.auth.settings import ClientRegistrationOptions, RevocationOptions
from mcp.shared.auth import OAuthClientInformationFull, OAuthToken
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from starlette.routing import Route
from starlette.types import ASGIApp, Message, Receive, Scope, Send

_T = TypeVar("_T")


class PersonalOAuth(OAuthProvider):
    """Persistent, static-client OAuth with explicit local-owner consent.

    The owner key authenticates the consent form only. MCP access tokens are
    scoped to ``public_base_url + '/mcp'`` and always identify ``owner_user_id``.
    Dynamic client registration is disabled; configure the exact callback URL
    and client ID shown by the dots plugin before connecting it.
    """

    SCOPES = ["events:read", "messages:reply"]
    COOKIE = "dots_oauth_csrf"
    ACCESS_TTL = 3600
    REFRESH_TTL = 30 * 24 * 3600
    CONSENT_TTL = 300
    MAX_PENDING = 256
    HEADERS = {
        "Cache-Control": "no-store",
        # Keep Origin on browser POSTs while hiding cross-origin referrers.
        "Referrer-Policy": "same-origin",
        "X-Content-Type-Options": "nosniff",
    }

    def __init__(
        self,
        *,
        db_path: Path,
        public_base_url: str,
        owner_key: str,
        redirect_uri: str | None = None,
        client_id: str = "dots-bridge",
        owner_user_id: str = "owner",
    ) -> None:
        if len(owner_key) < 32:
            raise ValueError("owner_key must contain at least 32 characters")
        if not client_id or not owner_user_id:
            raise ValueError("client_id and owner_user_id must not be empty")
        base = self._safe_url(public_base_url)
        if base.path not in ("", "/"):
            raise ValueError("public_base_url must be an origin without a path")
        self.base = public_base_url.rstrip("/")
        self.origin = f"{base.scheme}://{base.netloc}"
        self.resource = f"{self.base}/mcp"
        self.secure_cookie = base.scheme == "https"
        self.redirect_uri = redirect_uri
        self.client_id = client_id
        self.owner_user_id = owner_user_id
        self.on_revoke: Callable[[], Awaitable[None]] | None = None
        self._owner_digest = self._digest(owner_key)
        self._redirect_origin = ""
        if redirect_uri is not None:
            redirect = self._safe_url(redirect_uri, allow_query=True)
            host = redirect.hostname.encode("idna").decode("ascii")  # type: ignore[union-attr]
            if not re.fullmatch(r"[A-Za-z0-9.:-]+", host):
                raise ValueError("redirect_uri must have a concrete callback hostname")
            if ":" in host:
                host = f"[{host}]"
            port = f":{redirect.port}" if redirect.port is not None else ""
            self._redirect_origin = f"{redirect.scheme}://{host}{port}"
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._initialize()
        super().__init__(
            base_url=self.base,
            # Authorization is checked per action: event reads and final replies
            # have distinct scopes. Requiring both here would reject narrowed
            # tokens before FastMCP's tool/extension checks can run.
            required_scopes=[],
            client_registration_options=ClientRegistrationOptions(
                enabled=False,
                valid_scopes=list(self.SCOPES),
                default_scopes=list(self.SCOPES),
            ),
            revocation_options=RevocationOptions(enabled=True),
        )

    @staticmethod
    def _safe_url(value: str, *, allow_query: bool = False):
        parsed = urlsplit(value)
        local_http = parsed.scheme == "http" and parsed.hostname in (
            "localhost",
            "127.0.0.1",
            "::1",
        )
        if (
            not parsed.hostname
            or (parsed.scheme != "https" and not local_http)
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or (parsed.query and not allow_query)
            or any(character.isspace() for character in value)
        ):
            raise ValueError("URLs must use HTTPS (HTTP is allowed only on loopback)")
        # Validate malformed ports at configuration time, before rendering CSP.
        _ = parsed.port
        return parsed

    @staticmethod
    def _digest(value: str) -> str:
        return hashlib.sha256(value.encode()).hexdigest()

    def check_owner_key(self, key: str) -> bool:
        """Constant-time local owner-key check without storing its plaintext."""
        return secrets.compare_digest(self._digest(key), self._owner_digest)

    def _connect(self) -> sqlite3.Connection:
        if self.db_path.is_symlink():
            raise ValueError("OAuth database must not be a symbolic link")
        connection = sqlite3.connect(self.db_path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _initialize(self) -> None:
        if self.db_path.is_symlink():
            raise ValueError("OAuth database must not be a symbolic link")
        fd = os.open(self.db_path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
        try:
            if hasattr(os, "fchmod"):
                os.fchmod(fd, 0o600)
        finally:
            os.close(fd)
        with closing(self._connect()) as db, db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS oauth_pending (
                    request_hash TEXT PRIMARY KEY, params TEXT NOT NULL,
                    csrf_hash TEXT, expires_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS oauth_codes (
                    code_hash TEXT PRIMARY KEY, params TEXT NOT NULL,
                    expires_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS oauth_grants (
                    grant_id TEXT PRIMARY KEY, client_id TEXT NOT NULL,
                    scopes TEXT NOT NULL, resource TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    access_hash TEXT NOT NULL UNIQUE,
                    refresh_hash TEXT NOT NULL UNIQUE,
                    access_expires_at INTEGER NOT NULL,
                    refresh_expires_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS oauth_retired_refresh (
                    token_hash TEXT PRIMARY KEY,
                    grant_id TEXT NOT NULL REFERENCES oauth_grants(grant_id)
                        ON DELETE CASCADE
                );
                """
            )

    async def _db(self, operation: Callable[[sqlite3.Connection], _T]) -> _T:
        def run() -> _T:
            with closing(self._connect()) as db, db:
                # Serialize check-and-consume operations across worker processes.
                db.execute("BEGIN IMMEDIATE")
                now = time.time()
                db.execute("DELETE FROM oauth_pending WHERE expires_at <= ?", (now,))
                db.execute("DELETE FROM oauth_codes WHERE expires_at <= ?", (now,))
                db.execute("DELETE FROM oauth_grants WHERE refresh_expires_at <= ?", (now,))
                return operation(db)

        return await asyncio.to_thread(run)

    def _serialize_params(self, params: AuthorizationParams) -> str:
        return json.dumps(
            {
                "params": params.model_dump(mode="json"),
                "client_id": self.client_id,
                "subject": self.owner_user_id,
                "resource": self.resource,
            }
        )

    def _deserialize_params(self, serialized: str) -> AuthorizationParams | None:
        data = json.loads(serialized)
        if (
            not isinstance(data, dict)
            or data.get("client_id") != self.client_id
            or data.get("subject") != self.owner_user_id
            or data.get("resource") != self.resource
        ):
            return None
        params = AuthorizationParams.model_validate(data["params"])
        if str(params.redirect_uri) != self.redirect_uri:
            return None
        return params

    async def get_client(self, client_id: str) -> OAuthClientInformationFull | None:
        if client_id != self.client_id or self.redirect_uri is None:
            return None
        return OAuthClientInformationFull(
            client_id=self.client_id,
            client_name="dots-bridge personal connection",
            redirect_uris=[self.redirect_uri],
            token_endpoint_auth_method="none",
            grant_types=["authorization_code", "refresh_token"],
            response_types=["code"],
            scope=" ".join(self.SCOPES),
        )

    async def register_client(self, client_info: OAuthClientInformationFull) -> None:
        raise NotImplementedError("Dynamic client registration is disabled")

    async def authorize(
        self, client: OAuthClientInformationFull, params: AuthorizationParams
    ) -> str:
        if self.redirect_uri is None:
            raise AuthorizeError("temporarily_unavailable", "Configure the callback URI first")
        if client.client_id != self.client_id or str(params.redirect_uri) != self.redirect_uri:
            raise AuthorizeError("invalid_request", "Unregistered client or callback URI")
        if params.resource != self.resource:
            raise AuthorizeError("invalid_target", "The resource must identify this MCP endpoint")
        if not re.fullmatch(r"[A-Za-z0-9_-]{43}", params.code_challenge):
            raise AuthorizeError("invalid_request", "A valid S256 PKCE challenge is required")
        if params.state is not None and len(params.state) > 2048:
            raise AuthorizeError("invalid_request", "The state parameter is too large")
        scopes = params.scopes if params.scopes is not None else list(self.SCOPES)
        if not scopes or not set(scopes).issubset(self.SCOPES):
            raise AuthorizeError("invalid_scope", "Unsupported scope")
        params = params.model_copy(update={"scopes": scopes})
        request_id = secrets.token_urlsafe(32)

        def save(db: sqlite3.Connection) -> None:
            if db.execute("SELECT count(*) FROM oauth_pending").fetchone()[0] >= self.MAX_PENDING:
                raise AuthorizeError("temporarily_unavailable", "Too many pending logins")
            db.execute(
                "INSERT INTO oauth_pending VALUES (?, ?, NULL, ?)",
                (
                    self._digest(request_id),
                    self._serialize_params(params),
                    time.time() + self.CONSENT_TTL,
                ),
            )

        await self._db(save)
        return f"{self.base}/consent?{urlencode({'request_id': request_id})}"

    async def load_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: str
    ) -> AuthorizationCode | None:
        def load(db: sqlite3.Connection) -> AuthorizationCode | None:
            row = db.execute(
                "SELECT * FROM oauth_codes WHERE code_hash = ?", (self._digest(authorization_code),)
            ).fetchone()
            if row is None or client.client_id != self.client_id:
                return None
            params = self._deserialize_params(row["params"])
            if params is None:
                return None
            return AuthorizationCode(
                code=authorization_code,
                scopes=params.scopes or [],
                expires_at=row["expires_at"],
                client_id=self.client_id,
                code_challenge=params.code_challenge,
                redirect_uri=params.redirect_uri,
                redirect_uri_provided_explicitly=params.redirect_uri_provided_explicitly,
                resource=params.resource,
                subject=self.owner_user_id,
            )

        return await self._db(load)

    def _issue(
        self,
        db: sqlite3.Connection,
        scopes: list[str],
        *,
        grant_id: str | None = None,
    ) -> OAuthToken:
        access, refresh = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
        now = int(time.time())
        values = (
            self.client_id,
            json.dumps(scopes),
            self.resource,
            self.owner_user_id,
            self._digest(access),
            self._digest(refresh),
            now + self.ACCESS_TTL,
            now + self.REFRESH_TTL,
        )
        if grant_id is None:
            db.execute(
                "INSERT INTO oauth_grants VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (secrets.token_urlsafe(24), *values),
            )
        else:
            db.execute(
                "UPDATE oauth_grants SET client_id=?, scopes=?, resource=?, subject=?, "
                "access_hash=?, refresh_hash=?, access_expires_at=?, refresh_expires_at=? "
                "WHERE grant_id=?",
                (*values, grant_id),
            )
        return OAuthToken(
            access_token=access,
            refresh_token=refresh,
            expires_in=self.ACCESS_TTL,
            scope=" ".join(scopes),
            token_type="Bearer",
        )

    async def exchange_authorization_code(
        self, client: OAuthClientInformationFull, authorization_code: AuthorizationCode
    ) -> OAuthToken:
        def exchange(db: sqlite3.Connection) -> OAuthToken:
            row = db.execute(
                "SELECT * FROM oauth_codes WHERE code_hash=?",
                (self._digest(authorization_code.code),),
            ).fetchone()
            if row is None or client.client_id != self.client_id:
                raise TokenError("invalid_grant", "Invalid or already used authorization code")
            params = self._deserialize_params(row["params"])
            if params is None:
                raise TokenError("invalid_grant", "Authorization settings have changed")
            db.execute(
                "DELETE FROM oauth_codes WHERE code_hash=?",
                (self._digest(authorization_code.code),),
            )
            return self._issue(db, params.scopes or [])

        return await self._db(exchange)

    async def load_refresh_token(
        self, client: OAuthClientInformationFull, refresh_token: str
    ) -> RefreshToken | None:
        def load(db: sqlite3.Connection) -> tuple[RefreshToken | None, bool]:
            digest = self._digest(refresh_token)
            row = db.execute(
                "SELECT * FROM oauth_grants WHERE refresh_hash=? AND client_id=?",
                (digest, client.client_id),
            ).fetchone()
            if row is None:
                # Reuse of a rotated refresh token invalidates its successor.
                revoked = (
                    db.execute(
                        "DELETE FROM oauth_grants WHERE client_id=? AND grant_id IN "
                        "(SELECT grant_id FROM oauth_retired_refresh WHERE token_hash=?)",
                        (client.client_id, digest),
                    ).rowcount
                    > 0
                )
                return None, revoked
            if row["resource"] != self.resource or row["subject"] != self.owner_user_id:
                return None, False
            token = RefreshToken(
                token=refresh_token,
                client_id=row["client_id"],
                scopes=json.loads(row["scopes"]),
                expires_at=row["refresh_expires_at"],
                resource=row["resource"],
                subject=row["subject"],
            )
            return token, False

        token, revoked = await self._db(load)
        if revoked and self.on_revoke is not None:
            await self.on_revoke()
        return token

    async def exchange_refresh_token(
        self, client: OAuthClientInformationFull, refresh_token: RefreshToken, scopes: list[str]
    ) -> OAuthToken:
        def exchange(db: sqlite3.Connection) -> OAuthToken:
            digest = self._digest(refresh_token.token)
            row = db.execute(
                "SELECT * FROM oauth_grants WHERE refresh_hash=? AND client_id=?",
                (digest, client.client_id),
            ).fetchone()
            if row is None:
                raise TokenError("invalid_grant", "Invalid or already used refresh token")
            if not set(scopes).issubset(json.loads(row["scopes"])):
                raise TokenError("invalid_scope", "Cannot expand the granted scopes")
            db.execute("INSERT INTO oauth_retired_refresh VALUES (?, ?)", (digest, row["grant_id"]))
            return self._issue(db, scopes, grant_id=row["grant_id"])

        return await self._db(exchange)

    async def load_access_token(self, token: str) -> AccessToken | None:
        def load(db: sqlite3.Connection) -> AccessToken | None:
            row = db.execute(
                "SELECT * FROM oauth_grants WHERE access_hash=? AND access_expires_at>?",
                (self._digest(token), time.time()),
            ).fetchone()
            if row is None or row["resource"] != self.resource:
                return None
            if row["subject"] != self.owner_user_id or row["client_id"] != self.client_id:
                return None
            return AccessToken(
                token=token,
                client_id=row["client_id"],
                scopes=json.loads(row["scopes"]),
                expires_at=row["access_expires_at"],
                resource=row["resource"],
                subject=row["subject"],
                claims={"sub": row["subject"]},
            )

        return await self._db(load)

    async def revoke_token(self, token: AccessToken | RefreshToken) -> None:
        digest = self._digest(token.token)

        def revoke(db: sqlite3.Connection) -> None:
            db.execute(
                "DELETE FROM oauth_grants WHERE access_hash=? OR refresh_hash=? "
                "OR grant_id IN (SELECT grant_id FROM oauth_retired_refresh WHERE token_hash=?)",
                (digest, digest, digest),
            )

        await self._db(revoke)
        if self.on_revoke is not None:
            await self.on_revoke()

    async def revoke_all(self) -> None:
        """Invalidate every OAuth login, code and token; bridge data is separate."""

        def revoke(db: sqlite3.Connection) -> None:
            for table in ("oauth_pending", "oauth_codes", "oauth_grants"):
                db.execute(f"DELETE FROM {table}")

        await self._db(revoke)
        if self.on_revoke is not None:
            await self.on_revoke()

    def _error(self, message: str, status: int = 400) -> JSONResponse:
        return JSONResponse({"error": message}, status_code=status, headers=self.HEADERS)

    async def _consent_get(self, request: Request) -> Response:
        request_id = request.query_params.get("request_id", "")
        csrf = secrets.token_urlsafe(32)

        def load(db: sqlite3.Connection) -> AuthorizationParams | None:
            row = db.execute(
                "SELECT params FROM oauth_pending WHERE request_hash=?", (self._digest(request_id),)
            ).fetchone()
            if row is None:
                return None
            params = self._deserialize_params(row["params"])
            if params is None:
                return None
            db.execute(
                "UPDATE oauth_pending SET csrf_hash=? WHERE request_hash=?",
                (self._digest(csrf), self._digest(request_id)),
            )
            return params

        params = await self._db(load)
        if params is None:
            return self._error("Invalid or expired login", 403)
        scopes = "".join(f"<li>{html.escape(scope)}</li>" for scope in params.scopes or [])
        response = HTMLResponse(
            "<!doctype html><html><head><title>Connect dots-bridge</title></head>"
            "<body><h1>Connect dots-bridge</h1>"
            "<p>Allow dots to receive your external messages and send replies.</p>"
            f"<p>Requested permissions:</p><ul>{scopes}</ul>"
            '<form method="post" action="/consent">'
            f'<input type="hidden" name="request_id" value="{html.escape(request_id)}">'
            '<label>Owner key <input type="password" name="owner_key" required '
            'autocomplete="off"></label><button type="submit">Allow</button>'
            "</form></body></html>",
            headers={
                **self.HEADERS,
                "Content-Security-Policy": "default-src 'none'; "
                f"form-action 'self' {self._redirect_origin}; "
                "frame-ancestors 'none'; base-uri 'none'",
            },
        )
        response.set_cookie(
            self.COOKIE,
            csrf,
            max_age=self.CONSENT_TTL,
            httponly=True,
            secure=self.secure_cookie,
            samesite="strict",
            path="/consent",
        )
        return response

    @staticmethod
    async def _consent_form(request: Request) -> dict[str, str]:
        if request.headers.get("content-type", "").split(";")[0].strip() != (
            "application/x-www-form-urlencoded"
        ):
            raise ValueError("Expected URL-encoded form")
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 8192:
                raise ValueError("Consent form is too large")
        values = parse_qs(body.decode("utf-8"), keep_blank_values=True, max_num_fields=10)
        if any(len(items) != 1 for items in values.values()):
            raise ValueError("Repeated form fields")
        return {name: items[0] for name, items in values.items()}

    async def _consent_post(self, request: Request) -> Response:
        if request.headers.get("origin") != self.origin:
            return self._error("Invalid request origin", 403)
        try:
            form = await self._consent_form(request)
        except (ValueError, UnicodeError):
            return self._error("Invalid consent form")
        request_id = form.get("request_id", "")
        if not self.check_owner_key(form.get("owner_key", "")):
            return self._error("Invalid or expired login", 403)
        csrf_digest = self._digest(request.cookies.get(self.COOKIE, ""))
        code = secrets.token_urlsafe(32)

        def approve(db: sqlite3.Connection) -> AuthorizationParams | None:
            row = db.execute(
                "SELECT * FROM oauth_pending WHERE request_hash=?", (self._digest(request_id),)
            ).fetchone()
            if row is None or not row["csrf_hash"]:
                return None
            params = self._deserialize_params(row["params"])
            if params is None:
                return None
            if not secrets.compare_digest(csrf_digest, row["csrf_hash"]):
                return None
            db.execute(
                "DELETE FROM oauth_pending WHERE request_hash=?", (self._digest(request_id),)
            )
            db.execute(
                "INSERT INTO oauth_codes VALUES (?, ?, ?)",
                (self._digest(code), row["params"], time.time() + self.CONSENT_TTL),
            )
            return params

        params = await self._db(approve)
        if params is None:
            return self._error("Invalid or expired login", 403)
        query = {"code": code}
        if params.state is not None:
            query["state"] = params.state
        redirect = str(params.redirect_uri)
        separator = "&" if urlsplit(redirect).query else "?"
        response = RedirectResponse(
            redirect + separator + urlencode(query), status_code=302, headers=self.HEADERS
        )
        response.delete_cookie(
            self.COOKIE,
            path="/consent",
            secure=self.secure_cookie,
            httponly=True,
            samesite="strict",
        )
        return response

    def get_routes(self, mcp_path: str | None = None) -> list[Route]:
        if mcp_path not in (None, "/mcp"):
            raise ValueError("PersonalOAuth requires the MCP endpoint at /mcp")
        routes = super().get_routes(mcp_path or "/mcp")
        # The SDK's default metadata advertises secret-based methods only. This
        # deployment has one preconfigured public client and no client secret.
        metadata = build_metadata(
            self.base_url,
            self.service_documentation_url,
            self.client_registration_options,
            self.revocation_options,
        )
        metadata.token_endpoint_auth_methods_supported = ["none"]
        metadata.revocation_endpoint_auth_methods_supported = ["none"]

        async def authorization_metadata(request: Request) -> Response:
            return JSONResponse(
                metadata.model_dump(mode="json", exclude_none=True),
                headers={"Access-Control-Allow-Origin": "*", **self.HEADERS},
            )

        normalized_routes = []
        for route in routes:
            if route.path == "/.well-known/oauth-authorization-server":
                normalized_routes.append(
                    Route(route.path, authorization_metadata, methods=["GET", "OPTIONS"])
                )
            elif route.path == "/revoke":
                normalized_routes.append(
                    Route(
                        route.path,
                        self._public_revoke_endpoint(route.app),
                        methods=["POST", "OPTIONS"],
                    )
                )
            else:
                normalized_routes.append(route)
        routes = normalized_routes
        routes.append(Route("/consent", self._consent_get, methods=["GET"]))
        routes.append(Route("/consent", self._consent_post, methods=["POST"]))
        return routes

    @staticmethod
    def _public_revoke_endpoint(endpoint: ASGIApp) -> ASGIApp:
        """Adapt one SDK schema quirk; all revocation processing stays in SDK.

        MCP SDK's RevocationRequest currently requires a ``client_secret`` form
        field even for public clients. An empty field satisfies that schema
        without adding a client secret. Preserve SDK body limits and CORS.
        """

        async def public_revoke(scope: Scope, receive: Receive, send: Send) -> None:
            content_type = dict(scope.get("headers", [])).get(b"content-type", b"")
            if (
                scope.get("method") != "POST"
                or content_type.split(b";")[0] != b"application/x-www-form-urlencoded"
            ):
                await endpoint(scope, receive, send)
                return
            messages: list[Message] = []
            size = 0
            while True:
                message = await receive()
                messages.append(message)
                size += len(message.get("body", b""))
                if not message.get("more_body", False) or size > 65536:
                    break
            # Oversized requests pass unchanged to the SDK's body limiter.
            if size <= 65536 and messages[-1]["type"] == "http.request":
                body = b"".join(message.get("body", b"") for message in messages)
                if "client_secret" not in parse_qs(body.decode("latin-1"), keep_blank_values=True):
                    body += b"&client_secret="
                    messages = [{"type": "http.request", "body": body, "more_body": False}]
                    scope = dict(scope)
                    scope["headers"] = [
                        (key, value)
                        for key, value in scope.get("headers", [])
                        if key != b"content-length"
                    ] + [(b"content-length", str(len(body)).encode())]

            async def replay() -> Message:
                if messages:
                    return messages.pop(0)
                return await receive()

            await endpoint(scope, replay, send)

        class PublicRevokeAdapter:
            async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
                await public_revoke(scope, receive, send)

        return PublicRevokeAdapter()
