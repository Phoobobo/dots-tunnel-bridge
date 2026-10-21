"""Standard Webhooks signatures and bounded delivery to public HTTPS endpoints."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import ipaddress
import json
import socket
import time
from urllib.parse import SplitResult, urlsplit

import httpx

MAX_BODY_BYTES = 256 * 1024
MAX_RESPONSE_BYTES = 16 * 1024
DELIVERY_TIMEOUT = 10


def signing_key(secret: str) -> bytes:
    """Decode a Standard Webhooks secret containing a 24–64 byte key."""
    if not isinstance(secret, str) or not secret.startswith("whsec_"):
        raise ValueError("Expected a whsec_ signing secret")
    try:
        key = base64.b64decode(secret[6:], validate=True)
    except (ValueError, TypeError) as exc:
        raise ValueError("Invalid signing secret") from exc
    if not 24 <= len(key) <= 64:
        raise ValueError("Signing key must contain 24–64 bytes")
    return key


def signed_body(
    subscription_id: str,
    secret: str,
    message_id: str,
    payload: dict,
    *,
    old_secret: str | None = None,
) -> tuple[bytes, dict[str, str]]:
    """Serialize once and sign the exact UTF-8 bytes, optionally with two keys.

    The stable message ID identifies retries. The timestamp and signatures are
    regenerated on every attempt. Space-separated signatures allow a receiver
    to accept either key during a subscription secret rotation.
    """
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(body) > MAX_BODY_BYTES:
        raise ValueError("Event exceeds 256 KiB")
    timestamp = str(int(time.time()))
    message = message_id.encode("utf-8") + b"." + timestamp.encode("ascii") + b"." + body
    keys = [signing_key(secret)]
    if old_secret is not None:
        keys.append(signing_key(old_secret))
    signatures = []
    for key in keys:
        digest = hmac.digest(key, message, hashlib.sha256)
        signature = "v1," + base64.b64encode(digest).decode("ascii")
        if signature not in signatures:
            signatures.append(signature)
    return body, {
        "Content-Type": "application/json",
        "webhook-id": message_id,
        "webhook-timestamp": timestamp,
        "webhook-signature": " ".join(signatures),
        "X-MCP-Subscription-Id": subscription_id,
    }


def validate_url(url: str) -> SplitResult:
    """Accept HTTPS callback URLs on port 443; DNS is checked at delivery time."""
    if (
        not isinstance(url, str)
        or not url
        or any(
            character.isspace() or ord(character) < 32 or ord(character) == 127 for character in url
        )
        or "\\" in url
        or "#" in url
    ):
        raise ValueError("Callback must be an HTTPS URL on port 443")
    try:
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port not in (None, 443)
        ):
            raise ValueError("Invalid callback URL")
        # Let HTTPX check URL syntax before any DNS or network operation.
        httpx.URL(url)
    except (ValueError, httpx.InvalidURL) as exc:
        raise ValueError("Callback must be an HTTPS URL on port 443") from exc
    return parsed


async def post_webhook(url: str, body: bytes, headers: dict[str, str]) -> httpx.Response:
    """Resolve once, reject private addresses, and pin TLS to a validated IP.

    The original hostname remains in Host and TLS SNI. Each delivery creates a
    fresh connection, follows no redirects, and ignores environment proxies so
    DNS validation cannot be bypassed by a reused connection or proxy.
    """
    validate_url(url)
    if len(body) > MAX_BODY_BYTES:
        raise ValueError("Event exceeds 256 KiB")
    original = httpx.URL(url)
    hostname = original.raw_host.decode("ascii")
    async with asyncio.timeout(DELIVERY_TIMEOUT):
        addresses = await asyncio.get_running_loop().getaddrinfo(
            hostname, 443, type=socket.SOCK_STREAM
        )
        ips = list(dict.fromkeys(address[4][0] for address in addresses))
        if not ips or any(
            not ipaddress.ip_address(ip).is_global or ipaddress.ip_address(ip).is_multicast
            for ip in ips
        ):
            raise ValueError("Callback must resolve only to public addresses")
        target = original.copy_with(host=ips[0])
        async with httpx.AsyncClient(
            timeout=DELIVERY_TIMEOUT, follow_redirects=False, trust_env=False
        ) as client:
            async with client.stream(
                "POST",
                target,
                content=body,
                headers={**headers, "Host": original.netloc.decode("ascii")},
                extensions={"sni_hostname": hostname},
            ) as response:
                content = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=MAX_RESPONSE_BYTES + 1):
                    content.extend(chunk)
                    if len(content) > MAX_RESPONSE_BYTES:
                        raise ValueError("Callback response exceeds 16 KiB")
                # aiter_bytes has already decoded content encodings. Preserve
                # Retry-After and other headers without decoding the body twice.
                response_headers = {
                    key: value
                    for key, value in response.headers.items()
                    if key.lower() not in ("content-encoding", "content-length")
                }
                return httpx.Response(
                    response.status_code,
                    content=bytes(content),
                    headers=response_headers,
                    request=response.request,
                )
