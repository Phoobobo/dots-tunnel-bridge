"""Command-line setup, serving, and HTTP application client."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from collections.abc import Sequence

import httpx
from pydantic import ValidationError

from .client import BridgeClient, ReplyTimeoutError, _positive
from .config import Settings, write_config
from .errors import BridgeError


def _number(value: str) -> float:
    try:
        return _positive(float(value), "value")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None


def _port(value: str) -> int:
    try:
        port = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("port must be an integer between 1 and 65535") from None
    if not 1 <= port <= 65535:
        raise argparse.ArgumentTypeError("port must be an integer between 1 and 65535")
    return port


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dots-bridge",
        description="Connect applications and OpenAI dots over HTTP and MCP Events.",
    )
    parser.add_argument(
        "--config", default="bridge.toml", help="Configuration file (default: bridge.toml)"
    )
    parser.add_argument("--url", help="Bridge HTTP URL for application commands")
    commands = parser.add_subparsers(dest="command", required=True)

    def command(name: str, help_text: str, *, remote: bool = False, group=commands):
        sub = group.add_parser(name, help=help_text)
        sub.add_argument("--config", default=argparse.SUPPRESS, help="Configuration file")
        if remote:
            sub.add_argument("--url", default=argparse.SUPPRESS, help="Bridge HTTP URL")
        return sub

    init = command("init", "Create a private configuration with generated keys")
    init.add_argument("--public-url", required=True, help="Public HTTPS origin, without /mcp")
    command("serve", "Run the bridge HTTP and MCP server")
    connector = command("connector", "Run a client-side protocol connector", remote=True)
    connectors = connector.add_subparsers(dest="connector", required=True)
    openai = command(
        "openai", "Serve Chat Completions and Responses WebSocket", remote=True, group=connectors
    )
    openai.add_argument("--host", default="127.0.0.1", help="Connector bind host")
    openai.add_argument(
        "--port", type=_port, default=8766, help="Connector bind port (default: 8766)"
    )
    openai.add_argument("--channel", default="default")
    openai.add_argument("--subscription-id")
    openai.add_argument(
        "--reply-timeout", type=_number, default=600.0, help="Reply wait seconds (default: 600)"
    )
    command("subscriptions", "List active dots subscriptions", remote=True)
    send = command("send", "Queue a message for dots", remote=True)
    send.add_argument("text", metavar="TEXT")
    send.add_argument("--channel", default="default")
    send.add_argument("--subscription-id")
    send.add_argument("--idempotency-key")
    send.add_argument("--wait", action="store_true", help="Wait for dots to call reply_to_app")
    send.add_argument(
        "--timeout", type=_number, default=120.0, help="Reply wait seconds (default: 120)"
    )
    get = command("get", "Fetch delivery and reply status", remote=True)
    get.add_argument("request_id", metavar="REQUEST_ID")
    prune = command(
        "prune", "Delete terminal messages whose expiry is older than the retention period"
    )
    prune.add_argument("--days", type=_number, default=30.0, help="Retention days (default: 30)")
    command("revoke", "Revoke all OAuth credentials and dots subscriptions")
    return parser


def _local_url(settings: Settings) -> str:
    host = settings.host
    if host in {"0.0.0.0", "::", "[::]"}:
        host = "127.0.0.1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{settings.port}"


def _print_json(value) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2))


def _configure_connector_logging() -> None:
    """Show safe connector lifecycle diagnostics for the standalone CLI."""
    logger = logging.getLogger("dots_bridge.connectors.openai")
    if not logger.hasHandlers():
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)


async def _maintenance(settings: Settings, command: str, days: float = 30) -> dict:
    from .bridge import DotsBridge

    bridge = DotsBridge(settings)
    await bridge.initialize()
    try:
        if command == "prune":
            count = await bridge.store.prune(time.time() - days * 86400)
            return {"deleted_messages": count}

        from .auth import PersonalOAuth

        auth = PersonalOAuth(
            db_path=settings.db_path,
            public_base_url=settings.public_base_url,
            owner_key=settings.owner_key,
            redirect_uri=settings.redirect_uri,
            client_id=settings.client_id,
            owner_user_id=settings.owner_user_id,
        )
        await auth.revoke_all()
        await bridge.revoke_subscriptions()
        return {"revoked": True}
    finally:
        await bridge.close()


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "init":
            path = write_config(args.config, args.public_url)
            print(f"Created {path}")
            print("Set redirect_uri to the callback shown by your dots plugin registration.")
            return 0

        settings = Settings.from_file(args.config)
        if args.command == "serve":
            import uvicorn

            from .app import create_app

            uvicorn.run(
                create_app(settings),
                host=settings.host,
                port=settings.port,
                workers=1,
                access_log=False,
            )
            return 0
        if args.command == "connector" and args.connector == "openai":
            import uvicorn

            from .connectors.openai import create_openai_connector

            _configure_connector_logging()
            uvicorn.run(
                create_openai_connector(
                    args.url or _local_url(settings),
                    api_key=settings.api_key,
                    channel=args.channel,
                    subscription_id=args.subscription_id,
                    reply_timeout=args.reply_timeout,
                ),
                host=args.host,
                port=args.port,
                workers=1,
                access_log=False,
                ws_max_size=300 * 1024,
            )
            return 0
        if args.command in {"prune", "revoke"}:
            _print_json(
                asyncio.run(_maintenance(settings, args.command, getattr(args, "days", 30)))
            )
            return 0

        with BridgeClient(args.url or _local_url(settings), api_key=settings.api_key) as client:
            if args.command == "subscriptions":
                _print_json({"subscriptions": client.list_subscriptions()})
            elif args.command == "get":
                _print_json(client.get_message(args.request_id))
            elif args.command == "send":
                message = client.send_message(
                    args.text,
                    channel=args.channel,
                    subscription_id=args.subscription_id,
                    idempotency_key=args.idempotency_key,
                )
                # Emit the ID immediately so interrupted waits can be resumed.
                _print_json(message)
                if args.wait:
                    result = client.wait_for_reply(message["request_id"], timeout=args.timeout)
                    _print_json(result)
        return 0
    except ReplyTimeoutError as exc:
        print(str(exc), file=sys.stderr)
        return 3
    except BridgeError as exc:
        print(f"{exc.code}: {exc.message}", file=sys.stderr)
        return 1
    except ValidationError as exc:
        # ValidationError.__str__ includes input values, which may be secret config keys.
        for item in exc.errors(include_input=False, include_url=False, include_context=False):
            print(f"{'.'.join(map(str, item['loc']))}: {item['msg']}", file=sys.stderr)
        return 2
    except (OSError, ValueError, httpx.HTTPError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
