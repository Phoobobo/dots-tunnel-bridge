"""Optional MCP tools that let a dot drive local coding agents through the ``gbb`` CLI.

Local addition (not upstream). Disabled unless ``DOTS_BRIDGE_HERDR_TOOLS=1``; when
disabled nothing here is imported into the MCP surface or the HTTP routes, so the
upstream behaviour is unchanged.

``gbb`` (https://github.com/Phoobobo/grokbot-bridge-herdr) launches and watches
grok/codex agents in Herdr panes. Every tool here runs ``gbb`` with an argument
list (never a shell string), validates its inputs first, bounds its runtime, and
returns gbb's JSON document.

Status notifications: gbb can POST its status webhooks to extra targets
(``[[extra_webhooks]]`` in gbb's config). ``POST /herdr/webhook`` on this bridge
accepts them on loopback with a shared token and forwards each one to the dot as an
``app.message.created`` event on the first subscribed channel in
``DOTS_BRIDGE_HERDR_CHANNELS`` (default ``herdr,default``).

Environment:
  DOTS_BRIDGE_HERDR_TOOLS=1                  enable the tools and the webhook receiver
  DOTS_BRIDGE_GBB_BIN                        gbb path (default ~/.local/bin/gbb, then PATH)
  DOTS_BRIDGE_HERDR_PATH                     extra PATH entries (':'-separated), prepended
  DOTS_BRIDGE_HERDR_PROTECTED_WORKSPACES     comma list never touched (default wAS,wAT)
  DOTS_BRIDGE_HERDR_WEBHOOK_TOKEN_FILE       receiver token file
                                             (default ~/.config/dots-bridge/herdr_webhook_token)
  DOTS_BRIDGE_HERDR_CHANNELS                 preferred event channels (default herdr,default)
  DOTS_BRIDGE_GBB_OWNER                      owner recorded on tasks launched / handed over from
                                             here (default "dot"; empty = none). gbb sends their
                                             status callbacks to its [[owners]] target of that name
                                             and falls back to its primary webhook.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import secrets
import shutil
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, Literal

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError
from pydantic import Field
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from .errors import BridgeError

ENABLE_ENV = "DOTS_BRIDGE_HERDR_TOOLS"
DEFAULT_PROTECTED = ("wAS", "wAT")
DEFAULT_CHANNELS = ("herdr", "default")
WEBHOOK_PATH = "/herdr/webhook"
WEBHOOK_TOKEN_HEADER = "x-dots-herdr-token"
MAX_OUTPUT_BYTES = 200_000
MAX_WAIT_SECONDS = 120
LAUNCH_TIMEOUT_SECONDS = 300
SEND_TIMEOUT_SECONDS = 180
DEFAULT_TIMEOUT_SECONDS = 30

DRAFT_PR_POLICY = (
    "\n\n---\nDispatcher policy (from the user's ChatGPT dot via dots-bridge): work only in "
    "this worktree and branch. If you open a pull request, open it as a draft "
    "(`gh pr create --draft`). Never merge, never push to the default branch, never "
    "force-push, and never run `herdr server stop`."
)

TARGET_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9:_.-]{0,79}$")
PANE_RE = re.compile(r"^[A-Za-z0-9]{1,20}:[A-Za-z0-9]{1,20}$")
TASK_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,39}$")
BRANCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,99}$")
KEY_RE = re.compile(r"^[A-Za-z0-9+_-]{1,20}$")
OWNER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,39}$")  # same as gbb's OWNER_RE
DEFAULT_OWNER = "dot"

Runner = Callable[[Sequence[str], float], Awaitable[dict[str, Any]]]

_STATUS = Literal["idle", "working", "blocked", "done", "unknown"]


def _split(value: str | None, default: Sequence[str]) -> tuple[str, ...]:
    if value is None:
        return tuple(default)
    return tuple(item.strip() for item in value.split(",") if item.strip())


def tool_path(extra: str | None = None) -> str:
    """PATH for gbb and its children: launchd gives services a very thin PATH."""
    home = Path.home()
    parts = [
        *(p for p in (extra or "").split(":") if p),
        str(home / ".local/bin"),
        str(home / ".cargo/bin"),
        "/opt/homebrew/bin",
        "/opt/homebrew/sbin",
        "/usr/local/bin",
        "/usr/bin",
        "/bin",
        "/usr/sbin",
        "/sbin",
        *(p for p in os.environ.get("PATH", "").split(":") if p),
    ]
    seen: list[str] = []
    for part in parts:
        if part not in seen:
            seen.append(part)
    return ":".join(seen)


@dataclass
class HerdrConfig:
    gbb_bin: str
    path: str
    protected_workspaces: frozenset[str] = frozenset(DEFAULT_PROTECTED)
    channels: tuple[str, ...] = DEFAULT_CHANNELS
    webhook_token_file: Path | None = None
    owner: str | None = DEFAULT_OWNER
    extra_env: dict[str, str] = field(default_factory=dict)
    runner: Runner | None = None  # injectable for tests

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> HerdrConfig | None:
        env = os.environ if environ is None else environ
        if env.get(ENABLE_ENV) != "1":
            return None
        path = tool_path(env.get("DOTS_BRIDGE_HERDR_PATH"))
        gbb = env.get("DOTS_BRIDGE_GBB_BIN") or str(Path.home() / ".local/bin/gbb")
        if not Path(gbb).is_file():
            gbb = shutil.which("gbb", path=path) or gbb
        token_file = env.get("DOTS_BRIDGE_HERDR_WEBHOOK_TOKEN_FILE") or str(
            Path.home() / ".config/dots-bridge/herdr_webhook_token"
        )
        owner = env.get("DOTS_BRIDGE_GBB_OWNER", DEFAULT_OWNER).strip() or None
        if owner is not None and not OWNER_RE.fullmatch(owner):
            raise ValueError("DOTS_BRIDGE_GBB_OWNER must match " + OWNER_RE.pattern)
        return cls(
            gbb_bin=gbb,
            owner=owner,
            path=path,
            protected_workspaces=frozenset(
                _split(env.get("DOTS_BRIDGE_HERDR_PROTECTED_WORKSPACES"), DEFAULT_PROTECTED)
            ),
            channels=_split(env.get("DOTS_BRIDGE_HERDR_CHANNELS"), DEFAULT_CHANNELS)
            or DEFAULT_CHANNELS,
            webhook_token_file=Path(token_file).expanduser(),
        )

    def env(self) -> dict[str, str]:
        base = {
            k: v
            for k, v in os.environ.items()
            if k in {"HOME", "USER", "LOGNAME", "SHELL", "TMPDIR", "LANG", "LC_ALL", "TERM"}
        }
        base.setdefault("HOME", str(Path.home()))
        base.setdefault("LANG", "en_US.UTF-8")
        return {**base, **self.extra_env, "PATH": self.path}

    def webhook_token(self) -> str | None:
        if self.webhook_token_file is None:
            return None
        try:
            token = self.webhook_token_file.read_text().strip()
        except OSError:
            return None
        return token if len(token) >= 32 else None


def make_runner(config: HerdrConfig) -> Runner:
    async def run(args: Sequence[str], limit_seconds: float) -> dict[str, Any]:
        try:
            proc = await asyncio.create_subprocess_exec(
                config.gbb_bin,
                *args,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=config.env(),
                cwd=str(Path.home()),
            )
        except OSError as exc:
            raise ToolError(f"gbb_unavailable: cannot run gbb ({type(exc).__name__})") from None
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), limit_seconds)
        except TimeoutError:
            proc.kill()
            await proc.wait()
            raise ToolError(f"gbb_timeout: gbb {args[0]} exceeded {int(limit_seconds)}s") from None
        return _parse(stdout, stderr, proc.returncode)

    return run


def _parse(stdout: bytes, stderr: bytes, code: int | None) -> dict[str, Any]:
    text = stdout[-MAX_OUTPUT_BYTES:].decode("utf-8", "replace").strip()
    try:
        result = json.loads(text) if text else None
    except ValueError:
        result = None
    if not isinstance(result, dict):
        result = {"ok": False, "raw": text[-4000:] or None}
    if code not in (0, None) and stderr:
        result.setdefault("stderr", stderr[-2000:].decode("utf-8", "replace"))
    result["exit_code"] = code
    return result


def _check(pattern: re.Pattern[str], value: str, what: str) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value) or ".." in value:
        raise ToolError(f"invalid_argument: {what} has an unsupported format")
    return value


def _clean_text(value: str, what: str) -> str:
    if "\x00" in value:
        raise ToolError(f"invalid_argument: {what} contains NUL")
    if not value.strip():
        raise ToolError(f"invalid_argument: {what} is empty")
    return value


def _repo(value: str) -> str:
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ToolError("invalid_argument: repo must be an absolute path or start with ~/")
    path = path.resolve()
    if not path.is_dir():
        raise ToolError("invalid_argument: repo directory not found")
    if str(path).startswith("-"):
        raise ToolError("invalid_argument: repo path")
    return str(path)


def _find(managed: list[dict[str, Any]], target: str) -> dict[str, Any] | None:
    for entry in managed:
        if target in (entry.get("pane_id"), entry.get("task_id"), entry.get("agent_name")):
            return entry
    return None


def register_herdr_tools(
    mcp: FastMCP,
    config: HerdrConfig,
    *,
    meta: dict[str, Any] | None = None,
    auth: Any = None,
    runner: Runner | None = None,
) -> list[str]:
    """Register the gbb tools on ``mcp``. Returns the tool names."""
    run = runner or config.runner or make_runner(config)
    protected = config.protected_workspaces
    names: list[str] = []

    def tool(*, read_only: bool, destructive: bool = False, idempotent: bool = False, world=False):
        annotations = {"readOnlyHint": read_only, "openWorldHint": world}
        if not read_only:
            annotations.update(destructiveHint=destructive, idempotentHint=idempotent)

        def wrap(fn):
            names.append(fn.__name__)
            return mcp.tool(annotations=annotations, auth=auth, meta=meta)(fn)

        return wrap

    async def managed_entry(target: str, action: str) -> dict[str, Any]:
        """Second guard (gbb also refuses unmanaged panes): only registry entries."""
        listing = await run(["list"], DEFAULT_TIMEOUT_SECONDS)
        entry = _find(listing.get("managed") or [], target)
        if entry is None:
            raise ToolError(
                f"not_managed: '{target}' is not a gbb-managed task or pane; refusing to "
                f"{action}. Use gbb_list. User panes are touched only after the user hands "
                "them over in Herdr."
            )
        if entry.get("workspace_id") in protected and entry.get("origin") != "handover":
            raise ToolError(
                f"protected_workspace: {entry.get('workspace_id')} belongs to the user; "
                f"refusing to {action}."
            )
        return entry

    @tool(read_only=True)
    async def gbb_list() -> dict[str, Any]:
        """List the coding-agent tasks this bridge manages on the user's Mac (Herdr panes).

        Use this first to find task ids and pane ids, or to see which agents are running,
        idle, done or blocked. Only gbb-managed panes appear; the user's own panes never do.
        """
        return await run(["list"], DEFAULT_TIMEOUT_SECONDS)

    @tool(read_only=True)
    async def gbb_status(
        target: Annotated[str, Field(description="Task id (e.g. 'fix-login') or pane id")],
    ) -> dict[str, Any]:
        """Show one managed agent's live status, worktree git status and recent commits.

        Use this when you need to know whether an agent is working, idle, done or
        blocked, and what it has changed so far.
        """
        return await run(["status", _check(TARGET_RE, target, "target")], DEFAULT_TIMEOUT_SECONDS)

    @tool(read_only=True)
    async def gbb_read(
        target: Annotated[str, Field(description="Task id or pane id")],
        lines: Annotated[int, Field(ge=1, le=2000)] = 80,
        visible: Annotated[
            bool, Field(description="Read only the visible screen instead of recent output")
        ] = False,
    ) -> dict[str, Any]:
        """Read the recent terminal output of a managed agent's pane.

        Use this to see what the agent is doing or asking (for example a blocked
        permission prompt) before you answer it with gbb_answer, gbb_send or gbb_keys.
        """
        args = ["read", _check(TARGET_RE, target, "target"), "--lines", str(lines)]
        if visible:
            args.append("--visible")
        return await run(args, DEFAULT_TIMEOUT_SECONDS)

    @tool(read_only=True)
    async def gbb_result(
        target: Annotated[str, Field(description="Task id or pane id")],
    ) -> dict[str, Any]:
        """Summarise what a managed agent produced: branch, commits since launch, diff stat,
        uncommitted files and a resume command.

        Use this when an agent reports done/idle to review its work before telling the user.
        """
        return await run(["result", _check(TARGET_RE, target, "target")], DEFAULT_TIMEOUT_SECONDS)

    @tool(read_only=True)
    async def gbb_doctor() -> dict[str, Any]:
        """Check the local agent bridge: gbb and Herdr versions, plugin link, webhook config
        (host only, never secrets) and registry paths.

        Use this when a gbb tool fails or before the first launch.
        """
        return await run(["doctor"], DEFAULT_TIMEOUT_SECONDS)

    @tool(read_only=True)
    async def gbb_wait(
        target: Annotated[str, Field(description="Task id or pane id")],
        until: Annotated[
            list[_STATUS] | None,
            Field(description="States to wait for; default idle, done or blocked"),
        ] = None,
        timeout_seconds: Annotated[int, Field(ge=1, le=MAX_WAIT_SECONDS)] = 60,
    ) -> dict[str, Any]:
        """Wait (at most 120 s) until a managed agent reaches one of the given states.

        Use this to poll for completion after gbb_launch or gbb_send. On timeout it
        returns ok=false with a timeout error; call it again or check gbb_status.
        """
        args = ["wait", _check(TARGET_RE, target, "target")]
        for state in dict.fromkeys(until or []):
            args += ["--until", state]
        args += ["--timeout", str(timeout_seconds * 1000)]
        return await run(args, timeout_seconds + 15)

    @tool(read_only=False, destructive=False, world=True)
    async def gbb_launch(
        repo: Annotated[
            str, Field(description="Absolute path (or ~/...) of a git repo on the user's Mac")
        ],
        task: Annotated[
            str,
            Field(description="Short task id: lowercase letters, digits, '-' or '_' (max 40)"),
        ],
        prompt: Annotated[str, Field(min_length=1, max_length=32000)],
        kind: Literal["codex", "grok"] = "codex",
        worktree: Annotated[
            bool,
            Field(description="Work in a new git worktree on branch gbb/<task> (recommended)"),
        ] = True,
        branch: Annotated[
            str | None, Field(description="Branch for the worktree; default gbb/<task>")
        ] = None,
        base: Annotated[
            str | None, Field(description="Base ref for a new worktree branch; default HEAD")
        ] = None,
    ) -> dict[str, Any]:
        """Start a coding agent (codex or grok) on a task in a repo on the user's Mac.

        Use this only when the user asks for coding work to be done locally. The agent
        runs in its own Herdr workspace and, by default, its own git worktree and branch,
        so the user's checkout and their own Herdr workspaces are untouched. A policy
        footer is appended to the prompt: draft pull requests only, never merge or push to
        the default branch. Takes up to a few minutes; then poll gbb_wait / gbb_status.
        """
        args = [
            "launch",
            "--repo",
            _repo(repo),
            "--kind",
            kind,
            "--task",
            _check(TASK_RE, task, "task"),
        ]
        if worktree:
            if branch:
                args += ["--worktree", _check(BRANCH_RE, branch, "branch")]
        else:
            if branch:
                raise ToolError("invalid_argument: branch requires worktree=true")
            args.append("--no-worktree")
        if base:
            args += ["--base", _check(BRANCH_RE, base, "base")]
        args += ["--prompt", _clean_text(prompt, "prompt") + DRAFT_PR_POLICY]
        if config.owner:
            args += ["--owner", config.owner]
        return await run(args, LAUNCH_TIMEOUT_SECONDS)

    @tool(read_only=False, destructive=False, world=True)
    async def gbb_send(
        target: Annotated[str, Field(description="Task id or pane id of a managed agent")],
        text: Annotated[str | None, Field(min_length=1, max_length=16000)] = None,
        pending: Annotated[
            bool, Field(description="Resubmit the launch prompt saved by gbb (startup_blocked)")
        ] = False,
    ) -> dict[str, Any]:
        """Send a follow-up message or answer to a managed coding agent and submit it.

        Use this to give an agent more instructions or answer a free-text question. A
        multiple-choice question or startup dialog must be answered with gbb_answer instead
        (gbb refuses to type into it and says so). pending=true resubmits the launch prompt
        that gbb saved when a startup dialog blocked it (text is then ignored). Refuses
        panes that gbb does not manage and the user's own workspaces.
        """
        target = _check(TARGET_RE, target, "target")
        await managed_entry(target, "send text")
        if pending:
            return await run(["send", target, "--pending"], SEND_TIMEOUT_SECONDS)
        if text is None:
            raise ToolError("invalid_argument: text is required unless pending=true")
        return await run(["send", target, _clean_text(text, "text")], SEND_TIMEOUT_SECONDS)

    @tool(read_only=False, destructive=False, world=True)
    async def gbb_answer(
        target: Annotated[str, Field(description="Task id or pane id of a managed agent")],
        choice: Annotated[
            str,
            Field(
                min_length=1,
                max_length=200,
                description="Option number ('2') or (part of) its label, as shown by gbb_read",
            ),
        ],
    ) -> dict[str, Any]:
        """Pick an option in a Codex question or startup dialog (e.g. 'Hooks need review').

        Use this when a status event says blocked with blocked_reason question,
        hooks_review or dialog, or gbb_send refuses with agent_blocked. gbb opens a queued
        question, moves to the option, verifies the selection and presses enter; if the
        launch prompt was held back by the dialog it is submitted afterwards. Never pick
        options that opt in to data sharing; for hooks prefer 'Continue without trusting'
        unless the user said to trust them. Refuses unmanaged panes.
        """
        target = _check(TARGET_RE, target, "target")
        choice = _clean_text(choice, "choice").strip()
        if choice.startswith("-"):
            raise ToolError("invalid_argument: choice must not start with '-'")
        await managed_entry(target, "answer a question")
        return await run(["answer", target, choice], SEND_TIMEOUT_SECONDS)

    @tool(read_only=False, destructive=True, world=True)
    async def gbb_keys(
        target: Annotated[str, Field(description="Task id or pane id of a managed agent")],
        keys: Annotated[
            list[str],
            Field(
                min_length=1,
                max_length=10,
                description="Herdr key names, e.g. ['enter'], ['esc'], ['down','enter'], ['y']",
            ),
        ],
    ) -> dict[str, Any]:
        """Press keys in a managed agent's pane, e.g. to pick an option in a dialog.

        Use this only after gbb_read shows a prompt that needs a key press. Never opt in to
        data-sharing banners. Refuses unmanaged panes and the user's own workspaces.
        """
        target = _check(TARGET_RE, target, "target")
        checked = [_check(KEY_RE, key, "key") for key in keys]
        await managed_entry(target, "press keys")
        return await run(["keys", target, *checked], DEFAULT_TIMEOUT_SECONDS)

    @tool(read_only=False, destructive=True, idempotent=True)
    async def gbb_stop(
        target: Annotated[str, Field(description="Task id or pane id of a managed agent")],
    ) -> dict[str, Any]:
        """Stop a managed agent task and close the Herdr workspace gbb created for it.

        Use this when the user is done with a task. The git worktree and branch are kept.
        A handed-over user pane is only unregistered, never closed. Refuses unmanaged panes.
        """
        target = _check(TARGET_RE, target, "target")
        await managed_entry(target, "stop")
        return await run(["stop", target], DEFAULT_TIMEOUT_SECONDS)

    @tool(read_only=False, destructive=False, idempotent=True)
    async def gbb_handover(
        pane: Annotated[str, Field(description="Herdr pane id such as 'w12:p1'")],
        note: Annotated[str | None, Field(max_length=500)] = None,
    ) -> dict[str, Any]:
        """Take over watching an existing Herdr pane that the user explicitly hands to you.

        Use this only when the user names the pane and asks you to take it over. Refuses
        the user's protected workspaces; the user can still hand those over themselves from
        Herdr's pane action.
        """
        pane = _check(PANE_RE, pane, "pane")
        workspace = pane.split(":", 1)[0]
        if workspace in protected:
            raise ToolError(f"protected_workspace: {workspace} belongs to the user; refusing")
        args = ["handover", "--pane", pane]
        if note:
            args += ["--note", _clean_text(note, "note")]
        if config.owner:
            args += ["--owner", config.owner]
        return await run(args, DEFAULT_TIMEOUT_SECONDS)

    @tool(read_only=False, destructive=False, idempotent=True)
    async def gbb_reclaim(
        pane: Annotated[str, Field(description="Pane id of a managed pane")],
    ) -> dict[str, Any]:
        """Release a managed pane back to the user without closing it.

        Use this when the user wants to take a pane back. The agent keeps running.
        """
        pane = _check(PANE_RE, pane, "pane")
        await managed_entry(pane, "reclaim")
        return await run(["reclaim", "--pane", pane], DEFAULT_TIMEOUT_SECONDS)

    mcp.instructions = (mcp.instructions or "") + (
        " Local coding agents: the gbb_* tools drive codex/grok agents in Herdr on the "
        "owner's Mac. Launch only when the owner asks for local coding work; each task gets "
        "its own worktree and Herdr workspace. Poll with gbb_wait (max 120 s) or gbb_status, "
        "review with gbb_result, and gbb_stop when finished. If gbb_launch returns "
        "state=startup_blocked or an agent is blocked on a menu, answer it with gbb_answer "
        "(not gbb_send). Agents may open draft pull "
        "requests only. Never touch panes that gbb does not manage or the owner's own "
        "workspaces. Status changes may also arrive as app.message.created events whose "
        "metadata.source is 'herdr'; those need no reply beyond a brief acknowledgement."
    )
    return names


_EVENT_FIELDS = (
    "event",
    "status",
    "task_id",
    "pane_id",
    "workspace_id",
    "agent_kind",
    "agent_name",
    "origin",
    "repo",
    "worktree",
    "branch",
    "hostname",
    "ts",
    "note",
    "owner",
    "route",
    "blocked_reason",
    "fallback_reason",
    "prompt_pending",
    "herdr_status",
)

_BLOCKED_HINTS = {
    "hooks_review": "Codex is asking to review its hooks at startup; answer with gbb_answer "
    "(e.g. 'Continue without trusting' unless the owner said to trust them).",
    "question": "The agent asked a multiple-choice question; read it with gbb_read and pick "
    "an option with gbb_answer.",
    "dialog": "A dialog is open; read it with gbb_read and pick an option with gbb_answer.",
    "prompt_not_submitted": "The launch prompt never reached the agent; check gbb_read, then "
    "gbb_send with pending=true.",
}


def event_text(data: dict[str, Any], owner: str | None = DEFAULT_OWNER) -> str:
    task = data.get("task_id") or data.get("pane_id") or "unknown task"
    kind = f" ({data['agent_kind']})" if data.get("agent_kind") else ""
    status = f": {data['status']}" if data.get("status") else ""
    hint = {
        "done": "Review with gbb_result, then tell the owner.",
        "idle": "The agent is waiting; check gbb_read / gbb_result.",
        "blocked": "The agent needs input; read it with gbb_read, then gbb_answer for a menu "
        "or gbb_send for free text.",
    }.get(data.get("status") or "", "Check with gbb_status if needed.")
    if data.get("status") == "blocked" and data.get("blocked_reason") in _BLOCKED_HINTS:
        hint = _BLOCKED_HINTS[data["blocked_reason"]]
    if data.get("prompt_pending") is True and data.get("blocked_reason") != "prompt_not_submitted":
        hint += " The launch prompt is saved and is sent after the dialog is answered."
    if data.get("route") == "extra" and data.get("owner") and data.get("owner") != owner:
        hint = f"FYI only: this task belongs to {data['owner']}, not you; no action needed."
    return f"[herdr] {task}{kind} {data.get('event')}{status}. {hint}"


def make_webhook_route(bridge: Any, config: HerdrConfig) -> Route:
    """Loopback receiver for gbb's ``extra_webhooks``; forwards to the dot as an event."""

    async def receive(request: Request) -> JSONResponse:
        host = request.client.host if request.client else ""
        if host not in {"127.0.0.1", "::1"}:
            return JSONResponse({"code": "forbidden"}, 403)
        token = config.webhook_token()
        if token is None:
            return JSONResponse({"code": "not_configured"}, 503)
        supplied = request.headers.get(WEBHOOK_TOKEN_HEADER, "")
        if not secrets.compare_digest(supplied.encode(), token.encode()):
            return JSONResponse({"code": "unauthorized"}, 401)
        body = await request.body()
        try:
            payload = json.loads(body)
        except ValueError:
            return JSONResponse({"code": "invalid_json"}, 400)
        if not isinstance(payload, dict) or payload.get("source") != "grokbot-bridge-herdr":
            return JSONResponse({"code": "invalid_payload"}, 422)
        data = {
            k: (v[:500] if isinstance(v, str) else v)
            for k, v in payload.items()
            if k in _EVENT_FIELDS and (v is None or isinstance(v, str | int | float | bool))
        }
        metadata = {"source": "herdr", **data}
        key = "herdr_" + hashlib.sha256(body).hexdigest()[:48]
        for channel in config.channels:
            try:
                message = await bridge.send_message(
                    event_text(data, config.owner),
                    channel=channel,
                    metadata=metadata,
                    idempotency_key=key,
                )
            except BridgeError as exc:
                if exc.code == "no_subscription":
                    continue
                return JSONResponse({"code": exc.code}, 502)
            return JSONResponse(
                {"delivered": True, "channel": channel, "request_id": message.get("request_id")},
                202,
            )
        return JSONResponse({"delivered": False, "reason": "no_subscription"}, 202)

    return Route(WEBHOOK_PATH, receive, methods=["POST"])
