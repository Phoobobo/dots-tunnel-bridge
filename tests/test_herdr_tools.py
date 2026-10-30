"""Local addition: gbb/Herdr coding-agent tools and the herdr status receiver."""

from __future__ import annotations

import json
import os
import stat
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
from starlette.applications import Starlette
from test_mcp import Backend

from dots_bridge.errors import BridgeError
from dots_bridge.herdr_tools import (
    DRAFT_PR_POLICY,
    WEBHOOK_TOKEN_HEADER,
    HerdrConfig,
    event_text,
    make_runner,
    make_webhook_route,
    tool_path,
)
from dots_bridge.mcp import PROTOCOL_VERSION, create_mcp

MANAGED = [
    {"task_id": "mine", "pane_id": "w9:p1", "workspace_id": "w9", "origin": "launched"},
    {"task_id": "ho-was", "pane_id": "wAS:p2", "workspace_id": "wAS", "origin": "handover"},
    {"task_id": "odd", "pane_id": "wAT:p1", "workspace_id": "wAT", "origin": "launched"},
]

GBB_TOOLS = {
    "gbb_list",
    "gbb_status",
    "gbb_read",
    "gbb_result",
    "gbb_doctor",
    "gbb_wait",
    "gbb_launch",
    "gbb_send",
    "gbb_answer",
    "gbb_keys",
    "gbb_stop",
    "gbb_handover",
    "gbb_reclaim",
}


class FakeRunner:
    def __init__(self):
        self.calls: list[tuple[list[str], float]] = []

    async def __call__(self, args, limit_seconds):
        args = list(args)
        self.calls.append((args, limit_seconds))
        if args[0] == "list":
            return {"ok": True, "managed": MANAGED, "exit_code": 0}
        return {"ok": True, "cmd": args[0], "exit_code": 0}

    def non_list(self):
        return [c for c in self.calls if c[0][0] != "list"]


def config(runner=None, **kw) -> HerdrConfig:
    return HerdrConfig(gbb_bin="/nonexistent/gbb", path="/usr/bin:/bin", runner=runner, **kw)


@asynccontextmanager
async def rpc(herdr: HerdrConfig | None):
    mcp = create_mcp(Backend(), None, herdr=herdr)
    app = mcp.http_app(
        path="/mcp", json_response=True, stateless_http=True, allowed_hosts=["testserver"]
    )
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
            headers={"Accept": "application/json, text/event-stream"},
        ) as client:

            async def call(method: str, params: dict[str, Any] | None = None):
                payload = {
                    **(params or {}),
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
                        "io.modelcontextprotocol/clientCapabilities": {},
                    },
                }
                headers = {"MCP-Protocol-Version": PROTOCOL_VERSION, "Mcp-Method": method}
                if "name" in payload:
                    headers["Mcp-Name"] = payload["name"]
                response = await client.post(
                    "/mcp",
                    headers=headers,
                    json={"jsonrpc": "2.0", "id": "t", "method": method, "params": payload},
                )
                return response.json()["result"]

            async def tool(name: str, **arguments):
                return await call("tools/call", {"name": name, "arguments": arguments})

            yield call, tool


def test_disabled_by_default():
    assert HerdrConfig.from_env({}) is None
    assert HerdrConfig.from_env({"DOTS_BRIDGE_HERDR_TOOLS": "0"}) is None
    cfg = HerdrConfig.from_env(
        {"DOTS_BRIDGE_HERDR_TOOLS": "1", "DOTS_BRIDGE_HERDR_PROTECTED_WORKSPACES": "a, b"}
    )
    assert cfg is not None and cfg.protected_workspaces == {"a", "b"}
    assert cfg.channels == ("herdr", "default")
    assert cfg.owner == "dot"
    on = {"DOTS_BRIDGE_HERDR_TOOLS": "1"}
    assert HerdrConfig.from_env({**on, "DOTS_BRIDGE_GBB_OWNER": "dot-2"}).owner == "dot-2"
    assert HerdrConfig.from_env({**on, "DOTS_BRIDGE_GBB_OWNER": ""}).owner is None
    with pytest.raises(ValueError):
        HerdrConfig.from_env({**on, "DOTS_BRIDGE_GBB_OWNER": "--bad owner"})


def test_tool_path_includes_user_and_homebrew_bins():
    path = tool_path("/extra").split(":")
    assert path[0] == "/extra"
    assert os.path.expanduser("~/.local/bin") in path
    assert "/opt/homebrew/bin" in path and "/usr/bin" in path
    assert len(path) == len(set(path))


async def test_tools_hidden_when_disabled_and_annotated_when_enabled():
    async with rpc(None) as (call, _):
        names = {t["name"] for t in (await call("tools/list"))["tools"]}
        assert not names & GBB_TOOLS
    async with rpc(config(FakeRunner())) as (call, _):
        tools = {t["name"]: t for t in (await call("tools/list"))["tools"]}
        assert GBB_TOOLS <= set(tools)
        for name in ("gbb_list", "gbb_status", "gbb_read", "gbb_result", "gbb_doctor"):
            assert tools[name]["annotations"]["readOnlyHint"] is True
        assert tools["gbb_stop"]["annotations"]["destructiveHint"] is True
        assert tools["gbb_launch"]["annotations"]["readOnlyHint"] is False
        assert "Use this" in tools["gbb_launch"]["description"]
        assert tools["gbb_list"]["securitySchemes"] == tools["gbb_list"]["_meta"]["securitySchemes"]


async def test_read_only_tools_build_argument_lists():
    runner = FakeRunner()
    async with rpc(config(runner)) as (_, tool):
        result = await tool("gbb_list")
        assert result["structuredContent"]["managed"] == MANAGED
        await tool("gbb_status", target="mine")
        await tool("gbb_read", target="w9:p1", lines=20, visible=True)
        await tool("gbb_result", target="mine")
        await tool("gbb_doctor")
        await tool("gbb_wait", target="mine", until=["done", "blocked"], timeout_seconds=30)
    args = [c[0] for c in runner.calls]
    assert ["status", "mine"] in args
    assert ["read", "w9:p1", "--lines", "20", "--visible"] in args
    assert ["result", "mine"] in args and ["doctor"] in args
    wait = next(c for c in runner.calls if c[0][0] == "wait")
    assert wait[0] == [
        "wait",
        "mine",
        "--until",
        "done",
        "--until",
        "blocked",
        "--timeout",
        "30000",
    ]
    assert wait[1] == 45


async def test_wait_is_bounded_and_inputs_are_validated():
    runner = FakeRunner()
    async with rpc(config(runner)) as (_, tool):
        assert (await tool("gbb_wait", target="mine", timeout_seconds=121))["isError"] is True
        assert (await tool("gbb_status", target="--help"))["isError"] is True
        assert (await tool("gbb_status", target="a b"))["isError"] is True
        assert (await tool("gbb_keys", target="mine", keys=["enter; rm"]))["isError"] is True
        assert (await tool("gbb_read", target="mine", lines=5000))["isError"] is True
    assert runner.calls == []


async def test_launch_appends_draft_pr_policy_and_validates(tmp_path):
    runner = FakeRunner()
    repo = tmp_path / "repo"
    repo.mkdir()
    async with rpc(config(runner)) as (_, tool):
        result = await tool(
            "gbb_launch", repo=str(repo), task="dots-smoke", prompt="print hello", kind="codex"
        )
        assert result.get("isError") is not True
        bad = await tool("gbb_launch", repo=str(repo), task="Bad Task", prompt="x")
        assert bad["isError"] is True
        rel = await tool("gbb_launch", repo="relative/path", task="ok", prompt="x")
        assert rel["isError"] is True
        missing = await tool("gbb_launch", repo=str(tmp_path / "nope"), task="ok", prompt="x")
        assert missing["isError"] is True
        dots = await tool("gbb_launch", repo=str(repo), task="ok", prompt="x", branch="a/../b")
        assert dots["isError"] is True
        await tool(
            "gbb_launch",
            repo=str(repo),
            task="nowt",
            prompt="x",
            kind="grok",
            worktree=False,
            base="main",
        )
    (args, limit), (args2, _) = runner.calls
    assert args[:7] == [
        "launch",
        "--repo",
        str(repo.resolve()),
        "--kind",
        "codex",
        "--task",
        "dots-smoke",
    ]
    assert args[-4] == "--prompt" and args[-3] == "print hello" + DRAFT_PR_POLICY
    assert args[-2:] == ["--owner", "dot"]
    assert "draft" in args[-3] and limit == 300
    assert "--no-worktree" in args2 and args2[args2.index("--base") + 1] == "main"


async def test_launch_without_owner_keeps_old_argv(tmp_path):
    runner = FakeRunner()
    async with rpc(config(runner, owner=None)) as (_, tool):
        await tool("gbb_launch", repo=str(tmp_path), task="t1", prompt="x", worktree=False)
        await tool("gbb_handover", pane="w12:p3")
    (launch,), (handover,) = [c[0] for c in runner.calls[:1]], [c[0] for c in runner.calls[1:]]
    assert "--owner" not in launch and launch[-2] == "--prompt"
    assert handover == ["handover", "--pane", "w12:p3"]


async def test_answer_and_pending_send():
    runner = FakeRunner()
    async with rpc(config(runner)) as (_, tool):
        assert (await tool("gbb_answer", target="mine", choice="Continue without trusting"))[
            "isError"
        ] is False
        assert (await tool("gbb_answer", target="mine", choice="2"))["isError"] is False
        assert (await tool("gbb_answer", target="mine", choice="--pending"))["isError"] is True
        assert (await tool("gbb_answer", target="wAS:p1", choice="1"))["isError"] is True
        assert (await tool("gbb_answer", target="odd", choice="1"))["isError"] is True
        assert (await tool("gbb_send", target="mine", pending=True))["isError"] is False
        assert (await tool("gbb_send", target="mine"))["isError"] is True
    assert [c[0] for c in runner.non_list()] == [
        ["answer", "mine", "Continue without trusting"],
        ["answer", "mine", "2"],
        ["send", "mine", "--pending"],
    ]


async def test_control_tools_refuse_unmanaged_and_protected_panes():
    runner = FakeRunner()
    async with rpc(config(runner)) as (_, tool):
        refused = await tool("gbb_send", target="wAS:p1", text="hi")
        assert refused["isError"] is True and "not_managed" in refused["content"][0]["text"]
        assert (await tool("gbb_stop", target="unknown"))["isError"] is True
        protected = await tool("gbb_keys", target="odd", keys=["enter"])
        assert "protected_workspace" in protected["content"][0]["text"]
        assert (await tool("gbb_handover", pane="wAT:p1"))["isError"] is True
        assert (await tool("gbb_reclaim", pane="wX:p9"))["isError"] is True
        assert runner.non_list() == []
        # Managed panes, including a pane the user handed over from wAS, are allowed.
        assert (await tool("gbb_send", target="mine", text="continue"))["isError"] is False
        assert (await tool("gbb_keys", target="ho-was", keys=["down", "enter"]))["isError"] is False
        assert (await tool("gbb_stop", target="mine"))["isError"] is False
        assert (await tool("gbb_handover", pane="w12:p3", note="fix it"))["isError"] is False
        assert (await tool("gbb_reclaim", pane="w9:p1"))["isError"] is False
    assert [c[0] for c in runner.non_list()] == [
        ["send", "mine", "continue"],
        ["keys", "ho-was", "down", "enter"],
        ["stop", "mine"],
        ["handover", "--pane", "w12:p3", "--note", "fix it", "--owner", "dot"],
        ["reclaim", "--pane", "w9:p1"],
    ]


async def test_real_runner_passes_argv_without_shell_and_sets_path(tmp_path):
    script = tmp_path / "gbb"
    script.write_text(
        '#!/bin/sh\nprintf \'{"ok":true,"argv":["%s","%s"],"path":"%s"}\' "$1" "$2" "$PATH"\n'
    )
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    run = make_runner(HerdrConfig(gbb_bin=str(script), path="/custom/bin:/usr/bin:/bin"))
    result = await run(["send", "$(touch pwned); `id`"], 10)
    assert result["argv"] == ["send", "$(touch pwned); `id`"]
    assert result["path"].startswith("/custom/bin") and result["exit_code"] == 0
    assert not (tmp_path / "pwned").exists()
    script.write_text("#!/bin/sh\necho not json; exit 3\n")
    result = await run(["list"], 10)
    assert result["ok"] is False and result["raw"] == "not json" and result["exit_code"] == 3
    script.write_text("#!/bin/sh\nsleep 5\n")
    with pytest.raises(Exception, match="gbb_timeout"):
        await run(["wait"], 0.5)


class SendBackend:
    def __init__(self, subscribed: set[str]):
        self.subscribed = subscribed
        self.sent: list[dict[str, Any]] = []

    async def send_message(self, text, *, channel, metadata, idempotency_key):
        if channel not in self.subscribed:
            raise BridgeError("no_subscription", "none", 409)
        self.sent.append(
            {"text": text, "channel": channel, "metadata": metadata, "key": idempotency_key}
        )
        return {"request_id": "req_1"}


@asynccontextmanager
async def receiver(backend, token_file, client=("127.0.0.1", 5555)):
    app = Starlette(routes=[make_webhook_route(backend, config(webhook_token_file=token_file))])
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=client), base_url="http://127.0.0.1"
    ) as http:
        yield http


EVENT = {
    "source": "grokbot-bridge-herdr",
    "event": "agent_status",
    "status": "done",
    "task_id": "dots-smoke",
    "pane_id": "w9:p1",
    "agent_kind": "codex",
    "extra_field": "dropped",
    "nested": {"x": 1},
}


async def test_webhook_receiver_forwards_status_to_first_subscribed_channel(tmp_path):
    token = "t" * 40
    token_file = tmp_path / "token"
    token_file.write_text(token + "\n")
    backend = SendBackend({"default"})
    async with receiver(backend, token_file) as http:
        body = json.dumps(EVENT)
        headers = {WEBHOOK_TOKEN_HEADER: token, "content-type": "application/json"}
        response = await http.post("/herdr/webhook", content=body, headers=headers)
        assert response.status_code == 202
        assert response.json() == {"delivered": True, "channel": "default", "request_id": "req_1"}
        (sent,) = backend.sent
        assert sent["metadata"]["source"] == "herdr"
        assert sent["metadata"]["status"] == "done"
        assert "extra_field" not in sent["metadata"] and "nested" not in sent["metadata"]
        assert "dots-smoke" in sent["text"] and "gbb_result" in sent["text"]
        assert (await http.post("/herdr/webhook", content=body)).status_code == 401
        bad = {WEBHOOK_TOKEN_HEADER: "wrong" * 10}
        assert (await http.post("/herdr/webhook", content=body, headers=bad)).status_code == 401
        other = json.dumps({**EVENT, "source": "else"})
        assert (
            await http.post("/herdr/webhook", content=other, headers=headers)
        ).status_code == 422
    nobody = SendBackend(set())
    async with receiver(nobody, token_file) as http:
        response = await http.post("/herdr/webhook", content=body, headers=headers)
        assert response.status_code == 202 and response.json()["delivered"] is False
    async with receiver(backend, token_file, client=("10.0.0.5", 1)) as http:
        response = await http.post("/herdr/webhook", content=body, headers=headers)
        assert response.status_code == 403
    async with receiver(backend, tmp_path / "missing") as http:
        response = await http.post("/herdr/webhook", content=body, headers=headers)
        assert response.status_code == 503


def test_event_text_hints_by_blocked_reason_and_owner():
    base = {"event": "agent_status", "status": "blocked", "task_id": "t1"}
    assert "gbb_answer" in event_text({**base, "blocked_reason": "hooks_review"})
    assert "Continue without trusting" in event_text({**base, "blocked_reason": "hooks_review"})
    assert "gbb_answer" in event_text({**base, "blocked_reason": "question"})
    assert "pending=true" in event_text({**base, "blocked_reason": "prompt_not_submitted"})
    held = event_text({**base, "blocked_reason": "hooks_review", "prompt_pending": True})
    assert "launch prompt is saved" in held
    assert "gbb_answer" in event_text(base) and "gbb_send" in event_text(base)
    mine = {**base, "owner": "dot", "route": "owner"}
    assert "FYI" not in event_text(mine)
    other = {**base, "owner": "canmouzhang", "route": "extra"}
    assert "FYI only" in event_text(other) and "canmouzhang" in event_text(other)
    assert "FYI" not in event_text({**base, "route": "extra"})


async def test_webhook_receiver_keeps_owner_and_route_fields(tmp_path):
    token = "t" * 40
    token_file = tmp_path / "token"
    token_file.write_text(token)
    backend = SendBackend({"herdr"})
    event = {
        **EVENT,
        "status": "blocked",
        "owner": "dot",
        "route": "owner",
        "blocked_reason": "hooks_review",
        "prompt_pending": True,
        "herdr_status": "idle",
        "fallback_reason": None,
    }
    async with receiver(backend, token_file) as http:
        headers = {WEBHOOK_TOKEN_HEADER: token}
        response = await http.post("/herdr/webhook", content=json.dumps(event), headers=headers)
        assert response.status_code == 202
    (sent,) = backend.sent
    meta = sent["metadata"]
    assert meta["owner"] == "dot" and meta["route"] == "owner"
    assert meta["blocked_reason"] == "hooks_review" and meta["prompt_pending"] is True
    assert meta["herdr_status"] == "idle" and "gbb_answer" in sent["text"]
