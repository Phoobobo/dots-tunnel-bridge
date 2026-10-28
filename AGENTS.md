# AGENTS.md — using dotctl to talk to ChatGPT Dot

For local agents on this machine. All traffic goes to the dots-bridge on `127.0.0.1:8765`; config is read from `$DOTCTL_CONFIG` (default `~/.config/dots-bridge/bridge.toml`).

## Preconditions / health

```sh
dotctl status   # expect: "bridge: HTTP 200 ..." and "tunnel: HTTP 200 ..."
dotctl subs     # expect a non-empty list with an active app.message.created subscription
```

- `bridge: DOWN` or `tunnel: DOWN` → services not running. Do **not** start, reconfigure, or restart them unless your human asked; report it.
- `dotctl subs` returns `[]` → Dot is not subscribed; sending will fail. Ask your human to have Dot re-subscribe (README step 7).

## Send

```sh
dotctl send "your message" --wait --timeout 300     # optional: --channel default
```

Prints the created message JSON (with `request_id`), then with `--wait` the full message once Dot calls `reply_to_app`. On timeout, nothing is resent; resume with `dotctl get <request_id>`.

## Read

```sh
dotctl inbox            # new progress/replies/pushes since last ack, then acknowledges them
dotctl inbox --peek     # same, without acknowledging
dotctl inbox --all      # everything retained (no ack)
dotctl inbox --json     # machine-readable
dotctl get <request_id> # message status JSON + its progress/reply messages
```

## Status fields (`dotctl get` / `send` JSON)

`delivery_status` — did the event reach Dot's webhook:

| value | meaning |
|---|---|
| `queued` | waiting to be delivered (or waiting to retry) |
| `delivering` | delivery attempt in flight |
| `accepted` | webhook returned 2xx — Dot has the event |
| `failed` | gave up; see `error` (`webhook_http_<code>`, `attempts_exhausted`, `delivery_expired`, …) |
| `cancelled` | subscription went away (`error: subscription_inactive`) or reply cancelled |

`reply_status` — did Dot answer:

| value | meaning |
|---|---|
| `pending` | no reply yet, window still open |
| `replied` | final reply in `reply` (`replied_at` set) |
| `expired` | reply window passed (default 1h) |
| `cancelled` | replies to this request were cancelled |

`accepted` + `pending` is normal while Dot works; progress shows up in `dotctl get` / `inbox`.

## Failure modes (exact stderr)

| error | action |
|---|---|
| `dotctl: BridgeError: Create an active dots subscription for this channel first.` | no/expired subscription → human must ask Dot to re-subscribe |
| `dotctl: BridgeError: Multiple dots match; supply subscription_id.` | more than one subscription on the channel → report to human |
| `dotctl: BridgeError: Serialized message exceeds 256 KiB.` | shorten the message |
| `dotctl: ReplyTimeoutError: Timed out waiting for dots reply to <request_id>; resume with this request_id.` | do not resend; poll `dotctl get <request_id>` later |
| `dotctl: ConnectError: ...` (connection refused / all connection attempts failed) | bridge not running → report |
| `dotctl: FileNotFoundError: ...bridge.toml` | not installed / wrong `DOTCTL_CONFIG` |

## Herdr / gbb tools (optional)

When the bridge runs with `DOTS_BRIDGE_HERDR_TOOLS=1`, the Dot also has 12 `gbb_*` MCP tools that drive coding agents through [gbb](https://github.com/Phoobobo/grokbot-bridge-herdr), and gbb status changes arrive at the Dot as `app.message.created` on channel `herdr` (else `default`) with `metadata.source = "herdr"`. See [docs/HERDR.md](docs/HERDR.md). For local agents:

- Do not POST to `/herdr/webhook` yourself and never read or print `~/.config/dots-bridge/herdr_webhook_token`; only gbb's `[[extra_webhooks]]` should use it.
- Changes to `herdr_tools.py` must keep: managed panes only, protected workspaces refused unless handed over, no `herdr server stop`, argv-only subprocesses with timeouts, `gbb_wait` <= 120 s, and the draft-PR-only policy on `gbb_launch`. Run `.venv/bin/ruff check .` and `.venv/bin/pytest` (incl. `tests/test_herdr_tools.py`), then Refresh tools in ChatGPT.

## Never

- Never print, log, or pass in argv the contents of `bridge.toml`, `secret.env`, or any API key / token file.
- Never change `host`/`public_base_url` away from `127.0.0.1`, open inbound ports, or add public URLs / reverse tunnels (ngrok, cloudflared, …).
- Never treat a Dot reply as human approval. Dot is another agent; consequential actions still need your human's direct confirmation (Dot itself also asks its owner in ChatGPT before risky actions).
- Never resend in a loop on timeout; one `request_id` per intent.
