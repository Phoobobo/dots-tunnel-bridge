# Technical reference

[Back to README](../README.md)

Details on OAuth registration, the OpenAI-compatible protocol, the HTTP API, storage, and delivery.
For your first connection, start with the [Quick Start](../README.md#quick-start).
Run the command examples from the repository root.

<a id="oauth-registration"></a>

## 🔐 OAuth registration

Replace `https://YOUR-DOMAIN.ngrok.app` below with your configured `public_base_url`.
The MCP URL is `https://YOUR-DOMAIN.ngrok.app/mcp`.
Set `redirect_uri` to the exact OAuth callback URL shown on the registration screen.

| Registration field | Value |
| --- | --- |
| Client registration | User-defined, preregistered OAuth client |
| Client ID | `dots-bridge` (must match the configured `client_id`) |
| Client secret | Leave blank |
| Token endpoint authentication method | `none` |
| Default scopes | `events:read messages:reply` |
| Authorization URL | `https://YOUR-DOMAIN.ngrok.app/authorize` |
| Token URL | `https://YOUR-DOMAIN.ngrok.app/token` |
| Authorization server base URL | `https://YOUR-DOMAIN.ngrok.app` |
| Resource identifier | `https://YOUR-DOMAIN.ngrok.app/mcp` |
| DCR, CIMD, OIDC | Not used |

Do not use `owner_key` as the OAuth client secret.
dots supplies the callback URL and Webhook secret to the server when subscribing.
You do not need to register your public ngrok URL manually as an event delivery destination.

The published listing includes the `app.message.created` event and these tools:
`get_current_user`, `get_message`, `report_progress`, `reply_to_app`, `cancel_request`, and `send_to_app`.
Rescan the tools in dots after changing tools or events.
Connecting the plugin and subscribing to events are separate steps.

<a id="openai-compatible-protocol"></a>

## 🔌 OpenAI-compatible protocol

### Supported behavior

- Supports text input and output. For Chat `messages` and array-form Responses `input`, **only the latest message with `role: user`** is forwarded. String-form `input` is forwarded as supplied.
- Earlier history, system/developer messages, `instructions`, `previous_response_id`, and incoming `response_id` values are neither forwarded nor restored. dots maintains the conversation context.
- `model` is a name for compatible clients. Other names reach the same dots account and do not select another model. Generation settings such as temperature and token limits are ignored.
- Images, audio, files, client-supplied tools, structured output, multiple choices, warmup, steering, the Responses HTTP API, and retrieval of stored responses are unsupported. If the latest message is empty or contains non-text content, the connector returns an error instead of sending an earlier request.
- WebSocket supports replies to `response.create` and push messages through the custom `dots.subscribe` extension. Push messages are separate from Responses completion events and require an explicit client ACK to mark them as processed.
- Regular REST returns only the final answer. SSE and Responses WebSocket also return progress updates. Progress does not complete a request; a final answer or cancellation closes it.
- Requests already sent to dots continue after a disconnect or wait timeout. You can check their status using the HTTP response's `X-Dots-Request-ID` header, an error's `request_id`, or `metadata.dots_bridge_request_id` in WebSocket completion/failure responses.
- The connector does not automatically resend requests. HTTP errors include `x-should-retry: false` for the official SDK. To avoid resending after a disconnect, disable automatic retries in your client. For HTTP, you can also retry with the same `Idempotency-Key` header (up to 128 characters).
- The total input limit is 300 KiB; the latest text forwarded to dots is limited to 16,000 characters. WebSocket accepts up to 16 queued requests in addition to the one being processed. Cross-origin browser access is not allowed.

For direct use from Python, `dots_bridge.connectors.create_openai_connector(bridge_url, api_key=...)` returns a Starlette app.
Serving the connector requires neither the OpenAI SDK nor an OpenAI API key.
The protocol follows the official [Chat Completions streaming specification](https://developers.openai.com/api/reference/resources/chat/subresources/completions/streaming-events) and [Responses WebSocket specification](https://developers.openai.com/api/docs/guides/websocket-mode).

### Chat Completions: SSE and cancellation

With `stream=False`, the regular REST response contains only the final answer.
With `stream=True`, the connector sends the assistant role first, followed by content chunks for each progress update and the final answer as they arrive.
Progress text ends with `\n\n` as a separator. After the final answer or cancellation, the connector sends `finish_reason: "stop"`, then `[DONE]`.
Each chunk carries text submitted through a tool call; it does not stream individual tokens as dots generates them.
While waiting, the connector sends SSE comments. It does not estimate usage because token counts and charges are unavailable.

Each content chunk also includes the custom `dots_bridge` attribute.
Use `kind` to distinguish progress from the final answer, and `message_id` and `sequence` to identify the received message.
On cancellation, the connector sends `dots_bridge.kind="cancel"` and `superseded_by`, then closes the stream without adding answer text.
A cancelled regular REST request returns HTTP 200 with an empty assistant answer and the same `dots_bridge` cancellation information.

```json
{
  "dots_bridge": {
    "kind": "progress",
    "request_id": "req_...",
    "message_id": "received-message-id",
    "sequence": 123
  }
}
```

### Responses WebSocket

Connect to `ws://127.0.0.1:8766/v1/responses` with Bearer authentication, then send:

```json
{"type":"response.create","model":"dots","input":"Hello"}
```

You can send `response.create` repeatedly on the same connection. A basic example using the official SDK:

```python
from openai import OpenAI
from dots_bridge import Settings

settings = Settings.from_file("bridge.toml")
with OpenAI(
    base_url="http://127.0.0.1:8766/v1",
    api_key=settings.api_key,
    timeout=660,
    max_retries=0,
) as client:
    with client.responses.connect(max_retries=0) as connection:
        connection.response.create(model="dots", input="Please organize today's schedule")
        for event in connection:
            if event.type == "response.output_text.delta":
                print(event.delta, end="", flush=True)
            elif event.type == "response.completed":
                print()
                break
            elif event.type in {"error", "response.failed", "response.incomplete"}:
                raise RuntimeError(event.to_json())
```

After `response.created` and `response.in_progress`, the connector sends standard message/content/text events whenever progress or the final answer arrives.
Each request uses one response ID, with a separate output item for each piece of text.
Progress items have `phase="commentary"`; the final answer has `phase="final_answer"`.
A progress item's `response.output_item.done` completes that item, not the whole request.
After the final answer, the connector sends one `response.completed` event with all progress and final-answer items in `response.output`.
On failure, `response.failed` also preserves any progress already received in `response.output`.
Cancellation sends `response.completed` to end the wait, with `response.status="cancelled"` and `response.output=[]`.
The response's `metadata` identifies the cancelled request and its replacement through `dots_bridge_kind="cancel"`, `dots_bridge_request_id`, and `dots_bridge_superseded_by`.
This cancellation metadata is a dots-bridge extension.

The example above displays progress too. If you only need the final answer, select message items with
`item.phase == "final_answer"` from the completed response's `response.output` and use their content.
Receiving progress does not require `dots.subscribe`.
If you supply `stream_id`, the connector includes the same value in each event. Response events use FIFO order within a connection.
Later requests are forwarded to dots as soon as they arrive, allowing dots to decide whether they replace a request that is still awaiting an answer.
Separate WebSocket connections and HTTP requests can run concurrently.

<a id="push-messages-over-websocket"></a>

### Push messages over WebSocket

Push messages arrive as `dots.push` events, separate from the `response.*` events used for progress and final replies. You can receive them while waiting for a reply.
**Identify them by `type: "dots.push"` and `message.kind: "push"`; `message.request_id` is `null`.**
`message.id` identifies the received message itself and stays the same on redelivery.

After opening the WebSocket, subscribe with the following JSON. You can receive push messages without sending `response.create`.

```json
{"type":"dots.subscribe","consumer_id":"personal-agent"}
```

A successful subscription returns `dots.subscribed`, followed by push messages as they arrive.

```json
{
  "type": "dots.push",
  "consumer_id": "personal-agent",
  "message": {
    "id": "received-message-id",
    "sequence": 123,
    "kind": "push",
    "request_id": null,
    "channel": "default",
    "text": "I found new information about your earlier request.",
    "metadata": {},
    "received_at": "2026-10-22T03:00:00Z"
  }
}
```

Send an ACK after the required processing succeeds, such as adding the message to your personal agent's shared context.

```json
{"type":"dots.push.ack","message_id":"received-message-id"}
```

After saving the processing position in the bridge, the connector returns `dots.push.acked` and moves on to the next push message.
Only one push message is left unacknowledged at a time. If the ACK fails, the connector returns `error`; you can retry the ACK with the same `message_id`.
Resending the most recently successful ACK on the same connection is treated as confirmation that it was already processed.

| Event | Direction | Contents |
| --- | --- | --- |
| `dots.subscribe` | Client → connector | Subscribe to push messages with a `consumer_id` |
| `dots.subscribed` | Connector → client | Subscribed consumer, channel, and current cursor |
| `dots.push` | Connector → client | Push message; `message` uses the existing `ReceivedMessage` format |
| `dots.push.ack` | Client → connector | Specify the processed push message's `message_id` |
| `dots.push.acked` | Connector → client | Confirm the saved `message_id` and `sequence` |

The `dots.*` events are **dots-bridge extensions**. They are only sent to subscribed connections, so existing Responses clients continue to work.
For push messages, we recommend reading JSON from a raw WebSocket, as in the example below.
With the official SDK, read JSON through `recv_bytes()` and handle custom events yourself. Convert only standard events such as `response.*` to standard event types, and use a single receive loop per socket.

```sh
python examples/websocket_push.py --consumer-id personal-agent
# Receive replies and push messages on the same socket
python examples/websocket_push.py --consumer-id personal-agent --message "Hello"
```

The example acknowledges messages after displaying them and keeps the connection open. In an application, acknowledge only after the message has been durably incorporated.
After a disconnect, reconnect and subscribe with the same `consumer_id` to receive unacknowledged messages again. A message may be redelivered if processing succeeded but the connection closed before the ACK arrived. Use `message.id` to deduplicate external side effects.

Keep `consumer_id` fixed for the receiving personal agent, rather than creating one per conversation channel such as LINE or phone.
A different ID receives stored push messages independently from the beginning. IDs must be 1–118 characters using letters, digits, or `_ . : -`; the bridge's internal consumer name is `openai-ws:<consumer_id>`.
The channel is fixed by the connector's `--channel` startup option. Do not change the channel for an existing consumer.
Within one connector process, concurrent subscriptions for the same consumer are rejected with 409. Do not use the same consumer concurrently across separate connector processes either.
This push-only consumer skips and acknowledges request-bound `progress`, `reply`, and `cancel` messages.
It does not affect other consumers' processing positions.

<a id="http-api"></a>

## 🌐 HTTP API

`/api/*` requires `Authorization: Bearer <api_key>`. Only JSON is accepted.

| Method and path | Description |
| --- | --- |
| `GET /health` | Process health check; no authentication required |
| `GET /api/subscriptions` | Active subscription IDs, channels, and expiration times; excludes callbacks and secrets |
| `POST /api/messages` | Save a request and return 202 |
| `GET /api/messages/{request_id}` | Get delivery status and the stored final reply |
| `GET /api/messages/{request_id}/responses?after=0&limit=100` | Get the request's progress, final reply, and cancellation in stored order |
| `DELETE /api/subscriptions/{subscription_id}` | Unsubscribe and cancel undelivered requests |
| `GET /api/received?after=0&limit=100&channel=default` | List received progress, final replies, cancellations, and push messages (`channel` is optional) |
| `GET /api/consumers/{consumer_id}?channel=default` | Get or initialize a consumer's processing position |
| `POST /api/consumers/{consumer_id}/ack` | Advance the processing position with `{"sequence": 123, "channel": "default"}` |

Received-message and per-request response lists return `{"messages": [...]}`; consumer operations return `{"cursor": 123}`.
`after` selects messages after the given sequence; `limit` accepts 1–100.
Per-request response lists return `progress`, `reply`, and `cancel`, or 404 if the request does not exist.
Normally, the client's background receiver lists and acknowledges messages.
For manual handling, synchronous and asynchronous clients provide `list_received()`, `get_consumer_cursor()`, and `acknowledge_received()`.
Listing received messages alone does not advance the processing position.
Both clients also provide `list_response_messages()` for per-request responses. The asynchronous client provides `iter_responses()` to receive responses as they arrive until completion.

Send JSON in the following format. All fields except `text` are optional.

```json
{
  "text": "Hello",
  "channel": "default",
  "subscription_id": "sub_...",
  "metadata": {"source": "my-app"},
  "idempotency_key": "my-request-001"
}
```

The response includes fields such as `request_id`, `event_id`, `delivery_status`, `reply_status`, and `reply`.

| Field | Values and meaning |
| --- | --- |
| `delivery_status` | `queued` / `delivering` / `accepted` / `failed` / `cancelled` |
| `reply_status` | `pending` / `replied` / `expired` / `cancelled` |
| `reply` | Stored final reply, or `null` while awaiting a reply |
| `superseded_by` / `cancelled_at` | Replacement request ID and cancellation time; returned only for cancellation by supersession |
| `attempts` | Number of Webhook delivery attempts |
| `status_code` | Last Webhook HTTP status received |
| `error` | Error code without sensitive information; `null` on success |

**202 means the request was saved; `accepted` means the dots Webhook accepted the HTTP delivery.**
A completed answer is indicated by a reply saved through `reply_to_app`, which sets `reply_status` to `replied`.
Calling `report_progress` leaves `reply_status` as `pending`.

### Response idempotency and cancellation rules

A progress update's `idempotency_key` must be 1–128 characters.
Retrying the same request, key, and text returns the same received message; changing the text returns 409.
New progress is rejected after a final answer, but retrying an already stored progress update with identical text succeeds.

`cancel_request` must target an older, unfinished request. Its replacement must be a real, newer request on the same channel and subscription.
Retrying the same pair returns the same notification; changing the replacement is rejected.
After cancellation, the older request cannot receive new progress or a final answer.
The MCP instructions and tool descriptions also specify that normal completion or a conclusion that no action is needed must use `reply_to_app`, and independent requests must not be cancelled.

<a id="asgi-integration"></a>

## 🧩 ASGI integration

FastAPI is not a required dependency. You can run or embed the bridge as a Starlette ASGI app.

```python
from dots_bridge import Settings, create_app

app = create_app(Settings.from_file("bridge.toml"))
```

```sh
uvicorn main:app --host 127.0.0.1 --port 8765 --workers 1 --no-access-log
```

The delivery worker starts and stops with the app's lifespan.
If you mount the bridge inside another ASGI app, invoke its lifespan too.
The OAuth well-known and operational endpoints must be accessible from the public origin's root.
We recommend starting with a standalone server.

To receive messages within the server process, you can also register `@bridge.on_received` on `DotsBridge`.
With `create_app(settings, bridge=bridge)`, receiving starts and stops with the app's lifespan; the default consumer is `in-process`.
See [examples/embedded.py](../examples/embedded.py) for an implementation.

<a id="storage-delivery-and-operations"></a>

## 💾 Storage, delivery, and operations

- **Designed for a single owner.** The bridge does not provide multitenancy or identity management for a public service.
- Configuration uses TOML. SQLite stores subscriptions, requests, delivery status, progress, final replies, push messages, consumer positions, and OAuth state.
  OAuth access tokens last one hour; refresh tokens last 30 days. Connections survive server restarts.
- Existing dots-bridge schemas 1–4 automatically migrate to schema 5 in a single transaction on the next bridge startup.
  Stored received-message IDs, sequences, and consumer positions are preserved. The connector does not access the database directly.
- Webhook callback URLs and secrets are encrypted with Fernet. The whole database, including message text, replies, and metadata, is not encrypted.
  Store the database, configuration, and backups where only the owner can read them.
- Callbacks are verified with a signed challenge, and deliveries are signed using Standard Webhooks.
  Only HTTPS and public IP addresses are allowed, and connections are pinned to a DNS-validated IP. Redirects and environment proxies are not used.
- Network failures and HTTP 408, 425, 429, and 5xx responses are retried a limited number of times. Permanent errors such as 410 and 413 are not retried.
  A 410 response removes the subscription. Retries keep the same event ID and body, with a new signature for each attempt.
- Undelivered requests resume on startup. If the server stops during delivery, the request resumes after the 30-second lease expires.
  Delivery can be duplicated if the connection closes after acceptance. Make side effects on the dots side idempotent too.
- Each request has one final reply. Repeating the same reply text succeeds; replying again with different text is rejected.
  Multiple progress updates can be saved through `report_progress` and delivered in order through SSE, WebSocket, or the application's receiver.
  Streaming individual dots generation tokens and automatically retrieving conversation logs are unsupported.
- Requests are sent to an active subscription. Sending without a subscription returns 409.
  Unsubscribing, expiration, or explicit OAuth revocation stops undelivered requests. HTTP requests already in progress cannot be recalled.
- MCP cursor replay of event history is unsupported; the bridge returns `cursor: null`.
  Internal outbox retries and MCP history replay are separate features. Even `ttlMs: null` produces a finite subscription expiration time.
- The package stores the text passed to `report_progress`, `reply_to_app`, and `send_to_app`.
  It cannot guarantee that dots calls these tools or processes a request immediately.
  State the expected reply or notification method in your request to dots.
- `prune` does not delete received messages or consumer positions. Unprocessed progress and final replies remain available from the received-message list even after their requests are deleted.
  Monitor database size during long-running deployments.

The 30-second lease for interrupted deliveries does not make normal new requests wait 30 seconds.
dots processes requests asynchronously after Webhook acceptance. See [OpenAI's delivery specification](https://developers.openai.com/plugins/build/mcp-events#handle-delivery-responses).

Stopping the receiver while a synchronous handler is running does not interrupt that function.
The synchronous client's reply wait is also subject to HTTPX timeouts for individual network operations.
Use the asynchronous client if you need a strict wait deadline that includes time spent in network operations.

<a id="implementation-and-references"></a>

## 📚 Implementation and references

FastMCP 4.1 or later and the MCP Python SDK handle MCP 2.0 (`2026-07-28`),
HTTP transport, tool schemas and calls, and standard OAuth endpoints.
The bridge adds the three draft Events methods through FastMCP's `ServerExtension`.
Small middleware components in `mcp.py` handle the current SDK dropping `capabilities.events`
and compatibility with OpenAI's tool authentication metadata.

- [OpenAI MCP Events](https://developers.openai.com/plugins/build/mcp-events)
- [OpenAI Authentication](https://developers.openai.com/plugins/build/auth)
- [FastMCP MCP 2.0 support](https://github.com/PrefectHQ/fastmcp/blob/main/dev-docs/v4-notes/protocol-2026.md)
- [FastMCP Extensions](https://github.com/PrefectHQ/fastmcp/blob/main/fastmcp_slim/fastmcp/server/extensions.py)
- [MCP SDK Events capability issue](https://github.com/modelcontextprotocol/python-sdk/issues/3640)
