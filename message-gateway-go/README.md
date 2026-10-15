# Message Gateway Go

`message-gateway-go` is the Go implementation of the LobeHub message gateway: the service that holds a deployment's IM platform connections on behalf of a LobeHub server that cannot hold them itself. It is aimed at self-hosted, single-instance deployments, and it is deliberately protocol-compatible with the client LobeHub already ships, so no LobeHub-side change is needed to use it.

## What it does

LobeHub's server is often stateless — serverless functions or short-lived containers cannot keep a Discord WebSocket or a WeChat long poll alive. This gateway takes those connections:

1. LobeHub posts a connection's payload to this gateway, or the gateway pulls the set it should hold.
2. The gateway holds the platform link: a Discord Gateway WebSocket, a WeChat iLink long poll, or (for webhook platforms) nothing but an outbound typing surface.
3. Inbound platform events are forwarded to LobeHub's webhook endpoint, and connection state changes are reported back so the channel UI reflects reality.

Outbound messages are deliberately **not** routed through the gateway: LobeHub calls the platform REST APIs directly. The gateway owns connections, not conversations.

## Supported platforms

| Platform | Transport | What the gateway holds |
| --- | --- | --- |
| `discord` | `websocket` | A Discord Gateway v10 WebSocket session: identify, heartbeat, resume, and each dispatch event forwarded as `GATEWAY_<EVENT>` with the bot token in `x-discord-gateway-token`. |
| `wechat` | `polling` | An iLink Bot API long poll: each finished user message forwarded raw, plus the per-user typing indicator. |
| `slack` | `webhook` | Nothing inbound — LobeHub registers the platform webhook itself. The gateway drives `assistant.threads.setStatus` as the typing indicator. Slack's Socket Mode (`websocket`) is **not** implemented: a connection that asks for it is refused with `unsupported connection mode` rather than reported online, because nothing would reach LobeHub. Set the channel's `connectionMode` to `webhook`, or run that connection on a gateway that implements Socket Mode. |
| `telegram` | `webhook` | Nothing inbound. The gateway drives `sendChatAction` (`typing` / `cancel`). |

`GET /api/admin/capabilities` reports this list, and LobeHub only routes platforms a gateway declares. Capabilities are per platform, not per transport, so a platform's transport support is enforced when the connection is created.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `SERVICE_TOKEN` | — | Required. Authenticates every LobeHub call into this gateway, the state callbacks the gateway sends back, and forwarded WeChat events. The same value the other LobeHub gateways use. |
| `LOBE_API_BASE_URL` | `https://app.lobehub.com` | LobeHub base URL for state callbacks, the connection pull, and inbound event forwarding. |
| `PULL_TOKEN` | — | Credential for `POST /api/agent/gateway/desired-connections` (LobeHub's `MESSAGE_GATEWAY_NODE_PULL_TOKEN`). Without it the gateway can only be pushed to. |
| `PULL_INTERVAL` | `5m` | How often the connection set is re-pulled. Set to `0` to pull only at startup. |
| `MESSAGE_PORT` | `8790` | Listen port (unified binary). A standalone binary uses `PORT`. |
| `TYPING_INTERVAL` | `8s` | How often the typing indicator is re-issued; platform indicators expire after 5–10 seconds. |
| `DISCORD_GATEWAY_URL` | `wss://gateway.discord.gg` | Discord gateway endpoint. |
| `DISCORD_API_BASE_URL` | `https://discord.com/api/v10` | Discord REST endpoint (typing). |
| `WECHAT_BASE_URL` | `https://ilinkai.weixin.qq.com` | iLink Bot API base URL. A connection's own `baseUrl` credential wins over this. |
| `SLACK_API_BASE_URL` | `https://slack.com/api` | Slack Web API endpoint. |
| `TELEGRAM_API_BASE_URL` | `https://api.telegram.org` | Telegram Bot API endpoint. |
| `READ_TIMEOUT` / `WRITE_TIMEOUT` / `SHUTDOWN_TIMEOUT` | `30s` / `1m` / `10s` | HTTP server timeouts. |
| `REQUEST_LOG_ENABLED` | `false` | Emit one structured log line per request. |

## API

Every route below requires `Authorization: Bearer <SERVICE_TOKEN>`; `GET /health` does not.

### LobeHub calls in

| Method | Route | Purpose |
| --- | --- | --- |
| `POST` | `/api/connections` | Create or rebuild a connection. Body `{config, ensure?}`; `ensure` marks a reconcile-driven call, so an identical live connection is left running instead of being rebuilt. Answers `{status}`. |
| `DELETE` | `/api/connections` | Tear every connection down; answers `{total}`. |
| `DELETE` | `/api/connections/{id}` | Tear one connection down. |
| `POST` | `/api/connections/{id}/typing` | Start the typing indicator for `{platformThreadId}`. |
| `DELETE` | `/api/connections/{id}/typing` | Stop it. |
| `GET` | `/api/connections/{id}/status` | Answers `{config, state}`; `404` for an id this gateway does not hold. |
| `GET` | `/api/admin/stats` | `{total, byPlatform, connections[]}`. |
| `GET` | `/api/admin/registered-ids` | `{ids}`: every held id, including connections in an error state. |
| `GET` | `/api/admin/capabilities` | `{platforms}`. |

### The gateway calls back into LobeHub

| Route | Credential | Purpose |
| --- | --- | --- |
| `POST {LOBE_API_BASE_URL}/api/agent/gateway/callback` | `SERVICE_TOKEN` | Connection state changes (`connected`, `disconnected`, `dormant`, `error`). Skipped when the connection carries no `applicationId`, which LobeHub would ignore anyway. |
| `POST {LOBE_API_BASE_URL}/api/agent/gateway/desired-connections` | `PULL_TOKEN` | The connection set this gateway should hold — used at startup and on `PULL_INTERVAL`. |
| `POST {LOBE_API_BASE_URL}{webhookPath}` | the connection's `webhookToken`, or `SERVICE_TOKEN` | One inbound platform event, forwarded byte for byte. |

## Behaviour worth knowing

- **Start means online.** A connection's transport is proven live before the gateway answers `connected`: Discord waits for `READY`, WeChat runs a short readiness poll. Anything later — a dropped socket, an expired WeChat session — arrives through the state callback.
- **Disconnecting is silent.** Whoever asked for a disconnect (a credential save, an account unlink, a reconcile sweep) also sets the resulting runtime status; a callback racing it would overwrite a status the user just caused.
- **Errors stay registered.** A connection whose start failed remains in the registry and reports `error`, so LobeHub's reconcile still sees it and re-arms it once new credentials are saved.
- **Typing is best-effort and repeated.** Platform indicators expire after 5–10 seconds while a generation can run for minutes, so the gateway re-issues them on `TYPING_INTERVAL` until LobeHub stops them.
- **State is in memory.** Connections are lost when the process exits and rebuilt from the pull on the next start. Nothing is written to disk.

## Not implemented on purpose

The upstream message gateway runs on Cloudflare Durable Objects. Their semantics are not part of this implementation:

- No hibernation or wake-on-message; a held connection is a live goroutine.
- No park/backoff registry: a failed connection is reported and left registered for the next reconcile instead of being parked with a `409`.
- No stats cache window: `registered-ids` and `stats` both read the live registry, so a dormant connection stays visible.
- No multi-instance coordination. Run one active message gateway per LobeHub deployment.

## Development

```bash
go vet ./...
go test ./...
```

`internal/gateway` owns the contract — routes, auth, the connection registry, the state machine, and the LobeHub client. `internal/provider` owns the platform runtimes; adding a platform means one package that calls `provider.Register` and nothing else.
