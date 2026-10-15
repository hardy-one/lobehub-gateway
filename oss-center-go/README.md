# oss-center

`oss-center-go` provisions the deployment-level SystemBot providers behind LobeHub's Messenger, from this deployment's own environment. It replaces the slice of LobeHub's closed-source admin console that a self-hosted deployment actually needs: platform credentials, the enable switch, and — in a later step — the platform-level gateway connection.

It has no UI and no database access.

```text
oss-center  ──(HTTP + SERVICE_TOKEN)──▶  LobeHub  ──▶  system_bot_providers
              one POST per declared platform        (LobeHub encrypts)
```

## Why it talks to LobeHub instead of the database

Writing `system_bot_providers` directly would mean re-implementing LobeHub's credential encryption (`AES-256-GCM`, stored as `iv:authTag:ciphertext`) in Go, and getting it wrong is silent: LobeHub swallows a decryption failure to `credentials = {}`, so the platform card still appears while the platform simply never works. Going through LobeHub means the format stays owned by the process that reads it, and a rotation can invalidate LobeHub's 30-second config cache immediately instead of waiting for it to lapse.

The price is one small patch on the LobeHub side (`POST`/`GET /api/agent/system-bots`, see below).

## Configuration

Declaration is by presence: a platform is declared as soon as its credential group is set, and `<PLATFORM>_ENABLED=0` declares it as switched off rather than removing it.

| Variable | Default | Description |
| --- | --- | --- |
| `OSS_CENTER_ENABLED` | — | Opt-in switch. Without it the component never starts and nothing below is read. |
| `OSS_CENTER_PORT` | `8791` | Listen port for the read-only status surface. |
| `OSS_CENTER_INTERVAL` | `60s` | How often to converge. |
| `OSS_CENTER_ONESHOT` | — | Run a single pass at startup instead of a loop. |
| `LOBE_API_BASE_URL` | — | LobeHub base URL (the same variable the other gateways use). |
| `SERVICE_TOKEN` | — | Authenticates against LobeHub and the message gateway alike; must equal LobeHub's `MESSAGE_GATEWAY_SERVICE_TOKEN`. |
| `MESSAGE_GATEWAY_URL` | — | Base URL of the message gateway, used for platform-level connection registration. |

### Platform declarations

| Platform | Variables |
| --- | --- |
| Discord | `MESSENGER_DISCORD_APPLICATION_ID`, `MESSENGER_DISCORD_BOT_TOKEN`, `MESSENGER_DISCORD_PUBLIC_KEY`, `MESSENGER_DISCORD_CLIENT_SECRET` (only for the per-guild OAuth install flow) |
| Telegram | `MESSENGER_TELEGRAM_BOT_TOKEN`, `MESSENGER_TELEGRAM_BOT_USERNAME`, `MESSENGER_TELEGRAM_WEBHOOK_SECRET` |
| Slack | `MESSENGER_SLACK_APP_ID`, `MESSENGER_SLACK_CLIENT_ID`, `MESSENGER_SLACK_CLIENT_SECRET`, `MESSENGER_SLACK_SIGNING_SECRET` |
| WeChat | `MESSENGER_WECHAT_ENABLED` — the row is an availability switch only; the iLink bot token is issued per user by QR scan and never belongs to the deployment |
| Linq | `MESSENGER_LINQ_API_KEY`, `MESSENGER_LINQ_WEBHOOK_SECRET`, `MESSENGER_LINQ_NUMBERS` (comma or space separated pool) |

An incomplete group is a startup error naming the missing variable — never a half-provisioned platform.

`settings` and `connectionMode` are deliberately not configurable here: nothing in LobeHub's runtime reads them, so exposing them would only fake a knob that does nothing.

## What it does not do

- **No database, no encryption.** LobeHub stores and encrypts on its behalf.
- **No deletions.** A platform LobeHub stores that this deployment no longer declares is reported in the logs and in `/status`, never removed.
- **No per-user state.** Account links, the active agent, WeChat QR sessions and Slack/Discord installs are the user's own business in `Settings → Messenger`.
- **No behaviour settings.** Group `@mention` handling, access policies and reply formatting are hard-coded in LobeHub; see the module's upstream notes.

## Status

`GET /healthz` answers `200` with `{"status":"ok"}` after a clean pass, `503` after a failed one, and `200` with `{"status":"pending"}` before the first pass finishes. `GET /status` returns the declared platforms and the last pass in full, including which platforms failed and which are stored but undeclared.

## The LobeHub-side patch

Two files, both additive:

- `apps/server/src/router-hono/agent/handlers/systemBotProvider.ts` — `POST` upserts one platform through `SystemBotProviderModel.upsertByPlatform` (which encrypts) and invalidates the config cache; `GET` lists platforms without credentials. Both authenticate with `MESSAGE_GATEWAY_SERVICE_TOKEN`, inline, exactly like `/gateway/callback`.
- `apps/server/src/router-hono/agent/index.ts` — two route registrations.

Plus `SystemBotProviderModel.listSummaries`, a credential-free projection used by the `GET`.

## Run

```bash
go build ./...
go test ./...
```

In the unified binary it starts only when `OSS_CENTER_ENABLED` is set; `docker/docker-compose.yml` carries the full variable list.
