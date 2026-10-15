# LobeHub Gateway Go

[简体中文](./README.zh-CN.md)

`lobehub-gateway-go` is a Go reimplementation of the LobeHub gateway services. It is intended for self-hosted deployments that need a simple, single-instance gateway without Cloudflare platform bindings or external infrastructure dependencies.

The original gateway implementations remain the protocol reference. This repository focuses on keeping the public HTTP and WebSocket behavior compatible while making the runtime easier to run as a normal Go service.

## Goals

- Go implementation of LobeHub gateway protocols.
- Single-instance design for straightforward self-hosting.
- No Cloudflare Workers or Durable Objects dependency.
- No Redis, PostgreSQL, NATS, or other external runtime services.
- Keep protocol behavior compatible with the reference implementations where practical.

## Alignment Target

The protocol baseline for this repository is the pair of standalone reference implementations:

- `device-gateway` at `d9df5542f06df6f5555356efa0735f767f0e5ccf`
- `agent-gateway` at `aeaf0abc63134974f803eebf4c1851c5e8f7bf37`

For implemented gateway services, the target is compatibility with the public HTTP routes, WebSocket messages, request and response shapes, authentication behavior, routing semantics, and timeout behavior exposed by those reference implementations. This target intentionally keeps the Go runtime self-hosted and single-instance oriented.

The alignment target does not include deliberately excluded platform surfaces: admin APIs, metrics, Durable Object storage or WebSocket hibernation, multi-instance coordination, geo reporting, and admin reporting.

The message gateway has no standalone reference implementation to align against: its baseline is the client contract LobeHub itself speaks (`packages/env` gateway configuration plus the `MessageGatewayClient` in the server), which fixes the routes, payloads, credentials, and state vocabulary. Durable Object semantics that contract only implies — hibernation, park/backoff, and the cached-stats window — are deliberately not reproduced: an errored connection stays registered and is re-armed by the next reconcile instead of being parked.

## Projects

| Project | Status | Description |
| --- | --- | --- |
| [`device-gateway-go`](./device-gateway-go/README.md) | Implemented | Device gateway for routing LobeHub device connections, status queries, tool calls, system information, and agent-run requests. |
| [`agent-gateway-go`](./agent-gateway-go/README.md) | Implemented | Agent gateway for relaying browser WebSocket sessions and server-pushed agent events. |
| [`asr-gateway-go`](./asr-gateway-go/README.md) | Implemented | ASR dictation gateway for bridging LobeHub's realtime speech PCM stream to a streaming speech recognition backend (Volcengine). |
| [`message-gateway-go`](./message-gateway-go/README.md) | Implemented | Message gateway for holding IM platform connections on LobeHub's behalf (Discord WebSocket, WeChat iLink long polling) and forwarding platform events back to LobeHub. |
| [`oss-center-go`](./oss-center-go/README.md) | Implemented | Opt-in provisioner for the deployment's SystemBot (Messenger) providers: declares each platform from its own environment and converges `system_bot_providers` through LobeHub's management endpoint. |

## Unified Binary

The root module (`go.mod`) provides a unified entry point that starts every gateway service in a single process:

```text
lobehub-gateway-go/
  go.mod                          # umbrella module with replace directives
  cmd/gateway/main.go             # unified entry: runs agent + device + asr + message (+ oss-center when enabled)
  agent-gateway-go/
    gateway.go                    # facade re-exporting internal/gateway symbols
    go.mod                        # standalone module (unchanged)
    internal/gateway/
  device-gateway-go/
    gateway.go                    # facade re-exporting internal/gateway symbols
    go.mod                        # standalone module (unchanged)
    internal/gateway/
  asr-gateway-go/
    gateway.go                    # facade re-exporting internal/gateway symbols
    internal/gateway/
    internal/provider/            # ASR provider implementations (volcengine)
  message-gateway-go/
    gateway.go                    # facade re-exporting internal/gateway symbols
    internal/gateway/             # contract routes, connection registry, state machine, LobeHub client
    internal/provider/            # platform runtimes (discord / wechat / slack / telegram)
  oss-center-go/
    oss_center.go                 # facade re-exporting internal symbols
    internal/config/              # environment → platform declarations
    internal/lobehub/             # LobeHub management-endpoint client
    internal/reconcile/           # declarative convergence loop
    internal/server/              # read-only /healthz + /status
```

Each sub-project remains an independent Go module and can be built/tested on its own. The root module uses `replace` directives to import the sub-modules via facade files (`gateway.go`) that re-export the internal package symbols.

### Build & Run

```bash
# Build the unified binary
 go build -o gateway ./cmd/gateway

# Run every service
 SERVICE_TOKEN=your-token ./gateway

# Defaults: agent on :8787, device on :8788, asr on :8789, message on :8790, oss-center on :8791 (opt-in)
# Override ports via env vars
 AGENT_PORT=9000 DEVICE_PORT=9001 MESSAGE_PORT=9002 SERVICE_TOKEN=your-token ./gateway
```

### Environment Variables

| Variable | Default | Shared | Description |
| --- | --- | --- | --- |
| `SERVICE_TOKEN` | — | Yes | Service auth token (required) |
| `JWKS_PUBLIC_KEY` | — | Yes | JWKS public key for JWT verification |
| `LOBE_API_BASE_URL` | `https://app.lobehub.com` | Agent, Message | LobeHub API base URL |
| `AGENT_PORT` | `8787` | Agent only | Agent gateway listen port |
| `DEVICE_PORT` | `8788` | Device only | Device gateway listen port |
| `ASR_PORT` | `8789` | ASR only | ASR dictation gateway listen port |
| `ASR_PROVIDER` | `volcengine` | ASR only | Streaming ASR provider (`volcengine` today) |
| `VOLCENGINE_VOICE_API_KEY` | — | ASR only | Volcengine speech recognition API key |
| `VOLCENGINE_RESOURCE_ID` | `volc.seedasr.sauc.duration` | ASR only | Volcengine model resource id (Doubao streaming 2.0 by default) |
| `MESSAGE_PORT` | `8790` | Message only | Message gateway listen port |
| `OSS_CENTER_ENABLED` | — | oss-center | Opt-in: run the SystemBot provisioner (see [`oss-center-go`](./oss-center-go/README.md) for the `MESSENGER_*` declarations) |
| `OSS_CENTER_PORT` | `8791` | oss-center | oss-center status listener |
| `OSS_CENTER_INTERVAL` | `60s` | oss-center | How often to converge LobeHub's stored providers |
| `OSS_CENTER_ONESHOT` | — | oss-center | Run a single pass at startup instead of a loop |
| `MESSAGE_GATEWAY_URL` | — | oss-center | Message gateway base URL, used to register platform-level connections |
| `PULL_TOKEN` | — | Message only | Credential for pulling the connection list (LobeHub's `MESSAGE_GATEWAY_NODE_PULL_TOKEN`); without it the gateway can only be pushed to and cannot rebuild itself after a restart |
| `PULL_INTERVAL` | `5m` | Message only | How often to re-pull the desired connection set |
| `TYPING_INTERVAL` | `8s` | Message only | How often to re-issue the platform typing indicator |
| `READ_TIMEOUT` | `30s` | Yes | HTTP read timeout |
| `WRITE_TIMEOUT` | `1m` | Yes | HTTP write timeout |
| `REQUEST_LOG_ENABLED` | `false` | Yes | Enable per-request structured HTTP access logging |
| `SHUTDOWN_TIMEOUT` | `10s` | Yes | Graceful shutdown timeout |

The unified binary does not read `PORT`; each service's port is controlled only by `AGENT_PORT` / `DEVICE_PORT` / `ASR_PORT` / `MESSAGE_PORT`. The standalone binaries keep using `PORT` (default 8787) as before.

### Release Artifacts

Linux releases provide the unified binary and both standalone binaries for amd64 and arm64:

```text
gateway-linux-amd64
agent-gateway-go-linux-amd64
device-gateway-go-linux-amd64
```

Replace `amd64` with `arm64` for ARM systems. Standalone release asset names include the `-go` component.

### Docker

Build and run the unified image locally:

```bash
docker build -f docker/Dockerfile -t lobehub-gateway-go .
docker run --rm \
  -p 8787:8787 \
  -p 8788:8788 \
  -p 8789:8789 \
  -p 8790:8790 \
  -e SERVICE_TOKEN=your-token \
  lobehub-gateway-go
```

Or start the provided Compose service:

```bash
SERVICE_TOKEN=your-token docker compose -f docker/docker-compose.yml up -d --build
```

The Compose file uses the unified binary's default listener ports and forwards `SERVICE_TOKEN`, `JWKS_PUBLIC_KEY`, `LOBE_API_BASE_URL`, `PULL_TOKEN`, and `REQUEST_LOG_ENABLED`. The message gateway can only restore its connections by itself when `PULL_TOKEN` is set; without it LobeHub still pushes connections, but a restarted gateway waits for the next push. To use custom listener ports or timeout values, add the corresponding variables from the table above to the Compose service's `environment` section and update its `ports` mappings when changing `AGENT_PORT`, `DEVICE_PORT`, `ASR_PORT`, or `MESSAGE_PORT`.

The CD workflow publishes a multi-architecture image for linux/amd64 and linux/arm64. By default it is published as `ghcr.io/lobehub/lobehub-gateway:<version>`; repository maintainers can override the destination with the `DOCKER_IMAGE` Actions secret. Stable releases selected as the default version also publish the `latest` tag.

## Architecture

This repository is organized as a collection of gateway services. Each gateway lives in its own directory so more gateway implementations can be added without coupling them to the existing services. The root module provides a unified binary that runs all gateways together.

The current implementation style is intentionally minimal: state is held in memory, services run as ordinary HTTP/WebSocket servers, and deployment can be handled by systemd, Docker, Kubernetes, or any other process manager.

## Relationship To LobeHub Gateway

Compared with the original LobeHub gateway projects, this repository has several deliberate differences:

| Area | Original gateway | This repository |
| --- | --- | --- |
| Language | TypeScript | Go |
| Runtime | Cloudflare Workers | Standard Go process |
| Coordination | Durable Objects | In-memory single instance |
| Platform binding | Cloudflare-specific | Platform-neutral |
| Runtime dependencies | Cloudflare services | No external services required |

Because this repository targets a single gateway instance, it does not provide distributed session coordination by default. If you need horizontal scaling, run one active gateway instance per routing domain or add a shared coordination layer explicitly.

## Development

### Unified binary

```bash
# Build
 go build -o gateway ./cmd/gateway

# Test root module (unified binary)
 go test ./...
```

### Individual gateways

Each gateway remains a standalone module and can be built/tested independently:

```bash
cd agent-gateway-go && go test ./...
cd device-gateway-go && go test ./...
cd message-gateway-go && go test ./...
cd oss-center-go && go test ./...
```

See each gateway README for service-specific configuration, API routes, and local run instructions.
