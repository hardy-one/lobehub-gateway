FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
COPY agent-gateway-go/ ./agent-gateway-go/
COPY asr-gateway-go/ ./asr-gateway-go/
COPY device-gateway-go/ ./device-gateway-go/
COPY message-gateway-go/ ./message-gateway-go/
COPY oss-center-go/ ./oss-center-go/
COPY cmd/ ./cmd/
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags='-s -w' -o /out/gateway ./cmd/gateway

FROM alpine:3.24
# The agent gateway persists operation state to GATEWAY_STATE_DB (default
# /data/agent-gateway.db). The binary runs as an unprivileged user, so the
# directory must exist with that ownership before USER switches: otherwise a
# mounted volume lands as root-owned and the store silently degrades to memory,
# which would reintroduce the restart data loss this persistence fixes.
RUN adduser -D -H -u 10001 appuser \
    && mkdir -p /data \
    && chown appuser:appuser /data
USER appuser
EXPOSE 8787 8788 8789 8790 8791
COPY --from=builder /out/gateway /usr/local/bin/gateway
ENTRYPOINT ["gateway"]
