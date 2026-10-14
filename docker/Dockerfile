FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
COPY agent-gateway-go/ ./agent-gateway-go/
COPY asr-gateway-go/ ./asr-gateway-go/
COPY device-gateway-go/ ./device-gateway-go/
COPY cmd/ ./cmd/
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags='-s -w' -o /out/gateway ./cmd/gateway

FROM alpine:3.24
RUN adduser -D -H -u 10001 appuser
USER appuser
EXPOSE 8787 8788 8789
COPY --from=builder /out/gateway /usr/local/bin/gateway
ENTRYPOINT ["gateway"]
