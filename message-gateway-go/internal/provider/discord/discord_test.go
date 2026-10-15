package discord

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/lobehub/lobehub/apps/message-gateway-go/internal/provider"
)

// fakeGateway speaks the server half of the WebSocket handshake plus the part
// of the Discord gateway protocol this provider depends on.
type fakeGateway struct {
	conn   net.Conn
	reader *bufio.Reader
}

func (g *fakeGateway) send(t *testing.T, payload any) {
	t.Helper()

	body, err := json.Marshal(payload)
	if err != nil {
		t.Errorf("encoding a gateway frame failed: %v", err)
		return
	}

	frame := []byte{0x81}
	if len(body) < 126 {
		frame = append(frame, byte(len(body)))
	} else {
		frame = append(frame, 126, byte(len(body)>>8), byte(len(body)))
	}
	frame = append(frame, body...)

	_ = g.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := g.conn.Write(frame); err != nil {
		t.Errorf("writing a gateway frame failed: %v", err)
	}
}

func (g *fakeGateway) sendClose(code int) {
	payload := []byte{byte(code >> 8), byte(code)}
	frame := append([]byte{0x88, byte(len(payload))}, payload...)
	_ = g.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, _ = g.conn.Write(frame)
}

func (g *fakeGateway) read(t *testing.T) (gatewayPayload, bool) {
	t.Helper()

	var payload gatewayPayload
	_ = g.conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	header := make([]byte, 2)
	if _, err := io.ReadFull(g.reader, header); err != nil {
		return payload, false
	}

	length := int(header[1] & 0x7f)
	if length == 126 {
		buf := make([]byte, 2)
		if _, err := io.ReadFull(g.reader, buf); err != nil {
			return payload, false
		}
		length = int(binary.BigEndian.Uint16(buf))
	} else if length == 127 {
		buf := make([]byte, 8)
		if _, err := io.ReadFull(g.reader, buf); err != nil {
			return payload, false
		}
		length = int(binary.BigEndian.Uint64(buf))
	}

	var mask [4]byte
	if header[1]&0x80 != 0 {
		if _, err := io.ReadFull(g.reader, mask[:]); err != nil {
			return payload, false
		}
	}

	body := make([]byte, length)
	if _, err := io.ReadFull(g.reader, body); err != nil {
		return payload, false
	}
	for i := range body {
		body[i] ^= mask[i%4]
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		t.Errorf("client frame is not a gateway payload: %v", err)
		return payload, false
	}
	return payload, true
}

// awaitOp reads until the expected opcode arrives. Heartbeats and other
// in-flight frames are skipped, so the script never depends on their timing.
func (g *fakeGateway) awaitOp(t *testing.T, op int) (gatewayPayload, bool) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		payload, ok := g.read(t)
		if !ok {
			return gatewayPayload{}, false
		}
		if payload.Op == op {
			return payload, true
		}
	}
	// Reported rather than fatal: the script runs on the server's goroutine.
	t.Errorf("the client never sent op %d", op)
	return gatewayPayload{}, false
}

// startFakeGateway runs `script` against one client connection and keeps that
// connection open until the test finishes, so the provider stays connected for
// the assertions.
func startFakeGateway(t *testing.T, script func(g *fakeGateway)) *httptest.Server {
	t.Helper()

	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			return
		}

		sum := sha1.Sum([]byte(r.Header.Get("Sec-WebSocket-Key") + wsGUID))
		accept := base64.StdEncoding.EncodeToString(sum[:])
		_, _ = fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", accept)

		script(&fakeGateway{conn: conn, reader: bufio.NewReader(conn)})

		<-done
		_ = conn.Close()
	}))

	// Registered before the caller's provider cleanup, so LIFO ordering stops
	// the provider first and this handler can then return.
	t.Cleanup(func() {
		close(done)
		server.Close()
	})

	return server
}

func websocketURL(server *httptest.Server) string {
	return strings.Replace(server.URL, "http://", "ws://", 1)
}

func newTestProvider(t *testing.T, env provider.Env, forwarded chan<- provider.Inbound) provider.Provider {
	t.Helper()

	instance, err := newFactory(provider.Config{
		ApplicationID:  "app-1",
		ConnectionID:   "conn-discord",
		ConnectionMode: provider.ModeWebsocket,
		Credentials:    map[string]any{"botToken": "bot-token"},
		Platform:       "discord",
		WebhookPath:    "/api/agent/webhooks/discord/app-1",
	}, provider.Deps{
		Env: env,
		Forward: func(_ context.Context, event provider.Inbound) error {
			select {
			case forwarded <- event:
			default:
			}
			return nil
		},
		Log:    slog.New(slog.NewTextHandler(io.Discard, nil)),
		Report: func(provider.StateChange) {},
	})
	if err != nil {
		t.Fatalf("provider creation failed: %v", err)
	}
	return instance
}

func TestSessionIdentifiesAndForwardsDispatchEvents(t *testing.T) {
	forwarded := make(chan provider.Inbound, 4)
	identify := make(chan gatewayPayload, 1)

	server := startFakeGateway(t, func(g *fakeGateway) {
		g.send(t, map[string]any{
			"op": opHello,
			"d":  map[string]any{"heartbeat_interval": 45000},
		})
		payload, ok := g.awaitOp(t, opIdentify)
		if !ok {
			return
		}
		identify <- payload

		g.send(t, map[string]any{
			"op": opDispatch,
			"t":  "READY",
			"s":  1,
			"d":  map[string]any{"session_id": "sess-1"},
		})
		g.send(t, map[string]any{
			"op": opDispatch,
			"t":  "MESSAGE_CREATE",
			"s":  2,
			"d":  map[string]any{"channel_id": "C1", "content": "hi", "id": "m1"},
		})
	})

	instance := newTestProvider(t, provider.Env{DiscordGatewayURL: websocketURL(server)}, forwarded)
	if err := instance.Start(context.Background()); err != nil {
		t.Fatalf("Start returned %v", err)
	}
	t.Cleanup(func() { _ = instance.Stop(context.Background()) })

	select {
	case payload := <-identify:
		var data struct {
			Intents int    `json:"intents"`
			Token   string `json:"token"`
		}
		if err := json.Unmarshal(payload.D, &data); err != nil {
			t.Fatalf("identify payload could not be read: %v", err)
		}
		if data.Token != "bot-token" {
			t.Fatalf("identified with token %q, want the connection's bot token", data.Token)
		}
		if data.Intents != gatewayIntents {
			t.Fatalf("intents = %d, want %d", data.Intents, gatewayIntents)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the client never identified")
	}

	select {
	case event := <-forwarded:
		if event.Headers["x-discord-gateway-token"] != "bot-token" {
			t.Fatalf("forward headers = %v, want the gateway token header", event.Headers)
		}
		var envelope struct {
			Data map[string]any `json:"data"`
			Type string         `json:"type"`
		}
		if err := json.Unmarshal(event.Body, &envelope); err != nil {
			t.Fatalf("forwarded body is not the gateway envelope: %v", err)
		}
		if envelope.Type != "GATEWAY_MESSAGE_CREATE" {
			t.Fatalf("envelope type = %q, want GATEWAY_MESSAGE_CREATE", envelope.Type)
		}
		if envelope.Data["id"] != "m1" {
			t.Fatalf("envelope data = %v, want the dispatch body", envelope.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no dispatch event reached the webhook")
	}
}

func TestRejectedCredentialsFailTheConnection(t *testing.T) {
	server := startFakeGateway(t, func(g *fakeGateway) {
		g.send(t, map[string]any{
			"op": opHello,
			"d":  map[string]any{"heartbeat_interval": 45000},
		})
		if _, ok := g.awaitOp(t, opIdentify); !ok {
			return
		}
		g.sendClose(closeAuthFailed)
	})

	instance := newTestProvider(t, provider.Env{DiscordGatewayURL: websocketURL(server)}, make(chan provider.Inbound, 1))

	err := instance.Start(context.Background())
	if err == nil {
		t.Fatal("Start accepted a session Discord rejected")
	}
	if code := provider.Code(err); code != "4004" {
		t.Fatalf("error code = %q, want the close code 4004", code)
	}
}

func TestTypingPostsToTheChannelEndpoint(t *testing.T) {
	var (
		path          string
		authorization string
	)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		authorization = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(api.Close)

	instance := newTestProvider(t, provider.Env{DiscordAPIBaseURL: api.URL}, make(chan provider.Inbound, 1))

	if err := instance.SetTyping(context.Background(), "discord:guild-1:channel-1", true); err != nil {
		t.Fatalf("SetTyping returned %v", err)
	}
	if path != "/channels/channel-1/typing" {
		t.Fatalf("path = %q, want the channel typing endpoint", path)
	}
	if authorization != "Bot bot-token" {
		t.Fatalf("authorization = %q, want the bot credential", authorization)
	}

	// Discord expires the indicator on its own, so clearing is deliberately
	// not an API call.
	path = ""
	if err := instance.SetTyping(context.Background(), "discord:guild-1:channel-1", false); err != nil {
		t.Fatalf("clearing typing returned %v", err)
	}
	if path != "" {
		t.Fatal("clearing typing made a request; Discord has no clear call")
	}
}

func TestChannelParsingPrefersTheThread(t *testing.T) {
	cases := map[string]string{
		"discord:guild-1:channel-1":          "channel-1",
		"discord:guild-1:channel-1:thread-1": "thread-1",
		"discord::channel-1":                 "channel-1",
		"discord:guild-1:channel-1:":         "channel-1",
		"telegram:guild-1:channel-1":         "",
		"discord:guild-1":                    "",
	}

	for threadID, want := range cases {
		if got := parseChannelID(threadID); got != want {
			t.Fatalf("parseChannelID(%q) = %q, want %q", threadID, got, want)
		}
	}
}

func TestMissingBotTokenIsRefused(t *testing.T) {
	if _, err := newFactory(provider.Config{}, provider.Deps{}); err == nil {
		t.Fatal("a connection without a bot token was accepted")
	}
}
