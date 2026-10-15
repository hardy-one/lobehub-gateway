package typingonly

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/lobehub/lobehub/apps/message-gateway-go/internal/provider"
)

type capturedRequest struct {
	Path   string
	Auth   string
	Body   map[string]any
	Method string
}

func newRecorder(t *testing.T) (*httptest.Server, *[]capturedRequest) {
	t.Helper()

	var captured []capturedRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		record := capturedRequest{
			Auth:   r.Header.Get("Authorization"),
			Method: r.Method,
			Path:   r.URL.Path,
		}
		_ = json.Unmarshal(body, &record.Body)
		captured = append(captured, record)

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(server.Close)

	return server, &captured
}

func newProvider(t *testing.T, platform string, env provider.Env) provider.Provider {
	t.Helper()

	factory := newFactory(platform)
	instance, err := factory(provider.Config{
		Credentials: map[string]any{"botToken": "bot-token"},
		Platform:    platform,
	}, provider.Deps{
		Env: env,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("provider creation failed: %v", err)
	}
	return instance
}

func TestSlackTypingDrivesAssistantStatus(t *testing.T) {
	server, captured := newRecorder(t)
	instance := newProvider(t, "slack", provider.Env{SlackAPIBaseURL: server.URL})

	if err := instance.SetTyping(context.Background(), "slack:C123:1700000000.000100", true); err != nil {
		t.Fatalf("SetTyping returned %v", err)
	}

	requests := *captured
	if len(requests) != 1 {
		t.Fatalf("sent %d requests, want 1", len(requests))
	}
	if requests[0].Path != "/assistant.threads.setStatus" {
		t.Fatalf("path = %q, want the assistant status endpoint", requests[0].Path)
	}
	if requests[0].Auth != "Bearer bot-token" {
		t.Fatalf("auth = %q, want the connection's bot token", requests[0].Auth)
	}
	if requests[0].Body["channel_id"] != "C123" || requests[0].Body["thread_ts"] != "1700000000.000100" {
		t.Fatalf("body = %+v, want the channel and thread from the thread id", requests[0].Body)
	}

	// Slack clears its indicator when the reply is posted, so stopping is a
	// deliberate no-op rather than an API call with no Slack equivalent.
	if err := instance.SetTyping(context.Background(), "slack:C123:1700000000.000100", false); err != nil {
		t.Fatalf("clearing typing returned %v", err)
	}
	if len(*captured) != 1 {
		t.Fatalf("clearing typing sent a request; Slack has no clear call")
	}
}

func TestSlackTypingWithoutAThreadIsSkipped(t *testing.T) {
	server, captured := newRecorder(t)
	instance := newProvider(t, "slack", provider.Env{SlackAPIBaseURL: server.URL})

	if err := instance.SetTyping(context.Background(), "slack:C123", true); err != nil {
		t.Fatalf("SetTyping returned %v", err)
	}
	if len(*captured) != 0 {
		t.Fatal("a channel without a thread has no indicator to drive")
	}
}

func TestTelegramTypingUsesChatAction(t *testing.T) {
	server, captured := newRecorder(t)
	instance := newProvider(t, "telegram", provider.Env{TelegramAPIBaseURL: server.URL})

	if err := instance.SetTyping(context.Background(), "telegram:-100123:42", true); err != nil {
		t.Fatalf("SetTyping returned %v", err)
	}
	if err := instance.SetTyping(context.Background(), "telegram:-100123:42", false); err != nil {
		t.Fatalf("clearing typing returned %v", err)
	}

	requests := *captured
	if len(requests) != 2 {
		t.Fatalf("sent %d requests, want 2", len(requests))
	}
	if requests[0].Path != "/botbot-token/sendChatAction" {
		t.Fatalf("path = %q, want the sendChatAction endpoint", requests[0].Path)
	}
	if requests[0].Body["action"] != "typing" || requests[1].Body["action"] != "cancel" {
		t.Fatalf("actions = %v/%v, want typing then cancel", requests[0].Body["action"], requests[1].Body["action"])
	}
	if requests[0].Body["chat_id"] != "-100123" || requests[0].Body["message_thread_id"] != "42" {
		t.Fatalf("body = %+v, want the chat and topic from the thread id", requests[0].Body)
	}
}

func TestThreadIDParsing(t *testing.T) {
	t.Run("slack", func(t *testing.T) {
		channel, thread, err := parseSlackThreadID("slack:C1:TS1")
		if err != nil || channel != "C1" || thread != "TS1" {
			t.Fatalf("parsed %q/%q (%v), want C1/TS1", channel, thread, err)
		}
		if _, thread, err := parseSlackThreadID("slack:C1"); err != nil || thread != "" {
			t.Fatal("a channel-only thread id should parse without a thread")
		}
		for _, invalid := range []string{"", "slack", "telegram:C1:TS", "slack:C1:TS:extra"} {
			if _, _, err := parseSlackThreadID(invalid); err == nil {
				t.Fatalf("thread id %q was accepted", invalid)
			}
		}
	})

	t.Run("telegram", func(t *testing.T) {
		chat, topic, err := parseTelegramThreadID("telegram:-100:7")
		if err != nil || chat != "-100" || topic != "7" {
			t.Fatalf("parsed %q/%q (%v), want -100/7", chat, topic, err)
		}
		if _, topic, err := parseTelegramThreadID("telegram:-100"); err != nil || topic != "" {
			t.Fatal("a chat-only thread id should parse without a topic")
		}
		for _, invalid := range []string{"", "telegram:-100:abc", "slack:-100"} {
			if _, _, err := parseTelegramThreadID(invalid); err == nil {
				t.Fatalf("thread id %q was accepted", invalid)
			}
		}
	})
}

func TestMissingBotTokenIsRefused(t *testing.T) {
	if _, err := newFactory("slack")(provider.Config{}, provider.Deps{}); err == nil {
		t.Fatal("a connection without a bot token was accepted")
	}
}

func TestWebsocketTransportIsRefused(t *testing.T) {
	// LobeHub's Slack channel defaults to Socket Mode. This provider has no
	// inbound path, so it must refuse rather than report a bot as online.
	for _, platform := range []string{"slack", "telegram"} {
		_, err := newFactory(platform)(provider.Config{
			ConnectionMode: provider.ModeWebsocket,
			Credentials:    map[string]any{"botToken": "bot-token"},
			Platform:       platform,
		}, provider.Deps{})
		if err == nil {
			t.Fatalf("%s over websocket was accepted", platform)
		}
		if !errors.Is(err, provider.ErrUnsupportedMode) {
			t.Fatalf("%s refusal = %v, want an unsupported-mode error", platform, err)
		}
	}
}
