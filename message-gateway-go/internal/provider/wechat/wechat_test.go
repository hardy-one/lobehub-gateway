package wechat

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/lobehub/lobehub/apps/message-gateway-go/internal/provider"
)

// fakeILink stands in for the iLink Bot API: it answers the readiness probe
// with one batch, holds every later poll open like the real service does, and
// records the typing calls it receives.
type fakeILink struct {
	mu sync.Mutex

	batch          []map[string]any
	forwards       []provider.Inbound
	getConfigCalls int
	release        chan struct{}
	sendTyping     []int
	tickets        string
	updatesCalls   int
}

func newFakeILink(t *testing.T, batch []map[string]any) (*fakeILink, *httptest.Server) {
	t.Helper()

	fake := &fakeILink{batch: batch, release: make(chan struct{}), tickets: "ticket-1"}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ilink/bot/getupdates":
			fake.mu.Lock()
			fake.updatesCalls++
			first := fake.updatesCalls == 1
			batch := fake.batch
			fake.mu.Unlock()

			if !first {
				// The real long poll holds the request open until there is
				// something to deliver. The test releases this hold on the way
				// out, after the provider has been stopped.
				select {
				case <-fake.release:
				case <-r.Context().Done():
				}
				return
			}

			writeJSON(t, w, map[string]any{
				"get_updates_buf": "cursor-1",
				"msgs":            batch,
				"ret":             0,
			})
		case "/ilink/bot/getconfig":
			fake.mu.Lock()
			fake.getConfigCalls++
			ticket := fake.tickets
			fake.mu.Unlock()
			writeJSON(t, w, map[string]any{"ret": 0, "typing_ticket": ticket})
		case "/ilink/bot/sendtyping":
			var body struct {
				Status int `json:"status"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			fake.mu.Lock()
			fake.sendTyping = append(fake.sendTyping, body.Status)
			fake.mu.Unlock()
			writeJSON(t, w, map[string]any{"ret": 0})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	// Registered before the provider's own cleanup, so LIFO ordering stops the
	// provider first, then releases any held poll, then closes the server.
	t.Cleanup(func() {
		close(fake.release)
		server.Close()
	})

	return fake, server
}

func writeJSON(t *testing.T, w http.ResponseWriter, payload any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		t.Errorf("writing a fake iLink response failed: %v", err)
	}
}

func newTestProvider(t *testing.T, baseURL string, forwards *[]provider.Inbound) provider.Provider {
	t.Helper()

	instance, err := newFactory(provider.Config{
		ConnectionID: "conn-wechat",
		Credentials: map[string]any{
			"baseUrl":      baseURL,
			"botToken":     "bot-token",
			"webhookToken": "webhook-token",
		},
		Platform:    "wechat",
		WebhookPath: "/api/agent/messenger/webhooks/wechat",
	}, provider.Deps{
		Env: provider.Env{WechatBaseURL: baseURL},
		Forward: func(_ context.Context, event provider.Inbound) error {
			*forwards = append(*forwards, event)
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

func finishedUserMessage(id int) map[string]any {
	return map[string]any{
		"context_token":  "ctx-1",
		"create_time_ms": 1700000000000,
		"from_user_id":   "user-1",
		"item_list":      []any{},
		"message_id":     id,
		"message_state":  2,
		"message_type":   1,
		"to_user_id":     "bot-1",
	}
}

func TestPollingForwardsFinishedUserMessages(t *testing.T) {
	forwards := make([]provider.Inbound, 0, 1)
	fake, server := newFakeILink(t, []map[string]any{finishedUserMessage(1)})
	instance := newTestProvider(t, server.URL, &forwards)

	if err := instance.Start(context.Background()); err != nil {
		t.Fatalf("Start returned %v", err)
	}
	t.Cleanup(func() { _ = instance.Stop(context.Background()) })

	deadline := time.Now().Add(2 * time.Second)
	for len(forwards) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(forwards) != 1 {
		t.Fatalf("forwarded %d messages, want the polled one", len(forwards))
	}

	var message struct {
		ContextToken string `json:"context_token"`
		FromUserID   string `json:"from_user_id"`
	}
	if err := json.Unmarshal(forwards[0].Body, &message); err != nil {
		t.Fatalf("forwarded body is not the raw platform message: %v", err)
	}
	if message.FromUserID != "user-1" {
		t.Fatalf("forwarded message from %q, want user-1", message.FromUserID)
	}
	if forwards[0].Headers["Authorization"] != "Bearer webhook-token" {
		t.Fatalf("forward credential = %q, want the payload's webhook token", forwards[0].Headers["Authorization"])
	}

	if fakeUpdloadsCalled(t, fake) < 1 {
		t.Fatal("the iLink long poll was never called")
	}
}

func fakeUpdloadsCalled(t *testing.T, fake *fakeILink) int {
	t.Helper()
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return fake.updatesCalls
}

func TestBotAndUnfinishedMessagesAreSkipped(t *testing.T) {
	botMessage := finishedUserMessage(2)
	botMessage["message_type"] = 2

	streaming := finishedUserMessage(3)
	streaming["message_state"] = 1

	forwards := make([]provider.Inbound, 0, 1)
	_, server := newFakeILink(t, []map[string]any{botMessage, streaming})
	instance := newTestProvider(t, server.URL, &forwards)

	if err := instance.Start(context.Background()); err != nil {
		t.Fatalf("Start returned %v", err)
	}
	t.Cleanup(func() { _ = instance.Stop(context.Background()) })

	time.Sleep(50 * time.Millisecond)
	if len(forwards) != 0 {
		t.Fatalf("forwarded %d messages, want none: bot and unfinished messages are not inbound", len(forwards))
	}
}

func TestTypingUsesTheCachedContextToken(t *testing.T) {
	forwards := make([]provider.Inbound, 0, 1)
	fake, server := newFakeILink(t, []map[string]any{finishedUserMessage(1)})
	instance := newTestProvider(t, server.URL, &forwards)

	if err := instance.Start(context.Background()); err != nil {
		t.Fatalf("Start returned %v", err)
	}
	t.Cleanup(func() { _ = instance.Stop(context.Background()) })

	// Wait for the context token to be cached from the polled message.
	deadline := time.Now().Add(2 * time.Second)
	for len(forwards) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if err := instance.SetTyping(context.Background(), "wechat:single:user-1", true); err != nil {
		t.Fatalf("starting typing returned %v", err)
	}
	if err := instance.SetTyping(context.Background(), "wechat:single:user-1", false); err != nil {
		t.Fatalf("clearing typing returned %v", err)
	}

	fake.mu.Lock()
	statuses := append([]int(nil), fake.sendTyping...)
	getConfigCalls := fake.getConfigCalls
	fake.mu.Unlock()

	if getConfigCalls != 2 {
		t.Fatalf("getconfig called %d times, want one ticket lookup per typing change", getConfigCalls)
	}
	if len(statuses) != 2 || statuses[0] != 1 || statuses[1] != 2 {
		t.Fatalf("typing statuses = %v, want [1 2]", statuses)
	}
}

func TestTypingForAnUnknownUserIsSkipped(t *testing.T) {
	forwards := make([]provider.Inbound, 0, 1)
	fake, server := newFakeILink(t, nil)
	instance := newTestProvider(t, server.URL, &forwards)

	if err := instance.Start(context.Background()); err != nil {
		t.Fatalf("Start returned %v", err)
	}
	t.Cleanup(func() { _ = instance.Stop(context.Background()) })

	// No context token has ever been seen for this user, and iLink cannot
	// produce a typing ticket without one.
	if err := instance.SetTyping(context.Background(), "wechat:single:stranger", true); err != nil {
		t.Fatalf("SetTyping returned %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.getConfigCalls != 0 {
		t.Fatalf("getconfig called %d times without a context token", fake.getConfigCalls)
	}
}

func TestThreadIDParsingKeepsColonsInTheUserID(t *testing.T) {
	if user := parseThreadID("wechat:single:user:with:colons"); user != "user:with:colons" {
		t.Fatalf("parsed %q, want the whole user id", user)
	}
	for _, invalid := range []string{"", "wechat", "wechat:single", "discord:single:user"} {
		if user := parseThreadID(invalid); user != "" {
			t.Fatalf("thread id %q parsed to %q, want no user", invalid, user)
		}
	}
}

func TestMissingBotTokenIsRefused(t *testing.T) {
	if _, err := newFactory(provider.Config{}, provider.Deps{}); err == nil {
		t.Fatal("a connection without a bot token was accepted")
	}
}
