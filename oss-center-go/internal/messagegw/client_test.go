package messagegw

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEnsureSendsTheMessengerPayloadLobeHubExpects(t *testing.T) {
	var gotAuth string
	var gotBody struct {
		Config ConnectionConfig `json:"config"`
		Ensure bool             `json:"ensure"`
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if r.Method != http.MethodPost || r.URL.Path != connectionsPath {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"connected"}`))
	}))
	defer server.Close()

	err := New(server.URL, "service-token").Ensure(context.Background(), ConnectionConfig{
		ApplicationID:  "app-id",
		Capabilities:   Capabilities{MessageMonitoring: MessageMonitoring{Enabled: false}},
		ConnectionID:   "messenger:discord:singleton",
		ConnectionMode: "websocket",
		Credentials:    map[string]any{"botToken": "bot-token"},
		Platform:       "discord",
		UserID:         "system",
		WebhookPath:    "/api/agent/messenger/webhooks/discord",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotAuth != "Bearer service-token" {
		t.Fatalf("unexpected authorization header %q", gotAuth)
	}
	// ensure=true is what makes a periodic pass a no-op instead of a reconnect.
	if !gotBody.Ensure {
		t.Fatal("reconcile must pass ensure=true")
	}
	if gotBody.Config.Capabilities.MessageMonitoring.Enabled {
		t.Fatal("a Messenger connection never consumes passive monitoring")
	}
	if gotBody.Config.WebhookPath != "/api/agent/messenger/webhooks/discord" {
		t.Fatalf("inbound events would go nowhere: %+v", gotBody.Config)
	}
}

// A connection the gateway does not hold is the desired end state.
func TestDisconnectTreatsMissingConnectionAsDone(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("connection not found"))
	}))
	defer server.Close()

	if err := New(server.URL, "token").Disconnect(context.Background(), "messenger:discord:singleton"); err != nil {
		t.Fatalf("a missing connection must not error: %v", err)
	}
}

func TestDisconnectReportsUpstreamFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer server.Close()

	if err := New(server.URL, "token").Disconnect(context.Background(), "id"); err == nil {
		t.Fatal("expected a failure to surface")
	}
}

func TestStatusDecodesState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/connections/messenger:discord:singleton/status" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"config":{"connectionId":"messenger:discord:singleton","platform":"discord"},
			"state":{"status":"error","errorCode":"4004","error":"Authentication failed","connectedAt":0}}`))
	}))
	defer server.Close()

	status, err := New(server.URL, "token").Status(context.Background(), "messenger:discord:singleton")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status.State.Status != "error" || status.State.ErrorCode != "4004" {
		t.Fatalf("unexpected state: %+v", status.State)
	}
	if status.Config == nil || status.Config.Platform != "discord" {
		t.Fatalf("unexpected config: %+v", status.Config)
	}
}

func TestStatusSurfacesMissingConnection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	_, err := New(server.URL, "token").Status(context.Background(), "id")
	if !errors.Is(err, ErrConnectionNotHeld) {
		t.Fatalf("expected ErrConnectionNotHeld, got %v", err)
	}
}
