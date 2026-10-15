package lobehub

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestUpsertPlatformSendsTokenAndPlaintextCredentials(t *testing.T) {
	var gotAuth string
	var gotBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if r.URL.Path != platformsPath {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	client := New(server.URL+"/", "service-token")
	err := client.UpsertPlatform(context.Background(), Platform{
		ApplicationID: "app-id",
		Credentials:   map[string]any{"botToken": "bot-token"},
		Enabled:       true,
		Platform:      "discord",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotAuth != "Bearer service-token" {
		t.Fatalf("unexpected authorization header %q", gotAuth)
	}
	// Credentials travel as plaintext: LobeHub encrypts them, which is the whole
	// reason this process never needs the vault key or a cipher format.
	if gotBody["credentials"].(map[string]any)["botToken"] != "bot-token" {
		t.Fatalf("credentials not forwarded verbatim: %+v", gotBody)
	}
	if gotBody["platform"] != "discord" || gotBody["enabled"] != true {
		t.Fatalf("unexpected payload: %+v", gotBody)
	}
}

// A rejected credential must surface as an error an operator can read, not as a
// silent no-op.
func TestUpsertPlatformSurfacesUpstreamError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":"KEY_VAULTS_SECRET is not configured"}`))
	}))
	defer server.Close()

	err := New(server.URL, "token").UpsertPlatform(context.Background(), Platform{Platform: "discord"})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "503") || !strings.Contains(err.Error(), "KEY_VAULTS_SECRET") {
		t.Fatalf("error should carry the status and upstream message, got %v", err)
	}
}

func TestListPlatformsDecodesSummaries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected method %s", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"platforms":[{"platform":"telegram","enabled":true,"applicationId":"","updatedAt":"2026-10-15T08:00:00Z"}]}`))
	}))
	defer server.Close()

	platforms, err := New(server.URL, "token").ListPlatforms(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(platforms) != 1 || platforms[0].Platform != "telegram" || !platforms[0].Enabled {
		t.Fatalf("unexpected summaries: %+v", platforms)
	}
	if platforms[0].UpdatedAt.IsZero() {
		t.Fatalf("updatedAt should be decoded: %+v", platforms[0])
	}
}
