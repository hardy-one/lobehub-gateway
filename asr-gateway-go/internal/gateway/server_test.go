package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleCreateSession(t *testing.T) {
	cfg := Config{
		Provider:         "volcengine",
		VolcengineAPIKey: "test-key",
	}
	server := NewServer(cfg)

	req := httptest.NewRequest(http.MethodPost, "/webapi/asr/realtime/session", nil)
	w := httptest.NewRecorder()

	server.Routes().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	sessionID, ok := resp["sessionId"].(string)
	if !ok || sessionID == "" {
		t.Fatalf("expected non-empty sessionId")
	}

	token, ok := resp["token"].(string)
	if !ok || token == "" {
		t.Fatalf("expected non-empty token")
	}

	wsURL, ok := resp["websocketUrl"].(string)
	if !ok || !strings.Contains(wsURL, "/api/v1/realtime/ws?sessionId="+sessionID) {
		t.Fatalf("expected websocketUrl with sessionId, got %v", wsURL)
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := Config{
		Provider: "volcengine",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error when VOLCENGINE_VOICE_API_KEY is missing")
	}

	cfg.VolcengineAPIKey = "valid-key"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cfg.Provider = "unknown"
	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error for unknown provider")
	}
}
