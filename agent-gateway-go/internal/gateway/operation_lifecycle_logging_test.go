package gateway

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestOperationLifecycleLogs(t *testing.T) {
	buf := &logBuffer{}
	logger := lifecycleLogger(buf, slog.LevelDebug)
	_, ts, signJWT := newV2TestServerWithLogger(t, logger)
	defer ts.Close()

	postJSON(t, ts.URL+"/api/operations/init", map[string]any{
		"meta":        map[string]any{"agentId": "agent-9", "heteroType": "codex"},
		"operationId": "op-logs",
		"userId":      "user-1",
	}, 200)
	buf.waitFor(t,
		`"level":"INFO"`,
		`"msg":"operation started"`,
		`"operationId":"op-logs"`,
		`"userId":"user-1"`,
		`"agentId":"agent-9"`,
		`"heteroType":"codex"`,
	)

	postJSON(t, ts.URL+"/api/operations/push-event", eventBody("op-logs", "hello", "stream_chunk"), 200)
	buf.waitFor(t,
		`"level":"DEBUG"`,
		`"msg":"event pushed"`,
		`"operationId":"op-logs"`,
		`"eventType":"stream_chunk"`,
		`"eventId":"1"`,
	)

	ws := dialV2(t, ts.URL, signJWT, "user-1")
	defer ws.close()
	_ = ws.readJSON(t)
	ws.writeJSON(t, map[string]any{"operationId": "op-logs", "type": "subscribe", "executor": true})
	buf.waitFor(t,
		`"msg":"client subscribed"`,
		`"operationId":"op-logs"`,
		`"clientId":"test-client"`,
		`"userId":"user-1"`,
		`"executor":true`,
	)
	ws.writeJSON(t, map[string]any{"operationId": "op-logs", "type": "unsubscribe"})
	buf.waitFor(t,
		`"msg":"client unsubscribed"`,
		`"operationId":"op-logs"`,
		`"clientId":"test-client"`,
		`"executor":true`,
	)

	postJSON(t, ts.URL+"/api/operations/update-status", map[string]any{
		"operationId": "op-logs",
		"status":      "completed",
	}, 200)
	buf.waitFor(t,
		`"level":"INFO"`,
		`"msg":"operation finished"`,
		`"operationId":"op-logs"`,
		`"status":"completed"`,
		`"eventCount":2`,
		`"durationMs":`,
	)
}

func TestOperationFinishIsLoggedOnceAcrossTerminalPaths(t *testing.T) {
	buf := &logBuffer{}
	logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	srv := NewServer(Config{ServiceToken: "service-token", LobeAPIBaseURL: "http://127.0.0.1:1"}, logger)
	op := newOperation(srv, "op-once")
	op.init("op-once", "user-1", nil)
	defer op.watchdog.Stop()

	op.handleSessionEnd(StatusCompleted, "")
	op.updateStatus(StatusError, "late status")
	op.fireWatchdog()

	output := buf.String()
	if count := strings.Count(output, `"msg":"operation finished"`); count != 1 {
		t.Fatalf("operation finished logged %d times, want once:\n%s", count, output)
	}
	if !strings.Contains(output, `"status":"completed"`) {
		t.Fatalf("first terminal status was not logged:\n%s", output)
	}
}

func TestWatchdogOperationFinishedLog(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("helper process uses Unix sockets")
	}
	logPath := filepath.Join(t.TempDir(), "watchdog.log")
	cmd := exec.Command(os.Args[0], "-test.run=TestWatchdogOperationFinishedLogHelper")
	cmd.Env = append(os.Environ(), "LOBE_189_WATCHDOG_HELPER=1", "LOBE_189_WATCHDOG_LOG="+logPath)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("helper process failed: %v\n%s", err, output)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if output, err := os.ReadFile(logPath); err == nil && strings.Contains(string(output), `"msg":"operation finished"`) {
			text := string(output)
			for _, want := range []string{`"operationId":"op-watchdog"`, `"status":"error"`, `"eventCount":2`} {
				if !strings.Contains(text, want) {
					t.Fatalf("watchdog finish log missing %s:\n%s", want, text)
				}
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	output, _ := os.ReadFile(logPath)
	t.Fatalf("watchdog finish log missing:\n%s", output)
}

func TestWatchdogOperationFinishedLogHelper(t *testing.T) {
	if os.Getenv("LOBE_189_WATCHDOG_HELPER") != "1" {
		t.Skip("helper process")
	}
	logPath := os.Getenv("LOBE_189_WATCHDOG_LOG")
	file, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"abandoned": true})
	}))
	defer backend.Close()

	srv := NewServer(Config{
		LobeAPIBaseURL: backend.URL,
		ServiceToken:   "service-token",
	}, slog.New(slog.NewJSONHandler(file, &slog.HandlerOptions{Level: slog.LevelInfo})))
	op := newOperation(srv, "op-watchdog")
	op.init("op-watchdog", "user-1", nil)
	defer op.watchdog.Stop()
	op.pushEvent(agentStreamEvent{Type: "step_complete"})
	op.mu.Lock()
	op.lastEventAt = time.Now().Add(-defaultIdleWatchdog - time.Second)
	op.mu.Unlock()
	op.fireWatchdog()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func newV2TestServerWithLogger(t *testing.T, logger *slog.Logger) (*Server, *httptest.Server, func(string, time.Time, time.Time) string) {
	t.Helper()
	jwks, signJWT := testJWTSigner(t)
	srv := NewServer(Config{
		JWKSPublicKey:  jwks,
		LobeAPIBaseURL: "http://127.0.0.1:1",
		ServiceToken:   "service-token",
	}, logger)
	srv.cleanupDelay = time.Second
	return srv, httptest.NewServer(srv.Routes()), signJWT
}
