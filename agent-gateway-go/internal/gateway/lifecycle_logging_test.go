package gateway

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type logBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *logBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *logBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func (b *logBuffer) waitFor(t *testing.T, wants ...string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		out := b.String()
		all := true
		for _, want := range wants {
			if !strings.Contains(out, want) {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("missing log lines %v\noutput:\n%s", wants, b.String())
}

func lifecycleLogger(buf *logBuffer, level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level}))
}

func TestStateStoreLifecycleLogs(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	buf := &logBuffer{}
	store := newOperationStateStoreWithPath(dbPath, lifecycleLogger(buf, slog.LevelDebug))
	if store == nil {
		t.Fatal("expected SQLite state store")
	}

	buf.waitFor(t, `"msg":"state store enabled"`, `"path":"`+dbPath+`"`)

	store.save(operationStateSnapshot{
		operationRecord: operationRecord{
			CreatedAt:   time.Now(),
			OperationID: "op-lifecycle",
			Status:      StatusRunning,
			UserID:      "user-1",
		},
	})
	store.queueCounter("op-lifecycle", 7)
	store.remove("op-lifecycle")
	buf.waitFor(t, `"msg":"operation state removed"`, `"operationId":"op-lifecycle"`)

	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
	buf.waitFor(t,
		`"msg":"state store closed"`,
		`"databaseClosed":true`,
	)
}

func TestStateStoreDegradationLogIncludesReason(t *testing.T) {
	buf := &logBuffer{}
	missing := filepath.Join(t.TempDir(), "missing", "state.db")
	store := newOperationStateStoreWithPath(missing, lifecycleLogger(buf, slog.LevelInfo))
	if store != nil {
		t.Fatal("expected degraded state store to be nil")
	}
	buf.waitFor(t,
		`"level":"WARN"`,
		`"msg":"state store degraded to in-memory mode"`,
		`"path":"`+missing+`"`,
		`"reason":"state database directory unavailable`,
	)
}

func TestOperationRestoreTerminalAndGapLogs(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	buf := &logBuffer{}
	seedLogger := lifecycleLogger(&logBuffer{}, slog.LevelError)
	seed := newOperationStateStoreWithPath(dbPath, seedLogger)
	if seed == nil {
		t.Fatal("expected SQLite state store")
	}
	for _, snapshot := range []operationStateSnapshot{
		{
			operationRecord: operationRecord{
				CreatedAt:   time.Now(),
				OperationID: "op-restore",
				Status:      StatusRunning,
				UserID:      "user-1",
			},
			EventCounter: 3,
		},
		{
			operationRecord: operationRecord{
				CreatedAt:   time.Now(),
				OperationID: "op-terminal",
				Status:      StatusCompleted,
				UserID:      "user-1",
			},
			EventCounter: 1,
		},
	} {
		seed.save(snapshot)
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("seed Close failed: %v", err)
	}

	logger := lifecycleLogger(buf, slog.LevelInfo)
	srv := NewServer(Config{ServiceToken: "service-token", LobeAPIBaseURL: "http://127.0.0.1:1"}, logger)
	srv.stateStore = newOperationStateStoreWithPath(dbPath, logger)
	if srv.stateStore == nil {
		t.Fatal("expected SQLite state store")
	}
	t.Cleanup(func() { _ = srv.stateStore.Close() })
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	srv.getOrCreateOperation("op-restore")

	buf.waitFor(t,
		`"msg":"operation state restored"`,
		`"operationId":"op-restore"`,
		`"status":"running"`,
		`"eventCounter":3`,
	)

	op := srv.getOperation("op-restore")
	if op == nil {
		t.Fatal("operation was not created from restored state")
	}
	op.handleResume(nil, "99", false)
	buf.waitFor(t,
		`"msg":"event replay gap detected"`,
		`"operationId":"op-restore"`,
		`"lastEventId":"99"`,
		`"eventCounter":3`,
	)
}

func TestTerminalStatusFromStoreIsLogged(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	buf := &logBuffer{}
	logger := lifecycleLogger(buf, slog.LevelInfo)
	seed := newOperationStateStoreWithPath(dbPath, lifecycleLogger(&logBuffer{}, slog.LevelError))
	if seed == nil {
		t.Fatal("expected SQLite state store")
	}
	seed.save(operationStateSnapshot{
		operationRecord: operationRecord{
			CreatedAt:   time.Now(),
			OperationID: "op-terminal",
			Status:      StatusError,
			UserID:      "user-1",
		},
		EventCounter: 1,
	})
	if err := seed.Close(); err != nil {
		t.Fatalf("seed Close failed: %v", err)
	}

	srv := NewServer(Config{ServiceToken: "service-token", LobeAPIBaseURL: "http://127.0.0.1:1"}, logger)
	srv.stateStore = newOperationStateStoreWithPath(dbPath, logger)
	if srv.stateStore == nil {
		t.Fatal("expected SQLite state store")
	}
	t.Cleanup(func() { _ = srv.stateStore.Close() })
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/operations/status?operationId=op-terminal", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer service-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var result struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Status != string(StatusError) {
		t.Fatalf("status = %q, want error", result.Status)
	}
	buf.waitFor(t,
		`"msg":"terminal operation restored"`,
		`"operationId":"op-terminal"`,
		`"status":"error"`,
	)
}
