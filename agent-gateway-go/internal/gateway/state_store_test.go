package gateway

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testStateStore(t *testing.T) *operationStateStore {
	t.Helper()
	store := newOperationStateStoreWithPath(t.TempDir()+"/state.db", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if store == nil {
		t.Fatal("expected SQLite state store")
	}
	t.Cleanup(func() {
		store.flush()
		if err := store.db.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func testStateServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	srv := NewServer(Config{ServiceToken: "service-token", LobeAPIBaseURL: "http://127.0.0.1:1"}, nil)
	srv.stateStore = testStateStore(t)
	srv.authTimeout = time.Second
	srv.heartbeatTimeout = time.Second
	srv.cleanupDelay = 10 * time.Millisecond
	httpSrv := httptest.NewServer(srv.Routes())
	t.Cleanup(httpSrv.Close)
	return srv, httpSrv
}

func TestOperationStateStoreCloseFlushesPendingCounters(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.db")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := newOperationStateStoreWithPath(dbPath, logger)
	if store == nil {
		t.Fatal("expected SQLite state store")
	}
	store.save(operationStateSnapshot{
		operationRecord: operationRecord{
			CreatedAt:   time.Now(),
			OperationID: "op-close",
			Status:      StatusRunning,
			UserID:      "user-1",
		},
	})
	for counter := 1; counter <= 5; counter++ {
		store.queueCounter("op-close", counter)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened, err := openOperationStateDB(dbPath)
	if err != nil {
		t.Fatalf("reopen state DB failed: %v", err)
	}
	defer reopened.Close()
	var counter int
	if err := reopened.QueryRow(`SELECT event_counter FROM operation_state WHERE id = ?`, "op-close").Scan(&counter); err != nil {
		t.Fatalf("read counter after close failed: %v", err)
	}
	if counter != 5 {
		t.Fatalf("counter after Close = %d, want 5", counter)
	}
}

func TestServerCloseRejectsNewOperationWrites(t *testing.T) {
	srv, ts := testStateServer(t)
	if err := srv.Close(); err != nil {
		t.Fatalf("Server.Close failed: %v", err)
	}
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{
		"operationId": "op-after-close",
		"userId":      "user-1",
	}, http.StatusServiceUnavailable)
}

func TestOperationStateStoreCloseIsSafeWhenDBUnavailable(t *testing.T) {
	store := &operationStateStore{
		db:          nil,
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		pending:     map[string]int{"op-broken": 7},
		closed:      make(chan struct{}),
		closing:     make(chan struct{}),
		cleanupDone: make(chan struct{}),
	}
	close(store.cleanupDone)
	if err := store.Close(); err != nil {
		t.Fatalf("Close with unavailable DB should not fail: %v", err)
	}
}

func TestOperationStateNewOperationWithoutRecord(t *testing.T) {
	srv := NewServer(Config{ServiceToken: "service-token"}, nil)
	srv.stateStore = testStateStore(t)
	op := srv.getOrCreateOperation("op-new")
	_, status := op.statusSnapshot()
	if status != "unknown" || op.eventCounter != 0 || op.record.CreatedAt.IsZero() {
		t.Fatalf("unexpected fresh operation: status=%s counter=%d created=%v", status, op.eventCounter, op.record.CreatedAt)
	}
}

func TestOperationStateRestoresTerminalStatus(t *testing.T) {
	store := testStateStore(t)
	created := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	store.save(operationStateSnapshot{
		operationRecord: operationRecord{
			CreatedAt:   created,
			OperationID: "op-terminal",
			Status:      StatusCompleted,
			UserID:      "user-1",
		},
		EventCounter: 17,
	})
	srv := NewServer(Config{ServiceToken: "service-token"}, nil)
	srv.stateStore = store
	op := srv.getOrCreateOperation("op-terminal")
	if op.record.Status != StatusCompleted || op.eventCounter != 17 || op.record.UserID != "user-1" || !op.record.CreatedAt.Equal(created) {
		t.Fatalf("terminal state was not restored: %+v counter=%d", op.record, op.eventCounter)
	}
}

func TestOperationStateRestoresCounterOnlyForRunningStatus(t *testing.T) {
	for _, status := range []SessionStatus{StatusRunning, "idle", "waiting_for_human", "waiting_for_async_tool", "waiting_for_client"} {
		t.Run(string(status), func(t *testing.T) {
			store := testStateStore(t)
			operationID := "op-" + strings.ReplaceAll(string(status), "_", "-")
			created := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
			store.save(operationStateSnapshot{
				operationRecord: operationRecord{
					CreatedAt:   created,
					OperationID: operationID,
					Status:      status,
					UserID:      "user-1",
				},
				EventCounter: 9,
			})
			srv := NewServer(Config{ServiceToken: "service-token"}, nil)
			srv.stateStore = store
			op := srv.getOrCreateOperation(operationID)
			if op.record.Status != "" || op.eventCounter != 9 || op.record.UserID != "user-1" || !op.record.CreatedAt.Equal(created) {
				t.Fatalf("running state restore wrong: status=%q counter=%d user=%q created=%v", op.record.Status, op.eventCounter, op.record.UserID, op.record.CreatedAt)
			}
		})
	}
}

func TestOperationStateRestartReplayAndGap(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "gateway.db")
	newStateServer := func() (*Server, *httptest.Server) {
		srv := NewServer(Config{ServiceToken: "service-token", LobeAPIBaseURL: "http://127.0.0.1:1"}, nil)
		srv.stateStore = newOperationStateStoreWithPath(dbPath, slog.New(slog.NewTextHandler(io.Discard, nil)))
		srv.authTimeout = time.Second
		srv.heartbeatTimeout = time.Second
		srv.cleanupDelay = 10 * time.Millisecond
		t.Cleanup(func() {
			srv.stateStore.flush()
			_ = srv.stateStore.db.Close()
		})
		return srv, httptest.NewServer(srv.Routes())
	}

	srv, httpSrv := newStateServer()
	postJSON(t, httpSrv.URL+"/api/operations/init", map[string]any{"operationId": "op-restart", "userId": "user-1"}, http.StatusOK)
	postJSON(t, httpSrv.URL+"/api/operations/push-event", eventBody("op-restart", "first", "stream_chunk"), http.StatusOK)
	srv.stateStore.flush()
	if status := statusOf(t, httpSrv.URL, "op-restart"); status != "running" {
		t.Fatalf("unexpected status before restart: %s", status)
	}
	httpSrv.Close()

	_, restarted := newStateServer()
	defer restarted.Close()
	if status := statusOf(t, restarted.URL, "op-restart"); status != "" {
		t.Fatalf("running operation with no in-memory state must report an empty status, got %s", status)
	}
	postJSON(t, restarted.URL+"/api/operations/update-status", map[string]any{
		"operationId": "op-restart", "status": "completed", "summary": "done",
	}, http.StatusOK)
	if status := statusOf(t, restarted.URL, "op-restart"); status != "completed" {
		t.Fatalf("terminal status after restart was %s", status)
	}
	postJSON(t, restarted.URL+"/api/operations/push-event", eventBody("op-restart", "after-restart", "stream_chunk"), http.StatusOK)

	ws := dialWebSocket(t, restarted.URL, "/ws?operationId=op-restart")
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	if msg := ws.readJSON(t); msg["type"] != "auth_success" {
		t.Fatalf("unexpected auth message: %+v", msg)
	}
	ws.writeJSON(t, map[string]any{"type": "resume", "lastEventId": "999", "wantStatus": true})
	complete := ws.readJSON(t)
	if complete["type"] != "resume_complete" || complete["gap"] != true {
		t.Fatalf("expected gap=true after restart, got %+v", complete)
	}
	postJSON(t, restarted.URL+"/api/operations/push-event", eventBody("op-restart", "after-resume", "stream_chunk"), http.StatusOK)
	message := ws.readJSON(t)
	if message["type"] != "agent_event" {
		t.Fatalf("expected a live event after an ahead-client resume, got %+v", message)
	}
}

func TestCollectReplayAheadClientReportsGap(t *testing.T) {
	srv := NewServer(Config{ServiceToken: "service-token"}, nil)
	op := srv.getOrCreateOperation("op-ahead")
	op.eventCounter = 2
	_, gap, _ := op.collectReplay("7")
	if !gap {
		t.Fatal("expected gap when client event id is ahead of the local counter")
	}
	_, gap, _ = op.collectReplay("2")
	if gap {
		t.Fatal("expected no gap when client is current")
	}
}

func TestV1AheadClientResumeResetsLastSeq(t *testing.T) {
	_, ts := testStateServer(t)
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "op-ahead-v1", "userId": "user-1"}, http.StatusOK)
	postJSON(t, ts.URL+"/api/operations/push-event", eventBody("op-ahead-v1", "before-resume", "stream_chunk"), http.StatusOK)

	ws := dialWebSocket(t, ts.URL, "/ws?operationId=op-ahead-v1")
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	if msg := ws.readJSON(t); msg["type"] != "auth_success" {
		t.Fatalf("unexpected auth message: %+v", msg)
	}
	ws.writeJSON(t, map[string]any{"type": "resume", "lastEventId": "999", "wantStatus": true})
	complete := ws.readJSON(t)
	if complete["type"] != "resume_complete" || complete["gap"] != true {
		t.Fatalf("expected v1 ahead-client gap=true, got %+v", complete)
	}

	postJSON(t, ts.URL+"/api/operations/push-event", eventBody("op-ahead-v1", "after-resume", "stream_chunk"), http.StatusOK)
	message := ws.readJSON(t)
	if message["type"] != "agent_event" || message["id"] != "2" {
		t.Fatalf("expected v1 live event after lastSeq reset, got %+v", message)
	}
}

func TestV2AheadClientResumeResetsLastSeq(t *testing.T) {
	_, ts, signJWT := newV2TestServer(t)
	defer ts.Close()
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "op-ahead-v2", "userId": "user-1"}, http.StatusOK)
	postJSON(t, ts.URL+"/api/operations/push-event", eventBody("op-ahead-v2", "live", "stream_chunk"), http.StatusOK)

	ws := dialV2(t, ts.URL, signJWT, "user-1")
	defer ws.close()
	_ = ws.readJSON(t)
	ws.writeJSON(t, map[string]any{"type": "subscribe", "operationId": "op-ahead-v2", "lastEventId": "999"})
	complete := readUntil(t, ws, func(message map[string]any) bool { return message["type"] == "resume_complete" })
	if complete["gap"] != true || complete["status"] != "running" {
		t.Fatalf("unexpected ahead-client resume: %+v", complete)
	}

	postJSON(t, ts.URL+"/api/operations/push-event", eventBody("op-ahead-v2", "next", "stream_chunk"), http.StatusOK)
	message := ws.readJSON(t)
	if message["type"] != "agent_event" || message["operationId"] != "op-ahead-v2" {
		t.Fatalf("expected live event after lastSeq reset, got %+v", message)
	}
}

func TestSQLiteUnavailableFallsBackToMemory(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing-dir", "state.db")
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	store := newOperationStateStoreWithPath(missing, logger)
	if store != nil {
		t.Fatal("expected persistence to be disabled when directory is missing")
	}
	if !strings.Contains(logs.String(), "state store degraded to in-memory mode") {
		t.Fatalf("expected a warn log, got %q", logs.String())
	}
	srv := NewServer(Config{ServiceToken: "service-token"}, nil)
	srv.stateStore = store
	op := srv.getOrCreateOperation("op-fallback")
	op.init("op-fallback", "user-1", nil)
	_, status := op.statusSnapshot()
	if status != "running" {
		t.Fatalf("memory-only operation should still work, got %s", status)
	}
}

func TestFlushCounterPreservesTimerForOtherPendingOperations(t *testing.T) {
	store := testStateStore(t)
	store.save(operationStateSnapshot{
		operationRecord: operationRecord{
			CreatedAt:   time.Now(),
			OperationID: "op-concurrent-a",
			Status:      StatusRunning,
			UserID:      "user-1",
		},
	})
	store.save(operationStateSnapshot{
		operationRecord: operationRecord{
			CreatedAt:   time.Now(),
			OperationID: "op-concurrent-b",
			Status:      StatusRunning,
			UserID:      "user-1",
		},
	})
	store.queueCounter("op-concurrent-a", 1)
	store.queueCounter("op-concurrent-b", 2)
	store.flushCounter("op-concurrent-a", 1)

	deadline := time.Now().Add(2 * time.Second)
	for {
		if record, ok := store.load("op-concurrent-b"); ok && record.EventCounter == 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("pending counter for the other operation was never flushed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStatusOfUnknownOperationDoesNotCreateOne(t *testing.T) {
	srv, ts := testStateServer(t)
	defer ts.Close()

	srv.operationsMu.Lock()
	before := len(srv.operations)
	srv.operationsMu.Unlock()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/operations/status?operationId=op-never-created", nil)
	req.Header.Set("Authorization", "Bearer service-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for an unknown operation, got %d", resp.StatusCode)
	}
	var result struct {
		ClientConnected bool   `json:"clientConnected"`
		Status          string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Status != "" || result.ClientConnected {
		t.Fatalf("unexpected unknown-operation status: %+v", result)
	}

	srv.operationsMu.Lock()
	after := len(srv.operations)
	srv.operationsMu.Unlock()
	if after != before {
		t.Fatalf("status query created an operation: before=%d after=%d", before, after)
	}
}

func TestTerminalStatusSurvivesMemoryCleanup(t *testing.T) {
	srv, httpSrv := testStateServer(t)
	const opID = "op-cleanup-terminal"
	postJSON(t, httpSrv.URL+"/api/operations/init", map[string]any{"operationId": opID, "userId": "user-1"}, http.StatusOK)
	postJSON(t, httpSrv.URL+"/api/operations/update-status", map[string]any{
		"operationId": opID, "status": "completed", "summary": "done",
	}, http.StatusOK)
	srv.stateStore.flush()

	op := srv.getOperation(opID)
	if op == nil {
		t.Fatal("expected in-memory operation before cleanup")
	}
	// Simulate the in-memory object being reclaimed long after the client
	// suspended the app. The terminal status must remain queryable.
	op.cleanup()
	srv.stateStore.flush()

	if got := statusOf(t, httpSrv.URL, opID); got != string(StatusCompleted) {
		t.Fatalf("terminal status lost after cleanup: got %q", got)
	}
	if srv.getOperation(opID) != nil {
		t.Fatal("cleanup must remove the in-memory operation")
	}
	restored := srv.getOrCreateOperation(opID)
	if restored.record.Status != StatusCompleted {
		t.Fatalf("resubscribe restored status = %q, want completed", restored.record.Status)
	}
}

func statusOf(t *testing.T, serverURL string, operationID string) string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, serverURL+"/api/operations/status?operationId="+operationID, nil)
	req.Header.Set("Authorization", "Bearer service-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var result struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatal(err)
	}
	return result.Status
}
