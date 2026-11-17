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
	srv := NewServer(Config{ServiceToken: "service-token", LobeAPIBaseURL: "http://127.0.0.1:1"})
	srv.stateStore = testStateStore(t)
	srv.authTimeout = time.Second
	srv.heartbeatTimeout = time.Second
	srv.cleanupDelay = 10 * time.Millisecond
	httpSrv := httptest.NewServer(srv.Routes())
	t.Cleanup(httpSrv.Close)
	return srv, httpSrv
}

func TestOperationStateNewOperationWithoutRecord(t *testing.T) {
	srv := NewServer(Config{ServiceToken: "service-token"})
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
	srv := NewServer(Config{ServiceToken: "service-token"})
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
			srv := NewServer(Config{ServiceToken: "service-token"})
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
		srv := NewServer(Config{ServiceToken: "service-token", LobeAPIBaseURL: "http://127.0.0.1:1"})
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
	if status := statusOf(t, restarted.URL, "op-restart"); status != "unknown" {
		t.Fatalf("running operation must come back with empty status, got %s", status)
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
	srv := NewServer(Config{ServiceToken: "service-token"})
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
	if !strings.Contains(logs.String(), "operation state persistence disabled") {
		t.Fatalf("expected a warn log, got %q", logs.String())
	}
	srv := NewServer(Config{ServiceToken: "service-token"})
	srv.stateStore = store
	op := srv.getOrCreateOperation("op-fallback")
	op.init("op-fallback", "user-1", nil)
	_, status := op.statusSnapshot()
	if status != "running" {
		t.Fatalf("memory-only operation should still work, got %s", status)
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
