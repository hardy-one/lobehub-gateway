package gateway

// Regression tests for LOBE-183: after a gateway process restart, resuming
// clients must observe durable terminal status, a replay gap for stale
// cursors, and a non-lying status for runs that were still in flight.
//
// Protocol note verified against the implementation: on the v1 path
// handleResume only emits resume_complete when it holds a status; after a
// restart of a still-running operation the status is intentionally empty
// (LOBE-10443), so the restart scenarios below assert through the v2 path
// (resume_complete is always sent) or through delivered events.

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func lobiStateServer(t *testing.T, dbPath string) (*Server, *httptest.Server) {
	t.Helper()
	srv := NewServer(Config{ServiceToken: "service-token", LobeAPIBaseURL: "http://127.0.0.1:1"}, nil)
	if dbPath != "" {
		srv.stateStore = newOperationStateStoreWithPath(dbPath, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if srv.stateStore != nil {
			t.Cleanup(func() {
				srv.stateStore.flush()
				_ = srv.stateStore.db.Close()
			})
		}
	}
	srv.authTimeout = time.Second
	srv.heartbeatTimeout = time.Second
	srv.cleanupDelay = 10 * time.Millisecond
	httpSrv := httptest.NewServer(srv.Routes())
	t.Cleanup(httpSrv.Close)
	return srv, httpSrv
}

func lobiV2Server(t *testing.T, dbPath string, jwks string, signJWT func(string, time.Time, time.Time) string) (*Server, *httptest.Server) {
	t.Helper()
	srv := NewServer(Config{ServiceToken: "service-token", LobeAPIBaseURL: "http://127.0.0.1:1", JWKSPublicKey: jwks}, nil)
	if dbPath != "" {
		srv.stateStore = newOperationStateStoreWithPath(dbPath, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if srv.stateStore != nil {
			t.Cleanup(func() {
				srv.stateStore.flush()
				_ = srv.stateStore.db.Close()
			})
		}
	}
	srv.authTimeout = time.Second
	srv.heartbeatTimeout = time.Second
	srv.cleanupDelay = 10 * time.Millisecond
	httpSrv := httptest.NewServer(srv.Routes())
	t.Cleanup(httpSrv.Close)
	_ = signJWT
	return srv, httpSrv
}

func lobiSeedOperation(t *testing.T, serverURL string, operationID string, status string) {
	t.Helper()
	postJSON(t, serverURL+"/api/operations/init", map[string]any{"operationId": operationID, "userId": "user-1"}, http.StatusOK)
	if status != "" {
		postJSON(t, serverURL+"/api/operations/update-status", map[string]any{"operationId": operationID, "status": status, "summary": "seeded"}, http.StatusOK)
	}
}

// Scenario 1: an operation persisted as completed must come back completed
// from a brand-new Server instance, so the client can end instead of waiting.
func TestLobiRestartCompletedOpReturnsTerminalStatus(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "restart.db")
	_, first := lobiStateServer(t, dbPath)
	lobiSeedOperation(t, first.URL, "op-lobi-completed", string(StatusCompleted))

	// Fresh process: same SQLite file, brand-new Server.
	_, second := lobiStateServer(t, dbPath)
	if status := statusOf(t, second.URL, "op-lobi-completed"); status != string(StatusCompleted) {
		t.Fatalf("after restart expected completed, got %s", status)
	}

	ws := dialWebSocket(t, second.URL, "/ws?operationId=op-lobi-completed")
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	if msg := ws.readJSON(t); msg["type"] != "auth_success" {
		t.Fatalf("unexpected auth message: %+v", msg)
	}
	ws.writeJSON(t, map[string]any{"type": "resume", "lastEventId": "0", "wantStatus": true})
	complete := ws.readJSON(t)
	if complete["type"] != "resume_complete" || complete["status"] != string(StatusCompleted) {
		t.Fatalf("resume_complete must carry terminal status, got %+v", complete)
	}
}

// Scenario 2 (v2): a client whose lastEventId is ahead of a restored counter
// gets gap=true on subscribe, and events emitted afterwards (fresh ids) must
// still be delivered instead of being dropped by the id <= lastSeq filter.
func TestLobiRestartAheadClientGetsGapAndLaterEvents(t *testing.T) {
	jwks, signJWT := testJWTSigner(t)
	dbPath := filepath.Join(t.TempDir(), "ahead.db")
	_, first := lobiV2Server(t, dbPath, jwks, signJWT)
	lobiSeedOperation(t, first.URL, "op-lobi-ahead", string(StatusRunning))
	postJSON(t, first.URL+"/api/operations/push-event", eventBody("op-lobi-ahead", "before-restart", "stream_chunk"), http.StatusOK)
	first.Close()

	_, second := lobiV2Server(t, dbPath, jwks, signJWT)
	ws := dialV2(t, second.URL, signJWT, "user-1")
	defer ws.close()
	_ = ws.readJSON(t) // ready
	ws.writeJSON(t, map[string]any{"type": "subscribe", "operationId": "op-lobi-ahead", "lastEventId": "47"})
	complete := readUntil(t, ws, func(message map[string]any) bool { return message["type"] == "resume_complete" })
	if complete["gap"] != true {
		t.Fatalf("expected gap=true for ahead client after restart, got %+v", complete)
	}
	if complete["status"] == "waiting_for_human" {
		t.Fatalf("resume_complete must not resurrect a stale waiting status, got %+v", complete)
	}

	postJSON(t, second.URL+"/api/operations/push-event", eventBody("op-lobi-ahead", "after-gap", "stream_chunk"), http.StatusOK)
	message := ws.readJSON(t)
	if message["type"] != "agent_event" {
		t.Fatalf("new event after gap must be delivered, got %+v", message)
	}
	if id := message["id"]; id == nil || id == "" {
		t.Fatalf("expected a fresh event id, got %+v", message["id"])
	}
}

// Scenario 2 (v1): a terminal op restored from SQLite must report gap=true
// when the client cursor is ahead of the restored counter.
func TestLobiV1RestartTerminalResumeReportsGap(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ahead-v1.db")
	_, first := lobiStateServer(t, dbPath)
	lobiSeedOperation(t, first.URL, "op-lobi-ahead-v1", string(StatusCompleted))
	postJSON(t, first.URL+"/api/operations/push-event", eventBody("op-lobi-ahead-v1", "before-restart", "stream_chunk"), http.StatusOK)

	_, second := lobiStateServer(t, dbPath)
	ws := dialWebSocket(t, second.URL, "/ws?operationId=op-lobi-ahead-v1")
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	if msg := ws.readJSON(t); msg["type"] != "auth_success" {
		t.Fatalf("unexpected auth message: %+v", msg)
	}
	ws.writeJSON(t, map[string]any{"type": "resume", "lastEventId": "47", "wantStatus": true})
	complete := ws.readJSON(t)
	if complete["type"] != "resume_complete" || complete["gap"] != true || complete["status"] != string(StatusCompleted) {
		t.Fatalf("expected v1 gap=true with terminal status after restart, got %+v", complete)
	}
}

// Scenario 3: a run persisted as waiting_for_human must NOT be restored as
// waiting_for_human — the approval channel died with the old process — and
// the event counter must still be restored correctly.
func TestLobiRestartWaitingStatusIsNotRestored(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "waiting.db")
	first, firstHTTP := lobiStateServer(t, dbPath)
	lobiSeedOperation(t, firstHTTP.URL, "op-lobi-waiting", "waiting_for_human")
	// Real crash timing is unpredictable; flush here pins the worst case the
	// design guarantees: status is saved synchronously, counter may lag.
	first.stateStore.flush()

	_, second := lobiStateServer(t, dbPath)
	if status := statusOf(t, second.URL, "op-lobi-waiting"); status == "waiting_for_human" {
		t.Fatal("waiting_for_human must not survive a restart; client would wait forever")
	}

	ws := dialWebSocket(t, second.URL, "/ws?operationId=op-lobi-waiting")
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	if msg := ws.readJSON(t); msg["type"] != "auth_success" {
		t.Fatalf("unexpected auth message: %+v", msg)
	}
	// v1 sends no resume_complete while the restored status is empty; the
	// important contract is that it must not send waiting_for_human.
	ws.writeJSON(t, map[string]any{"type": "resume", "lastEventId": "0", "wantStatus": true})
	wsExpectNoMessage(t, ws, 300*time.Millisecond)

	// A new event must be delivered; with the counter restored it gets id 3
	// (ids 1 and 2 were consumed by the two seed messages). Without a restored
	// counter it would come back as id 1.
	postJSON(t, second.URL+"/api/operations/push-event", eventBody("op-lobi-waiting", "post-restart", "stream_chunk"), http.StatusOK)
	message := ws.readJSON(t)
	if message["type"] != "agent_event" {
		t.Fatalf("expected live event after restart, got %+v", message)
	}
	if message["id"] == "1" {
		t.Fatalf("event counter was not restored from SQLite, got id=%v", message["id"])
	}
}

// Scenario 4: an unusable SQLite path must not prevent startup; the gateway
// degrades to memory-only operation.
func TestLobiUnwritableSQLitePathStillStarts(t *testing.T) {
	readonlyDir := filepath.Join(t.TempDir(), "readonly")
	if err := os.MkdirAll(readonlyDir, 0o755); err != nil {
		t.Skipf("cannot set up read-only directory: %v", err)
	}
	if err := os.Chmod(readonlyDir, 0o500); err != nil {
		t.Skipf("cannot chmod read-only: %v", err)
	}
	if os.Geteuid() == 0 {
		t.Skip("running as root; read-only directory is still writable")
	}
	store := newOperationStateStoreWithPath(filepath.Join(readonlyDir, "state.db"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if store != nil {
		t.Fatal("expected nil store for unwritable SQLite path")
	}
	srv := NewServer(Config{ServiceToken: "service-token"}, nil)
	srv.stateStore = store
	op := srv.getOrCreateOperation("op-lobi-degraded")
	op.init("op-lobi-degraded", "user-1", nil)
	if _, status := op.statusSnapshot(); status != StatusRunning {
		t.Fatalf("memory fallback must still operate, got %s", status)
	}
}

// Scenario 5: single process, no restart — replay and dedup keep the
// pre-change behavior: no gap for a current cursor, no replay of events the
// client already has, and the cursor-ahead gap does not fire spuriously.
func TestLobiNoRestartReplayDedupUnchanged(t *testing.T) {
	_, ts := lobiStateServer(t, filepath.Join(t.TempDir(), "normal.db"))
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "op-lobi-normal", "userId": "user-1"}, http.StatusOK)
	postJSON(t, ts.URL+"/api/operations/push-event", eventBody("op-lobi-normal", "one", "stream_chunk"), http.StatusOK)
	postJSON(t, ts.URL+"/api/operations/push-event", eventBody("op-lobi-normal", "two", "stream_chunk"), http.StatusOK)

	ws := dialWebSocket(t, ts.URL, "/ws?operationId=op-lobi-normal")
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	if msg := ws.readJSON(t); msg["type"] != "auth_success" {
		t.Fatalf("unexpected auth message: %+v", msg)
	}
	// v1 writes replayed payloads first, then resume_complete.
	ws.writeJSON(t, map[string]any{"type": "resume", "lastEventId": "1", "wantStatus": true})
	replayed := ws.readJSON(t)
	if replayed["type"] != "agent_event" || replayed["id"] != "2" {
		t.Fatalf("expected only event id 2 replayed after resume from 1, got %+v", replayed)
	}
	complete := ws.readJSON(t)
	if complete["type"] != "resume_complete" || complete["gap"] != false {
		t.Fatalf("current client must not see a gap, got %+v", complete)
	}
	// The already-seen event 1 must never come back.
	postJSON(t, ts.URL+"/api/operations/push-event", eventBody("op-lobi-normal", "three", "stream_chunk"), http.StatusOK)
	message := ws.readJSON(t)
	if message["type"] != "agent_event" || message["id"] != "3" {
		t.Fatalf("expected next live event id 3, got %+v", message)
	}
}
