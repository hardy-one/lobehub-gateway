package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lobehub/lobehub/apps/oss-center-go/internal/lobehub"
	"github.com/lobehub/lobehub/apps/oss-center-go/internal/reconcile"
)

type fakeStore struct {
	err error
}

func (f fakeStore) ListPlatforms(context.Context) ([]lobehub.Summary, error) { return nil, f.err }

func (f fakeStore) UpsertPlatform(context.Context, lobehub.Platform) error { return f.err }

func get(t *testing.T, handler http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	return recorder
}

// A container's first health check can arrive before the first pass finishes;
// that is not a failure.
func TestHealthzIsOKBeforeTheFirstPass(t *testing.T) {
	reconciler := reconcile.New(fakeStore{}, nil, nil, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	handler := New(reconciler, []string{"telegram"}).Routes()

	recorder := get(t, handler, "/healthz")

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recorder.Code)
	}
	if body := recorder.Body.String(); body == "" || body[:1] != "{" {
		t.Fatalf("unexpected body %q", body)
	}
}

func TestHealthzReportsAFailedPass(t *testing.T) {
	reconciler := reconcile.New(fakeStore{err: context.DeadlineExceeded}, nil, nil, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	reconciler.RunOnce(context.Background())
	handler := New(reconciler, nil).Routes()

	recorder := get(t, handler, "/healthz")

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 after a failed pass, got %d", recorder.Code)
	}
}

func TestStatusReportsDeclaredPlatformsAndLastRun(t *testing.T) {
	reconciler := reconcile.New(fakeStore{}, nil, nil, time.Minute, slog.New(slog.NewTextHandler(io.Discard, nil)))
	reconciler.RunOnce(context.Background())
	handler := New(reconciler, []string{"discord", "telegram"}).Routes()

	recorder := get(t, handler, "/status")

	var payload struct {
		Declared []string          `json:"declared"`
		LastRun  *reconcile.Result `json:"lastRun"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if len(payload.Declared) != 2 || payload.LastRun == nil {
		t.Fatalf("unexpected status payload: %+v", payload)
	}
}
