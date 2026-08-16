package gateway

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a mutex-protected buffer safe for concurrent slog writes
// from server goroutines and reads from the test goroutine.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func withCapturedLogs(t *testing.T) (*syncBuffer, func()) {
	t.Helper()
	buf := &syncBuffer{}
	handler := slog.New(slog.NewJSONHandler(buf, nil))
	prev := slog.Default()
	slog.SetDefault(handler)
	return buf, func() { slog.SetDefault(prev) }
}

// waitForLog polls until all substrings appear in the captured logs, since
// the middleware logs after the response is written and the client may
// observe the response first.
func waitForLog(t *testing.T, logs *syncBuffer, wants ...string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		out := logs.String()
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
		time.Sleep(10 * time.Millisecond)
	}
	out := logs.String()
	for _, want := range wants {
		if !strings.Contains(out, want) {
			t.Errorf("log output missing %q\noutput:\n%s", want, out)
		}
	}
}

func testDeviceServer() *httptest.Server {
	srv := NewServer(Config{ServiceToken: "service-token", RequestLogging: true})
	return httptest.NewServer(srv.Routes())
}

func TestRequestLoggingSuccess(t *testing.T) {
	logs, restore := withCapturedLogs(t)
	defer restore()

	ts := testDeviceServer()
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Real-IP", "198.51.100.23")
	req.Header.Set("User-Agent", "test-client/1.0")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("X-Request-Id") == "" {
		t.Error("expected X-Request-Id response header")
	}
	waitForLog(t, logs,
		`"msg":"request"`,
		`"method":"GET"`,
		`"path":"/health"`,
		`"remote":"198.51.100.23"`,
		`"user_agent":"test-client/1.0"`,
		`"status":200`,
		`"success":true`,
		`"upgraded":false`,
	)
}

func TestRequestLoggingFailure(t *testing.T) {
	logs, restore := withCapturedLogs(t)
	defer restore()

	ts := testDeviceServer()
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/api/device/status", strings.NewReader(`{"userId":"u1"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
	waitForLog(t, logs,
		`"level":"WARN"`,
		`"msg":"request failed"`,
		`"path":"/api/device/status"`,
		`"status":401`,
		`"success":false`,
	)
}

func TestRequestLoggingDisabledByDefault(t *testing.T) {
	logs, restore := withCapturedLogs(t)
	defer restore()

	srv := NewServer(Config{ServiceToken: "service-token"})
	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.Header.Get("X-Request-Id") == "" {
		t.Error("expected X-Request-Id response header even when logging disabled")
	}

	// The logging middleware is opt-in; no request log should be emitted when
	// RequestLogging is false.
	time.Sleep(50 * time.Millisecond)
	if out := logs.String(); strings.Contains(out, `"msg":"request"`) {
		t.Fatalf("request log emitted while logging disabled\noutput:\n%s", out)
	}
}
