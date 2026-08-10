package gateway

import (
	"bufio"
	"bytes"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func testAgentServer() *httptest.Server {
	srv := NewServer(Config{ServiceToken: "service-token", LobeAPIBaseURL: "http://127.0.0.1:1"})
	return httptest.NewServer(srv.Routes())
}

func TestRequestLoggingSuccess(t *testing.T) {
	logs, restore := withCapturedLogs(t)
	defer restore()

	ts := testAgentServer()
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.1")
	req.Header.Set("User-Agent", "test-client/1.0")
	req.Header.Set("Referer", "https://example.com/")

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
		`"remote":"203.0.113.7"`,
		`"user_agent":"test-client/1.0"`,
		`"referer":"https://example.com/"`,
		`"status":200`,
		`"success":true`,
		`"upgraded":false`,
	)
}

func TestRequestLoggingWebSocketUpgrade(t *testing.T) {
	logs, restore := withCapturedLogs(t)
	defer restore()

	ts := testAgentServer()
	defer ts.Close()

	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", u.Host)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	upgrade := "GET /ws?operationId=op-1 HTTP/1.1\r\nHost: " + u.Host +
		"\r\nConnection: Upgrade\r\nUpgrade: websocket" +
		"\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(upgrade)); err != nil {
		t.Fatal(err)
	}
	statusLine, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(statusLine, "101") {
		t.Fatalf("expected 101 switching protocols, got %q", statusLine)
	}

	waitForLog(t, logs,
		`"msg":"request"`,
		`"path":"/ws"`,
		`"query":"operationId=op-1"`,
		`"status":101`,
		`"upgraded":true`,
		`"success":true`,
	)
}

func TestRequestLoggingFailure(t *testing.T) {
	logs, restore := withCapturedLogs(t)
	defer restore()

	ts := testAgentServer()
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/operations/status")
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
		`"path":"/api/operations/status"`,
		`"status":401`,
		`"success":false`,
	)
}
