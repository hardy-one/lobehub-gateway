package gateway

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// statusRecorder captures the response status code and byte count while
// preserving the optional interfaces http.Server relies on (Flusher,
// Hijacker, ReaderFrom). WebSocket upgrades record status 101.
type statusRecorder struct {
	http.ResponseWriter
	status   int
	bytes    int64
	hijacked bool
}

func (r *statusRecorder) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
	}
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("hijacking unsupported")
	}
	conn, rw, err := h.Hijack()
	if err == nil {
		r.hijacked = true
		r.status = http.StatusSwitchingProtocols
	}
	return conn, rw, err
}

func (r *statusRecorder) ReadFrom(src io.Reader) (int64, error) {
	if rf, ok := r.ResponseWriter.(io.ReaderFrom); ok {
		n, err := rf.ReadFrom(src)
		r.bytes += n
		return n, err
	}
	n, err := io.Copy(r.ResponseWriter, src)
	r.bytes += n
	return n, err
}

// clientIP resolves the originating client address, preferring the proxy
// headers set by reverse proxies (leftmost X-Forwarded-For entry), then
// X-Real-IP, and finally the TCP peer address.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			xff = xff[:i]
		}
		if ip := strings.TrimSpace(xff); ip != "" {
			return ip
		}
	}
	if xrip := strings.TrimSpace(r.Header.Get("X-Real-IP")); xrip != "" {
		return xrip
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func newRequestID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(buf)
}

// logRequests emits one structured log line per received request with the
// source address, client details, route, outcome, and timing when enabled.
// It always sets X-Request-Id so callers can correlate responses even when
// request logging is disabled.
func logRequests(next http.Handler, enabled bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := newRequestID()
		w.Header().Set("X-Request-Id", requestID)

		if !enabled {
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)

		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		attrs := []any{
			"request_id", requestID,
			"method", r.Method,
			"path", r.URL.Path,
			"query", r.URL.RawQuery,
			"remote", clientIP(r),
			"user_agent", r.UserAgent(),
			"referer", r.Referer(),
			"status", rec.status,
			"bytes", rec.bytes,
			"duration_ms", time.Since(start).Milliseconds(),
			"upgraded", rec.hijacked,
			"success", rec.status == http.StatusSwitchingProtocols || (rec.status >= http.StatusOK && rec.status < http.StatusBadRequest),
		}
		if rec.status >= http.StatusBadRequest {
			slog.Warn("request failed", attrs...)
			return
		}
		slog.Info("request", attrs...)
	})
}
