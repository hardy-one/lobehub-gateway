package gatewaylog

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestParseLevel(t *testing.T) {
	tests := map[string]slog.Level{
		"":       slog.LevelInfo,
		"debug":  slog.LevelDebug,
		"INFO":   slog.LevelInfo,
		" warn ": slog.LevelWarn,
		"error":  slog.LevelError,
		"bogus":  slog.LevelInfo,
	}
	for value, want := range tests {
		if got := ParseLevel(value); got != want {
			t.Errorf("ParseLevel(%q) = %v, want %v", value, got, want)
		}
	}
}

func TestServiceLoggerPrefixAndStructuredAttrs(t *testing.T) {
	var buf bytes.Buffer
	logger := NewServiceLogger(slog.NewJSONHandler(&buf, nil), "agent")
	logger.Info("state store enabled", "path", "/data/agent.db")
	logger.Warn("degraded", "reason", "readonly")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d log lines, want 2: %s", len(lines), buf.String())
	}
	for i, want := range []string{"[agent] state store enabled", "[agent] degraded"} {
		var record map[string]any
		if err := json.Unmarshal([]byte(lines[i]), &record); err != nil {
			t.Fatalf("line %d is not JSON: %v: %s", i, err, lines[i])
		}
		if record["msg"] != want {
			t.Errorf("line %d msg = %v, want %q", i, record["msg"], want)
		}
		if record["service"] != "agent" {
			t.Errorf("line %d service = %v, want agent", i, record["service"])
		}
	}
	if !strings.Contains(lines[0], `"path":"/data/agent.db"`) {
		t.Errorf("structured attrs missing: %s", lines[0])
	}
}

func TestServiceLoggerLevelFilter(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})
	logger := NewServiceLogger(handler, "device")
	logger.Debug("hidden debug")
	logger.Info("hidden info")
	logger.Warn("visible warn")
	if strings.Contains(buf.String(), "hidden") {
		t.Fatalf("level filter not applied: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "[device] visible warn") {
		t.Fatalf("warn record missing: %s", buf.String())
	}
}

func TestPrefixHandlerIsIdempotent(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, nil)
	wrapped := NewPrefixHandler(NewPrefixHandler(base, "agent"), "agent")
	slog.New(wrapped).Info("hello")
	if got := buf.String(); !strings.Contains(got, `"msg":"[agent] hello"`) || strings.Contains(got, "[agent] [agent]") {
		t.Fatalf("prefix applied more than once: %s", got)
	}
}
