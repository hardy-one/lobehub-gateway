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
	logger := NewServiceLoggerWithHandler(slog.NewJSONHandler(&buf, nil), "agent")
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
	logger := NewServiceLoggerWithHandler(handler, "device")
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

// Text is the default format: the tag belongs at the start of the line, not
// buried inside msg, so `docker logs | grep '\[agent\]'` matches cleanly.
func TestLinePrefixWriterTagsEveryLine(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(
		&linePrefixWriter{writer: &buf, prefix: "[agent] "},
		nil,
	))
	logger.Info("state store enabled", "path", "/data/agent.db")
	logger.Warn("replay gap", "clientLastEventId", "47")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("got %d lines, want 2: %q", len(lines), buf.String())
	}
	for _, line := range lines {
		if !strings.HasPrefix(line, "[agent] ") {
			t.Errorf("line does not start with the service tag: %q", line)
		}
	}
	if !strings.Contains(lines[0], `msg="state store enabled"`) {
		t.Errorf("message not preserved: %q", lines[0])
	}
	if !strings.Contains(lines[0], "path=/data/agent.db") {
		t.Errorf("attributes missing: %q", lines[0])
	}
}

func TestLinePrefixWriterLeavesBlankLinesAlone(t *testing.T) {
	var buf bytes.Buffer
	writer := &linePrefixWriter{writer: &buf, prefix: "[device] "}
	if _, err := writer.Write([]byte("\nhello\n")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if got, want := buf.String(), "\n[device] hello\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestServiceLoggerHonoursJSONFormat(t *testing.T) {
	t.Setenv("LOG_FORMAT", "json")
	var buf bytes.Buffer
	logger := NewServiceLoggerWithHandler(slog.NewJSONHandler(&buf, nil), "agent")
	logger.Info("hello")
	if !strings.Contains(buf.String(), `"service":"agent"`) {
		t.Fatalf("json mode should keep the service attribute: %s", buf.String())
	}
}
