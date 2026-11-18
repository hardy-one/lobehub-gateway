// Package gatewaylog builds the shared structured logger used by every
// gateway service and entry point.
package gatewaylog

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

var levelNames = map[string]slog.Level{
	"debug": slog.LevelDebug,
	"info":  slog.LevelInfo,
	"warn":  slog.LevelWarn,
	"error": slog.LevelError,
}

// ParseLevel reads LOG_LEVEL, defaulting to info for unset, empty or invalid
// values. Level names are case-insensitive and may carry surrounding spaces.
func ParseLevel(value string) slog.Level {
	if level, ok := levelNames[strings.ToLower(strings.TrimSpace(value))]; ok {
		return level
	}
	return slog.LevelInfo
}

// LevelFromEnv returns the LOG_LEVEL configured for the process.
func LevelFromEnv() slog.Level { return ParseLevel(os.Getenv("LOG_LEVEL")) }

// isJSONFormat reports whether LOG_FORMAT=json was requested. The default is
// line-oriented text, which is what `docker logs` and a terminal read best.
func isJSONFormat() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("LOG_FORMAT")), "json")
}

// linePrefixWriter writes prefix at the start of every line it forwards, so a
// TextHandler's records begin with the service tag instead of burying it in
// the message body.
type linePrefixWriter struct {
	writer io.Writer
	prefix string
}

func (w *linePrefixWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	var buf bytes.Buffer
	buf.Grow(len(p) + len(w.prefix)*2)
	rest := p
	for len(rest) > 0 {
		newline := bytes.IndexByte(rest, '\n')
		var line []byte
		if newline < 0 {
			line, rest = rest, nil
		} else {
			line, rest = rest[:newline+1], rest[newline+1:]
		}
		if len(bytes.TrimSpace(line)) > 0 {
			buf.WriteString(w.prefix)
		}
		buf.Write(line)
	}
	written, err := w.writer.Write(buf.Bytes())
	if err != nil {
		return 0, err
	}
	if written < buf.Len() {
		return 0, io.ErrShortWrite
	}
	return len(p), nil
}

// NewBaseHandler returns the level-filtered handler written to stderr. Text is
// the default; LOG_FORMAT=json switches to structured JSON lines.
func NewBaseHandler() slog.Handler {
	options := &slog.HandlerOptions{Level: LevelFromEnv()}
	if isJSONFormat() {
		return slog.NewJSONHandler(os.Stderr, options)
	}
	return slog.NewTextHandler(os.Stderr, options)
}

// PrefixHandler prepends "[name] " to every log message while keeping the
// handler's structured attributes and level filtering intact.
type PrefixHandler struct {
	slog.Handler
	prefix string
}

// NewPrefixHandler wraps handler so its records carry the service prefix.
func NewPrefixHandler(handler slog.Handler, name string) *PrefixHandler {
	return NewPrefixHandlerWithAttr(handler, name, "")
}

// NewPrefixHandlerWithAttr wraps handler with a service prefix and optionally
// adds a constant structured attribute (for example service=gateway) to every
// record. The prefix lives in the handler, not in individual log messages.
func NewPrefixHandlerWithAttr(handler slog.Handler, name string, attrKey string) *PrefixHandler {
	if handler == nil {
		handler = slog.Default().Handler()
	}
	prefix := "[" + name + "] "
	// If the handler already carries this exact prefix, reuse it so wrapping
	// a service logger twice cannot produce "[agent] [agent]".
	if existing, ok := handler.(*PrefixHandler); ok && existing.prefix == prefix {
		return existing
	}
	if attrKey != "" {
		handler = &attrHandler{Handler: handler, key: attrKey, value: name}
	}
	return &PrefixHandler{Handler: handler, prefix: prefix}
}

func (h *PrefixHandler) Handle(ctx context.Context, record slog.Record) error {
	record.Message = h.prefix + record.Message
	return h.Handler.Handle(ctx, record)
}

func (h *PrefixHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &PrefixHandler{Handler: h.Handler.WithAttrs(attrs), prefix: h.prefix}
}

func (h *PrefixHandler) WithGroup(name string) slog.Handler {
	return &PrefixHandler{Handler: h.Handler.WithGroup(name), prefix: h.prefix}
}

// attrHandler adds a single constant attribute to every record passing
// through it, including records emitted by handler children.
type attrHandler struct {
	slog.Handler
	key   string
	value string
}

func (h *attrHandler) Handle(ctx context.Context, record slog.Record) error {
	record.AddAttrs(slog.String(h.key, h.value))
	return h.Handler.Handle(ctx, record)
}

func (h *attrHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &attrHandler{Handler: h.Handler.WithAttrs(attrs), key: h.key, value: h.value}
}

func (h *attrHandler) WithGroup(name string) slog.Handler {
	return &attrHandler{Handler: h.Handler.WithGroup(name), key: h.key, value: h.value}
}

// NewServiceLogger returns a logger for one gateway service. Text output, the
// default, starts every line with "[name] " so a single `docker logs` stream
// can be filtered per service. LOG_FORMAT=json switches to structured records
// that carry the tag inside msg plus a service attribute.
func NewServiceLogger(name string) *slog.Logger {
	options := &slog.HandlerOptions{Level: LevelFromEnv()}
	if isJSONFormat() {
		return slog.New(NewPrefixHandlerWithAttr(slog.NewJSONHandler(os.Stderr, options), name, "service"))
	}
	return slog.New(slog.NewTextHandler(
		&linePrefixWriter{writer: os.Stderr, prefix: "[" + name + "] "},
		options,
	))
}

// NewServiceLoggerWithHandler derives a service logger from a caller-supplied
// handler, which is how tests capture output.
func NewServiceLoggerWithHandler(handler slog.Handler, name string) *slog.Logger {
	if handler == nil {
		handler = NewBaseHandler()
	}
	return slog.New(NewPrefixHandlerWithAttr(handler, name, "service"))
}

// DefaultLevelLogger installs the LOG_LEVEL-filtered base handler as the
// process default, replacing the default slog handler. Service loggers should
// be derived from the returned handler so every service shares the filter.
func DefaultLevelLogger() slog.Handler {
	handler := NewBaseHandler()
	slog.SetDefault(slog.New(handler))
	return handler
}

// DiscardLogger is a quiet logger used when a caller does not supply one.
func DiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
