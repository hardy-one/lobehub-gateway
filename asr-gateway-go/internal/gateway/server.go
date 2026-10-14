package gateway

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lobehub/lobehub/apps/asr-gateway-go/internal/provider"
	"github.com/lobehub/lobehub/apps/asr-gateway-go/internal/provider/volcengine"
)

type sessionInfo struct {
	token     string
	createdAt time.Time
}

type Server struct {
	cfg       Config
	auth      *authResolver
	sessions  map[string]sessionInfo
	sessionMu sync.RWMutex
}

func NewServer(cfg Config) *Server {
	return &Server{
		cfg:      cfg,
		auth:     newAuthResolver(cfg),
		sessions: make(map[string]sessionInfo),
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("POST /webapi/asr/realtime/session", s.handleCreateSession)
	mux.HandleFunc("OPTIONS /webapi/asr/realtime/session", s.handleOptions)
	mux.HandleFunc("GET /api/v1/realtime/ws", s.handleWebSocket)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "service": "asr-gateway-go", "provider": s.cfg.Provider})
}

func (s *Server) handleOptions(w http.ResponseWriter, r *http.Request) {
	setCORSHeaders(w)
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	setCORSHeaders(w)

	sessionID := generateUUID()
	token := generateHex(16)

	s.sessionMu.Lock()
	s.sessions[sessionID] = sessionInfo{
		token:     token,
		createdAt: time.Now(),
	}
	s.sessionMu.Unlock()

	host := r.Host
	wsScheme := "ws"
	if r.Header.Get("X-Forwarded-Proto") == "https" || r.TLS != nil {
		wsScheme = "wss"
	}
	websocketURL := fmt.Sprintf("%s://%s/api/v1/realtime/ws?sessionId=%s", wsScheme, host, sessionID)

	response := map[string]any{
		"sessionId":       sessionID,
		"token":           token,
		"websocketUrl":    websocketURL,
		"protocolVersion": 1,
		"expiresAt":       time.Now().Add(30 * time.Second).UTC().Format(time.RFC3339Nano),
		"audio": map[string]any{
			"channels":        1,
			"encoding":        "pcm_s16le",
			"frameBytes":      6400,
			"frameDurationMs": 200,
			"sampleRate":      16000,
		},
		"limits": map[string]any{
			"audioIdleTimeoutMs":       5000,
			"authTimeoutMs":            5000,
			"finalTimeoutMs":           5000,
			"maxBufferedFrames":        5,
			"maxFrames":                300,
			"maxSessionMs":             60000,
			"providerConnectTimeoutMs": 5000,
			"tokenTtlMs":               30000,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		http.Error(w, "missing sessionId", http.StatusBadRequest)
		return
	}

	s.sessionMu.RLock()
	localSession, hasLocalSession := s.sessions[sessionID]
	s.sessionMu.RUnlock()

	// If no local session was created on this instance (e.g. distributed gateway),
	// check if JWKS_PUBLIC_KEY or SERVICE_TOKEN is configured to allow remote validation
	if !hasLocalSession && s.cfg.JWKSPublicKey == "" && s.cfg.ServiceToken == "" {
		http.Error(w, "invalid or expired session", http.StatusUnauthorized)
		return
	}

	clientConn, err := upgradeWebSocket(w, r)
	if err != nil {
		slog.Error("websocket upgrade failed", "error", err)
		return
	}
	defer clientConn.close()

	// Connect to selected ASR backend
	asrProv, err := s.createProvider()
	if err != nil {
		slog.Error("failed to create asr provider", "error", err)
		_ = clientConn.writeJSON(mustMarshal(map[string]any{
			"sessionId": sessionID,
			"sequence":  1,
			"type":      "session.error",
			"code":      "PROVIDER_UNAVAILABLE",
			"retryable": false,
		}))
		return
	}
	defer asrProv.Close()

	var sequence int64 = 1
	nextSeq := func() int64 {
		return atomic.AddInt64(&sequence, 1) - 1
	}

	// Listen to transcription events from upstream provider
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case ev, ok := <-asrProv.Events():
				if !ok {
					return
				}
				eventType := "transcript.partial"
				if ev.IsFinal {
					eventType = "transcript.final"
				}
				_ = clientConn.writeJSON(mustMarshal(map[string]any{
					"sessionId": sessionID,
					"sequence":  nextSeq(),
					"segmentId": ev.SegmentID,
					"text":      ev.Text,
					"type":      eventType,
				}))
			case err, ok := <-asrProv.Errors():
				if ok && err != nil {
					slog.Error("upstream asr error", "error", err)
					_ = clientConn.writeJSON(mustMarshal(map[string]any{
						"sessionId": sessionID,
						"sequence":  nextSeq(),
						"type":      "session.error",
						"code":      "PROVIDER_UNAVAILABLE",
						"retryable": true,
					}))
					return
				}
			}
		}
	}()

	// Handle messages from client
	for {
		op, payload, err := clientConn.readMessage()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				slog.Debug("client websocket closed", "error", err)
			}
			break
		}

		if op == wsTextMessage {
			var msg struct {
				Type            string `json:"type"`
				Token           string `json:"token"`
				ProtocolVersion int    `json:"protocolVersion"`
			}
			if err := json.Unmarshal(payload, &msg); err != nil {
				continue
			}

			if msg.Type == "session.auth" {
				authenticated := false
				if hasLocalSession && msg.Token == localSession.token {
					authenticated = true
				} else if _, err := s.auth.resolve(msg.Token); err == nil {
					authenticated = true
				}

				if !authenticated {
					_ = clientConn.writeJSON(mustMarshal(map[string]any{
						"sessionId": sessionID,
						"sequence":  nextSeq(),
						"type":      "session.error",
						"code":      "AUTH_FAILED",
						"retryable": false,
					}))
					break
				}

				// Start session with upstream
				if err := asrProv.StartSession(r.Context(), sessionID); err != nil {
					slog.Error("failed to start upstream asr session", "error", err)
					_ = clientConn.writeJSON(mustMarshal(map[string]any{
						"sessionId": sessionID,
						"sequence":  nextSeq(),
						"type":      "session.error",
						"code":      "PROVIDER_UNAVAILABLE",
						"retryable": true,
					}))
					break
				}

				// Send session.ready
				_ = clientConn.writeJSON(mustMarshal(map[string]any{
					"sessionId":       sessionID,
					"sequence":        nextSeq(),
					"type":            "session.ready",
					"protocolVersion": 1,
					"audio": map[string]any{
						"channels":        1,
						"encoding":        "pcm_s16le",
						"frameBytes":      6400,
						"frameDurationMs": 200,
						"sampleRate":      16000,
					},
					"limits": map[string]any{
						"maxBufferedFrames": 5,
						"maxFrames":         300,
						"maxSessionMs":      60000,
					},
				}))
			} else if msg.Type == "session.end" {
				_ = asrProv.EndSession()
			} else if msg.Type == "session.cancel" {
				_ = asrProv.Close()
				break
			}
		} else if op == wsBinaryMessage {
			// Forward raw PCM audio chunk to upstream
			if err := asrProv.SendAudio(payload); err != nil {
				slog.Error("failed to send audio to upstream", "error", err)
				break
			}
		}
	}

	_ = clientConn.writeJSON(mustMarshal(map[string]any{
		"sessionId":        sessionID,
		"sequence":         nextSeq(),
		"type":             "session.completed",
		"forwardedAudioMs": 0,
	}))

	s.sessionMu.Lock()
	delete(s.sessions, sessionID)
	s.sessionMu.Unlock()
}

func (s *Server) createProvider() (provider.ASRProvider, error) {
	switch s.cfg.Provider {
	case "volcengine", "":
		return volcengine.NewVolcengineProvider(volcengine.Config{
			APIKey:     s.cfg.VolcengineAPIKey,
			Endpoint:   s.cfg.VolcengineEndpoint,
			ResourceID: s.cfg.VolcengineResourceID,
		}), nil
	default:
		return nil, fmt.Errorf("unknown asr provider: %s", s.cfg.Provider)
	}
}

func setCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC 4122
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func generateHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
