package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/lobehub/lobehub/apps/message-gateway-go/internal/provider"
)

// maxRequestBody caps what a single gateway API call may send. Connect
// payloads carry credentials and a webhook path, nothing bulkier.
const maxRequestBody = 1 << 20

// Server is the message gateway: it holds the platform connections LobeHub
// hands it and forwards what those platforms receive back to LobeHub.
type Server struct {
	cfg      Config
	log      *slog.Logger
	lobe     *lobeClient
	registry *registry

	cancel    context.CancelFunc
	startOnce sync.Once
	wg        sync.WaitGroup
}

func NewServer(cfg Config) *Server {
	log := slog.Default()
	lobe := newLobeClient(cfg)
	return &Server{
		cfg:      cfg,
		log:      log,
		lobe:     lobe,
		registry: newRegistry(cfg, lobe, log),
	}
}

// Start begins the background reconcile loop: it pulls the connection set
// LobeHub wants this gateway to hold, once at startup and then on an interval,
// so a gateway that restarted — or missed a push — rebuilds itself.
func (s *Server) Start() {
	s.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		s.cancel = cancel
		s.wg.Add(1)
		go s.reconcileLoop(ctx)
	})
}

// Shutdown stops the reconcile loop and every held connection.
func (s *Server) Shutdown(ctx context.Context) {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.registry.Shutdown(ctx)
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)

	mux.HandleFunc("POST /api/connections", s.withServiceAuth(s.handleConnect))
	mux.HandleFunc("DELETE /api/connections", s.withServiceAuth(s.handleDisconnectAll))
	mux.HandleFunc("DELETE /api/connections/{id}", s.withServiceAuth(s.handleDisconnect))
	mux.HandleFunc("POST /api/connections/{id}/typing", s.withServiceAuth(s.handleStartTyping))
	mux.HandleFunc("DELETE /api/connections/{id}/typing", s.withServiceAuth(s.handleStopTyping))
	mux.HandleFunc("GET /api/connections/{id}/status", s.withServiceAuth(s.handleConnectionStatus))

	mux.HandleFunc("GET /api/admin/stats", s.withServiceAuth(s.handleStats))
	mux.HandleFunc("GET /api/admin/registered-ids", s.withServiceAuth(s.handleRegisteredIDs))
	mux.HandleFunc("GET /api/admin/capabilities", s.withServiceAuth(s.handleCapabilities))

	return logRequests(mux, s.cfg.RequestLogging)
}

// withServiceAuth guards every route LobeHub calls. The credential is the same
// shared service token the other LobeHub gateways use, so one value
// authenticates the whole gateway fleet.
func (s *Server) withServiceAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.ServiceToken == "" || r.Header.Get("Authorization") != "Bearer "+s.cfg.ServiceToken {
			writeText(w, http.StatusUnauthorized, "Unauthorized")
			return
		}
		next(w, r)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":          true,
		"service":     "message-gateway-go",
		"platforms":   provider.Platforms(),
		"connections": s.registry.Stats().Total,
	})
}

func (s *Server) handleConnect(w http.ResponseWriter, r *http.Request) {
	var request ConnectRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.Config.ConnectionID == "" || request.Config.Platform == "" {
		writeText(w, http.StatusBadRequest, "connectionId and platform are required")
		return
	}

	status, err := s.registry.Connect(r.Context(), toProviderConfig(request.Config), request.Ensure)
	if err != nil {
		// Both are caller-side configuration mistakes: a platform this gateway
		// does not serve, or a transport it does not implement for one it does.
		if errors.Is(err, provider.ErrUnsupportedPlatform) || errors.Is(err, provider.ErrUnsupportedMode) {
			writeText(w, http.StatusBadRequest, err.Error())
			return
		}
		writeText(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, ConnectResponse{Status: status})
}

func (s *Server) handleDisconnectAll(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, DisconnectAllResponse{Total: s.registry.DisconnectAll(r.Context())})
}

func (s *Server) handleDisconnect(w http.ResponseWriter, r *http.Request) {
	status, held := s.registry.Disconnect(r.Context(), r.PathValue("id"))
	if !held {
		writeText(w, http.StatusNotFound, "connection not found")
		return
	}
	writeJSON(w, http.StatusOK, DisconnectResponse{Status: status})
}

func (s *Server) handleStartTyping(w http.ResponseWriter, r *http.Request) {
	var request TypingRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if err := s.registry.StartTyping(r.PathValue("id"), request.PlatformThreadID); err != nil {
		if errors.Is(err, ErrConnectionNotFound) {
			writeText(w, http.StatusNotFound, "connection not found")
			return
		}
		writeText(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleStopTyping(w http.ResponseWriter, r *http.Request) {
	var request TypingRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if err := s.registry.StopTyping(r.Context(), r.PathValue("id"), request.PlatformThreadID); err != nil {
		if errors.Is(err, ErrConnectionNotFound) {
			writeText(w, http.StatusNotFound, "connection not found")
			return
		}
		writeText(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleConnectionStatus(w http.ResponseWriter, r *http.Request) {
	status, held := s.registry.Status(r.PathValue("id"))
	if !held {
		// LobeHub reads this as "not held here" and connects it, which is
		// exactly what an unknown id needs.
		writeText(w, http.StatusNotFound, "connection not found")
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *Server) handleStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.registry.Stats())
}

func (s *Server) handleRegisteredIDs(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, RegisteredIDs{IDs: s.registry.RegisteredIDs()})
}

func (s *Server) handleCapabilities(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, GatewayCapabilities{Platforms: provider.Platforms()})
}

// reconcileLoop rebuilds this gateway's connection set from LobeHub. It runs
// without a pull credential as a no-op: a gateway that is only ever pushed to
// still works, it just cannot heal a restart by itself.
func (s *Server) reconcileLoop(ctx context.Context) {
	defer s.wg.Done()

	if s.cfg.PullToken == "" {
		s.log.Info("connection pull disabled", "reason", "PULL_TOKEN is not configured")
		return
	}

	s.pullDesiredConnections(ctx)
	if s.cfg.PullInterval <= 0 {
		return
	}

	ticker := time.NewTicker(s.cfg.PullInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.pullDesiredConnections(ctx)
		}
	}
}

func (s *Server) pullDesiredConnections(ctx context.Context) {
	pullCtx, cancel := context.WithTimeout(ctx, lobeRequestTimeout)
	defer cancel()

	desired, err := s.lobe.desiredConnections(pullCtx)
	if err != nil {
		s.log.Warn("desired-connections pull failed", "error", err)
		return
	}

	held := 0
	failed := 0
	for _, entry := range desired.Connections {
		// Connections outlive the pull: a refused or timed-out pull must not
		// cancel a link that is already being maintained.
		if _, err := s.registry.Connect(context.Background(), toProviderConfig(entry.Config), true); err != nil {
			failed++
			s.log.Warn("pull connect failed",
				"connection_id", entry.Config.ConnectionID,
				"platform", entry.Config.Platform,
				"error", err)
			continue
		}
		held++
	}

	s.log.Info("connection pull applied",
		"desired", len(desired.Connections),
		"held", held,
		"failed", failed,
		"excluded", desired.Excluded,
		"deferred", desired.Deferred,
		"complete", desired.Complete)
}

func toProviderConfig(config ConnectRequestConfig) provider.Config {
	return provider.Config{
		ApplicationID:  config.ApplicationID,
		Capabilities:   config.Capabilities,
		ConnectionID:   config.ConnectionID,
		ConnectionMode: config.ConnectionMode,
		Credentials:    config.Credentials,
		Platform:       config.Platform,
		UserID:         config.UserID,
		WebhookPath:    config.WebhookPath,
	}
}

// decodeJSON reads a bounded request body. Unknown fields are tolerated: this
// gateway follows LobeHub's payload, so a field added upstream must not turn a
// working connection into a 400.
func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err := decoder.Decode(target); err != nil {
		writeText(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeText(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(message))
}
