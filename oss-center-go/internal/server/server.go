// Package server exposes oss-center's read-only status surface.
//
// There is deliberately no management UI: the declared state *is* the
// environment, and the only question an operator has about a deployment-side
// provisioner is whether it converged. Both answers fit in one JSON document.
package server

import (
	"encoding/json"
	"net/http"

	"github.com/lobehub/lobehub/apps/oss-center-go/internal/reconcile"
)

type Server struct {
	declared   []string
	reconciler *reconcile.Reconciler
}

func New(reconciler *reconcile.Reconciler, declared []string) *Server {
	if declared == nil {
		declared = []string{}
	}
	return &Server{declared: declared, reconciler: reconciler}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /status", s.handleStatus)
	return mux
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	last := s.reconciler.Last()
	if last == nil {
		// No pass has finished yet. Reporting unhealthy here would fail a
		// container's very first health check for no reason.
		writeJSON(w, http.StatusOK, map[string]any{"status": "pending"})
		return
	}
	if last.Error != "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":      last.Error,
			"finishedAt": last.FinishedAt,
			"status":     "error",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"finishedAt": last.FinishedAt,
		"status":     "ok",
	})
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"declared": s.declared,
		"lastRun":  s.reconciler.Last(),
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
