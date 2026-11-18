package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/lobehub/lobehub/apps/agent-gateway-go/internal/gateway"
	"github.com/lobehub/lobehub/apps/gatewaylog"
)

func main() {
	gatewaylog.DefaultLevelLogger()
	log := gatewaylog.NewServiceLogger("agent")
	cfg := gateway.ConfigFromEnv()
	if err := cfg.Validate(); err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	srv := gateway.NewServer(cfg, log)

	httpServer := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      srv.Routes(),
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", httpServer.Addr)
		errCh <- httpServer.ListenAndServe()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	select {
	case <-stop:
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := httpServer.Shutdown(ctx); err != nil {
			log.Error("shutdown failed", "error", err)
		}
		if err := srv.Close(); err != nil {
			log.Error("state store close failed", "error", err)
		}
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "error", err)
			os.Exit(1)
		}
	}
	srv.Close()
}
