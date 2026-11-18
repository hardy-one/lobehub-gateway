package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	agentgateway "github.com/lobehub/lobehub/apps/agent-gateway-go"
	asrgateway "github.com/lobehub/lobehub/apps/asr-gateway-go"
	devicegateway "github.com/lobehub/lobehub/apps/device-gateway-go"
	"github.com/lobehub/lobehub/apps/gatewaylog"
	messagegateway "github.com/lobehub/lobehub/apps/message-gateway-go"
	osscenter "github.com/lobehub/lobehub/apps/oss-center-go"
)

type service struct {
	name     string
	logger   *slog.Logger
	addr     string
	listen   func() error
	shutdown func(context.Context) error
}

type serviceResult struct {
	name string
	err  error
}

type gatewayConfigs struct {
	agent     agentgateway.Config
	device    devicegateway.Config
	asr       *asrgateway.Config
	message   messagegateway.Config
	ossCenter *osscenter.Config
}

func main() {
	os.Exit(run())
}

func run() int {
	handler := gatewaylog.DefaultLevelLogger()
	bootLog := slog.New(handler)
	configs, err := configsFromEnv()
	if err != nil {
		bootLog.Error("invalid configuration", "error", err)
		return 1
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)

	return runServices(newServices(configs), bootLog, stop, configs.agent.ShutdownTimeout)
}

func configsFromEnv() (gatewayConfigs, error) {
	// The unified binary ignores PORT: each service gets its own variable so
	// the listeners cannot collide in a single process.
	agentCfg := agentgateway.ConfigFromEnv()
	agentCfg.Port = envOrDefault("AGENT_PORT", "8787")
	agentCfg.StateDBPath = envOrDefault("GATEWAY_STATE_DB", agentCfg.StateDBPath)
	if err := agentCfg.Validate(); err != nil {
		return gatewayConfigs{}, errors.New("invalid agent configuration: " + err.Error())
	}

	deviceCfg := devicegateway.ConfigFromEnv()
	deviceCfg.Port = envOrDefault("DEVICE_PORT", "8788")
	if err := deviceCfg.Validate(); err != nil {
		return gatewayConfigs{}, errors.New("invalid device configuration: " + err.Error())
	}

	var asrCfg *asrgateway.Config
	// ASR gateway is enabled if ASR_ENABLED=true or an ASR API key is provided
	if os.Getenv("ASR_ENABLED") == "true" || os.Getenv("VOLCENGINE_VOICE_API_KEY") != "" {
		cfg := asrgateway.ConfigFromEnv()
		cfg.Port = envOrDefault("ASR_PORT", "8789")
		if err := cfg.Validate(); err != nil {
			return gatewayConfigs{}, errors.New("invalid asr configuration: " + err.Error())
		}
		asrCfg = &cfg
	}

	// The message gateway holds no provider credentials of its own: LobeHub
	// sends every connection's credentials with the connection, so the service
	// always starts.
	messageCfg := messagegateway.ConfigFromEnv()
	messageCfg.Port = envOrDefault("MESSAGE_PORT", "8790")
	if err := messageCfg.Validate(); err != nil {
		return gatewayConfigs{}, errors.New("invalid message configuration: " + err.Error())
	}

	// oss-center writes deployment-level state (SystemBot providers) rather than
	// serving traffic, so it only starts when a deployment asks for it. Its
	// configuration is validated here, at startup, rather than at first use.
	var ossCenterCfg *osscenter.Config
	if ossCenter := osscenter.ConfigFromEnv(); ossCenter.Enabled {
		if err := ossCenter.Validate(); err != nil {
			return gatewayConfigs{}, errors.New("invalid oss center configuration: " + err.Error())
		}
		ossCenterCfg = &ossCenter
	}

	return gatewayConfigs{
		agent:     agentCfg,
		asr:       asrCfg,
		device:    deviceCfg,
		message:   messageCfg,
		ossCenter: ossCenterCfg,
	}, nil
}

func newServices(configs gatewayConfigs) []service {
	agentLog := gatewaylog.NewServiceLogger("agent")
	deviceLog := gatewaylog.NewServiceLogger("device")
	asrLog := gatewaylog.NewServiceLogger("asr")
	messageLog := gatewaylog.NewServiceLogger("message")
	ossLog := gatewaylog.NewServiceLogger("oss")
	// The agent gateway owns the operation state store, so its listener alone
	// is not the whole service: after the HTTP server stops accepting requests,
	// Close must flush pending event counters and close SQLite. Without this
	// hook a SIGTERM drops the last counter batch and the next process restarts
	// event numbering from a stale value.
	agentServer := agentgateway.NewServer(configs.agent, agentLog)
	agentService := newService(
		"agent gateway",
		agentLog,
		configs.agent.Port,
		agentServer.Routes(),
		configs.agent.ReadTimeout,
		configs.agent.WriteTimeout,
	)
	shutdownAgentHTTP := agentService.shutdown
	agentService.shutdown = func(ctx context.Context) error {
		httpErr := shutdownAgentHTTP(ctx)
		if err := agentServer.Close(); err != nil {
			return err
		}
		return httpErr
	}

	svcs := []service{
		agentService,
		newService(
			"device gateway",
			deviceLog,
			configs.device.Port,
			devicegateway.NewServer(configs.device, deviceLog).Routes(),
			configs.device.ReadTimeout,
			configs.device.WriteTimeout,
		),
	}

	if configs.asr != nil {
		svcs = append(svcs, newService(
			"asr gateway",
			asrLog,
			configs.asr.Port,
			asrgateway.NewServer(*configs.asr, asrLog).Routes(),
			configs.asr.ReadTimeout,
			configs.asr.WriteTimeout,
		))
	}

	// The message gateway owns background work (held connections and the
	// connection pull), so its shutdown has to stop that before the process
	// exits — the HTTP server alone is not the whole service.
	messageServer := messagegateway.NewServer(configs.message, messageLog)
	messageServer.Start()
	messageService := newService(
		"message gateway",
		messageLog,
		configs.message.Port,
		messageServer.Routes(),
		configs.message.ReadTimeout,
		configs.message.WriteTimeout,
	)
	shutdownHTTP := messageService.shutdown
	messageService.shutdown = func(ctx context.Context) error {
		messageServer.Shutdown(ctx)
		return shutdownHTTP(ctx)
	}
	svcs = append(svcs, messageService)

	// oss-center owns a background convergence loop, so — like the message
	// gateway above — its shutdown has to stop that loop, not just the listener.
	if configs.ossCenter != nil {
		ossCenter := osscenter.NewComponent(*configs.ossCenter, ossLog)
		ossCenter.Start(configs.ossCenter.Oneshot)
		ossCenterService := newService(
			"oss center",
			ossLog,
			configs.ossCenter.Port,
			ossCenter.Routes(),
			configs.message.ReadTimeout,
			configs.message.WriteTimeout,
		)
		shutdownOssCenterHTTP := ossCenterService.shutdown
		ossCenterService.shutdown = func(ctx context.Context) error {
			ossCenter.Shutdown(ctx)
			return shutdownOssCenterHTTP(ctx)
		}
		svcs = append(svcs, ossCenterService)
	}

	return svcs
}

func newService(name string, log *slog.Logger, port string, handler http.Handler, readTimeout, writeTimeout time.Duration) service {
	server := &http.Server{
		Addr:         ":" + port,
		Handler:      handler,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
	}
	return service{
		name:     name,
		logger:   log,
		addr:     server.Addr,
		listen:   server.ListenAndServe,
		shutdown: server.Shutdown,
	}
}

func runServices(services []service, bootLog *slog.Logger, stop <-chan os.Signal, shutdownTimeout time.Duration) int {
	errCh := make(chan serviceResult, len(services))
	for _, svc := range services {
		go func() {
			serviceLog(services, svc.name).Info("listening", "addr", svc.addr)
			errCh <- serviceResult{name: svc.name, err: svc.listen()}
		}()
	}

	exitCode := 0
	select {
	case <-stop:
	case result := <-errCh:
		if result.err != nil && !errors.Is(result.err, http.ErrServerClosed) {
			serviceLog(services, result.name).Error("failed", "error", result.err)
			exitCode = 1
		}
	}

	if shutdownServices(services, bootLog, shutdownTimeout) {
		exitCode = 1
	}
	return exitCode
}

func shutdownServices(services []service, bootLog *slog.Logger, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	results := make(chan serviceResult, len(services))
	for _, svc := range services {
		go func() {
			results <- serviceResult{name: svc.name, err: svc.shutdown(ctx)}
		}()
	}

	failed := false
	for range services {
		select {
		case result := <-results:
			if result.err != nil {
				serviceLog(services, result.name).Error("shutdown failed", "error", result.err)
				failed = true
			}
		case <-ctx.Done():
			fallbackLog(bootLog).Error("gateway shutdown timed out", "error", ctx.Err())
			return true
		}
	}
	return failed
}

func serviceLog(services []service, name string) *slog.Logger {
	fallback := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: gatewaylog.LevelFromEnv()}))
	for _, svc := range services {
		if svc.name == name {
			if svc.logger == nil {
				return fallback
			}
			return svc.logger
		}
	}
	return fallback
}

func fallbackLog(log *slog.Logger) *slog.Logger {
	if log == nil {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: gatewaylog.LevelFromEnv()}))
	}
	return log
}

func envOrDefault(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
