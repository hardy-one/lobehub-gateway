package gateway

import (
	"errors"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port            string
	Provider        string // e.g. "volcengine" (default)
	ReadTimeout     time.Duration
	RequestLogging  bool
	ShutdownTimeout time.Duration
	WriteTimeout    time.Duration

	// Volcengine specific
	VolcengineAPIKey     string
	VolcengineEndpoint   string
	VolcengineResourceID string
}

func (c Config) Validate() error {
	switch c.Provider {
	case "volcengine", "":
		if c.VolcengineAPIKey == "" {
			return errors.New("VOLCENGINE_VOICE_API_KEY is required when ASR_PROVIDER is volcengine")
		}
	default:
		return errors.New("unsupported ASR_PROVIDER: " + c.Provider)
	}
	return nil
}

func ConfigFromEnv() Config {
	return Config{
		Port:                 envOrDefault("ASR_PORT", "8789"),
		Provider:             envOrDefault("ASR_PROVIDER", "volcengine"),
		ReadTimeout:          durationEnvOrDefault("READ_TIMEOUT", 30*time.Second),
		RequestLogging:       boolEnvOrDefault("REQUEST_LOG_ENABLED", false),
		ShutdownTimeout:      durationEnvOrDefault("SHUTDOWN_TIMEOUT", 10*time.Second),
		WriteTimeout:         durationEnvOrDefault("WRITE_TIMEOUT", time.Minute),
		VolcengineAPIKey:     os.Getenv("VOLCENGINE_VOICE_API_KEY"),
		VolcengineEndpoint:   os.Getenv("VOLCENGINE_ENDPOINT"),
		VolcengineResourceID: os.Getenv("VOLCENGINE_RESOURCE_ID"),
	}
}

func envOrDefault(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func durationEnvOrDefault(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func boolEnvOrDefault(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
