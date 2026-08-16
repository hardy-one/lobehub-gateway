package gateway

import (
	"errors"
	"os"
	"strconv"
	"time"
)

const defaultLobeAPIBaseURL = "https://app.lobehub.com"

type Config struct {
	JWKSPublicKey   string
	LobeAPIBaseURL  string
	Port            string
	ReadTimeout     time.Duration
	RequestLogging  bool
	ServiceToken    string
	ShutdownTimeout time.Duration
	WriteTimeout    time.Duration
}

func (c Config) Validate() error {
	if c.ServiceToken == "" {
		return errors.New("SERVICE_TOKEN is required")
	}
	return nil
}

func ConfigFromEnv() Config {
	return Config{
		JWKSPublicKey:   os.Getenv("JWKS_PUBLIC_KEY"),
		LobeAPIBaseURL:  envOrDefault("LOBE_API_BASE_URL", defaultLobeAPIBaseURL),
		Port:            envOrDefault("PORT", "8787"),
		ReadTimeout:     durationEnvOrDefault("READ_TIMEOUT", 30*time.Second),
		RequestLogging:  boolEnvOrDefault("REQUEST_LOG_ENABLED", false),
		ServiceToken:    os.Getenv("SERVICE_TOKEN"),
		ShutdownTimeout: durationEnvOrDefault("SHUTDOWN_TIMEOUT", 10*time.Second),
		WriteTimeout:    durationEnvOrDefault("WRITE_TIMEOUT", time.Minute),
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
