package gateway

import (
	"errors"
	"os"
	"strconv"
	"time"
)

const (
	defaultDiscordAPIBaseURL  = "https://discord.com/api/v10"
	defaultDiscordGatewayURL  = "wss://gateway.discord.gg"
	defaultLobeAPIBaseURL     = "https://app.lobehub.com"
	defaultSlackAPIBaseURL    = "https://slack.com/api"
	defaultTelegramAPIBaseURL = "https://api.telegram.org"
	defaultWechatBaseURL      = "https://ilinkai.weixin.qq.com"

	// defaultTypingInterval sits under the shortest platform indicator life
	// (Discord's typing lasts ~10s), so a held conversation never flickers.
	defaultTypingInterval = 8 * time.Second

	// defaultPullInterval re-pulls the desired connection set so a gateway that
	// missed a reconcile push heals itself without waiting for a restart.
	defaultPullInterval = 5 * time.Minute
)

type Config struct {
	DiscordAPIBaseURL  string
	DiscordGatewayURL  string
	LobeAPIBaseURL     string
	Port               string
	PullInterval       time.Duration
	PullToken          string
	ReadTimeout        time.Duration
	RequestLogging     bool
	ServiceToken       string
	ShutdownTimeout    time.Duration
	SlackAPIBaseURL    string
	TelegramAPIBaseURL string
	TypingInterval     time.Duration
	WechatBaseURL      string
	WriteTimeout       time.Duration
}

func (c Config) Validate() error {
	if c.ServiceToken == "" {
		return errors.New("SERVICE_TOKEN is required")
	}
	return nil
}

func ConfigFromEnv() Config {
	return Config{
		DiscordAPIBaseURL:  envOrDefault("DISCORD_API_BASE_URL", defaultDiscordAPIBaseURL),
		DiscordGatewayURL:  envOrDefault("DISCORD_GATEWAY_URL", defaultDiscordGatewayURL),
		LobeAPIBaseURL:     envOrDefault("LOBE_API_BASE_URL", defaultLobeAPIBaseURL),
		Port:               envOrDefault("PORT", "8790"),
		PullInterval:       durationEnvOrDefault("PULL_INTERVAL", defaultPullInterval),
		PullToken:          os.Getenv("PULL_TOKEN"),
		ReadTimeout:        durationEnvOrDefault("READ_TIMEOUT", 30*time.Second),
		RequestLogging:     boolEnvOrDefault("REQUEST_LOG_ENABLED", false),
		ServiceToken:       os.Getenv("SERVICE_TOKEN"),
		ShutdownTimeout:    durationEnvOrDefault("SHUTDOWN_TIMEOUT", 10*time.Second),
		SlackAPIBaseURL:    envOrDefault("SLACK_API_BASE_URL", defaultSlackAPIBaseURL),
		TelegramAPIBaseURL: envOrDefault("TELEGRAM_API_BASE_URL", defaultTelegramAPIBaseURL),
		TypingInterval:     durationEnvOrDefault("TYPING_INTERVAL", defaultTypingInterval),
		WechatBaseURL:      envOrDefault("WECHAT_BASE_URL", defaultWechatBaseURL),
		WriteTimeout:       durationEnvOrDefault("WRITE_TIMEOUT", time.Minute),
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
