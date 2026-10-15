// Package config turns oss-center's own environment into the deployment-level
// SystemBot declarations it converges LobeHub towards.
//
// Declaration is by presence: a platform is declared as soon as its credential
// variables are set, and stays declared (as disabled) when
// MESSENGER_<PLATFORM>_ENABLED is set to a falsey value. Removing a variable
// therefore means "stop managing this platform", never "delete it" — the
// reconciler reports the difference instead.
package config

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

const (
	defaultInterval = 60 * time.Second
	defaultPort     = "8791"
)

// Platform is one declared SystemBot provider.
type Platform struct {
	ApplicationID string
	Credentials   map[string]any
	Enabled       bool
	ID            string
	// Singleton marks a platform whose transport makes LobeHub keep exactly one
	// platform-wide connection (a WebSocket) — the one piece of connection
	// state a deployment has to register itself. Everything else is created on
	// demand by LobeHub.
	Singleton bool
}

// Validate reports the first field this platform cannot be provisioned without.
func (p Platform) Validate() error {
	spec, ok := specByID(p.ID)
	if !ok {
		return fmt.Errorf("unknown platform %q", p.ID)
	}
	var missing []string
	if spec.applicationIDEnv != "" && p.ApplicationID == "" {
		missing = append(missing, spec.env(spec.applicationIDEnv))
	}
	for _, field := range spec.required {
		if _, ok := p.Credentials[field]; !ok {
			missing = append(missing, spec.envFor(field))
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("%s is declared but incomplete, missing: %s", p.ID, strings.Join(missing, ", "))
	}
	return nil
}

// Config is oss-center's whole input.
type Config struct {
	Enabled           bool
	Interval          time.Duration
	LobeHubBaseURL    string
	MessageGatewayURL string
	Oneshot           bool
	Platforms         []Platform
	Port              string
	ServiceToken      string
}

// ConfigFromEnv reads the environment. Absent or malformed values are left for
// Validate to reject, so an operator gets one exhaustive error at startup
// instead of a process that half-starts.
func ConfigFromEnv() Config {
	return Config{
		Enabled:           envBool("OSS_CENTER_ENABLED", false),
		Interval:          envDuration("OSS_CENTER_INTERVAL", defaultInterval),
		LobeHubBaseURL:    strings.TrimRight(os.Getenv("LOBE_API_BASE_URL"), "/"),
		MessageGatewayURL: strings.TrimRight(os.Getenv("MESSAGE_GATEWAY_URL"), "/"),
		Oneshot:           envBool("OSS_CENTER_ONESHOT", false),
		Platforms:         platformsFromEnv(),
		Port:              envOrDefault("OSS_CENTER_PORT", defaultPort),
		ServiceToken:      os.Getenv("SERVICE_TOKEN"),
	}
}

// Validate fails on anything that would make the first reconcile impossible:
// the two endpoints it talks to, the token both of them authenticate with, and
// every declared platform's required fields.
func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.LobeHubBaseURL == "" {
		return fmt.Errorf("LOBE_API_BASE_URL is required")
	}
	// Only a WebSocket singleton needs the gateway; a deployment running purely
	// webhook platforms talks to LobeHub and the platform APIs alone.
	if c.UsesPlatformConnections() && c.MessageGatewayURL == "" {
		return fmt.Errorf("MESSAGE_GATEWAY_URL is required for %s", strings.Join(c.singletonPlatforms(), ", "))
	}
	if c.ServiceToken == "" {
		return fmt.Errorf("SERVICE_TOKEN is required")
	}
	if c.Interval < time.Second {
		return fmt.Errorf("OSS_CENTER_INTERVAL must be at least 1s, got %s", c.Interval)
	}
	for _, platform := range c.Platforms {
		if err := platform.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// DeclaredPlatforms lists the platform ids oss-center owns.
func (c Config) DeclaredPlatforms() []string {
	ids := make([]string, 0, len(c.Platforms))
	for _, platform := range c.Platforms {
		ids = append(ids, platform.ID)
	}
	sort.Strings(ids)
	return ids
}

// UsesPlatformConnections reports whether any declared platform needs a
// platform-level gateway connection.
func (c Config) UsesPlatformConnections() bool {
	return len(c.singletonPlatforms()) > 0
}

func (c Config) singletonPlatforms() []string {
	var ids []string
	for _, platform := range c.Platforms {
		if platform.Singleton {
			ids = append(ids, platform.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

// --------------------------- platform declarations ---------------------------

// platformSpec maps one platform's environment onto its provider row. Only the
// fields LobeHub's runtime actually reads are listed: `settings` and
// `connectionMode` are deliberately absent, because nothing in the open-source
// runtime consumes them and offering them would fake a knob that does nothing.
type platformSpec struct {
	id string
	// applicationIDEnv is the env var holding the public app identifier, and
	// also the credential field name it maps to when non-empty.
	applicationIDEnv string
	// credentialEnvs maps env var suffix → credentials field name.
	credentialEnvs map[string]string
	// declaredByEnabledOnly marks platforms whose row is a switch rather than a
	// credential bundle (WeChat).
	declaredByEnabledOnly bool
	// listFields are credential fields parsed as comma/whitespace lists.
	listFields map[string]bool
	// required are the credentials fields a usable row must carry.
	required []string
	// transport mirrors LobeHub's `messenger/platforms/<id>/definition.ts`:
	// the SystemBot transport is fixed per platform, not per deployment. Only
	// `websocket` needs a connection registered from outside — LobeHub creates
	// webhook typing shards on demand and drives the polling long polls itself.
	transport string
}

var platformSpecs = []platformSpec{
	{
		applicationIDEnv: "APPLICATION_ID",
		credentialEnvs: map[string]string{
			"BOT_TOKEN":     "botToken",
			"CLIENT_SECRET": "clientSecret",
			"PUBLIC_KEY":    "publicKey",
		},
		id:        "discord",
		required:  []string{"botToken", "publicKey"},
		transport: "websocket",
	},
	{
		credentialEnvs: map[string]string{
			"BOT_TOKEN":      "botToken",
			"BOT_USERNAME":   "botUsername",
			"WEBHOOK_SECRET": "webhookSecret",
		},
		id:        "telegram",
		required:  []string{"botToken"},
		transport: "webhook",
	},
	{
		applicationIDEnv: "APP_ID",
		credentialEnvs: map[string]string{
			"CLIENT_ID":      "clientId",
			"CLIENT_SECRET":  "clientSecret",
			"SIGNING_SECRET": "signingSecret",
		},
		id:        "slack",
		required:  []string{"clientId", "clientSecret", "signingSecret"},
		transport: "webhook",
	},
	{
		// WeChat owns no deployment credential: the iLink bot token is issued
		// per user by QR scan and stored on that user's account link. The row is
		// only an availability switch, so MESSENGER_WECHAT_ENABLED declares it.
		declaredByEnabledOnly: true,
		id:                    "wechat",
		transport:             "polling",
	},
	{
		credentialEnvs: map[string]string{
			"API_BASE_URL":   "apiBaseUrl",
			"API_KEY":        "apiKey",
			"NUMBERS":        "numbers",
			"WEBHOOK_SECRET": "webhookSecret",
		},
		id:         "linq",
		listFields: map[string]bool{"numbers": true},
		required:   []string{"apiKey", "webhookSecret", "numbers"},
		transport:  "webhook",
	},
}

func specByID(id string) (platformSpec, bool) {
	for _, spec := range platformSpecs {
		if spec.id == id {
			return spec, true
		}
	}
	return platformSpec{}, false
}

func (s platformSpec) env(suffix string) string {
	return "MESSENGER_" + strings.ToUpper(s.id) + "_" + suffix
}

func (s platformSpec) enabledEnv() string { return s.env("ENABLED") }

// envFor names the variable behind a credentials field, so a startup error
// points at the exact thing an operator has to set.
func (s platformSpec) envFor(field string) string {
	for envSuffix, name := range s.credentialEnvs {
		if name == field {
			return s.env(envSuffix)
		}
	}
	return s.env(strings.ToUpper(field))
}

func platformsFromEnv() []Platform {
	var platforms []Platform
	for _, spec := range platformSpecs {
		credentials := map[string]any{}
		for envSuffix, field := range spec.credentialEnvs {
			value := strings.TrimSpace(os.Getenv(spec.env(envSuffix)))
			if value == "" {
				continue
			}
			if spec.listFields[field] {
				credentials[field] = splitList(value)
				continue
			}
			credentials[field] = value
		}

		applicationID := ""
		if spec.applicationIDEnv != "" {
			applicationID = strings.TrimSpace(os.Getenv(spec.env(spec.applicationIDEnv)))
		}

		declared := len(credentials) > 0 || applicationID != ""
		if spec.declaredByEnabledOnly {
			declared = os.Getenv(spec.enabledEnv()) != ""
		}
		if !declared {
			continue
		}

		platforms = append(platforms, Platform{
			ApplicationID: applicationID,
			Credentials:   credentials,
			Enabled:       envBool(spec.enabledEnv(), true),
			ID:            spec.id,
			Singleton:     spec.transport == "websocket",
		})
	}
	return platforms
}

// -------------------------------- env readers --------------------------------

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "":
		return fallback
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

func envDuration(key string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0
	}
	return parsed
}

func splitList(raw string) []string {
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n'
	})
	values := make([]string, 0, len(fields))
	for _, field := range fields {
		if trimmed := strings.TrimSpace(field); trimmed != "" {
			values = append(values, trimmed)
		}
	}
	return values
}
