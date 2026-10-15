package config

import (
	"strings"
	"testing"
	"time"
)

func TestPlatformsFromEnvDeclaresOnlyConfiguredPlatforms(t *testing.T) {
	t.Setenv("MESSENGER_TELEGRAM_BOT_TOKEN", "123:abc")

	platforms := platformsFromEnv()

	if len(platforms) != 1 {
		t.Fatalf("expected only telegram to be declared, got %d platforms", len(platforms))
	}
	telegram := platforms[0]
	if telegram.ID != "telegram" || !telegram.Enabled {
		t.Fatalf("unexpected platform %+v", telegram)
	}
	if telegram.Credentials["botToken"] != "123:abc" {
		t.Fatalf("credential not carried: %+v", telegram.Credentials)
	}
}

// A declared platform stays declared when it is switched off — it must be
// upserted as disabled rather than dropped, so the audience for the switch
// stays in the environment.
func TestPlatformsFromEnvKeepsDisabledPlatforms(t *testing.T) {
	t.Setenv("MESSENGER_TELEGRAM_BOT_TOKEN", "123:abc")
	t.Setenv("MESSENGER_TELEGRAM_ENABLED", "0")

	platforms := platformsFromEnv()

	if len(platforms) != 1 || platforms[0].Enabled {
		t.Fatalf("expected one disabled telegram, got %+v", platforms)
	}
}

// WeChat carries no deployment credential, so its enable switch is the only
// thing that can declare it.
func TestPlatformsFromEnvDeclaresWechatFromEnableSwitch(t *testing.T) {
	t.Setenv("MESSENGER_WECHAT_ENABLED", "1")

	platforms := platformsFromEnv()

	if len(platforms) != 1 || platforms[0].ID != "wechat" {
		t.Fatalf("expected wechat to be declared, got %+v", platforms)
	}
	if len(platforms[0].Credentials) != 0 {
		t.Fatalf("wechat should carry no credentials, got %+v", platforms[0].Credentials)
	}
}

func TestPlatformsFromEnvParsesLinqNumberPool(t *testing.T) {
	t.Setenv("MESSENGER_LINQ_API_KEY", "key")
	t.Setenv("MESSENGER_LINQ_WEBHOOK_SECRET", "whsec")
	t.Setenv("MESSENGER_LINQ_NUMBERS", "+15550001111, +15550002222 +15550001111")

	platforms := platformsFromEnv()

	if len(platforms) != 1 {
		t.Fatalf("expected one platform, got %+v", platforms)
	}
	numbers, ok := platforms[0].Credentials["numbers"].([]string)
	if !ok {
		t.Fatalf("numbers should be a list, got %T", platforms[0].Credentials["numbers"])
	}
	if len(numbers) != 3 {
		t.Fatalf("expected three entries (the client de-duplicates), got %v", numbers)
	}
}

func TestValidateRejectsIncompleteDeclaration(t *testing.T) {
	cfg := Config{
		Enabled:           true,
		Interval:          time.Minute,
		LobeHubBaseURL:    "http://lobehub:3210",
		MessageGatewayURL: "http://gateway:8790",
		ServiceToken:      "token",
		Platforms: []Platform{
			{ApplicationID: "app-id", ID: "discord", Credentials: map[string]any{"botToken": "x"}},
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected discord without publicKey to be rejected")
	}
	if !strings.Contains(err.Error(), "MESSENGER_DISCORD_PUBLIC_KEY") {
		t.Fatalf("error should name the missing variable, got %v", err)
	}
}

func TestValidateSkipsEverythingWhenDisabled(t *testing.T) {
	cfg := Config{}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("a disabled oss center must not require configuration: %v", err)
	}
}

func TestValidateRequiresBothEndpoints(t *testing.T) {
	base := Config{Enabled: true, Interval: time.Minute, ServiceToken: "token"}
	if err := base.Validate(); err == nil {
		t.Fatal("expected a missing LOBE_API_BASE_URL to be rejected")
	}

	base.LobeHubBaseURL = "http://lobehub:3210"
	// A deployment of webhook platforms alone never talks to the message
	// gateway, so it must not be asked for one.
	base.Platforms = []Platform{{Credentials: map[string]any{"botToken": "y"}, ID: "telegram"}}
	if err := base.Validate(); err != nil {
		t.Fatalf("a webhook-only deployment needs no gateway: %v", err)
	}

	// A WebSocket singleton has to be registered somewhere, so it does.
	base.Platforms = []Platform{{
		ApplicationID: "app-id",
		Credentials:   map[string]any{"botToken": "x", "publicKey": "key"},
		Singleton:     true,
		ID:            "discord",
	}}
	err := base.Validate()
	if err == nil {
		t.Fatal("expected a missing MESSAGE_GATEWAY_URL to be rejected")
	}
	if !strings.Contains(err.Error(), "MESSAGE_GATEWAY_URL") || !strings.Contains(err.Error(), "discord") {
		t.Fatalf("the error should name the variable and the platform, got %v", err)
	}
}

func TestPlatformsFromEnvMarksOnlyWebsocketTransportsAsSingleton(t *testing.T) {
	t.Setenv("MESSENGER_DISCORD_APPLICATION_ID", "app-id")
	t.Setenv("MESSENGER_DISCORD_BOT_TOKEN", "x")
	t.Setenv("MESSENGER_DISCORD_PUBLIC_KEY", "key")
	t.Setenv("MESSENGER_TELEGRAM_BOT_TOKEN", "y")

	cfg := Config{Platforms: platformsFromEnv()}

	if !cfg.UsesPlatformConnections() {
		t.Fatal("discord is a websocket singleton and needs a gateway connection")
	}
	byID := map[string]Platform{}
	for _, platform := range cfg.Platforms {
		byID[platform.ID] = platform
	}
	if !byID["discord"].Singleton {
		t.Fatal("discord should be a singleton")
	}
	if byID["telegram"].Singleton {
		t.Fatal("telegram is webhook transport; LobeHub connects it on demand")
	}
}

func TestDeclaredPlatformsIsSorted(t *testing.T) {
	cfg := Config{Platforms: []Platform{{ID: "telegram"}, {ID: "discord"}}}
	got := cfg.DeclaredPlatforms()
	if len(got) != 2 || got[0] != "discord" || got[1] != "telegram" {
		t.Fatalf("expected sorted platform ids, got %v", got)
	}
}
