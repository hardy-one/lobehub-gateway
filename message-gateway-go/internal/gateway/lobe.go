package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

const (
	callbackPath           = "/api/agent/gateway/callback"
	desiredConnectionsPath = "/api/agent/gateway/desired-connections"

	lobeRequestTimeout = 15 * time.Second
)

// lobeClient talks back to the LobeHub server: it forwards inbound platform
// events to the connection's webhook, reports connection state changes, and
// pulls the connection set this gateway should hold.
type lobeClient struct {
	baseURL      string
	httpClient   *http.Client
	pullToken    string
	serviceToken string
}

func newLobeClient(cfg Config) *lobeClient {
	return &lobeClient{
		baseURL:      strings.TrimRight(cfg.LobeAPIBaseURL, "/"),
		httpClient:   &http.Client{Timeout: lobeRequestTimeout},
		pullToken:    cfg.PullToken,
		serviceToken: cfg.ServiceToken,
	}
}

// forward delivers one raw platform event to the connection's webhook path.
// The platform headers the provider supplies are what the LobeHub-side handler
// authenticates against (WeChat uses the service token, Discord the bot token).
func (c *lobeClient) forward(ctx context.Context, webhookPath string, body []byte, headers map[string]string) error {
	if webhookPath == "" {
		return errors.New("connection has no webhookPath")
	}
	if c.baseURL == "" {
		return errors.New("LOBE_API_BASE_URL is not configured")
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+webhookPath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	for key, value := range headers {
		request.Header.Set(key, value)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		payload, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		return fmt.Errorf("webhook forward failed (%d): %s", response.StatusCode, strings.TrimSpace(string(payload)))
	}
	return nil
}

// report publishes one connection state change. LobeHub ignores callbacks that
// carry no applicationId, so callers skip those rather than spend a request.
func (c *lobeClient) report(ctx context.Context, callback StateCallback) error {
	if c.serviceToken == "" {
		return errors.New("SERVICE_TOKEN is not configured")
	}
	return c.postJSON(ctx, callbackPath, c.serviceToken, callback)
}

// desiredConnections pulls the connect payloads this gateway should be
// holding. LobeHub authenticates this route with a credential dedicated to
// pull-capable hosts, not the shared service token.
func (c *lobeClient) desiredConnections(ctx context.Context) (*DesiredConnections, error) {
	if c.pullToken == "" {
		return nil, errors.New("PULL_TOKEN is not configured")
	}

	body, err := c.postJSONRaw(ctx, desiredConnectionsPath, c.pullToken, struct{}{})
	if err != nil {
		return nil, err
	}

	var desired DesiredConnections
	if err := json.Unmarshal(body, &desired); err != nil {
		return nil, fmt.Errorf("invalid desired-connections response: %w", err)
	}
	return &desired, nil
}

func (c *lobeClient) postJSON(ctx context.Context, path string, token string, payload any) error {
	_, err := c.postJSONRaw(ctx, path, token, payload)
	return err
}

func (c *lobeClient) postJSONRaw(ctx context.Context, path string, token string, payload any) ([]byte, error) {
	if c.baseURL == "" {
		return nil, errors.New("LOBE_API_BASE_URL is not configured")
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		// LobeHub answers 204 for callbacks it has no work for; every other
		// non-2xx is a real failure the operator needs to see.
		slog.Debug("lobe request failed", "path", path, "status", response.StatusCode, "body", strings.TrimSpace(string(responseBody)))
		return nil, fmt.Errorf("%s failed (%d): %s", path, response.StatusCode, strings.TrimSpace(string(responseBody)))
	}
	return responseBody, nil
}
