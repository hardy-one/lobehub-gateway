// Package lobehub speaks to LobeHub's deployment-management endpoint.
//
// oss-center never talks to the database and never encrypts anything: the
// credentials it holds are handed to LobeHub as plaintext JSON, and LobeHub's
// own model encrypts them. That keeps the `iv:authTag:ciphertext` format — and
// the ability to read it — owned by the single process that consumes it.
package lobehub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	platformsPath  = "/api/agent/system-bots"
	requestTimeout = 20 * time.Second
	maxResponse    = 64 << 10
)

// Platform is one declared provider, as LobeHub expects it.
type Platform struct {
	ApplicationID string         `json:"applicationId,omitempty"`
	Credentials   map[string]any `json:"credentials"`
	Enabled       bool           `json:"enabled"`
	Platform      string         `json:"platform"`
}

// Summary is the read shape. LobeHub deliberately never returns credentials
// here, and this client has no way to ask for them.
type Summary struct {
	ApplicationID string    `json:"applicationId"`
	Enabled       bool      `json:"enabled"`
	Platform      string    `json:"platform"`
	UpdatedAt     time.Time `json:"updatedAt"`
}

// Client is a minimal authenticated HTTP client for that endpoint.
type Client struct {
	baseURL string
	http    *http.Client
	token   string
}

func New(baseURL, serviceToken string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: requestTimeout},
		token:   serviceToken,
	}
}

// UpsertPlatform converges one platform to the declared state. Idempotent —
// LobeHub upserts keyed on the platform column — which is what lets the
// reconciler run on a timer.
func (c *Client) UpsertPlatform(ctx context.Context, platform Platform) error {
	body, err := json.Marshal(platform)
	if err != nil {
		return fmt.Errorf("encode platform %s: %w", platform.Platform, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+platformsPath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	return c.do(req, nil)
}

// ListPlatforms returns every provider the deployment currently stores.
func (c *Client) ListPlatforms(ctx context.Context) ([]Summary, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+platformsPath, nil)
	if err != nil {
		return nil, err
	}
	var payload struct {
		Platforms []Summary `json:"platforms"`
	}
	if err := c.do(req, &payload); err != nil {
		return nil, err
	}
	return payload.Platforms, nil
}

func (c *Client) do(req *http.Request, out any) error {
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", req.Method, req.URL.Path, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponse))
	if err != nil {
		return fmt.Errorf("%s %s: read body: %w", req.Method, req.URL.Path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf(
			"%s %s: status %d: %s",
			req.Method, req.URL.Path, resp.StatusCode, strings.TrimSpace(string(raw)),
		)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("%s %s: decode body: %w", req.Method, req.URL.Path, err)
	}
	return nil
}
