// Package messagegw is the client for the message gateway's connection
// contract: register/refresh a connection, drop one, and read its state.
//
// oss-center only ever manages *platform-level* connections — the WebSocket
// singletons a platform-wide SystemBot needs. Per-user connections (webhook
// typing shards, WeChat long polls) are LobeHub's to create and destroy. The
// two are told apart by their connection id, and LobeHub's own reconcile
// deliberately never touches anything it did not create.
package messagegw

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	connectionsPath = "/api/connections"
	requestTimeout  = 20 * time.Second
	maxResponse     = 64 << 10
)

// ErrConnectionNotHeld means the gateway holds no such connection. Callers
// reaching for a desired end state should treat it as success.
var ErrConnectionNotHeld = errors.New("connection not held")

// Capabilities are the edge-filtering flags LobeHub sends per connection.
type Capabilities struct {
	MessageMonitoring MessageMonitoring `json:"messageMonitoring"`
}

// MessageMonitoring gates whether the gateway may drop ordinary channel
// messages. A Messenger connection never consumes passive monitoring — the
// shared bot answers DMs and explicit mentions only — so it is always false
// here, mirroring what LobeHub sends for its own messenger connections.
type MessageMonitoring struct {
	Enabled bool `json:"enabled"`
}

// ConnectionConfig is the payload for one connection.
type ConnectionConfig struct {
	ApplicationID  string         `json:"applicationId,omitempty"`
	Capabilities   Capabilities   `json:"capabilities"`
	ConnectionID   string         `json:"connectionId"`
	ConnectionMode string         `json:"connectionMode"`
	Credentials    map[string]any `json:"credentials"`
	Platform       string         `json:"platform"`
	UserID         string         `json:"userId,omitempty"`
	WebhookPath    string         `json:"webhookPath"`
}

type connectRequest struct {
	Config ConnectionConfig `json:"config"`
	Ensure bool             `json:"ensure"`
}

// ConnectionStatus is the reply to a status read. The shape mirrors the
// gateway's own contract so nothing has to be reinterpreted on the way in.
type ConnectionStatus struct {
	Config *struct {
		ConnectionID string `json:"connectionId"`
		Platform     string `json:"platform"`
	} `json:"config"`
	State struct {
		ConnectedAt int64  `json:"connectedAt"`
		Error       string `json:"error"`
		ErrorCode   string `json:"errorCode"`
		Platform    string `json:"platform"`
		Status      string `json:"status"`
	} `json:"state"`
}

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

// Ensure registers a connection, or refreshes one whose configuration changed.
//
// Safe to call on a timer: the gateway leaves an identical, live connection
// alone, so a steady-state pass costs one round trip and never churns the
// platform link — and a connection lost to a gateway restart comes back on the
// next pass, which is the only recovery path for the Messenger singleton.
func (c *Client) Ensure(ctx context.Context, config ConnectionConfig) error {
	body, err := json.Marshal(connectRequest{Config: config, Ensure: true})
	if err != nil {
		return fmt.Errorf("encode connection %s: %w", config.ConnectionID, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+connectionsPath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	return c.do(req, nil)
}

// Disconnect drops a connection. One the gateway does not hold is the desired
// end state, so it reads as success rather than 404.
func (c *Client) Disconnect(ctx context.Context, connectionID string) error {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		c.baseURL+connectionsPath+"/"+url.PathEscape(connectionID),
		nil,
	)
	if err != nil {
		return err
	}
	err = c.do(req, nil)
	if errors.Is(err, ErrConnectionNotHeld) {
		return nil
	}
	return err
}

// Status reads one connection's live state.
func (c *Client) Status(ctx context.Context, connectionID string) (ConnectionStatus, error) {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		c.baseURL+connectionsPath+"/"+url.PathEscape(connectionID)+"/status",
		nil,
	)
	if err != nil {
		return ConnectionStatus{}, err
	}
	var status ConnectionStatus
	if err := c.do(req, &status); err != nil {
		return ConnectionStatus{}, err
	}
	return status, nil
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
	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("%s %s: %w", req.Method, req.URL.Path, ErrConnectionNotHeld)
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
