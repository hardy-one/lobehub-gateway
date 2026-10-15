package gateway

import "encoding/json"

// ConnectRequest is the body of `POST /api/connections`.
type ConnectRequest struct {
	Config ConnectRequestConfig `json:"config"`
	// Ensure marks a reconcile-driven connect: an identical connection that is
	// already held keeps its state instead of being rebuilt.
	Ensure bool `json:"ensure,omitempty"`
}

// ConnectRequestConfig mirrors the connection payload LobeHub sends. It is
// decoded here and handed to the provider layer as `provider.Config`.
type ConnectRequestConfig struct {
	ApplicationID  string          `json:"applicationId,omitempty"`
	Capabilities   json.RawMessage `json:"capabilities,omitempty"`
	ConnectionID   string          `json:"connectionId"`
	ConnectionMode string          `json:"connectionMode,omitempty"`
	Credentials    map[string]any  `json:"credentials,omitempty"`
	Platform       string          `json:"platform"`
	UserID         string          `json:"userId,omitempty"`
	WebhookPath    string          `json:"webhookPath,omitempty"`
}

// ConnectResponse is the reply to `POST /api/connections`.
type ConnectResponse struct {
	Status string `json:"status"`
}

// DisconnectAllResponse is the reply to `DELETE /api/connections`.
type DisconnectAllResponse struct {
	Total int `json:"total"`
}

// DisconnectResponse is the reply to `DELETE /api/connections/{id}`.
type DisconnectResponse struct {
	Status string `json:"status"`
}

// TypingRequest is the body of the typing routes.
type TypingRequest struct {
	PlatformThreadID string `json:"platformThreadId"`
}

// ConnectionStatus is the reply to `GET /api/connections/{id}/status`.
// `Config` is null for an unknown connection; callers read that as "connect it".
type ConnectionStatus struct {
	Config *ConnectionStatusConfig `json:"config"`
	State  ConnectionStatusState   `json:"state"`
}

type ConnectionStatusConfig struct {
	ConnectionID string `json:"connectionId"`
	Platform     string `json:"platform"`
}

type ConnectionStatusState struct {
	ConnectedAt int64  `json:"connectedAt,omitempty"`
	Error       string `json:"error,omitempty"`
	ErrorCode   string `json:"errorCode,omitempty"`
	Platform    string `json:"platform"`
	Status      string `json:"status"`
}

// GatewayStats is the reply to `GET /api/admin/stats`.
type GatewayStats struct {
	ByPlatform  map[string]int         `json:"byPlatform"`
	Connections []GatewayStatsListItem `json:"connections"`
	Total       int                    `json:"total"`
}

type GatewayStatsListItem struct {
	ConnectionID string                `json:"connectionId"`
	Platform     string                `json:"platform"`
	State        ConnectionStatusState `json:"state"`
	UserID       string                `json:"userId"`
}

// RegisteredIDs is the reply to `GET /api/admin/registered-ids`.
type RegisteredIDs struct {
	IDs []string `json:"ids"`
}

// GatewayCapabilities is the reply to `GET /api/admin/capabilities`. Declaring
// a platform is what lets LobeHub route its connections here.
type GatewayCapabilities struct {
	Platforms []string `json:"platforms"`
}

// StateCallback is the body the gateway posts to LobeHub at
// `/api/agent/gateway/callback`.
type StateCallback struct {
	ApplicationID string              `json:"applicationId,omitempty"`
	ConnectionID  string              `json:"connectionId"`
	Platform      string              `json:"platform"`
	State         StateCallbackStatus `json:"state"`
}

type StateCallbackStatus struct {
	Error     string `json:"error,omitempty"`
	ErrorCode string `json:"errorCode,omitempty"`
	Status    string `json:"status"`
}

// DesiredConnections is what LobeHub returns from
// `/api/agent/gateway/desired-connections`: the connect payloads this gateway
// should be holding. A gateway that just restarted rebuilds itself from it.
type DesiredConnections struct {
	Complete    bool             `json:"complete"`
	Connections []DesiredConnect `json:"connections"`
	Deferred    int              `json:"deferred"`
	Excluded    int              `json:"excluded"`
}

type DesiredConnect struct {
	Config ConnectRequestConfig `json:"config"`
	Ensure bool                 `json:"ensure,omitempty"`
}
