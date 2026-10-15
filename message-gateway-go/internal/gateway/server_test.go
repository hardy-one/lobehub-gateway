package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lobehub/lobehub/apps/message-gateway-go/internal/provider"
)

const testToken = "test-service-token"

// The fake platform is registered once per test binary: the provider registry
// rejects a second registration, which is what keeps platform ids unique.
func init() {
	provider.Register("testplatform", newTestFactory())
}

// testProvider is a scripted platform runtime: it records what the gateway asks
// of it and lets a test drive state changes and inbound events.
type testProvider struct {
	deps provider.Deps

	mu     sync.Mutex
	typing map[string]bool
}

var (
	testProviderMu   sync.Mutex
	testProviderNext *testProvider
	startCount       int
	stopCount        int
)

func newTestFactory() provider.Factory {
	return func(cfg provider.Config, deps provider.Deps) (provider.Provider, error) {
		instance := &testProvider{deps: deps, typing: map[string]bool{}}
		testProviderMu.Lock()
		testProviderNext = instance
		startCount++
		testProviderMu.Unlock()
		return instance, nil
	}
}

func latestTestProvider(t *testing.T) *testProvider {
	t.Helper()
	testProviderMu.Lock()
	defer testProviderMu.Unlock()
	if testProviderNext == nil {
		t.Fatal("no provider has been created")
	}
	return testProviderNext
}

func (p *testProvider) Start(context.Context) error { return nil }

func (p *testProvider) Stop(context.Context) error {
	testProviderMu.Lock()
	stopCount++
	testProviderMu.Unlock()
	return nil
}

func (p *testProvider) SetTyping(_ context.Context, platformThreadID string, active bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.typing[platformThreadID] = active
	return nil
}

func (p *testProvider) typingState(platformThreadID string) (bool, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	active, seen := p.typing[platformThreadID]
	return active, seen
}

type lobeRecorder struct {
	mu        sync.Mutex
	callbacks []StateCallback
	forwards  []recordedForward
}

type recordedForward struct {
	Authorization string
	Body          []byte
	Path          string
}

func (r *lobeRecorder) callbacksSeen() []StateCallback {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]StateCallback(nil), r.callbacks...)
}

func (r *lobeRecorder) forwardsSeen() []recordedForward {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recordedForward(nil), r.forwards...)
}

func newTestServer(t *testing.T) (*Server, string, *lobeRecorder) {
	t.Helper()

	recorder := &lobeRecorder{}
	lobe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		switch r.URL.Path {
		case callbackPath:
			var callback StateCallback
			if err := json.Unmarshal(body, &callback); err != nil {
				t.Errorf("callback body is not a state callback: %v", err)
			}
			recorder.mu.Lock()
			recorder.callbacks = append(recorder.callbacks, callback)
			recorder.mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		default:
			recorder.mu.Lock()
			recorder.forwards = append(recorder.forwards, recordedForward{
				Authorization: r.Header.Get("Authorization"),
				Body:          body,
				Path:          r.URL.Path,
			})
			recorder.mu.Unlock()
			w.WriteHeader(http.StatusOK)
		}
	}))
	t.Cleanup(lobe.Close)

	cfg := Config{
		LobeAPIBaseURL: lobe.URL,
		ServiceToken:   testToken,
		TypingInterval: time.Hour, // tests drive typing explicitly
	}
	server := NewServer(cfg)
	t.Cleanup(func() { server.Shutdown(context.Background()) })

	return server, lobe.URL, recorder
}

func call(t *testing.T, handler http.Handler, method string, path string, body string) *httptest.ResponseRecorder {
	t.Helper()

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	request := httptest.NewRequest(method, path, reader)
	request.Header.Set("Authorization", "Bearer "+testToken)

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func decode[T any](t *testing.T, recorder *httptest.ResponseRecorder) T {
	t.Helper()
	var decoded T
	if err := json.Unmarshal(recorder.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("response body %q is not valid JSON: %v", recorder.Body.String(), err)
	}
	return decoded
}

func TestHealthNeedsNoCredential(t *testing.T) {
	server, _, _ := newTestServer(t)

	recorder := httptest.NewRecorder()
	server.Routes().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("health status = %d, want 200", recorder.Code)
	}
}

func TestEveryGatewayRouteRequiresTheServiceToken(t *testing.T) {
	server, _, _ := newTestServer(t)
	handler := server.Routes()

	routes := []struct{ method, path string }{
		{http.MethodPost, "/api/connections"},
		{http.MethodDelete, "/api/connections"},
		{http.MethodDelete, "/api/connections/abc"},
		{http.MethodPost, "/api/connections/abc/typing"},
		{http.MethodDelete, "/api/connections/abc/typing"},
		{http.MethodGet, "/api/connections/abc/status"},
		{http.MethodGet, "/api/admin/stats"},
		{http.MethodGet, "/api/admin/registered-ids"},
		{http.MethodGet, "/api/admin/capabilities"},
	}

	for _, route := range routes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			for _, credential := range []string{"", "Bearer wrong-token", "Bearer " + testToken + "x"} {
				request := httptest.NewRequest(route.method, route.path, strings.NewReader("{}"))
				if credential != "" {
					request.Header.Set("Authorization", credential)
				}

				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, request)

				if recorder.Code != http.StatusUnauthorized {
					t.Fatalf("credential %q status = %d, want 401", credential, recorder.Code)
				}
			}
		})
	}
}

func TestConnectRegistersTheConnectionAndReportsIt(t *testing.T) {
	server, _, _ := newTestServer(t)
	handler := server.Routes()

	connectBody := `{"config":{"connectionId":"conn-1","platform":"testplatform","connectionMode":"websocket","applicationId":"app-1","userId":"user-1"}}`
	recorder := call(t, handler, http.MethodPost, "/api/connections", connectBody)
	if recorder.Code != http.StatusOK {
		t.Fatalf("connect status = %d (%s), want 200", recorder.Code, recorder.Body.String())
	}
	if connected := decode[ConnectResponse](t, recorder); connected.Status != provider.StateConnected {
		t.Fatalf("connect status = %q, want %q", connected.Status, provider.StateConnected)
	}

	ids := decode[RegisteredIDs](t, call(t, handler, http.MethodGet, "/api/admin/registered-ids", ""))
	if len(ids.IDs) != 1 || ids.IDs[0] != "conn-1" {
		t.Fatalf("registered ids = %v, want [conn-1]", ids.IDs)
	}

	stats := decode[GatewayStats](t, call(t, handler, http.MethodGet, "/api/admin/stats", ""))
	if stats.Total != 1 || stats.ByPlatform["testplatform"] != 1 {
		t.Fatalf("stats = %+v, want one testplatform connection", stats)
	}
	if len(stats.Connections) != 1 || stats.Connections[0].UserID != "user-1" {
		t.Fatalf("stats connections = %+v, want the connected user", stats.Connections)
	}

	status := decode[ConnectionStatus](t, call(t, handler, http.MethodGet, "/api/connections/conn-1/status", ""))
	if status.Config == nil || status.Config.ConnectionID != "conn-1" || status.State.Status != provider.StateConnected {
		t.Fatalf("status = %+v, want a connected conn-1", status)
	}
}

func TestUnknownConnectionOperationsAnswerNotFound(t *testing.T) {
	server, _, _ := newTestServer(t)
	handler := server.Routes()

	for _, route := range []struct{ method, path string }{
		{http.MethodGet, "/api/connections/missing/status"},
		{http.MethodDelete, "/api/connections/missing"},
		{http.MethodPost, "/api/connections/missing/typing"},
		{http.MethodDelete, "/api/connections/missing/typing"},
	} {
		body := `{"platformThreadId":"discord:1:2"}`
		if recorder := call(t, handler, route.method, route.path, body); recorder.Code != http.StatusNotFound {
			t.Fatalf("%s %s status = %d, want 404", route.method, route.path, recorder.Code)
		}
	}
}

func TestEnsureKeepsALiveConnectionInsteadOfRebuildingIt(t *testing.T) {
	server, _, _ := newTestServer(t)
	handler := server.Routes()

	connectBody := `{"config":{"connectionId":"conn-ensure","platform":"testplatform"},"ensure":true}`
	if recorder := call(t, handler, http.MethodPost, "/api/connections", connectBody); recorder.Code != http.StatusOK {
		t.Fatalf("connect status = %d, want 200", recorder.Code)
	}

	testProviderMu.Lock()
	startsAfterFirst := startCount
	testProviderMu.Unlock()

	recorder := call(t, handler, http.MethodPost, "/api/connections", connectBody)
	if recorder.Code != http.StatusOK {
		t.Fatalf("second connect status = %d, want 200", recorder.Code)
	}

	testProviderMu.Lock()
	startsAfterSecond := startCount
	testProviderMu.Unlock()
	if startsAfterSecond != startsAfterFirst {
		t.Fatalf("ensure rebuilt a live connection: starts %d -> %d", startsAfterFirst, startsAfterSecond)
	}

	// A user-driven connect (no ensure) always rebuilds: credentials may have
	// changed and the caller expects a fresh lifecycle.
	if recorder := call(t, handler, http.MethodPost, "/api/connections",
		`{"config":{"connectionId":"conn-ensure","platform":"testplatform"}}`); recorder.Code != http.StatusOK {
		t.Fatalf("user connect status = %d, want 200", recorder.Code)
	}

	testProviderMu.Lock()
	startsAfterThird := startCount
	testProviderMu.Unlock()
	if startsAfterThird != startsAfterSecond+1 {
		t.Fatalf("user connect did not rebuild: starts %d -> %d", startsAfterSecond, startsAfterThird)
	}
}

func TestUnsupportedPlatformIsRejected(t *testing.T) {
	server, _, _ := newTestServer(t)

	recorder := call(t, server.Routes(), http.MethodPost, "/api/connections",
		`{"config":{"connectionId":"conn-x","platform":"nope"}}`)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
}

func TestTypingIsForwardedToTheProvider(t *testing.T) {
	server, _, _ := newTestServer(t)
	handler := server.Routes()

	body := `{"config":{"connectionId":"conn-typing","platform":"testplatform"}}`
	if recorder := call(t, handler, http.MethodPost, "/api/connections", body); recorder.Code != http.StatusOK {
		t.Fatalf("connect status = %d, want 200", recorder.Code)
	}

	instance := latestTestProvider(t)

	if recorder := call(t, handler, http.MethodPost, "/api/connections/conn-typing/typing",
		`{"platformThreadId":"test:1:2"}`); recorder.Code != http.StatusNoContent {
		t.Fatalf("start typing status = %d, want 204", recorder.Code)
	}
	waitFor(t, func() bool {
		active, seen := instance.typingState("test:1:2")
		return seen && active
	}, "the typing indicator was never started")

	if recorder := call(t, handler, http.MethodDelete, "/api/connections/conn-typing/typing",
		`{"platformThreadId":"test:1:2"}`); recorder.Code != http.StatusNoContent {
		t.Fatalf("stop typing status = %d, want 204", recorder.Code)
	}
	if active, _ := instance.typingState("test:1:2"); active {
		t.Fatal("typing indicator is still active after stop")
	}
}

func TestDisconnectAllDropsEveryConnection(t *testing.T) {
	server, _, _ := newTestServer(t)
	handler := server.Routes()

	for _, id := range []string{"conn-a", "conn-b"} {
		body := `{"config":{"connectionId":"` + id + `","platform":"testplatform"}}`
		if recorder := call(t, handler, http.MethodPost, "/api/connections", body); recorder.Code != http.StatusOK {
			t.Fatalf("connect %s status = %d, want 200", id, recorder.Code)
		}
	}

	recorder := call(t, handler, http.MethodDelete, "/api/connections", "")
	if dropped := decode[DisconnectAllResponse](t, recorder); dropped.Total != 2 {
		t.Fatalf("disconnect-all total = %d, want 2", dropped.Total)
	}
	if ids := decode[RegisteredIDs](t, call(t, handler, http.MethodGet, "/api/admin/registered-ids", "")); len(ids.IDs) != 0 {
		t.Fatalf("registered ids = %v, want none", ids.IDs)
	}
}

func TestInboundEventsAreForwardedWithTheServiceToken(t *testing.T) {
	server, _, recorder := newTestServer(t)
	handler := server.Routes()

	body := `{"config":{"connectionId":"conn-fwd","platform":"testplatform","webhookPath":"/api/agent/webhooks/testplatform/app-1"}}`
	if response := call(t, handler, http.MethodPost, "/api/connections", body); response.Code != http.StatusOK {
		t.Fatalf("connect status = %d, want 200", response.Code)
	}

	instance := latestTestProvider(t)
	if err := instance.deps.Forward(context.Background(), provider.Inbound{Body: []byte(`{"hello":"world"}`)}); err != nil {
		t.Fatalf("forward returned %v", err)
	}

	forwards := recorder.forwardsSeen()
	if len(forwards) != 1 {
		t.Fatalf("forwarded %d events, want 1", len(forwards))
	}
	if forwards[0].Path != "/api/agent/webhooks/testplatform/app-1" {
		t.Fatalf("forwarded to %q, want the connection's webhook path", forwards[0].Path)
	}
	if forwards[0].Authorization != "Bearer "+testToken {
		t.Fatalf("forward credential = %q, want the shared service token", forwards[0].Authorization)
	}
	if string(forwards[0].Body) != `{"hello":"world"}` {
		t.Fatalf("forwarded body = %q, want the raw platform payload", forwards[0].Body)
	}
}

func TestProviderHeadersOverrideTheDefaultCredential(t *testing.T) {
	server, _, recorder := newTestServer(t)
	handler := server.Routes()

	body := `{"config":{"connectionId":"conn-headers","platform":"testplatform","webhookPath":"/hook"}}`
	if response := call(t, handler, http.MethodPost, "/api/connections", body); response.Code != http.StatusOK {
		t.Fatalf("connect status = %d, want 200", response.Code)
	}

	instance := latestTestProvider(t)
	event := provider.Inbound{
		Body:    []byte(`{}`),
		Headers: map[string]string{"Authorization": "Bearer platform-specific"},
	}
	if err := instance.deps.Forward(context.Background(), event); err != nil {
		t.Fatalf("forward returned %v", err)
	}

	forwards := recorder.forwardsSeen()
	if len(forwards) != 1 || forwards[0].Authorization != "Bearer platform-specific" {
		t.Fatalf("forward credential = %+v, want the provider's own header", forwards)
	}
}

func TestStateChangesReachLobeHub(t *testing.T) {
	server, _, recorder := newTestServer(t)
	handler := server.Routes()

	body := `{"config":{"connectionId":"conn-state","platform":"testplatform","applicationId":"app-9"}}`
	if response := call(t, handler, http.MethodPost, "/api/connections", body); response.Code != http.StatusOK {
		t.Fatalf("connect status = %d, want 200", response.Code)
	}

	instance := latestTestProvider(t)
	instance.deps.Report(provider.StateChange{State: provider.StateError, Code: "4004", Message: "bad token"})

	// The connect that just happened reports its own state as well, so wait for
	// the specific failure rather than for "a" callback.
	waitFor(t, func() bool {
		for _, callback := range recorder.callbacksSeen() {
			if callback.State.ErrorCode == "4004" {
				return true
			}
		}
		return false
	}, "the reported failure never reached LobeHub")

	var callback StateCallback
	for _, seen := range recorder.callbacksSeen() {
		if seen.State.ErrorCode == "4004" {
			callback = seen
		}
	}
	if callback.ConnectionID != "conn-state" || callback.ApplicationID != "app-9" {
		t.Fatalf("callback = %+v, want conn-state/app-9", callback)
	}
	if callback.State.Status != provider.StateError || callback.State.Error != "bad token" {
		t.Fatalf("callback state = %+v, want the reported failure", callback.State)
	}
}

func TestCapabilitiesDeclareTheServablePlatforms(t *testing.T) {
	server, _, _ := newTestServer(t)

	capabilities := decode[GatewayCapabilities](t, call(t, server.Routes(), http.MethodGet, "/api/admin/capabilities", ""))
	if len(capabilities.Platforms) == 0 {
		t.Fatal("capabilities declared no platforms")
	}
}

func TestDisconnectIsSilentTowardsLobeHub(t *testing.T) {
	server, _, recorder := newTestServer(t)
	handler := server.Routes()

	body := `{"config":{"connectionId":"conn-quiet","platform":"testplatform","applicationId":"app-quiet"}}`
	if response := call(t, handler, http.MethodPost, "/api/connections", body); response.Code != http.StatusOK {
		t.Fatalf("connect status = %d, want 200", response.Code)
	}
	waitFor(t, func() bool { return len(recorder.callbacksSeen()) > 0 }, "connect callback never arrived")
	// Let any second callback from the same connect drain before counting.
	time.Sleep(20 * time.Millisecond)

	before := len(recorder.callbacksSeen())
	if response := call(t, handler, http.MethodDelete, "/api/connections/conn-quiet", ""); response.Code != http.StatusOK {
		t.Fatalf("disconnect status = %d, want 200", response.Code)
	}

	time.Sleep(50 * time.Millisecond)
	if after := len(recorder.callbacksSeen()); after != before {
		t.Fatalf("disconnect produced %d extra callbacks; the caller owns that status", after-before)
	}
}

func waitFor(t *testing.T, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(message)
}
