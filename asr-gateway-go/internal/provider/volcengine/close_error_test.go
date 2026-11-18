package volcengine

import (
	"errors"
	"io"
	"net"
	"testing"

	"github.com/gorilla/websocket"
)

// Closing a session locally surfaces as net.ErrClosed or io.EOF on the blocking
// read, not as a WebSocket close frame. Treating those as upstream failures
// logged a false ERROR and told the client the provider was unavailable.
func TestIsExpectedStreamEnd(t *testing.T) {
	expected := []error{
		net.ErrClosed,
		io.EOF,
		&websocket.CloseError{Code: websocket.CloseNormalClosure},
		&websocket.CloseError{Code: websocket.CloseGoingAway},
	}
	for _, err := range expected {
		if !isExpectedStreamEnd(err) {
			t.Errorf("isExpectedStreamEnd(%v) = false, want true", err)
		}
	}

	unexpected := []error{
		errors.New("connection reset by peer"),
		&websocket.CloseError{Code: websocket.CloseAbnormalClosure},
	}
	for _, err := range unexpected {
		if isExpectedStreamEnd(err) {
			t.Errorf("isExpectedStreamEnd(%v) = true, want false", err)
		}
	}
}
