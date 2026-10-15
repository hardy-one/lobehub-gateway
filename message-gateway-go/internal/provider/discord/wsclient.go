package discord

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// Minimal WebSocket client, hand-rolled to keep this repository free of
// runtime dependencies. It implements exactly what the Discord gateway needs:
// text frames in and out, ping/pong, fragmented messages, and close codes.

const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xa

	wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

	dialTimeout      = 15 * time.Second
	handshakeTimeout = 15 * time.Second
	writeTimeout     = 10 * time.Second
)

// errClosed reports a close frame from the peer. The code is available through
// wsClient.CloseCode.
var errClosed = errors.New("websocket closed by peer")

type wsClient struct {
	conn   net.Conn
	reader *bufio.Reader

	writeMu sync.Mutex

	closeCode   int
	closeReason string
}

func dialWebSocket(ctx context.Context, rawURL string) (*wsClient, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid websocket url %q: %w", rawURL, err)
	}
	if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		return nil, fmt.Errorf("unsupported websocket scheme %q", parsed.Scheme)
	}

	host := parsed.Host
	if parsed.Port() == "" {
		port := "80"
		if parsed.Scheme == "wss" {
			port = "443"
		}
		host = net.JoinHostPort(parsed.Hostname(), port)
	}

	dialer := &net.Dialer{Timeout: dialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", host)
	if err != nil {
		return nil, err
	}

	if parsed.Scheme == "wss" {
		tlsConn := tls.Client(conn, &tls.Config{ServerName: parsed.Hostname()})
		handshakeCtx, cancel := context.WithTimeout(ctx, handshakeTimeout)
		defer cancel()
		if err := tlsConn.HandshakeContext(handshakeCtx); err != nil {
			_ = conn.Close()
			return nil, err
		}
		conn = tlsConn
	}

	key, err := newWebSocketKey()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	path := parsed.Path
	if path == "" {
		path = "/"
	}
	// Header names are written exactly as the protocol spells them; net/http's
	// canonicalisation would produce `Sec-Websocket-*`, which servers reject.
	request := &http.Request{
		Method:     http.MethodGet,
		URL:        &url.URL{Path: path, RawQuery: parsed.RawQuery},
		Host:       parsed.Host,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header: http.Header{
			"Connection":            []string{"Upgrade"},
			"Upgrade":               []string{"websocket"},
			"Sec-WebSocket-Key":     []string{key},
			"Sec-WebSocket-Version": []string{"13"},
		},
	}

	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		deadline = time.Now().Add(handshakeTimeout)
	}
	if err := conn.SetDeadline(deadline); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := request.Write(conn); err != nil {
		_ = conn.Close()
		return nil, err
	}

	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, request)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	// Deliberately not closing the response body: for a 101 response the body
	// *is* the upgraded connection, and closing it would drop the socket.

	if response.StatusCode != http.StatusSwitchingProtocols {
		_ = conn.Close()
		return nil, fmt.Errorf("websocket upgrade refused (%d)", response.StatusCode)
	}
	if expected := acceptKey(key); response.Header.Get("Sec-WebSocket-Accept") != expected {
		_ = conn.Close()
		return nil, errors.New("websocket upgrade returned an invalid accept key")
	}

	_ = conn.SetDeadline(time.Time{})

	return &wsClient{conn: conn, reader: reader}, nil
}

// Read returns the next complete data message, answering pings on the way.
func (c *wsClient) Read() (byte, []byte, error) {
	var (
		messageOp byte
		message   []byte
	)

	for {
		final, opcode, payload, err := c.readFrame()
		if err != nil {
			return 0, nil, err
		}

		switch opcode {
		case opPing:
			_ = c.writeFrame(opPong, payload)
			continue
		case opPong:
			continue
		case opClose:
			if len(payload) >= 2 {
				c.closeCode = int(binary.BigEndian.Uint16(payload[:2]))
			}
			if len(payload) > 2 {
				c.closeReason = string(payload[2:])
			}
			return opClose, payload, errClosed
		case opText, opBinary:
			messageOp = opcode
			message = payload
		case opContinuation:
			message = append(message, payload...)
		default:
			return 0, nil, fmt.Errorf("unsupported websocket opcode %d", opcode)
		}

		if final {
			return messageOp, message, nil
		}
	}
}

func (c *wsClient) readFrame() (bool, byte, []byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(c.reader, header); err != nil {
		return false, 0, nil, err
	}

	final := header[0]&0x80 != 0
	opcode := header[0] & 0x0f
	masked := header[1]&0x80 != 0
	length := uint64(header[1] & 0x7f)

	switch length {
	case 126:
		buf := make([]byte, 2)
		if _, err := io.ReadFull(c.reader, buf); err != nil {
			return false, 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(buf))
	case 127:
		buf := make([]byte, 8)
		if _, err := io.ReadFull(c.reader, buf); err != nil {
			return false, 0, nil, err
		}
		length = binary.BigEndian.Uint64(buf)
	}

	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(c.reader, mask[:]); err != nil {
			return false, 0, nil, err
		}
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return false, 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}

	return final, opcode, payload, nil
}

func (c *wsClient) WriteText(payload []byte) error {
	return c.writeFrame(opText, payload)
}

func (c *wsClient) WriteClose(code int, reason string) error {
	payload := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(payload, uint16(code))
	copy(payload[2:], reason)
	return c.writeFrame(opClose, payload)
}

func (c *wsClient) writeFrame(opcode byte, payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	_ = c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))

	frame := []byte{0x80 | opcode}
	length := len(payload)
	switch {
	case length < 126:
		frame = append(frame, byte(length)|0x80)
	case length <= 0xffff:
		frame = append(frame, 126|0x80, byte(length>>8), byte(length))
	default:
		frame = append(frame, 127|0x80)
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(length))
		frame = append(frame, buf...)
	}

	mask := make([]byte, 4)
	if _, err := rand.Read(mask); err != nil {
		return err
	}
	frame = append(frame, mask...)

	masked := make([]byte, length)
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}
	frame = append(frame, masked...)

	_, err := c.conn.Write(frame)
	return err
}

// CloseCode reports the peer's close code, or 0 when the connection died
// without one.
func (c *wsClient) CloseCode() int { return c.closeCode }

// CloseReason reports the peer's close reason text.
func (c *wsClient) CloseReason() string { return c.closeReason }

// SetReadDeadline bounds the next read; a zero time clears it.
func (c *wsClient) SetReadDeadline(deadline time.Time) error {
	return c.conn.SetReadDeadline(deadline)
}

func (c *wsClient) Close() error { return c.conn.Close() }

func newWebSocketKey() (string, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(nonce), nil
}

func acceptKey(key string) string {
	sum := sha1.Sum([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}
