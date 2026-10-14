package volcengine

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/lobehub/lobehub/apps/asr-gateway-go/internal/provider"
)

const (
	defaultEndpoint   = "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async"
	defaultResourceID = "volc.seedasr.sauc.duration"

	protocolVersion = 0b0001
	headerSizeCode  = 0b0001 // 4 bytes

	msgFullClientRequest = 0b0001
	msgAudioOnly         = 0b0010
	msgFullServerResp    = 0b1001

	flagNoSequence = 0b0000
	flagLastPacket = 0b0010

	serializationJSON = 0b0001
	serializationNone = 0b0000

	compressionNone = 0b0000
	compressionGzip = 0b0001
)

type Config struct {
	APIKey     string
	Endpoint   string
	ResourceID string
}

type VolcengineProvider struct {
	cfg       Config
	conn      net.Conn
	writeMu   sync.Mutex
	eventsCh  chan provider.TranscriptEvent
	errorsCh  chan error
	closeOnce sync.Once
	closed    chan struct{}
}

func NewVolcengineProvider(cfg Config) *VolcengineProvider {
	if cfg.Endpoint == "" {
		cfg.Endpoint = defaultEndpoint
	}
	if cfg.ResourceID == "" {
		cfg.ResourceID = defaultResourceID
	}
	return &VolcengineProvider{
		cfg:      cfg,
		eventsCh: make(chan provider.TranscriptEvent, 32),
		errorsCh: make(chan error, 1),
		closed:   make(chan struct{}),
	}
}

func (p *VolcengineProvider) Events() <-chan provider.TranscriptEvent {
	return p.eventsCh
}

func (p *VolcengineProvider) Errors() <-chan error {
	return p.errorsCh
}

func (p *VolcengineProvider) StartSession(ctx context.Context, sessionID string) error {
	if p.cfg.APIKey == "" {
		return errors.New("VOLCENGINE_VOICE_API_KEY is not set")
	}

	u, err := url.Parse(p.cfg.Endpoint)
	if err != nil {
		return fmt.Errorf("invalid endpoint URL: %w", err)
	}

	host := u.Host
	if !hasPort(host) {
		if u.Scheme == "wss" {
			host += ":443"
		} else {
			host += ":80"
		}
	}

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", host)
	if err != nil {
		return fmt.Errorf("failed to dial volcengine: %w", err)
	}

	if u.Scheme == "wss" {
		serverName := u.Hostname()
		tlsConn := tls.Client(conn, &tls.Config{ServerName: serverName})
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = conn.Close()
			return fmt.Errorf("tls handshake failed: %w", err)
		}
		p.conn = tlsConn
	} else {
		p.conn = conn
	}

	// Perform WebSocket client handshake
	secKey := "dGhlIHNhbXBsZSBub25jZQ=="
	reqPath := u.Path
	if reqPath == "" {
		reqPath = "/"
	}
	if u.RawQuery != "" {
		reqPath += "?" + u.RawQuery
	}

	handshakeReq := fmt.Sprintf(
		"GET %s HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Key: %s\r\n"+
			"Sec-WebSocket-Version: 13\r\n"+
			"X-Api-Key: %s\r\n"+
			"X-Api-Resource-Id: %s\r\n"+
			"X-Api-Request-Id: %s\r\n"+
			"X-Api-Sequence: -1\r\n\r\n",
		reqPath, u.Host, secKey, p.cfg.APIKey, p.cfg.ResourceID, sessionID,
	)

	if _, err := p.conn.Write([]byte(handshakeReq)); err != nil {
		_ = p.conn.Close()
		return fmt.Errorf("failed to send handshake: %w", err)
	}

	// Read HTTP 101 Switching Protocols
	respBuf := make([]byte, 1024)
	n, err := p.conn.Read(respBuf)
	if err != nil {
		_ = p.conn.Close()
		return fmt.Errorf("failed to read handshake response: %w", err)
	}
	if !bytes.Contains(respBuf[:n], []byte("101 Switching Protocols")) {
		_ = p.conn.Close()
		return fmt.Errorf("handshake failed with response: %s", string(respBuf[:n]))
	}

	// Send initial Full Client Request
	if err := p.sendInitialPayload(); err != nil {
		_ = p.conn.Close()
		return fmt.Errorf("failed to send initial payload: %w", err)
	}

	// Start reading loop
	go p.readLoop()

	return nil
}

func (p *VolcengineProvider) sendInitialPayload() error {
	initPayload := map[string]any{
		"user": map[string]any{
			"uid": "lobe-user",
		},
		"audio": map[string]any{
			"format":  "raw",
			"codec":   "raw",
			"rate":    16000,
			"bits":    16,
			"channel": 1,
		},
		"request": map[string]any{
			"model_name":       "bigmodel",
			"enable_itn":       true,
			"enable_punc":      true,
			"enable_ddc":       true,
			"show_utterances":  true,
			"enable_nonstream": false, // Realtime streaming mode
			"result_type":      "full",
		},
	}

	data, err := json.Marshal(initPayload)
	if err != nil {
		return err
	}

	var gzipped bytes.Buffer
	gw := gzip.NewWriter(&gzipped)
	if _, err := gw.Write(data); err != nil {
		return err
	}
	if err := gw.Close(); err != nil {
		return err
	}

	hdr := buildHeader(msgFullClientRequest, flagNoSequence, serializationJSON, compressionGzip)
	payloadBytes := gzipped.Bytes()
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(payloadBytes)))

	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	packet := append(hdr, lenBuf...)
	packet = append(packet, payloadBytes...)

	return p.writeRawWSBinary(packet)
}

func (p *VolcengineProvider) SendAudio(pcmChunk []byte) error {
	select {
	case <-p.closed:
		return errors.New("provider closed")
	default:
	}

	hdr := buildHeader(msgAudioOnly, flagNoSequence, serializationNone, compressionNone)
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(pcmChunk)))

	packet := append(hdr, lenBuf...)
	packet = append(packet, pcmChunk...)

	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	return p.writeRawWSBinary(packet)
}

func (p *VolcengineProvider) EndSession() error {
	hdr := buildHeader(msgAudioOnly, flagLastPacket, serializationNone, compressionNone)
	lenBuf := make([]byte, 4) // length = 0

	packet := append(hdr, lenBuf...)

	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	return p.writeRawWSBinary(packet)
}

func (p *VolcengineProvider) Close() error {
	p.closeOnce.Do(func() {
		close(p.closed)
		if p.conn != nil {
			_ = p.conn.Close()
		}
	})
	return nil
}

func (p *VolcengineProvider) writeRawWSBinary(payload []byte) error {
	_ = p.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))

	// Client-to-server frames MUST be masked
	header := []byte{0x82} // binary frame, FIN=1
	length := len(payload)
	maskKey := []byte{0x12, 0x34, 0x56, 0x78}

	if length < 126 {
		header = append(header, byte(length|0x80))
	} else if length <= 0xffff {
		header = append(header, 126|0x80, byte(length>>8), byte(length))
	} else {
		header = append(header, 127|0x80)
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(length))
		header = append(header, buf...)
	}

	header = append(header, maskKey...)
	maskedPayload := make([]byte, length)
	for i := range payload {
		maskedPayload[i] = payload[i] ^ maskKey[i%4]
	}

	if _, err := p.conn.Write(header); err != nil {
		return err
	}
	_, err := p.conn.Write(maskedPayload)
	return err
}

func (p *VolcengineProvider) readLoop() {
	defer func() {
		close(p.eventsCh)
		p.Close()
	}()

	for {
		select {
		case <-p.closed:
			return
		default:
		}

		payload, err := p.readWSFrame()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				select {
				case p.errorsCh <- err:
				default:
				}
			}
			return
		}

		if len(payload) < 8 {
			continue
		}

		hdr := payload[:4]
		msgType := (hdr[1] >> 4) & 0x0f
		flags := hdr[1] & 0x0f
		compression := hdr[2] & 0x0f

		hasSequence := flags == 0b0001 || flags == 0b0011
		offset := 4
		if hasSequence {
			offset += 4
		}
		if len(payload) < offset+4 {
			continue
		}

		bodySize := binary.BigEndian.Uint32(payload[offset : offset+4])
		bodyOffset := offset + 4
		if uint32(len(payload)) < uint32(bodyOffset)+bodySize {
			continue
		}

		body := payload[bodyOffset : bodyOffset+int(bodySize)]

		if msgType == msgFullServerResp {
			if compression == compressionGzip {
				gr, err := gzip.NewReader(bytes.NewReader(body))
				if err == nil {
					decompressed, err := io.ReadAll(gr)
					_ = gr.Close()
					if err == nil {
						body = decompressed
					}
				}
			}

			var resp struct {
				Code       int `json:"code"`
				PayloadMsg struct {
					Result struct {
						Text       string `json:"text"`
						Utterances []struct {
							Definite bool   `json:"definite"`
							Text     string `json:"text"`
						} `json:"utterances"`
					} `json:"result"`
				} `json:"payload_msg"`
				Result struct {
					Text       string `json:"text"`
					Utterances []struct {
						Definite bool   `json:"definite"`
						Text     string `json:"text"`
					} `json:"utterances"`
				} `json:"result"`
			}

			if err := json.Unmarshal(body, &resp); err == nil {
				if resp.Code != 0 {
					continue
				}

				text := resp.PayloadMsg.Result.Text
				if text == "" {
					text = resp.Result.Text
				}

				utterances := resp.PayloadMsg.Result.Utterances
				if len(utterances) == 0 {
					utterances = resp.Result.Utterances
				}

				isFinal := false
				if len(utterances) > 0 {
					isFinal = utterances[len(utterances)-1].Definite
				}

				if text != "" {
					p.eventsCh <- provider.TranscriptEvent{
						SegmentID: fmt.Sprintf("seg_%d", len(utterances)),
						Text:      text,
						IsFinal:   isFinal,
					}
				}
			}
		}
	}
}

func (p *VolcengineProvider) readWSFrame() ([]byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(p.conn, header); err != nil {
		return nil, err
	}

	opcode := header[0] & 0x0f
	masked := header[1]&0x80 != 0
	length := uint64(header[1] & 0x7f)

	if length == 126 {
		buf := make([]byte, 2)
		if _, err := io.ReadFull(p.conn, buf); err != nil {
			return nil, err
		}
		length = uint64(binary.BigEndian.Uint16(buf))
	} else if length == 127 {
		buf := make([]byte, 8)
		if _, err := io.ReadFull(p.conn, buf); err != nil {
			return nil, err
		}
		length = binary.BigEndian.Uint64(buf)
	}

	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(p.conn, mask[:]); err != nil {
			return nil, err
		}
	}

	payload := make([]byte, length)
	if _, err := io.ReadFull(p.conn, payload); err != nil {
		return nil, err
	}

	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}

	if opcode == 8 { // close frame
		return nil, io.EOF
	}

	return payload, nil
}

func buildHeader(msgType, flags, serialization, compression int) []byte {
	b0 := byte((protocolVersion << 4) | headerSizeCode)
	b1 := byte((msgType << 4) | flags)
	b2 := byte((serialization << 4) | compression)
	b3 := byte(0x00)
	return []byte{b0, b1, b2, b3}
}

func hasPort(s string) bool {
	_, _, err := net.SplitHostPort(s)
	return err == nil
}
