package volcengine

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/bytedance/sonic"
	"github.com/gorilla/websocket"
	"github.com/lobehub/lobehub/apps/asr-gateway-go/internal/provider"
)

const (
	defaultEndpoint   = "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async"
	defaultResourceID = "volc.seedasr.sauc.duration"

	protocolVersion = byte(0b0001)

	clientFullRequest      = byte(0b0001)
	clientAudioOnlyRequest = byte(0b0010)
	serverFullResponse     = byte(0b1001)
	serverErrorResponse    = byte(0b1111)

	noSequence      = byte(0b0000)
	posSequence     = byte(0b0001)
	negWithSequence = byte(0b0011)

	noSerialization = byte(0b0000)
	jsonFormat      = byte(0b0001)

	gzipCompression = byte(0b0001)
)

type Config struct {
	APIKey     string
	Endpoint   string
	ResourceID string
}

type VolcengineProvider struct {
	cfg       Config
	conn      *websocket.Conn
	writeMu   sync.Mutex
	eventsCh  chan provider.TranscriptEvent
	errorsCh  chan error
	closeOnce sync.Once
	closed    chan struct{}
	seq       int32
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
		seq:      1,
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

	header := http.Header{}
	header.Set("X-Api-Key", p.cfg.APIKey)
	header.Set("X-Api-Resource-Id", p.cfg.ResourceID)
	header.Set("X-Api-Request-Id", sessionID)
	header.Set("X-Api-Sequence", "-1")

	dialer := websocket.DefaultDialer
	conn, resp, err := dialer.DialContext(ctx, p.cfg.Endpoint, header)
	if err != nil {
		return fmt.Errorf("dial volcengine websocket failed: %w", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	p.conn = conn

	// 发送官方完整的初始化首包
	if err := p.sendFullClientRequest(); err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to send full client request: %w", err)
	}

	// 启动官方消息读取监听
	go p.recvLoop()

	return nil
}

func (p *VolcengineProvider) sendFullClientRequest() error {
	payload := map[string]any{
		"user": map[string]any{
			"uid": "lobe-user",
		},
		"audio": map[string]any{
			"format":  "pcm",
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
			"show_utterances":  false, // 关闭分词和分句切片，获取纯净打字文本
			"enable_nonstream": false, // 实时流式优先
			"result_type":      "full",
		},
	}

	jsonBytes, err := sonic.Marshal(payload)
	if err != nil {
		return err
	}
	gzipped := gzipCompress(jsonBytes)

	// 官方 NewFullClientRequest 格式:
	// Header (4B) | Seq (4B) | Size (4B) | Payload
	var buf bytes.Buffer
	buf.Write([]byte{
		(protocolVersion << 4) | 1,
		(clientFullRequest << 4) | posSequence,
		(jsonFormat << 4) | gzipCompression,
		0x00,
	})
	_ = binary.Write(&buf, binary.BigEndian, int32(1))
	_ = binary.Write(&buf, binary.BigEndian, int32(len(gzipped)))
	buf.Write(gzipped)

	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	return p.conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
}

func (p *VolcengineProvider) SendAudio(pcmChunk []byte) error {
	select {
	case <-p.closed:
		return errors.New("provider closed")
	default:
	}

	curSeq := atomic.AddInt32(&p.seq, 1)
	gzipped := gzipCompress(pcmChunk)

	// 官方 NewAudioOnlyRequest 格式:
	// Header (4B) | Seq (4B) | Size (4B) | GzipPayload
	var buf bytes.Buffer
	buf.Write([]byte{
		(protocolVersion << 4) | 1,
		(clientAudioOnlyRequest << 4) | posSequence,
		(noSerialization << 4) | gzipCompression,
		0x00,
	})
	_ = binary.Write(&buf, binary.BigEndian, int32(curSeq))
	_ = binary.Write(&buf, binary.BigEndian, int32(len(gzipped)))
	buf.Write(gzipped)

	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	return p.conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
}

func (p *VolcengineProvider) EndSession() error {
	curSeq := atomic.AddInt32(&p.seq, 1)

	// 官方结束帧：序列号设为负数 (-seq)，负载为空
	var buf bytes.Buffer
	buf.Write([]byte{
		(protocolVersion << 4) | 1,
		(clientAudioOnlyRequest << 4) | negWithSequence,
		(noSerialization << 4) | gzipCompression,
		0x00,
	})
	_ = binary.Write(&buf, binary.BigEndian, int32(-curSeq))
	emptyGzip := gzipCompress([]byte{})
	_ = binary.Write(&buf, binary.BigEndian, int32(len(emptyGzip)))
	buf.Write(emptyGzip)

	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	return p.conn.WriteMessage(websocket.BinaryMessage, buf.Bytes())
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

func (p *VolcengineProvider) recvLoop() {
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

		_, msg, err := p.conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				select {
				case p.errorsCh <- err:
				default:
				}
			}
			return
		}

		if len(msg) < 4 {
			continue
		}

		headerSize := msg[0] & 0x0f
		messageType := msg[1] >> 4
		flags := msg[1] & 0x0f
		serialization := msg[2] >> 4
		compression := msg[2] & 0x0f

		payload := msg[headerSize*4:]
		// 官方 ParseResponse: 解析 messageTypeSpecificFlags
		if (flags & 0x01) != 0 { // has sequence
			if len(payload) >= 4 {
				payload = payload[4:]
			}
		}
		isLastPackage := (flags & 0x02) != 0
		if (flags & 0x04) != 0 { // has event
			if len(payload) >= 4 {
				payload = payload[4:]
			}
		}

		switch messageType {
		case serverFullResponse:
			if len(payload) >= 4 {
				payload = payload[4:] // skip payload size
			}
		case serverErrorResponse:
			if len(payload) >= 8 {
				code := int(binary.BigEndian.Uint32(payload[:4]))
				slog.Error("volcengine error response", "code", code)
				return
			}
		}

		if len(payload) == 0 {
			if isLastPackage {
				p.eventsCh <- provider.TranscriptEvent{
					SegmentID: "seg_dictation",
					Text:      "",
					IsFinal:   true,
				}
			}
			continue
		}

		if compression == gzipCompression {
			decompressed, err := gzipDecompress(payload)
			if err == nil {
				payload = decompressed
			}
		}

		if serialization == jsonFormat {
			var resp struct {
				Code       int    `json:"code"`
				Error      string `json:"error"`
				PayloadMsg struct {
					Result struct {
						Text string `json:"text"`
					} `json:"result"`
				} `json:"payload_msg"`
				Result struct {
					Text string `json:"text"`
				} `json:"result"`
			}

			if err := sonic.Unmarshal(payload, &resp); err == nil {
				if resp.Code != 0 || resp.Error != "" {
					slog.Warn("volcengine asr error payload", "code", resp.Code, "error", resp.Error)
					continue
				}

				text := resp.PayloadMsg.Result.Text
				if text == "" {
					text = resp.Result.Text
				}

				if text != "" || isLastPackage {
					p.eventsCh <- provider.TranscriptEvent{
						SegmentID: "seg_dictation",
						Text:      text,
						IsFinal:   isLastPackage,
					}
				}
			}
		}
	}
}

func gzipCompress(data []byte) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, _ = w.Write(data)
	_ = w.Close()
	return buf.Bytes()
}

func gzipDecompress(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
