//go:build e2e

// 端到端验证辅助（LOBE-185）：启动编译出来的真实 gateway 二进制，用真实
// HTTP + WebSocket 流量驱动，覆盖进程边界（端口重绑、SQLite/WAL 恢复）。
// 本包不引用 internal/gateway 的任何符号，WebSocket 客户端为自带实现。
package e2e

import (
	"bufio"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	e2eServiceToken = "e2e-service-token"
	e2eUserID       = "e2e-user"
	healthWait      = 15 * time.Second
	stopWait        = 10 * time.Second
	portReleaseWait = 5 * time.Second
	messageWait     = 5 * time.Second

	wsOpText  = 1
	wsOpClose = 8
	wsOpPing  = 9
	wsOpPong  = 10
)

// gatewayProcess 是一次真实进程启动的句柄。
type gatewayProcess struct {
	cmd     *exec.Cmd
	port    string
	logPath string
	exited  chan error
	stopped bool
}

func binaryPath(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("E2E_GATEWAY_BIN")
	if bin == "" {
		t.Fatal("E2E_GATEWAY_BIN 未设置：请通过 scripts/e2e-restart.sh 运行（脚本负责编译真实二进制），不要用 go run")
	}
	return bin
}

// freePort 先监听再关闭取得空闲端口，避免固定端口冲突。
func freePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("获取空闲端口失败: %v", err)
	}
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	if err := listener.Close(); err != nil {
		t.Fatalf("释放探测端口失败: %v", err)
	}
	return port
}

// runDir 返回本场景的文件目录。脚本模式（E2E_RUN_DIR）下目录由脚本管理，
// 失败时保留日志与数据库文件；直接 go test 模式退回 t.TempDir() 自动清理。
func runDir(t *testing.T) string {
	t.Helper()
	if root := os.Getenv("E2E_RUN_DIR"); root != "" {
		dir := filepath.Join(root, sanitize(t.Name()))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	return t.TempDir()
}

func sanitize(name string) string {
	return strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(name)
}

func startGateway(t *testing.T, dbPath string, port string, jwks string, logPath string) *gatewayProcess {
	t.Helper()
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("创建 gateway 日志文件失败: %v", err)
	}
	cmd := exec.Command(binaryPath(t))
	cmd.Env = append(os.Environ(),
		"SERVICE_TOKEN="+e2eServiceToken,
		"JWKS_PUBLIC_KEY="+jwks,
		"GATEWAY_STATE_DB="+dbPath,
		"PORT="+port,
		"REQUEST_LOG_ENABLED=true",
		// 指向黑洞地址：e2e 只测 gateway 自身，任何对 LobeHub 的回调都应视为异常。
		"LOBE_API_BASE_URL=http://127.0.0.1:1",
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("启动 gateway 进程失败: %v", err)
	}
	proc := &gatewayProcess{cmd: cmd, port: port, logPath: logPath, exited: make(chan error, 1)}
	go func() { proc.exited <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = proc.terminate()
		_ = logFile.Close()
	})
	proc.waitReady(t, healthWait)
	return proc
}

func (p *gatewayProcess) waitReady(t *testing.T, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	target := "http://127.0.0.1:" + p.port + "/health"
	for {
		select {
		case err := <-p.exited:
			t.Fatalf("gateway 进程提前退出（err=%v），日志尾部:\n%s", err, tailFile(p.logPath, 40))
		default:
		}
		resp, err := http.Get(target)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("等待 /health 200 超时（%s），日志尾部:\n%s", target, tailFile(p.logPath, 40))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// terminate 用 SIGTERM 请求退出，超时升级为 SIGKILL。不依赖固定 sleep 判定
// 进程死亡，由 cmd.Wait 的退出信号驱动。
func (p *gatewayProcess) terminate() error {
	if p.stopped {
		return nil
	}
	p.stopped = true
	if p.cmd.Process == nil {
		return fmt.Errorf("gateway 进程从未启动")
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case err := <-p.exited:
		return err
	case <-time.After(stopWait):
		_ = p.cmd.Process.Kill()
		select {
		case err := <-p.exited:
			return err
		case <-time.After(stopWait):
			return fmt.Errorf("gateway 进程无法终止（端口 %s）", p.port)
		}
	}
}

// stop 终止进程并确认端口已释放，之后才能启动下一个进程。
func (p *gatewayProcess) stop(t *testing.T) {
	t.Helper()
	if err := p.terminate(); err != nil {
		t.Fatal(err)
	}
	waitPortClosed(t, p.port, portReleaseWait)
}

func waitPortClosed(t *testing.T, port string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		conn, err := net.DialTimeout("tcp", "127.0.0.1:"+port, 200*time.Millisecond)
		if err != nil {
			return // 连接被拒绝即视为端口已释放
		}
		_ = conn.Close()
		if time.Now().After(deadline) {
			t.Fatalf("等待端口 %s 释放超时", port)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func tailFile(path string, n int) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("(无法读取日志 %s: %v)", path, err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// jwtSigner 测试自备 RSA 密钥对：私钥签 token，公钥以 JWKS 传给 gateway 进程。
type jwtSigner struct{ key *rsa.PrivateKey }

func newJWTSigner(t *testing.T) *jwtSigner {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("生成 RSA 密钥失败: %v", err)
	}
	return &jwtSigner{key: key}
}

func (s *jwtSigner) jwks() string {
	exponent := big.NewInt(int64(s.key.PublicKey.E)).Bytes()
	body, err := json.Marshal(map[string]any{
		"keys": []map[string]string{{
			"alg": "RS256",
			"kty": "RSA",
			"e":   base64.RawURLEncoding.EncodeToString(exponent),
			"n":   base64.RawURLEncoding.EncodeToString(s.key.PublicKey.N.Bytes()),
		}},
	})
	if err != nil {
		panic(err)
	}
	return string(body)
}

func (s *jwtSigner) sign(userID string) string {
	now := time.Now()
	header, _ := json.Marshal(map[string]any{"alg": "RS256", "typ": "JWT"})
	payload, _ := json.Marshal(map[string]any{
		"exp": now.Add(5 * time.Minute).Unix(),
		"iat": now.Unix(),
		"nbf": now.Add(-time.Minute).Unix(),
		"sub": userID,
	})
	signingInput := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	sum := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, s.key, crypto.SHA256, sum[:])
	if err != nil {
		panic(err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// wsClient 是自带的 WebSocket 客户端实现（RFC6455 文本帧，客户端帧加掩码）。
type wsClient struct {
	conn net.Conn
	br   *bufio.Reader
}

// dialHub 连接 /v2/ws（多路复用协议），JWT 放在查询参数里。
func dialHub(t *testing.T, port string, token string) *wsClient {
	t.Helper()
	addr := "127.0.0.1:" + port
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("连接 gateway(%s) 失败: %v", addr, err)
	}
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		t.Fatal(err)
	}
	request := "GET /v2/ws?clientId=e2e-client&token=" + url.QueryEscape(token) + " HTTP/1.1\r\n" +
		"Host: " + addr + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + base64.StdEncoding.EncodeToString(keyBytes) + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatalf("发送握手请求失败: %v", err)
	}
	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("读取握手响应失败: %v", err)
	}
	if !strings.Contains(status, "101") {
		t.Fatalf("WebSocket 握手被拒绝: %s", strings.TrimSpace(status))
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("读取握手头失败: %v", err)
		}
		if line == "\r\n" {
			break
		}
	}
	c := &wsClient{conn: conn, br: br}
	t.Cleanup(func() { _ = conn.Close() })
	return c
}

func (c *wsClient) readRaw(timeout time.Duration) ([]byte, error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(timeout))
	for {
		var header [2]byte
		if _, err := io.ReadFull(c.br, header[:]); err != nil {
			return nil, err
		}
		opcode := header[0] & 0x0f
		masked := header[1]&0x80 != 0
		length := uint64(header[1] & 0x7f)
		switch length {
		case 126:
			var ext [2]byte
			if _, err := io.ReadFull(c.br, ext[:]); err != nil {
				return nil, err
			}
			length = uint64(binary.BigEndian.Uint16(ext[:]))
		case 127:
			var ext [8]byte
			if _, err := io.ReadFull(c.br, ext[:]); err != nil {
				return nil, err
			}
			length = binary.BigEndian.Uint64(ext[:])
		}
		var mask [4]byte
		if masked {
			if _, err := io.ReadFull(c.br, mask[:]); err != nil {
				return nil, err
			}
		}
		payload := make([]byte, length)
		if _, err := io.ReadFull(c.br, payload); err != nil {
			return nil, err
		}
		if masked {
			for i := range payload {
				payload[i] ^= mask[i%4]
			}
		}
		switch opcode {
		case wsOpText:
			return payload, nil
		case wsOpClose:
			code := 0
			reason := ""
			if len(payload) >= 2 {
				code = int(binary.BigEndian.Uint16(payload[:2]))
				reason = string(payload[2:])
			}
			return nil, fmt.Errorf("连接被服务端关闭 (code=%d reason=%q)", code, reason)
		case wsOpPing:
			_ = c.writeFrame(wsOpPong, payload)
		default:
			// 忽略其他帧类型
		}
	}
}

func (c *wsClient) writeFrame(opcode byte, payload []byte) error {
	frame := []byte{0x80 | opcode}
	mask := [4]byte{0x11, 0x22, 0x33, 0x44}
	length := len(payload)
	if length < 126 {
		frame = append(frame, 0x80|byte(length))
	} else if length <= 0xffff {
		frame = append(frame, 0x80|126, byte(length>>8), byte(length))
	} else {
		frame = append(frame, 0x80|127)
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(length))
		frame = append(frame, ext[:]...)
	}
	frame = append(frame, mask[:]...)
	for i, b := range payload {
		frame = append(frame, b^mask[i%4])
	}
	written := 0
	for written < len(frame) {
		n, err := c.conn.Write(frame[written:])
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}

func (c *wsClient) writeJSON(value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.writeFrame(wsOpText, payload)
}

func (c *wsClient) close() { _ = c.conn.Close() }

// readMessage 读一条 JSON 消息，超时或断连时把已收到的原始消息
// 全部打印，方便排查。
func readMessage(t *testing.T, c *wsClient, timeout time.Duration) map[string]any {
	t.Helper()
	raw, err := c.readRaw(timeout)
	if err != nil {
		t.Fatalf("读取 WebSocket 消息失败: %v", err)
	}
	var msg map[string]any
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("消息不是合法 JSON: %s", string(raw))
	}
	return msg
}

// readUntil 逐条读取消息直到 match 命中；超时或断连时把已收到的原始消息
// 全部打印，方便排查。
func readUntil(t *testing.T, c *wsClient, match func(map[string]any) bool) map[string]any {
	t.Helper()
	var seen []string
	deadline := time.Now().Add(messageWait)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatalf("等待匹配消息超时，已收到 %d 条:\n%s", len(seen), strings.Join(seen, "\n"))
		}
		raw, err := c.readRaw(remaining)
		if err != nil {
			t.Fatalf("读取 WebSocket 消息失败: %v，已收到 %d 条:\n%s", err, len(seen), strings.Join(seen, "\n"))
		}
		seen = append(seen, string(raw))
		var msg map[string]any
		if err := json.Unmarshal(raw, &msg); err != nil {
			t.Fatalf("消息不是合法 JSON: %s", string(raw))
		}
		if match(msg) {
			return msg
		}
	}
}

func eventID(msg map[string]any) int {
	switch v := msg["id"].(type) {
	case float64:
		return int(v)
	case string:
		id, _ := strconv.Atoi(v)
		return id
	}
	return 0
}

func isAgentEvent(msg map[string]any) bool {
	return msg["type"] == "agent_event"
}

func isResumeComplete(msg map[string]any) bool {
	return msg["type"] == "resume_complete"
}

func baseURL(port string) string { return "http://127.0.0.1:" + port }

func postOperation(t *testing.T, port string, path string, body map[string]any) {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, baseURL(port)+path, bytesReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+e2eServiceToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s 失败: %v", path, err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s 返回 %d: %s", path, resp.StatusCode, string(respBody))
	}
}

func initOperation(t *testing.T, port string, operationID string) {
	t.Helper()
	postOperation(t, port, "/api/operations/init", map[string]any{
		"operationId": operationID,
		"userId":      e2eUserID,
	})
}

func pushEvent(t *testing.T, port string, operationID string, content string) {
	t.Helper()
	postOperation(t, port, "/api/operations/push-event", map[string]any{
		"operationId": operationID,
		"event": map[string]any{
			"type":        "stream_chunk",
			"stepIndex":   0,
			"timestamp":   time.Now().UnixMilli(),
			"operationId": operationID,
			"data":        map[string]any{"content": content},
		},
	})
}

// collectEvents 读满 count 条 agent_event 并返回事件 id 列表（按到达顺序）。
func collectEvents(t *testing.T, c *wsClient, count int) []int {
	t.Helper()
	ids := make([]int, 0, count)
	for len(ids) < count {
		ids = append(ids, eventID(readUntil(t, c, isAgentEvent)))
	}
	return ids
}

func bytesReader(data []byte) io.Reader { return strings.NewReader(string(data)) }
