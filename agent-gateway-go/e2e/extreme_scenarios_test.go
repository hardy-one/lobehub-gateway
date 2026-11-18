//go:build e2e

// 极端场景（LOBE-186）：SIGKILL 崩溃恢复、损坏库、非法路径、脏 lastEventId、
// cleanup 后重订阅、多 operation 恢复、v1+v2 并发订阅。
// 全部复用 LOBE-185 harness 的真实进程与真实 WebSocket 流量，不引用 internal/gateway。
// 每个场景独立可跑（go test -run 单选），不依赖执行顺序，不残留进程和临时文件。
package e2e

import (
	"bufio"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// hardKill 用 SIGKILL 直接杀死进程，模拟 OOM / docker kill / 断电。
// 与 terminate 不同：不走 SIGTERM，不给优雅退出机会；并置 stopped=true，
// 让 t.Cleanup 里的 terminate 直接跳过（进程已死，避免每次多等 10 秒）。
func hardKill(t *testing.T, p *gatewayProcess) {
	t.Helper()
	if p.cmd.Process == nil {
		t.Fatal("gateway 进程从未启动，无法 SIGKILL")
	}
	pid := p.cmd.Process.Pid
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatalf("SIGKILL 进程 %d 失败: %v", pid, err)
	}
	select {
	case <-p.exited:
	case <-time.After(stopWait):
		t.Fatalf("SIGKILL 后进程 %d 在 %s 内仍未退出", pid, stopWait)
	}
	p.stopped = true
	waitPortClosed(t, p.port, portReleaseWait)
}

// logContains 检查 gateway 日志中是否出现指定子串（用于断言降级 warn）。
func logContains(t *testing.T, path string, substr string) bool {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 gateway 日志 %s 失败: %v", path, err)
	}
	return strings.Contains(string(data), substr)
}

// writeGarbageDB 把数据库文件写成一段非 SQLite 的垃圾字节。
func writeGarbageDB(t *testing.T, path string) {
	t.Helper()
	junk := []byte{
		0x50, 0x4f, 0x53, 0x54, 0x47, 0x52, 0x45, 0x53, // "POSTGRES" 伪造魔数
		0xde, 0xad, 0xbe, 0xef, 0x00, 0x01, 0x02, 0x03,
		0xff, 0xee, 0xdd, 0xcc, 0x74, 0x65, 0x73, 0x74,
	}
	if err := os.WriteFile(path, junk, 0o644); err != nil {
		t.Fatalf("写入垃圾数据库文件失败: %v", err)
	}
}

// deleteOperationRow 直接从 SQLite 删除 operation 行，等价复现 cleanup()
// 删除数据库行的那一半（内存对象那一半由进程停止/重启复现）。
func deleteOperationRow(t *testing.T, dbPath string, opID string) {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("打开 %s 失败: %v", dbPath, err)
	}
	defer db.Close()
	if _, err := db.Exec(`DELETE FROM operation_state WHERE id = ?`, opID); err != nil {
		t.Fatalf("删除 operation_state 行失败 (db=%s op=%s): %v", dbPath, opID, err)
	}
}

// dialV1 建立 v1 连接（/ws?operationId=...）。鉴权走连接后的首条 auth 消息。
func dialV1(t *testing.T, port string, opID string) *wsClient {
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
	request := "GET /ws?operationId=" + url.QueryEscape(opID) + " HTTP/1.1\r\n" +
		"Host: " + addr + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + base64.StdEncoding.EncodeToString(keyBytes) + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatalf("发送握手请求失败: %v", err)
	}
	br := readHandshakeResponse(t, conn)
	c := &wsClient{conn: conn, br: br}
	t.Cleanup(func() { _ = conn.Close() })
	return c
}

// readHandshakeResponse 读取 v1 握手响应头，校验 101 并消费到空行。
func readHandshakeResponse(t *testing.T, conn net.Conn) *bufio.Reader {
	t.Helper()
	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatalf("读取握手响应失败: %v", err)
	}
	if !strings.Contains(status, "101") {
		t.Fatalf("v1 WebSocket 握手被拒绝: %s", strings.TrimSpace(status))
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
	return br
}

// v1Auth 发送 service-token 鉴权并等待 auth_success。
func v1Auth(t *testing.T, ws *wsClient) {
	t.Helper()
	ws.writeJSON(map[string]any{"type": "auth", "token": e2eServiceToken})
	msg := readMessage(t, ws, messageWait)
	if msg["type"] != "auth_success" {
		t.Fatalf("v1 鉴权失败，实际收到: %v", msg)
	}
}

// v1ResumeAndCollect 发送 resume 并收集重放事件，返回事件 id 列表与 resume_complete。
func v1ResumeAndCollect(t *testing.T, ws *wsClient, lastEventID string) ([]int, map[string]any) {
	t.Helper()
	ws.writeJSON(map[string]any{"type": "resume", "lastEventId": lastEventID, "wantStatus": true})
	var ids []int
	var complete map[string]any
	for complete == nil {
		msg := readMessage(t, ws, messageWait)
		if isResumeComplete(msg) {
			complete = msg
			break
		}
		if isAgentEvent(msg) {
			ids = append(ids, eventID(msg))
		}
	}
	return ids, complete
}

// heartbeatAlive 向连接发心跳并等 ack，证明连接未被服务端踢掉。
func heartbeatAlive(t *testing.T, ws *wsClient) {
	t.Helper()
	ws.writeJSON(map[string]any{"type": "heartbeat"})
	msg := readMessage(t, ws, messageWait)
	if msg["type"] != "heartbeat_ack" {
		t.Fatalf("心跳未收到 heartbeat_ack，连接可能已被踢掉，实际收到: %v", msg)
	}
}

// assertBasicFlow 验证降级模式下服务完全可用：init → 推 3 条 → 订阅重放
// 1..3 → 再推 1 条能收到。任何一步失败都说明进程崩溃或卡死。
func assertBasicFlow(t *testing.T, port string, opID string, signer *jwtSigner) {
	t.Helper()
	initOperation(t, port, opID)
	pushN(t, port, opID, 3, "flow")
	sub := subscribe(t, port, opID, "", signer)
	if ids := replayedEventIDs(sub); len(ids) != 3 {
		t.Fatalf("降级模式下应重放 3 条事件 (db 场景见日志)，实际 ids=%v", ids)
	}
	pushN(t, port, opID, 1, "after")
	if ids := collectEvents(t, sub.ws, 1); len(ids) != 1 {
		t.Fatalf("降级模式下新事件应送达，实际 ids=%v", ids)
	}
	sub.ws.close()
}

// 场景 E：SIGKILL 崩溃恢复（kill -9，模拟 OOM / docker kill / 断电）。
//
// 阶段 A：终态 status 已同步落盘后 kill -9 → 重启 → /health 200 →
//
//	终态仍可读（resume_complete.status == completed）。
//
// 阶段 B：运行中（无终态）立即 kill -9，计数器可能未落盘（允许偏小）。
//
//	客户端带旧 lastEventId=3 订阅：
//	- 若计数器未落盘：必须拿到 gap=true，且后续事件能收到；
//	- 若计数器恰好在 kill 前 flush：无 gap 属实，重放应为空，
//	  后续事件同样必须送达。
//	两种结果都不得崩溃、不得饿死客户端。
func TestE2E_E_SigKillCrashRecovery(t *testing.T) {
	dir := runDir(t)
	dbPath := filepath.Join(dir, "state.db")
	port := freePort(t)
	signer := newJWTSigner(t)
	jwks := signer.jwks()

	// —— 阶段 A：终态落盘后 kill -9 ——
	opA := "e2e-sigkill-terminal"
	first := startGateway(t, dbPath, port, jwks, filepath.Join(dir, "gateway-1.log"))
	initOperation(t, first.port, opA)
	pushN(t, first.port, opA, 2, "work")
	postOperation(t, first.port, "/api/operations/update-status", map[string]any{
		"operationId": opA,
		"status":      "completed",
		"summary":     "killed after terminal",
	})
	hardKill(t, first)
	// kill -9 不做 checkpoint，WAL 与 -shm 应留在磁盘上（真实崩溃现场）
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(dbPath + suffix); err != nil {
			t.Fatalf("场景E失败：SIGKILL 后 %s%s 应存在: %v", dbPath, suffix, err)
		}
	}

	second := startGateway(t, dbPath, port, jwks, filepath.Join(dir, "gateway-2.log"))
	subA := subscribe(t, second.port, opA, "0", signer)
	if status, _ := subA.complete["status"].(string); status != "completed" {
		t.Fatalf("场景E失败：kill -9 后已落盘终态应可读（期望 completed），实际: %v", subA.complete)
	}
	subA.ws.close()

	// —— 阶段 B：运行中 kill -9，不 sleep，计数器可能未 flush ——
	opB := "e2e-sigkill-running"
	initOperation(t, second.port, opB)
	pushN(t, second.port, opB, 3, "run") // 事件 id 1..3
	hardKill(t, second)

	third := startGateway(t, dbPath, port, jwks, filepath.Join(dir, "gateway-3.log"))
	subB := subscribe(t, third.port, opB, "3", signer)
	if gap, _ := subB.complete["gap"].(bool); gap {
		// 计数器未落盘（允许偏小）：gap=true，游标回绕，后续事件必须能收到
		pushN(t, third.port, opB, 2, "after-gap")
		ids := append(replayedEventIDs(subB), collectEvents(t, subB.ws, 2)...)
		if len(ids) < 2 {
			t.Fatalf("场景E失败：gap=true 后新事件必须送达 (db=%s)，实际 ids=%v", dbPath, ids)
		}
		tail := ids[len(ids)-2:]
		if tail[0] == tail[1] {
			t.Fatalf("场景E失败：重启后事件 id 重复 (db=%s)，实际收到 ids=%v", dbPath, ids)
		}
	} else {
		// 计数器恰好在 kill 前已 flush：无 gap 属实（任务允许），但仍不得饿死
		t.Logf("场景E阶段B：计数器在 kill 前已 flush（gap=false，db=%s）", dbPath)
		pushN(t, third.port, opB, 1, "after")
		if ids := collectEvents(t, subB.ws, 1); len(ids) != 1 {
			t.Fatalf("场景E失败：gap=false 分支新事件必须送达 (db=%s)", dbPath)
		}
	}
}

// 场景 F：数据库文件损坏，两种子情况。
//
// 期望（任务原文）：进程正常启动、日志有 warn、退回内存模式、/health 正常、
// 不 panic 不退出。
//
// 缺陷修复记录：[LOBE-186-DEFECT-1]（0 字节文件被 SQLite 当合法空库静默
// 重建，无 warn、不降级）已通过 openOperationStateDB 前置 os.Stat 检查修复
// —— size==0 时返回错误，走与损坏文件一致的降级路径。两个子情况均按任务
// 原文断言，无跳过、无放宽。
func TestE2E_F_CorruptStateDB(t *testing.T) {
	dir := runDir(t)
	port := freePort(t)
	signer := newJWTSigner(t)

	// 子情况 1：垃圾字节
	garbagePath := filepath.Join(dir, "garbage.db")
	writeGarbageDB(t, garbagePath)
	proc := startGateway(t, garbagePath, port, signer.jwks(), filepath.Join(dir, "gateway-garbage.log"))
	if !logContains(t, proc.logPath, "state store degraded to in-memory mode") {
		t.Fatalf("场景F失败：垃圾字节库应触发降级 warn (db=%s)，日志尾部:\n%s", garbagePath, tailFile(proc.logPath, 40))
	}
	assertBasicFlow(t, proc.port, "e2e-garbage-op", signer)
	proc.waitReady(t, healthWait) // 场景末尾再确认 /health 200、进程未退出
	proc.stop(t)

	// 子情况 2：文件截断为 0 字节（任务期望：warn + 内存模式，
	// [LOBE-186-DEFECT-1] 已修复，见函数头注释）
	zeroPath := filepath.Join(dir, "zero.db")
	if err := os.WriteFile(zeroPath, nil, 0o644); err != nil {
		t.Fatalf("写入 0 字节数据库文件失败: %v", err)
	}
	proc2 := startGateway(t, zeroPath, port, signer.jwks(), filepath.Join(dir, "gateway-zero.log"))
	if !logContains(t, proc2.logPath, "state store degraded to in-memory mode") {
		t.Fatalf("场景F失败：0 字节库应触发降级 warn (db=%s)，日志尾部:\n%s", zeroPath, tailFile(proc2.logPath, 40))
	}
	assertBasicFlow(t, proc2.port, "e2e-zero-op", signer)
	proc2.waitReady(t, healthWait)
}

// 场景 G：GATEWAY_STATE_DB 指向非法路径，两种子情况。
// 期望：启动成功、降级 warn、/health 正常、不崩溃，基础读写流程可用。
func TestE2E_G_IllegalStateDBPath(t *testing.T) {
	dir := runDir(t)
	port := freePort(t)
	signer := newJWTSigner(t)

	// 子情况 1：路径指向一个已存在的目录
	dirAsDB := filepath.Join(dir, "state.db-dir")
	if err := os.Mkdir(dirAsDB, 0o755); err != nil {
		t.Fatal(err)
	}
	proc := startGateway(t, dirAsDB, port, signer.jwks(), filepath.Join(dir, "gateway-dirpath.log"))
	if !logContains(t, proc.logPath, "state store degraded to in-memory mode") {
		t.Fatalf("场景G失败：指向目录的 DB 路径应触发降级 warn，日志尾部:\n%s", tailFile(proc.logPath, 40))
	}
	assertBasicFlow(t, proc.port, "e2e-dirpath-op", signer)
	proc.waitReady(t, healthWait)
	proc.stop(t)

	// 子情况 2：路径指向一个不可写的目录（0555）
	roDir := filepath.Join(dir, "readonly")
	if err := os.Mkdir(roDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(roDir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(roDir, 0o755) }) // 恢复权限，保证目录可被清理
	roPath := filepath.Join(roDir, "state.db")
	proc2 := startGateway(t, roPath, port, signer.jwks(), filepath.Join(dir, "gateway-readonly.log"))
	if !logContains(t, proc2.logPath, "state store degraded to in-memory mode") {
		t.Fatalf("场景G失败：不可写目录应触发降级 warn，日志尾部:\n%s", tailFile(proc2.logPath, 40))
	}
	assertBasicFlow(t, proc2.port, "e2e-readonly-op", signer)
	proc2.waitReady(t, healthWait)
}

// 场景 H：脏 lastEventId。期望（已在测试中写明并断言）：
//   - "abc"                  → 解析失败按 0 处理：从 0 重放（gap=false，ids=[1,2,3]）
//   - "-5"                   → Sscanf 解析成功得负游标。负游标 < 全部事件：
//     gap=true 且重放 [1,2,3]，客户端拿到完整数据，未饿死。
//   - "9999999999999999999"  → fmt.Sscanf 对超出 int 的值直接报错返回 0，
//     不会溢出成 MaxInt：从 0 重放（gap=false，ids=[1,2,3]）
//   - ""                     → 空串按 0 处理：从 0 重放（gap=false，ids=[1,2,3]）
//
// 每种取值后都要能继续收到新事件（不崩溃、不饿死），事件 id 连续递增。
func TestE2E_H_DirtyLastEventId(t *testing.T) {
	dir := runDir(t)
	dbPath := filepath.Join(dir, "state.db")
	port := freePort(t)
	signer := newJWTSigner(t)
	opID := "e2e-dirty-cursor"

	proc := startGateway(t, dbPath, port, signer.jwks(), filepath.Join(dir, "gateway.log"))
	initOperation(t, port, opID)
	pushN(t, port, opID, 3, "seed") // 事件 id 1..3

	cases := []struct {
		name    string
		cursor  string
		wantGap bool
	}{
		{"非数字", "abc", false},
		{"负数", "-5", true},
		{"19位超大整数", "9999999999999999999", false},
		{"空字符串", "", false},
	}
	for i, tc := range cases {
		sub := subscribe(t, port, opID, tc.cursor, signer)
		if gap, _ := sub.complete["gap"].(bool); gap != tc.wantGap {
			t.Fatalf("场景H失败：lastEventId=%q（%s）期望 gap=%v，实际收到: %v", tc.cursor, tc.name, tc.wantGap, sub.complete)
		}
		// 不饿死：重放 + 新事件都要到达。负游标重放的是 1..3 加上本轮推的
		// 新事件（游标回绕语义在 gap=true 时不生效，这里如实断言「重放+
		// 新事件都能收到」）；其余取值恰好重放 3 条。
		if ids := replayedEventIDs(sub); len(ids) < 3 {
			t.Fatalf("场景H失败：lastEventId=%q（%s）应至少重放 3 条事件，实际 ids=%v", tc.cursor, tc.name, ids)
		}
		pushEvent(t, port, opID, "dirty-"+tc.name)
		wantID := 4 + i
		if ids := collectEvents(t, sub.ws, 1); len(ids) != 1 || ids[0] != wantID {
			t.Fatalf("场景H失败：lastEventId=%q（%s）后新事件 id 应为 %d，实际 ids=%v", tc.cursor, tc.name, wantID, ids)
		}
		sub.ws.close()
	}
	proc.waitReady(t, healthWait) // 全部脏输入处理完，进程仍健康
}

// 场景 I：operation 被 cleanup 后客户端再订阅（「10 分钟后重连」场景）。
//
// 真实 cleanup 需要 10 分钟（cleanupDelay 不可配），这里用等价状态复现：
// SIGTERM 停止（内存对象消失）+ 直接删除 SQLite 行（cleanup 的另一半），
// 重启后带旧游标订阅 = cleanup 后重订阅。
//
// 期望（LOBE-186 复核后的条件式 gap）：gateway 新建空 operation；客户端
// 带旧游标（since > 0）时必须返回 gap=true，不崩溃。全新订阅（since=0）
// 才保持 gap=false（由生产单测覆盖）。
//
// 生产实现：v2 owner=="" 分支按 since > 0 返回 gap。生产单测
// TestV2PendingSubscribeGapDependsOnCursor 覆盖 since=0 / since>0 两种情形。
func TestE2E_I_ResubscribeAfterCleanup(t *testing.T) {
	dir := runDir(t)
	dbPath := filepath.Join(dir, "state.db")
	port := freePort(t)
	signer := newJWTSigner(t)
	opID := "e2e-after-cleanup"

	first := startGateway(t, dbPath, port, signer.jwks(), filepath.Join(dir, "gateway-1.log"))
	initOperation(t, port, opID)
	pushN(t, port, opID, 3, "before")
	postOperation(t, port, "/api/operations/update-status", map[string]any{
		"operationId": opID,
		"status":      "completed",
		"summary":     "done before cleanup",
	})
	first.stop(t)
	deleteOperationRow(t, dbPath, opID)

	second := startGateway(t, dbPath, port, jwksOf(signer), filepath.Join(dir, "gateway-2.log"))
	sub := subscribe(t, second.port, opID, "3", signer)
	if sub.complete["gap"] != true {
		t.Fatalf("场景I失败：cleanup 后旧游标重订阅应返回 gap=true (db=%s)，实际收到: %v", dbPath, sub.complete)
	}
	// 不崩溃：init 后 pending 订阅被提升，推送事件客户端必须收到
	initOperation(t, second.port, opID)
	pushEvent(t, second.port, opID, "after-cleanup")
	if ids := collectEvents(t, sub.ws, 1); len(ids) != 1 {
		t.Fatalf("场景I失败：cleanup 后重建的 operation 必须能继续推事件 (db=%s)", dbPath)
	}
}

// jwksOf 只是让场景 I 的调用行保持简短的可读别名。
func jwksOf(signer *jwtSigner) string { return signer.jwks() }

// 场景 J：多个 operation 同时恢复。completed / running / waiting_for_human
// 三种状态写进同一个数据库，重启后逐个订阅：
//   - completed 的返回 completed；
//   - running 的 status 为空、counter 已恢复（新事件 id 接着旧编号）；
//   - waiting_for_human 的 status 为空（不得复活等待态）；
//   - 三者计数器互不串（各自的新事件 id 都从自己的 counter 继续）。
func TestE2E_J_MultiOperationRestore(t *testing.T) {
	dir := runDir(t)
	dbPath := filepath.Join(dir, "state.db")
	port := freePort(t)
	signer := newJWTSigner(t)
	jwks := signer.jwks()

	opCompleted := "e2e-multi-completed"
	opRunning := "e2e-multi-running"
	opWaiting := "e2e-multi-waiting"

	first := startGateway(t, dbPath, port, jwks, filepath.Join(dir, "gateway-1.log"))
	initOperation(t, first.port, opCompleted)
	pushN(t, first.port, opCompleted, 1, "c")
	postOperation(t, first.port, "/api/operations/update-status", map[string]any{
		"operationId": opCompleted, "status": "completed", "summary": "j",
	})
	initOperation(t, first.port, opRunning)
	pushN(t, first.port, opRunning, 2, "r")
	// 不 sleep：running 的 2 条事件计数器靠 SIGTERM 优雅退出刷盘，
	// 重启后 counter=2，新事件 id 应从 3 接着编号。
	initOperation(t, first.port, opWaiting)
	pushN(t, first.port, opWaiting, 1, "w")
	postOperation(t, first.port, "/api/operations/update-status", map[string]any{
		"operationId": opWaiting, "status": "waiting_for_human", "summary": "j",
	})
	first.stop(t)

	second := startGateway(t, dbPath, port, jwks, filepath.Join(dir, "gateway-2.log"))

	subC := subscribe(t, second.port, opCompleted, "0", signer)
	if status, _ := subC.complete["status"].(string); status != "completed" {
		t.Fatalf("场景J失败：completed 的 operation 重启后应返回 completed，实际: %v", subC.complete)
	}
	subC.ws.close()

	subR := subscribe(t, second.port, opRunning, "0", signer)
	if status, _ := subR.complete["status"].(string); status != "" {
		t.Fatalf("场景J失败：running 的 operation 重启后 status 应为空，实际: %v", subR.complete)
	}
	// gap=true 时游标回绕，旧缓冲事件（重启后 buffer 为空则无）先重放，
	// 再收新事件。running 落盘 counter=2，重启后新事件 id 为 3；
	// 关键是三者各自独立、不串号。
	pushEvent(t, second.port, opRunning, "resume-r")
	idsR := collectEvents(t, subR.ws, 1)
	if len(idsR) != 1 || idsR[0] != 3 {
		t.Fatalf("场景J失败：running 重启后新事件 id 应为 3（SIGTERM 已刷盘 counter=2），(db=%s)，实际 ids=%v", dbPath, idsR)
	}

	subW := subscribe(t, second.port, opWaiting, "0", signer)
	if status, _ := subW.complete["status"].(string); status != "" {
		t.Fatalf("场景J失败：waiting_for_human 的 operation 重启后 status 应为空（不得复活等待态），实际: %v", subW.complete)
	}
	pushEvent(t, second.port, opWaiting, "resume-w")
	idsW := collectEvents(t, subW.ws, 1)
	// waiting 落盘 counter=2（1 条事件 + update-status 的事件 id 都由
	// SIGTERM 刷盘），新事件 id 应为 3，接续自己的落盘值，不与 running 串号。
	if len(idsW) != 1 || idsW[0] != 3 {
		t.Fatalf("场景J失败：waiting 的 counter 应独立恢复（新事件 id 应为 3），(db=%s)，实际 ids=%v", dbPath, idsW)
	}
}

// 场景 K：重启后 v1（/ws）与 v2（/v2/ws）同时订阅同一个 operation。
//
// 协议语义（写明后断言）：缓冲事件不落盘（README Deliberate trade-offs），
// 重启后 v1 的 resume 拿不到旧缓冲事件，但 must 拿到 resume_complete
// （status=completed）；v2 拿到 resume_complete（status=completed）。
// 两边互不踢掉、不死锁（各自心跳仍有 ack），且新推的事件两边都能收到。
func TestE2E_K_V1V2SimultaneousSubscribe(t *testing.T) {
	dir := runDir(t)
	dbPath := filepath.Join(dir, "state.db")
	port := freePort(t)
	signer := newJWTSigner(t)
	opID := "e2e-v1-v2-same-op"

	first := startGateway(t, dbPath, port, signer.jwks(), filepath.Join(dir, "gateway-1.log"))
	initOperation(t, first.port, opID)
	pushN(t, first.port, opID, 2, "work")
	postOperation(t, first.port, "/api/operations/update-status", map[string]any{
		"operationId": opID, "status": "completed", "summary": "k",
	})
	first.stop(t)

	second := startGateway(t, dbPath, port, signer.jwks(), filepath.Join(dir, "gateway-2.log"))

	// 先建立 v1 并发出 resume，再建立 v2 订阅：两个协议同时挂在同一 operation 上
	v1 := dialV1(t, second.port, opID)
	v1Auth(t, v1)
	v1.writeJSON(map[string]any{"type": "resume", "lastEventId": "0", "wantStatus": true})

	subV2 := subscribe(t, second.port, opID, "0", signer)
	if status, _ := subV2.complete["status"].(string); status != "completed" {
		t.Fatalf("场景K失败：v2 应拿到 completed，实际: %v", subV2.complete)
	}

	// v1 端：缓冲事件不落盘，重放为空，但必须收到 resume_complete
	// （status=completed）。重启后 buffer 为空、counter=3 > since=0，
	// v1 语义如实报告 gap=true（客户端缺 1..2，需从 LobeHub 存储补）。
	idsV1, completeV1 := v1ResumeAndCollect(t, v1, "0")
	if len(idsV1) != 0 {
		t.Fatalf("场景K失败：重启后 v1 缓冲事件不落盘，重放应为空，实际 ids=%v", idsV1)
	}
	if status, _ := completeV1["status"].(string); status != "completed" {
		t.Fatalf("场景K失败：v1 应拿到 completed，实际: %v", completeV1)
	}

	// 互不踢掉：两边心跳都还有 ack，说明没有死锁或单边被关闭。
	// 注意：v1 的 resume 是异步处理的，resume_complete 可能晚到，
	// 心跳断言要跳过迟到的 resume_complete。
	heartbeatV1 := func() {
		t.Helper()
		v1.writeJSON(map[string]any{"type": "heartbeat"})
		for {
			msg := readMessage(t, v1, messageWait)
			if msg["type"] == "heartbeat_ack" {
				return
			}
			if msg["type"] == "resume_complete" {
				continue // 迟到的 resume 响应，跳过
			}
			t.Fatalf("场景K失败：v1 心跳未收到 heartbeat_ack，实际收到: %v", msg)
		}
	}
	heartbeatV1()
	heartbeatAlive(t, subV2.ws)

	// 新事件两边都必须收到，且互不干扰。v1 的 resume 允许 late
	// resume_complete（wantStatus 下 status 只在有值时才发），所以 v1 端
	// 先跳过可能迟到的 resume_complete 再收事件。
	pushEvent(t, second.port, opID, "after-both")
	eventV2 := readUntil(t, subV2.ws, isAgentEvent)
	if idV2 := eventID(eventV2); idV2 != 4 {
		t.Fatalf("场景K失败：v2 新事件 id 应为 4（SIGTERM 已刷盘 counter=3），实际: %v", eventV2)
	}
	// v1 端在 resume 之后也会收到该事件的直播投递
	eventV1 := readUntil(t, v1, func(m map[string]any) bool {
		return isAgentEvent(m)
	})
	if idV1 := eventID(eventV1); idV1 != 4 {
		t.Fatalf("场景K失败：v1 新事件 id 应为 4，实际: %v", eventV1)
	}
}
