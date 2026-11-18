//go:build e2e

// 场景实现（LOBE-185）：真实进程 kill + restart 后的重连与终态恢复。
// 每个场景对应任务里的一条要求，失败时打印收到的原始消息。
package e2e

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// stateStoreFlushInterval 与生产实现里的批量落盘间隔一致，场景 C 用它
// 给计数器 flush 留出窗口。生产代码里的 200ms 是私有常量，这里独立声明。
const stateStoreFlushInterval = 200 * time.Millisecond

// runScenario 的公共骨架：第一次启动 → 动作（写入状态与事件）→ kill →
// 用同一个数据库文件重启 → 客户端动作（重连断言）。
type scenarioOptions struct {
	name string
	// firstRun 在第一个进程上执行（init / 推事件 / update-status）。
	// 返回客户端记录的 lastEventId，供重启后重连使用。
	firstRun func(t *testing.T, port string, opID string, signer *jwtSigner) string
	// restartRun 在重启后的进程上执行客户端断言。
	restartRun func(t *testing.T, port string, opID string, lastEventID string, signer *jwtSigner)
	// verifyLogs 在两轮进程都结束后检查真实日志行。
	verifyLogs func(t *testing.T, firstLog string, secondLog string)
}

func runScenario(t *testing.T, opts scenarioOptions) {
	t.Helper()
	dir := runDir(t)
	dbPath := filepath.Join(dir, "state.db")
	port := freePort(t)
	signer := newJWTSigner(t)
	jwks := signer.jwks()
	opID := "e2e-" + sanitize(opts.name)

	first := startGateway(t, dbPath, port, jwks, filepath.Join(dir, "gateway-1.log"))
	lastEventID := opts.firstRun(t, first.port, opID, signer)

	// 真实 kill：SIGTERM → 等待退出 → 超时 SIGKILL → 确认端口释放。
	first.stop(t)

	second := startGateway(t, dbPath, port, jwks, filepath.Join(dir, "gateway-2.log"))
	opts.restartRun(t, second.port, opID, lastEventID, signer)
	if opts.verifyLogs != nil {
		opts.verifyLogs(t, filepath.Join(dir, "gateway-1.log"), filepath.Join(dir, "gateway-2.log"))
	}
}

// subscribeResult 是一次成功的 v2 订阅。replayed 收集了 resume_complete
// 之前到达的重放事件（readUntil 会把中间消息缓存在这里），供调用方核对。
type subscribeResult struct {
	ws       *wsClient
	complete map[string]any
	replayed []map[string]any
}

// subscribe 建立 v2 连接并订阅。注意网关的重放事件与 resume_complete 一起
// 到达，readUntil 中间读到的消息都收进 replayed。
func subscribe(t *testing.T, port string, opID string, lastEventID string, signer *jwtSigner) *subscribeResult {
	t.Helper()
	// 必须复用与 startGateway 相同的 signer：gateway 进程只认启动时
	// JWKS_PUBLIC_KEY 里那把公钥，新 signer 签的 token 会被判 auth_failed。
	ws := dialHub(t, port, signer.sign(e2eUserID))
	ready := readUntil(t, ws, func(m map[string]any) bool { return m["type"] == "ready" })
	if ready["userId"] != e2eUserID {
		t.Fatalf("ready 消息 userId 不符: %v", ready)
	}
	ws.writeJSON(map[string]any{
		"type":        "subscribe",
		"operationId": opID,
		"lastEventId": lastEventID,
	})
	// 先读完整批：重放事件按序到达，resume_complete 是最后一条。
	// 这里不用 readUntil 直接跳过中间消息，而是把消息分两类记录，
	// 让调用方能同时断言事件序与 resume_complete 字段。
	var replayed []map[string]any
	var complete map[string]any
	for complete == nil {
		msg := readMessage(t, ws, messageWait)
		if isResumeComplete(msg) {
			complete = msg
			break
		}
		replayed = append(replayed, msg)
	}
	return &subscribeResult{ws: ws, complete: complete, replayed: replayed}
}

// replayedEventIDs 返回订阅批里 agent_event 的 id 序列。
func replayedEventIDs(sub *subscribeResult) []int {
	ids := make([]int, 0, len(sub.replayed))
	for _, msg := range sub.replayed {
		if isAgentEvent(msg) {
			ids = append(ids, eventID(msg))
		}
	}
	return ids
}

func pushN(t *testing.T, port string, opID string, n int, prefix string) {
	t.Helper()
	for i := 1; i <= n; i++ {
		pushEvent(t, port, opID, fmt.Sprintf("%s-%d", prefix, i))
	}
}

// 场景 A：运行中重启，客户端序号超前。
// init → 推 3 条事件 → 客户端订阅并收齐，记下 lastEventId → 杀进程 →
// 重启 → 用一个大于已刷盘 counter 的 lastEventId 重新 subscribe →
// 断言 gap=true → 继续推新事件，断言客户端能收到（证明没有被
// id <= lastSeq 过滤掉）。SIGTERM 修复后 lastEventId 不再天然超前，
// 所以这里显式报告一个超前游标来保持场景语义。
func TestE2E_A_RestartAheadClientGetsGap(t *testing.T) {
	runScenario(t, scenarioOptions{
		name: "ahead-client",
		firstRun: func(t *testing.T, port string, opID string, signer *jwtSigner) string {
			initOperation(t, port, opID)
			pushN(t, port, opID, 3, "before")

			sub := subscribe(t, port, opID, "", signer)
			ids := replayedEventIDs(sub)
			if len(ids) != 3 {
				t.Fatalf("首个进程应重放 3 条事件，实际收到 ids=%v", ids)
			}
			sub.ws.close()
			// 显式报告超前序号：3 条事件的 counter 已由 SIGTERM 刷盘，
			// 用 99 模拟客户端见过更新 gateway 的事件。
			return "99"
		},
		restartRun: func(t *testing.T, port string, opID string, lastEventID string, signer *jwtSigner) {
			sub := subscribe(t, port, opID, lastEventID, signer)
			if sub.complete["gap"] != true {
				t.Fatalf("场景A失败：客户端序号超前，resume_complete 应带 gap=true，实际收到: %v", sub.complete)
			}
			// gap=true 时游标已回绕为 0，旧事件会被重放；继续推新事件，
			// 客户端必须收到（证明没有被 id <= lastSeq 过滤掉）。
			pushN(t, port, opID, 2, "after")
			ids := append(replayedEventIDs(sub), collectEvents(t, sub.ws, 2)...)
			if len(ids) < 2 {
				t.Fatalf("场景A失败：重启后应至少收到 2 条新事件，实际 ids=%v", ids)
			}
			tail := ids[len(ids)-2:]
			if tail[0] == tail[1] {
				t.Fatalf("场景A失败：重启后事件 id 重复，实际收到 ids=%v", ids)
			}
		},
		verifyLogs: func(t *testing.T, firstLog string, secondLog string) {
			for _, entry := range []struct {
				path string
				text string
			}{
				{firstLog, `"msg":"[agent] state store enabled"`},
				{secondLog, `"msg":"[agent] state store enabled"`},
				{secondLog, `"msg":"[agent] operation state restored"`},
				{secondLog, `"msg":"[agent] event replay gap detected"`},
			} {
				if !logContains(t, entry.path, entry.text) {
					t.Errorf("场景A日志缺少 %q\n日志尾部:\n%s", entry.text, tailFile(entry.path, 40))
				}
			}
		},
	})
}

// 场景 B：终态重启，客户端拿到结束。
// init → 推事件 → update-status completed → 杀进程 → 重启 → 订阅 →
// 断言 resume_complete 的 status 为 completed。
func TestE2E_B_TerminalStatusSurvivesRestart(t *testing.T) {
	runScenario(t, scenarioOptions{
		name: "terminal-status",
		firstRun: func(t *testing.T, port string, opID string, signer *jwtSigner) string {
			initOperation(t, port, opID)
			pushN(t, port, opID, 2, "work")

			sub := subscribe(t, port, opID, "", signer)
			ids := replayedEventIDs(sub)
			if len(ids) != 2 {
				t.Fatalf("首个进程应重放 2 条事件，实际收到 ids=%v", ids)
			}
			sub.ws.close()

			postOperation(t, port, "/api/operations/update-status", map[string]any{
				"operationId": opID,
				"status":      "completed",
				"summary":     "e2e done",
			})
			return fmt.Sprintf("%d", ids[len(ids)-1])
		},
		restartRun: func(t *testing.T, port string, opID string, lastEventID string, signer *jwtSigner) {
			sub := subscribe(t, port, opID, lastEventID, signer)
			status, _ := sub.complete["status"].(string)
			if status != "completed" {
				t.Fatalf("场景B失败：重启后 resume_complete.status 应为 completed，实际收到: %v", sub.complete)
			}
		},
		verifyLogs: func(t *testing.T, firstLog string, secondLog string) {
			for _, entry := range []struct {
				path string
				text string
			}{
				{firstLog, `"msg":"[agent] state store enabled"`},
				{secondLog, `"msg":"[agent] operation state restored"`},
				{secondLog, `"msg":"[agent] terminal operation restored"`},
			} {
				if !logContains(t, entry.path, entry.text) {
					t.Errorf("场景B日志缺少 %q\n日志尾部:\n%s", entry.text, tailFile(entry.path, 40))
				}
			}
		},
	})
}

// 场景 C：运行态不撒谎。
// init（状态 running）→ 杀进程 → 重启 → 订阅 → 断言 resume_complete 的
// status 不是 waiting_for_human，且 event_counter 已恢复（后续推事件的
// id 接着之前的编号，不回到 1）。
//
// 场景 C 与场景 L 的区别：C 显式等过 flush 窗口，验证恢复后的序号延续；
// L 不等窗口，直接验证 SIGTERM 路径会把最后一批 pending 计数器刷盘。
func TestE2E_C_RunningStatusDoesNotLie(t *testing.T) {
	runScenario(t, scenarioOptions{
		name: "running-no-lie",
		firstRun: func(t *testing.T, port string, opID string, signer *jwtSigner) string {
			initOperation(t, port, opID)
			pushN(t, port, opID, 4, "pre")
			// 给 200ms 批量 flush 留出窗口，让计数器有机会落盘。
			time.Sleep(stateStoreFlushInterval + 300*time.Millisecond)
			return "4"
		},
		restartRun: func(t *testing.T, port string, opID string, lastEventID string, signer *jwtSigner) {
			sub := subscribe(t, port, opID, lastEventID, signer)
			if status, _ := sub.complete["status"].(string); status == "waiting_for_human" {
				t.Fatalf("场景C失败：重启后不得复活 waiting_for_human，实际收到: %v", sub.complete)
			}
			// event_counter 已恢复：重启后第一条事件 id 应接着 4 之后。
			pushN(t, port, opID, 1, "post")
			ids := collectEvents(t, sub.ws, 1)
			if ids[0] != 5 {
				t.Fatalf("场景C失败：重启后事件 id 应接着之前的编号（期望 5），实际收到 ids=%v", ids)
			}
		},
	})
}

// 场景 D：正常路径，不重启。
// 推一串事件，客户端按顺序全部收到，没有重复没有丢失。
func TestE2E_D_NormalPathOrderedDelivery(t *testing.T) {
	dir := runDir(t)
	dbPath := filepath.Join(dir, "state.db")
	port := freePort(t)
	signer := newJWTSigner(t)
	opID := "e2e-normal-path"

	proc := startGateway(t, dbPath, port, signer.jwks(), filepath.Join(dir, "gateway.log"))

	initOperation(t, port, opID)
	const total = 10
	pushN(t, port, opID, total, "seq")

	sub := subscribe(t, port, opID, "", signer)
	ids := replayedEventIDs(sub)
	if len(ids) != total {
		t.Fatalf("场景D失败：应收到 %d 条事件，实际 ids=%v", total, ids)
	}
	for i, id := range ids {
		if id != i+1 {
			t.Fatalf("场景D失败：事件应按 1..%d 顺序送达，实际 ids=%v", total, ids)
		}
	}
	proc.stop(t)
}

// 场景 L（LOBE-187）：SIGTERM 退出时刷盘计数器。
// 推 N 条事件后立刻发 SIGTERM（不等 200ms 批量 flush），等进程退出后
// 直接读 SQLite：operation_state.event_counter 必须等于 N。再启动新进程，
// 订阅并重放时事件 id 必须接着 N，不能从旧值回绕。
func TestE2E_L_SIGTERMFlushesCounter(t *testing.T) {
	dir := runDir(t)
	dbPath := filepath.Join(dir, "state.db")
	port := freePort(t)
	signer := newJWTSigner(t)
	opID := "e2e-sigterm-flush"
	const total = 5

	first := startGateway(t, dbPath, port, signer.jwks(), filepath.Join(dir, "gateway-1.log"))
	initOperation(t, port, opID)
	pushN(t, port, opID, total, "sigterm")
	first.stop(t)

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("场景L失败：打开状态库失败 (db=%s): %v", dbPath, err)
	}
	defer db.Close()
	var counter int
	if err := db.QueryRow(`SELECT event_counter FROM operation_state WHERE id = ?`, opID).Scan(&counter); err != nil {
		t.Fatalf("场景L失败：读取 event_counter 失败 (db=%s): %v", dbPath, err)
	}
	if counter != total {
		t.Fatalf("场景L失败：SIGTERM 后 event_counter 应为 %d，实际 %d (db=%s)", total, counter, dbPath)
	}

	second := startGateway(t, dbPath, port, signer.jwks(), filepath.Join(dir, "gateway-2.log"))
	defer second.stop(t)
	sub := subscribe(t, second.port, opID, "0", signer)
	pushEvent(t, second.port, opID, "after-sigterm")
	ids := append(replayedEventIDs(sub), collectEvents(t, sub.ws, 1)...)
	if len(ids) == 0 || ids[len(ids)-1] != total+1 {
		t.Fatalf("场景L失败：重启后新事件 id 应为 %d（counter=%d），实际 ids=%v (db=%s)", total+1, counter, ids, dbPath)
	}
}
