package scheduler

import "fmt"

// Logger 用于输出每次准入 / 排队 / 拒绝 / 释放的决策依据与资源快照。
// 生产环境可传 nil（内部使用空实现）；测试中注入带 t.Logf 的实现即可
// 得到可解释、可复盘的调度决策链路。
type Logger interface {
	Logf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Logf(format string, args ...any) {}

// logDecision 以统一格式记录一次调度决策：动作、所属业务组、对象、
// 判定依据与决策后快照。每条准入/排队/拒绝/释放记录都带组名，便于复盘。
func logDecision(l Logger, action, group, taskID string, cost int64, basis string, snap Snapshot) {
	l.Logf(
		"DECISION action=%s group=%q task=%s cost=%d basis=%q | snapshot{concurrency=%d/%d resource=%d/%d queued=%d paused=%t stopped=%t groups=%d}",
		action, group, taskID, cost, basis,
		snap.InFlight, snap.MaxConcurrency, snap.ResourceUsed, snap.ResourceLimit,
		snap.Queued, snap.Paused, snap.Stopped, len(snap.Groups))
}

// String 便于在错误信息与断言中阅读快照。
func (s Snapshot) String() string {
	return fmt.Sprintf(
		"snapshot{concurrency=%d/%d resource=%d/%d queued=%d paused=%t stopped=%t}",
		s.InFlight, s.MaxConcurrency, s.ResourceUsed, s.ResourceLimit,
		s.Queued, s.Paused, s.Stopped,
	)
}
