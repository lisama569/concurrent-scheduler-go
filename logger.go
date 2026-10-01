package scheduler

import (
	"fmt"
	"strings"
)

// Logger 用于输出每次准入 / 排队 / 拒绝 / 释放的决策依据与资源快照。
// 生产环境可传 nil（内部使用空实现）；测试中注入带 t.Logf 的实现即可
// 得到可解释、可复盘的调度决策链路。每条记录都带任务所属业务组，
// 便于多租户场景按组复盘。
type Logger interface {
	Logf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Logf(format string, args ...any) {}

// fmtLimit 把 <0（无界）的资源上限显示为 inf，其余按十进制显示。
func fmtLimit(v int64) string {
	if v < 0 {
		return "inf"
	}
	return fmt.Sprintf("%d", v)
}

// logDecision 以统一格式记录一次调度决策：动作、业务组、对象、开销、
// 判定依据与决策后快照。
func logDecision(l Logger, action, group, taskID string, cost int64, basis string, snap Snapshot) {
	l.Logf(
		"DECISION action=%s group=%q task=%s cost=%d basis=%q | %s",
		action, group, taskID, cost, basis, snap,
	)
}

// String 便于在错误信息与断言中阅读快照：先全局账目，再逐组列出。
func (s Snapshot) String() string {
	var b strings.Builder
	fmt.Fprintf(&b,
		"snapshot{global concurrency=%d/%d resource=%d/%s queued=%d paused=%t stopped=%t",
		s.InFlight, s.MaxConcurrency, s.ResourceUsed, fmtLimit(s.ResourceLimit),
		s.Queued, s.Paused, s.Stopped)
	if len(s.Groups) > 0 {
		b.WriteString(" groups=[")
		for i, g := range s.Groups {
			if i > 0 {
				b.WriteByte(' ')
			}
			name := g.Name
			if name == "" {
				name = "-"
			}
			fmt.Fprintf(&b, "{%s concurrency=%d/%s resource=%d/%s queued=%d stopped=%t}",
				name, g.InFlight, fmtConc(g.MaxConcurrency), g.ResourceUsed, fmtLimit(g.ResourceLimit),
				g.Queued, g.Stopped)
		}
		b.WriteByte(']')
	}
	b.WriteByte('}')
	return b.String()
}

// fmtConc 显示组级并发上限：<=0 表示无界。
func fmtConc(v int) string {
	if v <= 0 {
		return "inf"
	}
	return fmt.Sprintf("%d", v)
}
