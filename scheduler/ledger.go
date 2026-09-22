package scheduler

import "fmt"

// ledger 记录已占用资源与运行中任务数。
// 本身不加锁：所有访问都在持有 scheduler.mu 时进行。
type ledger struct {
	inUse   Resources
	running int
}

func newLedger() *ledger { return &ledger{} }

// canAdmit 返回是否可准入以及不可准入时的可区分原因：
//   - ReasonOverConcurrency：并发度已满；
//   - ReasonOverResource：CPU/Memory/IO 中某一维触顶（附带是哪一维）。
//
// 注意：任务自身超过总容量（ReasonTaskTooLarge）在准入前由 Submit 判定，
// 那种任务永远无法被容纳，不进入排队。
func (g *ledger) canAdmit(cost Resources, l Limits) (bool, RejectReason) {
	if g.running >= l.MaxTasks {
		return false, ReasonOverConcurrency
	}
	if g.inUse.CPU+cost.CPU > l.MaxCPU ||
		g.inUse.Memory+cost.Memory > l.MaxMemory ||
		g.inUse.IO+cost.IO > l.MaxIO {
		return false, ReasonOverResource
	}
	return true, ReasonNone
}

// admit 把任务开销计入账目。调用方必须先用 canAdmit 确认可行，
// 且每个任务只允许 admit 一次。
func (g *ledger) admit(cost Resources) {
	g.inUse = g.inUse.Add(cost)
	g.running++
}

// release 从账目中扣除任务开销。每个任务必须且只能 release 一次，
// 由 task.released 标志保证。
func (g *ledger) release(cost Resources) {
	g.inUse = g.inUse.Sub(cost)
	g.running--
}

// overbooked 是开发/测试期不变量自检：账目不应出现负数或并发数为负。
func (g *ledger) overbooked(l Limits) string {
	switch {
	case g.running < 0:
		return fmt.Sprintf("running=%d < 0", g.running)
	case g.inUse.CPU < 0:
		return fmt.Sprintf("cpu in use=%d < 0", g.inUse.CPU)
	case g.inUse.Memory < 0:
		return fmt.Sprintf("memory in use=%d < 0", g.inUse.Memory)
	case g.inUse.IO < 0:
		return fmt.Sprintf("io in use=%d < 0", g.inUse.IO)
	case g.inUse.CPU > l.MaxCPU:
		return fmt.Sprintf("cpu in use=%d > cap=%d", g.inUse.CPU, l.MaxCPU)
	case g.inUse.Memory > l.MaxMemory:
		return fmt.Sprintf("memory in use=%d > cap=%d", g.inUse.Memory, l.MaxMemory)
	case g.inUse.IO > l.MaxIO:
		return fmt.Sprintf("io in use=%d > cap=%d", g.inUse.IO, l.MaxIO)
	case g.running > l.MaxTasks:
		return fmt.Sprintf("running=%d > max=%d", g.running, l.MaxTasks)
	}
	return ""
}
