package scheduler

import "sort"

// waitEntry 是等待队列内部条目。
type waitEntry struct {
	handle *Handle
	cost   int64
	base   Priority
	// seq 为单调递增的入队序号，用于同优先级下 FIFO，保证可复现。
	seq uint64
	// scans 是该条目入队后经历过的"无法立即满足任何任务"的空扫描次数，
	// 作为老化（aging）依据。
	scans int64
}

// effectivePriority 随等待时间（以空扫描次数计）线性上升，
// 因此低优先级任务不会无限期饥饿，同时又不会立即抢占高优先级任务。
func (e *waitEntry) effectivePriority() int64 {
	return int64(e.base) + e.scans
}

// waitQueue 是带优先级 + 老化防饥饿的等待队列。
// 非线程安全：所有方法必须在 Scheduler.mu 下调用。
type waitQueue struct {
	entries []*waitEntry
	cap     int
}

func newWaitQueue(capacity int) *waitQueue {
	return &waitQueue{cap: capacity}
}

func (q *waitQueue) len() int { return len(q.entries) }

func (q *waitQueue) full() bool { return q.cap > 0 && len(q.entries) >= q.cap }

// push 入队。seq 由调度器以全局单调序号提供。
func (q *waitQueue) push(h *Handle, cost int64, p Priority, seq uint64) {
	q.entries = append(q.entries, &waitEntry{handle: h, cost: cost, base: p, seq: seq})
}

// remove 取出指定条目（用于排队中取消）。存在则返回 true。
func (q *waitQueue) remove(h *Handle) bool {
	for i, e := range q.entries {
		if e.handle == h {
			q.entries = append(q.entries[:i], q.entries[i+1:]...)
			return true
		}
	}
	return false
}

// ordered 返回按"有效优先级降序、同值按入队序号升序"排序的拷贝，
// 不改变原队列。排序完全确定 => 相同输入下调度顺序可复现。
func (q *waitQueue) ordered() []*waitEntry {
	out := make([]*waitEntry, len(q.entries))
	copy(out, q.entries)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.effectivePriority() != b.effectivePriority() {
			return a.effectivePriority() > b.effectivePriority()
		}
		return a.seq < b.seq
	})
	return out
}

// popEligible 从当前账目中选出最多一个可准入的任务：
// 按有效优先级顺序扫描，跳过因资源不足而装不下的任务，取第一个能装下的。
//
// 关键公平性语义：若队头高优先级任务因资源不足装不下，系统不会被它
// 完全卡死——能装下的较低优先级任务可以先走；但排在同一大任务之后、
// 同有效优先级的任务不会越过它（由 ordered 的稳定顺序 + 跳过逻辑共同保证）。
// 老化机制保证这个"装不下"的任务每次空转都会提高有效优先级，
// 最终资源一旦释放即被优先满足。
func (q *waitQueue) popEligible(ac *quota) *waitEntry {
	for _, e := range q.ordered() {
		if ac.canAdmit(e.cost) {
			q.remove(e.handle)
			return e
		}
	}
	return nil
}

// popProbe 仅报告当前是否存在可准入任务，不修改队列（用于条件变量谓词）。
func (q *waitQueue) popProbe(ac *quota) bool {
	for _, e := range q.ordered() {
		if ac.canAdmit(e.cost) {
			return true
		}
	}
	return false
}

// ageAll 在"本轮扫描没有任何任务可准入"时对所有等待者增加一次老化，
// 并返回当前最大等待扫描次数（供调度器在需要时做饥饿观测）。
func (q *waitQueue) ageAll() int64 {
	var maxAge int64
	for _, e := range q.entries {
		e.scans++
		if e.scans > maxAge {
			maxAge = e.scans
		}
	}
	return maxAge
}
