package scheduler

import "sort"

// waitEntry 是等待队列内部条目。
type waitEntry struct {
	handle *Handle
	group  string
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

// waitQueue 是带优先级 + 老化防饥饿、且组间轮转公平的等待队列。
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
func (q *waitQueue) push(h *Handle, group string, cost int64, p Priority, seq uint64) {
	q.entries = append(q.entries, &waitEntry{
		handle: h, group: group, cost: cost, base: p, seq: seq,
	})
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

// removeAllByGroup 摘走指定组的全部排队条目（用于按组关闭），
// 其它组的条目原样保留。
func (q *waitQueue) removeAllByGroup(group string) []*waitEntry {
	var out []*waitEntry
	kept := q.entries[:0]
	for _, e := range q.entries {
		if e.group == group {
			out = append(out, e)
		} else {
			kept = append(kept, e)
		}
	}
	q.entries = kept
	return out
}

// countByGroup 统计某个组的排队条目数。
func (q *waitQueue) countByGroup(group string) int {
	n := 0
	for _, e := range q.entries {
		if e.group == group {
			n++
		}
	}
	return n
}

// ordered 返回按"有效优先级降序、同值按入队序号升序"排序的拷贝，
// 不改变原队列。该全序在全队列范围内确定；组内挑选时按此顺序扫描，
// 即沿用单池时代的组内高优先级优先 + 队头大任务不阻塞小任务的语义。
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

// popEligibleRR 是组间公平的核心：以组为单位轮转（round-robin）发槽位。
//
//   - order 为组的注册顺序（全确定性）；after 为上一个拿到槽位的组，
//     本轮从它的下一个组开始扫描，从而任何一个组都无法连续越过其它组拿槽位；
//   - 轮到某个组时，在该组内按 ordered 的全序挑第一个"现在装得下"的任务
//     （组内：高有效优先级先；本组队头大任务装不下时，组内更小的任务可先走）；
//   - 该组当前没有任何可满足任务时立即轮到下一个组，游标不会空转丢失；
//   - eligible 同时检查组账本与全局账本两层硬约束。
//
// 返回被摘走的条目（无则 nil）与本轮中选的组名（作为下一轮的 after）。
func (q *waitQueue) popEligibleRR(order []string, after string,
	eligible func(group string, cost int64) bool) (*waitEntry, string) {

	n := len(order)
	if n == 0 {
		return nil, after
	}
	start := 0
	if after != "" {
		for i, g := range order {
			if g == after {
				start = i + 1
				break
			}
		}
	}
	ord := q.ordered()
	for k := 0; k < n; k++ {
		g := order[(start+k)%n]
		for _, e := range ord {
			if e.group != g {
				continue
			}
			if eligible(g, e.cost) {
				q.remove(e.handle)
				return e, g
			}
		}
	}
	return nil, after
}

// probeEligibleRR 仅报告当前是否存在可准入任务，不修改队列
// （用于条件变量谓词）。判定规则与 popEligibleRR 完全一致。
func (q *waitQueue) probeEligibleRR(order []string, after string,
	eligible func(group string, cost int64) bool) bool {

	e, _ := q.popEligibleRR(order, after, eligible)
	if e == nil {
		return false
	}
	// popEligibleRR 已摘走条目；probe 不得修改队列，原样放回。
	// 放回位置不影响排序全序（ordered 每次重排），seq 不变。
	q.entries = append(q.entries, e)
	return true
}

// ageAll 在"本轮扫描没有任何任务可准入"时对所有等待者增加一次老化，
// 并返回当前最大等待扫描次数（供调度器在需要时做饥饿观测）。
// 老化在所有组上统一发生：组内低优先级任务等久了会被抬上来；
// 而它能否真正出队仍受组间轮转约束，两层公平互不干扰。
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
