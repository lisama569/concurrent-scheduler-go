package scheduler

import "sort"

// groupQueue 是单个业务组的等待队列：组内沿用"有效优先级 + 老化 + seq"
// 的全确定排序规则。组间公平（轮转）由 registry 层负责，本结构不关心。
// 非线程安全：所有方法必须在 Scheduler.mu 下调用。
type groupQueue struct {
	name    string
	entries []*waitEntry
}

func newGroupQueue(name string) *groupQueue {
	return &groupQueue{name: name}
}

func (q *groupQueue) len() int { return len(q.entries) }

func (q *groupQueue) push(h *Handle, group string, cost int64, p Priority, seq uint64) {
	q.entries = append(q.entries, &waitEntry{handle: h, group: group, cost: cost, base: p, seq: seq})
}

func (q *groupQueue) remove(h *Handle) bool {
	for i, e := range q.entries {
		if e.handle == h {
			q.entries = append(q.entries[:i], q.entries[i+1:]...)
			return true
		}
	}
	return false
}

// drain 返回并清空组内全部等待条目（关闭该组时使用）。
func (q *groupQueue) drain() []*waitEntry {
	out := q.entries
	q.entries = nil
	return out
}

// ordered 返回按"有效优先级降序、同值按 seq 升序"排序的拷贝，
// 排序完全确定 => 同组内准入顺序可复现。
func (q *groupQueue) ordered() []*waitEntry {
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

// popEligible 依据调用方给出的组内配额谓词选出至多一个可准入条目并出队。
// 与单池语义一致：按有效优先级顺序扫描，跳过当前装不下的大任务，
// 取第一个能装下的，使队头大任务不阻塞能装下的其他任务。
func (q *groupQueue) popEligible(canAdmit func(cost int64) bool) *waitEntry {
	for _, e := range q.ordered() {
		if canAdmit(e.cost) {
			q.remove(e.handle)
			return e
		}
	}
	return nil
}

// popProbe 仅报告组内是否存在可准入条目，不修改队列。
func (q *groupQueue) popProbe(canAdmit func(cost int64) bool) bool {
	for _, e := range q.ordered() {
		if canAdmit(e.cost) {
			return true
		}
	}
	return false
}

// ageAll 在"本组本轮没有任何任务可准入"时对组内等待者增加一次老化，
// 返回当前最大扫描次数。
func (q *groupQueue) ageAll() int64 {
	var maxAge int64
	for _, e := range q.entries {
		e.scans++
		if e.scans > maxAge {
			maxAge = e.scans
		}
	}
	return maxAge
}
