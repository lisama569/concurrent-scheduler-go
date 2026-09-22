package scheduler

import (
	"container/heap"
	"fmt"
	"time"
)

// boostPriority 是年龄到期任务被临时提升到的有效优先级，
// 高于任何普通优先级，保证其在同一轮扫描中排在最前；
// 同被提升的任务之间按 id（提交顺序）FIFO。
const boostPriority = 10_000

// agingQueue 是带"年龄"提升的优先级队列。
//
// 排序键：(effectivePriority desc, id asc)。
// effectivePriority = priority，或在累计等待年龄达到 starvationAge 时
// 临时提升为 boostPriority，随后年龄清零、重新计数（可再次提升），
// 从而防止低优先级任务无限期饥饿。
// 准入选择时按堆顺序线性扫描第一个"当前资源放得下"的任务，
// 因此队头任务暂时放不下时不会阻塞其后可容纳的任务（无队头阻塞）。
type agingQueue struct {
	items         []*task
	starvationAge int64
}

func newAgingQueue(starvationAge int64) *agingQueue {
	if starvationAge <= 0 {
		starvationAge = DefaultStarvationAge
	}
	return &agingQueue{starvationAge: starvationAge}
}

func (q *agingQueue) Len() int { return len(q.items) }

// head 返回有效优先级最高的队头任务（不移除）；队列为空返回 nil。
func (q *agingQueue) head() *task {
	if len(q.items) == 0 {
		return nil
	}
	return q.items[0]
}

func (q *agingQueue) Push(t *task) {
	t.boosted = false
	heap.Push((*taskHeap)(q), t)
}

func (q *agingQueue) Remove(t *task) bool {
	if t.heapIndex < 0 || t.heapIndex >= len(q.items) || q.items[t.heapIndex] != t {
		return false
	}
	heap.Remove((*taskHeap)(q), t.heapIndex)
	t.heapIndex = -1
	return true
}

// Contains 判断任务是否仍在队列中。
func (q *agingQueue) Contains(t *task) bool {
	return t.heapIndex >= 0 && t.heapIndex < len(q.items) && q.items[t.heapIndex] == t
}

// PickEligible 按有效优先级顺序扫描，返回第一个在当前账目下可准入的任务
// （任务自身不超过总容量，且并入 inUse 后不触顶）。找不到返回 nil。
// 该任务仍保留在队列中，由调用方决定 Remove。
func (q *agingQueue) PickEligible(inUse Resources, l Limits) *task {
	for _, t := range q.items {
		if t.cost.Fits(l) && t.cost.FitsWithin(inUse, l) {
			return t
		}
	}
	return nil
}

// Expired 返回所有排队截止时间不晚于 now 的任务，但不移除，由调用方处理。
func (q *agingQueue) Expired(now time.Time) []*task {
	var out []*task
	for _, t := range q.items {
		if t.hasQueueDl && !t.queueDl.After(now) {
			out = append(out, t)
		}
	}
	return out
}

// AgeAll 按 now 相对入队时刻计算每个等待任务已等待的 tick 数，
// 累计达到 starvationAge（等待时长 >= starvationAge*tick）的任务
// 被临时提升到 boostPriority。返回本次新提升的任务数，必要时重建堆序。
// 基于时间而非扫描次数，真实时钟与虚拟时钟下语义一致、可复现。
func (q *agingQueue) AgeAll(now time.Time, tick time.Duration) int {
	if tick <= 0 {
		return 0
	}
	boosted := 0
	for _, t := range q.items {
		if t.boosted {
			continue
		}
		waited := int64(now.Sub(t.queuedAt) / tick)
		if waited >= q.starvationAge {
			t.ageTicks = waited
			t.boosted = true
			boosted++
		}
	}
	if boosted > 0 {
		heap.Init((*taskHeap)(q))
	}
	return boosted
}

// Snapshot 返回队列中任务的描述（按有效优先级排序，用于日志）。
func (q *agingQueue) Snapshot() []string {
	out := make([]string, 0, len(q.items))
	for _, t := range q.items {
		out = append(out, fmt.Sprintf("%s(p=%d,eff=%d,age=%d)", t.name, t.priority, effPri(t), t.ageTicks))
	}
	return out
}

// taskHeap 实现 heap.Interface。
type taskHeap agingQueue

func (h *taskHeap) Len() int { return len(h.items) }

func (h *taskHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	pa, pb := effPri(a), effPri(b)
	if pa != pb {
		return pa > pb // 有效优先级高者在前
	}
	return a.id < b.id // 同级 FIFO
}

func (h *taskHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].heapIndex = i
	h.items[j].heapIndex = j
}

func (h *taskHeap) Push(x any) {
	t := x.(*task)
	t.heapIndex = len(h.items)
	h.items = append(h.items, t)
}

func (h *taskHeap) Pop() any {
	n := len(h.items)
	t := h.items[n-1]
	h.items = h.items[:n-1]
	t.heapIndex = -1
	return t
}

func effPri(t *task) int {
	if t.boosted {
		return boostPriority
	}
	return t.priority
}
