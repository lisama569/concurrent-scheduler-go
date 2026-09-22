package scheduler

import (
	"container/heap"
	"sync"
	"time"
)

// timerHandle 是一次性计时器的句柄。
type timerHandle interface {
	// stop 阻止计时器触发；若计时器已触发或已停止返回 false。
	stop() bool
}

// clock 是时间来源与定时器抽象。生产环境使用 realClock；
// 可复现实验使用 fakeClock，由驱动方在单协程内手动推进时间，
// 从而保证同输入同种子下事件顺序逐次确定。
type clock interface {
	now() time.Time
	afterFunc(d time.Duration, f func()) timerHandle
}

// ---- realClock ----

type realClock struct{}

type realTimer struct{ t *time.Timer }

func (realClock) now() time.Time { return time.Now() }

func (realClock) afterFunc(d time.Duration, f func()) timerHandle {
	return &realTimer{t: time.AfterFunc(d, f)}
}

func (t *realTimer) stop() bool { return t.t.Stop() }

// ---- fakeClock ----

type fakeEntry struct {
	deadline time.Time
	seq      int64
	f        func()
	index    int
}

type fakeHeap []*fakeEntry

// fakeClock 是手动推进的确定性时钟。runUntil 按 (截止时间, 序号)
// 顺序同步触发回调；回调内会驱动调度器完成一整轮扫描，
// 因此多个回调即便截止时间相同，顺序也由提交顺序唯一确定。
type fakeClock struct {
	mu  sync.Mutex // 仅保护并发提交场景；虚拟引擎本身单协程使用
	cur time.Time
	seq int64
	tr  fakeHeap
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{cur: start, tr: fakeHeap{}}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur
}

func (c *fakeClock) afterFunc(d time.Duration, f func()) timerHandle {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	e := &fakeEntry{deadline: c.cur.Add(d), seq: c.seq, f: f}
	heap.Push(&c.tr, e)
	return &fakeTimerHandle{c: c, e: e}
}

type fakeTimerHandle struct {
	c *fakeClock
	e *fakeEntry
}

func (h *fakeTimerHandle) stop() bool { return h.c.remove(h.e) }

// runUntil 推进到 target（含），按序触发所有截止时间不晚于 target 的计时器。
// 返回是否曾推进时间。
func (c *fakeClock) runUntil(target time.Time) bool {
	moved := false
	for {
		c.mu.Lock()
		if c.tr.Len() == 0 || c.tr[0].deadline.After(target) {
			c.cur = target
			c.mu.Unlock()
			return moved
		}
		e := heap.Pop(&c.tr).(*fakeEntry)
		c.cur = e.deadline
		f := e.f
		c.mu.Unlock()
		moved = true
		f()
	}
}

// hasPending 报告是否还有未触发的计时器。
func (c *fakeClock) hasPending() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tr.Len() > 0
}

func (c *fakeClock) nextDeadline() (time.Time, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tr.Len() == 0 {
		return time.Time{}, false
	}
	return c.tr[0].deadline, true
}

func (c *fakeClock) remove(e *fakeEntry) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e.index >= 0 && e.index < c.tr.Len() && c.tr[e.index] == e {
		heap.Remove(&c.tr, e.index)
		return true
	}
	return false
}

func (h fakeHeap) Len() int { return len(h) }

func (h fakeHeap) Less(i, j int) bool {
	if !h[i].deadline.Equal(h[j].deadline) {
		return h[i].deadline.Before(h[j].deadline)
	}
	return h[i].seq < h[j].seq
}

func (h fakeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *fakeHeap) Push(x any) {
	e := x.(*fakeEntry)
	e.index = len(*h)
	*h = append(*h, e)
}

func (h *fakeHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	e.index = -1
	*h = old[:n-1]
	return e
}
