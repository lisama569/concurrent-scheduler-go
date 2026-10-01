package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// testLogger 把每次准入/排队/拒绝/释放决策与资源快照打印到测试日志，
// 满足"单测日志中需要打印每次准入与释放的决策依据及资源快照"。
type testLogger struct {
	t     *testing.T
	mu    sync.Mutex
	ended bool
}

// newTestLogger 构造一个在测试结束后自动静默的日志器。部分用例刻意保留
// 无视 ctx 的永久阻塞任务，其 goroutine 在 *testing.T 失效后仍可能存活。
// 关键：t.Logf 与 cleanup 的"标记结束"持有同一把 mu，彻底消除
// "检查 ended 与真正调用 t.Logf 之间"的窗口，杜绝在已销毁 testing 上竞争。
func newTestLogger(t *testing.T) *testLogger {
	l := &testLogger{t: t}
	t.Cleanup(func() {
		l.mu.Lock()
		l.ended = true
		l.mu.Unlock()
	})
	return l
}

func (l *testLogger) Logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ended {
		return
	}
	l.t.Logf(format, args...)
}

func newTestScheduler(t *testing.T, cfg Config) *Scheduler {
	t.Helper()
	return New(cfg).WithLogger(newTestLogger(t))
}

func waitHandle(t *testing.T, h *Handle, timeout time.Duration) {
	t.Helper()
	select {
	case <-h.Done():
	case <-time.After(timeout):
		t.Fatalf("task %s did not finish within %s", h.ID(), timeout)
	}
}

func noop() func(ctx context.Context) error {
	return func(ctx context.Context) error { return nil }
}

func sleepTask(d time.Duration) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		select {
		case <-time.After(d):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func task(id string, cost int64, p Priority, fn func(ctx context.Context) error) *Task {
	return &Task{ID: id, Cost: cost, Priority: p, Fn: fn}
}

// TestBasicAdmitAndRelease：基本准入与精确释放。
func TestBasicAdmitAndRelease(t *testing.T) {
	s := newTestScheduler(t, Config{MaxConcurrency: 2, ResourceLimit: 10})
	ctx := context.Background()

	h1, err := s.Submit(ctx, task("a", 4, PriorityNormal, noop()))
	if err != nil {
		t.Fatal(err)
	}
	h2, err := s.Submit(ctx, task("b", 6, PriorityNormal, noop()))
	if err != nil {
		t.Fatal(err)
	}
	waitHandle(t, h1, time.Second)
	waitHandle(t, h2, time.Second)

	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	snap := s.Snapshot()
	if snap.InFlight != 0 || snap.ResourceUsed != 0 {
		t.Fatalf("quota not fully released: %s", snap)
	}
}

// TestRejectExceedsHardLimit：cost 超过资源硬上限必须被显式拒绝且不记账。
func TestRejectExceedsHardLimit(t *testing.T) {
	s := newTestScheduler(t, Config{MaxConcurrency: 2, ResourceLimit: 10})
	h, err := s.Submit(context.Background(), task("too-big", 11, PriorityNormal, noop()))
	var re *RejectError
	if !errors.As(err, &re) || re.Reason != ReasonExceedsHardLimit {
		t.Fatalf("want ReasonExceedsHardLimit, got %v", err)
	}
	if h.Status() != StatusRejected {
		t.Fatalf("want REJECTED status, got %s", h.Status())
	}
	if snap := s.Snapshot(); snap.InFlight != 0 || snap.ResourceUsed != 0 {
		t.Fatalf("rejected task must not consume quota: %s", snap)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestRejectQueueFull：队列满必须按 QUEUE_FULL 拒绝，绝不静默丢弃。
func TestRejectQueueFull(t *testing.T) {
	s := newTestScheduler(t, Config{MaxConcurrency: 1, ResourceLimit: 10, QueueCapacity: 1})
	ctx := context.Background()
	block := make(chan struct{})
	running := make(chan struct{})

	_, err := s.Submit(ctx, task("runner", 5, PriorityNormal, func(ctx context.Context) error {
		close(running)
		<-block
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	<-running

	// 第二个进队列（容量 1）。
	if _, err := s.Submit(ctx, task("queued", 1, PriorityNormal, noop())); err != nil {
		t.Fatal(err)
	}
	// 第三个：队列已满，必须被拒绝。
	h, err := s.Submit(ctx, task("third", 1, PriorityNormal, noop()))
	var re *RejectError
	if !errors.As(err, &re) || re.Reason != ReasonQueueFull {
		t.Fatalf("want ReasonQueueFull, got %v", err)
	}
	if h.Status() != StatusRejected {
		t.Fatalf("want REJECTED, got %s", h.Status())
	}

	close(block)
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestRejectAfterShutdown：关闭后新任务必须按 SCHEDULER_STOPPED 拒绝。
func TestRejectAfterShutdown(t *testing.T) {
	s := newTestScheduler(t, Config{MaxConcurrency: 2, ResourceLimit: 10})
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := s.Submit(context.Background(), task("late", 1, PriorityNormal, noop()))
	var re *RejectError
	if !errors.As(err, &re) || re.Reason != ReasonSchedulerStopped {
		t.Fatalf("want ReasonSchedulerStopped, got %v", err)
	}
}

// TestQuotaConstraintBothDimensions：并发度与总资源两个维度必须同时约束。
func TestQuotaConstraintBothDimensions(t *testing.T) {
	s := newTestScheduler(t, Config{MaxConcurrency: 3, ResourceLimit: 10})
	ctx := context.Background()
	block := make(chan struct{})
	running := make(chan struct{})
	var runningMu sync.Mutex
	runningCount := 0
	maxConcurrent := 0

	barrier := func(ctx context.Context) error {
		runningMu.Lock()
		runningCount++
		if runningCount > maxConcurrent {
			maxConcurrent = runningCount
		}
		if runningCount == 2 {
			select {
			case <-running:
			default:
				close(running)
			}
		}
		runningMu.Unlock()
		<-block
		runningMu.Lock()
		runningCount--
		runningMu.Unlock()
		return nil
	}

	// 5 个任务，每个 cost=4：并发槽有 3 个，但资源 10 只允许 2 个同时运行。
	var handles []*Handle
	for i := 0; i < 5; i++ {
		h, err := s.Submit(ctx, task(fmt.Sprintf("q%d", i), 4, PriorityNormal, barrier))
		if err != nil {
			t.Fatal(err)
		}
		handles = append(handles, h)
	}
	<-running
	// 给调度器一点时间后断言：在运行数必须为 2（资源约束），且占用 8。
	time.Sleep(20 * time.Millisecond)
	snap := s.Snapshot()
	if snap.InFlight != 2 || snap.ResourceUsed != 8 {
		t.Fatalf("resource dimension should cap at 2 tasks / 8 used, got %s", snap)
	}
	if snap.Queued != 3 {
		t.Fatalf("want 3 queued, got %d", snap.Queued)
	}
	close(block)
	for _, h := range handles {
		waitHandle(t, h, 2*time.Second)
	}
	if maxConcurrent > 2 {
		t.Fatalf("resource quota violated: observed %d concurrent", maxConcurrent)
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	snap = s.Snapshot()
	if snap.InFlight != 0 || snap.ResourceUsed != 0 {
		t.Fatalf("quota leaked: %s", snap)
	}
}
