package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestCancelQueuedTask：排队中取消的任务绝不执行用户代码，也不占资源。
func TestCancelQueuedTask(t *testing.T) {
	s := newTestScheduler(t, Config{MaxConcurrency: 1, ResourceLimit: 10})
	ctx := context.Background()
	block := make(chan struct{})
	started := make(chan struct{})

	_, _ = s.Submit(ctx, task("holder", 1, PriorityNormal, func(ctx context.Context) error {
		close(started)
		<-block
		return nil
	}))
	<-started

	ran := false
	h, err := s.Submit(ctx, task("waiting", 1, PriorityNormal, func(ctx context.Context) error {
		ran = true
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if snap := s.Snapshot(); snap.Queued != 1 {
		t.Fatalf("want 1 queued, got %s", snap)
	}
	h.Cancel()
	waitHandle(t, h, time.Second)
	if h.Status() != StatusCanceled || !errors.Is(h.Err(), ErrTaskCanceled) {
		t.Fatalf("want CANCELED, got %s %v", h.Status(), h.Err())
	}
	if ran {
		t.Fatal("queued-canceled task must never execute its Fn")
	}
	close(block)
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if snap := s.Snapshot(); snap.InFlight != 0 || snap.ResourceUsed != 0 || snap.Queued != 0 {
		t.Fatalf("quota leak after queued cancel: %s", snap)
	}
}

// TestCancelRunningTask：运行中取消通过 ctx 通知；任务退出后资源被释放。
func TestCancelRunningTask(t *testing.T) {
	s := newTestScheduler(t, Config{MaxConcurrency: 1, ResourceLimit: 10})
	ctx := context.Background()
	h, err := s.Submit(ctx, task("running", 3, PriorityNormal, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}))
	if err != nil {
		t.Fatal(err)
	}
	// 等待任务进入运行态再取消。
	deadline := time.Now().Add(time.Second)
	for s.Snapshot().InFlight == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	h.Cancel()
	waitHandle(t, h, time.Second)
	if h.Status() != StatusCanceled {
		t.Fatalf("want CANCELED, got %s", h.Status())
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestTaskTimeout：超时任务结算为 TIMED_OUT，资源照常精确释放。
func TestTaskTimeout(t *testing.T) {
	s := newTestScheduler(t, Config{MaxConcurrency: 2, ResourceLimit: 10})
	tk := &Task{
		ID:       "slow",
		Cost:     4,
		Priority: PriorityNormal,
		Timeout:  20 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			select {
			case <-time.After(5 * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
	h, err := s.Submit(context.Background(), tk)
	if err != nil {
		t.Fatal(err)
	}
	waitHandle(t, h, time.Second)
	if h.Status() != StatusTimedOut {
		t.Fatalf("want TIMED_OUT, got %s", h.Status())
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if snap := s.Snapshot(); snap.InFlight != 0 || snap.ResourceUsed != 0 {
		t.Fatalf("quota not released after timeout: %s", snap)
	}
}

// TestPauseResume：暂停期间新任务排队且不占资源；恢复后继续派发。
func TestPauseResume(t *testing.T) {
	s := newTestScheduler(t, Config{MaxConcurrency: 2, ResourceLimit: 10})
	ctx := context.Background()
	s.Pause()

	done := make(chan struct{})
	h, err := s.Submit(ctx, task("paused-task", 2, PriorityNormal, func(ctx context.Context) error {
		close(done)
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("task must not run while paused")
	default:
	}
	if snap := s.Snapshot(); snap.InFlight != 0 || snap.ResourceUsed != 0 || snap.Queued != 1 {
		t.Fatalf("paused submission should only queue: %s", snap)
	}
	s.Resume()
	waitHandle(t, h, time.Second)
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestPanicIsolation：任务 panic 被捕获隔离，记为 PANICKED，
// 资源被释放，其他任务不受影响。
func TestPanicIsolation(t *testing.T) {
	s := newTestScheduler(t, Config{MaxConcurrency: 2, ResourceLimit: 10})
	ctx := context.Background()

	h1, err := s.Submit(ctx, task("panicker", 4, PriorityNormal, func(ctx context.Context) error {
		panic("boom")
	}))
	if err != nil {
		t.Fatal(err)
	}
	waitHandle(t, h1, time.Second)
	if h1.Status() != StatusPanicked {
		t.Fatalf("want PANICKED, got %s err=%v", h1.Status(), h1.Err())
	}
	snap := s.Snapshot()
	if snap.InFlight != 0 || snap.ResourceUsed != 0 {
		t.Fatalf("panic task must release quota exactly: %s", snap)
	}

	// panic 后调度器仍可正常准入、执行、结算新任务。
	h2, err := s.Submit(ctx, task("survivor", 6, PriorityNormal, noop()))
	if err != nil {
		t.Fatal(err)
	}
	waitHandle(t, h2, time.Second)
	if h2.Status() != StatusSucceeded {
		t.Fatalf("want SUCCEEDED after panic, got %s", h2.Status())
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestBlockedTaskDoesNotCorruptAccounts：任务永久阻塞且忽略 ctx 时，
// 账目保持一致；Shutdown 在超时时返回 ctx.Err，且该任务的资源仍被占用，
// 一旦（测试中）它被强制结束，账目逻辑仍正确。这里重点验证：
// 阻塞期间其他任务照常结算，used/inFlight 始终自洽。
func TestBlockedTaskDoesNotCorruptAccounts(t *testing.T) {
	s := newTestScheduler(t, Config{MaxConcurrency: 3, ResourceLimit: 10})
	ctx := context.Background()

	// 一个占 3 资源、永久阻塞的任务（goroutine 泄漏在此测试中被刻意保留，
	// 用于验证"长阻塞任务不毁账目"；它是纯 select{}，不持有任何锁）。
	_, err := s.Submit(ctx, task("blocked-forever", 3, PriorityNormal, func(ctx context.Context) error {
		select {}
	}))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for s.Snapshot().InFlight == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	// 阻塞期间再跑一批正常任务：剩余容量 concurrency=2 / resource=7。
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		h, err := s.Submit(ctx, task("normal", 2, PriorityNormal, noop()))
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func() { defer wg.Done(); <-h.Done() }()
	}
	wg.Wait()

	snap := s.Snapshot()
	if snap.InFlight != 1 || snap.ResourceUsed != 3 {
		t.Fatalf("blocked task alone must remain accounted: %s", snap)
	}

	// Shutdown 带短超时：阻塞任务不会让等待永久挂起。
	shutdownCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := s.Shutdown(shutdownCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded from shutdown, got %v", err)
	}
}

// TestDoubleReleaseGuardsAccount：白盒验证重复释放会被账本立即拦截，
// 而不是静默造成账目为负。
func TestDoubleReleaseGuardsAccount(t *testing.T) {
	q := newQuota(Config{MaxConcurrency: 2, ResourceLimit: 10})
	q.admit(4)
	q.release(4)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("second release must panic to expose corrupted accounting")
		}
	}()
	q.release(4) // 不应发生的重复释放
}

// TestFinishIdempotent：Handle.finish 幂等，重复结算只有第一次生效。
func TestFinishIdempotent(t *testing.T) {
	h := newHandle(task("x", 1, PriorityNormal, noop()), context.Background())
	if !h.finish(StatusSucceeded, nil) {
		t.Fatal("first finish should win")
	}
	if h.finish(StatusFailed, errors.New("late")) {
		t.Fatal("second finish must be a no-op")
	}
	if h.Status() != StatusSucceeded {
		t.Fatalf("terminal status mutated by duplicate finish: %s", h.Status())
	}
}
