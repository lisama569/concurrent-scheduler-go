package scheduler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestPanicIsolation: 任务 panic 被恢复隔离，
// 资源精确释放一次，调度器仍可继续服务后续任务，账目保持平衡。
func TestPanicIsolation(t *testing.T) {
	s, log := newTestScheduler(t,
		Limits{MaxCPU: 10, MaxMemory: 10, MaxIO: 10, MaxTasks: 2}, time.Millisecond)

	var afterOK atomic.Bool
	h, err := s.Submit(TaskSpec{
		Name: "panicker", Priority: 5, Cost: Resources{CPU: 4, Memory: 3},
		Fn: func(ctx context.Context) error {
			panic("boom")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	res, werr := h.Wait(context.Background())
	if werr != nil {
		t.Fatal(werr)
	}
	if res.Status != StatusPanicked {
		t.Fatalf("want StatusPanicked, got %s", res.Status)
	}
	var pe *panicError
	if !errors.As(res.Err, &pe) {
		t.Fatalf("want *panicError, got %T %v", res.Err, res.Err)
	}

	// panic 之后必须还能正常提交并运行任务。
	done := make(chan struct{})
	h2, err := s.Submit(TaskSpec{
		Name: "after", Cost: Resources{CPU: 2},
		Fn: func(ctx context.Context) error {
			afterOK.Store(true)
			close(done)
			return nil
		},
	})
	if err != nil {
		t.Fatalf("submit after panic: %v", err)
	}
	if r, _ := h2.Wait(context.Background()); r.Status != StatusSucceeded {
		t.Fatalf("after-panic task status=%s", r.Status)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("after-panic task did not run")
	}
	if !afterOK.Load() {
		t.Fatal("after-panic task body not observed")
	}

	waitZero(t, s)
	assertReleased(t, s)
	snap := s.Snapshot()
	if snap.Panicked != 1 || snap.Completed != 1 || snap.Runaway != 0 {
		t.Fatalf("unexpected counters: %+v", snap)
	}

	// 日志必须包含 panic 隔离决策与紧随其后的释放快照。
	var sawPanic, sawRelease bool
	for _, e := range log.Events() {
		if e.Type == EventPanic {
			sawPanic = true
		}
		if e.Type == EventRelease && e.TaskName == "panicker" {
			sawRelease = true
			if e.Snap.InUse.CPU != 0 || e.Snap.InUse.Memory != 0 || e.Snap.Running != 0 {
				t.Fatalf("resources not released after panic: %+v", e.Snap)
			}
		}
	}
	if !sawPanic || !sawRelease {
		t.Fatal("panic/release events missing")
	}
	printEvents(t, log)
}

// TestDuplicateRelease: 重复 Cancel / 重复等待不会导致二次释放或 panic；
// 账目最终精确归零。
func TestDuplicateRelease(t *testing.T) {
	s, log := newTestScheduler(t,
		Limits{MaxCPU: 10, MaxTasks: 2}, time.Millisecond)

	started := make(chan struct{})
	release := make(chan struct{})
	h, err := s.Submit(TaskSpec{
		Name: "dup", Cost: Resources{CPU: 3},
		Fn: func(ctx context.Context) error {
			close(started)
			<-release
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	<-started

	// 并发多次取消：恰好一次生效，其余返回 false。
	var nOK atomic.Int32
	done := make(chan struct{}, 8)
	for i := 0; i < 8; i++ {
		go func() {
			if h.Cancel() {
				nOK.Add(1)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < 8; i++ {
		<-done
	}
	if nOK.Load() != 1 {
		t.Fatalf("want exactly 1 effective cancel, got %d", nOK.Load())
	}
	// 让任务体退出（它可能在 ctx 检查前阻塞，这里放行业务门）。
	close(release)

	res, _ := h.Wait(context.Background())
	if res.Status != StatusCanceled {
		t.Fatalf("want canceled, got %s", res.Status)
	}
	// 再等一次、再取消一次：都必须是空操作，不 panic、不二次释放。
	res2, _ := h.Wait(context.Background())
	if res2.Status != StatusCanceled || h.Cancel() {
		t.Fatal("duplicate wait/cancel not idempotent")
	}

	waitZero(t, s)
	assertReleased(t, s)
	releaseCount := 0
	for _, e := range log.Events() {
		if e.Type == EventRelease && e.TaskName == "dup" {
			releaseCount++
		}
	}
	if releaseCount != 1 {
		t.Fatalf("want exactly 1 release event, got %d", releaseCount)
	}
	printEvents(t, log)
}

// TestCancelQueuedReleasesImmediately: 排队任务被取消时立即结算，
// 不占用任何资源；同资源随后可被其他任务使用。
func TestCancelQueuedReleasesImmediately(t *testing.T) {
	s, log := newTestScheduler(t,
		Limits{MaxCPU: 10, MaxTasks: 1}, time.Millisecond)

	hold := make(chan struct{})
	_, err := s.Submit(TaskSpec{
		Name: "holder", Cost: Resources{CPU: 10},
		Fn: func(ctx context.Context) error { <-hold; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	hq, err := s.Submit(TaskSpec{
		Name: "queued", Cost: Resources{CPU: 10},
		QueueTimeout: time.Hour,
		Fn:           func(ctx context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, func() bool { return s.Snapshot().Queued == 1 }) {
		t.Fatal("queued task did not enter queue")
	}
	if !hq.Cancel() {
		t.Fatal("cancel queued task should be effective")
	}
	res, _ := hq.Wait(context.Background())
	if res.Status != StatusCanceled {
		t.Fatalf("want canceled, got %s", res.Status)
	}
	snap := s.Snapshot()
	if snap.Queued != 0 || snap.InUse.CPU != 10 || snap.Running != 1 {
		t.Fatalf("queued cancel changed ledger unexpectedly: %+v", snap)
	}
	close(hold)
	waitZero(t, s)
	assertReleased(t, s)
	printEvents(t, log)
}

// TestRunTimeoutThenGrace: 超时任务先收到 ctx 取消；及时退出则正常结算，
// 资源释放一次。
func TestRunTimeoutCooperativeTask(t *testing.T) {
	log := NewSliceLogger()
	s := New(Config{
		Limits: Limits{MaxCPU: 10, MaxTasks: 2},
		Tick:   time.Millisecond, RunGrace: 20 * time.Millisecond,
		Logger: log,
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})

	h, err := s.Submit(TaskSpec{
		Name: "coop", Cost: Resources{CPU: 5},
		RunTimeout: 20 * time.Millisecond,
		Fn: func(ctx context.Context) error {
			<-ctx.Done() // 配合取消
			return ctx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	res, _ := h.Wait(context.Background())
	if res.Status != StatusTimedOut {
		t.Fatalf("want timed_out, got %s", res.Status)
	}
	waitZero(t, s)
	assertReleased(t, s)
	if s.Snapshot().TimedOut != 1 {
		t.Fatalf("want timeout counter 1, got %d", s.Snapshot().TimedOut)
	}
	printEvents(t, log)
}

// TestRunawayTaskLedgerReclaimed: 无视取消、长时间阻塞的任务，
// 在超时+宽限后账目被先行释放（runaway），其他任务不受影响。
func TestRunawayTaskLedgerReclaimed(t *testing.T) {
	log := NewSliceLogger()
	s := New(Config{
		Limits: Limits{MaxCPU: 10, MaxTasks: 1},
		Tick:   time.Millisecond, RunGrace: 10 * time.Millisecond,
		Logger: log,
	})

	blockForever := func(ctx context.Context) error {
		select {} // 故意永不退出，模拟失控任务
	}
	h, err := s.Submit(TaskSpec{
		Name: "runaway", Cost: Resources{CPU: 10},
		RunTimeout: 15 * time.Millisecond,
		Fn:         blockForever,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 等待 runaway 回收（超时 15ms + 宽限 10ms + 调度余量）。
	if !waitUntil(t, func() bool { return s.Snapshot().Runaway == 1 }) {
		t.Fatalf("runaway not reclaimed: %+v", s.Snapshot())
	}
	assertReleased(t, s)
	res, _ := h.Result()
	if res.Status != StatusTimedOut {
		t.Fatalf("runaway handle should resolve timed_out, got %s", res.Status)
	}

	// 失控 goroutine 脱离账目后，槽位必须可被新任务复用。
	h2, err := s.Submit(TaskSpec{
		Name: "after-runaway", Cost: Resources{CPU: 10},
		Fn: func(ctx context.Context) error { return nil },
	})
	if err != nil {
		t.Fatalf("slot not reusable after runaway: %v", err)
	}
	if r, _ := h2.Wait(context.Background()); r.Status != StatusSucceeded {
		t.Fatalf("replacement task status=%s", r.Status)
	}

	// 关闭时失控 goroutine 仍在，但 Shutdown 不能被无限阻塞。
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown blocked by runaway: %v", err)
	}
	printEvents(t, log)
}
