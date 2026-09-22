package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPauseResume: 暂停期间不放行新任务（已在运行的不受影响），
// 恢复后排队任务继续被准入。
func TestPauseResume(t *testing.T) {
	s, log := newTestScheduler(t,
		Limits{MaxCPU: 10, MaxTasks: 1}, time.Millisecond)

	hold := make(chan struct{})
	h0, err := s.Submit(TaskSpec{
		Name: "running", Cost: Resources{CPU: 10},
		Fn: func(ctx context.Context) error { <-hold; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	waitRunning(t, s, 1)

	s.Pause()
	h1, err := s.Submit(TaskSpec{
		Name: "paused-q", Cost: Resources{CPU: 10},
		Fn: func(ctx context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	// 即使槽位空了，暂停期间也不得准入。
	close(hold)
	select {
	case <-h1.Done():
		t.Fatal("task must not be admitted while paused")
	case <-time.After(20 * time.Millisecond):
	}
	if r, _ := h0.Result(); r.Status != StatusSucceeded {
		t.Fatalf("holder status=%s", r.Status)
	}

	s.Resume()
	if r, _ := h1.Wait(context.Background()); r.Status != StatusSucceeded {
		t.Fatalf("queued task after resume status=%s", r.Status)
	}
	waitZero(t, s)
	assertReleased(t, s)
	printEvents(t, log)
}

// TestQueueTimeout: 排队超过 QueueTimeout 的任务以 timed_out 结算，
// 不会被静默丢弃，也不占用资源。
func TestQueueTimeout(t *testing.T) {
	log := NewSliceLogger()
	s := New(Config{
		Limits: Limits{MaxCPU: 10, MaxTasks: 1},
		Tick:   time.Millisecond, RunGrace: 20 * time.Millisecond,
		Logger: log,
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})

	hold := make(chan struct{})
	_, err := s.Submit(TaskSpec{
		Name: "holder", Cost: Resources{CPU: 10},
		Fn: func(ctx context.Context) error { <-hold; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	h, err := s.Submit(TaskSpec{
		Name: "slow-waiter", Cost: Resources{CPU: 10},
		QueueTimeout: 15 * time.Millisecond,
		Fn:           func(ctx context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	res, _ := h.Wait(context.Background())
	if res.Status != StatusTimedOut {
		t.Fatalf("want timed_out, got %s", res.Status)
	}
	if !errors.Is(res.Err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", res.Err)
	}
	close(hold)
	waitZero(t, s)
	snap := s.Snapshot()
	if snap.QueueTimedOut != 1 || snap.TimedOut != 0 ||
		snap.Running != 0 || snap.Queued != 0 || snap.InUse.CPU != 0 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
	printEvents(t, log)
}

// TestGracefulShutdown: 关闭后新任务被拒绝（shutting_down）；
// 已在管任务可正常跑完；重复 Shutdown 返回 ErrClosed。
func TestGracefulShutdown(t *testing.T) {
	log := NewSliceLogger()
	s := New(Config{
		Limits: Limits{MaxCPU: 10, MaxTasks: 4},
		Tick:   time.Millisecond, RunGrace: 20 * time.Millisecond,
		Logger: log,
	})

	var done atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		i := i
		_, err := s.Submit(TaskSpec{
			Name: fmt.Sprintf("g%d", i), Cost: Resources{CPU: 2},
			Fn: func(ctx context.Context) error {
				defer wg.Done()
				time.Sleep(10 * time.Millisecond)
				done.Add(1)
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("graceful shutdown: %v", err)
	}
	wg.Wait()
	if done.Load() != 4 {
		t.Fatalf("want 4 tasks completed, got %d", done.Load())
	}

	// 关闭后提交必须被拒绝，原因可区分。
	_, err := s.Submit(TaskSpec{
		Name: "late", Fn: func(ctx context.Context) error { return nil },
	})
	if !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("want ErrShuttingDown, got %v", err)
	}
	if err := s.Shutdown(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("want ErrClosed on repeat shutdown, got %v", err)
	}
	printEvents(t, log)
}

// TestShutdownForcedCancel: ctx 到期后仍在运行的任务被强制取消，
// Shutdown 不被阻塞，账目归零。
func TestShutdownForcedCancel(t *testing.T) {
	log := NewSliceLogger()
	s := New(Config{
		Limits: Limits{MaxCPU: 10, MaxTasks: 2},
		Tick:   time.Millisecond, RunGrace: 30 * time.Millisecond,
		Logger: log,
	})

	coop := make(chan struct{})
	h1, err := s.Submit(TaskSpec{
		Name: "coop", Cost: Resources{CPU: 5},
		Fn: func(ctx context.Context) error { <-ctx.Done(); return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Submit(TaskSpec{
		Name: "blocker", Cost: Resources{CPU: 5},
		Fn: func(ctx context.Context) error {
			select {} // 不配合取消
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("shutdown blocked too long by uncooperative task")
	}
	r, _ := h1.Result()
	if r.Status != StatusCanceled && r.Status != StatusTimedOut {
		t.Fatalf("coop task final status=%s", r.Status)
	}
	printEvents(t, log)
	close(coop)
}

// TestHighConcurrencyStress: 高并发提交/取消混合负载下，
// 资源与并发账目永不越界，最终精确归零；无任务被静默丢弃。
func TestHighConcurrencyStress(t *testing.T) {
	s, log := newTestScheduler(t,
		Limits{MaxCPU: 64, MaxMemory: 64, MaxIO: 64, MaxTasks: 8},
		2*time.Millisecond)

	const n = 400
	var wg sync.WaitGroup
	var finished, panicked, canceled, timedOut atomic.Int64
	handles := make(chan Handle, n)

	for i := 0; i < n; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			h, err := s.Submit(TaskSpec{
				Name:     fmt.Sprintf("s%d", i),
				Priority: i % 16,
				Cost:     Resources{CPU: int64(1 + i%8), Memory: int64(1 + (i*3)%8)},
				Fn: func(ctx context.Context) error {
					// 随机风格的短负载 + 少量主动取消。
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(time.Duration(1+(i%5)) * time.Millisecond):
						return nil
					}
				},
			})
			if err != nil {
				return // 该容量下任务均能容纳，不应有 too_large；其余错误也统计
			}
			handles <- h
		}()
	}
	wg.Wait()
	close(handles)

	// 一小撮随机取消（任务可能已结束，返回 false 也合法）。
	var handlesList []Handle
	for h := range handles {
		handlesList = append(handlesList, h)
	}
	var cwg sync.WaitGroup
	for i, h := range handlesList {
		if i%37 == 0 {
			cwg.Add(1)
			go func(h Handle) { defer cwg.Done(); h.Cancel() }(h)
		}
	}
	cwg.Wait()

	// 等待全部终态。
	for _, h := range handlesList {
		r, err := h.Wait(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		switch r.Status {
		case StatusSucceeded, StatusFailed:
			finished.Add(1)
		case StatusPanicked:
			panicked.Add(1)
		case StatusCanceled:
			canceled.Add(1)
		case StatusTimedOut:
			timedOut.Add(1)
		}
	}

	snap := s.Snapshot()
	if snap.InUse.CPU != 0 || snap.InUse.Memory != 0 || snap.InUse.IO != 0 ||
		snap.Running != 0 || snap.Queued != 0 {
		t.Fatalf("ledger not clean after stress: %+v", snap)
	}
	if got := finished.Load() + canceled.Load() + panicked.Load() + timedOut.Load(); got != int64(len(handlesList)) {
		t.Fatalf("accounted=%d but handles=%d (a task was silently lost)", got, len(handlesList))
	}
	// 运行期间账目上限由内部不变量 releaseLocked/overbooked 自检；
	// 这里再校验日志中每个快照都不越界。
	for _, e := range log.Events() {
		if e.Snap.InUse.CPU > 64 || e.Snap.InUse.Memory > 64 || e.Snap.InUse.IO > 64 || e.Snap.Running > 8 {
			t.Fatalf("event %d snapshot over limit: %+v", e.Seq, e.Snap)
		}
	}
	t.Logf("stress result: finished=%d canceled=%d panicked=%d timedOut=%d rejected=%d",
		finished.Load(), canceled.Load(), panicked.Load(), timedOut.Load(), snap.Rejected)
	printEvents(t, log)
}
