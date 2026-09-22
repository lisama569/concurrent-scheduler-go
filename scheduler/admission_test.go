package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestOverResourceQueuesNotDrops: 资源暂时不足时任务排队等待，
// 日志中给出 over_resource 的决策依据；任务结束后自动补入。
func TestOverResourceQueues(t *testing.T) {
	s, log := newTestScheduler(t,
		Limits{MaxCPU: 10, MaxMemory: 10, MaxIO: 10, MaxTasks: 8}, time.Millisecond)

	hold := make(chan struct{})
	_, err := s.Submit(TaskSpec{
		Name: "holder", Cost: Resources{CPU: 8},
		Fn: func(ctx context.Context) error { <-hold; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	waitRunning(t, s, 1)

	// 需要 5 CPU：并发度充足（8），但资源不足 -> 排队而非拒绝。
	h, err := s.Submit(TaskSpec{
		Name: "needs5", Cost: Resources{CPU: 5},
		Fn: func(ctx context.Context) error { return nil },
	})
	if err != nil {
		t.Fatalf("temporary resource shortage must queue, got err=%v", err)
	}
	if !waitUntil(t, func() bool { return s.Snapshot().Queued == 1 }) {
		t.Fatal("task should be queued while resources are insufficient")
	}
	// Running 仍为 1：排队任务未被错误准入。
	if snap := s.Snapshot(); snap.Running != 1 || snap.InUse.CPU != 8 {
		t.Fatalf("queued task must not consume resources: %+v", snap)
	}

	close(hold)
	res, _ := h.Wait(context.Background())
	if res.Status != StatusSucceeded {
		t.Fatalf("queued task should run after resources free, got %s", res.Status)
	}
	waitZero(t, s)
	assertReleased(t, s)

	// 决策日志中必须能找到 over_resource 原因。
	found := false
	for _, e := range log.Events() {
		if e.Reason == ReasonOverResource {
			found = true
		}
	}
	if !found {
		t.Fatal("expected an over_resource decision in the log")
	}
	printEvents(t, log)
}

// TestOverConcurrencyQueues: 并发度满时任务排队，日志给出 over_concurrency。
func TestOverConcurrencyQueues(t *testing.T) {
	s, log := newTestScheduler(t,
		Limits{MaxCPU: 100, MaxMemory: 100, MaxIO: 100, MaxTasks: 1}, time.Millisecond)

	hold := make(chan struct{})
	_, err := s.Submit(TaskSpec{
		Name: "only-slot", Cost: Resources{CPU: 1},
		Fn: func(ctx context.Context) error { <-hold; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	waitRunning(t, s, 1)
	h, err := s.Submit(TaskSpec{
		Name: "wants-slot", Cost: Resources{CPU: 1},
		Fn: func(ctx context.Context) error { return nil },
	})
	if err != nil {
		t.Fatalf("concurrency cap must queue, not reject: %v", err)
	}
	close(hold)
	if r, _ := h.Wait(context.Background()); r.Status != StatusSucceeded {
		t.Fatalf("status=%s", r.Status)
	}
	waitZero(t, s)
	found := false
	for _, e := range log.Events() {
		if e.Reason == ReasonOverConcurrency {
			found = true
		}
	}
	if !found {
		t.Fatal("expected an over_concurrency decision in the log")
	}
	printEvents(t, log)
}

// TestShuttingDownReject: 关闭开始后新提交按 shutting_down 拒绝。
func TestShuttingDownReject(t *testing.T) {
	log := NewSliceLogger()
	s := New(Config{
		Limits: Limits{MaxCPU: 10, MaxTasks: 2},
		Tick:   time.Millisecond, RunGrace: 20 * time.Millisecond,
		Logger: log,
	})

	hold := make(chan struct{})
	_, err := s.Submit(TaskSpec{
		Name: "running-at-shutdown", Cost: Resources{CPU: 1},
		Fn: func(ctx context.Context) error { <-hold; return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	waitRunning(t, s, 1)

	shutdownErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownErr <- s.Shutdown(ctx)
	}()

	// 等待关闭开始（不再接收新任务）。
	if !waitUntil(t, func() bool { return s.Snapshot().Shutting }) {
		t.Fatal("scheduler never entered shutting state")
	}
	_, err = s.Submit(TaskSpec{
		Name: "late-submission", Fn: func(ctx context.Context) error { return nil },
	})
	if !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("want shutting_down rejection, got %v", err)
	}
	close(hold)
	if err := <-shutdownErr; err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	printEvents(t, log)
}
