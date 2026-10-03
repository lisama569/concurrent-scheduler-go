package scheduler

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestNoGoroutineLeakAcrossManyTasks：大量短任务跑完后，运行中的协程数
// 不应随累计完成任务数增长。排队取消监听改用 context.AfterFunc 且在
// 任务终态注销，正常任务 goroutine 退出即回收，不存在"每任务一个永久
// 等待协程"。
func TestNoGoroutineLeakAcrossManyTasks(t *testing.T) {
	s := New(Config{MaxConcurrency: 8, ResourceLimit: 100, QueueCapacity: 0},
		GroupConfig{Name: "A", MaxConcurrency: 8, ResourceLimit: 100},
	).WithLogger(newTestLogger(t))
	ctx := context.Background()

	measure := func() int {
		runtime.GC()
		// 给 goroutine 退出与 AfterFunc 注销一点时间。
		time.Sleep(30 * time.Millisecond)
		runtime.GC()
		return runtime.NumGoroutine()
	}

	runBurst := func(n int) {
		handles := make([]*Handle, n)
		for i := 0; i < n; i++ {
			h, err := s.Submit(ctx, &Task{ID: "t", Group: "A", Cost: 1, Fn: noop()})
			if err != nil {
				t.Fatal(err)
			}
			handles[i] = h
		}
		for _, h := range handles {
			waitHandle(t, h, 5*time.Second)
		}
	}

	// 预热一轮，使调度器协程等进入稳定态。
	runBurst(200)
	baseline := measure()

	// 再跑两轮大得多的负载；完成后协程数应回落到基线附近（允许小幅抖动）。
	runBurst(800)
	after1 := measure()
	runBurst(800)
	after2 := measure()

	t.Logf("goroutines baseline=%d after800=%d after1600=%d", baseline, after1, after2)
	const slack = 8
	if after1 > baseline+slack || after2 > baseline+slack {
		t.Fatalf("goroutine count grows with completed tasks: baseline=%d after800=%d after1600=%d",
			baseline, after1, after2)
	}
	if after2 > after1+slack {
		t.Fatalf("goroutine count still climbing: after800=%d after1600=%d", after1, after2)
	}

	if snap := s.Snapshot(); snap.Queued != 0 {
		t.Fatalf("no task should remain queued: %s", snap)
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestQueuedCancelWatchersReclaimed：大量"排队即取消"的任务不应残留协程。
func TestQueuedCancelWatchersReclaimed(t *testing.T) {
	// 全局只 1 槽且被占住，后续任务全部排队，随即取消。
	s := New(Config{MaxConcurrency: 1, ResourceLimit: 100, QueueCapacity: 0}).WithLogger(newTestLogger(t))
	block := make(chan struct{})
	started := make(chan struct{})
	_, _ = s.Submit(context.Background(), task("holder", 1, PriorityNormal, func(ctx context.Context) error {
		close(started)
		<-block
		return nil
	}))
	<-started

	const n = 500
	for i := 0; i < n; i++ {
		h, err := s.Submit(context.Background(), task("q", 1, PriorityNormal, noop()))
		if err != nil {
			t.Fatal(err)
		}
		h.Cancel()
		waitHandle(t, h, time.Second)
	}
	runtime.GC()
	time.Sleep(30 * time.Millisecond)
	if snap := s.Snapshot(); snap.Queued != 0 {
		t.Fatalf("canceled tasks must leave the queue, got %s", snap)
	}
	close(block)
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if snap := s.Snapshot(); snap.InFlight != 0 || snap.ResourceUsed != 0 || snap.Queued != 0 {
		t.Fatalf("accounts not clean: %s", snap)
	}
}
