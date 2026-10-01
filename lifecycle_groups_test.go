package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestCapacityReusedImmediatelyOnResult：上游一结束、下游立刻提交时，
// 必须马上看到空出来的容量并被立即准入，不允许出现"结果已出但容量未还"
// 的空窗。设计保证：结算时先在锁内归还容量、再登记终态，因此一旦上游
// Done 可见，空出来的容量必然已在账上并已唤醒派发者。
func TestCapacityReusedImmediatelyOnResult(t *testing.T) {
	s := New(Config{MaxConcurrency: 1, ResourceLimit: 10},
		GroupConfig{Name: "A", MaxConcurrency: 1, ResourceLimit: 10},
	).WithLogger(testLogger{t})
	ctx := context.Background()

	holderExit := make(chan struct{})
	holderStarted := make(chan struct{})
	holder, err := s.Submit(ctx, &Task{ID: "holder", Group: "A", Cost: 10, Fn: func(ctx context.Context) error {
		close(holderStarted)
		<-holderExit
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-holderStarted

	// 等待中的上游与 holder 等大：holder 一释放，容量恰好整体装入。
	hUp := mustGroupTask(t, s, "upstream", "A", 10, PriorityNormal, noop())
	if st := hUp.Status(); st != StatusQueued {
		t.Fatalf("upstream must queue until holder exits, got %s", st)
	}
	close(holderExit)
	waitHandle(t, holder, time.Second)
	waitHandle(t, hUp, time.Second)

	// 上游结果已可见；此刻组账必须已归零（容量先于 Done 在账上归还）。
	snap := s.Snapshot()
	if snap.Groups["A"].InFlight != 0 || snap.Groups["A"].ResourceUsed != 0 {
		t.Fatalf("group A capacity not returned before result observed: %+v", snap.Groups["A"])
	}

	// 下游严格在上游结束后提交：应被立即准入并很快跑完，绝不干等。
	downRan := make(chan struct{})
	hDown, derr := s.Submit(ctx, &Task{ID: "downstream", Group: "A", Cost: 10,
		Fn: func(ctx context.Context) error { close(downRan); return nil }})
	if derr != nil {
		t.Fatal(derr)
	}
	select {
	case <-downRan:
	case <-time.After(2 * time.Second):
		t.Fatalf("downstream not admitted immediately after upstream result: status=%s block=%s",
			hDown.Status(), hDown.BlockReason())
	}
	waitHandle(t, hDown, time.Second)
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestRuntimeAdjustGroupLimits：组上限运行期可调——扩容放行等待任务，
// 缩容不驱逐在运行任务、账目恒真。
func TestRuntimeAdjustGroupLimits(t *testing.T) {
	s := New(Config{MaxConcurrency: 10, ResourceLimit: 100},
		GroupConfig{Name: "A", MaxConcurrency: 2, ResourceLimit: 100},
	).WithLogger(testLogger{t})
	ctx := context.Background()
	block := make(chan struct{})
	started := make(chan struct{})
	var startMu sync.Mutex
	startCount := 0

	barrier := func(ctx context.Context) error {
		startMu.Lock()
		startCount++
		if startCount == 2 {
			select {
			case <-started:
			default:
				close(started)
			}
		}
		startMu.Unlock()
		<-block
		return nil
	}
	// 初始并发上限 2：前两个运行，第三个排队。
	for i := 0; i < 3; i++ {
		mustGroupTask(t, s, "t", "A", 1, PriorityNormal, barrier)
	}
	<-started
	time.Sleep(20 * time.Millisecond)
	if s.Snapshot().Groups["A"].InFlight != 2 {
		t.Fatalf("want 2 in-flight at group limit, got %d", s.Snapshot().Groups["A"].InFlight)
	}

	// 扩容到 4：等待任务立刻获得准入。
	if err := s.ConfigureGroup(GroupConfig{Name: "A", MaxConcurrency: 4, ResourceLimit: 100}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for s.Snapshot().Groups["A"].InFlight != 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Snapshot().Groups["A"].InFlight != 3 {
		t.Fatalf("queued task must advance after group cap grows, got %d", s.Snapshot().Groups["A"].InFlight)
	}

	// 缩容到 1：不杀在运行任务（仍有 3 个），但新任务必须被本组挡住。
	if err := s.ConfigureGroup(GroupConfig{Name: "A", MaxConcurrency: 1, ResourceLimit: 100}); err != nil {
		t.Fatal(err)
	}
	h4 := mustGroupTask(t, s, "t4", "A", 1, PriorityNormal, noop())
	time.Sleep(20 * time.Millisecond)
	if h4.Status() != StatusQueued || h4.BlockReason() != BlockGroupConcurrency {
		t.Fatalf("after shrink, new task must be blocked by group concurrency: status=%s block=%s",
			h4.Status(), h4.BlockReason())
	}
	if s.Snapshot().Groups["A"].InFlight != 3 {
		t.Fatalf("shrink must not evict running tasks: got %d", s.Snapshot().Groups["A"].InFlight)
	}

	close(block)
	waitHandle(t, h4, 2*time.Second)
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	final := s.Snapshot()
	if final.InFlight != 0 || final.ResourceUsed != 0 || final.Groups["A"].InFlight != 0 {
		t.Fatalf("accounts must reconcile after resize cycle: %s", final)
	}
}

// TestRuntimeAdjustGlobalLimits：全局上限运行期缩/扩同样即时生效且账目恒真。
func TestRuntimeAdjustGlobalLimits(t *testing.T) {
	s := New(Config{MaxConcurrency: 4, ResourceLimit: 100},
		GroupConfig{Name: "A", MaxConcurrency: 10, ResourceLimit: 100},
	).WithLogger(testLogger{t})
	ctx := context.Background()

	// 缩全局并发到 1，再提交两个阻塞任务：只能有一个在跑。
	s.UpdateLimits(Config{MaxConcurrency: 1, ResourceLimit: 100})
	block := make(chan struct{})
	started := make(chan struct{})
	mustGroupTask(t, s, "run1", "A", 1, PriorityNormal, func(ctx context.Context) error {
		close(started)
		<-block
		return nil
	})
	<-started
	h2 := mustGroupTask(t, s, "run2", "A", 1, PriorityNormal, noop())
	time.Sleep(20 * time.Millisecond)
	if s.Snapshot().InFlight != 1 {
		t.Fatalf("global shrink must cap new admissions at 1, got %d", s.Snapshot().InFlight)
	}
	if h2.Status() != StatusQueued || h2.BlockReason() != BlockGlobalConcurrency {
		t.Fatalf("want queued with GLOBAL_CONCURRENCY, got %s %s", h2.Status(), h2.BlockReason())
	}

	// 扩回 3：等待任务立即准入。
	s.UpdateLimits(Config{MaxConcurrency: 3, ResourceLimit: 100})
	select {
	case <-h2.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("queued task did not advance after global cap grew")
	}
	close(block)
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestShutdownReclaimsCapacityAfterMaxWait：不理会取消的任务超过最大等待
// 后，其容量必须被收回、标记 ABANDONED、账目归零；它日后真正退出时不得
// 重复释放（账目依旧为零），其他组不受影响。
func TestShutdownReclaimsCapacityAfterMaxWait(t *testing.T) {
	s := New(Config{MaxConcurrency: 4, ResourceLimit: 20},
		GroupConfig{Name: "A", MaxConcurrency: 4, ResourceLimit: 20},
		GroupConfig{Name: "B", MaxConcurrency: 4, ResourceLimit: 20},
	).WithLogger(testLogger{t})

	stuckExit := make(chan struct{})
	stuckStarted := make(chan struct{})
	hStuck, err := s.Submit(context.Background(), &Task{ID: "stuck", Group: "A", Cost: 5,
		Fn: func(ctx context.Context) error {
			close(stuckStarted)
			select {
			case <-stuckExit: // 故意先不响应 ctx
			case <-ctx.Done(): // 但仍允许测试在第二阶段放行
				<-stuckExit
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	<-stuckStarted

	// 另一个组的正常任务在卡死任务存在期间照常跑。
	hB, err := s.Submit(context.Background(), &Task{ID: "b", Group: "B", Cost: 1, Fn: noop()})
	if err != nil {
		t.Fatal(err)
	}
	waitHandle(t, hB, time.Second)

	shCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.Shutdown(shCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}

	// 容量已强制收回、任务被标记放弃，全局与组账目都归零。
	if st := hStuck.Status(); st != StatusAbandoned {
		t.Fatalf("want ABANDONED, got %s", st)
	}
	snap := s.Snapshot()
	if snap.InFlight != 0 || snap.ResourceUsed != 0 {
		t.Fatalf("capacity must be reclaimed after max wait: %s", snap)
	}
	if ga := snap.Groups["A"]; ga.InFlight != 0 || ga.ResourceUsed != 0 {
		t.Fatalf("group A capacity must be reclaimed: %+v", ga)
	}

	// 僵尸 goroutine 最终真正退出：走 detached 分支，绝不能把账目释放成负数。
	close(stuckExit)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap = s.Snapshot()
		if snap.InFlight == 0 && snap.ResourceUsed == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if snap.InFlight != 0 || snap.ResourceUsed != 0 {
		t.Fatalf("zombie exit must not corrupt (double-release) accounts: %s", snap)
	}
	if hStuck.Status() != StatusAbandoned {
		t.Fatalf("zombie exit must not overwrite ABANDONED status, got %s", hStuck.Status())
	}
}

// TestCloseGroupReclaimsAndOthersSurvive：单组关闭超时回收后，其他组
// （含默认组）继续正常服务，且账目精确。
func TestCloseGroupReclaimsAndOthersSurvive(t *testing.T) {
	s := New(Config{MaxConcurrency: 8, ResourceLimit: 100},
		GroupConfig{Name: "A", MaxConcurrency: 8, ResourceLimit: 100},
	).WithLogger(testLogger{t})

	stuckExit := make(chan struct{})
	stuckStarted := make(chan struct{})
	_, err := s.Submit(context.Background(), &Task{ID: "stuck-a", Group: "A", Cost: 3,
		Fn: func(ctx context.Context) error {
			close(stuckStarted)
			select {
			case <-stuckExit:
			case <-ctx.Done():
				<-stuckExit
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	<-stuckStarted

	cgCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.CloseGroup("A", cgCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded from CloseGroup, got %v", err)
	}
	if ga := s.Snapshot().Groups["A"]; ga.InFlight != 0 || ga.ResourceUsed != 0 {
		t.Fatalf("group A capacity must be reclaimed, got %+v", ga)
	}

	// A 已停收，但默认组仍可反复服务，且全局账目始终自洽。
	for i := 0; i < 20; i++ {
		h, err := s.Submit(context.Background(), task("default", 2, PriorityNormal, noop()))
		if err != nil {
			t.Fatalf("default group must survive A closure: %v", err)
		}
		waitHandle(t, h, time.Second)
	}
	close(stuckExit)
	time.Sleep(20 * time.Millisecond)
	if snap := s.Snapshot(); snap.InFlight != 0 || snap.ResourceUsed != 0 {
		t.Fatalf("accounts corrupted after group close reclaim: %s", snap)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestCloseGroupDrainsQueued：关闭某组时其排队任务被取消，他组排队不受影响。
func TestCloseGroupDrainsQueued(t *testing.T) {
	s := New(Config{MaxConcurrency: 1, ResourceLimit: 100},
		GroupConfig{Name: "A", MaxConcurrency: 10, ResourceLimit: 100},
		GroupConfig{Name: "B", MaxConcurrency: 10, ResourceLimit: 100},
	).WithLogger(testLogger{t})
	block := make(chan struct{})
	started := make(chan struct{})
	// holder 放默认组占住全局唯一槽，迫使 A/B 任务排队。
	_, _ = s.Submit(context.Background(), task("holder", 1, PriorityNormal, func(ctx context.Context) error {
		close(started)
		<-block
		return nil
	}))
	<-started

	hA := mustGroupTask(t, s, "qa", "A", 1, PriorityNormal, noop())
	hB := mustGroupTask(t, s, "qb", "B", 1, PriorityNormal, noop())
	if err := s.CloseGroup("A", context.Background()); err != nil {
		t.Fatal(err)
	}
	waitHandle(t, hA, time.Second)
	if hA.Status() != StatusCanceled {
		t.Fatalf("queued A task must be canceled, got %s", hA.Status())
	}
	if hB.Status() != StatusQueued {
		t.Fatalf("B queued task must be unaffected, got %s", hB.Status())
	}
	close(block)
	waitHandle(t, hB, time.Second)
	if hB.Status() != StatusSucceeded {
		t.Fatalf("B task should run after holder release, got %s", hB.Status())
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
