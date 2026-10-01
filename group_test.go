package scheduler

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// gtask 构造带业务组的任务。
func gtask(id, group string, cost int64, p Priority, fn func(ctx context.Context) error) *Task {
	t := task(id, cost, p, fn)
	t.Group = group
	return t
}

// groupCfg 构造一个两业务组 + 全局上限的常用配置。
func groupCfg() Config {
	return Config{
		MaxConcurrency: 2,
		ResourceLimit:  10,
		QueueCapacity:  64,
		Groups: []GroupConfig{
			{Name: "A", MaxConcurrency: 1, ResourceLimit: 4},
			{Name: "B", MaxConcurrency: 2, ResourceLimit: 6},
		},
	}
}

func snapGroup(s *Scheduler, name string) GroupSnapshot {
	for _, g := range s.Snapshot().Groups {
		if g.Name == name {
			return g
		}
	}
	return GroupSnapshot{Name: name + "(missing)"}
}

// TestGroupIsolationCannotStealOthersBudget：A 组占满自己的预算后，
// 不能动用 B 组或全局的余量；A 的后续任务排队，账目分层自洽。
func TestGroupIsolationCannotStealOthersBudget(t *testing.T) {
	s := newTestScheduler(t, groupCfg())
	ctx := context.Background()
	block := make(chan struct{})
	started := make(chan struct{})

	// A 组并发上限 1、资源上限 4：一个 cost=4 的任务即占满 A。
	if _, err := s.Submit(ctx, gtask("a1", "A", 4, PriorityNormal, func(ctx context.Context) error {
		close(started)
		<-block
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	<-started

	// A 再来任务：本组并发与资源均耗尽，只能排队，不能挤占全局余量。
	h, err := s.Submit(ctx, gtask("a2", "A", 1, PriorityNormal, noop()))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if h.Status() != StatusQueued {
		t.Fatalf("a2 must stay queued behind a1, got %s", h.Status())
	}
	if wr := h.WaitReason(); wr != WaitReasonGroupBudget {
		t.Fatalf("a2 wait reason = %s, want GROUP_BUDGET", wr)
	}
	snap := s.Snapshot()
	if ga := snapGroup(s, "A"); ga.InFlight != 1 || ga.ResourceUsed != 4 || ga.Queued != 1 {
		t.Fatalf("group A accounts wrong: %+v", ga)
	}
	if snap.InFlight != 1 || snap.ResourceUsed != 4 {
		t.Fatalf("global accounts should reflect only a1: %s", snap)
	}

	// B 组预算独立：cost=3 的任务应立即准入（B 余量充足，全局也还剩槽位）。
	bStarted := make(chan struct{})
	if _, err := s.Submit(ctx, gtask("b1", "B", 3, PriorityNormal, func(ctx context.Context) error {
		close(bStarted)
		<-block
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	select {
	case <-bStarted:
	case <-time.After(time.Second):
		t.Fatal("B must be admitted while A is at its own cap (no cross-group blocking)")
	}

	// 此刻全局并发 2/2 打满，但 B 组自身（上限 2 并发 / 6 资源）尚有余量：
	// B 再来任务应被"全局预算"挡住，而不是本组预算。
	hb, err := s.Submit(ctx, gtask("b2", "B", 1, PriorityNormal, noop()))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if hb.Status() != StatusQueued {
		t.Fatalf("b2 must be queued, got %s", hb.Status())
	}
	if wr := hb.WaitReason(); wr != WaitReasonGlobalBudget {
		t.Fatalf("b2 wait reason = %s, want GLOBAL_BUDGET", wr)
	}

	close(block)
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	final := s.Snapshot()
	if final.InFlight != 0 || final.ResourceUsed != 0 || final.Queued != 0 {
		t.Fatalf("accounts not clean: %s", final)
	}
}

// TestGroupRejectReasons：未注册组 / 组关闭 / 组级硬上限 / 全局硬上限 /
// 队列满，五种拒绝必须各有可区分的原因，且绝不静默丢弃。
func TestGroupRejectReasons(t *testing.T) {
	t.Run("unknown group", func(t *testing.T) {
		s := newTestScheduler(t, groupCfg())
		h, err := s.Submit(context.Background(), gtask("x", "NOPE", 1, PriorityNormal, noop()))
		var re *RejectError
		if !errors.As(err, &re) || re.Reason != ReasonUnknownGroup {
			t.Fatalf("want UNKNOWN_GROUP, got %v", err)
		}
		if h.Status() != StatusRejected {
			t.Fatalf("want REJECTED, got %s", h.Status())
		}
		if err := s.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("group stopped", func(t *testing.T) {
		s := newTestScheduler(t, groupCfg())
		if err := s.ShutdownGroup(context.Background(), "A"); err != nil {
			t.Fatal(err)
		}
		h, err := s.Submit(context.Background(), gtask("a", "A", 1, PriorityNormal, noop()))
		var re *RejectError
		if !errors.As(err, &re) || re.Reason != ReasonGroupStopped {
			t.Fatalf("want GROUP_STOPPED, got %v", err)
		}
		if h.Status() != StatusRejected {
			t.Fatalf("want REJECTED, got %s", h.Status())
		}
		// B 组不受 A 组关闭影响。
		hb, err := s.Submit(context.Background(), gtask("b", "B", 1, PriorityNormal, noop()))
		if err != nil {
			t.Fatalf("group B must remain usable after A shutdown: %v", err)
		}
		if hb.Wait() != nil {
			t.Fatalf("group B task should succeed, got %v", hb.Err())
		}
		if err := s.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("exceeds group limit", func(t *testing.T) {
		s := newTestScheduler(t, groupCfg())
		// A 组资源上限 4；cost=5 装不进 A，即便全局上限 10 也永不可能。
		h, err := s.Submit(context.Background(), gtask("big-a", "A", 5, PriorityNormal, noop()))
		var re *RejectError
		if !errors.As(err, &re) || re.Reason != ReasonExceedsGroupLimit {
			t.Fatalf("want EXCEEDS_GROUP_LIMIT, got %v", err)
		}
		if h.Status() != StatusRejected {
			t.Fatalf("want REJECTED, got %s", h.Status())
		}
		if snap := s.Snapshot(); snap.InFlight != 0 || snap.ResourceUsed != 0 {
			t.Fatalf("rejected task must not consume quota: %s", snap)
		}
		if err := s.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("exceeds global limit", func(t *testing.T) {
		s := newTestScheduler(t, Config{
			MaxConcurrency: 2, ResourceLimit: 10, QueueCapacity: 4,
			Groups: []GroupConfig{{Name: "A", MaxConcurrency: 0, ResourceLimit: -1}},
		})
		h, err := s.Submit(context.Background(), gtask("big", "A", 11, PriorityNormal, noop()))
		var re *RejectError
		if !errors.As(err, &re) || re.Reason != ReasonExceedsHardLimit {
			t.Fatalf("want EXCEEDS_HARD_LIMIT, got %v", err)
		}
		if h.Status() != StatusRejected {
			t.Fatalf("want REJECTED, got %s", h.Status())
		}
		if err := s.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("queue full is global", func(t *testing.T) {
		cfg := groupCfg()
		cfg.QueueCapacity = 1
		s := newTestScheduler(t, cfg)
		ctx := context.Background()
		block := make(chan struct{})
		started := make(chan struct{})
		// 占满 A（1 并发槽）。
		if _, err := s.Submit(ctx, gtask("a1", "A", 4, PriorityHigh, func(ctx context.Context) error {
			close(started)
			<-block
			return nil
		})); err != nil {
			t.Fatal(err)
		}
		<-started
		// 全局并发还剩 1 槽：让 B 的任务占掉它，之后所有任务只能排队。
		bStart := make(chan struct{})
		if _, err := s.Submit(ctx, gtask("b1", "B", 6, PriorityNormal, func(ctx context.Context) error {
			close(bStart)
			<-block
			return nil
		})); err != nil {
			t.Fatal(err)
		}
		<-bStart
		// 队列容量 1：A 的排队任务占掉唯一位。
		if _, err := s.Submit(ctx, gtask("a2", "A", 1, PriorityNormal, noop())); err != nil {
			t.Fatal(err)
		}
		// B 的下一个任务：本组尚有余量、但全局满且队列满 => QUEUE_FULL。
		h, err := s.Submit(ctx, gtask("b2", "B", 1, PriorityNormal, noop()))
		var re *RejectError
		if !errors.As(err, &re) || re.Reason != ReasonQueueFull {
			t.Fatalf("want QUEUE_FULL, got %v", err)
		}
		if h.Status() != StatusRejected {
			t.Fatalf("want REJECTED, got %s", h.Status())
		}
		close(block)
		if err := s.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

// TestInterGroupFairness：A 组持续提交高优先级任务时，不能把 B 组饿死。
// 组间轮转保证每一轮都给 B 组一个槽位；B 的低优任务在有限时间内必被准入。
func TestInterGroupFairness(t *testing.T) {
	cfg := Config{
		MaxConcurrency: 2, ResourceLimit: 10, QueueCapacity: 128,
		Groups: []GroupConfig{
			{Name: "A", MaxConcurrency: 1, ResourceLimit: 5},
			{Name: "B", MaxConcurrency: 1, ResourceLimit: 5},
		},
	}
	s := newTestScheduler(t, cfg)
	ctx := context.Background()
	block := make(chan struct{})
	aStarted := make(chan struct{})
	bStarted := make(chan struct{})

	// 两个占坑任务各占满本组唯一槽位。
	if _, err := s.Submit(ctx, gtask("holder-a", "A", 1, PriorityNormal, func(ctx context.Context) error {
		select {
		case <-aStarted:
		default:
			close(aStarted)
		}
		<-block
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(ctx, gtask("holder-b", "B", 1, PriorityNormal, func(ctx context.Context) error {
		select {
		case <-bStarted:
		default:
			close(bStarted)
		}
		<-block
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	<-aStarted
	<-bStarted

	// A 高优排队任务 + B 低优排队任务同时等待。
	if _, err := s.Submit(ctx, gtask("a-high", "A", 1, PriorityHigh, noop())); err != nil {
		t.Fatal(err)
	}
	bDone := make(chan struct{})
	if _, err := s.Submit(ctx, gtask("b-low", "B", 1, PriorityLow, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		close(bDone)
		return nil
	})); err != nil {
		t.Fatal(err)
	}

	// 占坑释放后，持续给 A 灌入高优短任务制造压力。
	stopPressure := make(chan struct{})
	var pressureWG sync.WaitGroup
	pressureWG.Add(1)
	go func() {
		defer pressureWG.Done()
		i := 0
		for {
			select {
			case <-stopPressure:
				return
			default:
			}
			h, err := s.Submit(ctx, gtask(fmt.Sprintf("a-hi-%d", i), "A", 1, PriorityHigh, sleepTask(time.Millisecond)))
			i++
			if err != nil {
				time.Sleep(time.Millisecond) // 队列满退避
				continue
			}
			<-h.Done()
		}
	}()

	close(block)
	select {
	case <-bDone:
	case <-time.After(3 * time.Second):
		t.Fatal("group B low-priority task starved: never admitted under group A high-priority pressure")
	}
	close(stopPressure)
	pressureWG.Wait()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestIntraGroupPriorityPreserved：组内仍按高优先级优先（含低优老化提升）。
// 两个同组任务在组槽位被占时排队；释放后 HIGH 先于 LOW。
func TestIntraGroupPriorityPreserved(t *testing.T) {
	cfg := Config{
		MaxConcurrency: 2, ResourceLimit: 10,
		Groups: []GroupConfig{{Name: "A", MaxConcurrency: 1, ResourceLimit: 5}},
	}
	s := newTestScheduler(t, cfg)
	ctx := context.Background()
	block := make(chan struct{})
	holderStarted := make(chan struct{})
	if _, err := s.Submit(ctx, gtask("holder", "A", 1, PriorityNormal, func(ctx context.Context) error {
		close(holderStarted)
		<-block
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	<-holderStarted

	var mu sync.Mutex
	var order []string
	gotAll := make(chan struct{})
	rec := func(id string) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			mu.Lock()
			order = append(order, id)
			if len(order) == 2 {
				close(gotAll)
			}
			mu.Unlock()
			return nil
		}
	}
	if _, err := s.Submit(ctx, gtask("low", "A", 1, PriorityLow, rec("low"))); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(ctx, gtask("high", "A", 1, PriorityHigh, rec("high"))); err != nil {
		t.Fatal(err)
	}
	close(block)
	select {
	case <-gotAll:
	case <-time.After(2 * time.Second):
		t.Fatalf("queued group tasks not drained, order=%v", order)
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "high" || order[1] != "low" {
		t.Fatalf("intra-group admit order=%v, want [high low]", order)
	}
}

// TestCapacityReusableImmediatelyOnResult：对"任务出结果"与"容量腾出来"
// 之间的空窗零容忍。settleBarrier 在结算记账完成的瞬间（仍持锁）触发，
// 我们在那一刻派生一个提交 goroutine：它立刻阻塞在 mu 上，成为结算解锁后
// 最早的观察者；解锁瞬间账目已是"容量已归还"，因此下游提交必须直接走立即
// 准入口径，而不是先排队再等派发（后者即代表存在可见空窗）。
func TestCapacityReusableImmediatelyOnResult(t *testing.T) {
	cfg := Config{
		MaxConcurrency: 1, ResourceLimit: 5,
		Groups: []GroupConfig{{Name: "A", MaxConcurrency: 1, ResourceLimit: 5}},
	}
	s := newTestScheduler(t, cfg)
	ctx := context.Background()
	block := make(chan struct{})
	started := make(chan struct{})

	hUp, err := s.Submit(ctx, gtask("up", "A", 5, PriorityNormal, func(ctx context.Context) error {
		close(started)
		<-block
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	<-started

	type submitResult struct {
		h   *Handle
		err error
	}
	submitAtRelease := make(chan submitResult, 1)
	once := sync.Once{}
	s.settleBarrier = func(id string) {
		if id != "up" {
			return
		}
		once.Do(func() {
			// 不能在持锁的 barrier 内直接 Submit（自锁死锁）；派生 goroutine，
			// 它在 settle 释放锁的同一刻即可取得锁并看到已归还的容量。
			go func() {
				h, subErr := s.Submit(ctx, gtask("down", "A", 5, PriorityNormal, noop()))
				submitAtRelease <- submitResult{h, subErr}
			}()
		})
	}

	close(block) // 放行上游 -> settle 归还容量 -> barrier 派生的提交抢锁
	var res submitResult
	select {
	case res = <-submitAtRelease:
	case <-time.After(2 * time.Second):
		t.Fatal("downstream submit at release point did not complete")
	}
	if res.err != nil {
		t.Fatalf("downstream submit at release point: %v", res.err)
	}
	waitHandle(t, hUp, time.Second)
	waitHandle(t, res.h, 2*time.Second)
	if res.h.Status() != StatusSucceeded {
		t.Fatalf("downstream final status=%s err=%v", res.h.Status(), res.h.Err())
	}
	// 关键断言：下游在容量归还的第一时点被准入，WaitReason 必须是 NONE
	// （若它曾因"本组/全局预算不够"而入队，即说明容量归还存在可见空窗）。
	if wr := res.h.WaitReason(); wr != WaitReasonNone {
		t.Fatalf("downstream should be admitted immediately at release, wait reason=%s", wr)
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestCapacityReusableAcrossGroups：A 组任务一释放全局容量，B 组等待者
// 立即被派发，不存在需要额外唤醒的空窗。
func TestCapacityReusableAcrossGroups(t *testing.T) {
	cfg := Config{
		MaxConcurrency: 1, ResourceLimit: 5,
		Groups: []GroupConfig{
			{Name: "A", MaxConcurrency: 1, ResourceLimit: 5},
			{Name: "B", MaxConcurrency: 1, ResourceLimit: 5},
		},
	}
	s := newTestScheduler(t, cfg)
	ctx := context.Background()
	block := make(chan struct{})
	started := make(chan struct{})
	if _, err := s.Submit(ctx, gtask("a", "A", 5, PriorityNormal, func(ctx context.Context) error {
		close(started)
		<-block
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	<-started

	bRan := make(chan struct{})
	if _, err := s.Submit(ctx, gtask("b", "B", 5, PriorityNormal, func(ctx context.Context) error {
		close(bRan)
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	close(block)
	select {
	case <-bRan:
	case <-time.After(2 * time.Second):
		t.Fatal("group B task should be dispatched immediately once A released capacity")
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestShutdownGroupOnlyStopsThatGroup：ShutdownGroup 只停收并清空该组队列，
// 其它组的排队与提交不受影响。
func TestShutdownGroupOnlyStopsThatGroup(t *testing.T) {
	cfg := Config{
		MaxConcurrency: 1, ResourceLimit: 10, QueueCapacity: 32,
		Groups: []GroupConfig{
			{Name: "A", MaxConcurrency: 1, ResourceLimit: 10},
			{Name: "B", MaxConcurrency: 1, ResourceLimit: 10},
		},
	}
	s := newTestScheduler(t, cfg)
	ctx := context.Background()
	started := make(chan struct{})
	release := make(chan struct{})

	// A 占住唯一全局槽位。
	if _, err := s.Submit(ctx, gtask("a-holder", "A", 1, PriorityNormal, func(ctx context.Context) error {
		close(started)
		<-release
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	<-started

	// 两个组各排一个任务。
	hA, err := s.Submit(ctx, gtask("a-wait", "A", 1, PriorityNormal, noop()))
	if err != nil {
		t.Fatal(err)
	}
	hB, err := s.Submit(ctx, gtask("b-wait", "B", 1, PriorityNormal, func(ctx context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}

	// 只关闭 A：A 的排队任务在摘队列阶段立即被取消；在运行的 holder 收到
	// ctx 取消后退出。先放行 holder（它会自然结束并归还容量），组关闭随即完成。
	close(release)
	if err := s.ShutdownGroup(context.Background(), "A"); err != nil {
		t.Fatal(err)
	}
	waitHandle(t, hA, time.Second)
	if hA.Status() != StatusCanceled {
		t.Fatalf("queued A task must be canceled, got %s", hA.Status())
	}
	if ga := snapGroup(s, "A"); ga.Queued != 0 || !ga.Stopped {
		t.Fatalf("group A should be stopped with empty queue: %+v", ga)
	}

	// B 的排队任务在 holder 释放全局槽位后照常被派发执行。
	waitHandle(t, hB, 2*time.Second)
	if hB.Status() != StatusSucceeded {
		t.Fatalf("group B task must still succeed, got %s", hB.Status())
	}

	// A 已停收：新提交按 GROUP_STOPPED 拒绝；B 仍可用。
	if _, err := s.Submit(ctx, gtask("a-late", "A", 1, PriorityNormal, noop())); err == nil {
		t.Fatal("submit to shut group A must be rejected")
	} else {
		var re *RejectError
		if !errors.As(err, &re) || re.Reason != ReasonGroupStopped {
			t.Fatalf("want GROUP_STOPPED, got %v", err)
		}
	}
	hB2, err := s.Submit(ctx, gtask("b2", "B", 1, PriorityNormal, noop()))
	if err != nil {
		t.Fatalf("group B must remain usable: %v", err)
	}
	waitHandle(t, hB2, time.Second)

	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestShutdownGraceReclaimsCapacity：无视取消一直卡着的任务，在宽限期满后
// 容量被收回、任务标记 ABANDONED；账目归零且不会重复释放或出现负数。
func TestShutdownGraceReclaimsCapacity(t *testing.T) {
	cfg := Config{
		MaxConcurrency: 2, ResourceLimit: 10,
		Groups: []GroupConfig{{Name: "A", MaxConcurrency: 2, ResourceLimit: 10}},
	}
	s := newTestScheduler(t, cfg)
	ctx := context.Background()
	started := make(chan struct{})

	hStuck, err := s.Submit(ctx, gtask("stuck", "A", 3, PriorityNormal, func(ctx context.Context) error {
		close(started)
		select {} // 故意无视 ctx，永不退出
	}))
	if err != nil {
		t.Fatal(err)
	}
	<-started
	// 卡住期间同组正常任务仍可使用剩余容量。
	hWork, err := s.Submit(ctx, gtask("work", "A", 1, PriorityNormal, noop()))
	if err != nil {
		t.Fatal(err)
	}
	waitHandle(t, hWork, time.Second)

	if snap := s.Snapshot(); snap.ResourceUsed != 3 {
		t.Fatalf("before grace: stuck task must still hold 3, got %s", snap)
	}

	shutdownCtx, cancel := context.WithTimeout(ctx, 60*time.Millisecond)
	defer cancel()
	if err := s.Shutdown(shutdownCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}

	// 宽限期满：句柄被终结为 ABANDONED，容量已收回。
	waitHandle(t, hStuck, time.Second)
	if hStuck.Status() != StatusAbandoned {
		t.Fatalf("want ABANDONED, got %s", hStuck.Status())
	}
	if !errors.Is(hStuck.Err(), ErrTaskAbandoned) {
		t.Fatalf("want ErrTaskAbandoned, got %v", hStuck.Err())
	}
	snap := s.Snapshot()
	if snap.InFlight != 0 || snap.ResourceUsed != 0 {
		t.Fatalf("capacity must be fully reclaimed: %s", snap)
	}
	if ga := snapGroup(s, "A"); ga.InFlight != 0 || ga.ResourceUsed != 0 {
		t.Fatalf("group A accounts must be zero: %+v", ga)
	}
	// 注意：stuck 的 goroutine 仍在 select{} 上（Go 无法强杀 goroutine），
	// 但它已与账目脱钩；即使日后退出也只会走"迟到结算"，不重复释放。
}

// TestShutdownGroupGraceReclaimKeepsOthersAlive：只对某组宽限期满收回容量后，
// 其它组仍能继续正常提交与执行（组件不必整体作废）。
func TestShutdownGroupGraceReclaimKeepsOthersAlive(t *testing.T) {
	cfg := Config{
		MaxConcurrency: 2, ResourceLimit: 10,
		Groups: []GroupConfig{
			{Name: "A", MaxConcurrency: 1, ResourceLimit: 10},
			{Name: "B", MaxConcurrency: 1, ResourceLimit: 10},
		},
	}
	s := newTestScheduler(t, cfg)
	ctx := context.Background()
	aStarted := make(chan struct{})
	hA, err := s.Submit(ctx, gtask("a-stuck", "A", 4, PriorityNormal, func(ctx context.Context) error {
		close(aStarted)
		select {}
	}))
	if err != nil {
		t.Fatal(err)
	}
	<-aStarted

	gctx, cancel := context.WithTimeout(ctx, 60*time.Millisecond)
	defer cancel()
	if err := s.ShutdownGroup(gctx, "A"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	waitHandle(t, hA, time.Second)
	if hA.Status() != StatusAbandoned {
		t.Fatalf("want ABANDONED for A, got %s", hA.Status())
	}
	if snap := s.Snapshot(); snap.Stopped {
		t.Fatal("scheduler-wide stopped flag must remain false after per-group shutdown")
	}

	// A 容量已从全局账本收回；B 仍可继续工作。
	hB, err := s.Submit(ctx, gtask("b-after", "B", 6, PriorityNormal, noop()))
	if err != nil {
		t.Fatalf("group B must remain usable after A reclamation: %v", err)
	}
	waitHandle(t, hB, time.Second)
	if hB.Status() != StatusSucceeded {
		t.Fatalf("want B SUCCEEDED, got %s", hB.Status())
	}

	// 整体关闭应当干净（A 票据已不存在，只剩 B 且已完成）。
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("final shutdown: %v", err)
	}
	if snap := s.Snapshot(); snap.InFlight != 0 || snap.ResourceUsed != 0 {
		t.Fatalf("accounts not clean: %s", snap)
	}
}

// TestRuntimeUpsertGroupThenProceeds：运行期新增业务组 / 扩容后，等待中的
// 任务继续往前走，无需重建调度器。
func TestRuntimeUpsertGroupThenProceeds(t *testing.T) {
	cfg := Config{
		MaxConcurrency: 4, ResourceLimit: 20, QueueCapacity: 32,
		Groups: []GroupConfig{{Name: "A", MaxConcurrency: 1, ResourceLimit: 2}},
	}
	s := newTestScheduler(t, cfg)
	ctx := context.Background()

	// C 组尚未注册：显式拒绝，不静默排队。
	h, err := s.Submit(ctx, gtask("c0", "C", 1, PriorityNormal, noop()))
	var re *RejectError
	if !errors.As(err, &re) || re.Reason != ReasonUnknownGroup {
		t.Fatalf("want UNKNOWN_GROUP for C, got %v", err)
	}
	if h.Status() != StatusRejected {
		t.Fatalf("want REJECTED, got %s", h.Status())
	}

	// 运行期注册 C：立即可以提交并执行。
	if err := s.UpsertGroup(GroupConfig{Name: "C", MaxConcurrency: 1, ResourceLimit: 2}); err != nil {
		t.Fatal(err)
	}
	hC, err := s.Submit(ctx, gtask("c1", "C", 2, PriorityNormal, noop()))
	if err != nil {
		t.Fatalf("group C should be usable after upsert: %v", err)
	}
	waitHandle(t, hC, time.Second)

	// A 组资源上限 2：提交 cost=4 会立即按组硬上限拒绝。
	if _, err := s.Submit(ctx, gtask("a-big", "A", 4, PriorityNormal, noop())); err == nil {
		t.Fatal("cost above current group limit must be rejected")
	} else {
		var re2 *RejectError
		if !errors.As(err, &re2) || re2.Reason != ReasonExceedsGroupLimit {
			t.Fatalf("want EXCEEDS_GROUP_LIMIT, got %v", err)
		}
	}

	// A 占满 cost=2 后，再来一个 cost=2 的任务只能排队（本组资源不足）。
	block := make(chan struct{})
	aStarted := make(chan struct{})
	hA1, err := s.Submit(ctx, gtask("a1", "A", 2, PriorityNormal, func(ctx context.Context) error {
		close(aStarted)
		<-block
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	<-aStarted
	hA2, err := s.Submit(ctx, gtask("a2", "A", 2, PriorityNormal, noop()))
	if err != nil {
		t.Fatal(err)
	}
	if wr := hA2.WaitReason(); wr != WaitReasonGroupBudget {
		t.Fatalf("a2 wait reason=%s want GROUP_BUDGET", wr)
	}
	time.Sleep(20 * time.Millisecond)
	if hA2.Status() != StatusQueued {
		t.Fatalf("a2 must stay queued until group expansion, got %s", hA2.Status())
	}

	// 扩容 A 资源到 4：a2 立刻被准入（全局仍有余量）。
	if err := s.UpsertGroup(GroupConfig{Name: "A", MaxConcurrency: 2, ResourceLimit: 4}); err != nil {
		t.Fatal(err)
	}
	waitHandle(t, hA2, 2*time.Second)
	if hA2.Status() != StatusSucceeded {
		t.Fatalf("a2 should proceed after group expansion, got %s", hA2.Status())
	}

	close(block)
	waitHandle(t, hA1, time.Second)
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestRuntimeGlobalLimitsShrinkGrow：全局缩容不抢占在跑任务，但排空期间
// 不会再放新任务把新上限顶穿；扩容后等待任务继续。看门狗全程断言账目不越界。
func TestRuntimeGlobalLimitsShrinkGrow(t *testing.T) {
	cfg := Config{
		MaxConcurrency: 4, ResourceLimit: 40, QueueCapacity: 128,
		Groups: []GroupConfig{
			{Name: "A", MaxConcurrency: 0, ResourceLimit: -1},
			{Name: "B", MaxConcurrency: 0, ResourceLimit: -1},
		},
	}
	s := newTestScheduler(t, cfg)
	ctx := context.Background()

	watchDone := make(chan struct{})
	var watchWG sync.WaitGroup
	watchWG.Add(1)
	go func() {
		defer watchWG.Done()
		for {
			select {
			case <-watchDone:
				return
			default:
			}
			snap := s.Snapshot()
			// 缩容允许在跑任务暂时超过新上限（不抢占）；真正的硬保证是
			// "准入那一刻 post 值 <= 当时上限"。因此这里断言任何时刻都不
			// 超过本测试曾配置过的准入峰值，且账目永不为负。
			if snap.ResourceUsed > 40 {
				t.Errorf("global resource %d exceeded max-ever-admit ceiling 40", snap.ResourceUsed)
				return
			}
			if snap.InFlight > 4 {
				t.Errorf("global inflight %d exceeded max-ever concurrency 4", snap.InFlight)
				return
			}
			if snap.ResourceUsed < 0 || snap.InFlight < 0 {
				t.Errorf("negative accounting: %s", snap)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// 先用满旧容量：4 个 cost=10 的任务占住 40。
	block := make(chan struct{})
	started := make(chan struct{})
	var startMu sync.Mutex
	running := 0
	holder := func(id, g string) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			startMu.Lock()
			running++
			if running == 4 {
				select {
				case <-started:
				default:
					close(started)
				}
			}
			startMu.Unlock()
			// 同时响应 block 与取消：缩容排空阶段需要能通过 Cancel 退出，
			// 以验证"释放后不超过新上限才准入"；不取消时由 block 正常放行。
			select {
			case <-block:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	var holders []*Handle
	for i, g := range []string{"A", "B", "A", "B"} {
		h, err := s.Submit(ctx, gtask(fmt.Sprintf("h%d", i), g, 10, PriorityNormal, holder(fmt.Sprintf("h%d", i), g)))
		if err != nil {
			t.Fatal(err)
		}
		holders = append(holders, h)
	}
	<-started

	// 运行期缩容到 2 并发 / 20 资源：4 个在跑任务不被抢占（used=40 > 20）。
	if err := s.SetGlobalLimits(2, 20); err != nil {
		t.Fatal(err)
	}
	snap := s.Snapshot()
	if snap.InFlight != 4 || snap.ResourceUsed != 40 {
		t.Fatalf("shrink must not preempt running tasks: %s", snap)
	}

	// 有等待者时，新任务一律入队；used(40) > 新上限(20) 期间绝无准入可能。
	queuedDone := make(chan struct{})
	hQ, err := s.Submit(ctx, gtask("q", "A", 1, PriorityNormal, func(ctx context.Context) error {
		close(queuedDone)
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	select {
	case <-queuedDone:
		t.Fatal("queued task must not run while used exceeds shrunken limit")
	default:
	}

	// 释放两个 holder：used 降到 20，恰好等于新上限；cost=1 的等待者仍需
	// 再等一个槽（20+1>20）。再释放一个（used=10）后它必须被准入。
	for i := 0; i < 2; i++ {
		holders[i].Cancel()
	}
	deadline := time.Now().Add(2 * time.Second)
	for s.Snapshot().ResourceUsed != 20 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	select {
	case <-queuedDone:
		t.Fatal("task must not be admitted when used is exactly at the shrunken limit")
	default:
	}
	holders[2].Cancel()
	waitHandle(t, hQ, 2*time.Second)
	if hQ.Status() != StatusSucceeded {
		t.Fatalf("queued task should run once capacity drains below new limit, got %s", hQ.Status())
	}

	// 扩容回 4/40，剩余 holder 与队列正常排空。
	if err := s.SetGlobalLimits(4, 40); err != nil {
		t.Fatal(err)
	}
	holders[3].Cancel()
	close(block)
	for _, hh := range holders {
		waitHandle(t, hh, 2*time.Second)
	}

	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	close(watchDone)
	doneCh := make(chan struct{})
	go func() { watchWG.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(time.Second):
	}
	if sp := s.Snapshot(); sp.InFlight != 0 || sp.ResourceUsed != 0 || sp.Queued != 0 {
		t.Fatalf("accounts not clean after resize test: %s", sp)
	}
}

// assignGroups 按任务序号轮转分配业务组（确定性，只依赖种子生成的序号），
// 使可复现性覆盖到分组层。
func assignGroups(jobs []SimJob, groups []string) []SimJob {
	for i := range jobs {
		jobs[i].Group = groups[i%len(groups)]
	}
	return jobs
}

// runDeterministicGrouped 在冻结态下跑一批分组任务，返回准入 ID 序列、
// "组/ID" 序列与终态表。
func runDeterministicGrouped(t *testing.T, jobs []SimJob, cfg Config) ([]string, []string, map[string]Status) {
	t.Helper()
	s := New(cfg)
	r := NewDeterministicRunner(s)
	r.freeze()
	if errs := r.SubmitAll(jobs); len(errs) != 0 {
		t.Fatalf("unexpected rejections in deterministic grouped run: %v", errs)
	}
	if !r.Run(100000) {
		t.Fatal("deterministic grouped run failed to drain")
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r.AdmitOrder(), r.AdmitGroupSequence(), r.TerminalStatuses()
}

// TestReproducibleSchedulingWithGroups：相同输入 + 相同种子跑两遍，
// 准入的"组/任务"序列与每个任务的终态必须逐位一致（组间轮转的确定性）。
func TestReproducibleSchedulingWithGroups(t *testing.T) {
	specs := assignGroups(
		jobsFromSpecs(GenerateWorkload(20261001, 90)),
		[]string{"A", "B", "C"},
	)
	cfg := Config{
		// 无界队列；全局资源=20 保证任何单项任务最终都装得下；
		// 各组资源上限 >= 单项最大 cost(20) 以免硬上限立即拒绝，
		// 用较窄的"组并发上限"制造组间轮转与组内排队压力。
		MaxConcurrency: 3, ResourceLimit: 20, QueueCapacity: 0,
		Groups: []GroupConfig{
			{Name: "A", MaxConcurrency: 1, ResourceLimit: 20},
			{Name: "B", MaxConcurrency: 2, ResourceLimit: 20},
			{Name: "C", MaxConcurrency: 1, ResourceLimit: 20},
		},
	}

	ids1, seq1, term1 := runDeterministicGrouped(t, specs, cfg)
	ids2, seq2, term2 := runDeterministicGrouped(t, specs, cfg)

	if len(seq1) != len(specs) || len(seq2) != len(specs) {
		t.Fatalf("admit count mismatch: %d %d want %d", len(seq1), len(seq2), len(specs))
	}
	for i := range seq1 {
		if seq1[i] != seq2[i] {
			t.Fatalf("grouped admit sequence diverges at %d:\nrun1=%v\nrun2=%v", i, seq1, seq2)
		}
		if ids1[i] != ids2[i] {
			t.Fatalf("admit id sequence diverges at %d", i)
		}
	}
	for id, st := range term1 {
		if term2[id] != st {
			t.Fatalf("terminal status for %s diverges: %s vs %s", id, st, term2[id])
		}
	}

	// 额外确认三个组确实都参与了准入（否则分组轮转没有被真正覆盖）。
	seen := map[string]bool{}
	for _, x := range seq1 {
		seen[x[:1]] = true // "A/id" 的首字符即组名
	}
	for _, g := range []string{"A", "B", "C"} {
		if !seen[g] {
			t.Fatalf("group %s never admitted; fairness not exercised", g)
		}
	}
}

// TestNoResourceGrowthOverLifetime：大量分组任务长期运行后，账目归零、
// 队列清空，堆与 goroutine 数不应随"已完成任务数"持续增长——
// 重点是排队取消监听协程必须在任务准入后退出，而不是随完成数无限堆积。
//
// 本用例刻意不挂 testLogger（t.Logf 会把全部决策日志留在 testing 缓冲里，
// 那属于测试设施而非调度器占用），使用内部空日志器，只度量调度器自身。
func TestNoResourceGrowthOverLifetime(t *testing.T) {
	cfg := Config{
		MaxConcurrency: 4, ResourceLimit: 16, QueueCapacity: 0,
		Groups: []GroupConfig{
			{Name: "A", MaxConcurrency: 2, ResourceLimit: 8},
			{Name: "B", MaxConcurrency: 2, ResourceLimit: 8},
		},
	}
	s := New(cfg) // 空日志器：排除测试日志缓冲对堆度量的干扰
	ctx := context.Background()

	runBatch := func(n int) {
		var wg sync.WaitGroup
		wg.Add(n)
		for i := 0; i < n; i++ {
			i := i
			g := []string{"A", "B"}[i%2]
			go func() {
				defer wg.Done()
				cost := int64(i%4 + 1)
				h, err := s.Submit(ctx, gtask(fmt.Sprintf("life-%d", i), g, cost, Priority(i%11),
					sleepTask(time.Duration(i%3)*time.Millisecond)))
				if err != nil {
					t.Errorf("submit life-%d: %v", i, err)
					return
				}
				<-h.Done()
			}()
		}
		wg.Wait()
	}

	collect := func() uint64 {
		runtime.GC()
		runtime.GC() // 两次 GC 降低代际/延迟回收带来的抖动
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}

	const batch = 250
	runBatch(batch) // 预热：建立稳态工作集
	warmHeap := collect()
	warmG := runtime.NumGoroutine()

	// 再跑若干批，记录每批后的稳态堆。若按完成任务数线性泄漏，这里会
	// 随批次持续上行；正确实现应在 warmHeap 附近窄幅波动。
	var heaps []uint64
	for b := 0; b < 4; b++ {
		runBatch(batch)
		if snap := s.Snapshot(); snap.InFlight != 0 || snap.ResourceUsed != 0 || snap.Queued != 0 {
			t.Fatalf("after batch accounts not clean: %s", snap)
		}
		heaps = append(heaps, collect())
	}
	endG := runtime.NumGoroutine()

	// goroutine 不随完成数增长（允许测试自身协程 ±2 的噪声）。
	if endG > warmG+2 {
		t.Errorf("goroutines grew with completed tasks: warm=%d end=%d", warmG, endG)
	}
	// 末批稳态堆相对预热基线：允许固定容差，禁止随批次数十 KB/任务地堆积。
	endHeap := heaps[len(heaps)-1]
	const allowance = uint64(256 * 1024)
	if endHeap > warmHeap+allowance {
		t.Errorf("heap kept growing with completed tasks: warm=%dKB heaps(KB)=%v end=%dKB",
			warmHeap/1024, kbSlice(heaps), endHeap/1024)
	}
	t.Logf("resource lifetime: goroutines warm=%d end=%d; heap warm=%dKB per-batch=%vKB",
		warmG, endG, warmHeap/1024, kbSlice(heaps))

	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	// 关闭后派发协程应退出，goroutine 数回落到预热基线以下。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n := runtime.NumGoroutine(); n <= warmG {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("goroutines did not settle after shutdown: warm=%d now=%d", warmG, runtime.NumGoroutine())
}

func kbSlice(hs []uint64) []uint64 {
	out := make([]uint64, len(hs))
	for i, h := range hs {
		out[i] = h / 1024
	}
	return out
}
