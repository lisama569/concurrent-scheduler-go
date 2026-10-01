package scheduler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// 分组测试公共助手：创建带两个业务组 A/B 的调度器。
func newGroupedScheduler(t *testing.T, cfg Config, ga, gb GroupConfig) *Scheduler {
	t.Helper()
	return New(cfg, ga, gb).WithLogger(testLogger{t})
}

// TestGroupQuotaIsolation：一个组用满自己的预算后，不能动用另一个组的
// 预算；其他组在全局容量允许时照常准入。
func TestGroupQuotaIsolation(t *testing.T) {
	s := newGroupedScheduler(t,
		Config{MaxConcurrency: 10, ResourceLimit: 100}, // 全局很宽松
		GroupConfig{Name: "A", MaxConcurrency: 1, ResourceLimit: 10},
		GroupConfig{Name: "B", MaxConcurrency: 1, ResourceLimit: 10},
	)
	ctx := context.Background()
	block := make(chan struct{})
	startedA := make(chan struct{})

	// A 组任务占住 A 的唯一并发槽（cost=10 也占满 A 的资源）。
	_, err := s.Submit(ctx, &Task{ID: "a1", Group: "A", Cost: 10, Fn: func(ctx context.Context) error {
		close(startedA)
		<-block
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-startedA

	// A 组第二个任务：组预算不足，应排队（全局其实还很空）。
	hA2 := mustGroupTask(t, s, "a2", "A", 1, PriorityNormal, noop())
	if br := hA2.BlockReason(); br != BlockGroupConcurrency && br != BlockGroupResource {
		t.Fatalf("A queued task should be blocked by group budget, got %s", br)
	}
	time.Sleep(20 * time.Millisecond)
	if hA2.Status() != StatusQueued {
		t.Fatalf("A task must remain queued while A budget exhausted, got %s", hA2.Status())
	}

	// B 组任务：B 自己有钱、全局也有钱，应立即准入，绝不受 A 影响。
	bDone := make(chan struct{})
	hB, err := s.Submit(ctx, &Task{ID: "b1", Group: "B", Cost: 5, Fn: func(ctx context.Context) error {
		close(bDone)
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-bDone:
	case <-time.After(time.Second):
		t.Fatal("group B task was starved by group A budget exhaustion")
	}
	waitHandle(t, hB, time.Second) // 等结算完成再断言终态
	if st := hB.Status(); st != StatusSucceeded {
		t.Fatalf("B task want SUCCEEDED, got %s", st)
	}

	snap := s.Snapshot()
	if snap.Groups["A"].InFlight != 1 || snap.Groups["A"].ResourceUsed != 10 {
		t.Fatalf("group A accounts wrong: %+v", snap.Groups["A"])
	}
	if snap.Groups["B"].InFlight != 0 || snap.Groups["B"].ResourceUsed != 0 {
		t.Fatalf("group B should be settled clean: %+v", snap.Groups["B"])
	}

	close(block)
	waitHandle(t, hA2, 2*time.Second)
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func mustGroupTask(t *testing.T, s *Scheduler, id, group string, cost int64, p Priority,
	fn func(ctx context.Context) error) *Handle {
	t.Helper()
	h, err := s.Submit(context.Background(), &Task{ID: id, Group: group, Cost: cost, Priority: p, Fn: fn})
	if err != nil {
		t.Fatalf("submit %s: %v", id, err)
	}
	return h
}

// TestBlockReasonsDistinguished：排队阻塞维度必须能区分本组并发、本组
// 资源、全局并发、全局资源四种原因。
func TestBlockReasonsDistinguished(t *testing.T) {
	// 场景 1：本组并发不足（组资源、全局都充足）。
	s1 := newGroupedScheduler(t,
		Config{MaxConcurrency: 8, ResourceLimit: 100},
		GroupConfig{Name: "A", MaxConcurrency: 1, ResourceLimit: 100},
		GroupConfig{Name: "B", MaxConcurrency: 8, ResourceLimit: 100},
	)
	holdAndBlock(t, s1, "A", 1)
	h, err := s1.Submit(context.Background(), &Task{ID: "a", Group: "A", Cost: 1, Fn: noop()})
	if err != nil {
		t.Fatal(err)
	}
	if h.BlockReason() != BlockGroupConcurrency {
		t.Fatalf("want GROUP_CONCURRENCY, got %s", h.BlockReason())
	}
	shutdownOK(t, s1)

	// 场景 2：本组资源不足（并发槽还空着）。
	s2 := newGroupedScheduler(t,
		Config{MaxConcurrency: 8, ResourceLimit: 100},
		GroupConfig{Name: "A", MaxConcurrency: 8, ResourceLimit: 10},
		GroupConfig{Name: "B", MaxConcurrency: 8, ResourceLimit: 100},
	)
	holdAndBlock(t, s2, "A", 8)
	h2, err := s2.Submit(context.Background(), &Task{ID: "a", Group: "A", Cost: 4, Fn: noop()})
	if err != nil {
		t.Fatal(err)
	}
	if h2.BlockReason() != BlockGroupResource {
		t.Fatalf("want GROUP_RESOURCE, got %s", h2.BlockReason())
	}
	shutdownOK(t, s2)

	// 场景 3：全局并发不足（各组自身并发都充足）。
	s3 := newGroupedScheduler(t,
		Config{MaxConcurrency: 1, ResourceLimit: 100},
		GroupConfig{Name: "A", MaxConcurrency: 8, ResourceLimit: 100},
		GroupConfig{Name: "B", MaxConcurrency: 8, ResourceLimit: 100},
	)
	holdAndBlock(t, s3, "A", 1)
	h3, err := s3.Submit(context.Background(), &Task{ID: "b", Group: "B", Cost: 1, Fn: noop()})
	if err != nil {
		t.Fatal(err)
	}
	if h3.BlockReason() != BlockGlobalConcurrency {
		t.Fatalf("want GLOBAL_CONCURRENCY, got %s", h3.BlockReason())
	}
	shutdownOK(t, s3)

	// 场景 4：全局资源不足（全局并发槽还空着）。
	s4 := newGroupedScheduler(t,
		Config{MaxConcurrency: 8, ResourceLimit: 10},
		GroupConfig{Name: "A", MaxConcurrency: 8, ResourceLimit: 100},
		GroupConfig{Name: "B", MaxConcurrency: 8, ResourceLimit: 100},
	)
	holdAndBlock(t, s4, "A", 10)
	h4, err := s4.Submit(context.Background(), &Task{ID: "b", Group: "B", Cost: 1, Fn: noop()})
	if err != nil {
		t.Fatal(err)
	}
	if h4.BlockReason() != BlockGlobalResource {
		t.Fatalf("want GLOBAL_RESOURCE, got %s", h4.BlockReason())
	}
	shutdownOK(t, s4)
}

// holdAndBlock 提交一个永久阻塞（直到测试关闭）的占位任务占住资源。
func holdAndBlock(t *testing.T, s *Scheduler, group string, cost int64) chan struct{} {
	t.Helper()
	block := make(chan struct{})
	started := make(chan struct{})
	h, err := s.Submit(context.Background(), &Task{
		ID: fmt.Sprintf("hold-%s-%d", group, cost), Group: group, Cost: cost,
		Fn: func(ctx context.Context) error {
			close(started)
			select {
			case <-block:
			case <-ctx.Done():
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-block:
		default:
			close(block)
		}
		<-h.Done()
	})
	<-started
	return block
}

func shutdownOK(t *testing.T, s *Scheduler) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// TestRejectUnknownAndClosedGroup：未注册组按 UNKNOWN_GROUP 拒绝；
// CloseGroup 后该组按 GROUP_STOPPED 拒绝，且不影响其他组。
func TestRejectUnknownAndClosedGroup(t *testing.T) {
	s := New(Config{MaxConcurrency: 4, ResourceLimit: 1000},
		GroupConfig{Name: "A", MaxConcurrency: 4, ResourceLimit: 100}).WithLogger(testLogger{t})

	_, err := s.Submit(context.Background(), &Task{ID: "x", Group: "NOPE", Cost: 1, Fn: noop()})
	var re *RejectError
	if !errors.As(err, &re) || re.Reason != ReasonUnknownGroup {
		t.Fatalf("want UNKNOWN_GROUP, got %v", err)
	}
	// 超过本组硬上限（但低于全局上限）：必须按组硬上限拒绝而非排队。
	_, err = s.Submit(context.Background(), &Task{ID: "big", Group: "A", Cost: 101, Fn: noop()})
	if !errors.As(err, &re) || re.Reason != ReasonGroupExceedsHardLimit {
		t.Fatalf("want GROUP_EXCEEDS_HARD_LIMIT, got %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.CloseGroup("A", shutdownCtx); err != nil {
		t.Fatal(err)
	}
	_, err = s.Submit(context.Background(), &Task{ID: "late", Group: "A", Cost: 1, Fn: noop()})
	if !errors.As(err, &re) || re.Reason != ReasonGroupStopped {
		t.Fatalf("want GROUP_STOPPED, got %v", err)
	}

	// 默认组未受影响，仍可服务。
	h, err := s.Submit(context.Background(), task("default-ok", 1, PriorityNormal, noop()))
	if err != nil {
		t.Fatalf("default group must remain usable after A closed: %v", err)
	}
	waitHandle(t, h, time.Second)
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestDefaultGroupIsUnlimited：不声明 Group 的任务只受全局上限约束，
// 与单池版本完全兼容（本组维度永不拦截）。
func TestDefaultGroupIsUnlimited(t *testing.T) {
	s := New(Config{MaxConcurrency: 2, ResourceLimit: 10}).WithLogger(testLogger{t})
	block := make(chan struct{})
	var handles []*Handle
	for i := 0; i < 2; i++ {
		h, err := s.Submit(context.Background(), task(fmt.Sprintf("d%d", i), 5, PriorityNormal,
			func(ctx context.Context) error {
				<-block
				return nil
			}))
		if err != nil {
			t.Fatal(err)
		}
		handles = append(handles, h)
	}
	// 直接等待两个任务都在运行（达到全局并发上限 2）。
	deadline := time.Now().Add(time.Second)
	for s.Snapshot().InFlight != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Snapshot().InFlight != 2 || s.Snapshot().ResourceUsed != 10 {
		t.Fatalf("default group tasks should run up to global limit, got %s", s.Snapshot())
	}
	close(block)
	for _, h := range handles {
		waitHandle(t, h, time.Second)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
