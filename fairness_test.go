package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestInterGroupFairnessRoundRobin：全局只有 1 个并发槽。A 组持续堆积
// HIGH 任务、B 组只有 LOW 任务。若只看全局优先级，B 会被永远饿死；
// 组间轮转必须保证 B 的任务也能拿到准入机会，且准入机会在两组间交替。
func TestInterGroupFairnessRoundRobin(t *testing.T) {
	s := New(Config{MaxConcurrency: 1, ResourceLimit: 100},
		GroupConfig{Name: "A", MaxConcurrency: 10, ResourceLimit: 100},
		GroupConfig{Name: "B", MaxConcurrency: 10, ResourceLimit: 100},
	).WithLogger(testLogger{t})
	ctx := context.Background()

	block := make(chan struct{})
	holderStarted := make(chan struct{})
	_, err := s.Submit(ctx, &Task{ID: "holder", Group: "A", Cost: 1, Fn: func(ctx context.Context) error {
		close(holderStarted)
		<-block
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-holderStarted

	var mu sync.Mutex
	var order []string
	const n = 6
	allDone := make(chan struct{})
	record := func(id, g string) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			mu.Lock()
			order = append(order, g)
			done := len(order) == n
			mu.Unlock()
			if done {
				close(allDone)
			}
			return nil
		}
	}

	// 先入队 A 的 3 个 HIGH，再入队 B 的 3 个 LOW（都在唯一槽被占时排队）。
	for i := 0; i < 3; i++ {
		mustGroupTask(t, s, fmt.Sprintf("a-hi-%d", i), "A", 1, PriorityHigh,
			record(fmt.Sprintf("a-hi-%d", i), "A"))
	}
	for i := 0; i < 3; i++ {
		mustGroupTask(t, s, fmt.Sprintf("b-lo-%d", i), "B", 1, PriorityLow,
			record(fmt.Sprintf("b-lo-%d", i), "B"))
	}

	close(block)
	select {
	case <-allDone:
	case <-time.After(3 * time.Second):
		t.Fatalf("inter-group starvation: B never admitted, order=%v", order)
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	// 轮转保证两组交替获得机会（具体起点取决于 cursor 推进，但不能让
	// 任意一组连续占用全部 6 次中的超过一半）。
	countA, countB, maxRunA, maxRunB := 0, 0, 0, 0
	run := 0
	for i, g := range order {
		if g == "A" {
			countA++
		} else {
			countB++
		}
		if i == 0 || order[i-1] != g {
			run = 1
		} else {
			run++
		}
		if g == "A" && run > maxRunA {
			maxRunA = run
		}
		if g == "B" && run > maxRunB {
			maxRunB = run
		}
	}
	if countA != 3 || countB != 3 {
		t.Fatalf("each group must get 3 admissions, got A=%d B=%d order=%v", countA, countB, order)
	}
	if maxRunA > 1 || maxRunB > 1 {
		t.Fatalf("groups must strictly alternate admission turns, order=%v maxRunA=%d maxRunB=%d",
			order, maxRunA, maxRunB)
	}
	t.Logf("inter-group admission order: %v", order)
}

// TestIntraGroupPriorityStillHolds：同一组内部依旧高优先级先拿资源。
func TestIntraGroupPriorityStillHolds(t *testing.T) {
	s := New(Config{MaxConcurrency: 1, ResourceLimit: 100},
		GroupConfig{Name: "A", MaxConcurrency: 10, ResourceLimit: 100},
	).WithLogger(testLogger{t})
	ctx := context.Background()
	block := make(chan struct{})
	started := make(chan struct{})
	_, _ = s.Submit(ctx, &Task{ID: "holder", Group: "A", Cost: 1, Fn: func(ctx context.Context) error {
		close(started)
		<-block
		return nil
	}})
	<-started

	var mu sync.Mutex
	var order []string
	doneAll := make(chan struct{})
	rec := func(id string) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			mu.Lock()
			order = append(order, id)
			n := len(order)
			mu.Unlock()
			if n == 3 {
				close(doneAll)
			}
			return nil
		}
	}
	// 同组：LOW、HIGH、NORMAL 依次排队，holder 释放后期望 HIGH>NORMAL>LOW。
	mustGroupTask(t, s, "lo", "A", 1, PriorityLow, rec("lo"))
	mustGroupTask(t, s, "hi", "A", 1, PriorityHigh, rec("hi"))
	mustGroupTask(t, s, "nm", "A", 1, PriorityNormal, rec("nm"))

	close(block)
	select {
	case <-doneAll:
	case <-time.After(2 * time.Second):
		t.Fatalf("intra-group tasks not drained: %v", order)
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	want := []string{"hi", "nm", "lo"}
	if len(order) != 3 || order[0] != want[0] || order[1] != want[1] || order[2] != want[2] {
		t.Fatalf("intra-group priority order=%v want=%v", order, want)
	}
}

// TestIntraGroupAgingPreventsStarvation：同一组内持续涌入的高优任务不能
// 让组内低优任务无限等待（沿用单池老化语义）。
func TestIntraGroupAgingPreventsStarvation(t *testing.T) {
	s := New(Config{MaxConcurrency: 1, ResourceLimit: 100, QueueCapacity: 64},
		GroupConfig{Name: "A", MaxConcurrency: 10, ResourceLimit: 100},
	).WithLogger(testLogger{t})
	ctx := context.Background()
	block := make(chan struct{})
	started := make(chan struct{})
	_, _ = s.Submit(ctx, &Task{ID: "holder", Group: "A", Cost: 1, Priority: PriorityHigh, Fn: func(ctx context.Context) error {
		close(started)
		<-block
		return nil
	}})
	<-started

	lowDone := make(chan struct{})
	mustGroupTask(t, s, "low-starve", "A", 1, PriorityLow, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		close(lowDone)
		return nil
	})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			h, err := s.Submit(ctx, &Task{
				ID: fmt.Sprintf("hi%d", i), Group: "A", Cost: 1, Priority: PriorityHigh,
				Fn: sleepTask(time.Millisecond),
			})
			i++
			if err != nil {
				time.Sleep(time.Millisecond)
				continue
			}
			<-h.Done()
		}
	}()

	close(block)
	select {
	case <-lowDone:
	case <-time.After(3 * time.Second):
		t.Fatal("intra-group low priority task starved under high priority pressure")
	}
	close(stop)
	wg.Wait()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
