package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestPriorityOrdering：高优先级任务先于低优先级获得资源。
// 两个任务在资源被占满时排队；资源一释放，应按 HIGH > NORMAL > LOW 准入。
func TestPriorityOrdering(t *testing.T) {
	s := newTestScheduler(t, Config{MaxConcurrency: 1, ResourceLimit: 10})
	ctx := context.Background()
	block := make(chan struct{})
	holderStarted := make(chan struct{})

	_, err := s.Submit(ctx, task("holder", 1, PriorityNormal, func(ctx context.Context) error {
		close(holderStarted)
		<-block
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	<-holderStarted

	var mu sync.Mutex
	var order []string
	allRecorded := make(chan struct{})
	record := func(id string) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			mu.Lock()
			order = append(order, id)
			if len(order) == 4 {
				select {
				case <-allRecorded:
				default:
					close(allRecorded)
				}
			}
			mu.Unlock()
			return nil
		}
	}
	// 全部在 holder 占用唯一槽位时入队；holder 释放后由派发者依次准入。
	for _, p := range []Priority{PriorityLow, PriorityNormal, PriorityHigh, PriorityLow} {
		id := fmt.Sprintf("p%d", p)
		if _, err := s.Submit(ctx, task(id, 1, p, record(id))); err != nil {
			t.Fatal(err)
		}
	}

	close(block) // 放行 holder，派发者随后按优先级准入
	select {
	case <-allRecorded:
	case <-time.After(2 * time.Second):
		t.Fatalf("queued tasks not all admitted, order=%v", order)
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	// holder 之后，期望准入顺序：HIGH 先，然后 NORMAL，然后两个 LOW 按 FIFO。
	want := []string{"p10", "p5", "p0", "p0"}
	if len(order) != len(want) {
		t.Fatalf("admission order len=%d want=%d (%v)", len(order), len(want), order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("admission order=%v want=%v", order, want)
		}
	}
}

// TestNoStarvationWithAging：持续涌入的高优先级任务不能让低优先级任务无限等待。
// 队列容量有界，我们循环制造"高优先级占满 + 一个低优先级等待 + 高频高优新任务"，
// 老化机制会让该低优先级任务在有限 tick 后被准入。
func TestNoStarvationWithAging(t *testing.T) {
	s := newTestScheduler(t, Config{MaxConcurrency: 1, ResourceLimit: 10, QueueCapacity: 8})
	ctx := context.Background()
	block := make(chan struct{})
	started := make(chan struct{})

	_, _ = s.Submit(ctx, task("holder", 1, PriorityHigh, func(ctx context.Context) error {
		close(started)
		<-block
		return nil
	}))
	<-started

	lowDone := make(chan struct{})
	if _, err := s.Submit(ctx, task("low-starve", 1, PriorityLow, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		close(lowDone)
		return nil
	})); err != nil {
		t.Fatal(err)
	}

	// 在 holder 释放前后，持续提交短高优任务，制造"永远有高优在排队"的压力。
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
			id := fmt.Sprintf("hi%d", i)
			i++
			h, err := s.Submit(ctx, task(id, 1, PriorityHigh, sleepTask(time.Millisecond)))
			if err != nil {
				// 队列满时退避重试（高优任务也可能被 QUEUE_FULL 拒绝，符合设计）。
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
		t.Fatal("low priority task starved: never admitted under high priority pressure")
	}
	close(stopPressure)
	pressureWG.Wait()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
}
