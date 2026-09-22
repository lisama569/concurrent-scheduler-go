package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestScheduler(t *testing.T, limits Limits, tick time.Duration) (*Scheduler, *SliceLogger) {
	t.Helper()
	log := NewSliceLogger()
	s := New(Config{
		Limits:        limits,
		Tick:          tick,
		RunGrace:      20 * time.Millisecond,
		StarvationAge: 16,
		Logger:        log,
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})
	return s, log
}

// TestTaskTooLargeRejected: 超过总容量的任务按 task_too_large 拒绝，不排队。
func TestTaskTooLargeRejected(t *testing.T) {
	s, log := newTestScheduler(t, Limits{MaxCPU: 4, MaxMemory: 4, MaxIO: 4, MaxTasks: 4}, time.Millisecond)
	h, err := s.Submit(TaskSpec{
		Name: "big", Cost: Resources{CPU: 5},
		Fn: func(ctx context.Context) error { return nil },
	})
	if !errors.Is(err, ErrTaskTooLarge) {
		t.Fatalf("want ErrTaskTooLarge, got %v", err)
	}
	var re *RejectError
	if !errors.As(err, &re) || re.Reason != ReasonTaskTooLarge {
		t.Fatalf("want reject reason task_too_large, got %v", err)
	}
	res, _ := h.Wait(context.Background())
	if res.Status != StatusRejected {
		t.Fatalf("want StatusRejected, got %s", res.Status)
	}
	if s.Snapshot().Queued != 0 {
		t.Fatalf("oversized task must not be queued")
	}
	printEvents(t, log)
}

// TestInvalidSpec: 非法规格按 invalid_spec 拒绝。
func TestInvalidSpec(t *testing.T) {
	s, log := newTestScheduler(t, Limits{MaxCPU: 4, MaxTasks: 4}, time.Millisecond)
	if _, err := s.Submit(TaskSpec{Name: "nilfn"}); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("want ErrInvalidSpec for nil Fn, got %v", err)
	}
	if _, err := s.Submit(TaskSpec{
		Name: "neg", Cost: Resources{CPU: -1},
		Fn: func(ctx context.Context) error { return nil },
	}); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("want ErrInvalidSpec for negative cost, got %v", err)
	}
	if _, err := s.Submit(TaskSpec{
		Name: "prio", Priority: 99,
		Fn: func(ctx context.Context) error { return nil },
	}); !errors.Is(err, ErrInvalidSpec) {
		t.Fatalf("want ErrInvalidSpec for bad priority, got %v", err)
	}
	printEvents(t, log)
}

// TestConcurrencyAndResourceCaps: 超额任务排队等待而非丢弃；
// 并发度与资源账目不被突破；任务结束后排队任务自动补入。
func TestConcurrencyAndResourceCaps(t *testing.T) {
	const maxTasks = 2
	s, log := newTestScheduler(t, Limits{MaxCPU: 20, MaxMemory: 10, MaxIO: 10, MaxTasks: maxTasks}, time.Millisecond)

	var running atomic.Int32
	var peakRunning atomic.Int32
	var peakCPU atomic.Int32
	gate := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 6; i++ {
		wg.Add(1)
		i := i
		h, err := s.Submit(TaskSpec{
			Name: fmt.Sprintf("w%d", i), Priority: i % 3,
			Cost: Resources{CPU: 6}, // 容量 20、并发度 2：两约束共同生效
			Fn: func(ctx context.Context) error {
				defer wg.Done()
				cur := running.Add(1)
				for {
					p := peakRunning.Load()
					if cur <= p || peakRunning.CompareAndSwap(p, cur) {
						break
					}
				}
				snap := s.Snapshot()
				for {
					p := peakCPU.Load()
					c := int32(snap.InUse.CPU)
					if c <= p || peakCPU.CompareAndSwap(p, c) {
						break
					}
				}
				<-gate
				running.Add(-1)
				return nil
			},
		})
		if err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
		if h == nil {
			t.Fatal("handle is nil")
		}
	}

	// 等待两个任务进入运行（并发上限 2）。
	if !waitUntil(t, func() bool { return s.Snapshot().Running == maxTasks }) {
		t.Fatalf("want running=2, snap=%+v", s.Snapshot())
	}
	snap := s.Snapshot()
	if snap.Queued != 4 || snap.InUse.CPU > 20 {
		t.Fatalf("want queued=4 and cpu<=20, snap=%+v", snap)
	}

	// 逐个放行，所有任务最终成功且资源归零。
	for i := 0; i < 6; i++ {
		gate <- struct{}{}
	}
	wg.Wait()

	// 等待账目结算（finishTask 在任务 goroutine 内）。
	waitZero(t, s)
	if peakRunning.Load() > maxTasks {
		t.Fatalf("concurrency cap violated: peak=%d", peakRunning.Load())
	}
	if peakCPU.Load() > 20 {
		t.Fatalf("cpu cap violated: peak cpu=%d", peakCPU.Load())
	}
	assertReleased(t, s)
	printEvents(t, log)
}

func waitZero(t *testing.T, s *Scheduler) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap := s.Snapshot()
		if snap.Running == 0 && snap.Queued == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("scheduler did not drain: %+v", s.Snapshot())
}

// TestPriorityOrderNoStarvation: 高优先级任务先获得资源；
// 在高优先级任务流持续到达的压力下，低优先级任务靠累积年龄被提升，
// 仍能在有界等待后被准入（不饥饿）。
func TestPriorityOrderNoStarvation(t *testing.T) {
	// tick=2ms、starvationAge=10：低优先级任务等待约 20ms 后被提升。
	log := NewSliceLogger()
	s := New(Config{
		Limits:        Limits{MaxCPU: 10, MaxTasks: 1},
		Tick:          2 * time.Millisecond,
		RunGrace:      20 * time.Millisecond,
		StarvationAge: 10,
		Logger:        log,
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})

	var mu sync.Mutex
	order := []string{}
	gates := map[string]chan struct{}{}
	started := make(chan string, 16)

	submit := func(name string, p int) {
		gate := make(chan struct{}, 1)
		gates[name] = gate
		_, err := s.Submit(TaskSpec{
			Name: name, Priority: p, Cost: Resources{CPU: 10},
			Fn: func(ctx context.Context) error {
				mu.Lock()
				order = append(order, name)
				mu.Unlock()
				started <- name
				<-gate
				return nil
			},
		})
		if err != nil {
			t.Fatalf("submit %s: %v", name, err)
		}
	}

	// 1) 高优先级 h0 独占槽位并运行约 30ms；low 在此期间排队、累计年龄。
	submit("h0", 10)
	waitChan(t, started, "h0")
	submit("low", 0)
	time.Sleep(30 * time.Millisecond) // > 10*tick=20ms，low 已满足提升条件

	// 2) 放行 h0：期间再到达一个高优先级 h1，但 low 已被提升，应先于 h1。
	submit("h1", 10)
	gates["h0"] <- struct{}{}
	got := waitChan(t, started, "low", "h1")
	if got != "low" {
		t.Fatalf("aging failed: expected boosted low before fresh h1, got %s, order=%v", got, order)
	}
	gates["low"] <- struct{}{}
	waitChan(t, started, "h1")
	gates["h1"] <- struct{}{}

	// 3) 新的一轮：验证无提升时高优先级仍然优先。
	submit("x0", 10)
	waitChan(t, started, "x0")
	submit("lo2", 0)
	submit("hi2", 9)
	gates["x0"] <- struct{}{}
	if got := waitChan(t, started, "hi2", "lo2"); got != "hi2" {
		t.Fatalf("priority failed: fresh high should go first, order=%v", order)
	}
	gates["hi2"] <- struct{}{}
	waitChan(t, started, "lo2")
	gates["lo2"] <- struct{}{}

	waitZero(t, s)

	mu.Lock()
	defer mu.Unlock()
	wantOrder := []string{"h0", "low", "h1", "x0", "hi2", "lo2"}
	for i, w := range wantOrder {
		if order[i] != w {
			t.Fatalf("position %d: want %s, got order=%v", i, w, order)
		}
	}
	foundBoost := false
	for _, e := range log.Events() {
		if e.Type == EventQueue && strings.Contains(e.Decision, "boosted") {
			foundBoost = true
		}
	}
	if !foundBoost {
		t.Fatal("expected an aging boost decision in the log")
	}
	printEvents(t, log)
}

// waitChan 等待 started 通道返回一个名字；若名字不在 want 中则失败。
func waitChan(t *testing.T, ch <-chan string, want ...string) string {
	t.Helper()
	select {
	case got := <-ch:
		for _, w := range want {
			if got == w {
				return got
			}
		}
		t.Fatalf("got %s, want one of %v", got, want)
		return ""
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for one of %v", want)
		return ""
	}
}

func waitUntil(t *testing.T, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}
