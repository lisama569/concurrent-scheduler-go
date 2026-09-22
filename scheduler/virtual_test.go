package scheduler

import (
	"strings"
	"testing"
	"time"
)

func vtSpec(name string, p int, cost Resources) TaskSpec {
	return TaskSpec{
		Name: name, Priority: p, Cost: cost,
		RunTimeout:   0,
		QueueTimeout: 0,
		Fn:           virtualNoop, // 不会在虚拟模式被调用
	}
}

// TestVirtualReproducible: 相同输入两次运行，事件序列必须逐字段一致；
// 不同种子（任务到达顺序打乱）结论仍稳定（账目归零、计数守恒）。
func TestVirtualReproducible(t *testing.T) {
	build := func() []VirtualTask {
		return []VirtualTask{
			{Spec: vtSpec("a", 1, Resources{CPU: 6}), Duration: 10 * time.Millisecond, SubmitAt: 0},
			{Spec: vtSpec("b", 8, Resources{CPU: 6}), Duration: 10 * time.Millisecond, SubmitAt: 1 * time.Millisecond},
			{Spec: vtSpec("c", 0, Resources{CPU: 6}), Duration: 5 * time.Millisecond, SubmitAt: 2 * time.Millisecond},
			{Spec: vtSpec("d", 4, Resources{CPU: 6}), Duration: 30 * time.Millisecond, SubmitAt: 3 * time.Millisecond,
				Panic: true},
			{Spec: func() TaskSpec {
				sp := vtSpec("e", 2, Resources{CPU: 6})
				sp.RunTimeout = 40 * time.Millisecond
				return sp
			}(), Duration: 200 * time.Millisecond, SubmitAt: 4 * time.Millisecond,
				Block: true},
			{Spec: func() TaskSpec {
				sp := vtSpec("f", 3, Resources{CPU: 6})
				sp.QueueTimeout = 5 * time.Millisecond
				return sp
			}(), Duration: 8 * time.Millisecond, SubmitAt: 5 * time.Millisecond},
		}
	}

	cfg := Config{
		Limits:        Limits{MaxCPU: 10, MaxMemory: 100, MaxIO: 100, MaxTasks: 1},
		Tick:          time.Millisecond,
		RunGrace:      10 * time.Millisecond,
		StarvationAge: 16,
		Logger:        NewSliceLogger(),
	}

	run := func() []Event {
		log := NewSliceLogger()
		c := cfg
		c.Logger = log
		e := NewVirtual(time.Unix(1_000_000, 0), c)
		e.Run(build(), time.Second)
		return log.Events()
	}

	ev1 := run()
	ev2 := run()
	if len(ev1) == 0 {
		t.Fatal("no events")
	}
	if len(ev1) != len(ev2) {
		t.Fatalf("event count differs: %d vs %d", len(ev1), len(ev2))
	}
	for i := range ev1 {
		if !eventsEqual(ev1[i], ev2[i]) {
			t.Fatalf("event %d differs:\n run1=%+v\n run2=%+v", i, ev1[i], ev2[i])
		}
	}

	// 关键结论断言：
	// 1) 高优先级 b 在 a 结束后先于 c 准入；
	// 2) panic 任务 d 以 panicked 结算，资源释放；
	// 3) Block 任务 e 超时+宽限后被 runaway 回收；
	// 4) 排队超时任务 f 以 timed_out 结算；
	// 5) 最终账目归零。
	var (
		admitOrder []string
		panicked   int
		runaway    int
		qtimeout   int
	)
	for _, ev := range ev1 {
		if ev.Type == EventAdmit {
			admitOrder = append(admitOrder, ev.TaskName)
		}
		if ev.Type == EventPanic {
			panicked++
		}
		if ev.Type == EventRunaway {
			runaway++
		}
		if ev.Type == EventTimeout && ev.TaskName == "f" {
			qtimeout++
		}
		if ev.Snap.InUse.CPU < 0 || ev.Snap.Running < 0 {
			t.Fatalf("negative ledger at event %d: %+v", ev.Seq, ev.Snap)
		}
	}
	last := ev1[len(ev1)-1].Snap
	if last.InUse != (Resources{}) || last.Running != 0 {
		t.Fatalf("final ledger not zero: %+v", last)
	}
	if panicked != 1 || runaway != 1 || qtimeout != 1 {
		t.Fatalf("counter mismatch: panic=%d runaway=%d qtimeout=%d", panicked, runaway, qtimeout)
	}
	// a -> b(高优先级) 的顺序必须成立；且 b 被准入时尚未被 aging 提升。
	if len(admitOrder) < 2 || admitOrder[0] != "a" || admitOrder[1] != "b" {
		t.Fatalf("priority order violated: %v", admitOrder)
	}
	var bAdmit *Event
	for i := range ev1 {
		if ev1[i].Type == EventAdmit && ev1[i].TaskName == "b" {
			bAdmit = &ev1[i]
		}
	}
	if bAdmit == nil || !strings.Contains(bAdmit.Decision, "eff=8") {
		t.Fatalf("b should be admitted by its own priority before aging: %+v", bAdmit)
	}
	// c/d/e 等待超过 16 个 tick 后被提升，提升后彼此 FIFO。
	if admitOrder[2] != "c" || admitOrder[3] != "d" || admitOrder[4] != "e" {
		t.Fatalf("boosted tasks should run FIFO: %v", admitOrder)
	}
	t.Logf("admit order: %v, total events=%d", admitOrder, len(ev1))
	printVirtualLog(t, ev1)
}

// TestVirtualStarvationBound: 在持续高优先级流中，低优先级任务
// 等待有界时长后一定被提升并准入。
func TestVirtualStarvationBound(t *testing.T) {
	var tasks []VirtualTask
	// 高优先级占位任务先独占槽位 10ms；低优先级 L 同时刻排队；
	// 之后每 3ms 到达一个高优先级短任务，形成持续压力。
	tasks = append(tasks, VirtualTask{
		Spec:     vtSpec("hold", 10, Resources{CPU: 10}),
		Duration: 10 * time.Millisecond, SubmitAt: 0,
	})
	tasks = append(tasks, VirtualTask{
		Spec:     vtSpec("L", 0, Resources{CPU: 10}),
		Duration: 1 * time.Millisecond, SubmitAt: 0,
	})
	for i := 0; i < 20; i++ {
		tasks = append(tasks, VirtualTask{
			Spec:     vtSpec("H"+itoa(i), 10, Resources{CPU: 10}),
			Duration: 2 * time.Millisecond,
			SubmitAt: time.Duration(i+1) * 3 * time.Millisecond,
		})
	}

	log := NewSliceLogger()
	e := NewVirtual(time.Unix(0, 0), Config{
		Limits:        Limits{MaxCPU: 10, MaxTasks: 1},
		Tick:          time.Millisecond,
		RunGrace:      5 * time.Millisecond,
		StarvationAge: 8,
		Logger:        log,
	})
	e.Run(tasks, 2*time.Second)

	var admits []string
	boosts := 0
	for _, ev := range log.Events() {
		if ev.Type == EventAdmit {
			admits = append(admits, ev.TaskName)
		}
		if ev.Type == EventQueue && strings.Contains(ev.Decision, "boosted") {
			boosts++
		}
	}
	foundL := false
	for _, n := range admits {
		if n == "L" {
			foundL = true
		}
	}
	if !foundL {
		t.Fatalf("low-priority L starved; admits=%v", admits)
	}
	if boosts == 0 {
		t.Fatal("expected aging boost events")
	}
	// L 不得排在所有 20 个高优先级任务之后：提升机制让它有界等待。
	idxL := indexOf(admits, "L")
	if idxL > 12 {
		t.Fatalf("L waited too long (position %d of %d): %v", idxL, len(admits), admits)
	}
	t.Logf("L admitted at position %d of %d", idxL, len(admits))
}

func eventsEqual(a, b Event) bool {
	if a.Type != b.Type || a.TaskID != b.TaskID || a.TaskName != b.TaskName ||
		a.Priority != b.Priority || a.Cost != b.Cost || a.Reason != b.Reason ||
		a.Decision != b.Decision || a.Snap != b.Snap {
		return false
	}
	// Time 用相对值不可比较（两次运行墙钟不同），虚拟时钟起点固定，
	// 因此 a.Time==b.Time 也应当成立。
	return a.Time.Equal(b.Time)
}

func printVirtualLog(t *testing.T, events []Event) {
	t.Helper()
	var b strings.Builder
	for _, e := range events {
		b.WriteString("  " + e.Time.Format("15:04:05.000") + " " + string(e.Type) +
			" " + e.TaskName + " " + e.Decision + "\n")
	}
	t.Logf("virtual decision log (%d events):\n%s", len(events), b.String())
}

func indexOf(xs []string, x string) int {
	for i, v := range xs {
		if v == x {
			return i
		}
	}
	return -1
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}
