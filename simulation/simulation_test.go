package simulation

import (
	"testing"
	"time"

	"github.com/lisama569/concurrent-scheduler-go/scheduler"
)

func baseSpec(seed int64) Spec {
	return Spec{
		Seed:              seed,
		Tasks:             120,
		Limits:            scheduler.Limits{MaxCPU: 20, MaxMemory: 20, MaxIO: 20, MaxTasks: 4},
		Tick:              time.Millisecond,
		RunGrace:          5 * time.Millisecond,
		StarvationAge:     8,
		TimeHorizon:       500 * time.Millisecond,
		AnomalyEvery:      17,
		QueueTimeoutEvery: 11,
		RunTimeoutEvery:   13,
	}
}

// TestSameSeedReproducible: 相同种子两次运行，事件序列逐字段一致。
func TestSameSeedReproducible(t *testing.T) {
	log1, rep1, err := Run(baseSpec(42))
	if err != nil {
		t.Fatal(err)
	}
	log2, rep2, err := Run(baseSpec(42))
	if err != nil {
		t.Fatal(err)
	}
	e1, e2 := log1.Events(), log2.Events()
	if len(e1) != len(e2) {
		t.Fatalf("event count differs: %d vs %d", len(e1), len(e2))
	}
	for i := range e1 {
		a, b := e1[i], e2[i]
		if a != b {
			t.Fatalf("event %d differs under same seed:\n %+v\n %+v", i, a, b)
		}
	}
	if rep1.EventCount != rep2.EventCount {
		t.Fatalf("report event count differs")
	}
}

// TestCrossSeedConclusionsHold: 多个不同种子下，账目守恒、
// 无越界、拒绝原因可区分、终态计数守恒。
func TestCrossSeedConclusionsHold(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 99, 2024} {
		_, rep, err := Run(baseSpec(seed))
		if err != nil {
			t.Fatal(err)
		}
		if len(rep.Violations) != 0 {
			t.Fatalf("seed %d ledger violations: %v", seed, rep.Violations)
		}
		snap := rep.FinalSnap
		if snap.InUse.CPU != 0 || snap.InUse.Memory != 0 || snap.InUse.IO != 0 ||
			snap.Running != 0 || snap.Queued != 0 {
			t.Fatalf("seed %d final ledger not clean: %+v", seed, snap)
		}
		// 准入守恒：获得过资源的任务（Accepted）最终恰好在
		// completed / canceled / 运行超时 / panic 中各计一次；
		// Runaway 是 TimedOut 的失控子集（账目先行释放），不另计终态；
		// 排队超时（QueueTimedOut）从未准入，与 Accepted 无关。
		admittedTerminal := snap.Completed + snap.Canceled + snap.TimedOut + snap.Panicked
		if snap.Accepted != admittedTerminal {
			t.Fatalf("seed %d accepted=%d but admitted terminals=%d (c=%d x=%d runTO=%d p=%d runaway=%d queueTO=%d)",
				seed, snap.Accepted, admittedTerminal,
				snap.Completed, snap.Canceled, snap.TimedOut, snap.Panicked,
				snap.Runaway, snap.QueueTimedOut)
		}
		if snap.Runaway > snap.TimedOut {
			t.Fatalf("seed %d runaway %d exceeds run-timeouts %d", seed, snap.Runaway, snap.TimedOut)
		}
		// 总任务守恒：所有任务要么准入，要么排队超时（本场景下无 too_large）。
		total := admittedTerminal + snap.QueueTimedOut + snap.Rejected
		if total != int64(120) {
			t.Fatalf("seed %d total accounted=%d want 120", seed, total)
		}
		// 异常注入必须被观测到（panic 与 runaway 均有）。
		if snap.Panicked == 0 {
			t.Fatalf("seed %d expected some panicked tasks", seed)
		}
		if snap.Runaway == 0 {
			t.Fatalf("seed %d expected some reclaimed runaway tasks", seed)
		}
		for reason, n := range rep.Rejected {
			if reason == scheduler.ReasonNone || n <= 0 {
				t.Fatalf("seed %d bad reject bucket: %v=%d", seed, reason, n)
			}
		}
	}
}
