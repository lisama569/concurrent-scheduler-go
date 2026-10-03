package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestCloseGroupMixedSettleAndStuck 复现"同组部分正常结束、部分卡死"的
// 混合场景：finisher 在最大等待内响应取消正常结算，stuck 完全不理会取消。
// 期望：不 panic、不出现负账；stuck 被标记 ABANDONED 且容量只回收一次。
func TestCloseGroupMixedSettleAndStuck(t *testing.T) {
	s := New(Config{MaxConcurrency: 8, ResourceLimit: 100},
		GroupConfig{Name: "A", MaxConcurrency: 8, ResourceLimit: 100},
	).WithLogger(testLogger{t})

	stuckExit := make(chan struct{})
	started := make(chan struct{}, 2)
	// finisher：收到取消立即退出，在最大等待内正常结算。
	mustGroupTask(t, s, "finisher", "A", 4, PriorityNormal, func(ctx context.Context) error {
		started <- struct{}{}
		<-ctx.Done()
		return nil
	})
	// stuck：完全不理会取消，直到测试最后放行（僵尸 goroutine）。
	hStuck := mustGroupTask(t, s, "stuck", "A", 6, PriorityNormal, func(ctx context.Context) error {
		started <- struct{}{}
		<-stuckExit
		return nil
	})
	<-started
	<-started

	cgCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.CloseGroup("A", cgCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded from CloseGroup, got %v", err)
	}

	if st := hStuck.Status(); st != StatusAbandoned {
		t.Fatalf("stuck task must be ABANDONED, got %s", st)
	}
	snap := s.Snapshot()
	if snap.InFlight != 0 || snap.ResourceUsed != 0 {
		t.Fatalf("global accounts must reconcile to zero, got %s", snap)
	}
	if ga := snap.Groups["A"]; ga.InFlight != 0 || ga.ResourceUsed != 0 {
		t.Fatalf("group A accounts must reconcile to zero, got %+v", ga)
	}

	// 僵尸 goroutine 真正退出：不得再动账目。
	close(stuckExit)
	time.Sleep(50 * time.Millisecond)
	if snap := s.Snapshot(); snap.InFlight != 0 || snap.ResourceUsed != 0 {
		t.Fatalf("zombie exit must not touch accounts: %s", snap)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// TestShutdownMixedSettleAndStuck：整体关闭路径上的同款混合场景——
// 同组内 finisher 在最大等待内正常结算、stuck 卡死被超时回收。
// 期望：不 panic、账目归零、stuck 为 ABANDONED、僵尸退出不再动账。
func TestShutdownMixedSettleAndStuck(t *testing.T) {
	s := New(Config{MaxConcurrency: 8, ResourceLimit: 100},
		GroupConfig{Name: "A", MaxConcurrency: 8, ResourceLimit: 100},
	).WithLogger(testLogger{t})

	stuckExit := make(chan struct{})
	started := make(chan struct{}, 2)
	hFin := mustGroupTask(t, s, "finisher", "A", 4, PriorityNormal, func(ctx context.Context) error {
		started <- struct{}{}
		<-ctx.Done()
		return nil
	})
	hStuck := mustGroupTask(t, s, "stuck", "A", 6, PriorityNormal, func(ctx context.Context) error {
		started <- struct{}{}
		<-stuckExit
		return nil
	})
	<-started
	<-started

	shCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.Shutdown(shCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded from Shutdown, got %v", err)
	}

	if st := hFin.Status(); st != StatusCanceled {
		t.Fatalf("finisher must settle as CANCELED (released exactly once), got %s", st)
	}
	if st := hStuck.Status(); st != StatusAbandoned {
		t.Fatalf("stuck task must be ABANDONED, got %s", st)
	}
	snap := s.Snapshot()
	if snap.InFlight != 0 || snap.ResourceUsed != 0 {
		t.Fatalf("global accounts must reconcile to zero, got %s", snap)
	}
	if ga := snap.Groups["A"]; ga.InFlight != 0 || ga.ResourceUsed != 0 {
		t.Fatalf("group A accounts must reconcile to zero, got %+v", ga)
	}

	// 僵尸 goroutine 真正退出：不得再动账目、不得覆盖 ABANDONED 终态。
	close(stuckExit)
	time.Sleep(50 * time.Millisecond)
	if snap := s.Snapshot(); snap.InFlight != 0 || snap.ResourceUsed != 0 {
		t.Fatalf("zombie exit must not touch accounts: %s", snap)
	}
	if st := hStuck.Status(); st != StatusAbandoned {
		t.Fatalf("zombie exit must not overwrite ABANDONED, got %s", st)
	}
}

// TestCloseGroupMixedMatrix：在跑任务数量、正常结束与卡死的组合如何变化，
// 单组关闭的超时回收都必须稳定正确：不 panic、账目归零不为负、卡死者
// 全部 ABANDONED、其他业务组（B）完全不受影响。
func TestCloseGroupMixedMatrix(t *testing.T) {
	cases := []struct{ finishers, stuck int }{
		{1, 1}, {2, 1}, {1, 2}, {3, 2}, {2, 3},
	}
	for _, tc := range cases {
		tc := tc
		name := "finishers=" + itoa(tc.finishers) + "_stuck=" + itoa(tc.stuck)
		t.Run(name, func(t *testing.T) {
			t.Logf("scenario: group A has %d cancel-responsive + %d cancel-ignoring tasks; close group A only", tc.finishers, tc.stuck)
			s := New(Config{MaxConcurrency: 32, ResourceLimit: 200},
				GroupConfig{Name: "A", MaxConcurrency: 32, ResourceLimit: 200},
				GroupConfig{Name: "B", MaxConcurrency: 32, ResourceLimit: 200},
			).WithLogger(testLogger{t})

			total := tc.finishers + tc.stuck
			started := make(chan struct{}, total)
			stuckExit := make(chan struct{})
			var fins, stucks []*Handle
			for i := 0; i < tc.finishers; i++ {
				h := mustGroupTask(t, s, "finisher-"+itoa(i), "A", 2, PriorityNormal,
					func(ctx context.Context) error {
						started <- struct{}{}
						<-ctx.Done()
						return nil
					})
				fins = append(fins, h)
			}
			for i := 0; i < tc.stuck; i++ {
				h := mustGroupTask(t, s, "stuck-"+itoa(i), "A", 3, PriorityNormal,
					func(ctx context.Context) error {
						started <- struct{}{}
						<-stuckExit // 完全不理会 ctx
						return nil
					})
				stucks = append(stucks, h)
			}
			for i := 0; i < total; i++ {
				<-started
			}

			cgCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			if err := s.CloseGroup("A", cgCtx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("want DeadlineExceeded from CloseGroup, got %v", err)
			}

			for _, h := range fins {
				if st := h.Status(); st != StatusCanceled {
					t.Fatalf("finisher %s must be CANCELED, got %s", h.ID(), st)
				}
			}
			for _, h := range stucks {
				if st := h.Status(); st != StatusAbandoned {
					t.Fatalf("stuck %s must be ABANDONED, got %s", h.ID(), st)
				}
			}
			assertZeroAccounts(t, s, "A")

			// 其他组不受影响：B 照常提交、跑完，且账目自洽。
			hB := mustGroupTask(t, s, "b-after", "B", 5, PriorityNormal, noop())
			waitHandle(t, hB, 2*time.Second)
			if st := hB.Status(); st != StatusSucceeded {
				t.Fatalf("group B must be unaffected by A reclaim, got %s", st)
			}

			// 全部僵尸退出后账目依旧为零、不为负。
			close(stuckExit)
			time.Sleep(50 * time.Millisecond)
			assertZeroAccounts(t, s, "A")
			if err := s.Shutdown(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestShutdownMixedRaceWindow：结束与卡死的先后贴着超时窗口变化时
// （finisher 恰好卡在最大等待边界附近退出），无论谁赢，结果都必须
// 稳定正确：不 panic、不为负、最终归零。多次迭代 + race 检测覆盖交错。
func TestShutdownMixedRaceWindow(t *testing.T) {
	for i := 0; i < 30; i++ {
		s := New(Config{MaxConcurrency: 4, ResourceLimit: 20},
			GroupConfig{Name: "A", MaxConcurrency: 4, ResourceLimit: 20},
		).WithLogger(testLogger{t})
		stuckExit := make(chan struct{})
		started := make(chan struct{}, 2)
		// finisher 在取消后约 50ms 退出，与 50ms 的最大等待打擂台。
		mustGroupTask(t, s, "borderline", "A", 4, PriorityNormal, func(ctx context.Context) error {
			started <- struct{}{}
			<-ctx.Done()
			time.Sleep(50 * time.Millisecond)
			return nil
		})
		hStuck := mustGroupTask(t, s, "stuck", "A", 6, PriorityNormal, func(ctx context.Context) error {
			started <- struct{}{}
			<-stuckExit
			return nil
		})
		<-started
		<-started

		shCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		_ = s.Shutdown(shCtx) // 边界竞争下 nil 或 DeadlineExceeded 都合法
		cancel()

		if st := hStuck.Status(); st != StatusAbandoned {
			t.Fatalf("iter %d: stuck must be ABANDONED, got %s", i, st)
		}
		close(stuckExit)
		time.Sleep(80 * time.Millisecond) // 等 borderline 与 stuck 的 goroutine 都收尾
		snap := s.Snapshot()
		if snap.InFlight < 0 || snap.ResourceUsed < 0 ||
			snap.Groups["A"].InFlight < 0 || snap.Groups["A"].ResourceUsed < 0 {
			t.Fatalf("iter %d: accounts must never go negative: %s %+v", i, snap, snap.Groups["A"])
		}
		if snap.InFlight != 0 || snap.ResourceUsed != 0 ||
			snap.Groups["A"].InFlight != 0 || snap.Groups["A"].ResourceUsed != 0 {
			t.Fatalf("iter %d: accounts must reconcile to zero: %s %+v", i, snap, snap.Groups["A"])
		}
	}
}

// assertZeroAccounts 断言全局账与指定组账都精确归零（且未出现负数，
// 负数会在释放路径上直接 panic，这里再做一层显式校验）。
func assertZeroAccounts(t *testing.T, s *Scheduler, group string) {
	t.Helper()
	snap := s.Snapshot()
	ga := snap.Groups[group]
	if snap.InFlight < 0 || snap.ResourceUsed < 0 || ga.InFlight < 0 || ga.ResourceUsed < 0 {
		t.Fatalf("accounts went negative: %s %+v", snap, ga)
	}
	if snap.InFlight != 0 || snap.ResourceUsed != 0 {
		t.Fatalf("global accounts must be zero, got %s", snap)
	}
	if ga.InFlight != 0 || ga.ResourceUsed != 0 {
		t.Fatalf("group %q accounts must be zero, got %+v", group, ga)
	}
}
