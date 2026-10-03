package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// 关闭与超时回收的"混合批次"专项测试。
//
// 背景：关闭某个业务组（CloseGroup）或整体关闭（Shutdown）时，同一批在跑
// 任务里可能一部分在最大等待时间内正常结束、另一部分完全不理会取消一直卡着。
// 历史缺陷：超时回收只认 detached 标记，而正常结算路径从不设置它，于是
// 已经结束的任务会被二次回收（quota.release / wg.Done 再来一次），账目被扣
// 成负数并直接 panic。本文件用可稳定复现的用例锁死修复语义：
//
//   - 正常结束的任务容量只归还一次，超时回收只落到确实还没结束的任务；
//   - 卡死任务被标记 ABANDONED，其僵尸 goroutine 日后退出只摘登记、不碰账；
//   - 回收完成后全局账与组账全部归零，回收全程不允许出现负数；
//   - 任务数量、提交先后（谁先开始谁卡死）、结束早晚如何变化，结果都稳定；
//   - 只关一个组时，其他业务组（含默认组）完全不受连累。
//
// 运行：go test -run 'TestCloseReclaim' -v，日志逐条打印覆盖的业务场景、
// 每个任务的终态与回收前后账目。

const (
	mixedMaxWait = 250 * time.Millisecond // 关闭流程的最大等待
	mixedPoll    = 2 * time.Second        // 断言轮询上限
)

// mixedCase 描述一个"部分正常结束 + 部分卡死"的关闭场景。
type mixedCase struct {
	name string
	op   string // "close-group" 或 "shutdown"

	nStuck int // 完全不理会取消的任务数
	// goodDelays：每个正常任务在观察到取消信号后"拖多久"再退出。
	// 通过 0、窗口中段、贴近上限三种延迟，覆盖结束与回收的各种先后交错；
	// 切片长度即正常任务数量，顺带覆盖"在跑任务有几个"的变化。
	goodDelays []time.Duration
	// order：提交（准入）顺序——卡死与正常任务谁先进入在跑集合，
	// 进而改变关闭时 running 表的遍历构成。
	order string // "goods-first" | "stuck-first" | "interleave"
}

func mixedCases() []mixedCase {
	return []mixedCase{
		{
			name:       "close-group/1good-immediate+1stuck/goods-first",
			op:         "close-group",
			nStuck:     1,
			goodDelays: []time.Duration{0},
			order:      "goods-first",
		},
		{
			name:       "close-group/3good-staggered+2stuck/stuck-first",
			op:         "close-group",
			nStuck:     2,
			goodDelays: []time.Duration{0, 60 * time.Millisecond, 180 * time.Millisecond},
			order:      "stuck-first",
		},
		{
			name:       "close-group/1good-near-deadline+1stuck/interleave",
			op:         "close-group",
			nStuck:     1,
			goodDelays: []time.Duration{180 * time.Millisecond},
			order:      "interleave",
		},
		{
			name:       "shutdown/1good-immediate+1stuck/goods-first",
			op:         "shutdown",
			nStuck:     1,
			goodDelays: []time.Duration{0},
			order:      "goods-first",
		},
		{
			name:       "shutdown/3good-staggered+2stuck/stuck-first",
			op:         "shutdown",
			nStuck:     2,
			goodDelays: []time.Duration{0, 60 * time.Millisecond, 180 * time.Millisecond},
			order:      "stuck-first",
		},
		{
			name:       "shutdown/1good-near-deadline+1stuck/interleave",
			op:         "shutdown",
			nStuck:     1,
			goodDelays: []time.Duration{180 * time.Millisecond},
			order:      "interleave",
		},
	}
}

// startMixedTask 启动一个任务：
//   - good：观察到取消后再拖 delay 退出（必然落在最大等待窗口内）；
//   - stuck：完全不看 ctx，只等测试的 stuckExit（僵尸收尾由测试驱动）。
func startMixedTask(t *testing.T, s *Scheduler, id, group string, cost int64,
	good bool, delay time.Duration, started chan<- string, stuckExit <-chan struct{}) *Handle {
	t.Helper()
	var fn func(ctx context.Context) error
	if good {
		fn = func(ctx context.Context) error {
			started <- id
			<-ctx.Done() // 响应取消，但可按用例安排拖延到窗口内任意时刻才退出
			if delay > 0 {
				time.Sleep(delay)
			}
			return nil
		}
	} else {
		fn = func(ctx context.Context) error {
			started <- id
			<-stuckExit // 故意永久无视取消信号
			return nil
		}
	}
	h, err := s.Submit(context.Background(), &Task{ID: id, Group: group, Cost: cost, Fn: fn})
	if err != nil {
		t.Fatalf("submit %s: %v", id, err)
	}
	return h
}

// accountMonitor 在关闭进行期间高频取样账目，任何一本账出现负数立即记录。
// 全部记账虽在同一把锁内完成（负数不可能被外部"看到"，只会随损坏性操作
// panic），这里仍做一道外部哨兵，显式坐实"任何时刻都不为负"。
func accountMonitor(t *testing.T, s *Scheduler, stop <-chan struct{}) func() error {
	var mu sync.Mutex
	bad := ""
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			snap := s.Snapshot()
			if snap.InFlight < 0 || snap.ResourceUsed < 0 {
				mu.Lock()
				bad = fmt.Sprintf("global account negative: %s", snap)
				mu.Unlock()
				return
			}
			for name, gs := range snap.Groups {
				if gs.InFlight < 0 || gs.ResourceUsed < 0 {
					mu.Lock()
					bad = fmt.Sprintf("group %q account negative: %+v", name, gs)
					mu.Unlock()
					return
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()
	return func() error {
		<-done
		mu.Lock()
		defer mu.Unlock()
		if bad != "" {
			return errors.New(bad)
		}
		return nil
	}
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(mixedPoll)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// TestCloseReclaimMixedBatch 覆盖两条关闭路径在"部分结束 + 部分卡死"混合
// 批次下的账目正确性（矩阵见 mixedCases）。
func TestCloseReclaimMixedBatch(t *testing.T) {
	t.Logf("场景矩阵：CloseGroup 与 Shutdown 各 3 个用例，共 %d 个；变量包括"+
		"卡死任务数(1/2)、正常任务数(1/3)、正常任务退出早晚(0ms/60ms/180ms，"+
		"最大等待=%s)、提交先后(goods-first/stuck-first/interleave)",
		len(mixedCases()), mixedMaxWait)

	for _, tc := range mixedCases() {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("[用例开始] %s：%d 个正常任务(延迟=%v) + %d 个卡死任务，提交顺序=%s",
				tc.name, len(tc.goodDelays), tc.goodDelays, tc.nStuck, tc.order)

			s := New(Config{MaxConcurrency: 32, ResourceLimit: 1000},
				GroupConfig{Name: "A", MaxConcurrency: 32, ResourceLimit: 1000},
				GroupConfig{Name: "B", MaxConcurrency: 32, ResourceLimit: 1000},
			).WithLogger(newTestLogger(t))

			stuckExit := make(chan struct{})
			started := make(chan string, tc.nStuck+len(tc.goodDelays)+1)

			var goods, stucks []*Handle
			costOf := func(i int) int64 { return int64(3 + i%5*2) } // 3,5,7,9,11… 顺带核对多 Cost 记账

			// 按用例声明的顺序构造提交序列。
			type spec struct {
				id    string
				good  bool
				delay time.Duration
			}
			var seq []spec
			for i, d := range tc.goodDelays {
				seq = append(seq, spec{fmt.Sprintf("good-%d", i+1), true, d})
			}
			for i := 0; i < tc.nStuck; i++ {
				seq = append(seq, spec{fmt.Sprintf("stuck-%d", i+1), false, 0})
			}
			ordered := make([]spec, 0, len(seq))
			switch tc.order {
			case "goods-first":
				ordered = append(ordered, seq...)
			case "stuck-first":
				for _, sp := range seq {
					if !sp.good {
						ordered = append(ordered, sp)
					}
				}
				for _, sp := range seq {
					if sp.good {
						ordered = append(ordered, sp)
					}
				}
			case "interleave":
				gi, si := 0, 0
				for gi < len(tc.goodDelays) || si < tc.nStuck {
					if si < tc.nStuck {
						ordered = append(ordered, spec{fmt.Sprintf("stuck-%d", si+1), false, 0})
						si++
					}
					if gi < len(tc.goodDelays) {
						ordered = append(ordered, spec{fmt.Sprintf("good-%d", gi+1), true, tc.goodDelays[gi]})
						gi++
					}
				}
			}

			// 他组锚点：阻塞在自己的闸门上，同时也响应自身 ctx 取消。
			// 只关 A 时 B 的 ctx 不会被取消，它靠放闸退出；整体关闭时它随
			// 全局取消而结束（CANCELED，不是 ABANDONED）。
			otherExit := make(chan struct{})
			anchorFn := func(ctx context.Context) error {
				started <- "other-B"
				select {
				case <-otherExit:
				case <-ctx.Done():
				}
				return nil
			}
			hOther, aerr := s.Submit(context.Background(),
				&Task{ID: "other-B", Group: "B", Cost: 2, Fn: anchorFn})
			if aerr != nil {
				t.Fatalf("submit other-B: %v", aerr)
			}

			for i, sp := range ordered {
				h := startMixedTask(t, s, sp.id, "A", costOf(i), sp.good, sp.delay, started, stuckExit)
				if sp.good {
					goods = append(goods, h)
				} else {
					stucks = append(stucks, h)
				}
			}

			total := 1 + len(goods) + len(stucks)
			for i := 0; i < total; i++ {
				<-started
			}
			t.Logf("[全部准入] A 组在跑=%d（正常=%d 卡死=%d），B 组锚点在跑=1；快照=%s",
				len(goods)+len(stucks), len(goods), len(stucks), s.Snapshot())

			// 关闭期间盯账：不允许出现任何负数。
			monStop := make(chan struct{})
			monJoin := accountMonitor(t, s, monStop)

			opErr := make(chan error, 1)
			go func() {
				cctx, cancel := context.WithTimeout(context.Background(), mixedMaxWait)
				defer cancel()
				if tc.op == "close-group" {
					opErr <- s.CloseGroup("A", cctx)
				} else {
					opErr <- s.Shutdown(cctx)
				}
			}()

			err := <-opErr
			close(monStop)
			if merr := monJoin(); merr != nil {
				t.Fatalf("关闭过程中账目出现负数：%v", merr)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("[%s] 因存在无视取消的卡死任务，应返回 DeadlineExceeded，实际=%v", tc.name, err)
			}
			t.Logf("[关闭返回] op=%s err=%v", tc.op, err)

			// 1) 正常任务：窗口内全部终态，且绝不能被标成 ABANDONED。
			for _, h := range goods {
				waitHandle(t, h, mixedPoll)
				st := h.Status()
				if st == StatusAbandoned {
					t.Fatalf("正常结束的任务 %s 被错误标记为 ABANDONED", h.ID())
				}
				if st != StatusCanceled {
					t.Fatalf("正常任务 %s 期望 CANCELED（响应关闭取消），实际 %s", h.ID(), st)
				}
				t.Logf("  正常任务 %-7s cost=%d 终态=%s（容量仅由 settle 归还一次）", h.ID(), h.Cost(), st)
			}

			// 2) 卡死任务：回收后必须被清楚标记为已放弃。
			for _, h := range stucks {
				waitFor(t, func() bool { return h.Status() == StatusAbandoned },
					fmt.Sprintf("stuck %s -> ABANDONED", h.ID()))
				if !errors.Is(h.Err(), ErrTaskAbandoned) {
					t.Fatalf("卡死任务 %s 的错误应为 ErrTaskAbandoned，实际 %v", h.ID(), h.Err())
				}
				t.Logf("  卡死任务 %-7s cost=%d 终态=%s（容量由回收路径归还，仅此一次）",
					h.ID(), h.Cost(), h.Status())
			}

			// 3) 回收完成：A 组账必须立即归零。
			//    - 只关 A：B 组锚点仍在跑，全局账应恰好等于 B 的占用（1/2），
			//      证明 A 的回收既没多扣也没漏扣到全局账；
			//    - 整体关闭：所有组都结算/回收完毕，全局账必须归零。
			var snap Snapshot
			waitFor(t, func() bool {
				snap = s.Snapshot()
				ga := snap.Groups["A"]
				if ga.InFlight != 0 || ga.ResourceUsed != 0 {
					return false
				}
				if tc.op == "shutdown" {
					return snap.InFlight == 0 && snap.ResourceUsed == 0
				}
				return snap.InFlight == 1 && snap.ResourceUsed == 2
			}, "group A zeroed (and global account matches the only surviving group)")
			t.Logf("[回收对账] 全局账 inFlight=%d resourceUsed=%d；A 组 inFlight=%d resourceUsed=%d",
				snap.InFlight, snap.ResourceUsed, snap.Groups["A"].InFlight, snap.Groups["A"].ResourceUsed)

			// 4) 他组不受连累。
			if tc.op == "close-group" {
				// 只关 A：B 组锚点仍在跑、账目保留，且 B/默认组仍可提交新任务。
				gb := snap.Groups["B"]
				if gb.InFlight != 1 || gb.ResourceUsed != 2 {
					t.Fatalf("关闭 A 不应波及 B 的在跑任务，B 快照=%+v", gb)
				}
				if hOther.Status() != StatusRunning {
					t.Fatalf("B 组任务不应被 CloseGroup(A) 取消，实际 %s", hOther.Status())
				}
				for i := 0; i < 5; i++ {
					h, err := s.Submit(context.Background(), &Task{
						ID: fmt.Sprintf("b-after-%d", i), Group: "B", Cost: 1, Fn: noop(),
					})
					if err != nil {
						t.Fatalf("A 关闭后 B 组必须继续可服务：%v", err)
					}
					waitHandle(t, h, time.Second)
				}
				hDef, derr := s.Submit(context.Background(),
					task("default-after", 1, PriorityNormal, noop()))
				if derr != nil {
					t.Fatalf("A 关闭后默认组必须继续可服务：%v", derr)
				}
				waitHandle(t, hDef, time.Second)
				if _, err := s.Submit(context.Background(), &Task{
					ID: "a-after", Group: "A", Cost: 1, Fn: noop(),
				}); err == nil {
					t.Fatal("A 关闭后新提交必须被拒绝，而不是被接受")
				} else {
					var re *RejectError
					if !errors.As(err, &re) || re.Reason != ReasonGroupStopped {
						t.Fatalf("A 关闭后新提交应被 ReasonGroupStopped 显式拒绝，实际 %v", err)
					}
				}
				t.Logf("[他组隔离] B 组锚点保持 RUNNING，B/默认组新任务全部成功，A 新提交被 GROUP_STOPPED 拒绝")

				// 收尾：放掉 B 锚点，再整体关闭（卡死僵尸此时仍未退出）。
				close(otherExit)
				waitHandle(t, hOther, time.Second)
				if err := s.Shutdown(context.Background()); err != nil {
					t.Fatalf("兜底 Shutdown 失败：%v", err)
				}
			} else {
				// 整体关闭：B 组锚点随全局取消而正常结束（CANCELED，非 ABANDONED），
				// B 组账目同样归零，调度器置 stopped。
				waitHandle(t, hOther, mixedPoll)
				if hOther.Status() != StatusCanceled {
					t.Fatalf("整体关闭下 B 组正常任务应为 CANCELED，实际 %s", hOther.Status())
				}
				waitFor(t, func() bool {
					snap = s.Snapshot()
					gb := snap.Groups["B"]
					return snap.Stopped && gb.InFlight == 0 && gb.ResourceUsed == 0
				}, "group B zeroed and scheduler stopped")
				t.Logf("[他组隔离] 整体关闭下 B 组正常任务 CANCELED，B 账归零，全局 stopped=true")
			}

			// 5) 僵尸 goroutine 真正退出：只摘登记，绝不再动账，终态保持放弃。
			close(stuckExit)
			deadline := time.Now().Add(mixedPoll)
			for time.Now().Before(deadline) {
				snap = s.Snapshot()
				if snap.InFlight == 0 && snap.ResourceUsed == 0 &&
					snap.Groups["A"].InFlight == 0 && snap.Groups["A"].ResourceUsed == 0 {
					break
				}
				time.Sleep(2 * time.Millisecond)
			}
			if snap.InFlight != 0 || snap.ResourceUsed != 0 ||
				snap.Groups["A"].InFlight != 0 || snap.Groups["A"].ResourceUsed != 0 {
				t.Fatalf("僵尸退出后账目仍必须全部为零：%s, A=%+v", snap, snap.Groups["A"])
			}
			for _, h := range stucks {
				if h.Status() != StatusAbandoned {
					t.Fatalf("僵尸退出不得改写 ABANDONED 终态，%s 实际 %s", h.ID(), h.Status())
				}
			}
			t.Logf("[僵尸收尾] 全部卡死 goroutine 已退出，账目依旧归零，终态保持 ABANDONED")
			t.Logf("[用例通过] %s", tc.name)
		})
	}
}

// TestCloseReclaimSettleRaceStress 反复制造"任务自行结算"与"关闭超时回收"
// 在同一把锁上激烈竞争的批次：部分任务一收到取消就立刻退出（极可能在
// 极短的最大等待窗口内与回收临界区擦肩），部分任务永久卡死。重复多轮且
// 建议配合 -race 运行：任何二次释放 / 负数 / wg 误用都会直接暴露。
func TestCloseReclaimSettleRaceStress(t *testing.T) {
	const rounds = 60
	t.Logf("竞争压力：%d 轮，每轮 4 个在跑任务（2 个立即响应取消 + 2 个永久卡死），"+
		"最大等待收紧到 1ms 以最大化 settle 与超时回收的交错概率", rounds)

	for r := 0; r < rounds; r++ {
		s := New(Config{MaxConcurrency: 16, ResourceLimit: 100},
			GroupConfig{Name: "A", MaxConcurrency: 16, ResourceLimit: 100},
			GroupConfig{Name: "B", MaxConcurrency: 16, ResourceLimit: 100},
		) // 故意不挂 logger，压力轮保持输出干净
		stuckExit := make(chan struct{})
		started := make(chan string, 4)

		var stucks []*Handle
		mk := func(id, group string, good bool, cost int64) *Handle {
			fn := func(ctx context.Context) error {
				started <- id
				if good {
					<-ctx.Done()
					return nil
				}
				<-stuckExit
				return nil
			}
			h, err := s.Submit(context.Background(), &Task{ID: id, Group: group, Cost: cost, Fn: fn})
			if err != nil {
				t.Fatalf("round %d submit %s: %v", r, id, err)
			}
			return h
		}
		h1 := mk("good-1", "A", true, 3)
		stucks = append(stucks, mk("stuck-1", "A", false, 4))
		h2 := mk("good-2", "A", true, 5)
		stucks = append(stucks, mk("stuck-2", "A", false, 6))
		// 他组锚点：每轮都在，验证 A 组回收不波及其账目。
		hb := mk("b", "B", true, 7)
		for i := 0; i < 5; i++ {
			<-started
		}

		cctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		op := r % 2
		var err error
		if op == 0 {
			err = s.CloseGroup("A", cctx)
		} else {
			err = s.Shutdown(cctx)
		}
		cancel()
		_ = err // 可能赶上 DeadlineExceeded（有卡死任务），也可能竞争时序不同，关键是不 panic

		// 正常任务终态确定（CANCELED），卡死任务终态确定（ABANDONED）。
		for _, h := range []*Handle{h1, h2} {
			waitHandle(t, h, mixedPoll)
			if h.Status() == StatusAbandoned {
				t.Fatalf("round %d op=%d: 正常任务 %s 不应被放弃", r, op, h.ID())
			}
		}
		for _, h := range stucks {
			waitFor(t, func() bool { return h.Status() == StatusAbandoned },
				fmt.Sprintf("round %d stuck %s ABANDONED", r, h.ID()))
		}

		snap := s.Snapshot()
		ga := snap.Groups["A"]
		if ga.InFlight != 0 || ga.ResourceUsed != 0 {
			t.Fatalf("round %d op=%d: A 组账目未归零 %+v", r, op, ga)
		}
		if snap.InFlight < 0 || snap.ResourceUsed < 0 {
			t.Fatalf("round %d op=%d: 全局账目为负 %s", r, op, snap)
		}
		if op == 1 { // shutdown：全部归零
			if snap.InFlight != 0 || snap.ResourceUsed != 0 {
				t.Fatalf("round %d: shutdown 后全局账目应归零，实际 %s", r, snap)
			}
			if hb.Status() == StatusAbandoned {
				t.Fatalf("round %d: B 组正常任务不应被放弃", r)
			}
		} else { // close-group：B 组锚点仍在跑，随后放掉并兜底关闭
			gb := snap.Groups["B"]
			if gb.InFlight != 1 || gb.ResourceUsed != 7 {
				t.Fatalf("round %d: 关闭 A 不应波及 B，实际 %+v", r, gb)
			}
			if err := s.Shutdown(context.Background()); err != nil {
				t.Fatalf("round %d: 兜底 shutdown（锚点仍在跑）: %v", r, err)
			}
		}
		close(stuckExit)
		if t.Failed() {
			return
		}
	}
	t.Logf("竞争压力 %d 轮全部通过：正常任务只结算一次、卡死任务只回收一次，账目恒非负并归零，他组不受影响", rounds)
}
