package scheduler

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
)

// Config 声明调度器的硬约束：
//   - MaxConcurrency：同时运行任务数上限（并发度配额）；
//   - ResourceLimit：所有运行中任务 Cost 之和的上限（总资源配额）；
//   - QueueCapacity：等待队列容量，<=0 表示无界。
type Config struct {
	MaxConcurrency int
	ResourceLimit  int64
	QueueCapacity  int
}

// Scheduler 是进程内的并发任务调度器。
//
// 同步模型：所有账目变更与队列操作都在唯一的 mu 临界区内完成；
// 任务在独立 goroutine 中执行，但"准入记账"与"结算释放"均回到 mu 下进行，
// 从而保证：检查与记账之间不存在 TOCTOU 窗口、每个任务的资源恰好释放一次。
type Scheduler struct {
	cfg Config
	log Logger

	mu      sync.Mutex
	cond    *sync.Cond // 与 mu 联动，唤醒唯一的 dispatch 循环
	quota   *quota
	queue   *waitQueue
	seq     uint64
	paused  bool
	stopped bool
	wg      sync.WaitGroup // 追踪所有已准入任务，供优雅关闭等待

	// onAdmit / onTerminal 为可选测试钩子，用于可复现性测试采集准入与
	// 终态序列；生产路径保持为 nil。
	onAdmit    func(id string)
	onTerminal func(id string, status Status)

	// dispatchFrozen 为测试专用：为真时，Submit 不走"立即准入"、异步
	// dispatchLoop 也被冻结，准入改由测试线程通过 dispatchOneLocked 在
	// 锁内同步驱动，从而把调度顺序与 goroutine 真实时序完全解耦。
	dispatchFrozen bool
	// settleBarrier 为可选测试回调，在每次 settle 记账后（仍持锁）调用。
	settleBarrier func(id string)
}

// New 创建调度器并启动派发循环。
func New(cfg Config) *Scheduler {
	if cfg.MaxConcurrency <= 0 {
		panic("scheduler: Config.MaxConcurrency must be > 0")
	}
	if cfg.ResourceLimit < 0 {
		panic("scheduler: Config.ResourceLimit must be >= 0")
	}
	s := &Scheduler{
		cfg:   cfg,
		log:   nopLogger{},
		quota: newQuota(cfg),
		queue: newWaitQueue(cfg.QueueCapacity),
	}
	s.cond = sync.NewCond(&s.mu)
	go s.dispatchLoop()
	return s
}

// WithLogger 注入决策日志器（须在提交任务前调用）。
func (s *Scheduler) WithLogger(l Logger) *Scheduler {
	if l != nil {
		s.log = l
	}
	return s
}

// Submit 申请准入一个任务：
//   - 已关闭        => ReasonSchedulerStopped（显式拒绝，不丢弃）
//   - Cost > 硬上限 => ReasonExceedsHardLimit（即使空闲也永远满足不了，立即拒绝）
//   - 队列已满      => ReasonQueueFull
//   - 其余情况排队等待；若当前可立即满足则直接准入。
//
// 返回的 Handle 可用于等待 / 取消；排队中被取消的任务不执行任何用户代码。
func (s *Scheduler) Submit(parent context.Context, t *Task) (*Handle, error) {
	if t == nil || t.Fn == nil {
		return nil, fmt.Errorf("scheduler: task and task.Fn must not be nil")
	}
	if t.Cost < 0 {
		return nil, fmt.Errorf("scheduler: task %q has negative cost", t.ID)
	}

	ctx, cancel := context.WithCancel(parent)
	h := newHandle(t, ctx)
	h.bindCancel(cancel)

	s.mu.Lock()
	switch {
	case s.stopped:
		h.finish(StatusRejected, reasonError(t.ID, ReasonSchedulerStopped, "scheduler is shutting down"))
		snap := s.snapshotLocked()
		s.mu.Unlock()
		logDecision(s.log, "REJECT", t.ID, t.Cost, "scheduler stopped", snap)
		return h, h.Err()
	case t.Cost > s.cfg.ResourceLimit:
		h.finish(StatusRejected, reasonError(t.ID, ReasonExceedsHardLimit,
			fmt.Sprintf("cost %d > resource limit %d", t.Cost, s.cfg.ResourceLimit)))
		snap := s.snapshotLocked()
		s.mu.Unlock()
		logDecision(s.log, "REJECT", t.ID, t.Cost, "cost exceeds hard limit", snap)
		return h, h.Err()
	case s.queue.full():
		h.finish(StatusRejected, reasonError(t.ID, ReasonQueueFull,
			fmt.Sprintf("waiting queue at capacity %d", s.cfg.QueueCapacity)))
		snap := s.snapshotLocked()
		s.mu.Unlock()
		logDecision(s.log, "REJECT", t.ID, t.Cost, "queue full", snap)
		return h, h.Err()
	}

	if !s.paused && !s.dispatchFrozen && s.quota.canAdmit(t.Cost) {
		// 立即准入路径：与排队准入共用 quota 的同一段记账逻辑，口径一致。
		// wg.Add 必须在 mu 内、Shutdown 的 Wait 之前完成，避免 WaitGroup 误用。
		s.seq++
		s.quota.admit(t.Cost)
		s.wg.Add(1)
		s.fireAdmitLocked(t.ID)
		snap := s.snapshotLocked()
		s.mu.Unlock()
		logDecision(s.log, "ADMIT", t.ID, t.Cost, "submit: concurrency & resource both available", snap)
		s.launch(h, t)
		return h, nil
	}

	s.seq++
	seq := s.seq
	s.queue.push(h, t.Cost, t.Priority, seq)
	snap := s.snapshotLocked()
	paused := s.paused
	s.mu.Unlock()
	logDecision(s.log, "ENQUEUE", t.ID, t.Cost,
		fmt.Sprintf("paused=%t or quota unavailable; priority=%d seq=%d", paused, t.Priority, seq), snap)
	s.cond.Signal()

	// 排队期间 ctx 被取消（调用方取消 / Cancel / Shutdown）：从队列摘除并结算。
	go s.waitQueuedCancel(h)
	return h, nil
}

// Pause 暂停派发：已在运行的任务不受影响，新提交与已排队任务一律等待。
func (s *Scheduler) Pause() {
	s.mu.Lock()
	s.paused = true
	snap := s.snapshotLocked()
	s.mu.Unlock()
	logDecision(s.log, "PAUSE", "-", 0, "manual pause", snap)
}

// Resume 恢复派发并唤醒派发循环。
func (s *Scheduler) Resume() {
	s.mu.Lock()
	s.paused = false
	snap := s.snapshotLocked()
	s.mu.Unlock()
	logDecision(s.log, "RESUME", "-", 0, "manual resume", snap)
	s.cond.Broadcast()
}

// Shutdown 优雅关闭：拒绝新任务、取消所有排队中任务、等待在运行任务
// 直到它们结束或 ctx 到期。无论运行任务是否配合取消，账目都只会在其
// goroutine 退出时被精确释放一次。
func (s *Scheduler) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	// 摘空队列：排队任务从未记账，只需结算为取消。
	for _, e := range s.queue.entries {
		e.handle.cancel()
		e.handle.finish(StatusCanceled, ErrTaskCanceled)
		logDecision(s.log, "CANCEL-SHUTDOWN", e.handle.task.ID, e.cost,
			"queued task canceled by shutdown; never consumed quota", s.snapshotLocked())
	}
	s.queue.entries = nil
	s.mu.Unlock()
	s.cond.Broadcast() // 唤醒派发循环使其观察到 stopped 并退出

	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		s.log.Logf("DECISION action=SHUTDOWN-DONE task=- cost=0 basis=\"all running tasks settled\" | %s", s.Snapshot())
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Snapshot 返回当前账目的一致快照。
func (s *Scheduler) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *Scheduler) snapshotLocked() Snapshot {
	snap := s.quota.snapshot()
	snap.Queued = s.queue.len()
	snap.Paused = s.paused
	snap.Stopped = s.stopped
	return snap
}

// waitQueuedCancel 监听排队任务的取消信号；若任务先被派发取走（出队），
// 摘除失败即为正常情况，结算完全交给运行路径，保证结算只发生一次。
func (s *Scheduler) waitQueuedCancel(h *Handle) {
	<-h.ctx.Done()
	s.mu.Lock()
	if !s.queue.remove(h) {
		s.mu.Unlock()
		return
	}
	if h.finish(StatusCanceled, ErrTaskCanceled) {
		snap := s.snapshotLocked()
		s.mu.Unlock()
		logDecision(s.log, "CANCEL-QUEUED", h.task.ID, h.task.Cost,
			"context done while waiting; never consumed quota", snap)
		s.cond.Signal()
		return
	}
	s.mu.Unlock()
}

// dispatchOneLocked 仅供确定性测试：在 mu 内同步地选出并记账一个可准入
// 任务，但不在锁内启动 goroutine，而是返回 (handle, task, cost) 由调用方
// 锁外启动。无可准入任务时返回 nil。整个准入决策由此方法的调用线程串行
// 驱动，与 goroutine 调度时序完全解耦。
func (s *Scheduler) dispatchOneLocked() (*Handle, *Task, int64) {
	if s.stopped || s.paused {
		return nil, nil, 0
	}
	e := s.queue.popEligible(s.quota)
	if e == nil {
		return nil, nil, 0
	}
	s.quota.admit(e.cost)
	s.wg.Add(1)
	s.fireAdmitLocked(e.handle.task.ID)
	return e.handle, e.handle.task, e.cost
}

// dispatchLoop 是唯一的派发者：串行地从等待队列挑选可准入任务。
// 单一派发者 + 全确定性的排序规则，是相同输入下调度顺序可复现的关键。
func (s *Scheduler) dispatchLoop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		// 1) 锁内收集本批全部"当前可满足"的任务并记账。
		//    全程不释放 mu，因此"谓词检查 -> cond.Wait"之间不存在缝隙，
		//    settle 的 Signal 不可能落在该缝隙里，从根本上杜绝漏唤醒。
		type admitted struct {
			h    *Handle
			t    *Task
			cost int64
			eff  int64
			seq  uint64
			snap Snapshot
		}
		var batch []admitted
		for !s.stopped && !s.paused && !s.dispatchFrozen {
			e := s.queue.popEligible(s.quota)
			if e == nil {
				break
			}
			s.quota.admit(e.cost)
			s.wg.Add(1)
			s.fireAdmitLocked(e.handle.task.ID)
			batch = append(batch, admitted{
				h: e.handle, t: e.handle.task, cost: e.cost,
				eff: e.effectivePriority(), seq: e.seq, snap: s.snapshotLocked(),
			})
		}
		// 2) 锁外统一启动本批 goroutine（持锁启动没有必要，且会让
		//    立即结束的任务在 launch 阶段排队争锁）。
		if len(batch) > 0 {
			s.mu.Unlock()
			for _, a := range batch {
				logDecision(s.log, "ADMIT", a.t.ID, a.cost,
					fmt.Sprintf("dispatch: effective-priority=%d seq=%d fits current quota",
						a.eff, a.seq), a.snap)
				s.launch(a.h, a.t)
			}
			s.mu.Lock()
		}
		if s.stopped {
			return
		}
		// 3) 无法继续派发：对所有等待者做一次老化（每次真正进入等待计
		//    一 tick），老任务的有效优先级终将超过后来的高优先级任务，
		//    不会无限期饥饿。冻结态（确定性测试）不计时。
		if !s.paused && !s.dispatchFrozen && s.queue.len() > 0 {
			maxAge := s.queue.ageAll()
			if maxAge > 0 && maxAge%16 == 0 {
				s.log.Logf("STARVATION-WATCH max-wait-ticks=%d | %s", maxAge, s.snapshotLocked())
			}
		}
		// 4) 冻结态直接等待在 cond 上（freezeDispatch 会广播唤醒它重判，
		//    但它会因 dispatchFrozen 再次回到等待）；正常态在谓词循环中
		//    等待，醒来后重新检查，杜绝漏唤醒。
		for s.dispatchFrozen || !s.canProgress() {
			if s.stopped {
				return
			}
			s.cond.Wait()
		}
	}
}

// canProgress 报告派发者此刻是否应被唤醒：
// 已停止（需退出）、或非暂停/未冻结且存在可立即满足的任务。
func (s *Scheduler) canProgress() bool {
	if s.stopped {
		return true
	}
	return !s.paused && !s.dispatchFrozen && s.queue.popProbe(s.quota)
}

// freezeDispatch 仅供确定性测试：冻结异步派发与"立即准入"，并广播唤醒
// 派发循环使其观察到冻结。冻结后准入完全由 dispatchOneLocked 同步驱动。
func (s *Scheduler) freezeDispatch() {
	s.mu.Lock()
	s.dispatchFrozen = true
	s.mu.Unlock()
	s.cond.Broadcast()
}

// launch 在锁外为已准入任务启动执行 goroutine。
// 调用方必须已在 mu 内完成 wg.Add（与 Shutdown 的 Wait 建立 happens-before）。
func (s *Scheduler) launch(h *Handle, t *Task) {
	go s.run(h, t)
}

// fireAdmitLocked 在 mu 内回调准入钩子。持锁触发保证该登记严格先于
// 任务可能的结算回调（onTerminal），确定性运行器据此建立"准入 -> 结算"偏序。
// 钩子实现不得回调调度器公共方法，以免重入死锁。
func (s *Scheduler) fireAdmitLocked(id string) {
	if s.onAdmit != nil {
		s.onAdmit(id)
	}
}

// run 是单个任务的全部执行与结算路径，与任务 goroutine 一一对应，
// 因此 settle 只会被调用一次，资源精确释放一次。
func (s *Scheduler) run(h *Handle, t *Task) {
	defer s.wg.Done()
	h.markRunning()

	runCtx := h.ctx
	if t.Timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(h.ctx, t.Timeout)
		defer cancel()
	}

	var (
		fnErr    error
		panicked any
	)
	func() {
		// panic 屏障：用户任务 panic 被就地捕获，绝不逃出 goroutine，
		// 也不触碰调度器锁，因此不会破坏配额账目或其他任务。
		defer func() {
			if r := recover(); r != nil {
				panicked = r
				s.log.Logf("PANIC-ISOLATED task=%s recover=%v stack=%s", t.ID, r, debug.Stack())
			}
		}()
		fnErr = t.Fn(runCtx)
	}()

	status, err := classify(t, fnErr, panicked, runCtx)
	s.settle(h, t, status, err)
}

// classify 依据执行结果与 ctx 状态给出互斥的终态。
func classify(t *Task, fnErr error, panicked any, ctx context.Context) (Status, error) {
	switch {
	case panicked != nil:
		return StatusPanicked, fmt.Errorf("scheduler: task %q panicked: %v", t.ID, panicked)
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return StatusTimedOut, fmt.Errorf("scheduler: task %q timed out after %s", t.ID, t.Timeout)
	case errors.Is(ctx.Err(), context.Canceled):
		return StatusCanceled, ErrTaskCanceled
	case fnErr != nil:
		return StatusFailed, fnErr
	default:
		return StatusSucceeded, nil
	}
}

// settle 完成"终态登记 + 账目释放"。仅由任务自身 goroutine 调用一次；
// 排队取消 / 关闭路径从不调用本方法（那些任务未占资源）。
func (s *Scheduler) settle(h *Handle, t *Task, status Status, err error) {
	h.finish(status, err)
	s.mu.Lock()
	s.quota.release(t.Cost) // 若重复释放，账本会立即 panic 暴露账目损坏
	snap := s.snapshotLocked()
	// 终态记录与结算令牌都在 mu 内、且先于解锁完成：等待令牌的一方一旦
	// 被唤醒，终态与账目必然都已可见，避免"收到令牌但终态表未更新"的竞态。
	if s.onTerminal != nil {
		s.onTerminal(t.ID, status)
	}
	if s.settleBarrier != nil {
		s.settleBarrier(t.ID)
	}
	s.mu.Unlock()
	logDecision(s.log, "RELEASE", t.ID, t.Cost,
		fmt.Sprintf("terminal=%s err=%v; quota returned exactly once", status, err), snap)
	s.cond.Signal()
}
