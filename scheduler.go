package scheduler

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
)

// Config 声明调度器的全局硬约束：
//   - MaxConcurrency：同时运行任务数上限（全局并发度配额）；
//   - ResourceLimit：所有运行中任务 Cost 之和的上限（全局总资源配额）；
//   - QueueCapacity：全局等待队列容量，<=0 表示无界。
//
// 组级上限是这些全局上限之外的"额外硬约束"：任务必须同时满足全局账
// 与所属组账才能准入，任何一个组都不能动用别的组的预算。
type Config struct {
	MaxConcurrency int
	ResourceLimit  int64
	QueueCapacity  int
}

// Scheduler 是进程内的并发任务调度器（多业务组版本）。
//
// 同步模型：所有账目变更、组注册表操作与队列操作都在唯一的 mu 临界区内
// 完成；任务在独立 goroutine 中执行，但"准入记账"与"结算释放"均回到 mu
// 下进行，从而保证：检查与记账之间不存在 TOCTOU 窗口、每个任务的资源
// （全局账与组账两本）恰好释放一次。
type Scheduler struct {
	cfg      Config
	queueCap int
	log      Logger

	mu       sync.Mutex
	cond     *sync.Cond // 与 mu 联动，唤醒唯一的 dispatch 循环
	quota    *quota
	registry *groupRegistry
	seq      uint64
	paused   bool
	stopped  bool
	wg       sync.WaitGroup // 追踪全部已准入任务，供全局优雅关闭等待

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

// New 创建调度器并启动派发循环。可选的 groups 用于预先注册业务组；
// 不传任何组时，行为与单池版本完全一致（任务都落在内建默认组，
// 默认组没有任何组级配额，仅受全局上限约束）。
func New(cfg Config, groups ...GroupConfig) *Scheduler {
	if cfg.MaxConcurrency <= 0 {
		panic("scheduler: Config.MaxConcurrency must be > 0")
	}
	if cfg.ResourceLimit < 0 {
		panic("scheduler: Config.ResourceLimit must be >= 0")
	}
	s := &Scheduler{
		cfg:      cfg,
		queueCap: cfg.QueueCapacity,
		log:      nopLogger{},
		quota:    newQuota(cfg),
		registry: newGroupRegistry(),
	}
	s.registry.ensureDefault()
	for _, g := range groups {
		if err := s.registry.configure(g); err != nil {
			panic(err)
		}
	}
	s.cond = sync.NewCond(&s.mu)
	go s.dispatchLoop()
	return s
}

// ConfigureGroup 运行期注册新业务组或调整已有业务组的组级上限：
//   - 新组随时可加入，加入后该组提交立即按其预算受约束；
//   - 加容量：只改上限并唤醒派发，等待中的任务可继续往前走；
//   - 缩容量：不驱逐在运行任务，used 可能瞬时高于新上限，期间冻结该组
//     新准入，等在跑任务结算自然回落，账目任何时候都对得上。
func (s *Scheduler) ConfigureGroup(cfg GroupConfig) error {
	if cfg.Name == DefaultGroup {
		return fmt.Errorf("scheduler: default group is built-in and cannot be reconfigured")
	}
	s.mu.Lock()
	err := s.registry.configure(cfg)
	s.mu.Unlock()
	if err == nil {
		s.cond.Broadcast()
	}
	return err
}

// UpdateLimits 运行期调整全局上限（并发/资源/队列容量）。
// 缩容语义与组级一致：不杀在运行任务，仅按新上限冻结后续准入。
// 非法参数与 New 一样按编程错误 panic。
func (s *Scheduler) UpdateLimits(cfg Config) {
	if cfg.MaxConcurrency <= 0 {
		panic("scheduler: Config.MaxConcurrency must be > 0")
	}
	if cfg.ResourceLimit < 0 {
		panic("scheduler: Config.ResourceLimit must be >= 0")
	}
	s.mu.Lock()
	s.cfg = cfg
	s.queueCap = cfg.QueueCapacity
	s.quota.reconfigure(cfg.MaxConcurrency, cfg.ResourceLimit)
	snap := s.snapshotLocked()
	s.mu.Unlock()
	logDecision(s.log, "UPDATE-LIMITS", DefaultGroup, "-", 0,
		"global limits adjusted; running tasks are not evicted", snap)
	s.cond.Broadcast()
}

// WithLogger 注入决策日志器（须在提交任务前调用）。
func (s *Scheduler) WithLogger(l Logger) *Scheduler {
	if l != nil {
		s.log = l
	}
	return s
}

// Submit 申请准入一个任务，任务通过 Task.Group 声明所属业务组。
// 拒绝原因彼此可区分，任务绝不被静默丢弃：
//   - 调度器已关闭         => ReasonSchedulerStopped
//   - 组未注册             => ReasonUnknownGroup
//   - 该组已被 CloseGroup  => ReasonGroupStopped
//   - Cost > 全局硬上限    => ReasonExceedsHardLimit
//   - Cost > 本组硬上限    => ReasonGroupExceedsHardLimit
//   - 全局排队区已满       => ReasonQueueFull
//   - 其余情况排队等待（Handle.BlockReason 记录被哪个维度挡住）；
//     若当前组账与全局账都可立即满足，则直接准入。
func (s *Scheduler) Submit(parent context.Context, t *Task) (*Handle, error) {
	if t == nil || t.Fn == nil {
		return nil, fmt.Errorf("scheduler: task and task.Fn must not be nil")
	}
	if t.Cost < 0 {
		return nil, fmt.Errorf("scheduler: task %q has negative cost", t.ID)
	}
	groupName := t.Group
	if groupName == DefaultGroup {
		groupName = DefaultGroup // 显式归一，便于日志与账目
	}

	ctx, cancel := context.WithCancel(parent)
	h := newHandle(t, ctx)
	h.bindCancel(cancel)

	s.mu.Lock()
	g := s.registry.get(groupName)
	h.group = groupName
	switch {
	case s.stopped:
		h.finish(StatusRejected, reasonError(t.ID, groupName, ReasonSchedulerStopped, "scheduler is shutting down"))
		snap := s.snapshotLocked()
		s.mu.Unlock()
		logDecision(s.log, "REJECT", groupName, t.ID, t.Cost, "scheduler stopped", snap)
		return h, h.Err()
	case g == nil:
		h.finish(StatusRejected, reasonError(t.ID, groupName, ReasonUnknownGroup,
			fmt.Sprintf("group %q is not registered; call ConfigureGroup first", groupName)))
		snap := s.snapshotLocked()
		s.mu.Unlock()
		logDecision(s.log, "REJECT", groupName, t.ID, t.Cost, "unknown group", snap)
		return h, h.Err()
	case g.stopped:
		h.finish(StatusRejected, reasonError(t.ID, groupName, ReasonGroupStopped,
			fmt.Sprintf("group %q is closed (CloseGroup); other groups unaffected", groupName)))
		snap := s.snapshotLocked()
		s.mu.Unlock()
		logDecision(s.log, "REJECT", groupName, t.ID, t.Cost, "group stopped", snap)
		return h, h.Err()
	case t.Cost > s.cfg.ResourceLimit:
		h.finish(StatusRejected, reasonError(t.ID, groupName, ReasonExceedsHardLimit,
			fmt.Sprintf("cost %d > global resource limit %d", t.Cost, s.cfg.ResourceLimit)))
		snap := s.snapshotLocked()
		s.mu.Unlock()
		logDecision(s.log, "REJECT", groupName, t.ID, t.Cost, "cost exceeds global hard limit", snap)
		return h, h.Err()
	case g.resourceLimit != unlimitedResource && t.Cost > g.resourceLimit:
		h.finish(StatusRejected, reasonError(t.ID, groupName, ReasonGroupExceedsHardLimit,
			fmt.Sprintf("cost %d > group %q resource limit %d", t.Cost, groupName, g.resourceLimit)))
		snap := s.snapshotLocked()
		s.mu.Unlock()
		logDecision(s.log, "REJECT", groupName, t.ID, t.Cost, "cost exceeds group hard limit", snap)
		return h, h.Err()
	case s.queueCap > 0 && s.registry.totalQueued() >= s.queueCap:
		h.finish(StatusRejected, reasonError(t.ID, groupName, ReasonQueueFull,
			fmt.Sprintf("global waiting queue at capacity %d", s.queueCap)))
		snap := s.snapshotLocked()
		s.mu.Unlock()
		logDecision(s.log, "REJECT", groupName, t.ID, t.Cost, "global queue full", snap)
		return h, h.Err()
	}

	if !s.paused && !s.dispatchFrozen && g.canAdmit(t.Cost) && s.quota.canAdmit(t.Cost) {
		// 立即准入路径：组账与全局账在同一临界区内一起记账，口径与
		// 排队准入完全一致。wg.Add 在 mu 内完成，先于任何 Wait。
		s.seq++
		s.quota.admit(t.Cost)
		g.admit(h, t.Cost)
		s.wg.Add(1)
		s.fireAdmitLocked(t.ID)
		snap := s.snapshotLocked()
		s.mu.Unlock()
		logDecision(s.log, "ADMIT", groupName, t.ID, t.Cost,
			"submit: group & global concurrency/resource all available", snap)
		s.launch(h, t)
		return h, nil
	}

	// 排队：记录它究竟被哪个维度挡住，方便复盘；任务只会排队，不会丢。
	s.seq++
	seq := s.seq
	h.setBlockReason(s.classifyBlockLocked(g, t.Cost))
	g.queue.push(h, groupName, t.Cost, t.Priority, seq)
	snap := s.snapshotLocked()
	paused := s.paused
	block := h.BlockReason()
	s.mu.Unlock()
	logDecision(s.log, "ENQUEUE", groupName, t.ID, t.Cost,
		fmt.Sprintf("paused=%t block=%s priority=%d seq=%d", paused, block, t.Priority, seq), snap)
	s.cond.Signal()

	// 排队期间 ctx 被取消（调用方取消 / Cancel / Shutdown / CloseGroup）：
	// 用 context.AfterFunc 注册一次性回调，任务结算时显式注销——
	// 长期运行下监听协程数量随"在排队任务数"而非"累计提交任务数"变化。
	stop := context.AfterFunc(h.ctx, func() { s.waitQueuedCancel(h) })
	h.bindWatch(stop)
	return h, nil
}

// classifyBlockLocked 依据当前组账与全局账给出排队阻塞维度（mu 内调用）。
// 组账优先判：本组预算不够时，即便全局也紧张，也明确记为"本组"原因。
func (s *Scheduler) classifyBlockLocked(g *groupState, cost int64) BlockReason {
	if g.maxConcurrency > 0 && g.inFlight >= g.maxConcurrency {
		return BlockGroupConcurrency
	}
	if g.resourceLimit != unlimitedResource && g.resourceUsed+cost > g.resourceLimit {
		return BlockGroupResource
	}
	if s.quota.inFlight >= s.quota.maxConcurrency {
		return BlockGlobalConcurrency
	}
	if s.quota.resourceUsed+cost > s.quota.resourceLimit {
		return BlockGlobalResource
	}
	return BlockNone // 暂停 / 冻结等非容量原因
}

// Pause 暂停派发：已在运行的任务不受影响，新提交与已排队任务一律等待。
func (s *Scheduler) Pause() {
	s.mu.Lock()
	s.paused = true
	snap := s.snapshotLocked()
	s.mu.Unlock()
	logDecision(s.log, "PAUSE", DefaultGroup, "-", 0, "manual pause", snap)
}

// Resume 恢复派发并唤醒派发循环。
func (s *Scheduler) Resume() {
	s.mu.Lock()
	s.paused = false
	snap := s.snapshotLocked()
	s.mu.Unlock()
	logDecision(s.log, "RESUME", DefaultGroup, "-", 0, "manual resume", snap)
	s.cond.Broadcast()
}

// CloseGroup 只关闭单个业务组，其他组不受影响：
//   - 该组立即停收新提交（ReasonGroupStopped）；
//   - 清空该组排队任务并结算为 CANCELED（它们从未占用资源）；
//   - 取消该组全部在运行任务的 ctx，请求它们退出；
//   - 等待在运行任务结束，或 ctx 到期（ctx 的 Deadline 即"最大等待时间"）；
//   - 超过最大等待仍不理会取消的任务：其容量被强制收回并标记 ABANDONED，
//     调度器（含本组之外的所有组）可继续服务，账目不重复释放、不为负。
func (s *Scheduler) CloseGroup(name string, ctx context.Context) error {
	s.mu.Lock()
	g := s.registry.get(name)
	if g == nil {
		s.mu.Unlock()
		return fmt.Errorf("scheduler: unknown group %q", name)
	}
	running := s.stopGroupLocked(g, "close-group")
	s.mu.Unlock()
	s.cond.Broadcast()

	for _, h := range running {
		h.Cancel()
	}
	return s.waitGroupSettle(g, running, ctx)
}

// stopGroupLocked 把一个组标记停收、摘空其队列并返回在运行句柄列表。
func (s *Scheduler) stopGroupLocked(g *groupState, basis string) []*Handle {
	g.stopped = true
	for _, e := range g.queue.drain() {
		e.handle.cancel()
		e.handle.finish(StatusCanceled, ErrTaskCanceled)
		logDecision(s.log, "CANCEL-GROUP", g.name, e.handle.task.ID, e.cost,
			basis+": queued task canceled; never consumed quota", s.snapshotLocked())
	}
	running := make([]*Handle, 0, len(g.running))
	for h := range g.running {
		running = append(running, h)
	}
	return running
}

// waitGroupSettle 等待组内在运行任务全部结算；ctx 到期则回收剩余容量。
func (s *Scheduler) waitGroupSettle(g *groupState, running []*Handle, ctx context.Context) error {
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
		s.log.Logf("DECISION action=GROUP-CLOSE-DONE group=%s task=- cost=0 basis=\"all running tasks settled\" | %s",
			g.name, s.Snapshot())
		return nil
	case <-ctx.Done():
		// 超过最大等待：把仍未退出（未脱管）任务的容量收回。
		s.mu.Lock()
		for _, h := range running {
			if !h.detached {
				s.abandonLocked(h, g, "group close max wait exceeded; capacity reclaimed")
			}
		}
		s.mu.Unlock()
		s.cond.Broadcast()
		return ctx.Err()
	}
}

// Shutdown 全局优雅关闭：所有组停收、取消全部排队与在运行任务，等待它们
// 结束或 ctx 到期。ctx 的 Deadline 即最大等待时间：超时后仍卡住的任务
// 被强制收回容量并标记 ABANDONED，账目归零，组件可被复用/重启。
func (s *Scheduler) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	var running []*Handle
	for _, name := range s.registry.names() {
		g := s.registry.get(name)
		running = append(running, s.stopGroupLocked(g, "shutdown")...)
	}
	s.mu.Unlock()
	s.cond.Broadcast() // 唤醒派发循环使其观察到 stopped 并退出

	for _, h := range running {
		h.Cancel()
	}

	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		s.log.Logf("DECISION action=SHUTDOWN-DONE task=- cost=0 basis=\"all running tasks settled\" | %s", s.Snapshot())
		return nil
	case <-ctx.Done():
		// 超过最大等待仍有任务卡死：逐个收回它们仍占着的容量。
		s.mu.Lock()
		for _, h := range running {
			if !h.detached {
				s.abandonLocked(h, s.registry.get(h.group),
					"shutdown max wait exceeded; capacity reclaimed")
			}
		}
		s.mu.Unlock()
		s.cond.Broadcast()
		return ctx.Err()
	}
}

// Snapshot 返回当前账目（全局 + 各组）的一致快照。
func (s *Scheduler) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *Scheduler) snapshotLocked() Snapshot {
	snap := s.quota.snapshot()
	snap.Queued = s.registry.totalQueued()
	snap.Paused = s.paused
	snap.Stopped = s.stopped
	groups := make(map[string]GroupSnapshot, len(s.registry.order))
	for _, name := range s.registry.names() {
		groups[name] = s.registry.get(name).snapshot()
	}
	snap.Groups = groups
	return snap
}

// waitQueuedCancel 监听排队任务的取消信号；若任务先被派发取走（出队），
// 摘除失败即为正常情况，结算完全交给运行路径，保证结算只发生一次。
func (s *Scheduler) waitQueuedCancel(h *Handle) {
	s.mu.Lock()
	g := s.registry.get(h.group)
	if g == nil || !g.queue.remove(h) {
		s.mu.Unlock()
		return
	}
	if h.finish(StatusCanceled, ErrTaskCanceled) {
		snap := s.snapshotLocked()
		s.mu.Unlock()
		logDecision(s.log, "CANCEL-QUEUED", h.group, h.task.ID, h.task.Cost,
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
	e := s.registry.popEligible(s.quota.canAdmit)
	if e == nil {
		return nil, nil, 0
	}
	s.admitEntryLocked(e)
	return e.handle, e.handle.task, e.cost
}

// admitEntryLocked 在 mu 内对一个已出队条目做全局账 + 组账的统一记账。
func (s *Scheduler) admitEntryLocked(e *waitEntry) {
	s.quota.admit(e.cost)
	s.registry.get(e.group).admit(e.handle, e.cost)
	s.wg.Add(1)
	s.fireAdmitLocked(e.handle.task.ID)
}

// dispatchLoop 是唯一的派发者：串行地从各组等待队列挑选可准入任务。
// 组间严格轮转（registry 维护游标）+ 组内全确定排序，是相同输入下
// 调度顺序可复现、且组间不会互相饿死的关键。
func (s *Scheduler) dispatchLoop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		// 1) 锁内收集本批全部"当前可满足"的任务并记账。全程不释放 mu，
		//    "谓词检查 -> cond.Wait"之间不存在缝隙，杜绝漏唤醒。
		type admitted struct {
			h     *Handle
			t     *Task
			cost  int64
			group string
			eff   int64
			seq   uint64
			snap  Snapshot
		}
		var batch []admitted
		for !s.stopped && !s.paused && !s.dispatchFrozen {
			e := s.registry.popEligible(s.quota.canAdmit)
			if e == nil {
				break
			}
			s.admitEntryLocked(e)
			batch = append(batch, admitted{
				h: e.handle, t: e.handle.task, cost: e.cost, group: e.group,
				eff: e.effectivePriority(), seq: e.seq, snap: s.snapshotLocked(),
			})
		}
		// 2) 锁外统一启动本批 goroutine。
		if len(batch) > 0 {
			s.mu.Unlock()
			for _, a := range batch {
				logDecision(s.log, "ADMIT", a.group, a.t.ID, a.cost,
					fmt.Sprintf("dispatch: group=%s effective-priority=%d seq=%d fits group & global quota",
						a.group, a.eff, a.seq), a.snap)
				s.launch(a.h, a.t)
			}
			s.mu.Lock()
		}
		if s.stopped {
			return
		}
		// 3) 无法继续派发：对所有组的等待者做一次老化（每次真正进入等待
		//    计一 tick），组内老任务的有效优先级终将反超后来的高优任务，
		//    但不会突破组间轮转边界。冻结态（确定性测试）不计时。
		if !s.paused && !s.dispatchFrozen && s.registry.totalQueued() > 0 {
			maxAge := s.registry.ageAll()
			if maxAge > 0 && maxAge%16 == 0 {
				s.log.Logf("STARVATION-WATCH max-wait-ticks=%d | %s", maxAge, s.snapshotLocked())
			}
		}
		// 4) 冻结态直接等待在 cond 上；正常态在谓词循环中等待，
		//    醒来后重新检查，杜绝漏唤醒。
		for s.dispatchFrozen || !s.canProgress() {
			if s.stopped {
				return
			}
			s.cond.Wait()
		}
	}
}

// canProgress 报告派发者此刻是否应被唤醒：
// 已停止（需退出）、或非暂停/未冻结且存在组账与全局账都可满足的任务。
func (s *Scheduler) canProgress() bool {
	if s.stopped {
		return true
	}
	return !s.paused && !s.dispatchFrozen && s.registry.popProbe(s.quota.canAdmit)
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

// run 是单个任务的全部执行与结算路径，与任务 goroutine 一一对应。
// 注意：wg.Done / 组账与全局账的释放都在 settle（或 abandon）中完成，
// 正常退出与"超时被放弃"两条路径恰好释放一次，因此这里不做 defer Done。
func (s *Scheduler) run(h *Handle, t *Task) {
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
				s.log.Logf("PANIC-ISOLATED group=%s task=%s recover=%v stack=%s",
					h.group, t.ID, r, debug.Stack())
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

// settle 完成"终态登记 + 两本账释放"，仅由任务自身 goroutine 调用一次。
//
// 零时间窗约定：终态（close Done）与容量归还处于同一个 mu 临界区、且
// 容量归还先于终态可见。下游任务一旦看到上游的结果（Wait/Done 返回），
// 空出来的全局与组容量必然已经在账上可见并已唤醒派发者，不会出现
// "上游有结果了、下游却因容量没还回来而干等"的空窗。
func (s *Scheduler) settle(h *Handle, t *Task, status Status, err error) {
	s.mu.Lock()
	g := s.registry.get(h.group)
	if h.detached {
		// 已在关闭流程中被放弃：容量早已收回，这里只摘掉僵尸登记，
		// 绝不再动账目，杜绝重复释放与负数。
		if g != nil {
			g.zombieDone(h)
		}
		h.finish(status, err) // 幂等：ABANDONED 终态已登记，此次不覆盖
		s.mu.Unlock()
		return
	}
	// 先归还容量（全局账 + 组账 + 等待计数），再登记终态，全程持锁。
	s.quota.release(t.Cost)
	g.release(h, t.Cost)
	s.wg.Done()
	h.finish(status, err)
	snap := s.snapshotLocked()
	if s.onTerminal != nil {
		s.onTerminal(t.ID, status)
	}
	if s.settleBarrier != nil {
		s.settleBarrier(t.ID)
	}
	s.mu.Unlock()
	logDecision(s.log, "RELEASE", h.group, t.ID, t.Cost,
		fmt.Sprintf("terminal=%s err=%v; group & global quota returned exactly once", status, err), snap)
	s.cond.Signal()
	h.endWatch()
}

// abandonLocked 在 mu 内放弃一个超过最大等待仍不退出的在运行任务：
// 收回它在全局账与组账上的容量并登记 ABANDONED 终态。每个任务至多一次；
// 其 goroutine 日后退出时走 settle 的 detached 分支，只摘登记、不碰账。
//
// 关键不变式：只有"赢得终态登记"（h.finish 返回 true）的路径才允许动账。
// 关闭流程枚举的是关闭时刻的在运行快照，其中可能已有任务在等待窗口内
// 正常结算（容量已由 settle 归还）——对这类任务 finish 必然返回 false，
// 必须原样跳过，否则会把同一笔容量归还第二次，把账目压成负数。
func (s *Scheduler) abandonLocked(h *Handle, g *groupState, basis string) {
	if h.detached {
		return
	}
	if !h.finish(StatusAbandoned, ErrTaskAbandoned) {
		// 该任务已在等待窗口内正常结算（或已被他途结算）：容量早已归还，
		// 这里绝不能再动账。
		return
	}
	h.detached = true
	s.quota.release(h.task.Cost)
	g.abandon(h, h.task.Cost)
	s.wg.Done()
	if s.onTerminal != nil {
		s.onTerminal(h.task.ID, StatusAbandoned)
	}
	logDecision(s.log, "RELEASE-ABANDON", g.name, h.task.ID, h.task.Cost,
		basis+"; task detached, future exit will not touch accounts", s.snapshotLocked())
	h.endWatch()
}
