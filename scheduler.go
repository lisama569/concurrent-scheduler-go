package scheduler

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
)

// Config 声明调度器的硬约束：
//   - MaxConcurrency：同时运行任务数的全局上限（并发度配额），必须 >0；
//   - ResourceLimit：所有运行中任务 Cost 之和的全局上限（总资源配额），<0 无界；
//   - QueueCapacity：全局等待队列容量，<=0 表示无界；
//   - Groups：各业务组的独立预算。空串默认组始终存在（无组级上限），
//     未在此列出（且名字非空）的组在 Submit 时会被显式拒绝。
type Config struct {
	MaxConcurrency int
	ResourceLimit  int64
	QueueCapacity  int
	Groups         []GroupConfig
}

// charge 是一次准入的计费票据：每个被准入的任务恰好一张，
// 是"资源精确释放一次"的凭据。任务自身退出与关闭期强制回收两条路径
// 竞争同一张票据，靠 released 标志（mu 内）去重。
type charge struct {
	id     string
	group  string
	cost   int64
	handle *Handle
	// cancel 为该任务派生上下文的取消函数。无论任务以何种终态结束，
	// 任务执行路径（run）退出时都会调用它，避免在父 ctx 长期存活时
	// 泄漏派生 context 与其在父取消链上的注册。
	cancel context.CancelFunc
	freed  bool // 仅在 Scheduler.mu 下读写
}

// Scheduler 是进程内的并发任务调度器（支持多业务组隔离）。
//
// 同步模型：所有账目变更与队列操作都在唯一的 mu 临界区内完成；
// 任务在独立 goroutine 中执行，但"准入记账"与"结算释放"均回到 mu 下进行，
// 从而保证：检查与记账之间不存在 TOCTOU 窗口、每个任务的资源恰好释放一次。
type Scheduler struct {
	cfg Config
	log Logger

	mu       sync.Mutex
	cond     *sync.Cond // 与 mu 联动，唤醒唯一的 dispatch 循环
	global   *quota
	groups   map[string]*groupQuota
	order    []string // 组注册顺序，组间轮转（RR）的确定性次序
	queue    *waitQueue
	charges  map[*charge]struct{}
	rrCursor string // 上一个拿到槽位的组，RR 从其后开始
	seq      uint64
	paused   bool
	stopped  bool
	wg       sync.WaitGroup // 追踪所有已准入票据，供优雅关闭等待

	// onAdmit / onTerminal 为可选测试钩子，用于可复现性测试采集准入与
	// 终态序列；生产路径保持为 nil。
	onAdmit    func(id string, group string)
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
	if cfg.ResourceLimit < 0 && cfg.ResourceLimit != -1 {
		panic("scheduler: Config.ResourceLimit must be >= 0 or -1 (unlimited)")
	}
	s := &Scheduler{
		cfg:     cfg,
		log:     nopLogger{},
		global:  newQuota(cfg.MaxConcurrency, cfg.ResourceLimit),
		groups:  map[string]*groupQuota{},
		queue:   newWaitQueue(cfg.QueueCapacity),
		charges: map[*charge]struct{}{},
	}
	// 空串默认组始终存在且不设组级预算：单池用法零改动即可兼容。
	s.registerGroupLocked(GroupConfig{Name: "", MaxConcurrency: 0, ResourceLimit: -1})
	for _, g := range cfg.Groups {
		if g.Name == "" {
			// 显式配置了名为空串的组：视作对默认组设预算（允许）。
			s.groups[""].q.setLimits(g.MaxConcurrency, g.ResourceLimit)
			continue
		}
		if err := s.registerGroupLocked(g); err != nil {
			panic(err)
		}
	}
	s.cond = sync.NewCond(&s.mu)
	go s.dispatchLoop()
	return s
}

// registerGroupLocked 注册一个新业务组。重名是配置错误。
func (s *Scheduler) registerGroupLocked(g GroupConfig) error {
	if g.ResourceLimit < 0 && g.ResourceLimit != -1 {
		return fmt.Errorf("scheduler: group %q ResourceLimit must be >= 0 or -1", g.Name)
	}
	if _, exists := s.groups[g.Name]; exists {
		return fmt.Errorf("scheduler: duplicate group %q in config", g.Name)
	}
	s.groups[g.Name] = newGroupQuota(g)
	s.order = append(s.order, g.Name)
	return nil
}

// WithLogger 注入决策日志器（须在提交任务前调用）。
func (s *Scheduler) WithLogger(l Logger) *Scheduler {
	if l != nil {
		s.log = l
	}
	return s
}

// Submit 申请准入一个任务。按顺序显式判定，绝不静默丢弃：
//   - 调度器已关闭         => ReasonSchedulerStopped
//   - 组未注册             => ReasonUnknownGroup
//   - 该组已单独关闭       => ReasonGroupStopped
//   - Cost > 全局硬上限    => ReasonExceedsHardLimit（永远满足不了，立即拒绝）
//   - Cost > 组硬上限      => ReasonExceedsGroupLimit
//   - 全局排队区已满       => ReasonQueueFull
//   - 其余排队或立即准入；入队原因可用 Handle.WaitReason 区分是
//     "本组预算不够"还是"全局预算不够"。
//
// 为保证组间公平与"排空不空窗"，只有当排队区为空时 Submit 才走立即准入；
// 排队区非空时一律入队，由唯一派发循环按组间轮转派发。
func (s *Scheduler) Submit(parent context.Context, t *Task) (*Handle, error) {
	if t == nil || t.Fn == nil {
		return nil, fmt.Errorf("scheduler: task and task.Fn must not be nil")
	}
	if t.Cost < 0 {
		return nil, fmt.Errorf("scheduler: task %q has negative cost", t.ID)
	}

	ctx, cancel := context.WithCancel(parent)
	h := newHandle(t, ctx)
	h.group = t.Group
	h.bindCancel(cancel)

	s.mu.Lock()
	gq, known := s.groups[t.Group]
	switch {
	case s.stopped:
		s.rejectLocked(h, t, ReasonSchedulerStopped, "scheduler is shutting down")
		s.mu.Unlock()
		cancel()
		logDecision(s.log, "REJECT", t.Group, t.ID, t.Cost, "scheduler stopped", s.snapshotLocked())
		return h, h.Err()
	case !known:
		s.rejectLocked(h, t, ReasonUnknownGroup,
			fmt.Sprintf("group %q is not registered", t.Group))
		s.mu.Unlock()
		cancel()
		logDecision(s.log, "REJECT", t.Group, t.ID, t.Cost, "unknown group", s.snapshotLocked())
		return h, h.Err()
	case gq.stopped:
		s.rejectLocked(h, t, ReasonGroupStopped,
			fmt.Sprintf("group %q is shut down", t.Group))
		s.mu.Unlock()
		cancel()
		logDecision(s.log, "REJECT", t.Group, t.ID, t.Cost, "group stopped", s.snapshotLocked())
		return h, h.Err()
	case s.global.resourceLimit >= 0 && t.Cost > s.global.resourceLimit:
		s.rejectLocked(h, t, ReasonExceedsHardLimit,
			fmt.Sprintf("cost %d > global resource limit %d", t.Cost, s.global.resourceLimit))
		s.mu.Unlock()
		cancel()
		logDecision(s.log, "REJECT", t.Group, t.ID, t.Cost, "cost exceeds global hard limit", s.snapshotLocked())
		return h, h.Err()
	case gq.q.resourceLimit >= 0 && t.Cost > gq.q.resourceLimit:
		s.rejectLocked(h, t, ReasonExceedsGroupLimit,
			fmt.Sprintf("cost %d > group %q resource limit %d", t.Cost, t.Group, gq.q.resourceLimit))
		s.mu.Unlock()
		cancel()
		logDecision(s.log, "REJECT", t.Group, t.ID, t.Cost, "cost exceeds group hard limit", s.snapshotLocked())
		return h, h.Err()
	case s.queue.full():
		s.rejectLocked(h, t, ReasonQueueFull,
			fmt.Sprintf("global waiting queue at capacity %d", s.cfg.QueueCapacity))
		s.mu.Unlock()
		cancel()
		logDecision(s.log, "REJECT", t.Group, t.ID, t.Cost, "queue full", s.snapshotLocked())
		return h, h.Err()
	}

	// 立即准入路径：仅当排队区已排空（保证有界队列背压与组间公平）且
	// 组、全局两层预算当前都够。与排队准入共用同一段记账逻辑，口径一致。
	if c := s.tryFastAdmitLocked(t, h); c != nil {
		snap := s.snapshotLocked()
		s.mu.Unlock()
		logDecision(s.log, "ADMIT", t.Group, t.ID, t.Cost,
			"submit: queue drained and group+global budget available", snap)
		s.launch(h, t, c)
		return h, nil
	}

	s.seq++
	seq := s.seq
	wr := s.waitReasonLocked(gq, t.Cost)
	s.queue.push(h, t.Group, t.Cost, t.Priority, seq)
	h.setWaitReason(wr)
	snap := s.snapshotLocked()
	paused := s.paused
	s.mu.Unlock()
	logDecision(s.log, "ENQUEUE", t.Group, t.ID, t.Cost,
		fmt.Sprintf("wait-reason=%s paused=%t priority=%d seq=%d", wr, paused, t.Priority, seq), snap)
	s.cond.Signal()

	// 排队期间 ctx 被取消（调用方取消 / Cancel / 关闭摘队列）：摘除并结算。
	go s.waitQueuedCancel(h)
	return h, nil
}

// rejectLocked 在锁内把任务结算为 REJECTED（拒绝任务从不记账、不入队）。
func (s *Scheduler) rejectLocked(h *Handle, t *Task, r RejectReason, detail string) {
	h.finish(StatusRejected, reasonError(t.ID, t.Group, r, detail))
}

// waitReasonLocked 判定任务若排队是被哪一层预算挡住：
// 先看本组（本组不够绝不可能放行），再看全局；都够则不是预算原因
// （暂停态，或排队区有等待者需按组间公平顺序派发）。
func (s *Scheduler) waitReasonLocked(gq *groupQuota, cost int64) WaitReason {
	if !gq.q.canAdmit(cost) {
		return WaitReasonGroupBudget
	}
	if !s.global.canAdmit(cost) {
		return WaitReasonGlobalBudget
	}
	return WaitReasonNone
}

// tryFastAdmitLocked 在"排队区为空、未暂停、未冻结"且两层预算都满足时
// 立即记账准入；否则返回 nil（交给排队路径）。
func (s *Scheduler) tryFastAdmitLocked(t *Task, h *Handle) *charge {
	if s.paused || s.dispatchFrozen || s.queue.len() != 0 {
		return nil
	}
	gq := s.groups[t.Group]
	if !gq.q.canAdmit(t.Cost) || !s.global.canAdmit(t.Cost) {
		return nil
	}
	return s.admitLocked(t.Group, t.Cost, h)
}

// Pause 暂停派发：已在运行的任务不受影响，新提交与已排队任务一律等待。
func (s *Scheduler) Pause() {
	s.mu.Lock()
	s.paused = true
	snap := s.snapshotLocked()
	s.mu.Unlock()
	logDecision(s.log, "PAUSE", "", "-", 0, "manual pause", snap)
}

// Resume 恢复派发并唤醒派发循环。
func (s *Scheduler) Resume() {
	s.mu.Lock()
	s.paused = false
	snap := s.snapshotLocked()
	s.mu.Unlock()
	logDecision(s.log, "RESUME", "", "-", 0, "manual resume", snap)
	s.cond.Broadcast()
}

// SetGlobalLimits 在运行期调整全局并发 / 资源上限。
// 扩容会唤醒派发循环，等待中的任务立刻有机会继续；
// 缩容不抢占在跑任务（账目允许 used 暂时高于新上限，自然排空），
// 且排空期间不会再放任何新任务把新上限顶穿。
func (s *Scheduler) SetGlobalLimits(maxConcurrency int, resourceLimit int64) error {
	if maxConcurrency <= 0 {
		return fmt.Errorf("scheduler: MaxConcurrency must be > 0, got %d", maxConcurrency)
	}
	if resourceLimit < 0 && resourceLimit != -1 {
		return fmt.Errorf("scheduler: ResourceLimit must be >= 0 or -1, got %d", resourceLimit)
	}
	s.mu.Lock()
	s.cfg.MaxConcurrency = maxConcurrency
	s.cfg.ResourceLimit = resourceLimit
	s.global.setLimits(maxConcurrency, resourceLimit)
	snap := s.snapshotLocked()
	s.mu.Unlock()
	logDecision(s.log, "LIMITS-GLOBAL", "", "-", 0,
		fmt.Sprintf("global limits updated: concurrency=%d resource=%d", maxConcurrency, resourceLimit), snap)
	s.cond.Broadcast()
	return nil
}

// UpsertGroup 在运行期注册新业务组或更新已有组的预算。
// 新注册后该组任务立即可以提交；扩容后组内等待任务立刻继续；
// 已关闭的组不能通过本方法复活。
func (s *Scheduler) UpsertGroup(cfg GroupConfig) error {
	if cfg.ResourceLimit < 0 && cfg.ResourceLimit != -1 {
		return fmt.Errorf("scheduler: group %q ResourceLimit must be >= 0 or -1", cfg.Name)
	}
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return fmt.Errorf("scheduler: cannot upsert group %q after shutdown", cfg.Name)
	}
	if gq, ok := s.groups[cfg.Name]; ok {
		if gq.stopped {
			s.mu.Unlock()
			return fmt.Errorf("scheduler: group %q already shut down and cannot be reopened", cfg.Name)
		}
		gq.q.setLimits(cfg.MaxConcurrency, cfg.ResourceLimit)
	} else {
		if err := s.registerGroupLocked(cfg); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	snap := s.snapshotLocked()
	s.mu.Unlock()
	logDecision(s.log, "LIMITS-GROUP", cfg.Name, "-", 0,
		fmt.Sprintf("group upserted: concurrency=%d resource=%d", cfg.MaxConcurrency, cfg.ResourceLimit), snap)
	s.cond.Broadcast()
	return nil
}

// Snapshot 返回当前全局与各组账目的一致快照。
func (s *Scheduler) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *Scheduler) snapshotLocked() Snapshot {
	snap := Snapshot{
		MaxConcurrency: s.global.maxConcurrency,
		InFlight:       s.global.inFlight,
		ResourceLimit:  s.global.resourceLimit,
		ResourceUsed:   s.global.resourceUsed,
		Queued:         s.queue.len(),
		Paused:         s.paused,
		Stopped:        s.stopped,
	}
	snap.Groups = make([]GroupSnapshot, 0, len(s.order))
	for _, name := range s.order {
		gq := s.groups[name]
		snap.Groups = append(snap.Groups, GroupSnapshot{
			Name:           name,
			MaxConcurrency: gq.q.maxConcurrency,
			InFlight:       gq.q.inFlight,
			ResourceLimit:  gq.q.resourceLimit,
			ResourceUsed:   gq.q.resourceUsed,
			Queued:         s.queue.countByGroup(name),
			Stopped:        gq.stopped,
		})
	}
	return snap
}

// admitLocked 在锁内完成一次准入记账并返回计费票据。
// 调用前必须已确认该组与全局两层预算均满足。
func (s *Scheduler) admitLocked(group string, cost int64, h *Handle) *charge {
	gq := s.groups[group]
	c := &charge{
		id: h.task.ID, group: group, cost: cost, handle: h,
		cancel: h.cancelFunc(),
	}
	s.charges[c] = struct{}{}
	gq.q.admit(cost)
	s.global.admit(cost)
	s.wg.Add(1)
	gq.wg.Add(1)
	h.markRunning()
	s.fireAdmitLocked(h.task.ID, group)
	return c
}

// eligibleLocked 构造"某组内 cost 的任务此刻能否准入"的双层谓词。
func (s *Scheduler) eligibleLocked(group string, cost int64) bool {
	gq := s.groups[group]
	return gq.q.canAdmit(cost) && s.global.canAdmit(cost)
}

// waitQueuedCancel 监听排队任务的取消信号。任务一旦被准入（admitCh 关闭），
// 监听协程立即退出，结算与释放完全交给运行路径——这样大量已完成任务不会
// 长期堆积对应的监听协程。若先收到取消，则尝试摘除并结算：摘除失败
// （恰好与派发竞争、任务已出队）属正常，结算交给运行路径，只结算一次。
func (s *Scheduler) waitQueuedCancel(h *Handle) {
	select {
	case <-h.ctx.Done():
	case <-h.admitCh:
		return
	}
	s.mu.Lock()
	if !s.queue.remove(h) {
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

// dispatchOneLocked 仅供确定性测试：在 mu 内同步选出并记账一个可准入
// 任务，但不在锁内启动 goroutine，而是返回该任务与计费票据，由调用方
// 锁外 launch。无可准入任务时返回 nil。准入决策完全由调用线程串行驱动，
// 与 goroutine 调度时序完全解耦；组间轮转游标同样在此推进，确定性覆盖到分组层。
func (s *Scheduler) dispatchOneLocked() (*Handle, *Task, *charge) {
	if s.stopped || s.paused {
		return nil, nil, nil
	}
	e, next := s.queue.popEligibleRR(s.order, s.rrCursor, s.eligibleLocked)
	if e == nil {
		return nil, nil, nil
	}
	s.rrCursor = next
	c := s.admitLocked(e.group, e.cost, e.handle)
	return e.handle, e.handle.task, c
}

// dispatchLoop 是唯一的派发者：按组间轮转串行挑选可准入任务。
// 单一派发者 + 全确定性的排序规则，是相同输入下调度顺序可复现的关键。
func (s *Scheduler) dispatchLoop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		// 1) 锁内收集本批全部"当前可满足"的任务并记账，全程不释放 mu：
		//    "谓词检查 -> cond.Wait"之间不存在缝隙，settle 的 Signal
		//    不可能落在该缝隙里，从根本上杜绝漏唤醒与"容量已还但下游干等"。
		type admitted struct {
			h    *Handle
			t    *Task
			c    *charge
			eff  int64
			seq  uint64
			snap Snapshot
		}
		var batch []admitted
		for !s.stopped && !s.paused && !s.dispatchFrozen {
			e, next := s.queue.popEligibleRR(s.order, s.rrCursor, s.eligibleLocked)
			if e == nil {
				break
			}
			s.rrCursor = next
			c := s.admitLocked(e.group, e.cost, e.handle)
			batch = append(batch, admitted{
				h: e.handle, t: e.handle.task, c: c,
				eff: e.effectivePriority(), seq: e.seq, snap: s.snapshotLocked(),
			})
		}
		// 2) 锁外统一启动本批 goroutine（持锁启动没有必要，且会让立即
		//    结束的任务在 launch 阶段排队争锁）。
		if len(batch) > 0 {
			s.mu.Unlock()
			for _, a := range batch {
				logDecision(s.log, "ADMIT", a.c.group, a.t.ID, a.c.cost,
					fmt.Sprintf("dispatch: rr-after=%q effective-priority=%d seq=%d fits group+global quota",
						rrPrevOf(s.order, a.c.group), a.eff, a.seq), a.snap)
				s.launch(a.h, a.t, a.c)
			}
			s.mu.Lock()
		}
		if s.stopped {
			return
		}
		// 3) 无法继续派发：对所有等待者统一老化一次（每次真正空转计一
		//    tick）。组内低优先级老任务会被抬上来；能否出队仍由组间
		//    轮转裁决，组间公平与组内防饥饿两层互不干扰。冻结态不计时。
		if !s.paused && !s.dispatchFrozen && s.queue.len() > 0 {
			maxAge := s.queue.ageAll()
			if maxAge > 0 && maxAge%16 == 0 {
				s.log.Logf("STARVATION-WATCH max-wait-ticks=%d | %s", maxAge, s.snapshotLocked())
			}
		}
		// 4) 在谓词循环中等待，醒来后重新检查，杜绝漏唤醒。
		for s.dispatchFrozen || !s.canProgress() {
			if s.stopped {
				return
			}
			s.cond.Wait()
		}
	}
}

// rrPrevOf 仅供日志：返回 RR 序中某个组的前一个组名（即"本轮从谁之后轮转"）。
func rrPrevOf(order []string, cur string) string {
	for i, g := range order {
		if g == cur {
			if i == 0 {
				if len(order) > 1 {
					return order[len(order)-1]
				}
				return ""
			}
			return order[i-1]
		}
	}
	return ""
}

// canProgress 报告派发者此刻是否应被唤醒：
// 已停止（需退出）、或非暂停/未冻结且存在可立即满足的任务。
func (s *Scheduler) canProgress() bool {
	if s.stopped {
		return true
	}
	return !s.paused && !s.dispatchFrozen &&
		s.queue.probeEligibleRR(s.order, s.rrCursor, s.eligibleLocked)
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
func (s *Scheduler) launch(h *Handle, t *Task, c *charge) {
	go s.run(h, t, c)
}

// fireAdmitLocked 在 mu 内回调准入钩子。持锁触发保证该登记严格先于
// 任务可能的结算回调（onTerminal），确定性运行器据此建立"准入 -> 结算"偏序。
func (s *Scheduler) fireAdmitLocked(id, group string) {
	if s.onAdmit != nil {
		s.onAdmit(id, group)
	}
}

// run 是单个任务的全部执行与结算路径，与任务 goroutine、计费票据一一对应。
func (s *Scheduler) run(h *Handle, t *Task, c *charge) {
	// 任务执行路径退出时务必释放派生 context：取消是幂等的，即使
	// Shutdown/超时已经取消过也安全；这样父 ctx 长期存活时不会泄漏。
	defer c.cancel()

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
				s.log.Logf("PANIC-ISOLATED group=%s task=%s recover=%v stack=%s", c.group, t.ID, r, debug.Stack())
			}
		}()
		fnErr = t.Fn(runCtx)
	}()

	status, err := classify(t, fnErr, panicked, runCtx)
	s.settle(h, t, c, status, err)
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

// settle 完成"终态登记 + 账目释放"。仅由任务自身 goroutine 调用。
// 票据若已在关闭流程中被强制回收（任务无视取消一直卡到宽限期满），
// 此处即为迟到结算：不重复释放、不把账目做成负数。
func (s *Scheduler) settle(h *Handle, t *Task, c *charge, status Status, err error) {
	s.mu.Lock()
	if !s.freeChargeLocked(c) {
		// 容量已被关闭流程收回：任务是"放弃后才退出"的迟到者。
		late := s.stopped || s.groups[c.group].stopped
		s.mu.Unlock()
		// 关闭（全局或本组）已进入终态流程后，注入的测试日志器可能已随
		// 测试结束失效；迟到结算静默处理，绝不触碰它，避免数据竞争。
		if !late {
			s.log.Logf("RELEASE-LATE group=%s task=%s cost=%d basis=\"capacity already reclaimed as ABANDONED; no double release\"",
				c.group, t.ID, c.cost)
		}
		return
	}
	h.finish(status, err)
	snap := s.snapshotLocked()
	// 关闭流程进行中/完成后，用户任务仍可能恰好在此刻正常退出并归还容量。
	// 这类"收尾结算"不应再回调测试钩子或注入日志器（它们的生命周期可能
	// 已随测试结束），只做纯账目归还。
	late := s.stopped || s.groups[c.group].stopped
	if !late {
		if s.onTerminal != nil {
			s.onTerminal(t.ID, status)
		}
		if s.settleBarrier != nil {
			s.settleBarrier(t.ID)
		}
	}
	s.mu.Unlock()
	if !late {
		logDecision(s.log, "RELEASE", c.group, t.ID, c.cost,
			fmt.Sprintf("terminal=%s err=%v; quota returned exactly once", status, err), snap)
	}
	s.cond.Signal()
}

// freeChargeLocked 尝试归还票据对应的组+全局容量，并结清两个 WaitGroup。
// 返回 false 表示票据此前已被关闭流程回收（绝不能再释放第二次）。
func (s *Scheduler) freeChargeLocked(c *charge) bool {
	if c.freed {
		return false
	}
	c.freed = true
	s.groups[c.group].q.release(c.cost)
	s.global.release(c.cost)
	delete(s.charges, c)
	s.wg.Done()
	s.groups[c.group].wg.Done()
	return true
}

// waitWG 在独立 goroutine 中等待 WaitGroup 归零，先到 channel 先返回，
// 避免在 mu 内等待在运行任务（会死锁——settle 需要重新取锁）。
func waitWG(wg *sync.WaitGroup) <-chan struct{} {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	return done
}

// Shutdown 优雅关闭整个调度器：拒绝所有组的新任务、取消全部排队任务，
// 等待在运行任务结束或 ctx 到期。ctx 到期时，仍无视取消不退出的任务会被
// 标记为 ABANDONED 且容量被单方面收回（精确一次、账目归零），组件据此
// 可被丢弃并在同进程重新创建；这些卡住的 goroutine 一旦日后真正退出，
// 会走"迟到结算"分支而不会重复释放或出现负数。
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
		logDecision(s.log, "CANCEL-SHUTDOWN", e.group, e.handle.task.ID, e.cost,
			"queued task canceled by shutdown; never consumed quota", s.snapshotLocked())
	}
	s.queue.entries = nil
	// 所有组标记停收。
	for _, gq := range s.groups {
		gq.stopped = true
	}
	// 通知所有在运行任务应尽快退出（是否配合取决于任务函数）。
	for c := range s.charges {
		c.handle.cancel()
	}
	s.mu.Unlock()
	s.cond.Broadcast() // 唤醒派发循环使其观察到 stopped 并退出

	select {
	case <-waitWG(&s.wg):
		s.log.Logf("DECISION action=SHUTDOWN-DONE group=%s task=- cost=0 basis=\"all running tasks settled\" | %s",
			"", s.Snapshot())
		return nil
	case <-ctx.Done():
		s.reclaimAfterGrace(ctx.Err())
		return ctx.Err()
	}
}

// ShutdownGroup 只关闭单个业务组：该组立即停收新任务、其排队任务全部
// 取消并出队；其它组的提交与排队完全不受影响。随后等待该组在运行任务
// 结束或 ctx 到期；到期仍不退出的，容量被收回并标记 ABANDONED。
// 组关闭后不可复活（UpsertGroup 会返回错误）。
func (s *Scheduler) ShutdownGroup(ctx context.Context, group string) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	gq, ok := s.groups[group]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("scheduler: unknown group %q", group)
	}
	if gq.stopped {
		s.mu.Unlock()
		return nil
	}
	gq.stopped = true
	for _, e := range s.queue.removeAllByGroup(group) {
		e.handle.cancel()
		e.handle.finish(StatusCanceled, ErrTaskCanceled)
		logDecision(s.log, "CANCEL-GROUP-SHUTDOWN", group, e.handle.task.ID, e.cost,
			"queued task canceled by group shutdown; never consumed quota", s.snapshotLocked())
	}
	for c := range s.charges {
		if c.group == group {
			c.handle.cancel()
		}
	}
	s.mu.Unlock()
	s.cond.Broadcast() // 释放出的全局容量立刻可被其它组使用

	select {
	case <-waitWG(&gq.wg):
		s.log.Logf("DECISION action=GROUP-SHUTDOWN-DONE group=%q task=- cost=0 basis=\"all group running tasks settled\" | %s",
			group, s.Snapshot())
		return nil
	case <-ctx.Done():
		s.reclaimGroupAfterGrace(group, ctx.Err())
		return ctx.Err()
	}
}

// reclaimAfterGrace 全局宽限期满：强制收回所有仍占用容量的票据。
func (s *Scheduler) reclaimAfterGrace(cause error) {
	s.reclaimLocked("", cause)
}

// reclaimGroupAfterGrace 组宽限期满：只收回该组仍占用容量的票据。
func (s *Scheduler) reclaimGroupAfterGrace(group string, cause error) {
	s.reclaimLocked(group, cause)
}

// reclaimLocked 取消并收回在运行任务的容量。group 为空表示收回全部组。
// 每个票据经 freeChargeLocked 精确处理一次，因此账目不会出现负数或重复释放。
func (s *Scheduler) reclaimLocked(group string, cause error) {
	s.mu.Lock()
	var pending []*charge
	for c := range s.charges {
		if group == "" || c.group == group {
			pending = append(pending, c)
		}
	}
	// 按 ID 排序使放弃顺序确定，日志与终态序列可复现。
	sortChargesByID(pending)
	for _, c := range pending {
		if !s.freeChargeLocked(c) {
			continue
		}
		c.handle.finish(StatusAbandoned, ErrTaskAbandoned)
		if s.onTerminal != nil {
			s.onTerminal(c.id, StatusAbandoned)
		}
		if s.settleBarrier != nil {
			s.settleBarrier(c.id)
		}
		logDecision(s.log, "RECLAIM", c.group, c.id, c.cost,
			fmt.Sprintf("grace period elapsed (%v); capacity force-reclaimed, task marked ABANDONED", cause),
			s.snapshotLocked())
	}
	remaining := len(s.charges)
	s.mu.Unlock()
	if remaining != 0 {
		s.log.Logf("DECISION action=SHUTDOWN-GRACE-DONE group=%q basis=\"%d task(s) of other groups still running\"", group, remaining)
	}
	s.cond.Broadcast()
}

// sortChargesByID 使关闭期强制回收顺序按任务 ID 确定排列，保证
// 放弃序列在相同输入下可复现。
func sortChargesByID(cs []*charge) {
	sort.Slice(cs, func(i, j int) bool { return cs[i].id < cs[j].id })
}
