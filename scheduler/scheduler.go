package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	maxPriority     = 15
	defaultTick     = 5 * time.Millisecond
	defaultRunGrace = 25 * time.Millisecond
)

// 防饥饿参数默认值（年龄按已等待的 tick 数累计）。
const (
	DefaultStarvationAge = 16
)

// Config 构造调度器的配置。
type Config struct {
	Limits Limits
	// Tick dispatcher 周期性扫描间隔（排队超时判定、年龄增长）。
	Tick time.Duration
	// RunGrace 任务被取消/超时后，调度器等待任务体自行退出的宽限期；
	// 超过后账目先行释放并记为 runaway，保证账目不被失控任务长期阻塞。
	RunGrace time.Duration
	// StarvationAge 排队任务累计等待多少个 tick 后被临时提升到最高有效优先级。
	StarvationAge int64
	// Logger 事件日志；为 nil 时不记录。
	Logger EventLogger
}

// Scheduler 是进程内并发任务调度器。
type Scheduler struct {
	cfg    Config
	limits Limits
	clk    clock
	log    EventLogger

	mu     sync.Mutex
	ledger *ledger
	queue  *agingQueue

	nextID int64
	seq    int64
	// tasks 保存所有尚未进入终态的在管任务（排队中 + 运行中）。
	tasks map[int64]*task

	accepted     int64
	completed    int64
	rejected     int64
	canceledN    int64
	timedOutN    int64
	runawayN     int64
	panickedN    int64
	queueTimeout int64

	paused   bool
	shutting bool
	closed   bool

	// tickArmed 标记周期扫描计时器是否已注册（防止重复注册）。
	// 计时器触发后在 scan 中重新武装。
	tickArmed bool
	// lastBlockReason 记录上一轮"无法准入"的原因，用于在持续积压期间
	// 只在原因首次出现或发生变化时记录一次日志，避免每个 tick 刷屏。
	// 队列排空后复位为 ReasonNone。
	lastBlockReason RejectReason

	wakeCh chan chan struct{}
	doneCh chan struct{}
	dispWG sync.WaitGroup // 后台 dispatcher goroutine

	// virtual 为 true 时由虚拟引擎（virtual.go）单协程驱动，不启动后台回路。
	virtual  bool
	fake     *fakeClock
	runAlive map[int64]*task // 已释放账目但任务体尚未退出的失控任务（真实模式）
}

// New 创建并启动一个调度器。
func New(cfg Config) *Scheduler {
	s := newScheduler(cfg, realClock{}, false, nil)
	s.startRealLoop()
	return s
}

func newScheduler(cfg Config, clk clock, virtual bool, fc *fakeClock) *Scheduler {
	if cfg.Tick <= 0 {
		cfg.Tick = defaultTick
	}
	if cfg.RunGrace <= 0 {
		cfg.RunGrace = defaultRunGrace
	}
	if cfg.StarvationAge <= 0 {
		cfg.StarvationAge = DefaultStarvationAge
	}
	s := &Scheduler{
		cfg:      cfg,
		limits:   cfg.Limits,
		clk:      clk,
		log:      cfg.Logger,
		ledger:   newLedger(),
		queue:    newAgingQueue(cfg.StarvationAge),
		tasks:    map[int64]*task{},
		wakeCh:   make(chan chan struct{}, 1),
		doneCh:   make(chan struct{}),
		virtual:  virtual,
		fake:     fc,
		runAlive: map[int64]*task{},
	}
	return s
}

// Submit 提交一个任务。
// 准入阶段被拒绝时返回 *RejectError（可用 errors.Is 匹配具体原因）；
// 因并发度/资源暂时不足而无法立即运行的任务不会被丢弃，而是进入排队等待，
// 此时返回有效 Handle 且无错误。
func (s *Scheduler) Submit(spec TaskSpec) (Handle, error) {
	return s.submitLocked(spec, nil)
}

func (s *Scheduler) submitLocked(spec TaskSpec, t *task) (Handle, error) {
	if err := validateSpec(spec); err != nil {
		s.mu.Lock()
		s.rejected++
		s.emitLocked(EventReject, nil, ReasonInvalidSpec, err.Error())
		s.mu.Unlock()
		return nil, err
	}

	s.mu.Lock()

	if s.closed {
		err := rejectError(ReasonShuttingDown, "scheduler already closed")
		s.rejected++
		s.emitLocked(EventReject, nil, ReasonShuttingDown, err.Error())
		s.mu.Unlock()
		return nil, err
	}
	if s.shutting {
		err := rejectError(ReasonShuttingDown, "shutdown in progress")
		s.rejected++
		s.emitLocked(EventReject, nil, ReasonShuttingDown, err.Error())
		s.mu.Unlock()
		return nil, err
	}

	id := s.nextID
	s.nextID++
	now := s.clk.now()

	if t == nil {
		ctx, cancel := context.WithCancel(context.Background())
		t = &task{
			id:        id,
			name:      spec.Name,
			priority:  spec.Priority,
			cost:      spec.Cost,
			spec:      spec,
			ctx:       ctx,
			cancel:    cancel,
			heapIndex: -1,
			status:    StatusQueued,
			queuedAt:  now,
			done:      make(chan struct{}),
		}
	} else {
		t.id = id
		t.heapIndex = -1
		t.status = StatusQueued
		t.queuedAt = now
		t.done = make(chan struct{})
	}
	if spec.QueueTimeout > 0 {
		t.hasQueueDl = true
		t.queueDl = now.Add(spec.QueueTimeout)
	}

	// 任务自身开销超过总容量：永远无法容纳，直接拒绝，不排队。
	if !t.cost.Fits(s.limits) {
		err := rejectError(ReasonTaskTooLarge,
			"cost=%v exceeds capacity=%v", t.cost, s.limits)
		s.rejected++
		t.status = StatusRejected
		t.result = TaskResult{Status: StatusRejected, Err: err}
		s.emitLocked(EventReject, t, ReasonTaskTooLarge, err.Error())
		close(t.done)
		t.cancel()
		s.mu.Unlock()
		return &taskHandle{s: s, t: t}, err
	}

	s.tasks[t.id] = t
	s.queue.Push(t)
	detail := fmt.Sprintf("accepted into queue: priority=%d cost=%v queue=%d",
		t.priority, t.cost, s.queue.Len())
	s.emitLocked(EventQueue, t, ReasonNone, detail)
	h := &taskHandle{s: s, t: t}
	s.mu.Unlock()

	s.wakeAck()
	return h, nil
}

// validateSpec 检查任务规格合法性。
func validateSpec(spec TaskSpec) error {
	if spec.Fn == nil {
		return rejectError(ReasonInvalidSpec, "Fn is nil")
	}
	if !spec.Cost.NonNegative() {
		return rejectError(ReasonInvalidSpec, "cost contains negative value: %v", spec.Cost)
	}
	if spec.Priority < 0 || spec.Priority > maxPriority {
		return rejectError(ReasonInvalidSpec, "priority %d out of range [0,%d]",
			spec.Priority, maxPriority)
	}
	return nil
}

// Pause 暂停准入：已在运行的任务不受影响，排队任务继续等待。
func (s *Scheduler) Pause() {
	s.mu.Lock()
	s.paused = true
	snap := s.snapshotLocked()
	s.mu.Unlock()
	s.logEvent(Event{Type: EventShutdown, Decision: "paused: admission suspended", Snap: snap})
}

// Resume 恢复准入。
func (s *Scheduler) Resume() {
	s.mu.Lock()
	s.paused = false
	snap := s.snapshotLocked()
	s.mu.Unlock()
	s.logEvent(Event{Type: EventShutdown, Decision: "resumed: admission re-enabled", Snap: snap})
	s.wakeAck()
}

// Snapshot 返回当前资源与计数快照。
func (s *Scheduler) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

// Cancel 实现 Handle.Cancel：排队中则移出并结算，运行中则取消其 context。
func (s *Scheduler) cancelTask(t *task) bool {
	s.mu.Lock()
	if t.status.Terminal() {
		s.mu.Unlock()
		return false
	}
	if t.status == StatusQueued {
		s.queue.Remove(t)
		s.settleLocked(t, StatusCanceled, ErrCanceled, EventCancel,
			"user canceled while queued")
	} else if t.userCanceled {
		// 取消请求已发出：重复取消是空操作。
		s.mu.Unlock()
		return false
	} else {
		// 运行中：请求取消，资源等待任务体退出后释放。
		t.userCanceled = true
		t.cancel()
		s.emitLocked(EventCancel, t, ReasonNone, "user canceled while running; ctx canceled")
	}
	s.mu.Unlock()
	s.wakeAck()
	return true
}

// Shutdown 优雅关闭。
func (s *Scheduler) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.shutting = true
	queued := s.queue.Len()
	s.emitLocked(EventShutdown, nil, ReasonNone,
		fmt.Sprintf("shutdown begin: %d queued, %d running", queued, s.ledger.running))
	s.mu.Unlock()
	s.wakeAck()
	// 等待排队任务自然排空 + 在管任务结束；ctx 到期则强制取消剩余任务。
	select {
	case <-s.doneCh:
		// 正常排空：等待 dispatcher 收尾。
		if !s.virtual {
			s.dispWG.Wait()
		}
		return nil
	case <-ctx.Done():
	}

	s.mu.Lock()
	for _, t := range s.tasks {
		if !t.status.Terminal() {
			if t.status == StatusQueued {
				s.queue.Remove(t)
				s.settleLocked(t, StatusCanceled, ErrCanceled, EventCancel,
					"forced cancel on shutdown deadline")
				continue
			}
			t.userCanceled = true
			t.cancel()
			s.emitLocked(EventCancel, t, ReasonNone, "forced cancel on shutdown deadline")
		}
	}
	s.mu.Unlock()
	if s.virtual {
		// 虚拟模式由引擎继续推进时间，scan 负责最终关闭。
		return nil
	}
	s.wakeAck()
	// 在宽限期内等待账目自然排空（doneCh）；逾期则对仍在管任务先行释放账目。
	select {
	case <-s.doneCh:
		s.dispWG.Wait()
		return nil
	case <-time.After(s.cfg.RunGrace):
	}
	s.mu.Lock()
	// 仍未退出的运行任务：账目先行释放并终态化，避免 Shutdown 无限期阻塞。
	// 之后任务体若退出，finishTask 会因其已终态成为空操作，不会二次释放。
	for _, t := range s.tasks {
		if t.admitted && !t.released {
			t.runaway = true
			s.runawayN++
			s.canceledN++ // 关闭路径强制取消，终态为 canceled
			s.releaseLocked(t, EventRunaway,
				"task ignored cancellation; ledger released on shutdown timeout")
			t.status = StatusCanceled
			t.result = TaskResult{Status: StatusCanceled, Err: ErrCanceled}
			delete(s.tasks, t.id)
			close(t.done)
		}
	}
	remain := len(s.tasks)
	s.mu.Unlock()
	if remain == 0 {
		s.markClosed()
	}
	<-s.doneCh
	// dispatcher 一定随关闭退出；失控任务 goroutine 有意不等待（已脱离账目）。
	s.dispWG.Wait()
	return nil
}

// ---- 内部核心：调度扫描 ----

// scan 执行一轮完整扫描：到期处理 -> 年龄增长 -> 尽力准入。
// 调用时机：提交/取消/任务结束等事件后，以及 tick 计时器到期时。
// 必须在未持锁状态下调用（内部自行加锁）。
func (s *Scheduler) scan() {
	// 周期 tick 已触发/正在处理：先标记为未武装，扫描结束后在锁内重新武装。
	s.mu.Lock()
	s.tickArmed = false
	s.mu.Unlock()

	now := s.clk.now()

	// 阶段 1：排队超时的任务移出并结算。
	s.mu.Lock()
	var qtimeouts []*task
	for _, t := range s.queue.Expired(now) {
		if s.queue.Remove(t) {
			qtimeouts = append(qtimeouts, t)
		}
	}
	for _, t := range qtimeouts {
		s.settleLocked(t, StatusTimedOut, ErrTimeout, EventTimeout,
			fmt.Sprintf("queue deadline %s reached while waiting for admission",
				t.queueDl.Format("15:04:05.000")))
	}
	s.mu.Unlock()

	// 阶段 2：处理运行中任务的两个阶段（超时取消 / runaway 回收）。
	s.reapRunaways(now)

	// 阶段 3：年龄增长（防饥饿），然后尽力准入。
	// 年龄按"已等待的 tick 数"计算，真实时钟与虚拟时钟语义一致。
	s.mu.Lock()
	if s.queue.Len() > 0 {
		if n := s.queue.AgeAll(now, s.cfg.Tick); n > 0 {
			s.emitLocked(EventQueue, nil, ReasonNone,
				fmt.Sprintf("aging: %d starved task(s) boosted to priority %d",
					n, boostPriority))
		}
	}
	// 关闭期间仍允许已排队任务获得准入（优雅排空），只是不再接收新提交；
	// 暂停期间则完全不放行。
	if !s.paused {
		s.admitLoopLocked()
	}
	allDone := s.shutting && len(s.tasks) == 0
	if !allDone {
		if s.virtual {
			// 虚拟模式没有后台 goroutine：直接安排一个 tick 事件，
			// 由引擎的主循环在下个虚拟时刻触发 scan。
			if !s.tickArmed {
				s.tickArmed = true
				s.clk.afterFunc(s.cfg.Tick, func() { s.scan() })
			}
		} else {
			s.armTickLocked()
		}
	}
	s.mu.Unlock()

	if allDone {
		s.markClosed()
	}
}

// armTickLocked 在锁内确保周期扫描计时器已注册。
// 每个 tick 到期后的 scan 会先把 tickArmed 置回 false，
// 再由这里重新武装，因此 tick 以固定周期持续触发。调用方须持锁。
func (s *Scheduler) armTickLocked() {
	if s.virtual || s.tickArmed || s.closed {
		return
	}
	s.tickArmed = true
	s.clk.afterFunc(s.cfg.Tick, func() { s.scan() })
}

// reapRunaways 处理运行任务的超时与失控回收。
func (s *Scheduler) reapRunaways(now time.Time) {
	s.mu.Lock()
	var toCancel []*task
	var toReap []*task
	for _, t := range s.tasks {
		if t.status != StatusRunning || !t.started {
			continue
		}
		if !t.hasRunDl {
			continue
		}
		if !t.runDeadline.After(now) {
			if !t.ctxCancelled() {
				toCancel = append(toCancel, t)
			} else if !t.runDeadline.Add(s.cfg.RunGrace).After(now) {
				toReap = append(toReap, t)
			}
		}
	}
	for _, t := range toCancel {
		t.timedOut = true
		t.cancel()
		s.emitLocked(EventTimeout, t, ReasonNone,
			fmt.Sprintf("run timeout deadline=%s reached; ctx canceled, grace=%s",
				t.runDeadline.Format("15:04:05.000"), s.cfg.RunGrace))
	}
	for _, t := range toReap {
		if t.admitted && !t.released {
			t.runaway = true
			s.runawayN++
			s.timedOutN++ // runaway 是 timed_out 的失控子集（账目先行释放）
			s.releaseLocked(t, EventRunaway,
				"task did not exit within grace after timeout; ledger released first")
			// 标记终态并交还句柄；此后任务体若退出，finishTask 为空操作，
			// 不会二次释放。失控 goroutine 本身脱离账目，不影响其他任务。
			t.status = StatusTimedOut
			t.result = TaskResult{Status: StatusTimedOut, Err: ErrTimeout}
			delete(s.tasks, t.id)
			close(t.done)
		}
	}
	s.mu.Unlock()
}

// admitLoopLocked 从队列中反复选择可容纳的最高有效优先级任务并准入，
// 直到没有可立即运行的任务为止。调用方须持锁。
func (s *Scheduler) admitLoopLocked() {
	for {
		if s.queue.Len() == 0 {
			// 积压已排空，复位原因记忆，下次积压时重新记录。
			s.lastBlockReason = ReasonNone
			return
		}
		// 并发度是与三维资源独立的约束：满了立即停止本轮准入。
		if s.ledger.running >= s.limits.MaxTasks {
			s.logBlockedOnce(s.queue.head(), ReasonOverConcurrency,
				"not admitted: concurrency cap reached; remains queued")
			return
		}
		t := s.queue.PickEligible(s.ledger.inUse, s.limits)
		if t == nil {
			// 并发度有余量但没有任何任务放得下：资源维度不足。
			h := s.queue.head()
			s.logBlockedOnce(h, ReasonOverResource,
				fmt.Sprintf("not admitted: cost=%v unavailable against inUse=%v cap=(cpu=%d,mem=%d,io=%d); remains queued",
					h.cost, s.ledger.inUse, s.limits.MaxCPU, s.limits.MaxMemory, s.limits.MaxIO))
			return
		}
		s.queue.Remove(t)
		s.startLocked(t)
	}
}

// logBlockedOnce 在同一轮持续积压中只记录一次阻塞原因；原因变化时记录新原因。
func (s *Scheduler) logBlockedOnce(t *task, reason RejectReason, decision string) {
	if s.lastBlockReason != reason {
		s.lastBlockReason = reason
		s.emitLocked(EventQueue, t, reason, decision)
	}
}

// startLocked 把任务从"排队"切换为"运行"：记账 + 启动执行。
func (s *Scheduler) startLocked(t *task) {
	s.ledger.admit(t.cost)
	t.admitted = true
	t.status = StatusRunning
	t.started = true
	s.accepted++
	if t.spec.RunTimeout > 0 {
		t.hasRunDl = true
		t.runDeadline = s.clk.now().Add(t.spec.RunTimeout)
	}
	detail := fmt.Sprintf("admitted: priority=%d eff=%d cost=%v -> running=%d inUse=%v",
		t.priority, effPri(t), t.cost, s.ledger.running, s.ledger.inUse)
	s.emitLocked(EventAdmit, t, ReasonNone, detail)

	if s.virtual {
		s.startVirtual(t)
	} else {
		s.startReal(t)
	}
}

// settleLocked 终态结算排队中（未占用资源）的任务。调用方须持锁。
func (s *Scheduler) settleLocked(t *task, status Status, err error, ev EventType, decision string) {
	if t.status.Terminal() {
		return
	}
	t.status = status
	t.result = TaskResult{Status: status, Err: err}
	switch status {
	case StatusCanceled:
		s.canceledN++
	case StatusTimedOut:
		// settleLocked 只处理排队中任务，因此这里必为排队超时。
		s.queueTimeout++
	case StatusRejected:
		s.rejected++
	}
	if _, ok := s.tasks[t.id]; ok {
		delete(s.tasks, t.id)
	}
	s.emitLocked(ev, t, ReasonNone, decision)
	close(t.done)
}

// releaseLocked 精确一次释放资源账目。调用方须持锁。
func (s *Scheduler) releaseLocked(t *task, ev EventType, decision string) {
	if t.released || !t.admitted {
		return
	}
	s.ledger.release(t.cost)
	t.released = true
	if bad := s.ledger.overbooked(s.limits); bad != "" {
		// 账目不变量被破坏属于程序缺陷：保留详细日志并 panic 前输出快照。
		s.emitLocked(EventRelease, t, ReasonNone, "LEDGER INVARIANT BROKEN: "+bad)
		panic("scheduler ledger invariant broken: " + bad)
	}
	s.emitLocked(ev, t, ReasonNone,
		fmt.Sprintf("%s; released cost=%v -> running=%d inUse=%v",
			decision, t.cost, s.ledger.running, s.ledger.inUse))
}

// finishTask 任务体退出后的终态处理（任意协程调用）。
func (s *Scheduler) finishTask(t *task, status Status, err error) {
	s.mu.Lock()
	if t.status.Terminal() {
		// 已被 settle（仅排队任务，运行任务不会走到这）或重复回调。
		s.mu.Unlock()
		return
	}
	ev := EventFinish
	decision := ""
	switch status {
	case StatusSucceeded:
		s.completed++
		decision = "task succeeded"
	case StatusFailed:
		s.completed++
		decision = "task failed: " + errString(err)
	case StatusPanicked:
		s.panickedN++
		ev = EventPanic
		decision = "panic recovered and isolated: " + errString(err)
	case StatusCanceled:
		s.canceledN++
		ev = EventCancel
		decision = "task ended after cancellation"
	case StatusTimedOut:
		s.timedOutN++
		ev = EventTimeout
		decision = "task ended after timeout signal"
	}
	if t.admitted && !t.released {
		s.releaseLocked(t, EventRelease, decision)
	} else if t.runaway {
		s.emitLocked(ev, t, ReasonNone, decision+" (ledger had been released early)")
	}
	t.status = status
	t.result = TaskResult{Status: status, Err: err}
	delete(s.tasks, t.id)
	s.emitLocked(ev, t, ReasonNone, decision)
	closedNow := t.done
	allDone := s.shutting && len(s.tasks) == 0
	s.mu.Unlock()

	safeClose(closedNow)
	if allDone {
		s.markClosed()
	}
	if !s.virtual {
		s.wake()
	}
}

// ---- 日志与快照 ----

func (s *Scheduler) snapshotLocked() Snapshot {
	return Snapshot{
		InUse:         s.ledger.inUse,
		Running:       s.ledger.running,
		Queued:        s.queue.Len(),
		Accepted:      s.accepted,
		Completed:     s.completed,
		Rejected:      s.rejected,
		Canceled:      s.canceledN,
		TimedOut:      s.timedOutN,
		QueueTimedOut: s.queueTimeout,
		Runaway:       s.runawayN,
		Panicked:      s.panickedN,
		Shutting:      s.shutting,
		Paused:        s.paused,
	}
}

func (s *Scheduler) emitLocked(typ EventType, t *task, reason RejectReason, decision string) {
	if s.log == nil {
		return
	}
	s.seq++
	e := Event{
		Seq:      s.seq,
		Time:     s.clk.now(),
		Type:     typ,
		Reason:   reason,
		Decision: decision,
		Snap:     s.snapshotLocked(),
	}
	if t != nil {
		e.TaskID = t.id
		e.TaskName = t.name
		e.Priority = t.priority
		e.Cost = t.cost
	}
	s.log.Log(e)
}

func (s *Scheduler) logEvent(e Event) {
	if s.log == nil {
		return
	}
	s.mu.Lock()
	s.seq++
	e.Seq = s.seq
	if e.Time.IsZero() {
		e.Time = s.clk.now()
	}
	s.mu.Unlock()
	s.log.Log(e)
}

// ---- 小工具 ----

// wake 异步唤醒 dispatcher 做一轮扫描，不阻塞调用方。
func (s *Scheduler) wake() {
	select {
	case s.wakeCh <- nil:
	default:
	}
}

// wakeAck 同步唤醒并等待 dispatcher 完成一轮扫描。
// 调用前必须已释放 s.mu（dispatcher 扫描时需要获取同一把锁）。
func (s *Scheduler) wakeAck() {
	if s.virtual {
		return
	}
	ack := make(chan struct{})
	select {
	case s.wakeCh <- ack:
		<-ack
	case <-s.doneCh:
	}
}

func (s *Scheduler) markClosed() {
	s.mu.Lock()
	first := !s.closed
	if first {
		s.closed = true
	}
	s.mu.Unlock()
	if first {
		close(s.doneCh)
	}
}

func safeClose(ch chan struct{}) {
	defer func() { _ = recover() }()
	close(ch)
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (t *task) ctxCancelled() bool {
	select {
	case <-t.ctx.Done():
		return true
	default:
		return false
	}
}
