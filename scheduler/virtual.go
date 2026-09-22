package scheduler

import (
	"context"
	"time"
)

// VirtualTask 是确定性模拟中的任务描述。虚拟模式不执行真实的 Go 函数，
// 而是按照给定的时长、异常类型推进，因此同种子下事件序列逐次可复现。
type VirtualTask struct {
	Spec TaskSpec
	// Duration 任务在虚拟时间中的运行时长；<=0 视为瞬时完成。
	Duration time.Duration
	// Panic 为 true 时任务在"运行结束"时以 panic 结算（验证故障隔离）。
	Panic bool
	// Block 为 true 时任务无视超时/取消，永不自行退出（验证 runaway 回收）。
	Block bool
	// SubmitAt 相对虚拟起点的提交时刻。
	SubmitAt time.Duration
}

// VirtualEngine 是单协程确定性调度引擎。
type VirtualEngine struct {
	s             *Scheduler
	clk           *fakeClock
	pendingSubmit int
}

// NewVirtual 创建一个由虚拟时钟驱动的调度器引擎。
func NewVirtual(start time.Time, cfg Config) *VirtualEngine {
	fc := newFakeClock(start)
	s := newScheduler(cfg, fc, true, fc)
	return &VirtualEngine{s: s, clk: fc}
}

// Scheduler 返回底层调度器。
func (e *VirtualEngine) Scheduler() *Scheduler { return e.s }

// Run 按提交时刻投递任务并推进虚拟时间，直到没有待触发事件或到达 horizon。
// 引擎在单协程内运行：每个计时器回调内完成"任务结束 + 一轮 scan"，
// 结果完全由输入列表与时钟决定。
func (e *VirtualEngine) Run(tasks []VirtualTask, horizon time.Duration) {
	start := e.clk.now()

	// 安排提交事件。
	e.pendingSubmit = len(tasks)
	for i := range tasks {
		vt := tasks[i]
		e.clk.afterFunc(vt.SubmitAt, func() {
			e.pendingSubmit--
			t := e.newVirtualTask(vt)
			_, _ = e.s.submitLocked(vt.Spec, t)
			e.s.scan()
		})
	}

	// 主循环：不断跳到下一个计时器到期点并触发，直到满足任一终止条件：
	//  1. 所有提交事件都已投递，且调度器完全排空（无运行、无排队任务）；
	//  2. 计时器堆清空；
	//  3. 到达 horizon。
	for {
		if e.pendingSubmit == 0 {
			if snap := e.s.Snapshot(); snap.Running == 0 && snap.Queued == 0 {
				return
			}
		}
		d, ok := e.clk.nextDeadline()
		if !ok {
			return
		}
		if d.Sub(start) > horizon {
			_ = e.clk.runUntil(start.Add(horizon))
			return
		}
		e.clk.runUntil(d)
	}
}

// newVirtualTask 为模拟任务构造内部记录；Fn 永远不会被调用，
// 因此放一个哨兵函数绕过规格校验。
func (e *VirtualEngine) newVirtualTask(vt VirtualTask) *task {
	spec := vt.Spec
	if spec.Fn == nil {
		spec.Fn = virtualNoop
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &task{
		name:      spec.Name,
		priority:  spec.Priority,
		cost:      spec.Cost,
		spec:      spec,
		ctx:       ctx,
		cancel:    cancel,
		heapIndex: -1,
		virtual:   true,
		vDuration: vt.Duration,
		vPanic:    vt.Panic,
		vBlock:    vt.Block,
	}
}

// startVirtual 由 startLocked 在持锁状态下调用：安排任务的完成与超时事件。
// 两个计时器相互独立，先发生者生效，其余空转，语义与真实模式一致。
func (s *Scheduler) startVirtual(t *task) {
	// 自然结束计时器（失控任务永不自行结束，不安排）。
	if !t.vBlock && t.vDuration > 0 {
		s.clk.afterFunc(t.vDuration, func() {
			s.finishVirtual(t)
		})
	} else if !t.vBlock {
		// 瞬时任务：在当前时刻安排完成事件（afterFunc 回调在锁外执行）。
		s.clk.afterFunc(0, func() { s.finishVirtual(t) })
	}

	// 运行超时：到达超时点 scan 会取消 ctx；再经过 RunGrace 仍未结束则
	// 被 reap 为 runaway，账目先行释放。
	if t.spec.RunTimeout > 0 {
		s.clk.afterFunc(t.spec.RunTimeout, func() { s.scan() })
		s.clk.afterFunc(t.spec.RunTimeout+s.cfg.RunGrace, func() {
			s.scan()
		})
	}
}

// finishVirtual 模拟任务体在"自然结束点"退出。
func (s *Scheduler) finishVirtual(t *task) {
	s.mu.Lock()
	terminal := t.status.Terminal()
	timedOut := t.timedOut
	userCanceled := t.userCanceled
	vPanic := t.vPanic
	s.mu.Unlock()
	if terminal {
		return
	}
	switch {
	case t.ctx.Err() != nil && timedOut && !userCanceled:
		s.finishTask(t, StatusTimedOut, ErrTimeout)
	case t.ctx.Err() != nil:
		s.finishTask(t, StatusCanceled, ErrCanceled)
	case vPanic:
		s.finishTask(t, StatusPanicked, panicErr("simulated panic: "+t.name))
	default:
		s.finishTask(t, StatusSucceeded, nil)
	}
	s.scan()
}

// VirtualNoop 是虚拟任务使用的哨兵函数体：虚拟引擎从不调用 Fn，
// 但 TaskSpec 校验要求非 nil，外部（如 simulation 包）可直接引用它。
func VirtualNoop(context.Context) error { return nil }

func virtualNoop(context.Context) error { return nil }
