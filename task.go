package scheduler

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Priority 数值越大优先级越高。
type Priority int

const (
	PriorityLow    Priority = 0
	PriorityNormal Priority = 5
	PriorityHigh   Priority = 10
)

// Status 是任务的生命周期状态。
type Status int

const (
	// StatusQueued 已排队，等待资源。
	StatusQueued Status = iota
	// StatusRunning 已获得资源并开始执行。
	StatusRunning
	// StatusSucceeded 正常完成。
	StatusSucceeded
	// StatusFailed 任务函数返回了非 nil 错误。
	StatusFailed
	// StatusCanceled 在排队中或运行中被外部 ctx / Cancel 取消。
	StatusCanceled
	// StatusTimedOut 超过任务声明的 Timeout。
	StatusTimedOut
	// StatusPanicked 任务函数发生 panic，已被调度器捕获隔离。
	StatusPanicked
	// StatusRejected 在准入阶段被拒绝，未占用任何资源。
	StatusRejected
	// StatusAbandoned 在关闭流程中超过最大等待时间仍不退出，调度器
	// 已单方面收回其容量。任务 goroutine 可能仍卡着，但账目上它已终结，
	// 容量可被复用；若它日后真正退出，会被识别为迟到结算而不再重复释放。
	StatusAbandoned
)

func (s Status) String() string {
	switch s {
	case StatusQueued:
		return "QUEUED"
	case StatusRunning:
		return "RUNNING"
	case StatusSucceeded:
		return "SUCCEEDED"
	case StatusFailed:
		return "FAILED"
	case StatusCanceled:
		return "CANCELED"
	case StatusTimedOut:
		return "TIMED_OUT"
	case StatusPanicked:
		return "PANICKED"
	case StatusRejected:
		return "REJECTED"
	case StatusAbandoned:
		return "ABANDONED"
	default:
		return "UNKNOWN"
	}
}

// WaitReason 区分一个任务进入等待队列时，到底是被哪一层预算挡住的。
// 这不是拒绝：任务已安全排队，绝不会被悄悄丢掉。
type WaitReason int

const (
	// WaitReasonNone 未排队（立即准入），或排队并非因预算不足
	// （例如调度器处于 Pause）。
	WaitReasonNone WaitReason = iota
	// WaitReasonGroupBudget 本组并发/资源预算暂时不足；全局仍有余量。
	WaitReasonGroupBudget
	// WaitReasonGlobalBudget 本组有余量，但全局并发/资源预算不足。
	WaitReasonGlobalBudget
)

func (w WaitReason) String() string {
	switch w {
	case WaitReasonGroupBudget:
		return "GROUP_BUDGET"
	case WaitReasonGlobalBudget:
		return "GLOBAL_BUDGET"
	default:
		return "NONE"
	}
}

// Task 由调用方声明业务组、资源开销、优先级、可选超时与执行体。
type Task struct {
	ID string
	// Group 为任务所属业务组（多租户隔离单位）。空串表示默认组：
	// 默认组不设组级上限，单池用法无需任何改动（兼容映射）。
	// 若调度器配置了严格组校验，引用未注册的组会被显式拒绝。
	Group    string
	Cost     int64
	Priority Priority
	// Timeout <= 0 表示不设任务级超时。
	Timeout time.Duration
	Fn      func(ctx context.Context) error
}

// ErrTaskCanceled 表示任务被取消（排队中或运行中）。
var ErrTaskCanceled = errors.New("scheduler: task canceled")

// Handle 用于等待结果或取消一个已提交的任务。其方法可被多个 goroutine
// 并发调用；Cancel 与结算都是幂等的。
type Handle struct {
	task *Task
	// group 记录准入与排队时归属的业务组，所有决策日志 / 账目释放都带上它。
	group string
	done  chan struct{}
	// admitCh 在任务真正获得资源（准入记账）时关闭一次。排队期的取消
	// 监听协程据此立即退出，避免"任务早已完成、监听协程仍挂着"的长期泄漏。
	admitCh chan struct{}
	// ctx 为任务的生命周期上下文（排队 + 运行共享），
	// 可被调用方 ctx、Cancel、超时或 Shutdown 取消。
	ctx context.Context

	mu         sync.Mutex
	err        error
	status     Status
	cancel     context.CancelFunc
	waitReason WaitReason
}

func newHandle(t *Task, ctx context.Context) *Handle {
	return &Handle{
		task:    t,
		done:    make(chan struct{}),
		admitCh: make(chan struct{}),
		status:  StatusQueued,
		ctx:     ctx,
	}
}

// markRunning 在任务真正获得资源时调用，仅做一次 Queued -> Running 迁移，
// 并关闭准入信号以释放排队取消监听协程。
func (h *Handle) markRunning() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.status == StatusQueued {
		h.status = StatusRunning
	}
	select {
	case <-h.admitCh:
	default:
		close(h.admitCh)
	}
}

// Done 在任务结算（成功/失败/取消/超时/panic）后关闭。
func (h *Handle) Done() <-chan struct{} { return h.done }

// Err 返回任务终态错误；任务未结束时返回 nil。
func (h *Handle) Err() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.err
}

// Status 返回任务当前状态。
func (h *Handle) Status() Status {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.status
}

// Cost 返回任务声明的资源开销。
func (h *Handle) Cost() int64 { return h.task.Cost }

// Group 返回任务所属业务组。
func (h *Handle) Group() string { return h.group }

// WaitReason 返回任务入队时被挡住的确切原因（本组预算 / 全局预算）。
// 立即准入或非预算原因排队时为 WaitReasonNone。
// 任务一旦被准入，该记录保留为入队时的历史信息。
func (h *Handle) WaitReason() WaitReason {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.waitReason
}

// setWaitReason 由调度器在入队时（mu 内）调用。
func (h *Handle) setWaitReason(r WaitReason) {
	h.mu.Lock()
	h.waitReason = r
	h.mu.Unlock()
}

// Wait 阻塞直到任务结算，返回终态错误（成功为 nil）。
func (h *Handle) Wait() error {
	<-h.done
	return h.Err()
}

// ID 返回任务 ID。
func (h *Handle) ID() string { return h.task.ID }

// Cancel 请求取消任务。排队中的任务会立即被结算为取消并释放其排队资格；
// 运行中的任务通过 ctx 收到取消信号，是否及时退出取决于任务函数。
// 可重复调用，仅第一次生效。
func (h *Handle) Cancel() {
	h.mu.Lock()
	c := h.cancel
	h.mu.Unlock()
	if c != nil {
		c()
	}
}

// bindCancel 由调度器在创建任务上下文后调用。
func (h *Handle) bindCancel(c context.CancelFunc) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cancel = c
}

// cancelFunc 返回派生上下文的取消函数（供计费票据在执行路径结束时调用，
// 避免 context 泄漏）；幂等，可与 Handle.Cancel / Shutdown 重复调用。
func (h *Handle) cancelFunc() context.CancelFunc {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.cancel
}

// finish 由调度器在任务终态恰好调用一次：记录结果并关闭 Done。
// 返回 true 表示本次调用完成了结算（用于调用方决定是否需要释放资源）。
func (h *Handle) finish(status Status, err error) bool {
	h.mu.Lock()
	select {
	case <-h.done:
		// 已结算：重复结算被显式吞掉，配合调用方保证资源只释放一次。
		h.mu.Unlock()
		return false
	default:
	}
	h.status = status
	h.err = err
	close(h.done)
	h.mu.Unlock()
	return true
}
