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
	// StatusAbandoned 在关闭（全局或本组）等待超过最大等待时间后，任务
	// 仍不理会取消信号；调度器已把它占用的容量收回并将其标记为放弃。
	// 此后该任务 goroutine 即便日后退出，也不会再触碰账目。
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

// Task 由调用方声明所属业务组、资源开销、优先级、可选超时与执行体。
type Task struct {
	ID string
	// Group 为业务组名；空串（DefaultGroup）表示内建默认组（无组级配额），
	// 从而与单池用法完全兼容。非空组必须先经 ConfigureGroup 注册，
	// 否则提交会得到 ReasonUnknownGroup 的显式拒绝。
	Group    string
	Cost     int64
	Priority Priority
	// Timeout <= 0 表示不设任务级超时。
	Timeout time.Duration
	Fn      func(ctx context.Context) error
}

// ErrTaskAbandoned 表示任务在关闭流程中超过最大等待时间仍未退出，
// 已被调度器放弃（容量已收回）。
var ErrTaskAbandoned = errors.New("scheduler: task abandoned after shutdown max wait exceeded")

// ErrTaskCanceled 表示任务被取消（排队中或运行中）。
var ErrTaskCanceled = errors.New("scheduler: task canceled")

// Handle 用于等待结果或取消一个已提交的任务。其方法可被多个 goroutine
// 并发调用；Cancel 与结算都是幂等的。
type Handle struct {
	task *Task
	done chan struct{}
	// ctx 为任务的生命周期上下文（排队 + 运行共享），
	// 可被调用方 ctx、Cancel、超时或 Shutdown 取消。
	ctx context.Context

	mu     sync.Mutex
	err    error
	status Status
	cancel context.CancelFunc
	// block 为入队时判定的阻塞维度，排队期间可由调用方查询，
	// 用于区分"本组预算不够 / 全局并发不够 / 全局资源不够"。
	block BlockReason

	// group 仅在 Scheduler.mu 下访问（解析后的所属组）。
	group string
	// watchStop 注销排队取消监听（context.AfterFunc 的停止函数），
	// 与 cancel 一样在 Handle 自身的 mu 下访问。
	watchStop context.CancelFunc
	// detached 仅在 Scheduler.mu 下访问：任务已被放弃（容量已收回），
	// 其僵尸 goroutine 日后退出时不得再次释放。
	detached bool
	// settled 仅在 Scheduler.mu 下访问：任务已通过正常结算路径归还过容量
	// （全局账 + 组账 + 两个 WaitGroup 都已 Done）。关闭流程超时回收时
	// 必须跳过这类任务，否则会二次释放导致账目为负 / WaitGroup 计数崩溃。
	// settled 与 detached 互斥：一个任务的容量恰好经由二者之一归还一次。
	settled bool
}

// bindWatch 由调度器在注册排队取消监听成功后调用。
func (h *Handle) bindWatch(stop func() bool) {
	h.mu.Lock()
	h.watchStop = func() { stop() }
	h.mu.Unlock()
}

// endWatch 注销排队取消监听；并触发一次底层 context cancel，使
// context.WithCancel 的相关资源随任务终态及时释放。cancel 幂等。
func (h *Handle) endWatch() {
	h.mu.Lock()
	if h.watchStop != nil {
		h.watchStop()
		h.watchStop = nil
	}
	c := h.cancel
	h.mu.Unlock()
	if c != nil {
		c()
	}
}

// SetBlockReason 由调度器在入队时（mu 内）记录阻塞维度。
func (h *Handle) setBlockReason(b BlockReason) {
	h.mu.Lock()
	h.block = b
	h.mu.Unlock()
}

// BlockReason 返回任务最近一次入队时的阻塞维度（未排队为 BlockNone）。
// 用于复盘：任务到底是被本组预算、全局并发还是全局资源挡住。
func (h *Handle) BlockReason() BlockReason {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.block
}

// GroupName 返回任务所属业务组名。
func (h *Handle) GroupName() string { return h.group }

func newHandle(t *Task, ctx context.Context) *Handle {
	return &Handle{task: t, done: make(chan struct{}), status: StatusQueued, ctx: ctx}
}

// markRunning 在任务真正获得资源时调用，仅做一次 Queued -> Running 迁移。
func (h *Handle) markRunning() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.status == StatusQueued {
		h.status = StatusRunning
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
