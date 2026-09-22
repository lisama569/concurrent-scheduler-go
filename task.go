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
	default:
		return "UNKNOWN"
	}
}

// Task 由调用方声明资源开销、优先级、可选超时与执行体。
type Task struct {
	ID       string
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
	done chan struct{}
	// ctx 为任务的生命周期上下文（排队 + 运行共享），
	// 可被调用方 ctx、Cancel、超时或 Shutdown 取消。
	ctx context.Context

	mu     sync.Mutex
	err    error
	status Status
	cancel context.CancelFunc
}

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
