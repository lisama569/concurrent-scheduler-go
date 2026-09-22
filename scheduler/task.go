package scheduler

import (
	"context"
	"time"
)

// Status 是任务在生命周期中的状态。
type Status int

const (
	StatusUnknown Status = iota
	StatusQueued
	StatusRunning
	StatusSucceeded
	StatusFailed
	StatusRejected
	StatusCanceled
	StatusTimedOut
	StatusPanicked
)

func (s Status) String() string {
	switch s {
	case StatusQueued:
		return "queued"
	case StatusRunning:
		return "running"
	case StatusSucceeded:
		return "succeeded"
	case StatusFailed:
		return "failed"
	case StatusRejected:
		return "rejected"
	case StatusCanceled:
		return "canceled"
	case StatusTimedOut:
		return "timed_out"
	case StatusPanicked:
		return "panicked"
	default:
		return "unknown"
	}
}

// Terminal 报告状态是否为终态（终态任务的资源必定已结算）。
func (s Status) Terminal() bool {
	switch s {
	case StatusSucceeded, StatusFailed, StatusRejected, StatusCanceled, StatusTimedOut, StatusPanicked:
		return true
	default:
		return false
	}
}

// TaskFunc 是用户提供的任务体。panic 会被调度器恢复并隔离。
type TaskFunc func(ctx context.Context) error

// TaskSpec 描述一次任务提交的开销与调度参数。
type TaskSpec struct {
	Name string
	// Priority 数值越大优先级越高，范围 [0, maxPriority]。
	Priority int
	Cost     Resources
	// QueueTimeout 排队等待准入的最长时间；<=0 表示不限制。
	QueueTimeout time.Duration
	// RunTimeout 开始执行后的最长时间；<=0 表示不限制。
	RunTimeout time.Duration
	// Fn 任务体；为 nil 视为非法规格。
	Fn TaskFunc
}

// TaskResult 是任务终态结果。
type TaskResult struct {
	Status Status
	Err    error
}

// Handle 是提交后返回的任务句柄，可用于等待结果或主动取消。
type Handle interface {
	ID() int64
	Name() string
	// Wait 阻塞直到任务进入终态或 ctx 结束。
	Wait(ctx context.Context) (TaskResult, error)
	// Done 在任务进入终态时关闭，随后可通过 Result 读取终态。
	Done() <-chan struct{}
	// Result 返回终态结果；任务未结束时 ok 为 false。
	Result() (TaskResult, bool)
	// Cancel 请求取消：排队中则立即移出队列并结算，运行中则取消其 context。
	// 若任务已处于终态返回 false。
	Cancel() bool
}

// task 是调度器内部的任务记录。所有字段仅允许在持有 scheduler.mu 时访问，
// 终态通道 done 的关闭与结果写入除外（由 finish 在锁内完成）。
type task struct {
	id       int64
	name     string
	priority int
	cost     Resources
	spec     TaskSpec

	ctx    context.Context
	cancel context.CancelFunc

	ageTicks    int64 // 已等待的 tick 数（用于防饥饿提升）
	boosted     bool  // 年龄到期后被临时提升，直到被准入或取消
	heapIndex   int
	status      Status
	queuedAt    time.Time
	runDeadline time.Time
	hasRunDl    bool
	queueDl     time.Time
	hasQueueDl  bool

	admitted bool // 是否已计入 ledger（资源占用）
	released bool // 资源是否已释放（保证精确一次）

	started      bool // dispatcher 是否已经启动过它
	runaway      bool // 任务体无视取消/超时，账目已先行释放
	timedOut     bool // 已因 run timeout 被取消（区别于用户取消）
	userCanceled bool // 已被用户主动取消

	// 虚拟模式专用（真实模式下均为零值）。
	virtual   bool
	vDuration time.Duration
	vPanic    bool
	vBlock    bool

	done   chan struct{}
	result TaskResult
}

// taskHandle 是 Handle 的内部实现。
type taskHandle struct {
	s *Scheduler
	t *task
}

func (h *taskHandle) ID() int64    { return h.t.id }
func (h *taskHandle) Name() string { return h.t.name }

func (h *taskHandle) Done() <-chan struct{} { return h.t.done }

func (h *taskHandle) Result() (TaskResult, bool) {
	return h.t.result, h.t.status.Terminal()
}

func (h *taskHandle) Wait(ctx context.Context) (TaskResult, error) {
	select {
	case <-h.t.done:
		return h.t.result, nil
	case <-ctx.Done():
		return TaskResult{}, ctx.Err()
	}
}

func (h *taskHandle) Cancel() bool { return h.s.cancelTask(h.t) }
