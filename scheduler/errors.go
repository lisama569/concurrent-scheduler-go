package scheduler

import "fmt"

// RejectReason 描述任务在准入阶段被拒绝的可区分原因。
type RejectReason int

const (
	// ReasonNone 未被拒绝。
	ReasonNone RejectReason = iota
	// ReasonTaskTooLarge 任务自身开销超过调度器总容量，永远无法容纳。
	ReasonTaskTooLarge
	// ReasonOverConcurrency 并发度已达上限。
	ReasonOverConcurrency
	// ReasonOverResource 某类资源已达上限（但任务本身并未超过总容量）。
	ReasonOverResource
	// ReasonShuttingDown 调度器正在关闭，不再接受新任务。
	ReasonShuttingDown
	// ReasonInvalidSpec 任务规格非法（资源为负、优先级越界等）。
	ReasonInvalidSpec
)

func (r RejectReason) String() string {
	switch r {
	case ReasonNone:
		return "none"
	case ReasonTaskTooLarge:
		return "task_too_large"
	case ReasonOverConcurrency:
		return "over_concurrency"
	case ReasonOverResource:
		return "over_resource"
	case ReasonShuttingDown:
		return "shutting_down"
	case ReasonInvalidSpec:
		return "invalid_spec"
	default:
		return fmt.Sprintf("unknown_reason(%d)", int(r))
	}
}

// RejectError 携带拒绝原因的准入错误。
type RejectError struct {
	Reason  RejectReason
	Message string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("task rejected: reason=%s: %s", e.Reason, e.Message)
}

// Is 使包装过的 RejectError 仍可被 errors.Is 按原因匹配到哨兵错误。
func (e *RejectError) Is(target error) bool {
	t, ok := target.(*RejectError)
	return ok && t.Reason == e.Reason
}

// 预定义的哨兵错误，便于调用方 errors.Is 判断。
var (
	ErrTaskTooLarge    = &RejectError{Reason: ReasonTaskTooLarge, Message: "task cost exceeds total capacity"}
	ErrOverConcurrency = &RejectError{Reason: ReasonOverConcurrency, Message: "max concurrency reached"}
	ErrOverResource    = &RejectError{Reason: ReasonOverResource, Message: "resource limit reached"}
	ErrShuttingDown    = &RejectError{Reason: ReasonShuttingDown, Message: "scheduler is shutting down"}
	ErrInvalidSpec     = &RejectError{Reason: ReasonInvalidSpec, Message: "invalid task spec"}
)

// 生命周期相关的非准入错误。
var (
	ErrNotRunning = constErr("scheduler is not running")
	ErrClosed     = constErr("scheduler already closed")
	ErrTimeout    = constErr("task timed out")
	ErrCanceled   = constErr("task canceled")
)

type constErr string

func (e constErr) Error() string { return string(e) }

func rejectError(reason RejectReason, format string, args ...any) error {
	return &RejectError{Reason: reason, Message: fmt.Sprintf(format, args...)}
}

// panicError 包装任务体 panic 时恢复到的值。
type panicError struct{ value any }

func (e *panicError) Error() string { return fmt.Sprintf("task panicked: %v", e.value) }

func panicErr(v any) error { return &panicError{value: v} }
