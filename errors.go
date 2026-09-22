package scheduler

import "fmt"

// RejectReason 描述任务被拒绝（不进入运行）的确切原因。
// 所有超额 / 非法准入都必须显式返回其中之一，绝不允许静默丢弃。
type RejectReason int

const (
	// ReasonNone 占位，表示未被拒绝。
	ReasonNone RejectReason = iota
	// ReasonSchedulerStopped 调度器已进入优雅关闭，不再接受新任务。
	ReasonSchedulerStopped
	// ReasonQueueFull 等待队列已满，任务无法排队。
	ReasonQueueFull
	// ReasonExceedsHardLimit 任务声明的资源开销超过全局硬上限，
	// 即使系统完全空闲也永远不可能被满足，因此立即拒绝而非排队。
	ReasonExceedsHardLimit
)

func (r RejectReason) String() string {
	switch r {
	case ReasonSchedulerStopped:
		return "SCHEDULER_STOPPED"
	case ReasonQueueFull:
		return "QUEUE_FULL"
	case ReasonExceedsHardLimit:
		return "EXCEEDS_HARD_LIMIT"
	default:
		return "NONE"
	}
}

// RejectError 携带可区分的拒绝原因与提交时刻的账目快照说明。
type RejectError struct {
	TaskID string
	Reason RejectReason
	Detail string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("scheduler: task %q rejected: %s: %s", e.TaskID, e.Reason, e.Detail)
}

func reasonError(taskID string, r RejectReason, detail string) *RejectError {
	return &RejectError{TaskID: taskID, Reason: r, Detail: detail}
}
