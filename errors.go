package scheduler

import (
	"errors"
	"fmt"
)

// RejectReason 描述任务被拒绝（不进入运行）的确切原因。
// 所有超额 / 非法准入都必须显式返回其中之一，绝不允许静默丢弃。
type RejectReason int

const (
	// ReasonNone 占位，表示未被拒绝。
	ReasonNone RejectReason = iota
	// ReasonSchedulerStopped 调度器已进入优雅关闭，不再接受新任务。
	ReasonSchedulerStopped
	// ReasonGroupStopped 该任务所属业务组已被单独关闭（停收 + 清队列），
	// 其它组不受影响。
	ReasonGroupStopped
	// ReasonUnknownGroup 任务引用了一个未注册（且未开启默认放行）的业务组。
	ReasonUnknownGroup
	// ReasonQueueFull 全局等待队列已满，任务无法排队。
	// 队列区是全局共享的有限背压资源，不属于任何一个组。
	ReasonQueueFull
	// ReasonExceedsHardLimit 任务声明的资源开销超过全局硬上限，
	// 即使系统完全空闲也永远不可能被满足，因此立即拒绝而非排队。
	ReasonExceedsHardLimit
	// ReasonExceedsGroupLimit 任务声明的资源开销超过所属组的组级硬上限，
	// 即使本组与全局完全空闲也永远不可能被满足，立即拒绝。
	ReasonExceedsGroupLimit
)

func (r RejectReason) String() string {
	switch r {
	case ReasonSchedulerStopped:
		return "SCHEDULER_STOPPED"
	case ReasonGroupStopped:
		return "GROUP_STOPPED"
	case ReasonUnknownGroup:
		return "UNKNOWN_GROUP"
	case ReasonQueueFull:
		return "QUEUE_FULL"
	case ReasonExceedsHardLimit:
		return "EXCEEDS_HARD_LIMIT"
	case ReasonExceedsGroupLimit:
		return "EXCEEDS_GROUP_LIMIT"
	default:
		return "NONE"
	}
}

// RejectError 携带可区分的拒绝原因与提交时刻的账目快照说明。
type RejectError struct {
	TaskID string
	// Group 为被拒绝任务声明归属的业务组（可能为空串默认组或未注册组）。
	Group  string
	Reason RejectReason
	Detail string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("scheduler: task %q group=%q rejected: %s: %s",
		e.TaskID, e.Group, e.Reason, e.Detail)
}

func reasonError(taskID, group string, r RejectReason, detail string) *RejectError {
	return &RejectError{TaskID: taskID, Group: group, Reason: r, Detail: detail}
}

// ErrTaskAbandoned 表示关闭流程中任务超过最大等待时间仍不退出，
// 其容量已被调度器单方面收回，任务被终结为 StatusAbandoned。
var ErrTaskAbandoned = errors.New("scheduler: task abandoned after shutdown grace period; capacity reclaimed")
