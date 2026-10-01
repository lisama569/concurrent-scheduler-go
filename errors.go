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
	// ReasonQueueFull 全局等待队列已满，任务无法排队。
	ReasonQueueFull
	// ReasonExceedsHardLimit 任务声明的资源开销超过全局硬上限，
	// 即使系统完全空闲也永远不可能被满足，因此立即拒绝而非排队。
	ReasonExceedsHardLimit
	// ReasonUnknownGroup 任务携带的业务组从未通过 ConfigureGroup 注册。
	// 不做隐式静默建组，避免任务悄悄落到错误的预算池。
	ReasonUnknownGroup
	// ReasonGroupStopped 该业务组已被 CloseGroup 单独停收，其他组不受影响。
	ReasonGroupStopped
	// ReasonGroupExceedsHardLimit 任务 Cost 超过本组资源硬上限，
	// 即使本组与全局都完全空闲也永远无法满足，立即拒绝。
	ReasonGroupExceedsHardLimit
)

func (r RejectReason) String() string {
	switch r {
	case ReasonSchedulerStopped:
		return "SCHEDULER_STOPPED"
	case ReasonQueueFull:
		return "QUEUE_FULL"
	case ReasonExceedsHardLimit:
		return "EXCEEDS_HARD_LIMIT"
	case ReasonUnknownGroup:
		return "UNKNOWN_GROUP"
	case ReasonGroupStopped:
		return "GROUP_STOPPED"
	case ReasonGroupExceedsHardLimit:
		return "GROUP_EXCEEDS_HARD_LIMIT"
	default:
		return "NONE"
	}
}

// RejectError 携带可区分的拒绝原因、所属业务组与说明。
type RejectError struct {
	TaskID string
	Reason RejectReason
	// Group 为被拒绝任务所属的业务组名（默认组为 DefaultGroup）。
	Group  string
	Detail string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("scheduler: task %q group=%q rejected: %s: %s",
		e.TaskID, e.Group, e.Reason, e.Detail)
}

func reasonError(taskID, group string, r RejectReason, detail string) *RejectError {
	return &RejectError{TaskID: taskID, Group: group, Reason: r, Detail: detail}
}

// BlockReason 描述一个已排队任务"当前为什么还没被准入"的确切阻塞维度。
// 入队时判定一次并随队列条目保存；每次提交都能据此区分是被本组预算、
// 全局资源还是全局并发挡住，任务本身只会排队、绝不被静默丢弃。
type BlockReason int

const (
	// BlockNone 未阻塞（例如已准入）。
	BlockNone BlockReason = iota
	// BlockGroupConcurrency 本组并发名额不足。
	BlockGroupConcurrency
	// BlockGroupResource 本组资源预算不足。
	BlockGroupResource
	// BlockGlobalConcurrency 全局并发名额不足（本组尚有余量）。
	BlockGlobalConcurrency
	// BlockGlobalResource 全局资源预算不足（本组尚有余量）。
	BlockGlobalResource
)

func (b BlockReason) String() string {
	switch b {
	case BlockGroupConcurrency:
		return "GROUP_CONCURRENCY"
	case BlockGroupResource:
		return "GROUP_RESOURCE"
	case BlockGlobalConcurrency:
		return "GLOBAL_CONCURRENCY"
	case BlockGlobalResource:
		return "GLOBAL_RESOURCE"
	default:
		return "NONE"
	}
}
