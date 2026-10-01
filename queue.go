package scheduler

// waitEntry 是等待队列（组内队列 groupQueue）的内部条目。
type waitEntry struct {
	handle *Handle
	// group 为解析后的所属业务组名，随每条准入/排队/释放记录一起带出。
	group string
	cost  int64
	base  Priority
	// seq 为单调递增的入队序号，用于同优先级下 FIFO，保证可复现。
	seq uint64
	// scans 是该条目入队后经历过的"无法立即满足任何任务"的空扫描次数，
	// 作为老化（aging）依据。
	scans int64
}

// effectivePriority 随等待时间（以空扫描次数计）线性上升，
// 因此低优先级任务不会无限期饥饿，同时又不会立即抢占高优先级任务。
func (e *waitEntry) effectivePriority() int64 {
	return int64(e.base) + e.scans
}
