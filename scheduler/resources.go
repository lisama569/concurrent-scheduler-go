package scheduler

// Resources 描述任务的资源开销或调度器的资源账目。
// 三个维度相互独立，任一维度超限即视为资源不足。
type Resources struct {
	CPU    int64
	Memory int64
	IO     int64
}

// Limits 描述调度器的总容量与并发上限。
type Limits struct {
	MaxCPU    int64
	MaxMemory int64
	MaxIO     int64
	MaxTasks  int
}

// Snapshot 是某一时刻调度器账目的不可变视图，用于日志与测试断言。
type Snapshot struct {
	InUse         Resources
	Running       int
	Queued        int
	Accepted      int64
	Completed     int64
	Rejected      int64
	Canceled      int64
	TimedOut      int64 // 运行超时（含被 runaway 回收的子集）
	QueueTimedOut int64 // 排队超时（从未获得资源）
	Runaway       int64
	Panicked      int64
	Shutting      bool
	Paused        bool
}

// Add 返回 r+o。
func (r Resources) Add(o Resources) Resources {
	return Resources{CPU: r.CPU + o.CPU, Memory: r.Memory + o.Memory, IO: r.IO + o.IO}
}

// Sub 返回 r-o。
func (r Resources) Sub(o Resources) Resources {
	return Resources{CPU: r.CPU - o.CPU, Memory: r.Memory - o.Memory, IO: r.IO - o.IO}
}

// Fits 判断资源自身是否不超过总容量（用于"任务过大，永远无法容纳"）。
func (r Resources) Fits(l Limits) bool {
	return r.CPU <= l.MaxCPU && r.Memory <= l.MaxMemory && r.IO <= l.MaxIO
}

// FitsWithin 判断在已占用 u 的基础上再放入 r 是否仍不超限，
// 且并发度有余量。并发度由调用方通过 running 检查，这里只判断资源三维。
func (r Resources) FitsWithin(u Resources, l Limits) bool {
	return u.CPU+r.CPU <= l.MaxCPU &&
		u.Memory+r.Memory <= l.MaxMemory &&
		u.IO+r.IO <= l.MaxIO
}

// NonNegative 判断三维资源是否均非负。
func (r Resources) NonNegative() bool {
	return r.CPU >= 0 && r.Memory >= 0 && r.IO >= 0
}
