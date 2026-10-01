package scheduler

// quota 维护并发度与总资源两项账目的精确记账。
//
// 设计约定：所有方法都必须在 Scheduler.mu 这一把锁内调用，
// 即账本自身不再加锁。这样"准入检查 + 记账 + 出队决策"处于同一个
// 临界区，杜绝 TOCTOU 竞态；资源的释放路径因此也能做到精确一次。
type quota struct {
	maxConcurrency int
	inFlight       int

	resourceLimit int64
	resourceUsed  int64
}

func newQuota(cfg Config) *quota {
	return &quota{
		maxConcurrency: cfg.MaxConcurrency,
		resourceLimit:  cfg.ResourceLimit,
	}
}

// canAdmit 判定一个声明 cost 的任务此刻能否被准入：
// 必须同时满足并发度余量与总资源余量两个硬约束。
func (q *quota) canAdmit(cost int64) bool {
	return q.inFlight < q.maxConcurrency && q.resourceUsed+cost <= q.resourceLimit
}

// admit 在账本上登记一次准入。调用前必须已通过 canAdmit。
func (q *quota) admit(cost int64) {
	q.inFlight++
	q.resourceUsed += cost
}

// release 在账本上归还一次资源。
// 调用方必须保证对每个任务恰好调用一次（由调度器的 done 状态保证），
// 重复调用会被上层拦截，不会造成账目为负。
func (q *quota) release(cost int64) {
	q.inFlight--
	q.resourceUsed -= cost
	if q.inFlight < 0 || q.resourceUsed < 0 {
		// 理论上不可达：任何负数都意味着释放多于准入，是账目损坏的信号。
		panic("scheduler: quota account corrupted (double release)")
	}
}

// reconfigure 运行期调整全局上限。与组级缩容语义一致：只改上限，
// 不驱逐在运行任务；缩容后 used 可能瞬时高于新上限，此时仅冻结新准入，
// 等在运行任务自然结算回落。新上限永远不会被在跑任务"顶穿"——
// canAdmit 按新上限逐次把关。
func (q *quota) reconfigure(maxConcurrency int, resourceLimit int64) {
	if maxConcurrency > 0 {
		q.maxConcurrency = maxConcurrency
	}
	if resourceLimit >= 0 {
		q.resourceLimit = resourceLimit
	}
}

// snapshot 在调用方持锁时拷贝出当前账目（附加字段由调度器补齐）。
func (q *quota) snapshot() Snapshot {
	return Snapshot{
		MaxConcurrency: q.maxConcurrency,
		InFlight:       q.inFlight,
		ResourceLimit:  q.resourceLimit,
		ResourceUsed:   q.resourceUsed,
	}
}

// Snapshot 是某一时刻调度器账目的不可变快照，用于日志与测试断言。
type Snapshot struct {
	MaxConcurrency int
	InFlight       int
	ResourceLimit  int64
	ResourceUsed   int64
	Queued         int
	Paused         bool
	Stopped        bool
	// Groups 为各业务组的账目快照，按组名为键。
	Groups map[string]GroupSnapshot
}
