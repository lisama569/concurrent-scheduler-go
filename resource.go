package scheduler

import "sync"

// unlimited 标记"该维度不设上限"。MaxConcurrency<=0 / ResourceLimit<0
// 沿用该约定：组级配额不配置时即为无界，只受全局硬约束限制。
const unlimited int64 = -1

// quota 维护并发度与总资源两项账目的精确记账（全局账本或某个组的账本）。
//
// 设计约定：所有方法都必须在 Scheduler.mu 这一把锁内调用，
// 即账本自身不再加锁。这样"准入检查 + 记账 + 出队决策"处于同一个
// 临界区，杜绝 TOCTOU 竞态；资源的释放路径因此也能做到精确一次。
type quota struct {
	maxConcurrency int
	inFlight       int

	resourceLimit int64 // <0 表示无界
	resourceUsed  int64
}

func newQuota(maxConcurrency int, resourceLimit int64) *quota {
	return &quota{
		maxConcurrency: maxConcurrency,
		resourceLimit:  resourceLimit,
	}
}

// canAdmit 判定一个声明 cost 的任务此刻能否被准入：
// 必须同时满足并发度余量与总资源余量两个硬约束。
func (q *quota) canAdmit(cost int64) bool {
	if q.maxConcurrency > 0 && q.inFlight >= q.maxConcurrency {
		return false
	}
	if q.resourceLimit >= 0 && q.resourceUsed+cost > q.resourceLimit {
		return false
	}
	return true
}

// admit 在账本上登记一次准入。调用前必须已通过 canAdmit。
func (q *quota) admit(cost int64) {
	q.inFlight++
	q.resourceUsed += cost
}

// release 在账本上归还一次资源。
// 调用方必须保证对每个计费令牌恰好调用一次；上层用 charge 票据去重，
// 重复归还不会发生，任何负数都是账目损坏的信号。
func (q *quota) release(cost int64) {
	q.inFlight--
	q.resourceUsed -= cost
	if q.inFlight < 0 || q.resourceUsed < 0 {
		panic("scheduler: quota account corrupted (double release)")
	}
}

// setLimits 运行期调整上限。扩容后等待中的任务由派发循环重新评估；
// 缩容不会抢占在跑任务，因此 used 可以暂时高于新上限（自然排空），
// 期间 canAdmit 恒为 false，新任务绝不可能把新上限顶穿。
func (q *quota) setLimits(maxConcurrency int, resourceLimit int64) {
	q.maxConcurrency = maxConcurrency
	q.resourceLimit = resourceLimit
}

func (q *quota) snapshot() quotaSnapshot {
	return quotaSnapshot{
		MaxConcurrency: q.maxConcurrency,
		InFlight:       q.inFlight,
		ResourceLimit:  q.resourceLimit,
		ResourceUsed:   q.resourceUsed,
	}
}

// quotaSnapshot 是单层账本（全局或某个组）的只读拷贝。
type quotaSnapshot struct {
	MaxConcurrency int
	InFlight       int
	ResourceLimit  int64 // <0 表示无界，String/日志中显示为 inf
	ResourceUsed   int64
}

// GroupConfig 声明一个业务组的独立硬预算。
//   - MaxConcurrency<=0：该组不设并发上限（仍受全局并发上限约束）；
//   - ResourceLimit<0  ：该组不设资源上限（仍受全局资源上限约束）。
type GroupConfig struct {
	// Name 为组的唯一标识；空串保留给默认组。
	Name           string
	MaxConcurrency int
	ResourceLimit  int64
}

// groupQuota 是一个业务组的运行期账本与生命周期状态。
type groupQuota struct {
	name string
	q    quota
	// stopped 为真表示该组已被单独关闭：拒绝新提交、排队任务已清空，
	// 组不可重新打开（避免"停收 -> 复活"造成语义歧义）。
	stopped bool
	// wg 追踪该组所有已准入、尚未释放（含被放弃）的计费任务，
	// 供 ShutdownGroup 的优雅等待使用。Add 在 mu 内完成，
	// Done 由任务退出 / 容量收回路径调用，Wait 在独立 goroutine 中进行。
	wg sync.WaitGroup
}

func newGroupQuota(cfg GroupConfig) *groupQuota {
	return &groupQuota{
		name: cfg.Name,
		q:    *newQuota(cfg.MaxConcurrency, cfg.ResourceLimit),
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
	// Groups 为每个组的组级账本快照，按注册顺序排列。
	Groups []GroupSnapshot
}

// GroupSnapshot 是单个业务组的账本与生命周期快照。
type GroupSnapshot struct {
	Name           string
	MaxConcurrency int
	InFlight       int
	ResourceLimit  int64
	ResourceUsed   int64
	Queued         int
	Stopped        bool
}
