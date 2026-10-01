package scheduler

import (
	"fmt"
	"sync"
)

// DefaultGroup 是未显式声明 Group 的任务所属的内建业务组名。
// 默认组不设任何组级配额（仅受全局上限约束），以此兼容单池用法。
const DefaultGroup = ""

// unlimitedResource 是组资源账"不设上限"的归一取值。
const unlimitedResource int64 = -1

// GroupConfig 声明单个业务组的硬约束（组级配额是全局配额之外的额外硬约束）：
//   - MaxConcurrency：该组同时运行任务数上限，<=0 表示不限制；
//   - ResourceLimit：该组所有运行中任务 Cost 之和的上限，<=0 表示不限制
//     （注册时归一为 -1）。
type GroupConfig struct {
	Name           string
	MaxConcurrency int
	ResourceLimit  int64
}

// GroupSnapshot 是某一时刻单个业务组账目的不可变快照。
// MaxConcurrency==0 表示并发不限；ResourceLimit==-1 表示资源不限。
type GroupSnapshot struct {
	Name           string
	MaxConcurrency int
	InFlight       int
	ResourceLimit  int64
	ResourceUsed   int64
	Queued         int
	Stopped        bool
}

// groupState 是一个业务组的全部运行期状态：组级两本账、组内等待队列、
// 在运行任务集合与独立的等待计数。除 wg 外的字段都必须在 Scheduler.mu
// 下访问（wg 的 Add/Done 与 running 集合变更同处于 mu 临界区，Wait 在锁外）。
type groupState struct {
	name string

	// 组级配额账；maxConcurrency<=0 表示不限制并发，
	// resourceLimit==unlimitedResource(-1) 表示不限制资源。
	maxConcurrency int
	resourceLimit  int64
	inFlight       int
	resourceUsed   int64

	stopped bool

	queue   *groupQueue
	running map[*Handle]struct{}
	wg      sync.WaitGroup
}

// newGroupState 创建业务组并初始化账目与容器。
func newGroupState(cfg GroupConfig) *groupState {
	maxC := cfg.MaxConcurrency
	if maxC < 0 {
		maxC = 0
	}
	res := cfg.ResourceLimit
	if res <= 0 {
		res = unlimitedResource
	}
	return &groupState{
		name:           cfg.Name,
		maxConcurrency: maxC,
		resourceLimit:  res,
		queue:          newGroupQueue(cfg.Name),
		running:        map[*Handle]struct{}{},
	}
}

// canAdmit 报告该组自身的两本账是否还能容纳声明 cost 的任务。
// 组上限是额外硬约束：并发维度（若设了）与资源维度（若设了）都必须满足。
func (g *groupState) canAdmit(cost int64) bool {
	if g.maxConcurrency > 0 && g.inFlight >= g.maxConcurrency {
		return false
	}
	if g.resourceLimit != unlimitedResource && g.resourceUsed+cost > g.resourceLimit {
		return false
	}
	return true
}

// admit 在组账上登记一次准入，并把句柄记入在运行集合。
// 调用前必须已通过 canAdmit，且调用方持有 Scheduler.mu。
func (g *groupState) admit(h *Handle, cost int64) {
	g.inFlight++
	g.resourceUsed += cost
	g.running[h] = struct{}{}
	g.wg.Add(1)
}

// release 在组账上归还一次资源并从在运行集合摘除。
// 对每个正常结算的任务恰好调用一次（由 Scheduler 的终态幂等 +
// detached 标记共同保证）；出现负数立即 panic 暴露账目损坏。
func (g *groupState) release(h *Handle, cost int64) {
	delete(g.running, h)
	g.wg.Done()
	g.inFlight--
	g.resourceUsed -= cost
	if g.inFlight < 0 || g.resourceUsed < 0 {
		panic(fmt.Sprintf("scheduler: group %q account corrupted (double release)", g.name))
	}
}

// abandon 用于"超过最大等待仍不退出"的在运行任务：把它的容量从组账上
// 收回并标记为已脱管，但保留 running 集合中的登记直到 goroutine 真正退出，
// 以拦截僵尸 goroutine 日后的重复结算。
func (g *groupState) abandon(h *Handle, cost int64) {
	g.wg.Done()
	g.inFlight--
	g.resourceUsed -= cost
	if g.inFlight < 0 || g.resourceUsed < 0 {
		panic(fmt.Sprintf("scheduler: group %q account corrupted on abandon", g.name))
	}
	_ = cost
}

// zombieDone 在被放弃的僵尸 goroutine 最终退出时调用：仅从 running 集合
// 摘除，绝不再触碰账目（容量已在 abandon 时归还）。
func (g *groupState) zombieDone(h *Handle) {
	delete(g.running, h)
}

// reconfigure 运行期调整组级上限；只改上限，不回填、不驱逐在运行任务。
// 缩容后 used 可能瞬时超过新上限：此时仅冻结新的准入，等在运行任务
// 自然结算把账目压回新上限以内；账目恒为真，不会被在跑任务顶穿。
func (g *groupState) reconfigure(cfg GroupConfig) {
	if cfg.MaxConcurrency < 0 {
		g.maxConcurrency = 0
	} else {
		g.maxConcurrency = cfg.MaxConcurrency
	}
	if cfg.ResourceLimit <= 0 {
		g.resourceLimit = unlimitedResource
	} else {
		g.resourceLimit = cfg.ResourceLimit
	}
}

// snapshot 拷贝出该组当前账目。
func (g *groupState) snapshot() GroupSnapshot {
	return GroupSnapshot{
		Name:           g.name,
		MaxConcurrency: g.maxConcurrency,
		InFlight:       g.inFlight,
		ResourceLimit:  g.resourceLimit,
		ResourceUsed:   g.resourceUsed,
		Queued:         g.queue.len(),
		Stopped:        g.stopped,
	}
}
