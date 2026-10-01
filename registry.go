package scheduler

import (
	"fmt"
	"sort"
)

// groupRegistry 管理全部业务组，并负责"组间公平"的调度策略：
// 严格轮转（round-robin）地在组之间分配准入机会，任何一个组即使
// 持续涌入高优先级任务，也不能连续把每一轮准入机会都拿走而饿死别的组。
//
// 非线程安全：所有方法必须在 Scheduler.mu 下调用。
type groupRegistry struct {
	groups map[string]*groupState
	order  []string // 已注册组名，字典序，保证遍历顺序确定
	// cursor 指向下一轮首先尝试的组在 order 中的位置：每成功准入一个
	// 任务就推进到下一个组，使机会在有等待者的组间轮转。
	cursor int
}

func newGroupRegistry() *groupRegistry {
	return &groupRegistry{groups: map[string]*groupState{}}
}

// ensureDefault 建立内建默认组（无组级限制），幂等。
func (r *groupRegistry) ensureDefault() *groupState {
	if g, ok := r.groups[DefaultGroup]; ok {
		return g
	}
	g := newGroupState(GroupConfig{Name: DefaultGroup})
	r.groups[DefaultGroup] = g
	r.order = append(r.order, DefaultGroup)
	r.sortNames()
	return g
}

// configure 创建或更新一个业务组的上限（运行期可调）：
// 已存在则 reconfigure（不摘队列、不驱逐在运行任务）；
// 不存在则创建并把组名纳入确定性遍历顺序。
func (r *groupRegistry) configure(cfg GroupConfig) error {
	if cfg.Name == DefaultGroup {
		return fmt.Errorf("scheduler: default group is built-in and cannot be reconfigured")
	}
	if g, ok := r.groups[cfg.Name]; ok {
		g.reconfigure(cfg)
		return nil
	}
	g := newGroupState(cfg)
	r.groups[cfg.Name] = g
	r.order = append(r.order, cfg.Name)
	r.sortNames()
	return nil
}

func (r *groupRegistry) sortNames() {
	sort.Strings(r.order)
	// 排序后 cursor 可能越界，回绕到起点（它仅是公平性提示，无需精确定位）。
	if len(r.order) == 0 {
		r.cursor = 0
	} else {
		r.cursor %= len(r.order)
	}
}

// get 返回已注册组；未注册返回 nil。
func (r *groupRegistry) get(name string) *groupState {
	return r.groups[name]
}

// names 按字典序返回全部组名的拷贝。
func (r *groupRegistry) names() []string {
	out := make([]string, len(r.order))
	copy(out, r.order)
	return out
}

// totalQueued 返回所有组等待者总数。
func (r *groupRegistry) totalQueued() int {
	n := 0
	for _, name := range r.order {
		n += r.groups[name].queue.len()
	}
	return n
}

// rotation 从 cursor 起、按字典序环形返回各组，使每一组在一个完整
// 扫描周期内恰好被当作"首选组"尝试一次，顺序对相同组集合完全确定。
func (r *groupRegistry) rotation() []*groupState {
	n := len(r.order)
	out := make([]*groupState, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, r.groups[r.order[(r.cursor+i)%n]])
	}
	return out
}

// popEligible 按"组轮转 + 组内优先级"选出至多一个当前可准入的条目。
// 先轮到哪个组，就在该组内按其自身规则挑一个"组账与全局账都满足"的
// 任务；该组无可满足任务时才尝试下一个组。准入成功后把机会游标让给
// 下一个组，从而保证连续的准入机会不会被同一个组独占。
func (r *groupRegistry) popEligible(canAdmitGlobal func(cost int64) bool) *waitEntry {
	if len(r.order) == 0 {
		return nil
	}
	groups := r.rotation()
	for i, g := range groups {
		if g.stopped || g.queue.len() == 0 {
			continue
		}
		fit := func(cost int64) bool { return g.canAdmit(cost) && canAdmitGlobal(cost) }
		if e := g.queue.popEligible(fit); e != nil {
			// 把下一轮首选权交给环上的下一个组。
			r.cursor = (r.cursor + i + 1) % len(r.order)
			return e
		}
	}
	return nil
}

// popProbe 仅报告当前是否存在"组账与全局账都满足"的可准入条目，
// 不修改队列与游标（用于条件变量谓词）。
func (r *groupRegistry) popProbe(canAdmitGlobal func(cost int64) bool) bool {
	for _, name := range r.order {
		g := r.groups[name]
		if g.stopped || g.queue.len() == 0 {
			continue
		}
		fit := func(cost int64) bool { return g.canAdmit(cost) && canAdmitGlobal(cost) }
		if g.queue.popProbe(fit) {
			return true
		}
	}
	return false
}

// ageAll 在"本轮任何组都无可准入任务"时对所有组的等待者做一次老化，
// 返回全局最大等待扫描次数。组内沿用原有老化语义：等得足够久的低优任务
// 会在组内被逐步抬升，但抬升不会让它越过组间轮转的公平边界。
func (r *groupRegistry) ageAll() int64 {
	var maxAge int64
	for _, name := range r.order {
		if a := r.groups[name].queue.ageAll(); a > maxAge {
			maxAge = a
		}
	}
	return maxAge
}
