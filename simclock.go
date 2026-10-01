package scheduler

import (
	"context"
	"sort"
	"sync"
	"time"
)

func nowMillis() int64 { return time.Now().UnixMilli() }

// 确定性模拟执行器（离散事件 + 门闩放行）
//
// 真实 wall-clock 下任务何时完成取决于 OS 调度与 CPU 速度；释放时机不同会
// 改变后续派发顺序。为严格验证"相同输入 + 相同随机种子 => 调度顺序一致"，
// 本运行器把时间虚拟化并由测试线程单步驱动：
//
//   - 任务被准入的瞬间记录其开始 tick，结束 tick = 开始 tick + 占用时长；
//   - 主循环取所有运行任务中最小的结束 tick，把虚拟时钟推进到该 tick；
//   - 同一 tick 结束的任务，按 ID 升序逐个放行并等待其结算——这保证资源
//     释放顺序也是确定的（成本不同的任务若同时释放，中间态可准入性可能
//     不同，因此必须串行化）；
//   - 派发循环是唯一的、锁内排序是确定的，于是整链路准入/释放顺序可复现。

// SimJob 声明一个确定性模拟任务。
type SimJob struct {
	ID       string
	Group    string // 所属业务组；空串为内建默认组
	Cost     int64
	Priority Priority
	LengthMS int64 // 虚拟占用时长（毫秒）
	Panic    bool  // 放行后立即 panic（验证隔离下的可复现终态）
}

type simGate struct {
	ch   chan struct{}
	once sync.Once
}

func newSimGate() *simGate { return &simGate{ch: make(chan struct{})} }
func (g *simGate) fire()   { g.once.Do(func() { close(g.ch) }) }

type runningJob struct {
	endTick int64
	fired   bool
}

// DeterministicRunner 驱动一个真实 Scheduler 完成一批 SimJob。
type DeterministicRunner struct {
	s *Scheduler

	mu sync.Mutex
	// plans：准入计划，一旦写入永不删除/改写，避免与锁外结算回调竞争。
	plans    map[string]runningJob
	length   map[string]int64
	handles  map[string]*Handle
	gates    map[string]*simGate
	terminal map[string]Status

	clock      int64
	total      int
	admitOrder []string

	// settleCh 为容量充足的缓冲通道：每次任务结算（经 settleBarrier）
	// 写入一个令牌。用通道而非 cond，是为了让"gate 打开后、等待开始前
	// 就已结算"的事件不丢失——缓冲令牌会保留到 waitSettled 读取。
	settleCh chan struct{}
}

// NewDeterministicRunner 包装调度器并安装确定性追踪钩子。
func NewDeterministicRunner(s *Scheduler) *DeterministicRunner {
	r := &DeterministicRunner{
		s:        s,
		plans:    map[string]runningJob{},
		length:   map[string]int64{},
		handles:  map[string]*Handle{},
		gates:    map[string]*simGate{},
		terminal: map[string]Status{},
		settleCh: make(chan struct{}, 1<<16),
	}
	s.onAdmit = r.onAdmit
	s.onTerminal = r.onTerminal
	s.settleBarrier = func(id string) {
		r.settleCh <- struct{}{}
	}
	return r
}

// freeze 必须在提交任何任务前调用：冻结后 Submit 只会把任务放入等待队列，
// 异步 dispatchLoop 也不会准入；准入完全由 Run 通过 pumpDispatch 在
// 锁内同步驱动，从而彻底消除 goroutine 调度时序对调度顺序的影响。
func (r *DeterministicRunner) freeze() {
	r.s.freezeDispatch()
}

// SubmitAll 按给定顺序（即随机种子生成顺序）提交全部任务。
// 返回提交错误（典型为可区分的 RejectError），便于断言拒绝原因。
func (r *DeterministicRunner) SubmitAll(jobs []SimJob) map[string]error {
	errs := map[string]error{}
	for _, j := range jobs {
		g := newSimGate()
		r.length[j.ID] = j.LengthMS
		r.gates[j.ID] = g
		tk := &Task{
			ID:       j.ID,
			Group:    j.Group,
			Cost:     j.Cost,
			Priority: j.Priority,
			Fn: func(ctx context.Context) error {
				select {
				case <-g.ch:
				case <-ctx.Done():
					return ctx.Err()
				}
				if j.Panic {
					panic("sim: " + j.ID)
				}
				return nil
			},
		}
		h, err := r.s.Submit(context.Background(), tk)
		if err != nil {
			errs[j.ID] = err
			continue
		}
		r.handles[j.ID] = h
		r.total++
	}
	return errs
}

// onAdmit 在 mu 下被调度器回调：登记结束 tick 与准入顺序。
// 计划只写一次且永不删除；任务是否仍在运行由 plans/terminal 动态推导。
func (r *DeterministicRunner) onAdmit(id string) {
	r.mu.Lock()
	r.admitOrder = append(r.admitOrder, id)
	if _, exists := r.plans[id]; !exists {
		// 排队任务被准入时才开始计时（排队不消耗资源、不推进其执行期）。
		r.plans[id] = runningJob{endTick: r.clock + r.length[id]}
	}
	r.mu.Unlock()
}

// onTerminal 在锁外被调度器回调：仅记录终态，不删除计划。
func (r *DeterministicRunner) onTerminal(id string, st Status) {
	r.mu.Lock()
	r.terminal[id] = st
	r.mu.Unlock()
}

// activePlans 返回当前"已准入、未终态且未放行"的任务计划。
// 只要 plans 中有条目就代表调度器已准入（onAdmit 只在准入时触发），
// 因此不再要求 handles 已登记——立即准入路径可能先触发 onAdmit、
// 后由提交方写入 handles。
func (r *DeterministicRunner) activePlans() map[string]runningJob {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]runningJob, len(r.plans))
	for id, p := range r.plans {
		if _, done := r.terminal[id]; done {
			continue
		}
		if !p.fired {
			out[id] = p
		}
	}
	return out
}

// pumpDispatch 在冻结态下由本测试线程在调度器锁内同步地把"当前账目下
// 所有能准入的任务"全部准入，并在锁外统一启动它们的 goroutine。
//
// 决策（出队 + 记账 + 准入顺序）完全由当前线程串行做出，与任务 goroutine
// 的真实调度时序无关；goroutine 只执行用户代码、在 gate 上等待或结算，
// 而此时派发循环处于冻结态，任何中途释放都不会引起异步准入。
// 因此相同输入下准入顺序逐字节一致。
func (r *DeterministicRunner) pumpDispatch() {
	type launched struct {
		h *Handle
		t *Task
	}
	var todo []launched

	r.s.mu.Lock()
	for {
		h, t, _ := r.s.dispatchOneLocked()
		if h == nil {
			break
		}
		todo = append(todo, launched{h, t})
	}
	r.s.mu.Unlock()

	for _, l := range todo {
		r.s.launch(l.h, l.t)
	}
}

// waitSettled 阻塞等待 n 个"新的"结算令牌。调用前必须先 drainTokens，
// 把本批放行之前可能已积压的令牌排空（确定性协议下准入后任务都阻塞在
// gate 上，理论上无积压，排空仅为防御）。
func (r *DeterministicRunner) drainTokens() {
	for {
		select {
		case <-r.settleCh:
		default:
			return
		}
	}
}

func (r *DeterministicRunner) waitSettled(n int) {
	for i := 0; i < n; i++ {
		select {
		case <-r.settleCh:
		case <-time.After(2 * time.Second):
			panic("deterministic runner: timeout waiting for task settlement")
		}
	}
}

// Run 推进模拟直到所有提交任务结算。
func (r *DeterministicRunner) Run(maxTicks int) bool {
	// 提交已在冻结态完成；先派发第一批可准入任务。
	r.pumpDispatch()
	for tick := 0; tick < maxTicks; tick++ {
		if r.terminalCount() >= r.total {
			return true
		}
		t, ok := r.nextTick()
		if !ok {
			// 无活动任务但仍有排队：尝试再次派发。
			r.pumpDispatch()
			if _, ok2 := r.nextTick(); !ok2 {
				if r.terminalCount() >= r.total {
					return true
				}
				// 仍无活动任务：可能确有不可满足的队头。
				snap := r.s.Snapshot()
				if snap.Queued == 0 && snap.InFlight == 0 {
					return r.terminalCount() >= r.total
				}
				return false
			}
			continue
		}
		batch := r.takeBatch(t)
		r.drainTokens()
		// 冻结态下一次性释放整批；由于派发被冻结，这些释放不会触发任何
		// 中间准入，账目最终态在每次运行中都一致。
		for _, id := range batch {
			r.gates[id].fire()
		}
		r.waitSettled(len(batch))
		// 全部结算到最终账目后，再统一派发下一批。
		r.pumpDispatch()
	}
	return false
}

// terminalCount 返回已结算任务数。
func (r *DeterministicRunner) terminalCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.terminal)
}

// nextTick 返回当前活动任务中最小的结束 tick；无活动任务时 ok=false。
func (r *DeterministicRunner) nextTick() (int64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var next int64 = -1
	for id, p := range r.plans {
		if _, done := r.terminal[id]; done || p.fired {
			continue
		}
		if next < 0 || p.endTick < next {
			next = p.endTick
		}
	}
	return next, next >= 0
}

// takeBatch 推进时钟到 t，原子地取出该 tick 应放行的任务 ID（按 ID 排序），
// 并把它们标记为 fired。
func (r *DeterministicRunner) takeBatch(t int64) []string {
	r.mu.Lock()
	r.clock = t
	var batch []string
	for id, p := range r.plans {
		if p.endTick == t && !p.fired {
			if _, done := r.terminal[id]; !done {
				batch = append(batch, id)
				p.fired = true
				r.plans[id] = p
			}
		}
	}
	r.mu.Unlock()
	sort.Strings(batch)
	return batch
}

func (r *DeterministicRunner) waitTerminal(id string, max time.Duration) {
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		_, done := r.terminal[id]
		r.mu.Unlock()
		if done {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// AdmitOrder 返回确定性的准入顺序快照。
func (r *DeterministicRunner) AdmitOrder() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.admitOrder))
	copy(out, r.admitOrder)
	return out
}

// TerminalStatuses 返回 id -> 终态。
func (r *DeterministicRunner) TerminalStatuses() map[string]Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]Status, len(r.terminal))
	for k, v := range r.terminal {
		out[k] = v
	}
	return out
}

// 保证 time 包仍被引用（超时任务在真实时钟测试中使用）。
var _ = time.Millisecond
