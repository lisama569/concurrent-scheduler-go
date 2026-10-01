# 进程内并发任务调度与资源配额组件

一个零依赖的 Go 库，提供**并发度 + 总资源**双维度配额、**多业务组配额隔离
（每个组独立的并发/资源预算）**、组间轮转公平与组内优先级防饥饿调度、
完整生命周期管理与故障隔离。所有调度决策都打印**所属业务组**、决策依据与
资源快照，并提供**虚拟时钟**驱动的确定性模拟器，可严格验证调度结论在相同
输入与随机种子下逐字节可复现（可复现覆盖到分组这一层）。

## 1. 快速开始

```go
// 可选地为各业务方注册独立的并发/资源预算
s := scheduler.New(scheduler.Config{
    MaxConcurrency: 16,   // 全局最多 16 个任务同时运行
    ResourceLimit:  200,  // 全局所有运行中任务 Cost 之和 <= 200
    QueueCapacity:  4096, // 全局等待队列容量；<=0 表示无界
},
    scheduler.GroupConfig{Name: "payments", MaxConcurrency: 4, ResourceLimit: 80},
    scheduler.GroupConfig{Name: "search",   MaxConcurrency: 8, ResourceLimit: 120},
).WithLogger(myLogger)

h, err := s.Submit(ctx, &scheduler.Task{
    ID:       "job-1",
    Group:    "payments",            // 所属业务组；留空则为内建默认组（仅受全局约束）
    Cost:     25,                    // 声明资源开销
    Priority: scheduler.PriorityHigh,
    Timeout:  3 * time.Second,       // 可选任务级超时
    Fn: func(ctx context.Context) error {
        // 业务逻辑；应尊重 ctx 以便及时响应取消/超时
        return nil
    },
})
if err != nil {
    var re *scheduler.RejectError
    if errors.As(err, &re) {
        // re.Reason 是可区分的拒绝原因（含 UNKNOWN_GROUP / GROUP_STOPPED 等），
        // re.Group 标明被拒绝任务属于哪个组；任务绝不被静默丢弃
    }
}
<-h.Done()

// 运行期也能加/调组：s.ConfigureGroup(scheduler.GroupConfig{Name: "billing", ...})
// 只关一个组、其他组继续：s.CloseGroup("search", shutdownCtx)
s.Shutdown(ctx)
```

## 2. 调度模型

### 2.1 核心组件

| 组件 | 文件 | 职责 |
|------|------|------|
| `Scheduler` | `scheduler.go` | 准入、派发、生命周期、全局/单组优雅关闭与容量回收 |
| `quota` | `resource.go` | 全局并发度与总资源两本账的精确记账 |
| `groupState` / `GroupConfig` | `group.go` | 单个业务组的两本账、在运行集合与独立等待计数 |
| `groupRegistry` | `registry.go` | 组注册表 + **组间轮转（round-robin）公平** |
| `groupQueue` | `group_queue.go` | 组内优先级队列 + 老化（aging）防饥饿 |
| `Task` / `Handle` | `task.go` | 任务声明（含 Group）、取消句柄、阻塞原因、幂等结算 |
| `DeterministicRunner` | `simclock.go` | 虚拟时钟确定性执行器（支持分组） |
| `GenerateWorkload` / `AssignGroups` | `simload.go` | 基于随机种子生成可复现负载并确定性分组 |

### 2.2 同步模型（为什么账目不会坏）

- **单一互斥锁 `mu`**：全局账、每个组的组账、组注册表与各组队列操作都在
  同一临界区完成，"准入检查 + 两本账记账 + 出队决策"不可分割，不存在
  TOCTOU 竞态。
- **唯一派发者**：一个 `dispatchLoop` goroutine 串行地决定"哪个组的哪个任务
  现在可以运行"（组间轮转游标 + 组内全序排序），这是调度顺序可解释、
  组间不饿死且可复现的基础。
- **任务 goroutine 只跑用户代码**：准入在锁内同时记全局账与组账，执行在
  锁外，结算时重新取锁同时归还两本账。每个任务 goroutine 与结算路径一一对应。
- **条件变量谓词等待**：派发循环在谓词循环中 `Wait`，
  即使信号发生在"检查与入睡前"的窗口也不会漏唤醒。

### 2.3 准入决策与三种可区分的拒绝原因

任务提交后按以下顺序判定，任何不满足都**显式返回 `RejectError`**：

1. `SCHEDULER_STOPPED`：调度器已关闭，不再接受新任务；
2. `EXCEEDS_HARD_LIMIT`：`Cost > ResourceLimit`，即使系统完全空闲也永远
   无法满足，立即拒绝（而非无意义地永久排队）；
3. `QUEUE_FULL`：有界等待队列已满，无法排队；
4. 否则进入等待队列（或在当前可立即满足时直接准入）。

### 2.4 优先级与防饥饿

- 等待队列按 **有效优先级 = 基础优先级 + 老化扫描次数（age）** 排序，
  同有效优先级按入队序号 FIFO（`sort.SliceStable`，全序确定）。
- 高优先级任务优先获得资源；但每当派发循环因"当前无人可满足"而入睡前，
  所有等待者的 age 加一。持续涌入的高优先级任务因此**不能**让低优先级
  任务无限期等待——老任务的有效优先级最终会反超。见 `TestNoStarvationWithAging`。
- **队头不阻塞**：若队首大任务当前装不下，能装下的较低优先级任务可以先走，
  系统不会被一个大任务完全卡死。

### 2.5 分组配额模型（多业务方隔离）

一个进程内可同时服务多个业务方，每个业务方是一个**业务组（group）**。
任务通过 `Task.Group` 声明归属；组在创建调度器时或运行期经
`ConfigureGroup` 注册：

```go
s := scheduler.New(
    scheduler.Config{MaxConcurrency: 16, ResourceLimit: 200, QueueCapacity: 4096},
    scheduler.GroupConfig{Name: "payments", MaxConcurrency: 4, ResourceLimit: 80},
    scheduler.GroupConfig{Name: "search",   MaxConcurrency: 8, ResourceLimit: 120},
)

h, err := s.Submit(ctx, &scheduler.Task{
    ID: "p-1", Group: "payments", Cost: 20,
    Fn: func(ctx context.Context) error { /* ... */ return nil },
})
```

准入必须**同时**满足两本账（全局账 + 所属组账），组上限是全局上限之外的
**额外硬约束**：

```
# 全局维度（原有）
globalInFlight      < MaxConcurrency
globalResourceUsed + cost <= ResourceLimit
# 组维度（新增；该组设了上限的维度才检查）
groupInFlight       < GroupConfig.MaxConcurrency   // <=0 表示该维度不限
groupResourceUsed + cost <= GroupConfig.ResourceLimit
```

**一个组绝不能动用别的组的预算**：A 组把自己的并发/资源用满，只会让 A 组的
新任务排队，B 组在全局容量允许时照常准入（见 `TestGroupQuotaIsolation`）。
`Snapshot()` 除全局账目外，还返回 `Groups map[string]GroupSnapshot`，每组有
独立的 `InFlight / ResourceUsed / Queued / Stopped`，方便逐组对账。

**入队时即可区分的三种"被挡住"原因**（排队任务永不被静默丢弃，可用
`Handle.BlockReason()` 查询，日志也会打印）：

| BlockReason | 含义 |
|-------------|------|
| `GROUP_CONCURRENCY` | 本组并发名额不足（本组预算不够） |
| `GROUP_RESOURCE` | 本组资源预算不足（本组预算不够） |
| `GLOBAL_CONCURRENCY` | 本组尚有余量，但全局并发名额不足 |
| `GLOBAL_RESOURCE` | 本组尚有余量，但全局资源预算不足 |

提交阶段的显式拒绝则区分：`UNKNOWN_GROUP`（组未注册，不做隐式建组）、
`GROUP_STOPPED`（该组已被 `CloseGroup` 停收）、`GROUP_EXCEEDS_HARD_LIMIT`
（Cost 超过本组资源硬上限，永远装不下）、以及原有的 `QUEUE_FULL`
（**全局**排队区已满）、`EXCEEDS_HARD_LIMIT`、`SCHEDULER_STOPPED`。
每条准入 / 排队 / 拒绝 / 释放记录都带 `group=` 字段。

### 2.6 组间公平规则

- **组间：严格轮转（round-robin）。** 派发者内部维护一个按组名字典序排列的
  环形游标，每准入一个任务就把下一次机会让给环上的下一个组。于是即使某个
  业务方拼命提交 HIGH 任务，它也**无法连续霸占**全部准入机会，别的组不会被
  饿死（`TestInterGroupFairnessRoundRobin` 断言两组严格交替）。
- **组内：沿用单池规则。** 组内仍按"有效优先级 = 基础优先级 + 老化次数"排序，
  高优先级先拿资源；等太久的低优先级在组内会被逐步抬升
  （`TestIntraGroupPriorityStillHolds`、`TestIntraGroupAgingPreventsStarvation`）。
  老化只在组内抬优先级，不会让某个任务越过组间轮转边界。
- 组的遍历顺序由组名字典序 + 环形游标唯一确定，配合组内的全序排序，
  **分组层同样可复现**（`TestReproducibleGroupedScheduling`）。

### 2.7 与单池用法的兼容映射

| 单池用法 | 分组版本 |
|----------|----------|
| 不设置 `Task.Group` | 自动落入内建**默认组**（组名为空串 `DefaultGroup`），默认组**无组级配额**，只受全局上限约束 |
| `New(cfg)` | `New(cfg)` 签名不变（`groups ...GroupConfig` 为可选变长参数），行为与旧版逐位一致 |
| 全局 `Snapshot` 字段 | 全部保留；新增 `Groups` 字段，默认组也在其中 |
| `EXCEEDS_HARD_LIMIT / QUEUE_FULL / SCHEDULER_STOPPED` | 语义与返回值不变 |

既有不感知分组的代码无需任何修改即可继续工作（原有全部用例原样通过）。

### 2.8 运行期调整上限

- `s.ConfigureGroup(GroupConfig{...})`：新建组或调整已有组的并发/资源上限。
- `s.UpdateLimits(Config{...})`：调整全局并发/资源/队列容量。
- **加容量**：等待中的任务立刻获得继续前进的机会（派发者被唤醒）。
- **缩容量**：**不杀、不驱逐**任何在运行任务；`used` 可能瞬时高于新上限，
  期间该账只冻结新的准入，等在跑任务自然结算把账目压回新上限以内。
  新上限不会被在跑任务"顶穿"——每次准入都按新上限逐次把关，账目任何时刻
  都对得上（`TestRuntimeAdjustGroupLimits`、`TestRuntimeAdjustGlobalLimits`）。

## 3. 配额核算规则

准入必须**同时**满足全局两维（见 `quota.canAdmit`）与所属组两维
（见 `groupState.canAdmit`）四个硬约束中"被启用"的那些：

```
global inFlight      < MaxConcurrency
global resourceUsed + cost <= ResourceLimit
group  inFlight      < GroupConfig.MaxConcurrency      (若 >0)
group  resourceUsed + cost <= GroupConfig.ResourceLimit(若 >0)
```

- 准入：全局账与组账在**同一个临界区**一起 `admit`；
- 释放：结算路径在**同一个临界区**里同时归还全局账与组账。

**精确一次释放保证**：

- 只有真正占用资源的任务（运行路径）才归还账本；排队取消、关闭摘队列的
  任务从未记账，因此不释放。
- `Handle.finish` 用关闭 `done` 通道的幂等模式保证终态只结算一次；
  任务另有 `detached` 标记，区分"正常结算释放"与"关闭超时强制回收"。
- 若因任何 bug 导致重复释放或出现负数，全局账 / 组账都会立即 `panic`，
  而不是静默把账做坏。见 `TestDoubleReleaseGuardsAccount`。

任意时刻可用 `s.Snapshot()` 获取一致快照（全局账目 + 每组账目）：

```
snapshot{concurrency=2/16 resource=30/200 queued=7 paused=false stopped=false}
Groups: payments{concurrency=1/4 resource=20/80 queued=2} search{...}
```

### 3.1 任务出结果与容量腾出之间不存在空窗

下游任务常常在上游一结束就立刻提交。本组件保证：**结算时先在 `mu` 内归还
全局账与组账、再登记终态（关闭 `Done`）并唤醒派发者**。因此一旦 `Wait()` /
`Done()` 返回上游结果，空出来的容量必然已经在账上可见、且派发循环已被唤醒，
紧接着提交的下游能立即看到容量并有机会被调度，不会出现"上游明明有结果了，
下游却还因容量没还回来而干等"（`TestCapacityReusedImmediatelyOnResult`）。

## 4. 生命周期与故障隔离

| 场景 | 行为 |
|------|------|
| 排队中取消 | 从本组等待队列摘除、结算为 `CANCELED`，**不执行用户代码、不占资源** |
| 运行中取消 | 通过 ctx 通知任务；任务退出后全局账与组账照常一起归还 |
| 任务超时 | `Task.Timeout` 到期，结算为 `TIMED_OUT` |
| 暂停 / 恢复 | `Pause` 后新任务与已排队任务等待，运行中任务不受影响；`Resume` 继续 |
| 全局优雅关闭 | 所有组停收、取消全部排队与在运行任务，等待结束或 ctx 到期 |
| **单独关闭某组** | `CloseGroup(name, ctx)`：只停收该组、清空并取消该组排队任务、取消该组在运行任务；**其他组（含默认组）不受影响、继续服务** |
| **超过最大等待** | `Shutdown` / `CloseGroup` 的 ctx Deadline 即最大等待时间；超时后仍不理会取消的任务被**强制收回容量**并标记新终态 `ABANDONED`，组件可继续被复用/重启 |
| 任务 panic | panic 屏障就地 `recover`，结算为 `PANICKED` 并打印栈；不逃逸、不毁账、不影响他人 |
| 永久阻塞 | 忽略 ctx 的任务不会破坏账目；超最大等待后容量被回收，僵尸 goroutine 日后退出走"已脱管"分支，只摘登记、绝不重复释放 |

**容量回收与关闭语义（`ABANDONED`）**：

- 被放弃任务的容量在超时点从**全局账与组账两本账**上同时收回，组与全局
  `Snapshot` 立即归零，调度器可继续服务其他组或被重启复用；
- 任务被标记为 `StatusAbandoned`（`Handle.Wait()` 返回 `ErrTaskAbandoned`）；
- 该任务的 goroutine 若日后真正退出，因带 `detached` 标记，结算路径只会
  摘除运行登记而**不再触碰账目**——不会重复释放、不会出现负数
  （`TestShutdownReclaimsCapacityAfterMaxWait`、
  `TestCloseGroupReclaimsAndOthersSurvive`、`TestCloseGroupDrainsQueued`）。

终态互斥归类于 `classify`：`PANICKED > TIMED_OUT > CANCELED > FAILED > SUCCEEDED`；
`ABANDONED / REJECTED` 为准入/关闭阶段的独立终态。

**长期运行资源有界**：排队取消监听使用 `context.AfterFunc` 注册，并在任务
终态（正常结算或放弃）显式注销；正常任务 goroutine 退出即回收。因此协程与
内存随"在排队 + 在运行任务数"变化，**不随累计完成任务数增长**
（`TestNoGoroutineLeakAcrossManyTasks`、`TestQueuedCancelWatchersReclaimed`）。

## 5. 决策日志

注入实现了 `Logf(format, args...)` 的 `Logger` 后，每次准入 / 排队 /
拒绝 / 释放 / 暂停 / 关闭都会打印**动作、所属业务组、对象、开销、判定依据
与决策后快照**：

```
DECISION action=ADMIT group="payments" task=p-7 cost=20 basis="dispatch: group=payments effective-priority=10 seq=5 fits group & global quota" | snapshot{concurrency=3/16 resource=60/200 queued=12 paused=false stopped=false groups=3}
DECISION action=ENQUEUE group="payments" task=p-8 cost=20 basis="paused=false block=GROUP_RESOURCE priority=5 seq=6" | snapshot{...}
DECISION action=RELEASE-ABANDON group="payments" task=stuck cost=20 basis="shutdown max wait exceeded; capacity reclaimed; task detached, future exit will not touch accounts" | snapshot{...}
DECISION action=REJECT group="payments" task=big cost=99 basis="cost exceeds group hard limit" | snapshot{...}
```

测试通过 `testLogger{t}` 把这些决策写入 `go test -v` 输出。

## 6. 可复现验证方法（关键）

真实 wall-clock 下任务"何时执行完"取决于 OS 调度，释放时机会改变后续派发
顺序。为严格验证**相同输入 + 相同随机种子 ⇒ 调度结论一致**，本组件提供
虚拟时钟驱动的 `DeterministicRunner`（`simclock.go`）：

1. `GenerateWorkload(seed, n)` 用固定种子的 `math/rand` 生成任务序列
   （普通 / 大开销 / 超时 / panic / 永久阻塞五类，分布见源码注释）；
   需要分组时再用确定性的 `AssignGroups(specs, k)` 按位置分到 `g-0..g-(k-1)`；
2. 运行前**冻结异步派发**，所有提交只入对应组的队列；
3. 准入由测试线程通过 `dispatchOneLocked` 在调度器锁内**同步串行**驱动，
   组间轮转游标与组内排序都在该线程确定性推进，goroutine 时序不影响决策；
4. 任务被准入后阻塞在门闩（gate）上，运行器按"准入时刻 + 声明占用时长"
   计算结束 tick，到点一次性放行整批、等待整批结算到最终账目，再派下一批；
5. 同一 tick 结束的任务作为单次原子状态变化释放，杜绝"逐个释放的中间态"
   导致的分叉。

`TestReproducibleScheduling` 用相同负载跑两次，逐位比较**准入顺序**与
**每个任务的终态**完全一致；`TestReproducibleGroupedScheduling` 在三个业务组
各自配额偏紧的场景下做同样的逐位比较，证明**可复现覆盖到分组层**；
`TestGroupedAdmissionInterleavesGroups` 额外断言组间轮转确实交替了各组。

## 7. 运行测试（本地验证方法）

```bash
go test -race -v ./...                  # 全量竞态检测 + 决策日志
go test -run TestGroup ./...            # 分组隔离 / 未知组 / 组关闭
go test -run TestInterGroup ./...       # 组间轮转公平
go test -run TestIntraGroup ./...       # 组内优先级 / 老化
go test -run TestRuntimeAdjust ./...    # 运行期调整全局/组上限
go test -run TestReclaims ./...         # 超最大等待后容量回收
go test -run TestCapacityReused ./...   # 任务结束后容量立即可复用
go test -run TestReproducible ./...     # 单池 + 分组可复现性
go test -run TestNoGoroutineLeak ./...  # 长期运行协程有界
go test -run TestPanic ./...            # panic 隔离
```

测试覆盖：全局 + 组双维度配额隔离、四种排队阻塞原因、六类拒绝原因、队列满、
单池与组内优先级顺序、组内老化防饥饿、组间轮转防饿死、排队/运行取消、超时、
暂停/恢复、全局与单组优雅关闭、超时容量回收与僵尸不重复释放、panic 隔离、
永久阻塞不毁账、重复释放拦截、句柄幂等结算、运行期扩容/缩容账目不变量、
任务结束容量零窗复用、长期运行协程有界、400 任务高并发账目不变量看门狗，
以及单池与分组随机种子负载下的逐位可复现调度。

## 8. 设计边界

- 队列设为无界（`QueueCapacity<=0`）时，准入压力由调用方负责；需要背压时
  配置有界队列并处理 `QUEUE_FULL`。队列容量是**全局**共享的，组隔离的是
  并发/资源预算，而不是排队名额（组级排队容量可作为后续扩展）。
- 组在注册前不会被隐式创建：提交未知组返回 `UNKNOWN_GROUP`，避免任务悄悄
  落到错误预算池。默认组（`Task.Group == ""`）始终存在且不设组级上限。
- 缩容不驱逐在运行任务：`used` 可能瞬时高于新上限，这是被允许且可观测的，
  期间只冻结新准入直到账目自然回落。
- 忽略 ctx 的任务无法被安全强杀（Go 没有安全杀 goroutine 的机制）；超过
  最大等待后组件会**回收其容量并标记放弃**以保证自身可复用，泄漏的仅是那个
  不可中断的用户 goroutine 本身，账目与其他任务不受影响。
