# 进程内并发任务调度与资源配额组件

一个零依赖的 Go 库，提供**并发度 + 总资源**双维度配额、**多业务组（多租户）
配额隔离**、组间公平与组内优先级防饥饿调度、完整生命周期管理与故障隔离。
所有调度决策都打印决策依据与资源快照（含所属业务组），并提供**虚拟时钟**
驱动的确定性模拟器，可严格验证调度结论在相同输入与随机种子下逐字节可复现
——这份可复现同样覆盖到分组这一层。

## 1. 快速开始

### 1.1 单池用法（与旧版完全兼容）

不配置 `Groups`、任务不带 `Group` 时，行为与原来一致：所有任务都落在一个
无组级上限的默认组里，只受全局上限约束。

```go
s := scheduler.New(scheduler.Config{
    MaxConcurrency: 4,    // 最多 4 个任务同时运行
    ResourceLimit:  100,  // 所有运行中任务 Cost 之和 <= 100（-1 表示无界）
    QueueCapacity:  1024, // 等待队列容量；<=0 表示无界
}).WithLogger(myLogger)

h, err := s.Submit(ctx, &scheduler.Task{
    ID:       "job-1",
    Cost:     25,                 // 声明资源开销
    Priority: scheduler.PriorityHigh,
    Timeout:  3 * time.Second,    // 可选任务级超时
    Fn: func(ctx context.Context) error {
        // 业务逻辑；应尊重 ctx 以便及时响应取消/超时
        return nil
    },
})
if err != nil {
    var re *scheduler.RejectError
    if errors.As(err, &re) {
        // re.Reason 是可区分的拒绝原因，任务绝不被静默丢弃
    }
}
<-h.Done()
s.Shutdown(ctx)
```

### 1.2 多业务组用法

```go
s := scheduler.New(scheduler.Config{
    MaxConcurrency: 16,   // 全局并发上限（所有组之和的硬顶）
    ResourceLimit:  200,  // 全局资源上限（所有组之和的硬顶）
    QueueCapacity:  4096, // 全局共享的排队区
    Groups: []scheduler.GroupConfig{
        {Name: "payments", MaxConcurrency: 4, ResourceLimit: 80},
        {Name: "reports",  MaxConcurrency: 8, ResourceLimit: 120},
        // MaxConcurrency<=0 / ResourceLimit<0 表示该维度不设组级上限
    },
})

h, err := s.Submit(ctx, &scheduler.Task{
    ID: "p-1", Group: "payments", Cost: 20, Priority: scheduler.PriorityHigh,
    Fn: func(ctx context.Context) error { /* ... */ return nil },
})
```

运行期还可 `s.UpsertGroup(...)` 动态注册新组或调整已有组预算、
`s.SetGlobalLimits(...)` 调整全局预算、`s.ShutdownGroup(ctx, "reports")`
只关停某个业务组。


## 2. 调度模型

### 2.1 核心组件

| 组件 | 文件 | 职责 |
|------|------|------|
| `Scheduler` | `scheduler.go` | 准入、派发、组注册表、生命周期、按组/全局关闭与容量回收 |
| `quota` | `resource.go` | 单层账本（全局或某个组）：并发度与总资源两本账的精确记账 |
| `groupQuota` | `resource.go` | 一个业务组的独立账本 + 停收状态 + 本组在运行任务 WaitGroup |
| `charge` | `scheduler.go` | 一次准入的计费票据，支撑"精确释放一次"与关闭期强制回收去重 |
| `waitQueue` | `queue.go` | 组间轮转（round-robin）+ 组内优先级/老化防饥饿等待队列 |
| `Task` / `Handle` | `task.go` | 任务声明（含 `Group`）、取消句柄、入队原因、幂等结算 |
| `DeterministicRunner` | `simclock.go` | 虚拟时钟确定性执行器（记录"组/任务"准入序列） |
| `GenerateWorkload` | `simload.go` | 基于随机种子生成可复现模拟负载 |

### 2.2 同步模型（为什么账目不会坏）

- **单一互斥锁 `mu`**：所有全局与各组配额记账、等待队列操作都在同一临界区
  完成，"准入检查 + 记账 + 出队决策"不可分割，不存在 TOCTOU 竞态。
- **唯一派发者**：一个 `dispatchLoop` goroutine 串行地决定"谁现在可以运行"，
  这是调度顺序可解释、可复现的基础。
- **计费票据 `charge`**：每个被准入的任务恰好一张票据，同时挂在全局账本与
  所属组账本上。任务正常退出与关闭期强制回收两条路径竞争同一张票据，
  靠票据上的 `freed` 标志（mu 内）去重，保证容量精确释放一次。
- **任务 goroutine 只跑用户代码**：准入在锁内记账，执行在锁外，
  结算时重新取锁归还票据。每个任务 goroutine 与结算路径一一对应。
- **条件变量谓词等待**：派发循环在谓词循环中 `Wait`，
  即使信号发生在"检查与入睡前"的窗口也不会漏唤醒。

### 2.3 分组配额模型（两层硬约束）

准入一个属于组 `g`、开销 `cost` 的任务，必须**同时**满足三层检查：

```
group[g].inFlight < group[g].maxConcurrency   （组级并发，<=0 表示该维度无界）
group[g].used+cost <= group[g].resourceLimit  （组级资源，<0 表示该维度无界）
global.inFlight < global.maxConcurrency       （全局并发，始终生效）
global.used+cost <= global.resourceLimit      （全局资源，-1 表示无界）
```

- 全局上限**始终**是硬顶；组上限是叠加在其上的**额外**硬约束。
- 每个组只能花自己的预算：A 组用满后，哪怕全局还空着，A 的任务也只能排队，
  绝不能动用 B 组的余量。各组账本独立计数，互不可见、互不可借。
- 排队区（`QueueCapacity`）是**全局共享**的有限背压资源，不属于任何一个组；
  它满了以后，任何组的新任务都按 `QUEUE_FULL` 拒绝。
- 空串 `""` 是始终存在的**默认组**，不设组级上限——这正是单池兼容映射：
  旧代码不带 `Group` 的任务全部落进默认组。

**入队时被挡住的确切原因**可由 `Handle.WaitReason()` 区分（注意：排队不是
拒绝，任务已安全进入队列，绝不会被悄悄丢掉）：

| WaitReason | 含义 |
|------------|------|
| `GROUP_BUDGET` | 本组并发或资源暂时不足（全局是否有余量都先算本组） |
| `GLOBAL_BUDGET` | 本组有余量，但全局并发或资源不足 |
| `NONE` | 非预算原因排队（如调度器 `Pause`，或排队区有等待者需按公平顺序派发） |

### 2.4 组间公平 + 组内优先级防饥饿

调度派发槽位时分两层裁决：

1. **组间：轮转（round-robin）**。维护一个"上一个拿到槽位的组"游标，
   每发一个槽位就从下一个注册组开始扫描。于是任何一个业务方——哪怕它持续
   提交高优先级任务——都无法连续越过其它组拿槽位，其它组不会被饿死。
   某组当前没有任何可满足任务时立即跳过，游标不会空转丢失。
2. **组内：有效优先级 + 老化**。轮到某组时，在该组内按
   **有效优先级 = 基础优先级 + 老化扫描次数（age）** 降序、同值按入队序号
   FIFO 挑选第一个现在装得下的任务。组内高优先级先拿资源；等太久的低优先级
   任务 age 累积后会被逐步抬上来（老化在所有组上统一发生）。
3. **队头不阻塞**：某组队首大任务当前装不下时，该组内更小的任务可以先走，
   系统不会被一个大任务完全卡死。

组间轮转与组内老化是两条正交的规则：轮转保证"每个组都有机会"，老化保证
"组内每个任务最终都有机会"。

### 2.5 准入决策与可区分的拒绝原因

任务提交后按以下顺序显式判定，任何不满足都**显式返回带 `Group` 的
`RejectError`**，绝不静默丢弃：

1. `SCHEDULER_STOPPED`：调度器整体已关闭；
2. `UNKNOWN_GROUP`：引用了一个未注册（也不是默认组）的业务组；
3. `GROUP_STOPPED`：该业务组已被单独关闭（其它组照常服务）；
4. `EXCEEDS_HARD_LIMIT`：`Cost` 超过全局资源硬上限，任何时刻都满足不了；
5. `EXCEEDS_GROUP_LIMIT`：`Cost` 超过所属组的组级资源硬上限；
6. `QUEUE_FULL`：全局共享排队区已满；
7. 否则进入等待队列（或在当前可立即满足、且排队区已排空时直接准入）。

## 3. 配额核算规则

`quota` 是单层账本，全局与每个组各有一本；准入时对"所属组账本 + 全局账本"
各记一次，释放时各还一次。单层账本内：

```
inFlight      < maxConcurrency       （<=0 表示该维度无界，跳过检查）
resourceUsed + cost <= resourceLimit （<0 表示该维度无界，跳过检查）
```

- 准入：`inFlight++`、`resourceUsed += cost`（组账本与全局账本同步记账）；
- 释放：`inFlight--`、`resourceUsed -= cost`，组与全局同步归还。

### 3.1 运行期调整上限

- `SetGlobalLimits(conc, res)` 与 `UpsertGroup(GroupConfig{...})` 都在
  `mu` 内改上限并广播唤醒派发循环。
- **扩容**：等待中的任务在下一个派发节拍立刻被重新评估并继续前进，无需重建
  调度器；运行期新注册的组立即可用。
- **缩容不抢占**：已经在跑的任务不会被打断，因此 `used` 允许**暂时高于**新
  上限并随任务结束自然排空；而排空期间 `canAdmit` 恒为 false，**绝无任何新
  任务能把新上限顶穿**。账目任何时刻都自洽（见
  `TestRuntimeGlobalLimitsShrinkGrow`）。
- 已单独关闭的组不能用 `UpsertGroup` 复活（避免"停收→复活"的语义歧义）。

**精确一次释放保证**：

- 只有真正占用资源的任务（运行路径或关闭期回收路径）才处理一张 `charge`
  票据；排队取消、关闭摘队列的任务从未记账，因此不释放。
- 票据的 `freed` 标志在 `mu` 内去重：任务正常退出与关闭期强制回收竞争同一张
  票据时，只会有一方真正归还组+全局容量。
- `Handle.finish` 用关闭 `done` 通道的幂等模式保证终态只结算一次。
- 若因任何 bug 导致重复释放，账本会立即 `panic`（出现负数即账目损坏信号），
  而不是静默把账做坏。见 `TestDoubleReleaseGuardsAccount`。

任意时刻可用 `s.Snapshot()` 获取一致快照（全局账目 + 每个组的组级账目）：

```
snapshot{global concurrency=2/16 resource=30/200 queued=7 paused=false stopped=false groups=[{- concurrency=0/inf resource=0/inf queued=0 stopped=false} {payments concurrency=1/4 resource=20/80 queued=3 stopped=false} {reports concurrency=1/8 resource=10/120 queued=4 stopped=false}]}
```

（快照里 `-` 即空串默认组；`inf` 表示该维度无界。）

## 4. 生命周期与故障隔离

| 场景 | 行为 |
|------|------|
| 排队中取消 | 从等待队列摘除、结算为 `CANCELED`，**不执行用户代码、不占资源** |
| 运行中取消 | 通过 ctx 通知任务；任务退出后票据释放，组+全局容量照常归还 |
| 任务超时 | `Task.Timeout` 到期，结算为 `TIMED_OUT` |
| 暂停 / 恢复 | `Pause` 后新任务与已排队任务等待，运行中任务不受影响；`Resume` 继续 |
| 整体优雅关闭 | `Shutdown`：拒绝所有组新任务、取消全部排队任务、等待在运行任务结束或 ctx 到期 |
| 按组关闭 | `ShutdownGroup(ctx, g)`：**只**让组 g 停收并取消/清空 g 的排队任务；其它组的提交、排队、执行完全不受影响 |
| 宽限期满容量回收 | 在运行任务无视取消一直卡着、超过关闭给的最大等待时间后，其票据被强制结算为 `ABANDONED` 并**精确一次收回组+全局容量**；调度器可继续复用（按组关闭时其它组照常服务） |
| 迟到结算 | 被放弃的任务 goroutine 若日后真正退出，会被识别为"容量已回收"而**不重复释放、不出现负数**（记 `RELEASE-LATE`） |
| 出结果即腾容量 | 任务结算是在同一段 `mu` 临界区内先归还容量再解锁并 Signal，下游在解锁的第一时点就能看到空出来的容量，无"出结果 vs 容量还没还"的空窗 |
| 任务 panic | panic 屏障就地 `recover`，结算为 `PANICKED` 并打印栈；不逃逸、不毁账、不影响他人 |
| 永久阻塞 | 忽略 ctx 的任务不会破坏账目；配合关闭宽限期可强制回收容量，`Shutdown` 不因它死锁 |

### 4.1 关闭与容量回收语义

- `Shutdown(ctx)` / `ShutdownGroup(ctx, g)` 先同步完成"停收 + 摘队列 + 取消
  在运行任务的 ctx"，然后等待本组（或全局）的在运行票据归零，或 `ctx` 到期。
- `ctx` 正常到期（任务都配合退出）：返回 `nil`。
- `ctx` 超时仍有任务不退出：把这些仍占用容量的票据**按任务 ID 确定序**逐个
  标记 `ABANDONED`（`Handle.Err()` 为 `ErrTaskAbandoned`）并回收容量，
  函数返回 `ctx.Err()`（通常是 `context.DeadlineExceeded`）。此后：
  - 全局关闭时调度器已停收，可安全丢弃并在同进程 `New` 一个新实例；
  - 按组关闭时只是该组停收，其它组与调度器整体仍可继续使用；
  - 卡住的 goroutine 与账目彻底脱钩，不会重复释放或把账做成负数。

终态互斥归类于 `classify`：
`PANICKED > TIMED_OUT > CANCELED > FAILED > SUCCEEDED`；
`ABANDONED` 与 `REJECTED` 是不经过用户结果归类的特殊终态（分别由回收路径与
准入拒绝路径直接给出）。

### 4.2 长期运行不随完成任务数膨胀

- 排队任务的取消监听协程在任务**被准入的瞬间**即退出（`Handle.admitCh`
  关闭），因此不会随已完成任务数堆积 goroutine；
- 已完成任务的票据在结算时从内部 `charges` 表删除，等待队列条目随出队移除，
  不保留随任务数增长的内部结构。
- 见 `TestNoResourceGrowthOverLifetime`：多批共上千个分组任务后 goroutine 数
  与稳态堆不随完成数上涨，关闭后派发协程退出。

## 5. 决策日志

注入实现了 `Logf(format, args...)` 的 `Logger` 后，每次准入 / 排队 /
拒绝 / 释放 / 暂停 / 关闭 / 调额 / 强制回收都会打印**动作、所属业务组、
对象、开销、判定依据与决策后快照（含各组账目）**：

```
DECISION action=ENQUEUE group="payments" task=p-7 cost=20 basis="wait-reason=GROUP_BUDGET paused=false priority=10 seq=5" | snapshot{global concurrency=4/16 resource=80/200 queued=1 ... groups=[{payments concurrency=4/4 resource=80/80 queued=1 ...} ...]}
DECISION action=ADMIT group="reports" task=r-2 cost=7 basis="dispatch: rr-after="payments" effective-priority=10 seq=6 fits group+global quota" | snapshot{...}
DECISION action=RELEASE group="payments" task=p-3 cost=20 basis="terminal=SUCCEEDED err=<nil>; quota returned exactly once" | snapshot{...}
DECISION action=REJECT group="reports" task=big cost=99 basis="cost exceeds global hard limit" | snapshot{...}
DECISION action=RECLAIM group="reports" task=stuck cost=5 basis="grace period elapsed (context deadline exceeded); capacity force-reclaimed, task marked ABANDONED" | snapshot{...}
```

每条记录都带 `group=`，方便按业务方复盘；被放弃任务迟到退出时记
`RELEASE-LATE`（不再归还容量）。测试通过 `testLogger{t}` 把这些决策写入
`go test -v` 输出。

## 6. 可复现验证方法（关键）

真实 wall-clock 下任务"何时执行完"取决于 OS 调度，释放时机会改变后续派发
顺序。为严格验证**相同输入 + 相同随机种子 ⇒ 调度结论一致**，本组件提供
虚拟时钟驱动的 `DeterministicRunner`（`simclock.go`）：

1. `GenerateWorkload(seed, n)` 用固定种子的 `math/rand` 生成任务序列
   （普通 / 大开销 / 超时 / panic / 永久阻塞五类，分布见源码注释）；
   分组场景再按任务序号确定性轮转分配业务组；
2. 运行前**冻结异步派发**，所有提交只入队；
3. 准入由测试线程通过 `dispatchOneLocked` 在调度器锁内**同步串行**驱动，
   goroutine 调度时序完全不影响准入决策；组间轮转游标也在锁内同步推进；
4. 任务被准入后阻塞在门闩（gate）上，运行器按"准入时刻 + 声明占用时长"
   计算结束 tick，到点一次性放行整批、等待整批结算到最终账目，再派下一批；
5. 同一 tick 结束的任务作为单次原子状态变化释放，杜绝"逐个释放的中间态"
   导致的分叉。

`TestReproducibleScheduling` 用相同负载跑两次，逐位比较**准入顺序**与
**每个任务的终态**完全一致；`TestReproducibleSchedulingWithGroups` 在多组、
组间轮转 + 组内排队压力下，逐位比较 **"组/任务"准入序列**（
`DeterministicRunner.AdmitGroupSequence()`）与终态完全一致，证明可复现覆盖到
分组这一层；`TestReproducibleSeed` 验证种子决定负载。

## 7. 运行测试（本地验证方法）

```bash
go build ./...                              # 编译
go vet ./...                                # 静态检查
go test -race -count=1 ./...                # 全量竞态检测（推荐）
go test -race -v -count=1 ./...             # 全量 + 打印每次准入/排队/拒绝/释放决策
go test -run TestGroup            .         # 分组隔离与可区分拒绝原因
go test -run 'TestInterGroup|TestIntraGroup' . # 组间公平 / 组内优先级
go test -run TestCapacityReusable .         # 任务出结果后容量立即可复用
go test -run TestShutdown         .         # 按组关闭与宽限期容量回收
go test -run TestRuntime          .         # 运行期调整上限（扩容/缩容）
go test -run TestReproducible     .         # 单池 + 分组两层可复现
go test -run TestNoResourceGrowth .         # 长期运行 goroutine/堆不随完成数增长
go test -run TestPanic            .         # panic 隔离（旧用例不回归）
go test -run TestHighConcurrency  .         # 高并发账目不变量看门狗（旧用例不回归）
```

测试覆盖：

- **新增分组行为**：组预算隔离（不能动用别人的余量）、组/全局两层入队原因、
  未注册组/组停收/组硬上限/全局硬上限/队列满五类拒绝、组间轮转公平、
  组内优先级与老化、任务结束容量立即可复用（同组与跨组）、按组关闭只影响本组、
  宽限期满容量回收（`ABANDONED`，账目归零、不重复释放）、回收后组件继续可用、
  运行期注册/扩容/全局缩容扩容、分组层确定性可复现、长期运行资源不膨胀；
- **既有用例不回归**：双维度配额、优先级顺序、组内老化防饥饿、排队/运行取消、
  超时、暂停/恢复、整体优雅关闭、panic 隔离、永久阻塞不毁账、重复释放拦截、
  句柄幂等结算、400 任务高并发账目不变量看门狗、随机种子可复现调度，全部保留。

## 8. 与原单池用法的兼容映射

| 旧概念 / API | 分组版本 |
|--------------|----------|
| `Task` 不带 `Group` | 自动归入空串默认组（无组级上限） |
| 只有全局 `MaxConcurrency` / `ResourceLimit` | 默认组不设组级账本约束，语义与旧版一致 |
| `ResourceLimit`（旧：`>=0`） | 语义不变；额外允许 `-1` 表示全局无界（组级同此约定，`<=0` 并发无界） |
| `Submit / Pause / Resume / Shutdown / Snapshot` | 签名与语义保持；日志与快照额外带组信息 |
| 新增（可选） | `Task.Group`、`Handle.Group()` / `WaitReason()`、`Config.Groups`、`GroupConfig`、`UpsertGroup`、`SetGlobalLimits`、`ShutdownGroup`、`StatusAbandoned` / `ErrTaskAbandoned`、`ReasonUnknownGroup` / `ReasonGroupStopped` / `ReasonExceedsGroupLimit` |
| 旧拒绝原因 | `SCHEDULER_STOPPED` / `QUEUE_FULL` / `EXCEEDS_HARD_LIMIT` 保留；`RejectError` 新增 `Group` 字段 |

旧代码无需任何改动即可工作（全部落默认组）；要做多租户隔离时只需声明组并给
任务打上 `Group`。

## 9. 设计边界

- 队列设为无界（`QueueCapacity<=0`）时，准入压力由调用方负责；需要背压时
  配置有界队列并处理 `QUEUE_FULL`。排队区是**全局共享**的，不按组切分；
  若需要"每组独立排队容量"，应在调用方按组做独立背压。
- 忽略 ctx 的任务无法被强制杀死（Go 没有安全杀 goroutine 的机制）；宽限期满
  后组件能在**账目层面**把容量收回并继续复用，但那个 goroutine 本身仍会泄漏
  到它自行退出——退出时走"迟到结算"，不影响账目。因此仍应让任务尊重 ctx。
- 组一旦被 `ShutdownGroup` 关闭即不可复活（只能新建调度器），这是为了让
  "停收 + 清队列 + 容量回收"的语义保持单一、可复盘。
- 组间公平是"每槽位轮转"而非"按容量加权"的公平；若某组没有可满足任务，
  该轮让给下一组，不空转。
