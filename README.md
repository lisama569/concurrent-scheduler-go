# 进程内并发任务调度与资源配额组件

一个零依赖的 Go 库，提供**并发度 + 总资源**双维度配额、优先级与防饥饿调度、
完整生命周期管理与故障隔离。所有调度决策都打印决策依据与资源快照，
并提供**虚拟时钟**驱动的确定性模拟器，可严格验证调度结论在相同输入与
随机种子下逐字节可复现。

## 1. 快速开始

```go
s := scheduler.New(scheduler.Config{
    MaxConcurrency: 4,    // 最多 4 个任务同时运行
    ResourceLimit:  100,  // 所有运行中任务 Cost 之和 <= 100
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

## 2. 调度模型

### 2.1 核心组件

| 组件 | 文件 | 职责 |
|------|------|------|
| `Scheduler` | `scheduler.go` | 准入、派发、生命周期、优雅关闭 |
| `quota` | `resource.go` | 并发度与总资源两本账的精确记账 |
| `waitQueue` | `queue.go` | 优先级队列 + 老化（aging）防饥饿 |
| `Task` / `Handle` | `task.go` | 任务声明、取消句柄、幂等结算 |
| `DeterministicRunner` | `simclock.go` | 虚拟时钟确定性执行器 |
| `GenerateWorkload` | `simload.go` | 基于随机种子生成可复现模拟负载 |

### 2.2 同步模型（为什么账目不会坏）

- **单一互斥锁 `mu`**：所有配额记账与等待队列操作都在同一临界区完成，
  "准入检查 + 记账 + 出队决策"不可分割，不存在 TOCTOU 竞态。
- **唯一派发者**：一个 `dispatchLoop` goroutine 串行地决定"谁现在可以运行"，
  这是调度顺序可解释、可复现的基础。
- **任务 goroutine 只跑用户代码**：准入在锁内记账，执行在锁外，
  结算时重新取锁归还资源。每个任务 goroutine 与结算路径一一对应。
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

## 3. 配额核算规则

准入必须**同时**满足两个硬约束（见 `quota.canAdmit`）：

```
inFlight      < MaxConcurrency
resourceUsed + cost <= ResourceLimit
```

- 准入：`inFlight++`、`resourceUsed += cost`；
- 释放：`inFlight--`、`resourceUsed -= cost`。

**精确一次释放保证**：

- 只有真正占用资源的任务（运行路径）才调用 `quota.release`；
  排队取消、关闭摘队列的任务从未记账，因此不释放。
- `Handle.finish` 用关闭 `done` 通道的幂等模式保证终态只结算一次。
- 若因任何 bug 导致重复释放，账本会立即 `panic`（出现负数即账目损坏信号），
  而不是静默把账做坏。见 `TestDoubleReleaseGuardsAccount`。

任意时刻可用 `s.Snapshot()` 获取一致快照：

```
snapshot{concurrency=2/4 resource=30/100 queued=7 paused=false stopped=false}
```

## 4. 生命周期与故障隔离

| 场景 | 行为 |
|------|------|
| 排队中取消 | 从等待队列摘除、结算为 `CANCELED`，**不执行用户代码、不占资源** |
| 运行中取消 | 通过 ctx 通知任务；任务退出后资源照常归还 |
| 任务超时 | `Task.Timeout` 到期，结算为 `TIMED_OUT` |
| 暂停 / 恢复 | `Pause` 后新任务与已排队任务等待，运行中任务不受影响；`Resume` 继续 |
| 优雅关闭 | 拒绝新任务、取消全部排队任务、等待在运行任务结束或 ctx 到期 |
| 任务 panic | panic 屏障就地 `recover`，结算为 `PANICKED` 并打印栈；不逃逸、不毁账、不影响他人 |
| 永久阻塞 | 忽略 ctx 的任务不会破坏账目；`Shutdown` 在 ctx 超时时返回而不死锁 |

终态互斥归类于 `classify`：`PANICKED > TIMED_OUT > CANCELED > FAILED > SUCCEEDED`。

## 5. 决策日志

注入实现了 `Logf(format, args...)` 的 `Logger` 后，每次准入 / 排队 /
拒绝 / 释放 / 暂停 / 关闭都会打印**动作、对象、开销、判定依据与决策后快照**：

```
DECISION action=ADMIT task=t-12 cost=7 basis="dispatch: effective-priority=10 seq=5 fits current quota" | snapshot{concurrency=3/4 resource=24/20 queued=12 paused=false stopped=false}
DECISION action=RELEASE task=t-3 cost=4 basis="terminal=SUCCEEDED err=<nil>; quota returned exactly once" | snapshot{concurrency=2/4 resource=20/20 queued=12 paused=false stopped=false}
DECISION action=REJECT task=big cost=99 basis="cost exceeds hard limit" | snapshot{concurrency=0/4 resource=0/20 ...}
```

测试通过 `testLogger{t}` 把这些决策写入 `go test -v` 输出。

## 6. 可复现验证方法（关键）

真实 wall-clock 下任务"何时执行完"取决于 OS 调度，释放时机会改变后续派发
顺序。为严格验证**相同输入 + 相同随机种子 ⇒ 调度结论一致**，本组件提供
虚拟时钟驱动的 `DeterministicRunner`（`simclock.go`）：

1. `GenerateWorkload(seed, n)` 用固定种子的 `math/rand` 生成任务序列
   （普通 / 大开销 / 超时 / panic / 永久阻塞五类，分布见源码注释）；
2. 运行前**冻结异步派发**，所有提交只入队；
3. 准入由测试线程通过 `dispatchOneLocked` 在调度器锁内**同步串行**驱动，
   goroutine 调度时序完全不影响准入决策；
4. 任务被准入后阻塞在门闩（gate）上，运行器按"准入时刻 + 声明占用时长"
   计算结束 tick，到点一次性放行整批、等待整批结算到最终账目，再派下一批；
5. 同一 tick 结束的任务作为单次原子状态变化释放，杜绝"逐个释放的中间态"
   导致的分叉。

`TestReproducibleScheduling` 用相同负载跑两次，逐位比较**准入顺序**与
**每个任务的终态**完全一致；`TestReproducibleSeed` 验证种子决定负载。

## 7. 运行测试

```bash
go test -race -v ./...          # 全量竞态检测 + 决策日志
go test -run TestPriority .     # 仅优先级 / 防饥饿
go test -run TestReproducible . # 可复现性
go test -run TestPanic .        # panic 隔离
```

测试覆盖：双维度配额约束、三类拒绝原因、队列满、优先级顺序、老化防饥饿、
排队/运行取消、超时、暂停/恢复、优雅关闭、panic 隔离、永久阻塞不毁账、
重复释放拦截、句柄幂等结算、400 任务高并发账目不变量看门狗，以及
随机种子负载下的可复现调度。

## 8. 设计边界

- 队列设为无界（`QueueCapacity<=0`）时，准入压力由调用方负责；需要背压时
  配置有界队列并处理 `QUEUE_FULL`。
- 忽略 ctx 的任务无法被强制杀死（Go 没有安全杀 goroutine 的机制）；
  组件保证其账目在 goroutine 真正退出时被精确释放，且不影响其他任务与关闭流程。
