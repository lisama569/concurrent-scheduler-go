# 进程内并发任务调度与资源配额组件

一个纯 Go（无第三方依赖）的进程内调度器，提供**任务准入与多维资源配额核算**、
**带防饥饿的优先级调度**、以及**完整的生命周期与故障隔离**。
配套一套本地可复现的模拟负载：相同输入与随机种子下，调度决策逐事件可复现；
不同种子下，账目守恒、拒绝原因可区分、故障被隔离的结论稳定成立。

## 目录结构

```
scheduler/          调度器核心
  errors.go         可区分的拒绝原因与错误
  resources.go      资源/容量/快照类型与核算原语
  task.go           任务记录、状态机、句柄
  queue.go          带"年龄提升"的优先级堆
  ledger.go         资源账目（admit/release 精确一次 + 不变量自检）
  scheduler.go      准入、排队、扫描、结算、暂停/关闭（共享核心）
  real.go           真实并发回路（后台 dispatcher + 任务 goroutine）
  virtual.go        单协程确定性虚拟引擎（可复现实验）
  clock.go          真实时钟 / 手动推进的虚拟时钟
  logger.go         结构化决策事件日志
simulation/         种子驱动的模拟负载与结论汇总
cmd/simulate/       命令行复现工具
```

## 调度模型

### 1. 任务提交与三种去向

任务通过 `Submit(TaskSpec)` 提交，必须声明三维资源开销 `Cost{CPU,Memory,IO}`、
优先级（0–15，越大越高）、以及可选的排队/运行超时。提交后只有三种去向，
**任何任务都不会被静默丢弃**：

1. **直接拒绝**（返回 `*scheduler.RejectError`，可用 `errors.Is` 匹配原因）：
   - `task_too_large`：任务自身开销超过总容量，永远无法容纳，不进入排队；
   - `invalid_spec`：Fn 为 nil、资源为负、优先级越界；
   - `shutting_down`：调度器关闭中或已关闭。
2. **排队等待**（返回有效 Handle、无错误）：任务合法但当前**并发度**或
   **某一维资源**暂时不足。决策日志中记录可区分的依据：
   - `over_concurrency`：并发度 `MaxTasks` 已满；
   - `over_resource`：并发度有余量，但 CPU/Memory/IO 至少一维放不下。
   排队任务可设置 `QueueTimeout`，超时后以 `timed_out` 终态结算（从未占用资源）。
3. **立即准入**：资源与并发度同时满足，计入账目并开始执行。

### 2. 双重配额约束

准入必须**同时**满足（见 `ledger.canAdmit`）：

- 并发度：`running < MaxTasks`；
- 三维资源：对每一维 `inUse[d] + cost[d] <= Max[d]`。

两约束相互独立。准入选择时按有效优先级从高到低扫描第一个"当前放得下"的任务，
因此队头大任务暂时放不下时，不会阻塞其后放得下的小任务（无队头阻塞）。

### 3. 优先级与公平（防饥饿）

- 调度键为 `(effectivePriority desc, id asc)`：高优先级先得资源，
  同优先级按提交顺序 FIFO。
- 每个排队任务按**已等待的 tick 数**累计年龄；达到 `StarvationAge` 后
  被临时提升到高于所有普通优先级的有效优先级（`aging` 决策会写入日志），
  直到它被准入或取消。这样即使高优先级任务流持续到达，低优先级任务的
  等待时间也有上界，不会无限期饥饿。
- 年龄基于时钟时间计算（而非扫描次数），真实时钟与虚拟时钟语义一致。

### 4. 生命周期与故障隔离

- **取消**：排队中任务立即移出队列并释放（未占用资源）；运行中任务取消其
  context，资源等任务体退出后释放。重复取消/重复等待幂等。
- **超时**：`RunTimeout` 到期先取消 context 并给予 `RunGrace` 宽限；
  配合退出的任务以 `timed_out` 结算。
- **失控（runaway）隔离**：宽限期满仍不退出（无视取消、长时间阻塞）的任务，
  调度器**先行释放其账目占用**并把句柄置为 `timed_out` 终态，槽位立即可被
  其他任务复用；失控 goroutine 脱离账目，之后若退出不会二次结算。
- **panic 隔离**：任务体 panic 在该任务自己的 goroutine 中被 recover，
  以 `panicked` 结算并释放资源，调度器与其他任务不受影响。
- **暂停/恢复**：`Pause` 期间不放行新准入（运行中的不受影响），`Resume` 恢复。
- **优雅关闭**：`Shutdown(ctx)` 后拒绝新提交，已在管任务继续排空；
  ctx 到期则强制取消剩余任务，再经一个宽限期回收仍不退出者，保证
  `Shutdown` 不会被失控任务无限阻塞。重复调用返回 `ErrClosed`。

### 5. 资源精确释放一次

- 每个任务用 `admitted`/`released` 标志保护：`ledger.release` 只调用一次。
- 每次释放后执行账目不变量自检（不允许负占用、不允许超容量），
  一旦被破坏会 panic 并留下带快照的决策日志——这属于程序缺陷而非可恢复错误。
- 正常退出、失败、panic、取消、超时、runaway、关闭回收共用同一条结算路径。

## 可观测性：决策日志

调度器接受一个 `EventLogger`（内置线程安全的 `SliceLogger`），对每次
**准入、排队、拒绝、释放、取消、超时、panic、runaway、关闭**记录一条事件，
包含：

- 决策类型、任务、优先级、资源开销、拒绝原因；
- 人类可读的 `Decision`（为什么准入/不放行/释放）；
- 决策**后**的完整资源快照 `Snapshot`（running/queued/三维占用/各终态计数）。

`go test -v` 会打印完整决策日志；CLI 用 `-verbose` 打印。

## 可复现验证

调度器有两种执行回路，共享同一套准入/结算核心：

- **真实回路**（`New`）：后台 dispatcher + 每任务独立 goroutine，用于
  高并发压力与真实竞争场景（测试在 `-race` 下运行）。
- **虚拟确定性回路**（`NewVirtual` + `VirtualEngine`）：单协程、手动推进的
  虚拟时钟，任务只声明时长与异常类型而不执行真实函数。计时器按
  `(截止时间, 序号)` 严格定序触发，因此**相同输入与种子下事件序列逐字段一致**。

### 运行测试

```bash
go test ./...                 # 全部单测
go test -race ./scheduler     # 竞态检测（高并发/重复释放/取消链路）
go test -v ./scheduler        # 查看每次准入/释放决策依据与资源快照
```

覆盖的关键场景：

| 测试 | 验证结论 |
| --- | --- |
| `TestTaskTooLargeRejected` / `TestInvalidSpec` | 超容量/非法规格按原因拒绝，不排队 |
| `TestOverResourceQueues` / `TestOverConcurrencyQueues` | 资源/并发不足排队等待且原因可区分 |
| `TestConcurrencyAndResourceCaps` | 双约束永不越界，结束后自动补位 |
| `TestPriorityOrderNoStarvation` | 高优先级优先；等待有界后低优先级被提升、不饥饿 |
| `TestDuplicateRelease` | 并发重复取消恰好一次生效、资源只释放一次 |
| `TestPanicIsolation` | panic 被隔离，账目平衡，后续任务照常运行 |
| `TestRunTimeoutCooperativeTask` / `TestRunawayTaskLedgerReclaimed` | 超时语义；失控任务账目先行释放、槽位复用 |
| `TestPauseResume` / `TestQueueTimeout` / `TestGracefulShutdown` / `TestShutdownForcedCancel` | 暂停、排队超时、优雅/强制关闭 |
| `TestHighConcurrencyStress` | 400 任务并发提交+随机取消，账目永不越界且最终归零，无任务丢失 |
| `TestVirtualReproducible` | 同种子两次运行事件逐字段相等；优先级/超时/panic/runaway/排队超时结论正确 |
| `simulation.TestSameSeedReproducible` / `TestCrossSeedConclusionsHold` | 种子复现 + 多种子下账目守恒 |

### 命令行模拟

```bash
go run ./cmd/simulate -seed 42 -tasks 120
go run ./cmd/simulate -seed 42 -tasks 300 -max-tasks 2 -cpu 8 -mem 8 -io 8 -verbose
```

输出每个终态桶的计数、按原因的拒绝分布、最终账目，以及两个核心结论：

- `ledger_balanced=true`：终态 running=queued=三维占用=0；
- `admission_conserved=true`：每个获得过资源的任务恰好在
  completed/canceled/runTimeout/panicked 中结算一次
  （runaway 是 runTimeout 的失控子集；queueTimeout 从未准入，不计入）。

相同种子连续两次运行汇总一致；`-verbose` 可对照逐条决策与快照。
