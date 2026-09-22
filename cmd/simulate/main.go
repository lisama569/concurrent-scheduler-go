// simulate 用固定随机种子在本地复现高并发与异常场景下的调度结论。
//
// 用法：
//
//	go run ./cmd/simulate -seed 42 -tasks 120
//	go run ./cmd/simulate -seed 42 -verbose   # 打印每次准入/释放决策与资源快照
//
// 相同种子两次运行，决策日志逐事件一致；不同种子下账目守恒、
// 拒绝原因可区分、panic/失控任务被隔离。
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/lisama569/concurrent-scheduler-go/scheduler"
	"github.com/lisama569/concurrent-scheduler-go/simulation"
)

func main() {
	var (
		seed      = flag.Int64("seed", 42, "随机种子（同种子逐事件可复现）")
		tasks     = flag.Int("tasks", 120, "模拟任务数")
		maxTasks  = flag.Int("max-tasks", 4, "并发度上限")
		capCPU    = flag.Int64("cpu", 20, "CPU 总容量")
		capMem    = flag.Int64("mem", 20, "内存总容量")
		capIO     = flag.Int64("io", 20, "IO 总容量")
		horizonMs = flag.Int("horizon-ms", 2000, "虚拟时间上限（毫秒）")
		verbose   = flag.Bool("verbose", false, "打印每次决策与资源快照")
	)
	flag.Parse()

	spec := simulation.Spec{
		Seed:              *seed,
		Tasks:             *tasks,
		Limits:            scheduler.Limits{MaxCPU: *capCPU, MaxMemory: *capMem, MaxIO: *capIO, MaxTasks: *maxTasks},
		Tick:              time.Millisecond,
		RunGrace:          5 * time.Millisecond,
		StarvationAge:     8,
		TimeHorizon:       time.Duration(*horizonMs) * time.Millisecond,
		AnomalyEvery:      17,
		QueueTimeoutEvery: 11,
		RunTimeoutEvery:   13,
	}

	log, rep, err := simulation.Run(spec)
	if err != nil {
		fmt.Fprintln(os.Stderr, "simulation failed:", err)
		os.Exit(1)
	}

	s := rep.FinalSnap
	fmt.Printf("seed=%d tasks=%d events=%d\n", rep.Seed, *tasks, rep.EventCount)
	fmt.Printf("accepted=%d completed=%d canceled=%d runTimeout=%d queueTimeout=%d panicked=%d runaway=%d rejected=%d\n",
		s.Accepted, s.Completed, s.Canceled, s.TimedOut, s.QueueTimedOut, s.Panicked, s.Runaway, s.Rejected)
	fmt.Print("rejected by reason:")
	for reason, n := range rep.Rejected {
		fmt.Printf(" %s=%d", reason, n)
	}
	if len(rep.Rejected) == 0 {
		fmt.Print(" (none)")
	}
	fmt.Println()
	fmt.Printf("final ledger: running=%d queued=%d inUse=cpu:%d mem:%d io:%d\n",
		s.Running, s.Queued, s.InUse.CPU, s.InUse.Memory, s.InUse.IO)

	conserved := s.Accepted == s.Completed+s.Canceled+s.TimedOut+s.Panicked
	balanced := s.Running == 0 && s.Queued == 0 &&
		s.InUse.CPU == 0 && s.InUse.Memory == 0 && s.InUse.IO == 0
	fmt.Printf("conclusions: ledger_balanced=%v admission_conserved=%v violations=%d\n",
		balanced, conserved, len(rep.Violations))

	if *verbose {
		fmt.Println("---- decision log (seq type task reason decision | snapshot) ----")
		for _, e := range log.Events() {
			fmt.Printf("[%03d] %-8s %-8s p=%-2d %-17s %-50s | run=%d q=%d cpu=%2d mem=%2d io=%2d acc=%d done=%d qto=%d rto=%d panic=%d runaway=%d\n",
				e.Seq, e.Type, e.TaskName, e.Priority, e.Reason, trunc(e.Decision, 50),
				e.Snap.Running, e.Snap.Queued, e.Snap.InUse.CPU, e.Snap.InUse.Memory, e.Snap.InUse.IO,
				e.Snap.Accepted, e.Snap.Completed, e.Snap.QueueTimedOut, e.Snap.TimedOut,
				e.Snap.Panicked, e.Snap.Runaway)
		}
	}

	if !balanced || !conserved || len(rep.Violations) != 0 {
		os.Exit(2)
	}
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
