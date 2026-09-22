// Package simulation 提供可复现的本地模拟负载，用于验证调度器
// 在高并发与异常场景下的调度结论：同一种子下逐事件可复现，
// 不同种子下结论（账目守恒、拒绝原因分布、故障隔离）稳定。
package simulation

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/lisama569/concurrent-scheduler-go/scheduler"
)

// Spec 描述一次模拟运行。
type Spec struct {
	Seed int64
	// Tasks 生成的任务总数。
	Tasks int
	// Limits 调度器容量与并发上限。
	Limits scheduler.Limits
	// Tick 调度扫描间隔（同时决定排队超时/aging 的时间粒度）。
	Tick time.Duration
	// RunGrace 失控任务宽限期。
	RunGrace time.Duration
	// StarvationAge 防饥饿阈值（tick 数）。
	StarvationAge int64
	// TimeHorizon 虚拟时间推进上限。
	TimeHorizon time.Duration
	// AnomalyEvery 每 N 个任务插入一个异常任务（panic 或阻塞），0 表示不插入。
	AnomalyEvery int
	// QueueTimeoutEvery 每 N 个任务给一个较短的排队超时。
	QueueTimeoutEvery int
	// RunTimeoutEvery 每 N 个任务给一个运行超时。
	RunTimeoutEvery int
}

// Report 汇总一次模拟运行的结论。
type Report struct {
	Seed       int64
	Rejected   map[scheduler.RejectReason]int64
	FinalSnap  scheduler.Snapshot
	EventCount int
	// Violations 记录任何账目越界/负值的快照（应为空）。
	Violations []string
}

// Run 以确定性虚拟引擎执行一次模拟。
func Run(spec Spec) (*scheduler.SliceLogger, *Report, error) {
	if spec.Tick <= 0 {
		spec.Tick = time.Millisecond
	}
	if spec.RunGrace <= 0 {
		spec.RunGrace = 5 * time.Millisecond
	}
	if spec.StarvationAge <= 0 {
		spec.StarvationAge = 8
	}
	if spec.TimeHorizon <= 0 {
		spec.TimeHorizon = 10 * time.Second
	}

	rng := rand.New(rand.NewPCG(uint64(spec.Seed), uint64(spec.Seed)^0x9e3779b97f4a7c15))
	log := scheduler.NewSliceLogger()
	eng := scheduler.NewVirtual(time.Unix(1_700_000_000, 0), scheduler.Config{
		Limits:        spec.Limits,
		Tick:          spec.Tick,
		RunGrace:      spec.RunGrace,
		StarvationAge: spec.StarvationAge,
		Logger:        log,
	})

	tasks := make([]scheduler.VirtualTask, 0, spec.Tasks)
	for i := 0; i < spec.Tasks; i++ {
		cost := scheduler.Resources{
			CPU:    int64(1 + rng.IntN(int(spec.Limits.MaxCPU/2+1))),
			Memory: int64(1 + rng.IntN(int(spec.Limits.MaxMemory/2+1))),
			IO:     int64(1 + rng.IntN(int(maxInt64(1, spec.Limits.MaxIO/2+1)))),
		}
		sp := scheduler.TaskSpec{
			Name:     fmt.Sprintf("t%04d", i),
			Priority: rng.IntN(16),
			Cost:     cost,
			Fn:       scheduler.VirtualNoop,
		}
		dur := time.Duration(1+rng.IntN(20)) * spec.Tick
		submitAt := time.Duration(rng.IntN(int(spec.TimeHorizon/(4*spec.Tick)+1))) * spec.Tick

		vt := scheduler.VirtualTask{Spec: sp, Duration: dur, SubmitAt: submitAt}

		if spec.QueueTimeoutEvery > 0 && i%spec.QueueTimeoutEvery == 3 {
			sp.QueueTimeout = time.Duration(1+rng.IntN(6)) * spec.Tick
			vt.Spec = sp
		}
		if spec.RunTimeoutEvery > 0 && i%spec.RunTimeoutEvery == 7 {
			sp.RunTimeout = time.Duration(2+rng.IntN(8)) * spec.Tick
			vt.Spec = sp
		}
		if spec.AnomalyEvery > 0 && i%spec.AnomalyEvery == 0 {
			if rng.IntN(2) == 0 {
				vt.Panic = true
			} else {
				vt.Block = true
				// 阻塞任务必须有运行超时，否则永远无法回收。
				sp.RunTimeout = time.Duration(3+rng.IntN(6)) * spec.Tick
				vt.Spec = sp
			}
		}
		tasks = append(tasks, vt)
	}

	eng.Run(tasks, spec.TimeHorizon)

	rep := buildReport(spec.Seed, log)
	return log, rep, nil
}

func buildReport(seed int64, log *scheduler.SliceLogger) *Report {
	events := log.Events()
	rep := &Report{
		Seed:       seed,
		Rejected:   map[scheduler.RejectReason]int64{},
		EventCount: len(events),
	}
	for _, e := range events {
		if e.Type == scheduler.EventReject {
			rep.Rejected[e.Reason]++
		}
		if e.Snap.InUse.CPU < 0 || e.Snap.InUse.Memory < 0 || e.Snap.InUse.IO < 0 ||
			e.Snap.Running < 0 {
			rep.Violations = append(rep.Violations,
				fmt.Sprintf("seq=%d negative ledger: %+v", e.Seq, e.Snap))
		}
	}
	if len(events) > 0 {
		rep.FinalSnap = events[len(events)-1].Snap
	}
	return rep
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
