package scheduler

import (
	"context"
	"math/rand"
	"time"
)

// SimTaskSpec 是模拟负载中单个任务的声明参数。
type SimTaskSpec struct {
	ID       string
	Cost     int64
	Priority Priority
	Duration time.Duration
	// Panic=true 时任务在执行中途 panic，用于验证故障隔离。
	Panic bool
	// BlockForever=true 时任务忽略 ctx 永久阻塞，用于验证长阻塞不毁账目、
	// 且 Shutdown 不会死锁（通过泄漏隔离 + 超时关闭验证）。
	BlockForever bool
	// Timeout 为任务级超时，0 表示不设置。
	Timeout time.Duration
}

// GenerateWorkload 用确定性的随机源（math/rand，种子固定）生成模拟任务，
// 因此同 seed 同 n 产出逐字段相同的序列——调度顺序可复现性的输入基础。
//
// 生成规则固定且不依赖 map 遍历 / goroutine 时序：
//   - 70% 普通任务：cost 1..10，运行 1..8ms；
//   - 12% 大开销任务：cost 11..20（用于制造资源排队 / 队头装不下场景）；
//   - 9%  超时任务：运行 30..50ms 但 Timeout 为 3..8ms；
//   - 6%  panic 任务；
//   - 3%  永久阻塞任务（忽略 ctx）。
func GenerateWorkload(seed int64, n int) []SimTaskSpec {
	rng := rand.New(rand.NewSource(seed))
	specs := make([]SimTaskSpec, n)
	for i := 0; i < n; i++ {
		roll := rng.Intn(100)
		spec := SimTaskSpec{ID: specID(i)}
		switch {
		case roll < 70:
			spec.Cost = int64(rng.Intn(10) + 1)
			spec.Duration = time.Duration(rng.Intn(8)+1) * time.Millisecond
		case roll < 82:
			spec.Cost = int64(rng.Intn(10) + 11)
			spec.Duration = time.Duration(rng.Intn(8)+1) * time.Millisecond
		case roll < 91:
			spec.Cost = int64(rng.Intn(10) + 1)
			spec.Duration = time.Duration(rng.Intn(21)+30) * time.Millisecond
			spec.Timeout = time.Duration(rng.Intn(6)+3) * time.Millisecond
		case roll < 97:
			spec.Cost = int64(rng.Intn(10) + 1)
			spec.Duration = time.Millisecond
			spec.Panic = true
		default:
			spec.Cost = int64(rng.Intn(10) + 1)
			spec.BlockForever = true
		}
		// 固定分布的优先级，保证各优先级都有一定占比。
		switch rng.Intn(10) {
		case 0, 1:
			spec.Priority = PriorityHigh
		case 2, 3, 4, 5, 6, 7:
			spec.Priority = PriorityNormal
		default:
			spec.Priority = PriorityLow
		}
		specs[i] = spec
	}
	return specs
}

func specID(i int) string {
	return "t-" + itoa(i)
}

// itoa 避免引入 strconv 仅为一处用途。
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	neg := i < 0
	if neg {
		i = -i
	}
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}

// TaskOf 把模拟规格转成可提交的 *Task（Fn 内建对应的负载行为）。
func (sp SimTaskSpec) TaskOf() *Task {
	fn := func(ctx context.Context) error {
		if sp.BlockForever {
			select {} // 故意忽略 ctx：验证最恶劣的长阻塞也不破坏账目
		}
		if sp.Panic {
			panic("simulated task panic: " + sp.ID)
		}
		if sp.Duration <= 0 {
			return nil
		}
		select {
		case <-time.After(sp.Duration):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return &Task{
		ID:       sp.ID,
		Cost:     sp.Cost,
		Priority: sp.Priority,
		Timeout:  sp.Timeout,
		Fn:       fn,
	}
}
