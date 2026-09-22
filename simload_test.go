package scheduler

import (
	"context"
	"testing"
	"time"
)

// TestSimulatedWorkloadEndToEnd：用随机种子生成的真实模拟负载（含超时、
// panic、大开销任务，剔除永久阻塞型）端到端跑一遍，断言全部结算、
// 账目归零，并统计各类终态，证明模拟负载分类与调度器协同正确。
func TestSimulatedWorkloadEndToEnd(t *testing.T) {
	specs := GenerateWorkload(20260923, 120)
	s := newTestScheduler(t, Config{MaxConcurrency: 6, ResourceLimit: 20, QueueCapacity: 0})
	ctx := context.Background()

	var handles []*Handle
	var kept int
	for _, sp := range specs {
		if sp.BlockForever {
			continue // 永久阻塞型由专门用例验证，避免本用例无法关闭
		}
		h, err := s.Submit(ctx, sp.TaskOf())
		if err != nil {
			t.Fatalf("submit %s: %v", sp.ID, err)
		}
		handles = append(handles, h)
		kept++
	}
	for _, h := range handles {
		waitHandle(t, h, 5*time.Second)
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if snap := s.Snapshot(); snap.InFlight != 0 || snap.ResourceUsed != 0 || snap.Queued != 0 {
		t.Fatalf("quota leaked after simulated workload: %s", snap)
	}

	counts := map[Status]int{}
	for _, h := range handles {
		counts[h.Status()]++
	}
	if sum := counts[StatusSucceeded] + counts[StatusTimedOut] + counts[StatusPanicked]; sum != kept {
		t.Fatalf("unexpected terminal mix (succeeded=%d timedout=%d panicked=%d) total=%d",
			counts[StatusSucceeded], counts[StatusTimedOut], counts[StatusPanicked], kept)
	}
	t.Logf("workload terminals: succeeded=%d timed_out=%d panicked=%d",
		counts[StatusSucceeded], counts[StatusTimedOut], counts[StatusPanicked])
}
