package scheduler

import (
	"context"
	"testing"
)

// runGroupedDeterministic 在冻结态下驱动一个含业务组的调度器跑完 jobs。
func runGroupedDeterministic(t *testing.T, jobs []SimJob, cfg Config, groups []GroupConfig,
	log bool) ([]string, map[string]Status) {
	t.Helper()
	s := New(cfg, groups...)
	if log {
		s.WithLogger(testLogger{t})
	}
	r := NewDeterministicRunner(s)
	r.freeze()
	if errs := r.SubmitAll(jobs); len(errs) != 0 {
		t.Fatalf("unexpected rejections in grouped deterministic run: %v", errs)
	}
	if !r.Run(100000) {
		t.Fatal("grouped deterministic run failed to drain")
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r.AdmitOrder(), r.TerminalStatuses()
}

// TestReproducibleGroupedScheduling：相同输入、相同种子、相同分组配置，
// 两次运行在"分组这一层"的准入顺序与每个任务终态必须逐位一致——
// 覆盖组间轮转 + 组内优先级/老化 + 组账/全局账的共同作用。
func TestReproducibleGroupedScheduling(t *testing.T) {
	specs := jobsFromSpecs(AssignGroups(GenerateWorkload(20261001, 100), 3))
	// 三个组各自有偏紧的预算，全局也偏紧，以充分制造跨组轮转与组内排队；
	// 资源维度用 30（>= 负载最大单项 cost 20），保证任何任务在轮到机会时
	// 都装得下，模拟可必然排空。
	cfg := Config{MaxConcurrency: 3, ResourceLimit: 30, QueueCapacity: 0}
	groups := []GroupConfig{
		{Name: "g-0", MaxConcurrency: 2, ResourceLimit: 24},
		{Name: "g-1", MaxConcurrency: 2, ResourceLimit: 24},
		{Name: "g-2", MaxConcurrency: 2, ResourceLimit: 24},
	}

	admits1, term1 := runGroupedDeterministic(t, specs, cfg, groups, false)
	admits2, term2 := runGroupedDeterministic(t, specs, cfg, groups, true)

	if len(admits1) != len(specs) || len(admits2) != len(specs) {
		t.Fatalf("admit count mismatch: %d %d want %d", len(admits1), len(admits2), len(specs))
	}
	for i := range admits1 {
		if admits1[i] != admits2[i] {
			t.Fatalf("grouped admit order diverges at %d:\nrun1=%v\nrun2=%v", i, admits1, admits2)
		}
	}
	for id, st := range term1 {
		if term2[id] != st {
			t.Fatalf("grouped terminal status for %s diverges: %s vs %s", id, st, term2[id])
		}
	}

	// 额外不变量：输入确实覆盖了全部三个组（配合上面的逐位一致断言，
	// 保证轮转公平在分组层真正生效）。
	groupInput := map[string]int{"g-0": 0, "g-1": 0, "g-2": 0}
	for _, j := range specs {
		groupInput[j.Group]++
	}
	if groupInput["g-0"] == 0 || groupInput["g-1"] == 0 || groupInput["g-2"] == 0 {
		t.Fatalf("test setup should populate all groups: %+v", groupInput)
	}
	t.Logf("grouped reproducible admission order (len=%d): %v", len(admits1), admits1)
}

// TestGroupedAdmissionInterleavesGroups：用确定性运行器证明组间轮转确实
// 交替了不同组（而非同组高优任务霸占全部早期准入机会）。
func TestGroupedAdmissionInterleavesGroups(t *testing.T) {
	// 全局并发 2，三个组各有预算。构造：A 组两个 HIGH，B 组两个 NORMAL，
	// C 组两个 LOW，全部同一时刻冻结提交。前两个被立即准入后，后续按
	// 轮转交替。检查前 6 个准入中三个组都至少出现一次且无组长期霸占。
	jobs := []SimJob{
		{ID: "a0", Group: "g-0", Cost: 1, Priority: PriorityHigh, LengthMS: 10},
		{ID: "a1", Group: "g-0", Cost: 1, Priority: PriorityHigh, LengthMS: 10},
		{ID: "b0", Group: "g-1", Cost: 1, Priority: PriorityNormal, LengthMS: 10},
		{ID: "b1", Group: "g-1", Cost: 1, Priority: PriorityNormal, LengthMS: 10},
		{ID: "c0", Group: "g-2", Cost: 1, Priority: PriorityLow, LengthMS: 10},
		{ID: "c1", Group: "g-2", Cost: 1, Priority: PriorityLow, LengthMS: 10},
	}
	cfg := Config{MaxConcurrency: 2, ResourceLimit: 30}
	groups := []GroupConfig{
		{Name: "g-0", MaxConcurrency: 4, ResourceLimit: 30},
		{Name: "g-1", MaxConcurrency: 4, ResourceLimit: 30},
		{Name: "g-2", MaxConcurrency: 4, ResourceLimit: 30},
	}
	admits, _ := runGroupedDeterministic(t, jobs, cfg, groups, true)
	if len(admits) != 6 {
		t.Fatalf("want 6 admits, got %d: %v", len(admits), admits)
	}
	groupOf := map[string]string{
		"a0": "g-0", "a1": "g-0", "b0": "g-1", "b1": "g-1", "c0": "g-2", "c1": "g-2",
	}
	seen := map[string]int{}
	var seq []string
	for _, id := range admits {
		g := groupOf[id]
		seen[g]++
		seq = append(seq, g)
	}
	for g, n := range map[string]int{"g-0": 2, "g-1": 2, "g-2": 2} {
		if seen[g] != n {
			t.Fatalf("group %s admitted %d want %d, seq=%v", g, seen[g], n, seq)
		}
	}
	// 前 4 次准入必须覆盖全部三个组（否则说明某组被长期饿死）。
	head := map[string]bool{}
	for i := 0; i < 4; i++ {
		head[seq[i]] = true
	}
	if len(head) != 3 {
		t.Fatalf("first 4 admissions should cover all 3 groups, seq=%v", seq)
	}
}
