package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// jobsFromSpecs 把随机种子生成的模拟规格转成确定性 SimJob
// （剔除永久阻塞型——它们在真实时钟故障隔离测试中单独验证）。
func jobsFromSpecs(specs []SimTaskSpec) []SimJob {
	var jobs []SimJob
	for _, sp := range specs {
		if sp.BlockForever {
			continue
		}
		length := sp.Duration.Milliseconds()
		if length <= 0 {
			length = 1
		}
		jobs = append(jobs, SimJob{
			ID:       sp.ID,
			Group:    sp.Group,
			Cost:     sp.Cost,
			Priority: sp.Priority,
			LengthMS: length,
			Panic:    sp.Panic,
		})
	}
	return jobs
}

func runDeterministic(t *testing.T, jobs []SimJob, cfg Config, log bool) ([]string, map[string]Status) {
	t.Helper()
	s := New(cfg)
	if log {
		s.WithLogger(testLogger{t})
	}
	r := NewDeterministicRunner(s)
	r.freeze()
	if errs := r.SubmitAll(jobs); len(errs) != 0 {
		t.Fatalf("unexpected rejections in deterministic run: %v", errs)
	}
	if !r.Run(100000) {
		t.Fatal("deterministic run failed to drain (possible deadlocked input)")
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	return r.AdmitOrder(), r.TerminalStatuses()
}

// TestReproducibleSeed：同种子生成的负载逐字段相同，异种子应不同。
func TestReproducibleSeed(t *testing.T) {
	a := GenerateWorkload(42, 60)
	b := GenerateWorkload(42, 60)
	if len(a) != 60 || len(b) != 60 {
		t.Fatalf("workload size: %d %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("workload differs at %d: %+v vs %+v", i, a[i], b[i])
		}
	}
	c := GenerateWorkload(43, 60)
	same := true
	for i := range a {
		if a[i] != c[i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("different seeds should produce different workloads")
	}
}

// TestReproducibleScheduling：相同输入 + 相同随机种子，两次运行的
// 准入顺序与每个任务的终态必须完全一致（虚拟时钟，剔除真实时间抖动）。
func TestReproducibleScheduling(t *testing.T) {
	specs := jobsFromSpecs(GenerateWorkload(7, 80))
	// 确定性运行是"先全部提交、再驱动时钟"，因此使用无界队列；
	// QUEUE_FULL 的拒绝语义由 TestRejectQueueFull 单独覆盖。
	// ResourceLimit=20 与负载生成器的最大单项 cost(11..20) 相等，
	// 保证任何一个任务在"有空闲并发槽"时都装得下，模拟可必然排空。
	cfg := Config{MaxConcurrency: 4, ResourceLimit: 20, QueueCapacity: 0}

	admits1, term1 := runDeterministic(t, specs, cfg, false)
	admits2, term2 := runDeterministic(t, specs, cfg, true)

	if len(admits1) != len(specs) || len(admits2) != len(specs) {
		t.Fatalf("admit count mismatch: %d %d want %d", len(admits1), len(admits2), len(specs))
	}
	for i := range admits1 {
		if admits1[i] != admits2[i] {
			t.Fatalf("admit order diverges at %d:\nrun1=%v\nrun2=%v", i, admits1, admits2)
		}
	}
	for id, st := range term1 {
		if term2[id] != st {
			t.Fatalf("terminal status for %s diverges: %s vs %s", id, st, term2[id])
		}
	}
}

// TestDifferentSeedMayDiffer（弱校验）：不同种子产生的负载至少有概率
// 导致不同轨迹，避免测试退化成"任何输入都一样"的假通过。
func TestDifferentSeedsProduceDifferentLoads(t *testing.T) {
	a := jobsFromSpecs(GenerateWorkload(1, 50))
	b := jobsFromSpecs(GenerateWorkload(2, 50))
	diff := false
	for i := range a {
		if a[i].Cost != b[i].Cost || a[i].Priority != b[i].Priority || a[i].LengthMS != b[i].LengthMS {
			diff = true
			break
		}
	}
	if !diff {
		t.Fatal("expected different seeds to yield different workloads")
	}
}

// TestHighConcurrencyInvariants：大量任务并发提交下，账目永不越界，
// 最终全部归零，所有任务恰好结算一次。
func TestHighConcurrencyInvariants(t *testing.T) {
	s := newTestScheduler(t, Config{MaxConcurrency: 8, ResourceLimit: 40, QueueCapacity: 0})
	ctx := context.Background()

	const n = 400
	var wg sync.WaitGroup
	handles := make([]*Handle, n)
	var hmu sync.Mutex
	var stopWatch sync.WaitGroup

	// 后台看门狗：运行期间持续断言账目不变量。
	watchDone := make(chan struct{})
	stopWatch.Add(1)
	go func() {
		defer stopWatch.Done()
		for {
			select {
			case <-watchDone:
				return
			default:
			}
			snap := s.Snapshot()
			if snap.InFlight > snap.MaxConcurrency {
				t.Errorf("inflight %d exceeds max %d", snap.InFlight, snap.MaxConcurrency)
				return
			}
			if snap.ResourceUsed > snap.ResourceLimit {
				t.Errorf("used %d exceeds limit %d", snap.ResourceUsed, snap.ResourceLimit)
				return
			}
			if snap.InFlight < 0 || snap.ResourceUsed < 0 {
				t.Errorf("negative accounting: %s", snap)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			cost := int64(i%15 + 1) // 1..15，部分任务单独即接近上限
			h, err := s.Submit(ctx, task(fmt.Sprintf("hc-%d", i), cost, Priority(i%11), sleepTask(time.Duration(i%5)*time.Millisecond)))
			if err != nil {
				// 无界队列下不应出现拒绝；若出现必须是可区分原因。
				t.Errorf("unexpected rejection for hc-%d: %v", i, err)
				return
			}
			hmu.Lock()
			handles[i] = h
			hmu.Unlock()
		}()
	}
	wg.Wait()
	for _, h := range handles {
		if h != nil {
			waitHandle(t, h, 5*time.Second)
		}
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	close(watchDone)
	stopWaitCh := make(chan struct{})
	go func() { stopWatch.Wait(); close(stopWaitCh) }()
	select {
	case <-stopWaitCh:
	case <-time.After(time.Second):
	}

	snap := s.Snapshot()
	if snap.InFlight != 0 || snap.ResourceUsed != 0 || snap.Queued != 0 {
		t.Fatalf("accounts not clean after shutdown: %s", snap)
	}
	// 终态计数：全部成功（sleepTask 在无取消关闭下应成功）。
	succeeded := 0
	for _, h := range handles {
		if h != nil && h.Status() == StatusSucceeded {
			succeeded++
		}
	}
	if succeeded != n {
		t.Fatalf("want %d succeeded, got %d", n, succeeded)
	}
}
