package scheduler

import (
	"fmt"
	"strings"
	"testing"
)

// printEvents 在 -v 模式下打印每次准入/释放等决策依据与资源快照。
func printEvents(t *testing.T, l *SliceLogger) {
	t.Helper()
	var b strings.Builder
	for _, e := range l.Events() {
		fmt.Fprintf(&b, "  [%03d] %-8s %-8s p=%-2d reason=%-17s %-46s | run=%d cpu=%3d mem=%3d io=%3d q=%d acc=%d done=%d rej=%d cancel=%d timeout=%d panic=%d runaway=%d\n",
			e.Seq, e.Type, e.TaskName, e.Priority, e.Reason, clip(e.Decision, 46),
			e.Snap.Running, e.Snap.InUse.CPU, e.Snap.InUse.Memory, e.Snap.InUse.IO,
			e.Snap.Queued, e.Snap.Accepted, e.Snap.Completed, e.Snap.Rejected,
			e.Snap.Canceled, e.Snap.TimedOut, e.Snap.Panicked, e.Snap.Runaway)
	}
	t.Logf("decision log (%d events):\n%s", len(l.Events()), b.String())
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

// assertReleased 断言终态时资源账目完全归零、无在管任务。
func assertReleased(t *testing.T, s *Scheduler) {
	t.Helper()
	snap := s.Snapshot()
	if snap.Running != 0 || snap.Queued != 0 ||
		snap.InUse.CPU != 0 || snap.InUse.Memory != 0 || snap.InUse.IO != 0 {
		t.Fatalf("resources not fully released: %+v", snap)
	}
}

func waitRunning(t *testing.T, s *Scheduler, n int) {
	t.Helper()
	if !waitUntil(t, func() bool { return s.Snapshot().Running == n }) {
		t.Fatalf("want running=%d, snap=%+v", n, s.Snapshot())
	}
}
