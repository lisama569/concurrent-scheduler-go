package scheduler

import (
	"sync"
	"time"
)

// EventType 标识调度器产生的可观测事件类型。
type EventType string

const (
	EventSubmit   EventType = "submit"
	EventQueue    EventType = "queue"
	EventAdmit    EventType = "admit"
	EventReject   EventType = "reject"
	EventStart    EventType = "start"
	EventFinish   EventType = "finish"
	EventRelease  EventType = "release"
	EventCancel   EventType = "cancel"
	EventTimeout  EventType = "timeout"
	EventPanic    EventType = "panic"
	EventRunaway  EventType = "runaway"
	EventShutdown EventType = "shutdown"
)

// Event 是一次调度决策的结构化记录，Decision 字段写明决策依据，
// Snap 为决策后的资源快照。
type Event struct {
	Seq      int64
	Time     time.Time
	Type     EventType
	TaskID   int64
	TaskName string
	Priority int
	Cost     Resources
	Reason   RejectReason
	Decision string
	Snap     Snapshot
}

// EventLogger 接收调度事件，便于测试打印每次准入/释放的决策依据与资源快照。
type EventLogger interface {
	Log(e Event)
	Events() []Event
}

// SliceLogger 是线程安全的内存日志实现。
type SliceLogger struct {
	mu     sync.Mutex
	events []Event
}

func NewSliceLogger() *SliceLogger { return &SliceLogger{} }

func (l *SliceLogger) Log(e Event) {
	l.mu.Lock()
	l.events = append(l.events, e)
	l.mu.Unlock()
}

func (l *SliceLogger) Events() []Event {
	l.mu.Lock()
	out := make([]Event, len(l.events))
	copy(out, l.events)
	l.mu.Unlock()
	return out
}
