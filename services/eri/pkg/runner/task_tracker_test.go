package runner

import (
	"sync"
	"testing"
	"time"
)

func TestTaskTracker_LifecycleAndPruning(t *testing.T) {
	tracker := NewTaskTracker()
	if tracker.ActiveCount() != 0 || tracker.Add(TaskMetadata{TaskID: "", ToolName: "tool"}) || tracker.Remove("") {
		t.Fatalf("expected 0 active tasks and false on empty add/remove")
	}

	now := time.Now()
	if !tracker.Add(TaskMetadata{TaskID: "zero-time", ToolName: "tool"}) {
		t.Fatalf("expected add to succeed")
	}
	tasks := tracker.ActiveTasks()
	if len(tasks) != 1 || tasks[0].StartedAt.IsZero() || tasks[0].StartedAt.Before(now.Add(-time.Second)) {
		t.Fatalf("expected 1 task with valid StartedAt")
	}
	tracker.Remove("zero-time")

	meta := TaskMetadata{TaskID: "task-123", ToolName: "run_command", CommandLine: "sleep 10", StartedAt: time.Now()}
	if !tracker.Add(meta) || tracker.ActiveCount() != 1 || !tracker.Has("task-123") {
		t.Errorf("initial Add failed")
	}
	if tracker.Add(meta) || tracker.ActiveCount() != 1 || !tracker.Remove("task-123") || tracker.ActiveCount() != 0 || tracker.Remove("task-123") {
		t.Errorf("add/remove operations failed")
	}

	pruneNow := time.Now()
	tracker.Add(TaskMetadata{TaskID: "fresh", ToolName: "tool", StartedAt: pruneNow})
	tracker.Add(TaskMetadata{TaskID: "stale", ToolName: "tool", StartedAt: pruneNow.Add(-3 * time.Hour)})

	pruned := tracker.PruneExpired(2 * time.Hour)
	if len(pruned) != 1 || pruned[0] != "stale" || tracker.Has("stale") || !tracker.Has("fresh") {
		t.Fatalf("expected ['stale'] pruned, got %v", pruned)
	}
	if p := tracker.PruneExpired(0); p != nil || tracker.PruneExpired(-time.Minute) != nil {
		t.Errorf("expected nil from PruneExpired(<=0)")
	}
}

func TestTaskTracker_ConcurrentAccess(t *testing.T) {
	tracker := NewTaskTracker()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			taskID := "task-" + string(rune('a'+id))
			tracker.Add(TaskMetadata{TaskID: taskID, ToolName: "tool", StartedAt: time.Now()})
			_ = tracker.Has(taskID)
			_ = tracker.ActiveCount()
			_ = tracker.ActiveTasks()
			tracker.Remove(taskID)
		}(i)
	}
	wg.Wait()
	if tracker.ActiveCount() != 0 {
		t.Errorf("expected 0 tasks after concurrent add/remove, got %d", tracker.ActiveCount())
	}
}
