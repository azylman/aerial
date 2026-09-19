package runner

import (
	"sync"
	"testing"
	"time"
)

func TestTaskTracker_IdempotentOperations(t *testing.T) {
	tracker := NewTaskTracker()
	if tracker.ActiveCount() != 0 {
		t.Fatalf("expected 0 active tasks, got %d", tracker.ActiveCount())
	}

	meta := TaskMetadata{
		TaskID:      "task-123",
		ToolName:    "run_command",
		CommandLine: "sleep 10",
		StartedAt:   time.Now(),
	}

	// First add should succeed
	if !tracker.Add(meta) {
		t.Errorf("expected true on initial Add")
	}
	if tracker.ActiveCount() != 1 {
		t.Errorf("expected 1 active task, got %d", tracker.ActiveCount())
	}
	if !tracker.Has("task-123") {
		t.Errorf("expected tracker to have task-123")
	}

	// Idempotent duplicate add should not duplicate or error
	if tracker.Add(meta) {
		t.Errorf("expected false on duplicate Add")
	}
	if tracker.ActiveCount() != 1 {
		t.Errorf("expected active count to remain 1, got %d", tracker.ActiveCount())
	}

	// Remove existing
	if !tracker.Remove("task-123") {
		t.Errorf("expected true on Remove of existing task")
	}
	if tracker.ActiveCount() != 0 {
		t.Errorf("expected 0 active tasks after remove, got %d", tracker.ActiveCount())
	}

	// Idempotent remove non-existent
	if tracker.Remove("task-123") {
		t.Errorf("expected false on Remove of already removed task")
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

func TestTaskTracker_PruneExpired(t *testing.T) {
	tracker := NewTaskTracker()
	now := time.Now()

	tracker.Add(TaskMetadata{TaskID: "fresh", ToolName: "tool", StartedAt: now})
	tracker.Add(TaskMetadata{TaskID: "stale", ToolName: "tool", StartedAt: now.Add(-3 * time.Hour)})

	pruned := tracker.PruneExpired(2 * time.Hour)
	if len(pruned) != 1 || pruned[0] != "stale" {
		t.Fatalf("expected ['stale'] pruned, got %v", pruned)
	}
	if tracker.Has("stale") {
		t.Errorf("expected stale task to be removed")
	}
	if !tracker.Has("fresh") {
		t.Errorf("expected fresh task to remain")
	}
}

func TestTaskTracker_EdgeCases(t *testing.T) {
	tracker := NewTaskTracker()

	// Empty TaskID should fail
	if tracker.Add(TaskMetadata{TaskID: "", ToolName: "tool"}) {
		t.Errorf("expected false when adding task with empty TaskID")
	}
	if tracker.Remove("") {
		t.Errorf("expected false when removing empty taskID")
	}

	// Zero StartedAt should be populated
	now := time.Now()
	if !tracker.Add(TaskMetadata{TaskID: "zero-time", ToolName: "tool"}) {
		t.Fatalf("expected add to succeed")
	}
	tasks := tracker.ActiveTasks()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	if tasks[0].StartedAt.IsZero() || tasks[0].StartedAt.Before(now.Add(-time.Second)) {
		t.Errorf("expected StartedAt to be populated near current time, got %v", tasks[0].StartedAt)
	}

	// Prune with non-positive duration should return nil
	if pruned := tracker.PruneExpired(0); pruned != nil {
		t.Errorf("expected nil from PruneExpired(0), got %v", pruned)
	}
	if pruned := tracker.PruneExpired(-time.Minute); pruned != nil {
		t.Errorf("expected nil from PruneExpired(-1m), got %v", pruned)
	}
}
