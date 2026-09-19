package runner

import (
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/session"
)

// TaskMetadata aliases session.TaskMetadata to prevent circular dependencies between runner and session.
type TaskMetadata = session.TaskMetadata

// TaskTracker provides thread-safe, idempotent tracking of active background tasks.
type TaskTracker struct {
	mu    sync.RWMutex
	tasks map[string]TaskMetadata
}

// NewTaskTracker constructs an empty, initialized TaskTracker.
func NewTaskTracker() *TaskTracker {
	return &TaskTracker{
		tasks: make(map[string]TaskMetadata),
	}
}

// Add idempotently registers a background task. Returns true if newly added, false if already tracked.
func (t *TaskTracker) Add(meta TaskMetadata) bool {
	if meta.TaskID == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.tasks[meta.TaskID]; exists {
		return false
	}
	if meta.StartedAt.IsZero() {
		meta.StartedAt = time.Now()
	}
	t.tasks[meta.TaskID] = meta
	return true
}

// Remove idempotently removes a background task. Returns true if existed and removed, false otherwise.
func (t *TaskTracker) Remove(taskID string) bool {
	if taskID == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, exists := t.tasks[taskID]; !exists {
		return false
	}
	delete(t.tasks, taskID)
	return true
}

// Has reports whether the specified task is currently tracked.
func (t *TaskTracker) Has(taskID string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, exists := t.tasks[taskID]
	return exists
}

// ActiveCount returns the current count of tracked background tasks.
func (t *TaskTracker) ActiveCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.tasks)
}

// ActiveTasks returns a snapshot slice of all currently tracked tasks.
func (t *TaskTracker) ActiveTasks() []TaskMetadata {
	t.mu.RLock()
	defer t.mu.RUnlock()

	out := make([]TaskMetadata, 0, len(t.tasks))
	for _, meta := range t.tasks {
		out = append(out, meta)
	}
	return out
}

// PruneExpired removes tasks whose runtime has exceeded maxAge, returning the pruned task IDs.
func (t *TaskTracker) PruneExpired(maxAge time.Duration) []string {
	if maxAge <= 0 {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	var pruned []string
	for id, meta := range t.tasks {
		if now.Sub(meta.StartedAt) > maxAge {
			pruned = append(pruned, id)
			delete(t.tasks, id)
		}
	}
	return pruned
}
