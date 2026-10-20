package runner

import (
	"sync"
	"time"

	"github.com/azylman/aerial/brain/pkg/session"
)

type TaskMetadata = session.TaskMetadata

type TaskTracker struct {
	mu    sync.RWMutex
	tasks map[string]TaskMetadata
}

func NewTaskTracker() *TaskTracker {
	return &TaskTracker{tasks: make(map[string]TaskMetadata)}
}

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

func (t *TaskTracker) Remove(taskID string) bool {
	if taskID == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.tasks[taskID]; exists {
		delete(t.tasks, taskID)
		return true
	}
	return false
}

func (t *TaskTracker) Has(taskID string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, exists := t.tasks[taskID]
	return exists
}

func (t *TaskTracker) ActiveCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.tasks)
}

func (t *TaskTracker) ActiveTasks() []TaskMetadata {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]TaskMetadata, 0, len(t.tasks))
	for _, meta := range t.tasks {
		out = append(out, meta)
	}
	return out
}

func (t *TaskTracker) PruneExpired(maxAge time.Duration) []string {
	if maxAge <= 0 {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now, pruned := time.Now(), []string(nil)
	for id, meta := range t.tasks {
		if now.Sub(meta.StartedAt) > maxAge {
			pruned = append(pruned, id)
			delete(t.tasks, id)
		}
	}
	return pruned
}
