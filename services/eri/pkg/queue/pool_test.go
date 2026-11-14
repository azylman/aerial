package queue

import (
	"context"
	"slices"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/db"
)

func TestWorkerPool_InterruptThread_FinalizedTurnLatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		finalized       bool
		seedStatus      string
		wantLatchedIDs  []string
		wantStatus      string
		wantInterrupted bool
	}{
		{name: "running turn is latched and failed", seedStatus: db.StatusProcessing, wantLatchedIDs: []string{"msg-latch"}, wantStatus: db.StatusFailed, wantInterrupted: true},
		{name: "finalized turn refreshing its summary keeps completed status", finalized: true, seedStatus: db.StatusCompleted, wantStatus: db.StatusCompleted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := setupTestStore(t)
			threadID := "th-latch"
			msg := db.Message{ID: "msg-latch", ThreadID: threadID, Content: "hi", Status: db.StatusPending}
			if err := store.InsertMessage(ctx, msg); err != nil {
				t.Fatalf("insert: %v", err)
			}
			if err := store.UpdateMessageStatus(ctx, msg.ID, tc.seedStatus, ""); err != nil {
				t.Fatalf("seed status: %v", err)
			}
			turnCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			te := &turnExecution{threadID: threadID, burst: []db.Message{msg}, turnCtx: turnCtx, turnCancel: cancel}
			te.finalized.Store(tc.finalized)
			pool := &WorkerPool{ctx: ctx, cfg: WorkerPoolConfig{Store: store}, activeTurns: map[string]*turnExecution{db.DiscordSessionID(threadID): te}}

			pool.mu.Lock()
			active, latchedIDs := pool.latchActiveTurnsLocked(threadID)
			pool.mu.Unlock()
			if len(active) != 1 || active[0] != te {
				t.Errorf("latched active turns = %d, want the registered turn", len(active))
			}
			if !slices.Equal(latchedIDs, tc.wantLatchedIDs) {
				t.Errorf("latched burst IDs = %v, want %v", latchedIDs, tc.wantLatchedIDs)
			}
			if got := te.isInterrupted(); got != tc.wantInterrupted {
				t.Errorf("interrupted after latch = %t, want %t", got, tc.wantInterrupted)
			}

			pool.InterruptThread(ctx, threadID, false)
			if turnCtx.Err() == nil {
				t.Errorf("turn context not cancelled by interrupt")
			}
			got, err := store.GetMessage(ctx, msg.ID)
			if err != nil {
				t.Fatalf("get message: %v", err)
			}
			if got.Status != tc.wantStatus {
				t.Errorf("message status = %q, want %q", got.Status, tc.wantStatus)
			}
		})
	}
}
