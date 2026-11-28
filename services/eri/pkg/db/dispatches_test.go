package db

import (
	"context"
	"testing"
)

type dispatchPersistenceCase struct {
	name               string
	dispatch           Dispatch
	wantTargetID       string
	wantVanadisChunkID string
	wantTurnID         string
}

func runDispatchPersistenceCase(ctx context.Context, t *testing.T, store Store, tc dispatchPersistenceCase) {
	t.Helper()
	if _, err := store.InsertDispatch(ctx, tc.dispatch); err != nil {
		t.Fatalf("InsertDispatch failed: %v", err)
	}

	got, err := store.GetDispatch(ctx, tc.dispatch.ID)
	if err != nil {
		t.Fatalf("GetDispatch failed: %v", err)
	}
	if got == nil {
		t.Fatalf("GetDispatch returned nil for id %q", tc.dispatch.ID)
	}
	if got.TargetID != tc.wantTargetID {
		t.Errorf("GetDispatch TargetID = %q, want %q", got.TargetID, tc.wantTargetID)
	}
	if got.VanadisChunkID != tc.wantVanadisChunkID {
		t.Errorf("GetDispatch VanadisChunkID = %q, want %q", got.VanadisChunkID, tc.wantVanadisChunkID)
	}
	if got.TurnID != tc.wantTurnID {
		t.Errorf("GetDispatch TurnID = %q, want %q", got.TurnID, tc.wantTurnID)
	}
}

func TestDispatchPersistence_TargetAndChunkID(t *testing.T) {
	t.Parallel()

	tests := []dispatchPersistenceCase{
		{
			name: "persists target_id, vanadis_chunk_id, and turn_id",
			dispatch: Dispatch{
				ID:             "disp-target-chunk-1",
				Source:         "vanadis",
				SessionID:      "vanadis-build-1-chunk-1",
				TargetID:       "123456789012345678",
				VanadisChunkID: "build-1-chunk-1",
				TurnID:         "turn-dispatch-test-1",
				Prompt:         "Execute build chunk 1",
				Status:         StatusPending,
			},
			wantTargetID:       "123456789012345678",
			wantVanadisChunkID: "build-1-chunk-1",
			wantTurnID:         "turn-dispatch-test-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewSQLStore(newTestDB(t))
			runDispatchPersistenceCase(context.Background(), t, store, tc)
		})
	}
}

func TestInsertDispatch_IdempotentDuplicate(t *testing.T) {
	t.Parallel()
	store := NewSQLStore(newTestDB(t))
	ctx := context.Background()

	d := Dispatch{
		ID:     "disp-idempotent-1",
		Source: "vanadis",
		Prompt: "chunk execution",
		Status: StatusPending,
	}

	inserted, err := store.InsertDispatch(ctx, d)
	if err != nil {
		t.Fatalf("first InsertDispatch failed: %v", err)
	}
	if !inserted {
		t.Fatalf("first InsertDispatch returned inserted=false, want true")
	}

	// Re-inserting the same dispatch ID must succeed idempotently (ON CONFLICT DO NOTHING) and report inserted=false
	insertedAgain, err := store.InsertDispatch(ctx, d)
	if err != nil {
		t.Fatalf("second InsertDispatch with duplicate ID failed: %v", err)
	}
	if insertedAgain {
		t.Fatalf("second InsertDispatch returned inserted=true, want false")
	}
}
