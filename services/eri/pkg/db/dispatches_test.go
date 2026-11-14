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
}

func assertPendingDispatch(ctx context.Context, t *testing.T, store Store, tc dispatchPersistenceCase) {
	t.Helper()
	pending, err := store.GetPendingOrProcessingDispatches(ctx, 100)
	if err != nil {
		t.Fatalf("GetPendingOrProcessingDispatches failed: %v", err)
	}
	var found *Dispatch
	for i := range pending {
		if pending[i].ID == tc.dispatch.ID {
			found = &pending[i]
			break
		}
	}
	if found == nil {
		t.Fatalf("GetPendingOrProcessingDispatches did not return dispatch %q", tc.dispatch.ID)
	}
	if found.TargetID != tc.wantTargetID {
		t.Errorf("pending TargetID = %q, want %q", found.TargetID, tc.wantTargetID)
	}
	if found.VanadisChunkID != tc.wantVanadisChunkID {
		t.Errorf("pending VanadisChunkID = %q, want %q", found.VanadisChunkID, tc.wantVanadisChunkID)
	}
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

	assertPendingDispatch(ctx, t, store, tc)
}

func TestDispatchPersistence_TargetAndChunkID(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	store := NewSQLStore(database)
	ctx := context.Background()

	tests := []dispatchPersistenceCase{
		{
			name: "persists target_id and vanadis_chunk_id",
			dispatch: Dispatch{
				ID:             "disp-target-chunk-1",
				Source:         "vanadis",
				SessionID:      "vanadis-build-1-chunk-1",
				TargetID:       "123456789012345678",
				VanadisChunkID: "build-1-chunk-1",
				Prompt:         "Execute build chunk 1",
				Status:         StatusPending,
			},
			wantTargetID:       "123456789012345678",
			wantVanadisChunkID: "build-1-chunk-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runDispatchPersistenceCase(ctx, t, store, tc)
		})
	}
}

func TestInsertDispatch_IdempotentDuplicate(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	store := NewSQLStore(database)
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
