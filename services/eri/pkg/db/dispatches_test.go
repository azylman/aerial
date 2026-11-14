package db

import (
	"context"
	"testing"
)

func TestDispatchPersistence_TargetAndChunkID(t *testing.T) {
	database := setupTestDB(t)
	defer database.Close()

	store := NewSQLStore(database)
	ctx := context.Background()

	tests := []struct {
		name               string
		dispatch           Dispatch
		wantTargetID       string
		wantVanadisChunkID string
	}{
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
			if err := store.InsertDispatch(ctx, tc.dispatch); err != nil {
				t.Fatalf("InsertDispatch failed: %v", err)
			}

			// Verify GetDispatch retrieves normalized fields
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

			// Verify GetPendingOrProcessingDispatches includes the fields
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

	if err := store.InsertDispatch(ctx, d); err != nil {
		t.Fatalf("first InsertDispatch failed: %v", err)
	}

	// Re-inserting the same dispatch ID must succeed idempotently (ON CONFLICT DO NOTHING)
	if err := store.InsertDispatch(ctx, d); err != nil {
		t.Fatalf("second InsertDispatch with duplicate ID failed: %v", err)
	}

	got, err := store.GetDispatch(ctx, d.ID)
	if err != nil || got == nil {
		t.Fatalf("GetDispatch failed: %v", err)
	}
	if got.ID != d.ID {
		t.Errorf("got ID %q, want %q", got.ID, d.ID)
	}
}
