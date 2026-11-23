package db

import (
	"context"
	"strings"
	"testing"
)

func TestSQLTranscriptStore_NilStore(t *testing.T) {
	t.Parallel()
	var nilStore *SQLTranscriptStore
	if _, err := nilStore.GetKnownTurnSessionIDs(context.Background()); err == nil {
		t.Errorf("expected error on nil store GetKnownTurnSessionIDs")
	}
}

func TestSQLTranscriptStore_GetKnownTurnSessionIDs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		stack   string
		nilDB   bool
		closeDB bool
		seedSQL string
		wantErr string
		wantNil bool
		wantIDs []string
	}{
		{name: "nil database", nilDB: true, stack: "stack-a", wantErr: ErrNilDatabase.Error()},
		{name: "empty stack read-only guard", stack: "", wantErr: "stack cannot be empty"},
		{name: "closed handle fails the query", stack: "stack-a", closeDB: true, wantErr: "query known turn session ids"},
		{name: "empty turn tables returns nil", stack: "stack-a", wantNil: true},
		{
			name:    "heartbeat only returns empty non-nil map",
			stack:   "stack-a",
			seedSQL: "INSERT INTO sessions (session_id, conversation_id) VALUES ('heartbeat-check', 'hb-conv-1'); INSERT INTO dispatches (id, source, conversation_id) VALUES ('d-hb', 'heartbeat', 'hb-disp-1');",
			wantIDs: []string{},
		},
		{
			name:  "populated turn tables filters heartbeats and returns allowed session ids",
			stack: "stack-a",
			seedSQL: "INSERT INTO messages (id, conversation_id) VALUES ('m1', 'conv-msg-1'); " +
				"INSERT INTO sessions (session_id, conversation_id, previous_conversation_id) VALUES ('discord-1', 'conv-sess-1', 'conv-prev-1'); " +
				"INSERT INTO sessions (session_id, conversation_id, previous_conversation_id) VALUES ('heartbeat-task', 'hb-conv-sess', 'hb-conv-prev'); " +
				"INSERT INTO dispatches (id, source, conversation_id) VALUES ('d1', 'user', 'conv-disp-1'), ('d2', 'heartbeat', 'hb-disp-ignored');",
			wantIDs: []string{"conv-msg-1", "conv-sess-1", "conv-prev-1", "conv-disp-1"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var database DBTX
			if !tc.nilDB {
				database = seededTestDB(t, tc.seedSQL, tc.closeDB)
			}
			got, err := NewSQLTranscriptStore(database, tc.stack).GetKnownTurnSessionIDs(context.Background())
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantNil != (got == nil) {
				t.Fatalf("got %+v, wantNil=%t", got, tc.wantNil)
			}
			if len(got) != len(tc.wantIDs) {
				t.Fatalf("got %d session IDs (%v), want %d (%v)", len(got), got, len(tc.wantIDs), tc.wantIDs)
			}
			for _, wantID := range tc.wantIDs {
				if !got[wantID] {
					t.Errorf("missing expected session ID %q in got %+v", wantID, got)
				}
			}
		})
	}
}

// seededTestDB opens a private database, runs seedSQL, and optionally closes
// the handle so the next query fails.
func seededTestDB(t *testing.T, seedSQL string, closeDB bool) DBTX {
	t.Helper()
	sqlDB := newTestDB(t)
	execSQL(t, sqlDB, seedSQL)
	if closeDB {
		if err := sqlDB.Close(); err != nil {
			t.Fatalf("close test database: %v", err)
		}
	}
	return sqlDB
}
