package db

import (
	"context"
	"database/sql"
	"errors"
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

type testCustomResult struct {
	rows int64
	err  error
}

func (testCustomResult) LastInsertId() (int64, error)   { return 0, nil }
func (r testCustomResult) RowsAffected() (int64, error) { return r.rows, r.err }

type testMockDBTX struct {
	execFn     func(ctx context.Context, query string, args ...any) (sql.Result, error)
	queryFn    func(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	queryRowFn func(ctx context.Context, query string, args ...any) *sql.Row
}

func (m testMockDBTX) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if m.execFn != nil {
		return m.execFn(ctx, query, args...)
	}
	return testCustomResult{}, nil
}

func (m testMockDBTX) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if m.queryFn != nil {
		return m.queryFn(ctx, query, args...)
	}
	return nil, nil
}

func (m testMockDBTX) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if m.queryRowFn != nil {
		return m.queryRowFn(ctx, query, args...)
	}
	return nil
}

type sqlKnownTurnCase struct {
	name      string
	stack     string
	nilDB     bool
	queryErr  error
	customSQL string
	seedSQL   string
	wantErr   string
	wantNil   bool
	wantIDs   []string
}

func runSQLTranscriptStoreKnownTurnCase(t *testing.T, tc sqlKnownTurnCase) {
	t.Helper()
	dbMem := initTestSQLiteDB(t)
	if tc.seedSQL != "" {
		if _, err := dbMem.Exec(tc.seedSQL); err != nil {
			t.Fatalf("seedSQL failed: %v", err)
		}
	}
	var database DBTX = dbMem
	if tc.queryErr != nil || tc.customSQL != "" {
		database = testMockDBTX{
			queryFn: func(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
				if tc.queryErr != nil {
					return nil, tc.queryErr
				}
				return dbMem.QueryContext(ctx, tc.customSQL)
			},
		}
	}
	if tc.nilDB {
		database = nil
	}
	store := NewSQLTranscriptStore(database, tc.stack)
	got, err := store.GetKnownTurnSessionIDs(context.Background())
	if tc.wantErr != "" {
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tc.wantNil {
		if got != nil {
			t.Fatalf("expected nil map, got %+v", got)
		}
		return
	}
	if got == nil {
		t.Fatalf("expected non-nil map, got nil")
	}
	if len(got) != len(tc.wantIDs) {
		t.Fatalf("got %d session IDs (%v), want %d (%v)", len(got), got, len(tc.wantIDs), tc.wantIDs)
	}
	for _, wantID := range tc.wantIDs {
		if !got[wantID] {
			t.Errorf("missing expected session ID %q in got %+v", wantID, got)
		}
	}
}

func TestSQLTranscriptStore_GetKnownTurnSessionIDs(t *testing.T) {
	t.Parallel()
	tests := []sqlKnownTurnCase{
		{name: "nil database", nilDB: true, stack: "stack-a", wantErr: ErrNilDatabase.Error()},
		{name: "empty stack read-only guard", stack: "", wantErr: "stack cannot be empty"},
		{name: "query error", stack: "stack-a", queryErr: errors.New("db down"), wantErr: "query known turn session ids"},
		{name: "scan error", stack: "stack-a", customSQL: "SELECT 1", wantErr: "scan known turn session id"},
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
			runSQLTranscriptStoreKnownTurnCase(t, tc)
		})
	}
}
