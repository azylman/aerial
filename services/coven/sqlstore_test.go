package main

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

type testCustomResult struct {
	rows int64
	err  error
}

func (testCustomResult) LastInsertId() (int64, error)   { return 0, nil }
func (r testCustomResult) RowsAffected() (int64, error) { return r.rows, r.err }

type queryMockDBTX struct {
	dbMem         *sql.DB
	queryErr      error
	execErr       error
	rowsAffected  int64
	capturedQuery *string
	rowSQL        string
	rowsSQL       string
	watermarkSQL  string
	summarySQL    string
	joinSQL       string
}

func (q queryMockDBTX) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if q.capturedQuery != nil {
		*q.capturedQuery = query
	}
	if q.execErr != nil {
		return nil, q.execErr
	}
	return testCustomResult{rows: q.rowsAffected}, nil
}

func (q queryMockDBTX) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if q.capturedQuery != nil {
		*q.capturedQuery = query
	}
	if q.queryErr != nil {
		if errors.Is(q.queryErr, sql.ErrNoRows) {
			return q.dbMem.QueryRowContext(ctx, "SELECT 1 WHERE 1 = 0")
		}
		return q.dbMem.QueryRowContext(ctx, "SELECT syntax error")
	}
	if strings.Contains(query, "COUNT(*)") {
		return q.dbMem.QueryRowContext(ctx, "SELECT 1")
	}
	if q.watermarkSQL != "" && strings.Contains(query, "extraction_watermarks") {
		return q.dbMem.QueryRowContext(ctx, q.watermarkSQL)
	}
	if strings.Contains(query, "JOIN sessions") {
		if q.joinSQL != "" {
			return q.dbMem.QueryRowContext(ctx, q.joinSQL)
		}
		return q.dbMem.QueryRowContext(ctx, "SELECT 1 WHERE 1 = 0")
	}
	if strings.Contains(query, "sessions") {
		if q.summarySQL != "" {
			return q.dbMem.QueryRowContext(ctx, q.summarySQL)
		}
		return q.dbMem.QueryRowContext(ctx, "SELECT 1 WHERE 1 = 0")
	}
	if q.rowSQL != "" {
		return q.dbMem.QueryRowContext(ctx, q.rowSQL)
	}
	return q.dbMem.QueryRowContext(ctx, "SELECT 1")
}

func (q queryMockDBTX) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if q.capturedQuery != nil {
		*q.capturedQuery = query
	}
	if q.queryErr != nil {
		return nil, q.queryErr
	}
	if q.rowsSQL != "" {
		return q.dbMem.QueryContext(ctx, q.rowsSQL)
	}
	if q.rowSQL != "" {
		return q.dbMem.QueryContext(ctx, q.rowSQL)
	}
	return q.dbMem.QueryContext(ctx, "SELECT 1 WHERE 1 = 0")
}

func setupTestDBMem(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestSQLStore_ConstructorsAndSetters(t *testing.T) {
	dbMem := setupTestDBMem(t)

	tests := []struct {
		name      string
		nilStore  bool
		setStack  string
		wantStack string
	}{
		{name: "DefaultStack", wantStack: DefaultStack},
		{name: "CustomStack", setStack: "agent-alpha", wantStack: "agent-alpha"},
		{name: "StackWithWhitespace", setStack: "  agent-beta  ", wantStack: "agent-beta"},
		{name: "WhitespaceStackResetsToDefault", setStack: "   ", wantStack: DefaultStack},
		{name: "NilStoreFallback", nilStore: true, setStack: "agent-alpha", wantStack: DefaultStack},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := NewSQLStore(dbMem)
			wantDB := DBTX(dbMem)
			if tc.nilStore {
				store = nil
				wantDB = nil
			}
			store.SetStack(tc.setStack)
			ic := NewIndexerClient("http://transcripts", "test", nil)
			store.SetIndexerClient(ic)
			if got := store.effectiveStack(); got != tc.wantStack {
				t.Errorf("effectiveStack() = %q, want %q", got, tc.wantStack)
			}
			if got := store.DB(); got != wantDB {
				t.Errorf("DB() = %v, want %v", got, wantDB)
			}
			if !tc.nilStore && store.indexerClient != ic {
				t.Errorf("indexerClient = %v, want %v", store.indexerClient, ic)
			}
			if _, err := store.Conn(context.Background()); tc.nilStore && err == nil {
				t.Errorf("Conn() expected error for nil store")
			}
		})
	}

	mockStore := NewSQLStore(queryMockDBTX{dbMem: dbMem})
	if c, err := mockStore.Conn(context.Background()); !errors.Is(err, errNoPinnedConn) || c != nil {
		t.Errorf("mockStore.Conn() = (%v, %v), want (nil, errNoPinnedConn)", c, err)
	}
}

func TestSQLStore_InsertFact(t *testing.T) {
	dbMem := setupTestDBMem(t)
	validEmb := make([]float32, ExpectedEmbeddingDim)

	tests := []sqlStoreInsertFactCase{
		{
			name:     "NilDBError",
			nilDB:    true,
			factText: "valid",
			emb:      validEmb,
			wantErr:  true,
		},
		{
			name:     "EmptyFactText",
			factText: "",
			emb:      validEmb,
			wantErr:  true,
		},
		{
			name:     "InvalidEmbeddingDim",
			factText: "valid",
			emb:      make([]float32, 10),
			wantErr:  true,
		},
		{
			name:     "QueryError",
			factText: "valid fact",
			emb:      validEmb,
			queryErr: errors.New("insert error"),
			wantErr:  true,
		},
		{
			name:     "SuccessValidFact",
			factText: "valid fact",
			emb:      validEmb,
			rowSQL:   "SELECT 42",
			wantID:   42,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runInsertFactCase(t, tc, dbMem)
		})
	}
}

func TestSQLStore_UpdateFact(t *testing.T) {
	dbMem := setupTestDBMem(t)
	validEmb := make([]float32, ExpectedEmbeddingDim)

	tests := []sqlStoreUpdateFactCase{
		{
			name:     "NilDBError",
			nilDB:    true,
			factText: "valid",
			emb:      validEmb,
			wantErr:  true,
		},
		{
			name:     "EmptyFactText",
			factText: "",
			emb:      validEmb,
			wantErr:  true,
		},
		{
			name:     "InvalidEmbeddingDim",
			factText: "valid",
			emb:      make([]float32, 10),
			wantErr:  true,
		},
		{
			name:     "ExecError",
			factText: "valid",
			emb:      validEmb,
			execErr:  errors.New("update err"),
			wantErr:  true,
		},
		{
			name:         "NotFoundZeroRows",
			factText:     "valid",
			emb:          validEmb,
			rowsAffected: 0,
			wantErr:      true,
		},
		{
			name:         "SuccessValidFact",
			factText:     "valid",
			emb:          validEmb,
			rowsAffected: 1,
			wantErr:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runUpdateFactCase(t, tc, dbMem)
		})
	}
}

func TestSQLStore_ReinforceFact(t *testing.T) {
	dbMem := setupTestDBMem(t)
	for _, tc := range sqlStoreFactMutateCases("reinforce") {
		t.Run(tc.name, func(t *testing.T) {
			runSQLStoreFactMutateCase(t, "ReinforceFact", tc, dbMem)
		})
	}
}

func TestSQLStore_DeleteFact(t *testing.T) {
	dbMem := setupTestDBMem(t)
	for _, tc := range sqlStoreFactMutateCases("delete") {
		t.Run(tc.name, func(t *testing.T) {
			runSQLStoreFactMutateCase(t, "DeleteFact", tc, dbMem)
		})
	}
}

func TestSQLStore_FindDuplicateFact(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()

	tests := []struct {
		name       string
		nilDB      bool
		embLen     int
		minSim     float64
		queryErr   error
		rowSQL     string
		wantErr    bool
		wantNil    bool
		wantID     int64
		wantSimMin float64
	}{
		{
			name:    "NilDBError",
			nilDB:   true,
			embLen:  ExpectedEmbeddingDim,
			wantErr: true,
		},
		{
			name:    "ShortEmbeddingReturnsNil",
			embLen:  10,
			wantNil: true,
		},
		{
			name:    "ErrNoRowsReturnsNil",
			embLen:  ExpectedEmbeddingDim,
			minSim:  -1,
			rowSQL:  "SELECT 1 WHERE 1=0",
			wantNil: true,
		},
		{
			name:     "QueryError",
			embLen:   ExpectedEmbeddingDim,
			queryErr: errors.New("dup query error"),
			wantErr:  true,
		},
		{
			name:       "MatchedAboveMin",
			embLen:     ExpectedEmbeddingDim,
			minSim:     0.85,
			rowSQL:     "SELECT 7, 'fact text', '2026-10-21 09:00:00', '2026-10-21 09:00:00', 2, 0.92",
			wantID:     7,
			wantSimMin: 0.90,
		},
		{
			name:    "MatchedBelowMin",
			embLen:  ExpectedEmbeddingDim,
			minSim:  0.85,
			rowSQL:  "SELECT 8, 'fact text', '2026-10-21 09:00:00', '2026-10-21 09:00:00', 2, 0.40",
			wantNil: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var store *SQLStore
			if tc.nilDB {
				store = &SQLStore{}
			} else {
				store = NewSQLStore(queryMockDBTX{
					dbMem:    dbMem,
					queryErr: tc.queryErr,
					rowSQL:   tc.rowSQL,
				})
			}
			testEmb := make([]float32, tc.embLen)
			dup, sim, err := store.FindDuplicateFact(ctx, testEmb, tc.minSim)
			if (err != nil) != tc.wantErr {
				t.Fatalf("FindDuplicateFact() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantNil && dup != nil {
				t.Errorf("expected nil duplicate, got %+v", dup)
			}
			if tc.wantID > 0 {
				if dup == nil || dup.ID != tc.wantID {
					t.Errorf("dup.ID = %v, want %d", dup, tc.wantID)
				}
				if sim < tc.wantSimMin {
					t.Errorf("sim = %f, want >= %f", sim, tc.wantSimMin)
				}
			}
		})
	}
}

func TestSQLStore_SearchCandidateFacts(t *testing.T) {
	dbMem := setupTestDBMem(t)
	emb := make([]float32, ExpectedEmbeddingDim)

	tests := []sqlStoreSearchCandidateFactsCase{
		{
			name:    "NilDBError",
			nilDB:   true,
			emb:     emb,
			query:   "q",
			wantErr: true,
		},
		{
			name:      "ShortEmbeddingReturnsNil",
			emb:       make([]float32, 10),
			query:     "fact",
			wantCount: 0,
		},
		{
			name:      "EmptyQueryReturnsNil",
			emb:       emb,
			query:     "",
			wantCount: 0,
		},
		{
			name:     "BothWithDefaultBounds",
			emb:      emb,
			query:    "test query",
			limit:    -1,
			minScore: -1,
		},
		{
			name:     "QueryError",
			emb:      emb,
			query:    "test query",
			queryErr: errors.New("search failure"),
			wantErr:  true,
		},
		{
			name:      "SuccessfulRowsScan",
			emb:       emb,
			query:     "fact",
			rowSQL:    "SELECT 5, 'scanned fact', '2026-10-21 09:00:00', '2026-10-21 09:00:00', 1, '[0.1,0.2]', 0.88",
			wantCount: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runSearchCandidateFactsCase(t, tc, dbMem)
		})
	}
}

func TestSQLStore_GetFactsPaginated(t *testing.T) {
	dbMem := setupTestDBMem(t)

	tests := []sqlStoreGetFactsPaginatedCase{
		{
			name:    "NilDBError",
			nilDB:   true,
			wantErr: true,
		},
		{
			name:     "CountQueryError",
			filter:   FactsFilter{Limit: 10},
			queryErr: errors.New("count err"),
			wantErr:  true,
		},
		{
			name:         "SuccessWithQuery",
			filter:       FactsFilter{Query: "linux", Limit: 5, Offset: -1},
			wantPatterns: []string{"LOWER(fact_text) LIKE", "LIMIT 5 OFFSET 0"},
		},
		{
			name:         "OffsetOnly",
			filter:       FactsFilter{Offset: 10},
			wantPatterns: []string{"OFFSET 10"},
		},
		{
			name:      "SuccessfulRowsScan",
			filter:    FactsFilter{Limit: 5},
			rowSQL:    "SELECT 1, 'fact', '2026-10-21 09:00:00', '2026-10-21 09:00:00', 1",
			wantTotal: 1,
		},
		{
			name:     "SelectQueryError",
			filter:   FactsFilter{Limit: 5},
			rowSQL:   "syntax error",
			queryErr: errors.New("select query err"),
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runGetFactsPaginatedCase(t, tc, dbMem)
		})
	}
}

func TestIsExtractableAdminTurn(t *testing.T) {
	tests := []struct {
		name string
		msg  Message
		want bool
	}{
		{
			name: "CompletedAdminNormalText",
			msg: Message{
				Status:       "COMPLETED",
				ResponseText: "Here is your answer.",
				IsAdmin:      true,
			},
			want: true,
		},
		{
			name: "NotCompleted",
			msg: Message{
				Status:       "PENDING",
				ResponseText: "Here is your answer.",
				IsAdmin:      true,
			},
			want: false,
		},
		{
			name: "NotAdmin",
			msg: Message{
				Status:       "COMPLETED",
				ResponseText: "Here is your answer.",
				IsAdmin:      false,
			},
			want: false,
		},
		{
			name: "BracketPrefix",
			msg: Message{
				Status:       "COMPLETED",
				ResponseText: "[SILENT_TURN]",
				IsAdmin:      true,
			},
			want: false,
		},
		{
			name: "EmptyResponseText",
			msg: Message{
				Status:       "COMPLETED",
				ResponseText: "   ",
				IsAdmin:      true,
			},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := IsExtractableAdminTurn(tc.msg)
			if got != tc.want {
				t.Fatalf("IsExtractableAdminTurn() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSQLStore_GetUnextractedAdminTurns(t *testing.T) {
	dbMem := setupTestDBMem(t)

	tests := []unextractedAdminTurnsTestCase{
		{
			name:     "NilDBError",
			nilDB:    true,
			threadID: "th-1",
			wantErr:  true,
		},
		{
			name:      "EmptyThreadID",
			threadID:  "   ",
			wantErr:   false,
			wantEmpty: true,
		},
		{
			name:     "DBQueryError",
			threadID: "th-1",
			queryErr: errors.New("query error"),
			wantErr:  true,
		},
		{
			name:         "Success",
			threadID:     "th-1",
			limit:        50,
			agent:        "agent-alpha",
			watermarkSQL: "SELECT 10, NULL",
			rowsSQL:      "SELECT 'm1', 12, 'th-1', 'hello', 'world', 'COMPLETED', 1, '2026-01-01 00:00:00', '2026-01-01 00:00:00'",
			wantCount:    1,
			wantRowIDs:   []int64{12},
			wantPatterns: []string{"FROM messages", "WHERE agent = $1 AND thread_id = $2"},
		},
		{
			name:         "Success_MultipleTurnsSortedAscending",
			threadID:     "th-2",
			limit:        50,
			watermarkSQL: "SELECT 0, NULL",
			rowsSQL:      "SELECT 'm1', 10, 'th-2', 'turn 1', 'answer 1', 'COMPLETED', 1, '2026-01-01 00:00:00', '2026-01-01 00:00:00' UNION ALL SELECT 'm2', 25, 'th-2', 'turn 2', 'answer 2', 'COMPLETED', 1, '2026-01-01 00:00:00', '2026-01-01 00:00:00'",
			wantCount:    2,
			wantRowIDs:   []int64{10, 25},
			wantPatterns: []string{"FROM messages", "WHERE agent = $1 AND thread_id = $2"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runGetUnextractedAdminTurnsCase(t, tc, dbMem)
		})
	}
}

func TestSQLStore_GetMaxMessageRowID(t *testing.T) {
	dbMem := setupTestDBMem(t)
	ctx := context.Background()

	tests := []struct {
		name     string
		nilDB    bool
		threadID string
		agent    string
		queryErr error
		rowSQL   string
		wantErr  bool
		wantID   int64
	}{
		{
			name:     "NilDBError",
			nilDB:    true,
			threadID: "th-1",
			wantErr:  true,
		},
		{
			name:     "EmptyThreadID",
			threadID: "",
			wantErr:  false,
			wantID:   0,
		},
		{
			name:     "QueryError",
			threadID: "th-1",
			queryErr: errors.New("db error"),
			wantErr:  true,
		},
		{
			name:     "SuccessWithRow",
			threadID: "th-1",
			agent:    "agent-alpha",
			rowSQL:   "SELECT 42",
			wantID:   42,
		},
		{
			name:     "SuccessEmptyDefaultZero",
			threadID: "th-2",
			rowSQL:   "SELECT 0",
			wantID:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var store *SQLStore
			if tc.nilDB {
				store = &SQLStore{}
			} else {
				store = NewSQLStore(queryMockDBTX{
					dbMem:    dbMem,
					queryErr: tc.queryErr,
					rowSQL:   tc.rowSQL,
				})
			}

			id, err := store.GetMaxMessageRowID(ctx, tc.threadID, tc.agent)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetMaxMessageRowID() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && id != tc.wantID {
				t.Errorf("GetMaxMessageRowID() = %d, want %d", id, tc.wantID)
			}
		})
	}
}

func TestSQLStore_UpdateConversationFactWatermark(t *testing.T) {
	dbMem := setupTestDBMem(t)

	tests := []sqlStoreUpdateWatermarkCase{
		{
			name:     "NilDBError",
			nilDB:    true,
			threadID: "th-1",
			wantErr:  true,
		},
		{
			name:     "EmptyThreadID",
			threadID: "",
			wantErr:  false,
		},
		{
			name:     "ExecError",
			threadID: "th-1",
			execErr:  errors.New("exec error"),
			wantErr:  true,
		},
		{
			name:         "Success",
			threadID:     "th-1",
			maxRowID:     99,
			wantPatterns: []string{"INSERT INTO extraction_watermarks (agent, thread_id", "ON CONFLICT (agent, thread_id) DO UPDATE"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runUpdateConversationFactWatermarkCase(t, tc, dbMem)
		})
	}
}

func TestSQLStore_FindNearestScoredFacts(t *testing.T) {
	dbMem := setupTestDBMem(t)
	emb := make([]float32, ExpectedEmbeddingDim)

	tests := []sqlStoreFindNearestScoredFactsCase{
		{
			name:    "NilDBError",
			nilDB:   true,
			emb:     emb,
			wantErr: true,
		},
		{
			name:      "InvalidEmbeddingDim",
			emb:       []float32{1.0, 2.0},
			wantErr:   false,
			wantCount: 0,
		},
		{
			name:     "QueryError",
			emb:      emb,
			queryErr: errors.New("nearest scored query error"),
			wantErr:  true,
		},
		{
			name:    "RowScanError",
			emb:     emb,
			rowSQL:  "SELECT 'invalid' AS id",
			wantErr: true,
		},
		{
			name:         "SuccessDefaults",
			emb:          emb,
			limit:        -1,
			minSim:       -1,
			rowSQL:       "SELECT 10, 'Morning walk', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 1, 0.77",
			wantCount:    1,
			wantMinScore: 0.77,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runFindNearestScoredFactsCase(t, tc, dbMem)
		})
	}
}

type sqlStoreInsertFactCase struct {
	name     string
	nilDB    bool
	factText string
	emb      []float32
	queryErr error
	rowSQL   string
	wantErr  bool
	wantID   int64
}

func runInsertFactCase(t *testing.T, tc sqlStoreInsertFactCase, dbMem *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var store *SQLStore
	if tc.nilDB {
		store = &SQLStore{}
	} else {
		store = NewSQLStore(queryMockDBTX{
			dbMem:    dbMem,
			queryErr: tc.queryErr,
			rowSQL:   tc.rowSQL,
		})
	}
	id, err := store.InsertFact(ctx, tc.factText, tc.emb)
	if (err != nil) != tc.wantErr {
		t.Fatalf("InsertFact() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantID > 0 && id != tc.wantID {
		t.Errorf("InsertFact() id = %d, want %d", id, tc.wantID)
	}
}

type sqlStoreUpdateFactCase struct {
	name         string
	nilDB        bool
	factText     string
	emb          []float32
	execErr      error
	rowsAffected int64
	wantErr      bool
}

func runUpdateFactCase(t *testing.T, tc sqlStoreUpdateFactCase, dbMem *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var store *SQLStore
	if tc.nilDB {
		store = &SQLStore{}
	} else {
		store = NewSQLStore(queryMockDBTX{
			dbMem:        dbMem,
			execErr:      tc.execErr,
			rowsAffected: tc.rowsAffected,
		})
	}
	err := store.UpdateFact(ctx, 10, tc.factText, tc.emb)
	if (err != nil) != tc.wantErr {
		t.Fatalf("UpdateFact() err = %v, wantErr = %v", err, tc.wantErr)
	}
}

type sqlStoreFactMutateCase struct {
	name         string
	nilDB        bool
	execErr      error
	rowsAffected int64
	wantErr      bool
}

func runSQLStoreFactMutateCase(t *testing.T, opName string, tc sqlStoreFactMutateCase, dbMem *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var store *SQLStore
	if tc.nilDB {
		store = &SQLStore{}
	} else {
		store = NewSQLStore(queryMockDBTX{
			dbMem:        dbMem,
			execErr:      tc.execErr,
			rowsAffected: tc.rowsAffected,
		})
	}
	var err error
	if opName == "ReinforceFact" {
		err = store.ReinforceFact(ctx, 10)
	} else {
		err = store.DeleteFact(ctx, 10)
	}
	if (err != nil) != tc.wantErr {
		t.Fatalf("%s() err = %v, wantErr = %v", opName, err, tc.wantErr)
	}
}

func sqlStoreFactMutateCases(errPrefix string) []sqlStoreFactMutateCase {
	return []sqlStoreFactMutateCase{
		{name: "NilDBError", nilDB: true, wantErr: true},
		{name: "ExecError", execErr: errors.New(errPrefix + " err"), wantErr: true},
		{name: "NotFoundZeroRows", rowsAffected: 0, wantErr: true},
		{name: "Success", rowsAffected: 1, wantErr: false},
	}
}

type sqlStoreSearchCandidateFactsCase struct {
	name         string
	nilDB        bool
	emb          []float32
	query        string
	limit        int
	minScore     float64
	queryErr     error
	rowSQL       string
	wantErr      bool
	wantPatterns []string
	wantCount    int
}

func runSearchCandidateFactsCase(t *testing.T, tc sqlStoreSearchCandidateFactsCase, dbMem *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var captured string
	var store *SQLStore
	if tc.nilDB {
		store = &SQLStore{}
	} else {
		store = NewSQLStore(queryMockDBTX{
			dbMem:         dbMem,
			queryErr:      tc.queryErr,
			rowSQL:        tc.rowSQL,
			capturedQuery: &captured,
		})
	}
	results, err := store.SearchCandidateFacts(ctx, tc.emb, tc.query, tc.limit, tc.minScore)
	if (err != nil) != tc.wantErr {
		t.Fatalf("SearchCandidateFacts() err = %v, wantErr = %v", err, tc.wantErr)
	}
	assertCapturedQueryPatterns(t, captured, tc.wantPatterns)
	if tc.wantCount > 0 && len(results) != tc.wantCount {
		t.Errorf("len(results) = %d, want %d", len(results), tc.wantCount)
	}
}

type sqlStoreGetFactsPaginatedCase struct {
	name         string
	nilDB        bool
	filter       FactsFilter
	queryErr     error
	rowSQL       string
	wantErr      bool
	wantPatterns []string
	wantTotal    int
}

func assertCapturedQueryPatterns(t *testing.T, captured string, patterns []string) {
	t.Helper()
	for _, pat := range patterns {
		if !strings.Contains(captured, pat) {
			t.Errorf("captured query missing %q: %s", pat, captured)
		}
	}
}

func runGetFactsPaginatedCase(t *testing.T, tc sqlStoreGetFactsPaginatedCase, dbMem *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var captured string
	var store *SQLStore
	if tc.nilDB {
		store = &SQLStore{}
	} else {
		store = NewSQLStore(queryMockDBTX{
			dbMem:         dbMem,
			queryErr:      tc.queryErr,
			rowSQL:        tc.rowSQL,
			capturedQuery: &captured,
		})
	}
	res, err := store.GetFactsPaginated(ctx, tc.filter)
	if (err != nil) != tc.wantErr {
		t.Fatalf("GetFactsPaginated() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if !tc.wantErr && tc.wantTotal > 0 && res != nil && res.Total != tc.wantTotal {
		t.Errorf("TotalCount = %d, want %d", res.Total, tc.wantTotal)
	}
	assertCapturedQueryPatterns(t, captured, tc.wantPatterns)
}

type unextractedAdminTurnsTestCase struct {
	name         string
	nilDB        bool
	threadID     string
	limit        int
	agent        string
	queryErr     error
	watermarkSQL string
	rowsSQL      string
	wantErr      bool
	wantEmpty    bool
	wantCount    int
	wantRowIDs   []int64
	wantPatterns []string
}

func runGetUnextractedAdminTurnsCase(t *testing.T, tc unextractedAdminTurnsTestCase, dbMem *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var captured string
	var store *SQLStore

	if tc.nilDB {
		store = &SQLStore{}
	} else {
		store = NewSQLStore(queryMockDBTX{
			dbMem:         dbMem,
			queryErr:      tc.queryErr,
			rowSQL:        tc.watermarkSQL,
			rowsSQL:       tc.rowsSQL,
			capturedQuery: &captured,
		})
	}

	var turns []Message
	var err error
	if tc.agent != "" {
		turns, err = store.GetUnextractedAdminTurns(ctx, tc.threadID, tc.limit, tc.agent)
	} else {
		turns, err = store.GetUnextractedAdminTurns(ctx, tc.threadID, tc.limit)
	}
	if (err != nil) != tc.wantErr {
		t.Fatalf("GetUnextractedAdminTurns() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantEmpty && len(turns) != 0 {
		t.Errorf("expected empty turns, got %d", len(turns))
	}
	if tc.wantCount > 0 && len(turns) != tc.wantCount {
		t.Errorf("len(turns) = %d, want %d", len(turns), tc.wantCount)
	}
	if len(tc.wantRowIDs) > 0 {
		assertTurnRowIDsMatch(t, turns, tc.wantRowIDs)
	}
	for _, pat := range tc.wantPatterns {
		if !strings.Contains(captured, pat) {
			t.Errorf("captured query missing %q: %s", pat, captured)
		}
	}
}

func assertTurnRowIDsMatch(t *testing.T, turns []Message, wantRowIDs []int64) {
	t.Helper()
	gotRowIDs := make([]int64, 0, len(turns))
	for _, turn := range turns {
		gotRowIDs = append(gotRowIDs, turn.RowID)
	}
	if len(gotRowIDs) != len(wantRowIDs) {
		t.Errorf("got rowIDs len = %d, want %d", len(gotRowIDs), len(wantRowIDs))
		return
	}
	for i, id := range gotRowIDs {
		if id != wantRowIDs[i] {
			t.Errorf("rowID[%d] = %d, want %d", i, id, wantRowIDs[i])
		}
	}
}

type sqlStoreUpdateWatermarkCase struct {
	name         string
	nilDB        bool
	threadID     string
	maxRowID     int64
	execErr      error
	wantErr      bool
	wantPatterns []string
}

func runUpdateConversationFactWatermarkCase(t *testing.T, tc sqlStoreUpdateWatermarkCase, dbMem *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var captured string
	var store *SQLStore
	if tc.nilDB {
		store = &SQLStore{}
	} else {
		store = NewSQLStore(queryMockDBTX{
			dbMem:         dbMem,
			execErr:       tc.execErr,
			capturedQuery: &captured,
		})
	}
	if err := store.UpdateConversationFactWatermark(ctx, tc.threadID, tc.maxRowID); (err != nil) != tc.wantErr {
		t.Fatalf("UpdateConversationFactWatermark() err = %v, wantErr = %v", err, tc.wantErr)
	}
	assertCapturedQueryPatterns(t, captured, tc.wantPatterns)
}

type sqlStoreFindNearestScoredFactsCase struct {
	name         string
	nilDB        bool
	emb          []float32
	limit        int
	minSim       float64
	queryErr     error
	rowSQL       string
	wantErr      bool
	wantCount    int
	wantMinScore float64
}

func runFindNearestScoredFactsCase(t *testing.T, tc sqlStoreFindNearestScoredFactsCase, dbMem *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var captured string
	var store *SQLStore
	if tc.nilDB {
		store = &SQLStore{}
	} else {
		store = NewSQLStore(queryMockDBTX{
			dbMem:         dbMem,
			queryErr:      tc.queryErr,
			rowSQL:        tc.rowSQL,
			capturedQuery: &captured,
		})
	}
	scored, err := store.FindNearestScoredFacts(ctx, tc.emb, tc.limit, tc.minSim)
	if (err != nil) != tc.wantErr {
		t.Fatalf("FindNearestScoredFacts() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantCount > 0 && len(scored) != tc.wantCount {
		t.Errorf("len(scored) = %d, want %d", len(scored), tc.wantCount)
	}
	if tc.wantMinScore > 0 && len(scored) > 0 && scored[0].Score < tc.wantMinScore {
		t.Errorf("scored[0].Score = %f, want >= %f", scored[0].Score, tc.wantMinScore)
	}
}
