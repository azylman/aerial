package main

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	"github.com/azylman/aerial/pkg/httpkit"

	_ "modernc.org/sqlite"
)

type testCustomResult struct {
	rows int64
	err  error
}

func (r testCustomResult) LastInsertId() (int64, error) { return 0, nil }
func (r testCustomResult) RowsAffected() (int64, error) { return r.rows, r.err }

type queryMockDBTX struct {
	dbMem         *sql.DB
	queryErr      error
	execErr       error
	rowsAffected  int64
	capturedQuery *string
	rowSQL        string
	rowsSQL       string
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
		name         string
		initialURL   string
		setURL       string
		setToken     string
		wantBrainURL string
		wantToken    string
	}{
		{
			name:         "DefaultURL",
			initialURL:   "",
			wantBrainURL: DefaultBrainURL,
		},
		{
			name:         "CustomURL",
			initialURL:   "http://my-eri:5000/",
			setToken:     "test-token",
			wantBrainURL: "http://my-eri:5000",
			wantToken:    "test-token",
		},
		{
			name:         "SetterWithWhitespace",
			initialURL:   DefaultBrainURL,
			setURL:       " http://new-eri:6000/ ",
			setToken:     "  padded-token  ",
			wantBrainURL: "http://new-eri:6000",
			wantToken:    "padded-token",
		},
		{
			name:         "SetterEmptyResetsToDefault",
			initialURL:   "http://custom:1234",
			setURL:       "   ",
			wantBrainURL: DefaultBrainURL,
		},
		{
			name:         "NilStoreFallback",
			initialURL:   DefaultBrainURL,
			wantBrainURL: DefaultBrainURL,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var store *SQLStore
			if tc.name != "NilStoreFallback" {
				store = NewSQLStoreWithBrainURL(dbMem, tc.initialURL, nil)
				if tc.setURL != "" {
					store.SetBrainURL(tc.setURL)
				}
				if tc.setToken != "" {
					store.SetBrainToken(tc.setToken)
				}
			}
			if got := store.getBrainURL(); got != tc.wantBrainURL {
				t.Errorf("getBrainURL() = %q, want %q", got, tc.wantBrainURL)
			}
			if store != nil && store.brainToken != tc.wantToken {
				t.Errorf("brainToken = %q, want %q", store.brainToken, tc.wantToken)
			}
		})
	}
}

func TestSQLStore_FetchBrainJSON(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		token      string
		serverCode int
		serverBody string
		wantErr    bool
		wantVal    string
	}{
		{
			name:       "SuccessWithToken",
			token:      "secret-token",
			serverCode: http.StatusOK,
			serverBody: `{"val":"hello"}`,
			wantErr:    false,
			wantVal:    "hello",
		},
		{
			name:       "NonOKStatus",
			serverCode: http.StatusNotFound,
			serverBody: `not found`,
			wantErr:    true,
		},
		{
			name:       "InvalidJSON",
			serverCode: http.StatusOK,
			serverBody: `{bad json}`,
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotAuthHeader string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotAuthHeader = r.Header.Get("Authorization")
				w.WriteHeader(tc.serverCode)
				_, _ = w.Write([]byte(tc.serverBody))
			}))
			defer srv.Close()

			store := &SQLStore{
				brainURL:   srv.URL,
				httpClient: srv.Client(),
				brainToken: tc.token,
			}
			var dst struct {
				Val string `json:"val"`
			}
			err := store.fetchBrainJSON(ctx, srv.URL+"/test", "test", &dst)
			if (err != nil) != tc.wantErr {
				t.Fatalf("fetchBrainJSON() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				if dst.Val != tc.wantVal {
					t.Errorf("dst.Val = %q, want %q", dst.Val, tc.wantVal)
				}
				if tc.token != "" && gotAuthHeader != "Bearer "+tc.token {
					t.Errorf("got auth = %q, want Bearer %s", gotAuthHeader, tc.token)
				}
			}
		})
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
			name:         "BothWithDefaultBounds",
			emb:          emb,
			query:        "test query",
			limit:        -1,
			minScore:     -1,
			wantPatterns: []string{"vector_hits", "text_hits", "rrf"},
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

func TestIsClientDisconnect(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	reqCanceled, _ := http.NewRequestWithContext(canceledCtx, http.MethodGet, "/health", nil)

	tests := []struct {
		name string
		req  *http.Request
		err  error
		want bool
	}{
		{"NilError", nil, nil, false},
		{"ContextCanceled", reqCanceled, errors.New("other"), true},
		{"SyscallEPIPE", nil, syscall.EPIPE, true},
		{"SyscallECONNRESET", nil, syscall.ECONNRESET, true},
		{"NetErrClosed", nil, net.ErrClosed, true},
		{"UnrelatedError", nil, errors.New("unrelated"), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := httpkit.IsClientDisconnect(tc.req, tc.err)
			if got != tc.want {
				t.Errorf("isClientDisconnect() = %v, want %v", got, tc.want)
			}
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

func TestSQLStore_GetActiveConversationsForExtraction(t *testing.T) {
	dbMem := setupTestDBMem(t)

	tests := []activeConversationsTestCase{
		{
			name:    "NilDBError",
			nilDB:   true,
			wantErr: true,
		},
		{
			name:     "DBQueryError",
			queryErr: errors.New("query failed"),
			wantErr:  true,
		},
		{
			name:      "EriHTTPError",
			eriStatus: http.StatusInternalServerError,
			eriBody:   `{"error":"failed"}`,
			wantErr:   true,
		},
		{
			name:    "EriJSONError",
			eriBody: `not-valid-json`,
			wantErr: true,
		},
		{
			name:         "SuccessDefaultHours",
			activeHours:  -1,
			watermarkSQL: "SELECT 'th-1', 5 UNION ALL SELECT 'th-2', 10",
			eriBody: `{"status":"ok","tasks":[
				{"thread_id":"th-1","row_id":10},
				{"thread_id":"th-2","row_id":5},
				{"thread_id":"th-3","row_id":20},
				{"thread_id":"th-1","row_id":12}
			]}`,
			wantThreads:  []string{"th-1", "th-3"},
			wantPatterns: []string{"SELECT thread_id, COALESCE(last_extracted_rowid, 0) FROM extraction_watermarks"},
		},
		{
			name:         "SuccessCustomHours",
			activeHours:  48,
			watermarkSQL: "SELECT 'th-9', 0",
			eriBody: `{"status":"ok","tasks":[
				{"thread_id":"th-9","row_id":100}
			]}`,
			wantThreads:  []string{"th-9"},
			wantPatterns: []string{"SELECT thread_id, COALESCE(last_extracted_rowid, 0) FROM extraction_watermarks"},
			wantURLSub:   "limit=200",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runGetActiveConversationsCase(t, tc, dbMem)
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
			name:         "EriHTTPError",
			threadID:     "th-1",
			watermarkSQL: "SELECT 5, NULL",
			eriStatus:    http.StatusInternalServerError,
			wantErr:      true,
		},
		{
			name:         "EriJSONError",
			threadID:     "th-1",
			watermarkSQL: "SELECT 5, NULL",
			eriBody:      "invalid-json",
			wantErr:      true,
		},
		{
			name:         "Success",
			threadID:     "th-1",
			watermarkSQL: "SELECT 10, NULL",
			eriBody: `{"status":"ok","tasks":[
				{"id":"m1","row_id":12,"thread_id":"th-1","prompt":"hello","response_text":"world","status":"COMPLETED","is_admin":true},
				{"id":"m2","row_id":14,"thread_id":"th-1","prompt":"skip","response_text":"[SILENT]","status":"COMPLETED","is_admin":true},
				{"id":"m3","row_id":15,"thread_id":"th-1","prompt":"non-admin","response_text":"ok","status":"COMPLETED","is_admin":false}
			]}`,
			wantCount:    1,
			wantRowIDs:   []int64{12},
			wantPatterns: []string{"FROM extraction_watermarks WHERE thread_id = $1"},
			wantURLSub:   "after_row_id=10",
		},
		{
			name:         "Success_ReverseOrderSortedAscending",
			threadID:     "th-2",
			watermarkSQL: "SELECT 0, NULL",
			eriBody: `{"status":"ok","tasks":[
				{"id":"m2","row_id":25,"thread_id":"th-2","prompt":"turn 2","response_text":"answer 2","status":"COMPLETED","is_admin":true},
				{"id":"m1","row_id":10,"thread_id":"th-2","prompt":"turn 1","response_text":"answer 1","status":"COMPLETED","is_admin":true}
			]}`,
			wantCount:    2,
			wantRowIDs:   []int64{10, 25},
			wantPatterns: []string{"FROM extraction_watermarks WHERE thread_id = $1"},
			wantURLSub:   "after_row_id=0",
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
		name       string
		nilDB      bool
		threadID   string
		eriStatus  int
		eriBody    string
		wantErr    bool
		wantID     int64
		wantURLSub string
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
			name:      "EriHTTPError",
			threadID:  "th-1",
			eriStatus: http.StatusInternalServerError,
			wantErr:   true,
		},
		{
			name:     "EmptyTasks",
			threadID: "th-1",
			eriBody:  `{"status":"ok","tasks":[]}`,
			wantID:   0,
		},
		{
			name:       "SuccessWithRow",
			threadID:   "th-1",
			eriBody:    `{"status":"ok","tasks":[{"row_id":42}]}`,
			wantID:     42,
			wantURLSub: "status=COMPLETED&limit=1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var capturedURL string
			var store *SQLStore

			if tc.nilDB {
				store = &SQLStore{}
			} else {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					capturedURL = r.URL.String()
					if tc.eriStatus != 0 {
						w.WriteHeader(tc.eriStatus)
					}
					if tc.eriBody != "" {
						_, _ = w.Write([]byte(tc.eriBody))
					}
				}))
				defer server.Close()

				store = NewSQLStoreWithBrainURL(queryMockDBTX{
					dbMem: dbMem,
				}, server.URL, server.Client())
			}

			id, err := store.GetMaxMessageRowID(ctx, tc.threadID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetMaxMessageRowID() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && id != tc.wantID {
				t.Errorf("GetMaxMessageRowID() = %d, want %d", id, tc.wantID)
			}
			if tc.wantURLSub != "" && !strings.Contains(capturedURL, tc.wantURLSub) {
				t.Errorf("captured URL missing %q: %s", tc.wantURLSub, capturedURL)
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
			wantPatterns: []string{"INSERT INTO extraction_watermarks (thread_id", "ON CONFLICT(thread_id) DO UPDATE"},
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

func setupBrainMockServer(status int, body string, capturedURL *string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if capturedURL != nil {
			*capturedURL = r.URL.String()
		}
		if status != 0 {
			w.WriteHeader(status)
		}
		if body != "" {
			_, _ = w.Write([]byte(body))
		}
	}))
}

type activeConversationsTestCase struct {
	name         string
	nilDB        bool
	activeHours  int
	queryErr     error
	watermarkSQL string
	eriStatus    int
	eriBody      string
	wantErr      bool
	wantThreads  []string
	wantPatterns []string
	wantURLSub   string
}

func runGetActiveConversationsCase(t *testing.T, tc activeConversationsTestCase, dbMem *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var captured string
	var capturedURL string
	var store *SQLStore

	if tc.nilDB {
		store = &SQLStore{}
	} else {
		server := setupBrainMockServer(tc.eriStatus, tc.eriBody, &capturedURL)
		defer server.Close()

		store = NewSQLStoreWithBrainURL(queryMockDBTX{
			dbMem:         dbMem,
			queryErr:      tc.queryErr,
			rowsSQL:       tc.watermarkSQL,
			capturedQuery: &captured,
		}, server.URL, server.Client())
	}

	threads, err := store.GetActiveConversationsForExtraction(ctx, tc.activeHours)
	if (err != nil) != tc.wantErr {
		t.Fatalf("GetActiveConversationsForExtraction() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if !tc.wantErr {
		assertThreadsMatch(t, threads, tc.wantThreads)
	}
	for _, pat := range tc.wantPatterns {
		if !strings.Contains(captured, pat) {
			t.Errorf("captured query missing %q: %s", pat, captured)
		}
	}
	if tc.wantURLSub != "" && !strings.Contains(capturedURL, tc.wantURLSub) {
		t.Errorf("captured URL missing %q: %s", tc.wantURLSub, capturedURL)
	}
}

func assertThreadsMatch(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len(threads) = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("threads[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

type unextractedAdminTurnsTestCase struct {
	name         string
	nilDB        bool
	threadID     string
	queryErr     error
	watermarkSQL string
	eriStatus    int
	eriBody      string
	wantErr      bool
	wantEmpty    bool
	wantCount    int
	wantRowIDs   []int64
	wantPatterns []string
	wantURLSub   string
}

func runGetUnextractedAdminTurnsCase(t *testing.T, tc unextractedAdminTurnsTestCase, dbMem *sql.DB) {
	t.Helper()
	ctx := context.Background()
	var captured string
	var capturedURL string
	var store *SQLStore

	if tc.nilDB {
		store = &SQLStore{}
	} else {
		server := setupBrainMockServer(tc.eriStatus, tc.eriBody, &capturedURL)
		defer server.Close()

		store = NewSQLStoreWithBrainURL(queryMockDBTX{
			dbMem:         dbMem,
			queryErr:      tc.queryErr,
			rowSQL:        tc.watermarkSQL,
			capturedQuery: &captured,
		}, server.URL, server.Client())
	}

	turns, err := store.GetUnextractedAdminTurns(ctx, tc.threadID)
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
	if tc.wantURLSub != "" && !strings.Contains(capturedURL, tc.wantURLSub) {
		t.Errorf("captured URL missing %q: %s", tc.wantURLSub, capturedURL)
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
