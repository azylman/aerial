package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/services/coven/internal/config"
	"github.com/azylman/aerial/services/coven/internal/store"
)

func testHTTPBrainLLMClientCase(ctx context.Context, t *testing.T, tc brainLLMClientCase) {
	t.Helper()
	var srv *httptest.Server
	var gotTraceparent string
	if tc.statusCode != 0 {
		srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotTraceparent = r.Header.Get("Traceparent")
			if r.Header.Get("Content-Type") != "application/json" {
				http.Error(w, "bad content type", http.StatusBadRequest)
				return
			}
			var req brainLLMRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(tc.statusCode)
			_, _ = w.Write([]byte(tc.body))
		}))
		defer srv.Close()
	}

	targetURL := tc.rawURL
	if srv != nil {
		targetURL = srv.URL
	}
	callCtx := ctx
	const parentTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	if tc.parent {
		traceID, err := trace.TraceIDFromHex(parentTraceID)
		if err != nil {
			t.Fatalf("TraceIDFromHex: %v", err)
		}
		spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
		if err != nil {
			t.Fatalf("SpanIDFromHex: %v", err)
		}
		callCtx = trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
			TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
		}))
	}
	fn := HTTPBrainLLMClient(targetURL, nil)
	resp, err := fn(callCtx, tc.prompt)
	if tc.parent && !strings.HasPrefix(gotTraceparent, "00-"+parentTraceID+"-") {
		t.Errorf("traceparent = %q, want trace ID %s", gotTraceparent, parentTraceID)
	}
	if (err != nil) != tc.wantErr {
		t.Fatalf("HTTPBrainLLMClient() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErr && tc.errSubstr != "" {
		if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
			t.Errorf("err = %v, want substring %q", err, tc.errSubstr)
		}
	}
	if !tc.wantErr && resp != tc.wantResp {
		t.Errorf("resp = %q, want %q", resp, tc.wantResp)
	}
}

type brainLLMClientCase struct {
	name       string
	statusCode int
	body       string
	prompt     string
	rawURL     string
	parent     bool
	wantErr    bool
	errSubstr  string
	wantResp   string
}

func TestHTTPBrainLLMClient(t *testing.T) {
	ctx := context.Background()

	tests := []brainLLMClientCase{
		{
			name:       "TraceparentPropagatedFromParentSpan",
			statusCode: http.StatusOK,
			body:       `{"response":"ok"}`,
			prompt:     "hello",
			parent:     true,
			wantResp:   "ok",
		},
		{
			name:       "Success200",
			statusCode: http.StatusOK,
			body:       `{"response":"extracted fact output"}`,
			prompt:     "hello",
			wantResp:   "extracted fact output",
		},
		{
			name:       "Non200Status",
			statusCode: http.StatusBadRequest,
			body:       `error prompt too long`,
			prompt:     "hello",
			wantErr:    true,
		},
		{
			name:       "InvalidJSONResponse",
			statusCode: http.StatusOK,
			body:       `not-json`,
			prompt:     "hello",
			wantErr:    true,
		},
		{
			name:       "UnconfiguredURLError",
			statusCode: 0,
			rawURL:     "",
			prompt:     "hello",
			wantErr:    true,
			errSubstr:  "brain LLM URL is not configured",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			testHTTPBrainLLMClientCase(ctx, t, tc)
		})
	}
}

// extractLLM returns an LLM boundary fake that answers every prompt with resp.
func extractLLM(resp string) LLMClientFunc {
	return func(context.Context, string) (string, error) { return resp, nil }
}

// readExtractWatermark returns the stored last_extracted_rowid for
// (agent, thread), or 0 when no watermark row exists.
func readExtractWatermark(t *testing.T, db store.DBTX, agent, thread string) int64 {
	t.Helper()
	var wm int64
	err := db.QueryRowContext(context.Background(), `SELECT last_extracted_rowid FROM extraction_watermarks WHERE agent = $1 AND thread_id = $2`, agent, thread).Scan(&wm)
	if errors.Is(err, sql.ErrNoRows) {
		return 0
	}
	if err != nil {
		t.Fatalf("read watermark: %v", err)
	}
	return wm
}

// adminTurnRange builds completed admin turns first..last of thread.
func adminTurnRange(thread string, first, last int64) []testTurn {
	var turns []testTurn
	for i := first; i <= last; i++ {
		turns = append(turns, adminTurn(thread, i, fmt.Sprintf("User message turn %d", i), fmt.Sprintf("Assistant response turn %d", i)))
	}
	return turns
}

type extractBatchingTestCase struct {
	name           string
	totalTurns     int64
	timeoutOnCall  int
	timeoutAlways  bool
	wantBatchSizes []int
	wantWatermark  int64
	wantErr        bool
	errSubstr      string
}

func runBatchingAndHalvingCase(t *testing.T, tc extractBatchingTestCase) {
	t.Helper()
	ctx := context.Background()
	db := newTestDB(t)
	const threadID = "th-batch-test"
	seedTurns(t, db, adminTurnRange(threadID, 1, tc.totalTurns)...)

	var callIdx int
	var capturedBatchSizes []int
	llm := func(ctx context.Context, prompt string) (string, error) {
		callIdx++
		// Count only the new turns; prior turns are context, not batch members.
		_, newTurns, found := strings.Cut(prompt, "NEW UNEXTRACTED TURNS:\n")
		if !found {
			t.Errorf("prompt missing marker: %s", prompt)
		}
		capturedBatchSizes = append(capturedBatchSizes, strings.Count(newTurns, "User message turn"))

		if tc.timeoutAlways || (tc.timeoutOnCall > 0 && callIdx == tc.timeoutOnCall) {
			return "", context.DeadlineExceeded
		}
		return `{"facts": []}`, nil
	}

	client := &MockOllamaClient{embedding: testEmbedding(1.0)}
	err := ExtractThreadFacts(ctx, store.NewSQLStore(db), client, llm, threadID)
	if (err != nil) != tc.wantErr {
		t.Fatalf("ExtractThreadFacts() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErr && tc.errSubstr != "" {
		if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
			t.Errorf("err = %v, want substring %q", err, tc.errSubstr)
		}
	}
	if !slices.Equal(capturedBatchSizes, tc.wantBatchSizes) {
		t.Errorf("batch sizes = %v, want %v", capturedBatchSizes, tc.wantBatchSizes)
	}
	if wm := readExtractWatermark(t, db, config.DefaultStack, threadID); wm != tc.wantWatermark {
		t.Errorf("final watermark = %d, want %d", wm, tc.wantWatermark)
	}
}

func TestExtractThreadFacts_BatchingAndHalving(t *testing.T) {
	t.Parallel()

	tests := []extractBatchingTestCase{
		{
			name:           "120Turns_ThreeBatchesOldestFirst",
			totalTurns:     120,
			wantBatchSizes: []int{50, 50, 20},
			wantWatermark:  120,
			wantErr:        false,
		},
		{
			name:           "HalvingOnDeadlineExceeded_ThenSuccess",
			totalTurns:     50,
			timeoutOnCall:  1,
			wantBatchSizes: []int{50, 25, 25},
			wantWatermark:  50,
			wantErr:        false,
		},
		{
			name:           "HalvingDownToOneTurnBeforeFailing",
			totalTurns:     50,
			timeoutAlways:  true,
			wantBatchSizes: []int{50, 25, 12, 6, 3, 1},
			wantWatermark:  0,
			wantErr:        true,
			errSubstr:      "context deadline exceeded",
		},
		{
			name:           "SingleTurnFailsImmediatelyWithoutHalving",
			totalTurns:     1,
			timeoutAlways:  true,
			wantBatchSizes: []int{1},
			wantWatermark:  0,
			wantErr:        true,
			errSubstr:      "context deadline exceeded",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runBatchingAndHalvingCase(t, tc)
		})
	}
}

// Real Postgres never returns a turn with row_id 0 (the unextracted query
// filters row_id > watermark >= 0), so the zero-row-id fallback is exercised
// on the step function directly.
func TestResolveThreadTurnWatermark(t *testing.T) {
	t.Parallel()
	const threadID = "th-zero-rowid"

	tests := []struct {
		name      string
		turns     []store.Message
		seed      []testTurn
		sabotage  string
		want      int64
		errSubstr string
	}{
		{name: "MaxRowIDFromTurns", turns: []store.Message{{RowID: 3}, {RowID: 7}}, want: 7},
		{name: "ZeroRowIDFallsBackToMaxMessageRowID", turns: []store.Message{{RowID: 0}}, seed: []testTurn{{thread: threadID, rowID: 9, content: "c"}}, want: 9},
		{name: "ZeroRowIDNoMessagesIsZero", turns: []store.Message{{RowID: 0}}, want: 0},
		{name: "ZeroRowIDMaxQueryFails", turns: []store.Message{{RowID: 0}}, sabotage: sabotageMessages, errSubstr: "failed to get max message rowid for thread"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			seedTurns(t, db, tc.seed...)
			execSQL(t, db, tc.sabotage)

			got, err := resolveThreadTurnWatermark(context.Background(), store.NewSQLStore(db), threadID, tc.turns)
			if tc.errSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("err = %v, want substring %q", err, tc.errSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveThreadTurnWatermark() err = %v", err)
			}
			if got != tc.want {
				t.Errorf("watermark = %d, want %d", got, tc.want)
			}
		})
	}
}

// GetMaxMessageRowID cannot fail on its own after GetUnextractedAdminTurns
// succeeded, so its failure branches are exercised on the step functions.
func TestHandleEmptyUnextractedTurns(t *testing.T) {
	t.Parallel()
	const threadID = "th-empty"

	tests := []struct {
		name          string
		lastWatermark int64
		seed          []testTurn
		sabotage      string
		wantWatermark int64
		errSubstr     string
	}{
		{name: "ExistingWatermarkIsNoop", lastWatermark: 5, seed: []testTurn{{thread: threadID, rowID: 9, content: "c"}}},
		{name: "NoMessagesLeavesNoWatermark"},
		{name: "MaxQueryFails", sabotage: sabotageMessages, errSubstr: "failed to get max message rowid for thread"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			seedTurns(t, db, tc.seed...)
			execSQL(t, db, tc.sabotage)

			err := handleEmptyUnextractedTurns(context.Background(), store.NewSQLStore(db), threadID, tc.lastWatermark)
			if tc.errSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("err = %v, want substring %q", err, tc.errSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("handleEmptyUnextractedTurns() err = %v", err)
			}
			if wm := readExtractWatermark(t, db, config.DefaultStack, threadID); wm != tc.wantWatermark {
				t.Errorf("watermark = %d, want %d", wm, tc.wantWatermark)
			}
		})
	}
}

// Every turn the unextracted query returns has a non-empty response
// (IsExtractableAdminTurn), so an empty transcript only reaches
// prepareTurnBatchPrompt when called directly.
func TestPrepareTurnBatchPrompt_EmptyTranscript(t *testing.T) {
	t.Parallel()
	const threadID = "th-silent-turn"

	tests := []struct {
		name          string
		sabotage      string
		wantWatermark int64
		errSubstr     string
	}{
		{name: "AdvancesWatermarkWithoutLLM", wantWatermark: 15},
		{name: "WatermarkWriteFails", sabotage: sabotageWatermarkWrite, errSubstr: "failed to update conversation fact watermark for empty transcript"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			execSQL(t, db, tc.sabotage)
			turns := []store.Message{{RowID: 15, ThreadID: threadID, Status: "COMPLETED", IsAdmin: true}}

			prompt, skipped, err := prepareTurnBatchPrompt(context.Background(), store.NewSQLStore(db), threadID, turns, 15, config.DefaultStack)
			if tc.errSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("err = %v, want substring %q", err, tc.errSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("prepareTurnBatchPrompt() err = %v", err)
			}
			if !skipped || prompt != "" {
				t.Errorf("prepareTurnBatchPrompt() = (%q, %v), want skipped with no prompt", prompt, skipped)
			}
			if wm := readExtractWatermark(t, db, config.DefaultStack, threadID); wm != tc.wantWatermark {
				t.Errorf("watermark = %d, want %d", wm, tc.wantWatermark)
			}
		})
	}
}

// extractFactRow is one facts row as asserted by extraction tests.
type extractFactRow struct {
	id    int64
	text  string
	count int
}

// readExtractFacts lists every facts row ordered by id.
func readExtractFacts(t *testing.T, db store.DBTX) []extractFactRow {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT id, fact_text, reinforce_count FROM facts ORDER BY id`)
	if err != nil {
		t.Fatalf("read facts: %v", err)
	}
	defer rows.Close()
	var got []extractFactRow
	for rows.Next() {
		var r extractFactRow
		if err := rows.Scan(&r.id, &r.text, &r.count); err != nil {
			t.Fatalf("scan fact: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read facts: %v", err)
	}
	return got
}

func TestReconcileCandidateFact_Postgres(t *testing.T) {
	t.Parallel()
	const factText = "User loves dark mode strictly"

	tests := []struct {
		name      string
		sabotage  string
		wantErr   bool
		errSubstr string
		wantFacts []extractFactRow
	}{
		{name: "CommitsInsert", wantFacts: []extractFactRow{{1, factText, 1}}},
		{name: "FailedWriteRollsBack", sabotage: sabotageInsertFact, wantErr: true, errSubstr: "sabotaged"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := newTestDB(t)
			execSQL(t, db, tc.sabotage)

			item := ExtractedFactItem{FactText: factText, Action: "ADD"}
			err := reconcileCandidateFact(ctx, store.NewSQLStore(db), &MockOllamaClient{embedding: testEmbedding(1.0)}, nil, item, "")
			if tc.wantErr {
				if err == nil || (tc.errSubstr != "" && !strings.Contains(err.Error(), tc.errSubstr)) {
					t.Fatalf("reconcileCandidateFact() err = %v, want substring %q", err, tc.errSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("reconcileCandidateFact() err = %v, want nil", err)
			}
			// Read through the pool: only committed rows are visible.
			if got := readExtractFacts(t, db); !slices.Equal(got, tc.wantFacts) {
				t.Errorf("facts = %+v, want %+v", got, tc.wantFacts)
			}
		})
	}
}

func TestProcessThreadFacts_WatermarkErrorPaths(t *testing.T) {
	t.Parallel()
	const threadID = "th-wm-err"
	nonAdmin42 := testTurn{thread: threadID, rowID: 42, content: "hi", response: "hello"}
	silent15 := adminTurn(threadID, 15, "", "")
	admin1 := adminTurn(threadID, 1, "hi", "hello")
	const noFacts = `{"facts": []}`

	tests := []struct {
		name          string
		seed          []testTurn
		sabotage      string
		llmResp       string
		errSubstr     string
		wantWatermark int64
	}{
		{name: "WatermarkGetErrorFails", sabotage: sabotageWatermarks, errSubstr: "failed to get conversation fact watermark"},
		{name: "GetUnextractedAdminTurnsErrorFails", seed: []testTurn{admin1}, sabotage: sabotageTurnReads, errSubstr: "failed to get unextracted turns for thread"},
		// Non-admin and silent turns are never extractable: the watermark jumps
		// to the thread's max row id without an LLM call (empty reply would fail).
		{name: "EmptyTurnsAdvanceWatermark", seed: []testTurn{nonAdmin42}, wantWatermark: 42},
		{name: "SilentTurnAdvancesWatermark", seed: []testTurn{silent15}, wantWatermark: 15},
		{name: "EmptyTurnsUpdateWatermarkFails", seed: []testTurn{nonAdmin42}, sabotage: sabotageWatermarkWrite, errSubstr: "failed to update conversation fact watermark for empty turns"},
		{name: "ExtractedTurnAdvancesWatermark", seed: []testTurn{admin1}, llmResp: noFacts, wantWatermark: 1},
		{name: "TurnUpdateWatermarkFails", seed: []testTurn{admin1}, sabotage: sabotageWatermarkWrite, llmResp: noFacts, errSubstr: "failed to update conversation fact watermark"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			seedTurns(t, db, tc.seed...)
			execSQL(t, db, tc.sabotage)

			err := ExtractThreadFacts(context.Background(), store.NewSQLStore(db), &MockOllamaClient{embedding: testEmbedding(1.0)}, extractLLM(tc.llmResp), threadID)
			if tc.errSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("err = %v, want substring %q", err, tc.errSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ExtractThreadFacts() err = %v", err)
			}
			if wm := readExtractWatermark(t, db, config.DefaultStack, threadID); wm != tc.wantWatermark {
				t.Errorf("watermark = %d, want %d", wm, tc.wantWatermark)
			}
		})
	}
}

// deadlineFetchStore is the injected fetch-timeout boundary: a real SQLStore
// whose unextracted-turn fetch times out for batches larger than maxLimit
// (as a slow query past its deadline would). Every other call is real.
type deadlineFetchStore struct {
	*store.SQLStore

	maxLimit   int
	fetchSizes []int
}

func (s *deadlineFetchStore) GetUnextractedAdminTurns(ctx context.Context, threadID string, limit int, agent ...string) ([]store.Message, error) {
	s.fetchSizes = append(s.fetchSizes, limit)
	if limit > s.maxLimit {
		return nil, context.DeadlineExceeded
	}
	return s.SQLStore.GetUnextractedAdminTurns(ctx, threadID, limit, agent...)
}

func TestProcessThreadFacts_FetchDeadlineHalving(t *testing.T) {
	t.Parallel()
	const threadID = "th-fetch-halve"

	tests := []struct {
		name           string
		maxLimit       int
		wantFetchSizes []int
		wantWatermark  int64
		errSubstr      string
	}{
		{
			// 50 times out, 25 fetches all 10 turns, the reset 50 times out
			// again and the halved 25 finds nothing left.
			name:           "HalveOnceThenSucceed",
			maxLimit:       25,
			wantFetchSizes: []int{50, 25, 50, 25},
			wantWatermark:  10,
		},
		{
			name:           "HalveDownToOneThenFail",
			maxLimit:       0,
			wantFetchSizes: []int{50, 25, 12, 6, 3, 1},
			wantWatermark:  0,
			errSubstr:      "context deadline exceeded",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			seedTurns(t, db, adminTurnRange(threadID, 1, 10)...)
			fetchStore := &deadlineFetchStore{SQLStore: store.NewSQLStore(db), maxLimit: tc.maxLimit}

			err := ExtractThreadFacts(context.Background(), fetchStore, &MockOllamaClient{embedding: testEmbedding(1.0)}, extractLLM(`{"facts": []}`), threadID)
			if tc.errSubstr == "" && err != nil {
				t.Fatalf("ExtractThreadFacts() err = %v, want nil", err)
			}
			if tc.errSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.errSubstr)) {
				t.Fatalf("ExtractThreadFacts() err = %v, want substring %q", err, tc.errSubstr)
			}
			if !slices.Equal(fetchStore.fetchSizes, tc.wantFetchSizes) {
				t.Errorf("fetchSizes = %v, want %v", fetchStore.fetchSizes, tc.wantFetchSizes)
			}
			if wm := readExtractWatermark(t, db, config.DefaultStack, threadID); wm != tc.wantWatermark {
				t.Errorf("final watermark = %d, want %d", wm, tc.wantWatermark)
			}
		})
	}
}

func TestHTTPBrainLLMClientIdempotencyKeyHeader(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		key  string
		want string
	}{
		{"key sent", "coven:extract:aerial:t1:42", "coven:extract:aerial:t1:42"},
		{"blank key omits header", "  ", ""},
		{"no key omits header", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Get("Idempotency-Key")
				_, _ = w.Write([]byte(`{"response":"ok"}`))
			}))
			defer srv.Close()
			ctx := context.Background()
			if tc.key != "" {
				ctx = withLLMKey(ctx, tc.key)
			}
			if _, err := HTTPBrainLLMClient(srv.URL, nil)(ctx, "p"); err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("Idempotency-Key = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestHash16(t *testing.T) {
	t.Parallel()
	// sha256("abc") = ba7816bf8f01cfea...
	if got := hash16("abc"); got != "ba7816bf8f01cfea" {
		t.Errorf("hash16(abc) = %q", got)
	}
}

// unpinnedFactStore wraps Store but hides factConnProvider so withFactWriteTx runs fn directly.
type unpinnedFactStore struct {
	store.Store
}

func TestWithFactWriteTx(t *testing.T) {
	t.Parallel()
	const factText = "Alex keeps the espresso grinder on setting twelve"
	testErr := errors.New("write failed")

	tests := []struct {
		name         string
		storeKind    string
		canceledCtx  bool
		fnErr        error
		wantCalled   bool
		wantErr      bool
		wantFacts    int
		wantConnOpen bool
	}{
		{name: "unpinned store runs fn directly", storeKind: "unpinned", wantCalled: true, wantFacts: 1},
		{name: "unpinned store propagates fn error", storeKind: "unpinned", fnErr: testErr, wantCalled: true, wantErr: true, wantFacts: 1},
		{name: "uninitialized sql store returns conn error", storeKind: "nil-db", wantErr: true},
		{name: "sql store commits the transaction", storeKind: "sql", wantCalled: true, wantFacts: 1},
		{name: "pre-pinned sql store keeps connection open", storeKind: "pinned", wantCalled: true, wantFacts: 1, wantConnOpen: true},
		{name: "fn error rolls back the transaction", storeKind: "sql", fnErr: testErr, wantCalled: true, wantErr: true, wantFacts: 0},
		{name: "canceled context fails before fn", storeKind: "sql", canceledCtx: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			sqlStore := store.NewSQLStore(db)
			pinnedConn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = pinnedConn.Close() })
			stores := map[string]store.Store{
				"unpinned": unpinnedFactStore{Store: sqlStore},
				"nil-db":   &store.SQLStore{},
				"sql":      sqlStore,
				"pinned":   sqlStore.WithPinnedConn(pinnedConn),
			}

			ctx := context.Background()
			if tc.canceledCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			called := false
			err = withFactWriteTx(ctx, stores[tc.storeKind], func(ctx context.Context, txStore store.Store) error {
				called = true
				if _, insErr := txStore.InsertFact(ctx, factText, testEmbedding(1.0)); insErr != nil {
					return insErr
				}
				return tc.fnErr
			})
			if called != tc.wantCalled {
				t.Fatalf("called = %v, want %v (err = %v)", called, tc.wantCalled, err)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("withFactWriteTx() err = %v, wantErr = %v", err, tc.wantErr)
			}
			// Read through the pool: only committed rows are visible.
			if got := len(readExtractFacts(t, db)); got != tc.wantFacts {
				t.Errorf("committed facts = %d, want %d", got, tc.wantFacts)
			}
			if tc.wantConnOpen {
				if pingErr := pinnedConn.PingContext(context.Background()); pingErr != nil {
					t.Fatalf("expected pre-pinned connection to remain open, got: %v", pingErr)
				}
			}
		})
	}
}

// TestReconcileCandidateFact_ConcurrentWrites runs reconciliations in parallel with no global
// fact lock: tier-2 reinforcements of one row serialize on its row lock and none is lost, and
// unrelated inserts proceed independently.
func TestReconcileCandidateFact_ConcurrentWrites(t *testing.T) {
	t.Parallel()
	const seededText = "Alex prefers single-origin espresso in the morning"

	tests := []struct {
		name          string
		seed          bool
		workers       int
		distinct      bool
		wantFacts     int
		wantReinforce int
	}{
		{name: "ReinforcementsOfOneNeighborAllLand", seed: true, workers: 6, wantFacts: 1, wantReinforce: 7},
		{name: "UnrelatedInsertsAllLand", workers: 6, distinct: true, wantFacts: 6, wantReinforce: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			s := store.NewSQLStore(db)
			if tc.seed {
				seedFacts(t, s, []string{seededText}, [][]float32{testEmbeddingAt(0, 1)})
			}

			errCh := make(chan error, tc.workers)
			var wg sync.WaitGroup
			for i := range tc.workers {
				idx, text := 0, seededText
				if tc.distinct {
					idx, text = i, fmt.Sprintf("Alex owns household appliance number %d", i)
				}
				client := &MockOllamaClient{embedding: testEmbeddingAt(idx, 1)}
				item := ExtractedFactItem{Action: actionAdd, FactText: text}
				wg.Go(func() {
					errCh <- reconcileCandidateFact(context.Background(), s, client, nil, item, "aerial")
				})
			}
			wg.Wait()
			close(errCh)
			for err := range errCh {
				if err != nil {
					t.Fatalf("reconcileCandidateFact error: %v", err)
				}
			}

			got := readExtractFacts(t, db)
			if len(got) != tc.wantFacts {
				t.Fatalf("facts = %+v, want %d rows", got, tc.wantFacts)
			}
			if got[0].count != tc.wantReinforce {
				t.Errorf("first fact reinforce_count = %d, want %d", got[0].count, tc.wantReinforce)
			}
		})
	}
}

// vanishingStore deletes the target fact right before each fact mutation, standing in for a
// concurrent writer that removed the neighbor between the read and the write.
type vanishingStore struct {
	*store.SQLStore
}

func (v vanishingStore) ReinforceFact(ctx context.Context, id int64) error {
	if err := v.SQLStore.DeleteFact(ctx, id); err != nil {
		return err
	}
	return v.SQLStore.ReinforceFact(ctx, id)
}

func (v vanishingStore) UpdateFact(ctx context.Context, id int64, text string, emb []float32) error {
	if err := v.SQLStore.DeleteFact(ctx, id); err != nil {
		return err
	}
	return v.SQLStore.UpdateFact(ctx, id, text, emb)
}

func (v vanishingStore) DeleteFact(ctx context.Context, id int64) error {
	if err := v.SQLStore.DeleteFact(ctx, id); err != nil {
		return err
	}
	return v.SQLStore.DeleteFact(ctx, id)
}

func TestWriteCandidateReconcile_TargetVanished(t *testing.T) {
	t.Parallel()
	const (
		seededText = "Alex prefers oat milk in every latte"
		candText   = "Alex prefers whole milk in cappuccinos"
	)

	tests := []struct {
		name      string
		candDeg   float64
		decision  *ExtractedFactItem
		wantFacts []extractFactRow
	}{
		{name: "Tier2ReinforceInsertsCandidate", candDeg: 0, wantFacts: []extractFactRow{{2, candText, 1}}},
		{name: "Tier3UpdateInsertsCandidate", candDeg: 40, decision: &ExtractedFactItem{Action: actionUpdate, SupersedesID: 1, FactText: candText}, wantFacts: []extractFactRow{{2, candText, 1}}},
		{name: "Tier3NoopInsertsCandidate", candDeg: 40, decision: &ExtractedFactItem{Action: actionNoop, SupersedesID: 1}, wantFacts: []extractFactRow{{2, candText, 1}}},
		{name: "Tier3DeleteIsAlreadyDone", candDeg: 40, decision: &ExtractedFactItem{Action: actionDelete, DeleteID: 1}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newTestDB(t)
			s := store.NewSQLStore(db)
			seedFacts(t, s, []string{seededText}, [][]float32{angleEmbedding(0)})

			plan := reconcilePlan{Admit: true, Embedding: angleEmbedding(tc.candDeg), Decision: tc.decision}
			item := ExtractedFactItem{Action: actionAdd, FactText: candText}
			if err := writeCandidateReconcile(context.Background(), vanishingStore{SQLStore: s}, item, plan, "aerial"); err != nil {
				t.Fatalf("writeCandidateReconcile() err = %v", err)
			}
			if got := readExtractFacts(t, db); !slices.Equal(got, tc.wantFacts) {
				t.Errorf("facts = %+v, want %+v", got, tc.wantFacts)
			}
		})
	}
}

func TestExtractLLMKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		attempt int
		want    string
	}{
		{name: "FirstAttemptKeepsBareKey", attempt: 0, want: "coven:extract:aerial:th-1:42"},
		{name: "SecondAttemptAppendsOrdinal", attempt: 1, want: "coven:extract:aerial:th-1:42:a1"},
		{name: "ThirdAttemptAppendsOrdinal", attempt: 2, want: "coven:extract:aerial:th-1:42:a2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := extractLLMKey("aerial", "th-1", 42, tc.attempt); got != tc.want {
				t.Errorf("extractLLMKey() = %q, want %q", got, tc.want)
			}
		})
	}
}
