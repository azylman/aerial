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
// processTurnBatch when called directly.
func TestProcessTurnBatch_EmptyTranscript(t *testing.T) {
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

			// An empty LLM reply fails parsing, so any LLM call would surface as an error.
			err := processTurnBatch(context.Background(), store.NewSQLStore(db), &MockOllamaClient{embedding: testEmbedding(1.0)}, extractLLM(""), threadID, turns, 15)
			if tc.errSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("err = %v, want substring %q", err, tc.errSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("processTurnBatch() err = %v", err)
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
		{name: "CommitsInsertUnderFactWriteLock", wantFacts: []extractFactRow{{1, factText, 1}}},
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

// unpinnedFactStore wraps Store but hides factConnProvider so withFactWriteLock falls back to factWriteMutex.
type unpinnedFactStore struct {
	store.Store
}

func TestWithFactWriteLock(t *testing.T) {
	db := newTestDB(t)
	sqlStore := store.NewSQLStore(db)
	pinnedConn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pinnedConn.Close() })
	pinnedStore := sqlStore.WithPinnedConn(pinnedConn)

	unpinnedStore := unpinnedFactStore{Store: sqlStore}
	nilDBStore := &store.SQLStore{}
	testErr := errors.New("write failed")

	tests := []struct {
		name         string
		store        store.Store
		canceledCtx  bool
		fnErr        error
		wantCalled   bool
		wantErr      bool
		wantConnOpen bool
	}{
		{
			name:       "nil store executes without error",
			store:      nil,
			wantCalled: true,
			wantErr:    false,
		},
		{
			name:       "unpinned store propagates error via mutex",
			store:      unpinnedStore,
			fnErr:      testErr,
			wantCalled: true,
			wantErr:    true,
		},
		{
			name:       "unpinned store falls back to mutex without error",
			store:      unpinnedStore,
			wantCalled: true,
			wantErr:    false,
		},
		{
			name:       "uninitialized sql store returns conn error",
			store:      nilDBStore,
			wantCalled: false,
			wantErr:    true,
		},
		{
			name:       "sql store executes with pinned connection lock",
			store:      sqlStore,
			wantCalled: true,
			wantErr:    false,
		},
		{
			name:         "pre-pinned sql store keeps connection open after lock",
			store:        pinnedStore,
			wantCalled:   true,
			wantErr:      false,
			wantConnOpen: true,
		},
		{
			name:       "sql store rolls back and propagates callback error",
			store:      sqlStore,
			fnErr:      testErr,
			wantCalled: true,
			wantErr:    true,
		},
		{
			name:        "canceled context fails to acquire lock",
			store:       sqlStore,
			canceledCtx: true,
			wantCalled:  false,
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.canceledCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			called := false
			err := withFactWriteLock(ctx, tc.store, func(ctx context.Context, lockedStore store.Store) error {
				called = true
				if tc.store != nil && lockedStore == nil {
					t.Error("expected non-nil lockedStore when store is non-nil")
				}
				return tc.fnErr
			})
			if called != tc.wantCalled {
				t.Fatalf("called = %v, want %v (err = %v)", called, tc.wantCalled, err)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("withFactWriteLock() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantConnOpen {
				if pingErr := pinnedConn.PingContext(context.Background()); pingErr != nil {
					t.Fatalf("expected pre-pinned connection to remain open, got: %v", pingErr)
				}
			}
		})
	}
}

func TestReconcileCandidateFact_ConcurrentExtraction(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	s := store.NewSQLStore(db)

	item := ExtractedFactItem{
		Action:   "ADD",
		FactText: "Alex prefers single-origin espresso in the morning",
	}

	const concurrency = 2
	errCh := make(chan error, concurrency)
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for range concurrency {
		client := &MockOllamaClient{embedding: testEmbedding(1.0)}
		go func() {
			defer wg.Done()
			errCh <- reconcileCandidateFact(context.Background(), s, client, nil, item, "aerial")
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("reconcileCandidateFact error: %v", err)
		}
	}

	res, err := s.GetFactsPaginated(context.Background(), store.FactsFilter{})
	if err != nil {
		t.Fatalf("GetFactsPaginated error: %v", err)
	}
	if len(res.Facts) != 1 {
		t.Fatalf("expected 1 fact stored after concurrent extraction, got %d", len(res.Facts))
	}
	if res.Facts[0].ReinforceCount != 2 {
		t.Fatalf("expected fact to be reinforced once (reinforce_count=2), got %d", res.Facts[0].ReinforceCount)
	}
}

func TestDecisionMatchesNeighbors(t *testing.T) {
	t.Parallel()

	neighbors := []store.ScoredFact{
		{Fact: store.Fact{ID: 10, FactText: "fact 10"}},
		{Fact: store.Fact{ID: 20, FactText: "fact 20"}},
	}

	tests := []struct {
		name      string
		dec       ExtractedFactItem
		neighbors []store.ScoredFact
		want      bool
	}{
		{
			name:      "ADD always matches",
			dec:       ExtractedFactItem{Action: "ADD"},
			neighbors: neighbors,
			want:      true,
		},
		{
			name:      "DELETE matching delete_id",
			dec:       ExtractedFactItem{Action: "DELETE", DeleteID: 10},
			neighbors: neighbors,
			want:      true,
		},
		{
			name:      "DELETE matching supersedes_id fallback",
			dec:       ExtractedFactItem{Action: "DELETE", SupersedesID: 20},
			neighbors: neighbors,
			want:      true,
		},
		{
			name:      "DELETE missing target",
			dec:       ExtractedFactItem{Action: "DELETE", DeleteID: 99},
			neighbors: neighbors,
			want:      false,
		},
		{
			name:      "DELETE non-positive target",
			dec:       ExtractedFactItem{Action: "DELETE", DeleteID: 0},
			neighbors: neighbors,
			want:      false,
		},
		{
			name:      "UPDATE matching supersedes_id",
			dec:       ExtractedFactItem{Action: "UPDATE", SupersedesID: 20},
			neighbors: neighbors,
			want:      true,
		},
		{
			name:      "UPDATE matching delete_id fallback",
			dec:       ExtractedFactItem{Action: "UPDATE", DeleteID: 10},
			neighbors: neighbors,
			want:      true,
		},
		{
			name:      "UPDATE missing target",
			dec:       ExtractedFactItem{Action: "UPDATE", SupersedesID: 99},
			neighbors: neighbors,
			want:      false,
		},
		{
			name:      "UPDATE non-positive target",
			dec:       ExtractedFactItem{Action: "UPDATE", SupersedesID: 0},
			neighbors: neighbors,
			want:      false,
		},
		{
			name:      "NOOP matching supersedes_id",
			dec:       ExtractedFactItem{Action: "NOOP", SupersedesID: 10},
			neighbors: neighbors,
			want:      true,
		},
		{
			name:      "NOOP missing target",
			dec:       ExtractedFactItem{Action: "NOOP", SupersedesID: 99},
			neighbors: neighbors,
			want:      false,
		},
		{
			name:      "NOOP non-positive target with non-empty neighbors",
			dec:       ExtractedFactItem{Action: "NOOP", SupersedesID: 0},
			neighbors: neighbors,
			want:      true,
		},
		{
			name:      "NOOP non-positive target with empty neighbors",
			dec:       ExtractedFactItem{Action: "NOOP", SupersedesID: 0},
			neighbors: nil,
			want:      false,
		},
		{
			name:      "unknown action returns false",
			dec:       ExtractedFactItem{Action: "UNKNOWN"},
			neighbors: neighbors,
			want:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := decisionMatchesNeighbors(tc.dec, tc.neighbors)
			if got != tc.want {
				t.Fatalf("decisionMatchesNeighbors(%+v) = %v, want %v", tc.dec, got, tc.want)
			}
		})
	}
}
