package main

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
	"testing"

	"go.opentelemetry.io/otel/trace"
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
			_ = json.NewDecoder(r.Body).Decode(&req)
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
		traceID, _ := trace.TraceIDFromHex(parentTraceID)
		spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
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

type extractBatchingTestCase struct {
	name           string
	totalTurns     int
	timeoutOnCall  int
	timeoutAlways  bool
	wantCalls      int
	wantBatchSizes []int
	wantWatermark  int64
	wantErr        bool
	errSubstr      string
}

func runBatchingAndHalvingCase(t *testing.T, tc extractBatchingTestCase) {
	t.Helper()
	ctx := context.Background()
	mem := newTestMemStore()
	const threadID = "th-batch-test"

	turns := make([]Message, tc.totalTurns)
	for i := range tc.totalTurns {
		turns[i] = Message{
			RowID:        int64(i + 1),
			ThreadID:     threadID,
			Content:      fmt.Sprintf("User message turn %d", i+1),
			ResponseText: fmt.Sprintf("Assistant response turn %d", i+1),
			Status:       "COMPLETED",
			IsAdmin:      true,
		}
	}
	mem.unextractedTurns[threadID] = turns

	var callIdx int
	var capturedBatchSizes []int
	llm := func(ctx context.Context, prompt string) (string, error) {
		callIdx++
		count := strings.Count(prompt, "User message turn")
		capturedBatchSizes = append(capturedBatchSizes, count)

		if tc.timeoutAlways || (tc.timeoutOnCall > 0 && callIdx == tc.timeoutOnCall) {
			return "", context.DeadlineExceeded
		}
		return `{"facts": []}`, nil
	}

	client := &MockOllamaClient{embedding: testEmbedding(1.0)}
	err := ExtractThreadFacts(ctx, mem, client, llm, threadID)
	if (err != nil) != tc.wantErr {
		t.Fatalf("ExtractThreadFacts() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErr && tc.errSubstr != "" {
		if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
			t.Errorf("err = %v, want substring %q", err, tc.errSubstr)
		}
	}
	if callIdx != tc.wantCalls {
		t.Errorf("LLM call count = %d, want %d", callIdx, tc.wantCalls)
	}
	if !slices.Equal(capturedBatchSizes, tc.wantBatchSizes) {
		t.Errorf("batch sizes = %v, want %v", capturedBatchSizes, tc.wantBatchSizes)
	}
	if wm := mem.watermarks[threadID]; wm != tc.wantWatermark {
		t.Errorf("final watermark = %d, want %d", wm, tc.wantWatermark)
	}
}

func TestExtractThreadFacts_BatchingAndHalving(t *testing.T) {
	t.Parallel()

	tests := []extractBatchingTestCase{
		{
			name:           "120Turns_ThreeBatchesOldestFirst",
			totalTurns:     120,
			wantCalls:      3,
			wantBatchSizes: []int{50, 50, 20},
			wantWatermark:  120,
			wantErr:        false,
		},
		{
			name:           "HalvingOnDeadlineExceeded_ThenSuccess",
			totalTurns:     50,
			timeoutOnCall:  1,
			wantCalls:      3,
			wantBatchSizes: []int{50, 25, 25},
			wantWatermark:  50,
			wantErr:        false,
		},
		{
			name:           "HalvingDownToOneTurnBeforeFailing",
			totalTurns:     50,
			timeoutAlways:  true,
			wantCalls:      6,
			wantBatchSizes: []int{50, 25, 12, 6, 3, 1},
			wantWatermark:  0,
			wantErr:        true,
			errSubstr:      "context deadline exceeded",
		},
		{
			name:           "SingleTurnFailsImmediatelyWithoutHalving",
			totalTurns:     1,
			timeoutAlways:  true,
			wantCalls:      1,
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

type mockConnStore struct {
	*TestMemStore

	connErr   error
	connCalls int
	conn      *sql.Conn
}

func (m *mockConnStore) Conn(ctx context.Context) (*sql.Conn, error) {
	m.connCalls++
	if m.connErr != nil {
		return nil, m.connErr
	}
	return m.conn, nil
}

func (*mockConnStore) DB() DBTX {
	return nil
}

func TestExtractThreadFacts_ConnectionPinning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbMem := setupTestDBMem(t)

	tests := []struct {
		name       string
		injectErr  error
		wantErr    bool
		errSubstr  string
		wantClosed bool
	}{
		{
			name:      "PoolExhaustedFailsFast",
			injectErr: errors.New("connection pool exhausted"),
			wantErr:   true,
			errSubstr: "failed to pin connection for thread extraction",
		},
		{
			name:       "PinnedConnectionReleasedOnFailure",
			wantErr:    true,
			errSubstr:  "failed to acquire thread lock",
			wantClosed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mem := newTestMemStore()
			var pinnedConn *sql.Conn
			if tc.wantClosed {
				c, err := dbMem.Conn(ctx)
				if err != nil {
					t.Fatalf("dbMem.Conn() err = %v", err)
				}
				pinnedConn = c
			}

			mockStore := &mockConnStore{
				TestMemStore: mem,
				connErr:      tc.injectErr,
				conn:         pinnedConn,
			}
			client := &MockOllamaClient{embedding: testEmbedding(1.0)}
			llm := func(ctx context.Context, prompt string) (string, error) {
				return `{"facts": []}`, nil
			}

			err := ExtractThreadFacts(ctx, mockStore, client, llm, "th-pin-test")
			if (err != nil) != tc.wantErr {
				t.Fatalf("ExtractThreadFacts() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr && tc.errSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
					t.Errorf("err = %v, want substring %q", err, tc.errSubstr)
				}
			}
			if tc.wantClosed && pinnedConn != nil {
				if pingErr := pinnedConn.PingContext(ctx); !errors.Is(pingErr, sql.ErrConnDone) {
					t.Errorf("pinnedConn.PingContext() = %v, want sql.ErrConnDone (connection closed)", pingErr)
				}
			}
		})
	}
}

func TestResolvePinnedStore_PrePinnedConnNotClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbMem := setupTestDBMem(t)
	conn, err := dbMem.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open conn: %v", err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			t.Logf("close conn: %v", closeErr)
		}
	}()

	store := NewSQLStore(conn)

	updatedStore, lockDB, ownedConn, err := resolvePinnedStore(ctx, store)
	if err != nil {
		t.Fatalf("resolvePinnedStore err = %v", err)
	}
	if lockDB != conn {
		t.Errorf("lockDB = %v, want %v", lockDB, conn)
	}
	if ownedConn != nil {
		t.Errorf("ownedConn = %v, want nil for pre-pinned connection", ownedConn)
	}
	if updatedStore == nil {
		t.Errorf("expected updatedStore non-nil")
	}

	// For a store backed by *sql.DB, ownedConn should be populated
	dbStore := NewSQLStore(dbMem)
	_, _, ownedFromDB, err := resolvePinnedStore(ctx, dbStore)
	if err != nil {
		t.Fatalf("resolvePinnedStore(dbStore) err = %v", err)
	}
	if ownedFromDB == nil {
		t.Errorf("expected ownedFromDB to be non-nil for *sql.DB store")
	} else {
		if closeErr := ownedFromDB.Close(); closeErr != nil {
			t.Logf("close ownedFromDB: %v", closeErr)
		}
	}
}

func TestProcessThreadFacts_ZeroRowIDNoProgressBreaks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := newTestMemStore()
	const threadID = "th-zero-rowid"
	mem.unextractedTurns[threadID] = []Message{
		{RowID: 0, Content: "zero turn", ResponseText: "resp", Status: "COMPLETED", IsAdmin: true},
	}
	mem.maxRowIDs[threadID] = 0

	var llmCalls int
	llm := func(ctx context.Context, prompt string) (string, error) {
		llmCalls++
		return `{"facts": []}`, nil
	}
	client := &MockOllamaClient{embedding: testEmbedding(1.0)}

	err := ExtractThreadFacts(ctx, mem, client, llm, threadID)
	if err != nil {
		t.Fatalf("ExtractThreadFacts() err = %v", err)
	}
	if llmCalls != 0 {
		t.Errorf("llmCalls = %d, want 0 when maxRowID <= 0", llmCalls)
	}
}

func TestProcessThreadFacts_EmptyTranscriptAdvancesWatermark(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	mem := newTestMemStore()
	const threadID = "th-silent-turn"
	mem.unextractedTurns[threadID] = []Message{
		{RowID: 15, ThreadID: threadID, Content: "", ResponseText: "", Status: "COMPLETED", IsAdmin: true},
	}
	mem.maxRowIDs[threadID] = 15

	var llmCalls int
	llm := func(ctx context.Context, prompt string) (string, error) {
		llmCalls++
		return `{"facts": []}`, nil
	}
	client := &MockOllamaClient{embedding: testEmbedding(1.0)}

	err := ExtractThreadFacts(ctx, mem, client, llm, threadID)
	if err != nil {
		t.Fatalf("ExtractThreadFacts() err = %v", err)
	}
	if llmCalls != 0 {
		t.Errorf("llmCalls = %d, want 0 when turns contain only empty prompt and response", llmCalls)
	}
	if wm := mem.watermarks[threadID]; wm != 15 {
		t.Errorf("watermark = %d, want 15", wm)
	}
}

func TestReconcileCandidateFact_WithLockDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fake := newLockFakeDB(t)
	mem := newTestMemStore()
	client := &MockOllamaClient{embedding: testEmbedding(1.0)}

	item := ExtractedFactItem{
		FactText: "User loves dark mode strictly",
		Action:   "ADD",
	}

	err := reconcileCandidateFact(ctx, mem, client, nil, item, fake, "")
	if err != nil {
		t.Fatalf("reconcileCandidateFact() with lockDB err = %v", err)
	}

	queries := fake.recorded()
	if !lockQueriesContain(queries, "BEGIN") {
		t.Errorf("missing BEGIN in queries: %v", queries)
	}
	if !lockQueriesContain(queries, "COMMIT") {
		t.Errorf("missing COMMIT in queries: %v", queries)
	}
	if len(mem.facts) != 1 {
		t.Errorf("expected 1 fact inserted, got %d", len(mem.facts))
	}
}

type mockLockDBStore struct {
	*TestMemStore

	lockDB DBTX
}

func (*mockLockDBStore) Conn(_ context.Context) (*sql.Conn, error) {
	return nil, nil
}

func (m *mockLockDBStore) DB() DBTX {
	return m.lockDB
}

func TestProcessThreadFacts_LockErrorPaths(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name      string
		failOn    string
		wantErr   bool
		errSubstr string
	}{
		{
			name:      "AdvisoryLockFailure",
			failOn:    "LOCK",
			wantErr:   true,
			errSubstr: "failed to acquire thread lock",
		},
		{
			name:    "UnlockFailureClosesConnection",
			failOn:  "UNLOCK",
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fake := newLockFakeDB(t)
			fake.failOn = tc.failOn
			fake.failErr = errors.New("injected lock failure")

			mem := newTestMemStore()
			mem.unextractedTurns["th-lock-test"] = []Message{
				{RowID: 1, ThreadID: "th-lock-test", Content: "Turn 1", ResponseText: "Resp 1", Status: "COMPLETED", IsAdmin: true},
			}

			store := &mockLockDBStore{
				TestMemStore: mem,
				lockDB:       fake,
			}

			client := &MockOllamaClient{embedding: testEmbedding(1.0)}
			llm := func(ctx context.Context, prompt string) (string, error) {
				return `{"facts": []}`, nil
			}

			err := ExtractThreadFacts(ctx, store, client, llm, "th-lock-test")
			if (err != nil) != tc.wantErr {
				t.Fatalf("ExtractThreadFacts() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr && tc.errSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
					t.Errorf("err = %v, want substring %q", err, tc.errSubstr)
				}
			}
		})
	}
}

func TestProcessThreadFacts_WatermarkErrorPaths(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := &MockOllamaClient{embedding: testEmbedding(1.0)}
	llm := func(ctx context.Context, prompt string) (string, error) {
		return `{"facts": []}`, nil
	}

	tests := []struct {
		name            string
		failOp          string
		hasTurns        bool
		zeroRowID       bool
		emptyTranscript bool
		wantErr         bool
		errSubstr       string
	}{
		{
			name:      "WatermarkGetErrorFails",
			failOp:    "GetConversationFactWatermark",
			wantErr:   true,
			errSubstr: "failed to get conversation fact watermark",
		},
		{
			name:      "GetUnextractedAdminTurnsErrorFails",
			failOp:    "GetUnextractedAdminTurns",
			hasTurns:  true,
			wantErr:   true,
			errSubstr: "failed to get unextracted turns for thread",
		},
		{
			name:      "EmptyTurnsGetMaxRowIDFails",
			failOp:    "GetMaxMessageRowID",
			hasTurns:  false,
			wantErr:   true,
			errSubstr: "failed to get max message rowid",
		},
		{
			name:      "EmptyTurnsUpdateWatermarkFails",
			failOp:    "UpdateConversationFactWatermark",
			hasTurns:  false,
			wantErr:   true,
			errSubstr: "failed to update conversation fact watermark",
		},
		{
			name:      "TurnRowIDZeroFallbackGetMaxRowIDFails",
			failOp:    "GetMaxMessageRowID",
			hasTurns:  true,
			zeroRowID: true,
			wantErr:   true,
			errSubstr: "failed to get max message rowid for thread",
		},
		{
			name:      "TurnUpdateWatermarkFails",
			failOp:    "UpdateConversationFactWatermark",
			hasTurns:  true,
			wantErr:   true,
			errSubstr: "failed to update conversation fact watermark",
		},
		{
			name:            "EmptyTranscriptUpdateWatermarkFails",
			failOp:          "UpdateConversationFactWatermark",
			hasTurns:        true,
			emptyTranscript: true,
			wantErr:         true,
			errSubstr:       "failed to update conversation fact watermark for empty transcript",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mem := newTestMemStore()
			const threadID = "th-wm-err"
			if tc.hasTurns {
				rowID := int64(1)
				if tc.zeroRowID {
					rowID = 0
				}
				content := "hi"
				resp := "hello"
				if tc.emptyTranscript {
					content = ""
					resp = ""
				}
				mem.unextractedTurns[threadID] = []Message{
					{RowID: rowID, Content: content, ResponseText: resp, Status: "COMPLETED", IsAdmin: true},
				}
			} else {
				mem.maxRowIDs[threadID] = 42
			}
			mem.SetFail(tc.failOp, errors.New("simulated error"))

			err := ExtractThreadFacts(ctx, mem, client, llm, threadID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ExtractThreadFacts() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr && tc.errSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
					t.Errorf("err = %v, want substring %q", err, tc.errSubstr)
				}
			}
		})
	}
}

type mockFetchHalvingStore struct {
	*TestMemStore

	fetchSizes []int
	failUntil  int
}

func (m *mockFetchHalvingStore) GetUnextractedAdminTurns(ctx context.Context, threadID string, limit int, agent ...string) ([]Message, error) {
	m.fetchSizes = append(m.fetchSizes, limit)
	if len(m.fetchSizes) <= m.failUntil {
		return nil, context.DeadlineExceeded
	}
	return m.TestMemStore.GetUnextractedAdminTurns(ctx, threadID, limit, agent...)
}

func TestProcessThreadFacts_FetchDeadlineHalving(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name           string
		failUntil      int
		wantFetchSizes []int
		wantWatermark  int64
		wantErr        bool
		errSubstr      string
	}{
		{
			name:           "HalveOnceThenSucceed",
			failUntil:      1,
			wantFetchSizes: []int{50, 25, 50},
			wantWatermark:  10,
			wantErr:        false,
		},
		{
			name:           "HalveDownToOneThenFail",
			failUntil:      10,
			wantFetchSizes: []int{50, 25, 12, 6, 3, 1},
			wantWatermark:  0,
			wantErr:        true,
			errSubstr:      "context deadline exceeded",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mem := newTestMemStore()
			const threadID = "th-fetch-halve"
			turns := make([]Message, 10)
			for i := range turns {
				turns[i] = Message{
					RowID:        int64(i + 1),
					ThreadID:     threadID,
					Content:      fmt.Sprintf("Turn %d", i+1),
					ResponseText: fmt.Sprintf("Resp %d", i+1),
					Status:       "COMPLETED",
					IsAdmin:      true,
				}
			}
			mem.unextractedTurns[threadID] = turns

			store := &mockFetchHalvingStore{
				TestMemStore: mem,
				failUntil:    tc.failUntil,
			}
			client := &MockOllamaClient{embedding: testEmbedding(1.0)}
			llm := func(ctx context.Context, prompt string) (string, error) {
				return `{"facts": []}`, nil
			}

			err := ExtractThreadFacts(ctx, store, client, llm, threadID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ExtractThreadFacts() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr && tc.errSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
					t.Errorf("err = %v, want substring %q", err, tc.errSubstr)
				}
			}
			if !slices.Equal(store.fetchSizes, tc.wantFetchSizes) {
				t.Errorf("fetchSizes = %v, want %v", store.fetchSizes, tc.wantFetchSizes)
			}
			if wm := mem.watermarks[threadID]; wm != tc.wantWatermark {
				t.Errorf("final watermark = %d, want %d", wm, tc.wantWatermark)
			}
		})
	}
}
