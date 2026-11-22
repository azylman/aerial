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
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/pgtest"
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

// heldAdvisoryLocks counts advisory locks currently held in the test's own database.
func heldAdvisoryLocks(t *testing.T, db store.DBTX) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&n); err != nil {
		t.Fatalf("count advisory locks: %v", err)
	}
	return n
}

// holdAdvisoryLock pins a second real connection holding the session
// advisory lock for key until the returned release runs.
func holdAdvisoryLock(t *testing.T, db *sql.DB, key string) func() {
	t.Helper()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("holder conn: %v", err)
	}
	execSQL(t, conn, `SELECT pg_advisory_lock(hashtext($1))`, key)
	return func() {
		// sql.Conn.Close returns the session to the pool, so unlock explicitly.
		execSQL(t, conn, `SELECT pg_advisory_unlock(hashtext($1))`, key)
		_ = conn.Close()
	}
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
		_, newTurns, _ := strings.Cut(prompt, "NEW UNEXTRACTED TURNS:\n")
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

// unpinnedStore hides SQLStore's Conn/DB methods: a Store that is not a
// connProvider, so extraction runs without a pinned connection or thread lock.
type unpinnedStore struct {
	store.Store
}

// pinningTestDBs returns two pools over one private database: db for the code
// under test and observer for seeding, lock holders and assertions. sabotage
// runs before db opens, so database-level settings reach every db session.
func pinningTestDBs(t *testing.T, sabotage string) (db, observer *sql.DB) {
	t.Helper()
	dsn := pgtest.NewDSN(t)
	open := func() *sql.DB {
		h, err := store.OpenDB(context.Background(), &config.Config{DatabaseURL: dsn})
		if err != nil {
			t.Fatalf("open test database: %v", err)
		}
		t.Cleanup(func() { _ = h.Close() })
		return h
	}
	observer = open()
	execSQL(t, observer, sabotage)
	return open(), observer
}

// pinningStore builds the store fixture named by handle over db and returns a
// release func that proves caller-owned handles are still usable.
func pinningStore(t *testing.T, db *sql.DB, handle string) (store.Store, func()) {
	t.Helper()
	ctx := context.Background()
	switch handle {
	case "closed":
		_ = db.Close()
	case "unpinned":
		return unpinnedStore{store.NewSQLStore(db)}, func() {}
	case "conn":
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("db.Conn() err = %v", err)
		}
		return store.NewSQLStore(conn), func() {
			execSQL(t, conn, `SELECT 1`)
			if err := conn.Close(); err != nil {
				t.Errorf("caller conn close err = %v, want nil (still owned by caller)", err)
			}
		}
	case "tx":
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("db.BeginTx() err = %v", err)
		}
		return store.NewSQLStore(tx), func() {
			if err := tx.Commit(); err != nil {
				t.Errorf("caller tx commit err = %v", err)
			}
		}
	}
	return store.NewSQLStore(db), func() {}
}

// sabotageThreadUnlock makes the thread lock's pg_advisory_unlock call fail
// for every session opened afterwards (public shadows pg_catalog).
const sabotageThreadUnlock = `CREATE FUNCTION public.pg_advisory_unlock(bigint) RETURNS boolean LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'sabotaged unlock'; END$$;
DO $$BEGIN EXECUTE format('ALTER DATABASE %I SET search_path = public, pg_catalog', current_database()); END$$;`

func TestExtractThreadFacts_ConnectionPinning(t *testing.T) {
	t.Parallel()
	const threadID = "th-pin-test"

	tests := []struct {
		name          string
		handle        string
		holdLock      bool
		sabotage      string
		errSubstr     string
		wantWatermark int64
		wantLocks     int
	}{
		{name: "PoolPinsAndReleasesConnection", handle: "pool", wantWatermark: 1},
		{name: "PrePinnedConnStaysOpenForCaller", handle: "conn", wantWatermark: 1},
		{name: "TxHandleLocksWithoutPinning", handle: "tx", wantWatermark: 1},
		{name: "NonConnProviderSkipsThreadLock", handle: "unpinned", wantWatermark: 1},
		{name: "ClosedPoolFailsFast", handle: "closed", errSubstr: "failed to pin connection for thread extraction"},
		// wantLocks 1 is the holder's own lock, still held during assertions.
		{name: "HeldThreadLockReleasesPinnedConn", handle: "pool", holdLock: true, errSubstr: "failed to acquire thread lock", wantLocks: 1},
		// KNOWN BUG: the failed-unlock cleanup "destroys" the pinned conn with
		// sql.Conn.Close, which returns the session to the pool still holding
		// the thread lock. Expect wantLocks 0 once the conn is really discarded.
		{name: "UnlockFailureStillSucceeds", handle: "pool", sabotage: sabotageThreadUnlock, wantWatermark: 1, wantLocks: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, observer := pinningTestDBs(t, tc.sabotage)
			seedTurns(t, observer, adminTurn(threadID, 1, "Turn 1", "Resp 1"))
			if tc.holdLock {
				// Released only after the assertions: the timed-out waiter's
				// backend may still be queued on the lock until it notices the
				// cancelled client.
				defer holdAdvisoryLock(t, observer, threadID)()
			}
			pinnedStore, release := pinningStore(t, db, tc.handle)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			err := ExtractThreadFacts(ctx, pinnedStore, &MockOllamaClient{embedding: testEmbedding(1.0)}, extractLLM(`{"facts": []}`), threadID)
			if tc.errSubstr == "" && err != nil {
				t.Fatalf("ExtractThreadFacts() err = %v, want nil", err)
			}
			if tc.errSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.errSubstr)) {
				t.Fatalf("ExtractThreadFacts() err = %v, want substring %q", err, tc.errSubstr)
			}
			release()
			if inUse := db.Stats().InUse; inUse != 0 {
				t.Errorf("pool connections in use = %d, want 0 (pinned conn released)", inUse)
			}
			if wm := readExtractWatermark(t, observer, config.DefaultStack, threadID); wm != tc.wantWatermark {
				t.Errorf("watermark = %d, want %d", wm, tc.wantWatermark)
			}
			if locks := heldAdvisoryLocks(t, observer); locks != tc.wantLocks {
				t.Errorf("held advisory locks = %d, want %d", locks, tc.wantLocks)
			}
		})
	}
}

func TestResolvePinnedStore_PrePinnedConnNotClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newTestDB(t)
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatalf("failed to open conn: %v", err)
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			t.Logf("close conn: %v", closeErr)
		}
	}()

	sqlStore := store.NewSQLStore(conn)

	updatedStore, lockDB, ownedConn, err := resolvePinnedStore(ctx, sqlStore)
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
	dbStore := store.NewSQLStore(db)
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
			err := processTurnBatch(context.Background(), store.NewSQLStore(db), &MockOllamaClient{embedding: testEmbedding(1.0)}, extractLLM(""), threadID, turns, 15, nil)
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

func TestReconcileCandidateFact_WithLockDB(t *testing.T) {
	t.Parallel()
	const factText = "User loves dark mode strictly"

	tests := []struct {
		name      string
		holdLock  bool
		sabotage  string
		wantErr   error
		errSubstr string
		wantFacts []extractFactRow
	}{
		{name: "CommitsInsertUnderXactLock", wantFacts: []extractFactRow{{1, factText, 1}}},
		{name: "BusyFactLockRollsBack", holdLock: true, wantErr: store.ErrAdvisoryLockBusy},
		{name: "FailedWriteRollsBack", sabotage: sabotageInsertFact, errSubstr: "sabotaged"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := newTestDB(t)
			execSQL(t, db, tc.sabotage)
			if tc.holdLock {
				defer holdAdvisoryLock(t, db, "coven:facts")()
			}
			conn, err := db.Conn(ctx)
			if err != nil {
				t.Fatalf("db.Conn() err = %v", err)
			}
			defer conn.Close()

			item := ExtractedFactItem{FactText: factText, Action: "ADD"}
			err = reconcileCandidateFact(ctx, store.NewSQLStore(conn), &MockOllamaClient{embedding: testEmbedding(1.0)}, nil, item, conn, "")
			switch {
			case tc.wantErr != nil && !errors.Is(err, tc.wantErr):
				t.Fatalf("reconcileCandidateFact() err = %v, want %v", err, tc.wantErr)
			case tc.errSubstr != "" && (err == nil || !strings.Contains(err.Error(), tc.errSubstr)):
				t.Fatalf("reconcileCandidateFact() err = %v, want substring %q", err, tc.errSubstr)
			case tc.wantErr == nil && tc.errSubstr == "" && err != nil:
				t.Fatalf("reconcileCandidateFact() err = %v, want nil", err)
			}
			// The pinned conn must be back outside any transaction: a SAVEPOINT
			// outside a transaction block is rejected.
			if _, spErr := conn.ExecContext(ctx, `SAVEPOINT probe`); spErr == nil {
				t.Errorf("pinned conn still inside a transaction after reconcile")
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
