package store

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/pkg/pgutil"
	"github.com/azylman/aerial/pkg/transcriptwire"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Main(m))
}

// newTestStore opens a private clone of the production schema through the
// production constructor (pgutil.OpenSQL, as RunApp does).
func newTestStore(t *testing.T, stack string) *Store {
	t.Helper()
	db, err := pgutil.OpenSQL(context.Background(), pgtest.NewDSN(t), pgutil.SQLOptions{})
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewStore(db, stack)
}

func testBatch(session string, mtime time.Time, first, last int, final, reset bool) transcriptwire.TranscriptStepBatch {
	steps := make([]transcriptwire.TranscriptStep, 0, last-first+1)
	for i := first; i <= last; i++ {
		steps = append(steps, transcriptwire.TranscriptStep{StepIndex: i, Type: "model_response", Content: "content"})
	}
	return transcriptwire.TranscriptStepBatch{
		Stack: "aerial", SessionID: session, ThreadID: "thread-1", Engine: "agy",
		FileMtime: mtime, BaseStep: first, FirstStep: first, LastStep: last,
		Final: final, Reset: reset, Steps: steps,
	}
}

func TestDecideBatch(t *testing.T) {
	t.Parallel()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		cur   sessionCursor
		batch transcriptwire.TranscriptStepBatch
		want  BatchDecision
	}{
		{"no row inserts", sessionCursor{}, testBatch("s", epoch, 0, 4, false, false), DecisionInsert},
		{"older epoch is stale", sessionCursor{found: true, lastMtime: epoch, lastStep: 4}, testBatch("s", epoch.Add(-time.Hour), 5, 6, false, false), DecisionStale},
		{"redelivered reset at epoch re-publishes", sessionCursor{found: true, lastMtime: epoch, lastStep: 4}, testBatch("s", epoch, 0, 2, false, true), DecisionDuplicateFinal},
		{"reset on newer epoch deletes then inserts", sessionCursor{found: true, lastMtime: epoch, lastStep: 4}, testBatch("s", epoch.Add(time.Hour), 0, 2, false, true), DecisionReset},
		{"equal epoch reset past cursor deletes then inserts", sessionCursor{found: true, lastMtime: epoch, lastStep: 1}, testBatch("s", epoch, 0, 4, false, true), DecisionReset},
		{"reset on older epoch is stale", sessionCursor{found: true, lastMtime: epoch, lastStep: 4}, testBatch("s", epoch.Add(-time.Hour), 0, 2, false, true), DecisionStale},
		{"indexed final re-publishes", sessionCursor{found: true, lastMtime: epoch, lastStep: 4, indexedFinal: true}, testBatch("s", epoch, 0, 4, true, false), DecisionDuplicateFinal},
		{"final past cursor appends", sessionCursor{found: true, lastMtime: epoch, lastStep: 4, indexedFinal: true}, testBatch("s", epoch, 5, 6, true, false), DecisionAppend},
		{"non-final at cursor appends", sessionCursor{found: true, lastMtime: epoch, lastStep: 4}, testBatch("s", epoch, 0, 4, false, false), DecisionAppend},
		{"equal epoch appends", sessionCursor{found: true, lastMtime: epoch, lastStep: 4}, testBatch("s", epoch, 5, 9, false, false), DecisionAppend},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := decideBatch(tc.cur, tc.batch); got != tc.want {
				t.Errorf("decideBatch() = %q, want %q", got, tc.want)
			}
		})
	}
}

type applyBatchCase struct {
	name            string
	seed            []transcriptwire.TranscriptStepBatch
	batch           transcriptwire.TranscriptStepBatch
	wantDecision    BatchDecision
	wantSteps       int
	wantLastStep    int
	wantStepContent map[int]string
}

func finalSeedBatch(session string, mtime time.Time) transcriptwire.TranscriptStepBatch {
	b := testBatch(session, mtime, 0, 4, false, false)
	b.Final = true
	return b
}

func rewrittenResetBatch(session string, mtime time.Time) transcriptwire.TranscriptStepBatch {
	b := testBatch(session, mtime, 0, 2, false, true)
	for i := range b.Steps {
		b.Steps[i].Content = "rewritten"
	}
	return b
}

func runApplyBatchCase(t *testing.T, tc applyBatchCase) {
	t.Helper()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	for _, s := range tc.seed {
		if _, err := store.ApplyBatch(ctx, s); err != nil {
			t.Fatalf("seed ApplyBatch: %v", err)
		}
	}
	outcome, err := store.ApplyBatch(ctx, tc.batch)
	if err != nil {
		t.Fatalf("ApplyBatch() err = %v", err)
	}
	if outcome.Decision != tc.wantDecision {
		t.Errorf("Decision = %q, want %q", outcome.Decision, tc.wantDecision)
	}
	if outcome.StepsIndexed != tc.wantSteps {
		t.Errorf("StepsIndexed = %d, want %d", outcome.StepsIndexed, tc.wantSteps)
	}
	if (tc.wantDecision == DecisionStale) != outcome.Stale {
		t.Errorf("Stale = %v, want stale=%v", outcome.Stale, tc.wantDecision == DecisionStale)
	}
	marks, err := store.GetWatermarks(ctx, "aerial")
	if err != nil {
		t.Fatalf("GetWatermarks() err = %v", err)
	}
	if len(marks) != 1 || marks[0].LastIndexedStep != tc.wantLastStep {
		t.Fatalf("watermarks = %+v, want last step %d", marks, tc.wantLastStep)
	}
	assertStepContents(ctx, t, store, tc.wantStepContent)
}

func assertStepContents(ctx context.Context, t *testing.T, store *Store, want map[int]string) {
	t.Helper()
	if want == nil {
		return
	}
	steps, err := store.GetSteps(ctx, "aerial", "sess-1", 0, 10)
	if err != nil {
		t.Fatalf("GetSteps() err = %v", err)
	}
	got := map[int]string{}
	for _, st := range steps {
		got[st.StepIndex] = st.Content
	}
	for idx, want := range want {
		if got[idx] != want {
			t.Errorf("step %d content = %q, want %q", idx, got[idx], want)
		}
	}
	if len(steps) != len(want) {
		t.Errorf("reset left %d steps, want %d", len(steps), len(want))
	}
}

func TestApplyBatch(t *testing.T) {
	t.Parallel()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	plainSeed := testBatch("sess-1", epoch, 0, 4, false, false)
	finalSeed := finalSeedBatch("sess-1", epoch)
	resetSeed := testBatch("sess-1", epoch, 0, 1, false, false)
	resetBatch := rewrittenResetBatch("sess-1", epoch)
	tests := []applyBatchCase{
		{"first batch inserts", nil, testBatch("sess-1", epoch, 0, 4, false, false), DecisionInsert, 5, 4, nil},
		{"next batch appends", []transcriptwire.TranscriptStepBatch{plainSeed}, testBatch("sess-1", epoch.Add(time.Hour), 5, 6, true, false), DecisionAppend, 2, 6, nil},
		{"older epoch batch is stale", []transcriptwire.TranscriptStepBatch{plainSeed}, testBatch("sess-1", epoch.Add(-time.Hour), 5, 6, false, false), DecisionStale, 0, 4, nil},
		{"reset rewrites steps", []transcriptwire.TranscriptStepBatch{resetSeed}, resetBatch, DecisionReset, 3, 2, map[int]string{0: "rewritten", 1: "rewritten", 2: "rewritten"}},
		{"duplicate final re-publishes without steps", []transcriptwire.TranscriptStepBatch{finalSeed}, finalSeed, DecisionDuplicateFinal, 0, 4, nil},
		{"redelivered reset after append re-publishes", []transcriptwire.TranscriptStepBatch{resetBatch}, resetBatch, DecisionDuplicateFinal, 0, 2, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runApplyBatchCase(t, tc)
		})
	}
}

func TestApplyBatchValidation(t *testing.T) {
	t.Parallel()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	badAgentBatch := testBatch("s", epoch, 0, 1, false, false)
	badAgentBatch.Agent = "invalid_slug!"
	tests := []struct {
		name      string
		storeKind string
		batch     transcriptwire.TranscriptStepBatch
		wantErr   string
	}{
		{"nil database", "nildb", testBatch("s", epoch, 0, 1, false, false), "database is nil"},
		{"empty stack is read-only", "nostack", testBatch("s", epoch, 0, 1, false, false), "cannot be empty for stack-scoped writes"},
		{"empty session id", "ok", testBatch("", epoch, 0, 1, false, false), "session id cannot be empty"},
		{"invalid agent slug", "ok", badAgentBatch, "invalid transcript batch agent slug"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var store *Store
			switch tc.storeKind {
			case "nildb":
				store = NewStore(nil, "aerial")
			case "nostack":
				store = newTestStore(t, "")
			default:
				store = newTestStore(t, "aerial")
			}
			if _, err := store.ApplyBatch(context.Background(), tc.batch); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("ApplyBatch() err = %v, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func TestApplyBatchConcurrentDuplicateFirstBatch(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	batch := testBatch("sess-dup", epoch, 0, 3, false, false)

	first, err := store.ApplyBatch(ctx, batch)
	if err != nil {
		t.Fatalf("first ApplyBatch() err = %v", err)
	}
	second, err := store.ApplyBatch(ctx, batch)
	if err != nil {
		t.Fatalf("redelivered ApplyBatch() err = %v", err)
	}
	if first.Decision != DecisionInsert || second.Decision != DecisionAppend {
		t.Fatalf("decisions = %q, %q; want insert then append", first.Decision, second.Decision)
	}
	if second.StepsIndexed != 0 {
		t.Fatalf("redelivery indexed %d steps, want 0", second.StepsIndexed)
	}
	steps, err := store.GetSteps(ctx, "aerial", "sess-dup", 0, 10)
	if err != nil || len(steps) != 4 {
		t.Fatalf("steps = %d (%v), want exactly 4", len(steps), err)
	}
}

func TestApplyBatchRacingFirstBatches(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	batch := testBatch("sess-race", epoch, 0, 3, false, false)

	// Two deliveries of the same first batch race on separate connections:
	// the summary insert and step inserts collide on primary keys, so
	// exactly four steps land and are counted once in total.
	var wg sync.WaitGroup
	outcomes := make([]BatchOutcome, 2)
	errs := make([]error, 2)
	for i := range outcomes {
		wg.Go(func() { outcomes[i], errs[i] = store.ApplyBatch(ctx, batch) })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("delivery %d ApplyBatch() err = %v", i, err)
		}
	}
	if total := outcomes[0].StepsIndexed + outcomes[1].StepsIndexed; total != 4 {
		t.Errorf("deliveries indexed %d + %d steps, want 4 in total", outcomes[0].StepsIndexed, outcomes[1].StepsIndexed)
	}
	steps, err := store.GetSteps(ctx, "aerial", "sess-race", 0, 10)
	if err != nil || len(steps) != 4 {
		t.Fatalf("steps = %d (%v), want exactly 4", len(steps), err)
	}
}

func TestApplyBatchRedeliveredResetPreservesAppends(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	reset := testBatch("sess-replay", epoch, 0, 2, false, true)
	appended := testBatch("sess-replay", epoch, 3, 4, false, false)

	if _, err := store.ApplyBatch(ctx, reset); err != nil {
		t.Fatalf("reset ApplyBatch() err = %v", err)
	}
	if _, err := store.ApplyBatch(ctx, appended); err != nil {
		t.Fatalf("append ApplyBatch() err = %v", err)
	}
	replay, err := store.ApplyBatch(ctx, reset)
	if err != nil {
		t.Fatalf("replay ApplyBatch() err = %v", err)
	}
	if replay.Decision != DecisionDuplicateFinal || replay.StepsIndexed != 0 {
		t.Fatalf("replay = %+v, want duplicate re-publish of 0 steps", replay)
	}
	steps, err := store.GetSteps(ctx, "aerial", "sess-replay", 0, 10)
	if err != nil || len(steps) != 5 {
		t.Fatalf("steps = %d (%v), want all 5 preserved", len(steps), err)
	}
	marks, err := store.GetWatermarks(ctx, "aerial")
	if err != nil || len(marks) != 1 || marks[0].LastIndexedStep != 4 {
		t.Fatalf("watermarks = %+v (%v), want cursor at step 4", marks, err)
	}
}

func TestAcquireThreadSummaryLock(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		threadID string
		wantErr  bool
	}{
		{"valid thread locks", "thread-1", false},
		{"empty thread rejected", "  ", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t, "aerial")
			release, err := store.AcquireThreadSummaryLock(context.Background(), tc.threadID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("AcquireThreadSummaryLock() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			release()
			release()
		})
	}
}

func TestAcquireThreadSummaryLockNilStore(t *testing.T) {
	t.Parallel()
	var store *Store
	if _, err := store.AcquireThreadSummaryLock(context.Background(), "thread-1"); err == nil {
		t.Errorf("expected error from nil store")
	}
}

// advisoryLocks counts advisory locks currently granted in the test database.
func advisoryLocks(t *testing.T, store *Store) int {
	t.Helper()
	var n int
	if err := store.db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&n); err != nil {
		t.Fatalf("count advisory locks: %v", err)
	}
	return n
}

func TestAcquireThreadSummaryLockExclusion(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	release, err := store.AcquireThreadSummaryLock(ctx, "thread-1")
	if err != nil {
		t.Fatalf("AcquireThreadSummaryLock() err = %v", err)
	}
	if n := advisoryLocks(t, store); n != 1 {
		t.Fatalf("advisory locks held = %d, want 1", n)
	}
	// A second summarizer for the same thread blocks until its deadline.
	waitCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if _, waitErr := store.AcquireThreadSummaryLock(waitCtx, "thread-1"); waitErr == nil || !strings.Contains(waitErr.Error(), "acquire summary lock") {
		t.Fatalf("contended AcquireThreadSummaryLock() err = %v, want acquire summary lock failure", waitErr)
	}
	other, err := store.AcquireThreadSummaryLock(ctx, "thread-2")
	if err != nil {
		t.Fatalf("other thread AcquireThreadSummaryLock() err = %v", err)
	}
	other()
	release()
	if n := advisoryLocks(t, store); n != 0 {
		t.Fatalf("advisory locks held after release = %d, want 0", n)
	}
	again, err := store.AcquireThreadSummaryLock(ctx, "thread-1")
	if err != nil {
		t.Fatalf("re-acquire after release err = %v", err)
	}
	again()
}

func TestAcquireThreadSummaryLockTerminatedSession(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	release, err := store.AcquireThreadSummaryLock(ctx, "thread-term")
	if err != nil {
		t.Fatalf("AcquireThreadSummaryLock() err = %v", err)
	}
	// Kill the lock holder's backend so the release's unlock fails and the
	// connection is destroyed instead of returned to the pool.
	if _, killErr := store.db.ExecContext(ctx,
		`SELECT pg_terminate_backend(pid, 5000) FROM pg_locks WHERE locktype = 'advisory' AND pid <> pg_backend_pid() AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`); killErr != nil {
		t.Fatalf("terminate lock holder: %v", killErr)
	}
	release()
	release()
	reacquireCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	again, err := store.AcquireThreadSummaryLock(reacquireCtx, "thread-term")
	if err != nil {
		t.Fatalf("re-acquire after terminated holder err = %v", err)
	}
	again()
	if n := advisoryLocks(t, store); n != 0 {
		t.Fatalf("advisory locks held = %d, want 0", n)
	}
}

func TestSummaryLockKey(t *testing.T) {
	t.Parallel()
	if got := summaryLockKey("  thread-1 "); got != "transcripts-summary:thread-1" {
		t.Errorf("summaryLockKey = %q", got)
	}
}

func TestExtractPgxConn(t *testing.T) {
	t.Parallel()
	fakePgx := &pgx.Conn{}
	wrapper := &fakeConnWrapper{conn: fakePgx}
	tests := []struct {
		name    string
		input   any
		wantNil bool
	}{
		{"nil input", nil, true},
		{"unrelated type", "not-a-conn", true},
		{"direct pgx conn", fakePgx, false},
		{"wrapper conn", wrapper, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractPgxConn(tc.input)
			if tc.wantNil && got != nil {
				t.Fatalf("extractPgxConn() = %v, want nil", got)
			}
			if !tc.wantNil && got != fakePgx {
				t.Fatalf("extractPgxConn() = %v, want %v", got, fakePgx)
			}
		})
	}
}

type fakeConnWrapper struct {
	conn *pgx.Conn
}

func (w *fakeConnWrapper) Conn() *pgx.Conn {
	return w.conn
}

func runDestroyConnCase(ctx context.Context, t *testing.T, store *Store, name string, holdLock, closeBefore bool) {
	t.Helper()
	conn, err := store.db.Conn(ctx)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	lockKey := "test-destroy-conn-" + name
	if holdLock {
		if _, lockErr := conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext($1))`, lockKey); lockErr != nil {
			t.Fatalf("acquire lock: %v", lockErr)
		}
	}
	if closeBefore {
		if closeErr := conn.Close(); closeErr != nil {
			t.Fatalf("close: %v", closeErr)
		}
	}
	destroyConn(conn)
	if closeErr := conn.Close(); closeErr == nil {
		t.Errorf("expected error closing destroyed conn")
	}
	if !holdLock {
		return
	}
	probeConn, err := store.db.Conn(ctx)
	if err != nil {
		t.Fatalf("probe conn: %v", err)
	}
	defer probeConn.Close()

	deadline := time.Now().Add(5 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		var acquired bool
		if queryErr := probeConn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, lockKey).Scan(&acquired); queryErr == nil && acquired {
			if _, unlockErr := probeConn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext($1))`, lockKey); unlockErr != nil {
				t.Fatalf("unlock advisory lock: %v", unlockErr)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for advisory lock release after destroyConn")
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}

func TestDestroyConn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		holdLock    bool
		closeBefore bool
	}{
		{name: "already closed", holdLock: false, closeBefore: true},
		{name: "live without advisory lock", holdLock: false, closeBefore: false},
		{name: "live holding advisory lock", holdLock: true, closeBefore: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t, "aerial")
			runDestroyConnCase(context.Background(), t, store, tc.name, tc.holdLock, tc.closeBefore)
		})
	}
}

func TestApplyBatchToolCallID(t *testing.T) {
	t.Parallel()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	batch := testBatch("sess-tools", epoch, 0, 0, false, false)
	batch.Steps[0].ToolCalls = []transcriptwire.TranscriptToolCall{{Name: "bash", ID: "call-9"}}
	batch.Steps[0].ToolName = "bash"
	store := newTestStore(t, "aerial")
	if _, err := store.ApplyBatch(context.Background(), batch); err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	steps, err := store.GetSteps(context.Background(), "aerial", "sess-tools", 0, 0)
	if err != nil {
		t.Fatalf("GetSteps: %v", err)
	}
	if len(steps) != 1 {
		t.Fatalf("steps = %+v, want 1 step", steps)
	}
	var toolCallID string
	if err := store.db.QueryRowContext(context.Background(), `SELECT tool_call_id FROM transcript_steps WHERE agent = 'aerial' AND session_id = 'sess-tools' AND step_index = 0`).Scan(&toolCallID); err != nil {
		t.Fatalf("query tool_call_id: %v", err)
	}
	if toolCallID != "call-9" {
		t.Fatalf("tool_call_id = %q, want call-9", toolCallID)
	}
}

func TestParseStepCreatedAt(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	tests := []struct {
		name    string
		raw     string
		want    time.Time
		useNear bool
	}{
		{"empty string", "", now, true},
		{"rfc3339nano", "2026-11-18T12:00:00.123456789Z", time.Date(2026, 11, 18, 12, 0, 0, 123456789, time.UTC), false},
		{"rfc3339", "2026-11-18T12:00:00Z", time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC), false},
		{"garbage string", "not-a-timestamp", now, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := parseStepCreatedAt(tc.raw)
			if tc.useNear {
				if got.Before(now.Add(-time.Minute)) || got.After(time.Now().UTC().Add(time.Minute)) {
					t.Errorf("parseStepCreatedAt(%q) = %v, want near now", tc.raw, got)
				}
			} else if !got.Equal(tc.want) {
				t.Errorf("parseStepCreatedAt(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

// abortFn is a trigger function each sabotage row installs in its own
// cloned database to make one production statement fail.
const abortFn = `CREATE FUNCTION abort_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'sabotaged write'; END $$;`

func TestApplyBatchExecFailures(t *testing.T) {
	t.Parallel()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	plainSeed := testBatch("sess-x", epoch, 0, 1, false, false)
	newInsert := testBatch("sess-new", epoch, 0, 1, false, false)
	appendBatch := testBatch("sess-x", epoch.Add(time.Hour), 2, 3, false, false)
	resetBatch := rewrittenResetBatch("sess-x", epoch)
	tests := []struct {
		name        string
		seed        bool
		batch       transcriptwire.TranscriptStepBatch
		sabotageSQL string
		wantSub     string
	}{
		{"lock row fails", false, newInsert, `DROP TABLE session_summaries CASCADE`, "lock session summary row"},
		{"reset delete fails", true, resetBatch, abortFn + `CREATE TRIGGER abort_delete BEFORE DELETE ON transcript_steps FOR EACH ROW WHEN (OLD.session_id = 'sess-x') EXECUTE FUNCTION abort_write();`, "reset transcript steps"},
		{"reset upsert fails", true, resetBatch, abortFn + `CREATE TRIGGER abort_summary_insert BEFORE INSERT ON session_summaries FOR EACH ROW WHEN (NEW.session_id = 'sess-x') EXECUTE FUNCTION abort_write();`, "upsert reset session summary"},
		{"insert summary fails", false, newInsert, abortFn + `CREATE TRIGGER abort_summary_insert BEFORE INSERT ON session_summaries FOR EACH ROW WHEN (NEW.session_id = 'sess-new') EXECUTE FUNCTION abort_write();`, "insert session summary"},
		{"insert path step fails", false, newInsert, abortFn + `CREATE TRIGGER abort_step_insert BEFORE INSERT ON transcript_steps FOR EACH ROW WHEN (NEW.session_id = 'sess-new') EXECUTE FUNCTION abort_write();`, "insert transcript step"},
		{"append path step fails", true, appendBatch, abortFn + `CREATE TRIGGER abort_step_insert BEFORE INSERT ON transcript_steps FOR EACH ROW WHEN (NEW.session_id = 'sess-x') EXECUTE FUNCTION abort_write();`, "insert transcript step"},
		{"insert advance fails", false, newInsert, abortFn + `CREATE TRIGGER abort_summary_update BEFORE UPDATE ON session_summaries FOR EACH ROW WHEN (NEW.session_id = 'sess-new') EXECUTE FUNCTION abort_write();`, "advance session summary"},
		{"append advance fails", true, appendBatch, abortFn + `CREATE TRIGGER abort_summary_update BEFORE UPDATE ON session_summaries FOR EACH ROW WHEN (NEW.session_id = 'sess-x') EXECUTE FUNCTION abort_write();`, "advance session summary"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t, "aerial")
			ctx := context.Background()
			if tc.seed {
				if _, err := store.ApplyBatch(ctx, plainSeed); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}
			if _, err := store.db.ExecContext(ctx, tc.sabotageSQL); err != nil {
				t.Fatalf("sabotage: %v", err)
			}
			if _, err := store.ApplyBatch(ctx, tc.batch); err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("ApplyBatch() err = %v, want substring %q", err, tc.wantSub)
			}
		})
	}
}

func TestApplyBatchEmptyStackDefaults(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	batch := testBatch("sess-d", epoch, 0, 1, false, false)
	batch.Stack = "  "
	outcome, err := store.ApplyBatch(context.Background(), batch)
	if err != nil {
		t.Fatalf("ApplyBatch() err = %v", err)
	}
	if outcome.Decision != DecisionInsert || outcome.StepsIndexed != 2 {
		t.Errorf("outcome = %+v, want insert of 2 steps", outcome)
	}
}

func TestDestroyConnClosed(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	conn, err := store.db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	destroyConn(conn)
}

func TestApplyBatchEmptySteps(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	batch := testBatch("sess-empty", epoch, 0, 0, false, false)
	batch.Steps = nil
	batch.FirstStep, batch.LastStep = 0, 0
	outcome, err := store.ApplyBatch(context.Background(), batch)
	if err != nil {
		t.Fatalf("ApplyBatch() err = %v", err)
	}
	if outcome.Decision != DecisionInsert || outcome.StepsIndexed != 0 {
		t.Errorf("outcome = %+v, want empty insert", outcome)
	}
}

func TestApplyBatchAppendEqualEpoch(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	if _, err := store.ApplyBatch(ctx, testBatch("sess-eq", epoch, 0, 2, false, false)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	outcome, err := store.ApplyBatch(ctx, testBatch("sess-eq", epoch, 3, 4, false, false))
	if err != nil {
		t.Fatalf("ApplyBatch() err = %v", err)
	}
	if outcome.Decision != DecisionAppend || outcome.StepsIndexed != 2 {
		t.Errorf("outcome = %+v, want append of 2", outcome)
	}
}

func TestApplyBatchMultiAgent(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

	batchAlpha := testBatch("sess-alpha", epoch, 0, 2, false, false)
	batchAlpha.Agent = "agent-alpha"
	batchAlpha.Stack = "agent-alpha"

	outcome, err := store.ApplyBatch(ctx, batchAlpha)
	if err != nil {
		t.Fatalf("ApplyBatch(agent-alpha) err = %v", err)
	}
	if outcome.Decision != DecisionInsert || outcome.StepsIndexed != 3 {
		t.Fatalf("outcome = %+v, want insert of 3", outcome)
	}

	steps, err := store.GetSteps(ctx, "agent-alpha", "sess-alpha", 0, 5)
	if err != nil {
		t.Fatalf("GetSteps(agent-alpha) err = %v", err)
	}
	if len(steps) != 3 {
		t.Fatalf("got %d steps, want 3", len(steps))
	}
}

func TestStoreSummaryOperations(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	epoch := now.Add(-time.Hour)

	batch := testBatch("sess-due", epoch, 0, 5, true, false)
	if _, err := store.ApplyBatch(ctx, batch); err != nil {
		t.Fatalf("seed batch: %v", err)
	}

	candidates, err := store.ListDueSessions(ctx, 10*time.Minute, 1, 10, now)
	if err != nil {
		t.Fatalf("ListDueSessions() err = %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("ListDueSessions() returned %d candidates, want 1", len(candidates))
	}
	cand := candidates[0]
	if cand.SessionID != "sess-due" || cand.LastIndexed != 5 {
		t.Errorf("unexpected candidate: %+v", cand)
	}

	if saveErr := store.SaveSummary(ctx, "aerial", "sess-due", "persisted summary", cand.LastIndexed); saveErr != nil {
		t.Fatalf("SaveSummary() err = %v", saveErr)
	}

	after, afterErr := store.ListDueSessions(ctx, 10*time.Minute, 1, 10, now)
	if afterErr != nil {
		t.Fatalf("ListDueSessions() after save err = %v", afterErr)
	}
	if len(after) != 0 {
		t.Errorf("ListDueSessions() after save returned %d candidates, want 0", len(after))
	}
}

func TestStoreSummaryValidationErrors(t *testing.T) {
	t.Parallel()
	st := newTestStore(t, "aerial")
	emptyStackStore := NewStore(st.db, "")
	if _, err := emptyStackStore.ListDueSessions(context.Background(), time.Hour, 1, 10, time.Now()); err == nil {
		t.Errorf("expected ListDueSessions error on empty stack")
	}
	if err := emptyStackStore.SaveSummary(context.Background(), "aerial", "s", "sum", 1); err == nil {
		t.Errorf("expected SaveSummary error on empty stack")
	}
}

func TestStoreClosedDBErrors(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	_ = store.db.Close()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	batch := testBatch("sess-closed", epoch, 0, 1, false, false)

	if _, err := store.ApplyBatch(context.Background(), batch); err == nil {
		t.Errorf("expected ApplyBatch error on closed db")
	}
	if _, err := store.AcquireThreadSummaryLock(context.Background(), "thread-closed"); err == nil {
		t.Errorf("expected AcquireThreadSummaryLock error on closed db")
	}
	if _, err := store.ListDueSessions(context.Background(), time.Hour, 1, 10, time.Now()); err == nil {
		t.Errorf("expected ListDueSessions error on closed db")
	}
	if err := store.SaveSummary(context.Background(), "aerial", "s", "sum", 1); err == nil {
		t.Errorf("expected SaveSummary error on closed db")
	}
}
