package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"

	"github.com/azylman/aerial/pkg/transcriptwire"
)

const transcriptsTestSchema = `CREATE TABLE session_summaries (
    agent TEXT NOT NULL,
    session_id TEXT NOT NULL,
    engine TEXT NOT NULL DEFAULT '',
    thread_id TEXT NOT NULL DEFAULT '',
    summary TEXT NOT NULL DEFAULT '',
    last_indexed_step INT NOT NULL DEFAULT -1,
    last_mtime DATETIME,
    summary_step_watermark INT NOT NULL DEFAULT -1,
    is_settled BOOLEAN NOT NULL DEFAULT 0,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (agent, session_id)
);
CREATE TABLE transcript_steps (
    agent TEXT NOT NULL,
    session_id TEXT NOT NULL,
    step_index INT NOT NULL,
    step_type TEXT NOT NULL DEFAULT '',
    tool_name TEXT NOT NULL DEFAULT '',
    tool_kind TEXT NOT NULL DEFAULT '',
    tool_call_id TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (agent, session_id, step_index)
);`

var testStoreSeq atomic.Int64

func newTestStore(t *testing.T, stack string) *Store {
	t.Helper()
	// A unique shared-cache in-memory database per fixture: same schema and
	// queries as a file DB without the per-fixture fsync cost.
	dsn := fmt.Sprintf("file:transcripts-%d?mode=memory&cache=shared", testStoreSeq.Add(1))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite fixture: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(transcriptsTestSchema); err != nil {
		t.Fatalf("create fixture schema: %v", err)
	}
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

func TestParseDBTime(t *testing.T) {
	t.Parallel()
	fixed := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		value any
		want  bool
	}{
		{"time value", fixed, true},
		{"zero time", time.Time{}, false},
		{"rfc3339 string", "2026-11-18T12:00:00Z", true},
		{"sqlite datetime string", "2026-11-18 12:00:00", true},
		{"sqlite datetime nanos", "2026-11-18 12:00:00.123456789+00", true},
		{"byte string", []byte("2026-11-18T12:00:00Z"), true},
		{"garbage string", "not-a-time", false},
		{"nil", nil, false},
		{"other type", 42, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, ok := ParseDBTime(tc.value); ok != tc.want {
				t.Errorf("ParseDBTime(%v) ok = %v, want %v", tc.value, ok, tc.want)
			}
		})
	}
}

func TestSummaryLockKey(t *testing.T) {
	t.Parallel()
	if got := summaryLockKey("  thread-1 "); got != "transcripts-summary:thread-1" {
		t.Errorf("summaryLockKey = %q", got)
	}
}

func TestDestroyConn(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	conn, err := store.db.Conn(context.Background())
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	destroyConn(conn)
	if err := conn.Close(); err == nil {
		t.Errorf("expected error closing destroyed conn")
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

func TestApplyBatchExecFailures(t *testing.T) {
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	plainSeed := testBatch("sess-x", epoch, 0, 1, false, false)
	newInsert := testBatch("sess-new", epoch, 0, 1, false, false)
	appendBatch := testBatch("sess-x", epoch.Add(time.Hour), 2, 3, false, false)
	resetBatch := rewrittenResetBatch("sess-x", epoch)
	tests := []struct {
		name    string
		seed    bool
		batch   transcriptwire.TranscriptStepBatch
		trigger string
		wantSub string
	}{
		{
			"reset delete fails", true, resetBatch,
			`CREATE TRIGGER abort_delete BEFORE DELETE ON transcript_steps WHEN OLD.session_id = 'sess-x' BEGIN SELECT RAISE(ABORT, 'no delete'); END;`,
			"reset transcript steps",
		},
		{
			"reset upsert fails", true, resetBatch,
			`CREATE TRIGGER abort_summary_insert BEFORE INSERT ON session_summaries WHEN NEW.session_id = 'sess-x' BEGIN SELECT RAISE(ABORT, 'no insert'); END;`,
			"upsert reset session summary",
		},
		{
			"insert summary fails", false, newInsert,
			`CREATE TRIGGER abort_summary_insert BEFORE INSERT ON session_summaries WHEN NEW.session_id = 'sess-new' BEGIN SELECT RAISE(ABORT, 'no insert'); END;`,
			"insert session summary",
		},
		{
			"insert step fails", true, appendBatch,
			`CREATE TRIGGER abort_step_insert BEFORE INSERT ON transcript_steps WHEN NEW.session_id = 'sess-x' BEGIN SELECT RAISE(ABORT, 'no step'); END;`,
			"insert transcript step",
		},
		{
			"insert advance fails", false, newInsert,
			`CREATE TRIGGER abort_summary_update BEFORE UPDATE ON session_summaries WHEN NEW.session_id = 'sess-new' BEGIN SELECT RAISE(ABORT, 'no update'); END;`,
			"advance session summary",
		},
		{
			"append advance fails", true, appendBatch,
			`CREATE TRIGGER abort_summary_update BEFORE UPDATE ON session_summaries WHEN NEW.session_id = 'sess-x' BEGIN SELECT RAISE(ABORT, 'no update'); END;`,
			"advance session summary",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t, "aerial")
			ctx := context.Background()
			if tc.seed {
				if _, err := store.ApplyBatch(ctx, plainSeed); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}
			if _, err := store.db.ExecContext(ctx, tc.trigger); err != nil {
				t.Fatalf("create trigger: %v", err)
			}
			if _, err := store.ApplyBatch(ctx, tc.batch); err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("ApplyBatch() err = %v, want substring %q", err, tc.wantSub)
			}
		})
	}
}

func TestApplyBatchLockScanError(t *testing.T) {
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	if _, err := store.db.ExecContext(ctx, `DROP TABLE session_summaries`); err != nil {
		t.Fatalf("drop: %v", err)
	}
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	if _, err := store.ApplyBatch(ctx, testBatch("s", epoch, 0, 0, false, false)); err == nil {
		t.Errorf("expected lock row error on missing table")
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

func TestApplyBatchInsertStepFailure(t *testing.T) {
	store := newTestStore(t, "aerial")
	ctx := context.Background()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	if _, err := store.db.ExecContext(ctx, `CREATE TRIGGER abort_step_insert BEFORE INSERT ON transcript_steps WHEN NEW.session_id = 'sess-fresh' BEGIN SELECT RAISE(ABORT, 'no step'); END;`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}
	if _, err := store.ApplyBatch(ctx, testBatch("sess-fresh", epoch, 0, 1, false, false)); err == nil {
		t.Errorf("expected step insert error on insert path")
	}
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

func TestStoreAccessors(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	if store.DB() == nil {
		t.Errorf("DB() is nil")
	}
	if store.Stack() != "aerial" {
		t.Errorf("Stack() = %q, want aerial", store.Stack())
	}
	if err := store.CheckDB(); err != nil {
		t.Errorf("CheckDB() err = %v", err)
	}
	if err := store.CheckWriter(); err != nil {
		t.Errorf("CheckWriter() err = %v", err)
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
}

type mockLockConnector struct {
	d       driver.Driver
	connErr error
	conn    *mockLockConn
}

func (m mockLockConnector) Connect(context.Context) (driver.Conn, error) {
	if m.connErr != nil {
		return nil, m.connErr
	}
	if m.conn != nil {
		return m.conn, nil
	}
	return &mockLockConn{}, nil
}

func (m mockLockConnector) Driver() driver.Driver {
	return m.d
}

type mockLockConn struct {
	execErr error
}

func (*mockLockConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (*mockLockConn) Close() error { return nil }

func (*mockLockConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (c *mockLockConn) ExecContext(_ context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	if c.execErr != nil {
		return nil, c.execErr
	}
	return driver.RowsAffected(1), nil
}

func TestAcquireThreadSummaryLockPostgresDriver(t *testing.T) {
	t.Parallel()
	pgDriver := stdlib.GetDefaultDriver()

	t.Run("lock failure returns error", func(t *testing.T) {
		t.Parallel()
		db := sql.OpenDB(mockLockConnector{d: pgDriver, connErr: errors.New("advisory lock fail")})
		t.Cleanup(func() { _ = db.Close() })
		store := NewStore(db, "aerial")
		if _, err := store.AcquireThreadSummaryLock(context.Background(), "thread-1"); err == nil {
			t.Errorf("expected error on failed advisory lock")
		}
	})

	t.Run("lock and unlock success", func(t *testing.T) {
		t.Parallel()
		db := sql.OpenDB(mockLockConnector{d: pgDriver})
		t.Cleanup(func() { _ = db.Close() })
		store := NewStore(db, "aerial")
		release, err := store.AcquireThreadSummaryLock(context.Background(), "thread-1")
		if err != nil {
			t.Fatalf("AcquireThreadSummaryLock err = %v", err)
		}
		release()
	})

	t.Run("unlock failure destroys connection", func(t *testing.T) {
		t.Parallel()
		conn := &mockLockConn{}
		db := sql.OpenDB(mockLockConnector{d: pgDriver, conn: conn})
		t.Cleanup(func() { _ = db.Close() })
		store := NewStore(db, "aerial")
		release, err := store.AcquireThreadSummaryLock(context.Background(), "thread-1")
		if err != nil {
			t.Fatalf("AcquireThreadSummaryLock err = %v", err)
		}
		conn.execErr = errors.New("unlock fail")
		release()
	})
}
