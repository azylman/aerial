package queue

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	restate "github.com/restatedev/sdk-go"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

func stepMsg(id, status string, retry, restart int) db.Message {
	return db.Message{ID: id, ThreadID: "thread-steps", Content: "hello " + id, Status: status, RetryCount: retry, RestartCount: restart, CreatedAt: time.Now().UTC()}
}

func doneMsg(id, status, errText, response string) db.Message {
	m := stepMsg(id, status, 0, 0)
	m.ErrorMessage, m.ResponseText = errText, response
	return m
}

type rowWant struct {
	status, errText string
	retry, restart  int
}

func assertRows(t *testing.T, store db.Store, want map[string]rowWant) {
	t.Helper()
	for id, w := range want {
		m, err := store.GetMessage(context.Background(), id)
		if err != nil || m == nil {
			t.Fatalf("GetMessage(%s) = %v, %v", id, m, err)
		}
		if m.Status != w.status || m.ErrorMessage != w.errText || m.RetryCount != w.retry || m.RestartCount != w.restart {
			t.Errorf("row %s = {%s %q retry=%d restart=%d}, want {%s %q retry=%d restart=%d}",
				id, m.Status, m.ErrorMessage, m.RetryCount, m.RestartCount, w.status, w.errText, w.retry, w.restart)
		}
	}
}

// stepFixture declares a step test's store: noStore rows get no database (Store stays nil);
// the rest get a private Postgres database seeded with msgs and disps, then sabotageSQL is run
// against it and closeDB closes the handle so every store call fails retryably.
type stepFixture struct {
	noStore     bool
	msgs        []db.Message
	disps       []db.Dispatch
	sabotageSQL string
	closeDB     bool
}

// stepDB is a step test's store plus its raw handle (both nil for noStore rows).
type stepDB struct {
	store db.Store
	raw   *sql.DB
}

// openStepDB opens the fixture's store; noStore rows get no database.
func openStepDB(t *testing.T, fx stepFixture) stepDB {
	t.Helper()
	if fx.noStore {
		return stepDB{}
	}
	store, sqlDB := newTestStore(t)
	seedMessageRows(t, store, fx.msgs)
	seedDispatchRows(t, store, fx.disps)
	execSQL(t, sqlDB, fx.sabotageSQL)
	if fx.closeDB {
		if err := sqlDB.Close(); err != nil {
			t.Fatalf("close test database: %v", err)
		}
	}
	return stepDB{store: store, raw: sqlDB}
}

// stepPool builds a pool over the fixture's store and returns the store for assertions.
func stepPool(t *testing.T, fx stepFixture, cfg WorkerPoolConfig) (*WorkerPool, db.Store) {
	t.Helper()
	cfg.Store = openStepDB(t, fx).store
	return NewWorkerPool(cfg), cfg.Store
}

// testStore opens a production SQLStore over a private database for tests that only seed
// through the store.
func testStore(t testing.TB) *db.SQLStore {
	t.Helper()
	return db.NewSQLStore(newTestDB(t))
}

// faultTriggerSQL makes the first count firings of event on table fail with SQLSTATE sqlstate,
// counted by a sequence in the test's private database (nextval survives the rollback). event may
// list columns ("UPDATE OF duration_ms") so only statements setting them fire the trigger.
func faultTriggerSQL(table, event, sqlstate string, count int) string {
	name := sabotageName("fault", table, event)
	return fmt.Sprintf(`CREATE SEQUENCE %[1]s_seq;
CREATE FUNCTION %[1]s() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	IF nextval('%[1]s_seq') <= %[5]d THEN
		RAISE EXCEPTION 'sabotaged %% on %%', TG_OP, TG_TABLE_NAME USING ERRCODE = '%[4]s';
	END IF;
	RETURN NEW;
END $$;
CREATE TRIGGER %[1]s BEFORE %[3]s ON %[2]s FOR EACH ROW EXECUTE FUNCTION %[1]s();`, name, table, event, sqlstate, count)
}

// sabotageName derives a trigger identifier from its kind, table and event.
func sabotageName(kind, table, event string) string {
	return "pgtest_" + kind + "_" + table + "_" + strings.ToLower(strings.ReplaceAll(event, " ", "_"))
}

// renameColumnSQL renames a column the store's SELECTs read but its claim/write statements never
// touch, so reads fail with SQLSTATE 42703 (terminal) after earlier writes succeeded.
func renameColumnSQL(table string) string {
	return fmt.Sprintf("ALTER TABLE %s RENAME COLUMN response_text TO response_text_sabotaged", table)
}

// dispatchStatus reads a dispatch's status column directly, so it works under renameColumnSQL.
func dispatchStatus(t *testing.T, sqlDB *sql.DB, id string) string {
	t.Helper()
	var status string
	if err := sqlDB.QueryRowContext(context.Background(), "SELECT status FROM dispatches WHERE id = $1", id).Scan(&status); err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("read dispatch %s status: %v", id, err)
	}
	return status
}

// assertStepErr checks a step's error presence and terminal tagging.
func assertStepErr(t *testing.T, err error, wantErr, wantTerminal bool) {
	t.Helper()
	if (err != nil) != wantErr {
		t.Fatalf("err = %v, wantErr %v", err, wantErr)
	}
	if err != nil && restate.IsTerminalError(err) != wantTerminal {
		t.Errorf("terminal = %v, want %v (err %v)", restate.IsTerminalError(err), wantTerminal, err)
	}
}

func TestRecordBurstStarted(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		rows          []db.Message
		ids           []string
		wantIDs       []string
		wantBaseRetry int
		wantRows      map[string]rowWant
		sabotageSQL   string
		noStore       bool
		wantErr       bool
		wantTerminal  bool
	}{
		{name: "ClaimErrorRetryable", rows: []db.Message{stepMsg("a", db.StatusPending, 0, 0)}, ids: []string{"a"}, sabotageSQL: failTriggerSQL("messages", "UPDATE", "40001"), wantErr: true, wantRows: map[string]rowWant{"a": {status: db.StatusPending}}},
		{name: "ReadIntegrityErrorTerminal", rows: []db.Message{stepMsg("a", db.StatusPending, 0, 0)}, ids: []string{"a"}, sabotageSQL: renameColumnSQL("messages"), wantErr: true, wantTerminal: true},
		{name: "NoStoreTerminal", ids: []string{"a"}, noStore: true, wantErr: true, wantTerminal: true},
		{
			name:    "AllPendingClaimed",
			rows:    []db.Message{stepMsg("a", db.StatusPending, 0, 0), stepMsg("b", db.StatusPending, 1, 0)},
			ids:     []string{"a", "b"},
			wantIDs: []string{"a", "b"}, wantBaseRetry: 1,
			wantRows: map[string]rowWant{"a": {status: db.StatusProcessing}, "b": {status: db.StatusProcessing, retry: 1}},
		},
		{
			name:     "RetryAfterPartialClaimAdoptsProcessing",
			rows:     []db.Message{stepMsg("a", db.StatusProcessing, 0, 1), stepMsg("b", db.StatusProcessing, 0, 0), stepMsg("c", db.StatusPending, 0, 0)},
			ids:      []string{"a", "b", "c"},
			wantIDs:  []string{"a", "b", "c"},
			wantRows: map[string]rowWant{"a": {status: db.StatusProcessing, restart: 1}, "b": {status: db.StatusProcessing}, "c": {status: db.StatusProcessing}},
		},
		{
			name:    "CompletedAndMissingSkipped",
			rows:    []db.Message{doneMsg("a", db.StatusCompleted, "", "hi"), stepMsg("b", db.StatusPending, 2, 0)},
			ids:     []string{"a", "b", "missing", "b", " "},
			wantIDs: []string{"b"}, wantBaseRetry: 2,
			wantRows: map[string]rowWant{"a": {status: db.StatusCompleted}, "b": {status: db.StatusProcessing, retry: 2}},
		},
		{
			name: "NothingClaimable",
			rows: []db.Message{doneMsg("a", db.StatusFailed, "boom", "")},
			ids:  []string{"a"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, store := stepPool(t, stepFixture{noStore: tc.noStore, msgs: tc.rows, sabotageSQL: tc.sabotageSQL}, WorkerPoolConfig{})
			got, err := pool.RecordBurstStarted(context.Background(), "thread-steps", tc.ids)
			assertStepErr(t, err, tc.wantErr, tc.wantTerminal)
			if strings.Join(got.IDs, ",") != strings.Join(tc.wantIDs, ",") || got.BaseRetry != tc.wantBaseRetry {
				t.Errorf("got %+v, want ids=%v base=%d", got, tc.wantIDs, tc.wantBaseRetry)
			}
			assertRows(t, store, tc.wantRows)
		})
	}
}

func TestRecordDispatchStarted(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		status      string
		retry       int
		seed        bool
		wantIDs     []string
		wantBase    int
		wantStatus  string
		dispatchID  string
		sabotageSQL string
		noStore     bool
		wantErr     bool
		wantTerm    bool
	}{
		{name: "BlankIDNoop", dispatchID: " "},
		{name: "ClaimErrorRetryable", status: db.StatusPending, seed: true, sabotageSQL: failTriggerSQL("dispatches", "UPDATE", "40001"), wantErr: true, wantStatus: db.StatusPending},
		{name: "ReadErrorTerminal", status: db.StatusPending, seed: true, sabotageSQL: renameColumnSQL("dispatches"), wantErr: true, wantTerm: true, wantStatus: db.StatusProcessing},
		{name: "NoStoreTerminal", noStore: true, wantErr: true, wantTerm: true},
		{name: "PendingClaimed", status: db.StatusPending, retry: 1, seed: true, wantIDs: []string{"d1"}, wantBase: 1, wantStatus: db.StatusProcessing},
		{name: "ProcessingAdopted", status: db.StatusProcessing, seed: true, wantIDs: []string{"d1"}, wantStatus: db.StatusProcessing},
		{name: "CompletedSkipped", status: db.StatusCompleted, seed: true, wantStatus: db.StatusCompleted},
		{name: "MissingSkipped"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fx := stepFixture{noStore: tc.noStore, sabotageSQL: tc.sabotageSQL}
			if tc.seed {
				fx.disps = []db.Dispatch{{ID: "d1", Source: "test", SessionID: "sess-1", Prompt: "p", Status: tc.status, RetryCount: tc.retry}}
			}
			sdb := openStepDB(t, fx)
			store, sqlDB := sdb.store, sdb.raw
			pool := NewWorkerPool(WorkerPoolConfig{Store: store})
			id := "d1"
			if tc.dispatchID != "" {
				id = tc.dispatchID
			}
			got, err := pool.RecordDispatchStarted(context.Background(), "sess-1", id)
			assertStepErr(t, err, tc.wantErr, tc.wantTerm)
			if strings.Join(got.IDs, ",") != strings.Join(tc.wantIDs, ",") || got.BaseRetry != tc.wantBase {
				t.Errorf("got %+v, want ids=%v base=%d", got, tc.wantIDs, tc.wantBase)
			}
			if tc.seed {
				if got := dispatchStatus(t, sqlDB, "d1"); got != tc.wantStatus {
					t.Errorf("dispatch status = %q, want %s", got, tc.wantStatus)
				}
			}
		})
	}
}

type prepWant struct {
	decision, promptSub, injected string
	delayMs                       int64
	metadata                      map[string]string
}

func assertPrep(t *testing.T, got TurnPrep, want prepWant) {
	t.Helper()
	if got.Decision != want.decision || got.DelayMs != want.delayMs || got.InjectedContext != want.injected {
		t.Errorf("prep = %+v, want decision=%s delay=%d injected=%q", got, want.decision, want.delayMs, want.injected)
	}
	if !strings.Contains(got.Prompt, want.promptSub) {
		t.Errorf("prompt %q missing %q", got.Prompt, want.promptSub)
	}
	if len(got.HookMetadata) != len(want.metadata) {
		t.Errorf("metadata = %v, want %v", got.HookMetadata, want.metadata)
	}
	for k, v := range want.metadata {
		if got.HookMetadata[k] != v {
			t.Errorf("metadata[%s] = %q, want %q", k, got.HookMetadata[k], v)
		}
	}
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var back TurnPrep
	if err := json.Unmarshal(b, &back); err != nil || back.Prompt != got.Prompt || back.Decision != got.Decision || len(back.HookMetadata) != len(got.HookMetadata) {
		t.Errorf("TurnPrep did not round-trip: %+v err=%v", back, err)
	}
}

func TestPrepareBurstTurn_GuardsAndFaults(t *testing.T) {
	t.Parallel()
	preTurn := &config.WebhookEndpoint{URL: "http://sidecar.local/pre", TimeoutMs: 1000}
	requeueResp := &PreTurnResponse{Allow: false, Action: "retry"}
	tests := []struct {
		name          string
		rows          []db.Message
		mode          string
		failLease     bool
		sabotageSQL   string
		closeDB       bool
		wantDecision  string
		wantPromptSub string
		wantRows      map[string]rowWant
		wantErr       bool
	}{
		{
			name: "ReadErrorRetryable", mode: "thread", closeDB: true, wantErr: true,
			rows: []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)},
		},
		{
			name: "RequeueErrorRetryable", mode: "thread", sabotageSQL: failTriggerSQL("messages", "UPDATE", "40001"), wantErr: true,
			rows:     []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)},
			wantRows: map[string]rowWant{"a": {status: db.StatusProcessing}},
		},
		{
			name: "IgnoredChannelFinished", mode: "ignore",
			rows:         []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)},
			wantDecision: TurnDecisionFinished,
			wantRows:     map[string]rowWant{"a": {status: db.StatusCompleted, errText: "[IGNORE]"}},
		},
		{
			name: "LeaseErrorDeferredToAttemptRunner", mode: "thread", failLease: true,
			rows:          []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)},
			wantDecision:  TurnDecisionRun,
			wantPromptSub: "hello a",
			wantRows:      map[string]rowWant{"a": {status: db.StatusProcessing}},
		},
		{
			name: "TerminalRowsFinishedWithoutWrites", mode: "thread",
			rows:         []db.Message{doneMsg("a", db.StatusCompleted, "", "done")},
			wantDecision: TurnDecisionFinished,
			wantRows:     map[string]rowWant{"a": {status: db.StatusCompleted}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := openStepDB(t, stepFixture{msgs: tc.rows, sabotageSQL: tc.sabotageSQL, closeDB: tc.closeDB}).store
			policy := config.ChannelPolicy{Mode: tc.mode}
			if tc.sabotageSQL != "" {
				policy.Hooks.PreTurn = preTurn
			}
			cfg := WorkerPoolConfig{
				Store: store, MaxAttempts: 3,
				ResolveChannelPolicy: func(string, string) config.ChannelPolicy { return policy },
				WebhookDispatcher: &mockWebhookDispatcher{callPreTurnHookFunc: func(context.Context, *config.WebhookEndpoint, *PreTurnRequest) (*PreTurnResponse, error) {
					return requeueResp, nil
				}},
				RunnerFunc: func(context.Context, string, string, string, string, string, int) (string, string, int, error) {
					return "", "", 0, errors.New("prep must not run the agent")
				},
			}
			if tc.failLease {
				cfg.ProcessPool = &mockAcquireLeaseFailPool{}
			}
			pool := New(nil, cfg)
			pool.Start()
			defer pool.Stop()
			ids := make([]string, len(tc.rows))
			for i, m := range tc.rows {
				ids[i] = m.ID
			}
			got, err := pool.PrepareBurstTurn(context.Background(), "thread-steps", "turn-1", ids, false)
			assertStepErr(t, err, tc.wantErr, false)
			assertPrep(t, got, prepWant{decision: tc.wantDecision, promptSub: tc.wantPromptSub})
			assertRows(t, store, tc.wantRows)
		})
	}
}

func TestPrepareBurstTurn(t *testing.T) {
	t.Parallel()
	preTurn := &config.WebhookEndpoint{URL: "http://sidecar.local/pre", TimeoutMs: 1000}
	tests := []struct {
		name          string
		rows          []db.Message
		hooked        bool
		preResp       *PreTurnResponse
		preErr        error
		runs          int
		wantDecision  string
		wantDelayMs   int64
		wantPromptSub string
		wantInjected  string
		wantMetadata  map[string]string
		wantRows      map[string]rowWant
		wantErr       bool
	}{
		{
			name: "PreTurnRejectedFinished", hooked: true, runs: 1,
			rows:         []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)},
			preResp:      &PreTurnResponse{Allow: false, Reason: "busy"},
			wantDecision: TurnDecisionFinished,
			wantRows:     map[string]rowWant{"a": {status: db.StatusCompleted, errText: "[SKIPPED pre_turn_rejected: busy]"}},
		},
		{
			name: "PreTurnTimeoutDrop", hooked: true, runs: 1,
			rows:         []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)},
			preErr:       errors.New("deadline"),
			wantDecision: TurnDecisionFinished,
			wantRows:     map[string]rowWant{"a": {status: db.StatusCompleted, errText: "[SKIPPED pre_turn_timeout_drop]"}},
		},
		{
			name: "RetryLaterTwiceBumpsOnce", hooked: true, runs: 2,
			rows:         []db.Message{stepMsg("a", db.StatusProcessing, 0, 0), stepMsg("b", db.StatusProcessing, 1, 0)},
			preResp:      &PreTurnResponse{Allow: false, Action: "retry", RetryAfterSeconds: 7},
			wantDecision: TurnDecisionReschedule, wantDelayMs: 5000,
			wantRows: map[string]rowWant{"a": {status: db.StatusPending, errText: "pre_turn_deferred", retry: 1}, "b": {status: db.StatusPending, errText: "pre_turn_deferred", retry: 2}},
		},
		{
			name: "RetryLaterExhaustsLastAttempt", hooked: true, runs: 1,
			rows:         []db.Message{stepMsg("a", db.StatusProcessing, 2, 0), stepMsg("b", db.StatusProcessing, 0, 0)},
			preResp:      &PreTurnResponse{Allow: false, Action: "retry", RetryAfterSeconds: 7},
			wantDecision: TurnDecisionReschedule, wantDelayMs: 7000,
			wantRows: map[string]rowWant{"a": {status: db.StatusFailed, errText: "[EXHAUSTED_PRE_TURN_RETRIES]", retry: 2}, "b": {status: db.StatusPending, errText: "pre_turn_deferred", retry: 1}},
		},
		{
			name: "RetryLaterAllExhaustedFinished", hooked: true, runs: 1,
			rows:         []db.Message{stepMsg("a", db.StatusProcessing, 2, 0)},
			preResp:      &PreTurnResponse{Allow: false, Action: "retry"},
			wantDecision: TurnDecisionFinished,
			wantRows:     map[string]rowWant{"a": {status: db.StatusFailed, errText: "[EXHAUSTED_PRE_TURN_RETRIES]", retry: 2}},
		},
		{
			name: "AllowedBuildsPrompt", hooked: true, runs: 1,
			rows:          []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)},
			preResp:       &PreTurnResponse{Allow: true, InjectedContext: "be brief", Metadata: map[string]any{"lane": "fast", "n": 2}},
			wantDecision:  TurnDecisionRun,
			wantPromptSub: "hello a",
			wantInjected:  "<COORDINATION_CONTEXT>\nbe brief\n</COORDINATION_CONTEXT>",
			wantMetadata:  map[string]string{"lane": "fast", "n": "2"},
			wantRows:      map[string]rowWant{"a": {status: db.StatusProcessing}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := openStepDB(t, stepFixture{msgs: tc.rows}).store
			policy := config.ChannelPolicy{Mode: "thread"}
			if tc.hooked {
				policy.Hooks.PreTurn = preTurn
			}
			preResp, preErr := tc.preResp, tc.preErr
			cfg := WorkerPoolConfig{
				Store: store, MaxAttempts: 3,
				ResolveChannelPolicy: func(string, string) config.ChannelPolicy { return policy },
				WebhookDispatcher: &mockWebhookDispatcher{callPreTurnHookFunc: func(context.Context, *config.WebhookEndpoint, *PreTurnRequest) (*PreTurnResponse, error) {
					return preResp, preErr
				}},
				RunnerFunc: func(context.Context, string, string, string, string, string, int) (string, string, int, error) {
					return "", "", 0, errors.New("prep must not run the agent")
				},
			}
			pool := New(nil, cfg)
			pool.Start()
			defer pool.Stop()
			ids := make([]string, len(tc.rows))
			for i, m := range tc.rows {
				ids[i] = m.ID
			}
			var got TurnPrep
			var err error
			for range tc.runs {
				got, err = pool.PrepareBurstTurn(context.Background(), "thread-steps", "turn-1", ids, false)
			}
			assertStepErr(t, err, tc.wantErr, false)
			assertPrep(t, got, prepWant{decision: tc.wantDecision, delayMs: tc.wantDelayMs, promptSub: tc.wantPromptSub, injected: tc.wantInjected, metadata: tc.wantMetadata})
			assertRows(t, store, tc.wantRows)
		})
	}
}

func TestPrepareDispatchTurn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		status        string
		sessionID     string
		seed          bool
		wantDecision  string
		wantPromptSub string
		sabotageSQL   string
		noStore       bool
		wantErr       bool
		wantTerminal  bool
		noAgent       bool
		failLease     bool
		wantStatus    string
	}{
		{name: "SessionLeaseUnavailableFinished", status: db.StatusProcessing, sessionID: "sess-1", seed: true, noAgent: true, wantDecision: TurnDecisionFinished, wantStatus: db.StatusFailed},
		{name: "SessionLeaseErrorDeferredToAttemptRunner", status: db.StatusProcessing, sessionID: "sess-1", seed: true, failLease: true, wantDecision: TurnDecisionRun, wantPromptSub: "do the thing", wantStatus: db.StatusProcessing},
		{name: "ReadErrorTerminal", status: db.StatusProcessing, seed: true, sabotageSQL: renameColumnSQL("dispatches"), wantErr: true, wantTerminal: true, wantStatus: db.StatusProcessing},
		{name: "NoStoreTerminal", noStore: true, wantErr: true, wantTerminal: true},
		{name: "EphemeralBuildsPrompt", status: db.StatusProcessing, seed: true, wantDecision: TurnDecisionRun, wantPromptSub: "do the thing"},
		{name: "SessionBuildsPrompt", status: db.StatusProcessing, sessionID: "sess-1", seed: true, wantDecision: TurnDecisionRun, wantPromptSub: "do the thing"},
		{name: "TerminalFinished", status: db.StatusFailed, seed: true, wantDecision: TurnDecisionFinished},
		{name: "MissingFinished", wantDecision: TurnDecisionFinished},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fx := stepFixture{noStore: tc.noStore, sabotageSQL: tc.sabotageSQL}
			if tc.seed {
				fx.disps = []db.Dispatch{{ID: "d1", Source: "test", SessionID: tc.sessionID, Prompt: "do the thing", Status: tc.status}}
			}
			sdb := openStepDB(t, fx)
			store, sqlDB := sdb.store, sdb.raw
			cfg := WorkerPoolConfig{Store: store, RunnerFunc: func(context.Context, string, string, string, string, string, int) (string, string, int, error) {
				return "", "", 0, errors.New("prep must not run the agent")
			}}
			if tc.noAgent {
				cfg.RunnerFunc = nil
			}
			if tc.failLease {
				cfg.ProcessPool = &mockAcquireLeaseFailPool{}
			}
			pool := New(nil, cfg)
			pool.Start()
			defer pool.Stop()
			got, err := pool.PrepareDispatchTurn(context.Background(), "sess-1", "turn-1", "d1")
			assertStepErr(t, err, tc.wantErr, tc.wantTerminal)
			if tc.wantStatus != "" {
				if status := dispatchStatus(t, sqlDB, "d1"); status != tc.wantStatus {
					t.Errorf("dispatch status = %q, want %s", status, tc.wantStatus)
				}
			}
			if got.Decision != tc.wantDecision || !strings.Contains(got.Prompt, tc.wantPromptSub) {
				t.Errorf("prep = %+v, want decision=%s prompt containing %q", got, tc.wantDecision, tc.wantPromptSub)
			}
		})
	}
}

// noticeRecorder captures failure notices delivered to Discord.
type noticeRecorder struct {
	mu      sync.Mutex
	notices []string
}

func (r *noticeRecorder) deliver(_ *discordgo.Session, _, text string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.notices = append(r.notices, text)
	return nil
}

func (r *noticeRecorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.notices...)
}

func TestRecordBurstOutcome(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		rows        []db.Message
		summary     AttemptSummary
		runs        int
		wantErr     bool
		wantNotices []string
		wantRows    map[string]rowWant
		sabotageSQL string
		closeDB     bool
		noStore     bool
	}{
		{name: "NoStoreTerminal", runs: 1, noStore: true, wantErr: true, rows: []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)}, summary: AttemptSummary{Outcome: OutcomeFailed}},
		{
			name: "ReadErrorRetryable", runs: 1, wantErr: true, closeDB: true,
			rows:    []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)},
			summary: AttemptSummary{Outcome: OutcomeFailed, Reason: "x"},
		},
		{
			name: "FailWriteErrorRetryable", runs: 1, wantErr: true, sabotageSQL: failTriggerSQL("messages", "UPDATE", "40001"),
			rows:     []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)},
			summary:  AttemptSummary{Outcome: OutcomeCancelled},
			wantRows: map[string]rowWant{"a": {status: db.StatusProcessing}},
		},
		{
			name: "TelemetryWriteErrorRetryable", runs: 1, wantErr: true, sabotageSQL: faultTriggerSQL("messages", "UPDATE OF duration_ms", "40001", 1),
			rows:     []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)},
			summary:  AttemptSummary{Outcome: OutcomeCancelled, DurationMs: 5},
			wantRows: map[string]rowWant{"a": {status: db.StatusFailed, errText: "cancelled by operator"}},
		},
		{name: "NoIDsNoop", runs: 1, summary: AttemptSummary{Outcome: OutcomeFailed}},
		{
			name: "CustomNoticePrefix", runs: 1,
			rows:        []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)},
			summary:     AttemptSummary{Outcome: OutcomeTimeout, Reason: "stalled", NoticePrefix: "agent went quiet"},
			wantNotices: []string{"agent went quiet: stalled"},
			wantRows:    map[string]rowWant{"a": {status: db.StatusFailed, errText: "stalled"}},
		},
		{
			name: "FailedDefaultReason", runs: 1,
			rows:        []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)},
			summary:     AttemptSummary{Outcome: OutcomeFailed, NoticePrefix: "exhausted"},
			wantNotices: []string{"exhausted: failed"},
			wantRows:    map[string]rowWant{"a": {status: db.StatusFailed, errText: "failed"}},
		},
		{
			name: "FailedNotifiesAndFailsActiveRows", runs: 1,
			rows:        []db.Message{stepMsg("a", db.StatusProcessing, 1, 0), doneMsg("b", db.StatusCompleted, "", "ok")},
			summary:     AttemptSummary{Outcome: OutcomeFailed, Reason: "agent exploded", DurationMs: 1200},
			wantNotices: []string{"execution failed: agent exploded"},
			wantRows:    map[string]rowWant{"a": {status: db.StatusFailed, errText: "agent exploded", retry: 1}, "b": {status: db.StatusCompleted}},
		},
		{
			name: "TimeoutReplayNotifiesOnce", runs: 2,
			rows:        []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)},
			summary:     AttemptSummary{Outcome: OutcomeTimeout, Reason: "[watchdog] inactivity timeout exceeded"},
			wantNotices: []string{"execution timed out while processing the request: [watchdog] inactivity timeout exceeded"},
			wantRows:    map[string]rowWant{"a": {status: db.StatusFailed, errText: "[watchdog] inactivity timeout exceeded"}},
		},
		{
			name: "CancelledNoNotice", runs: 1,
			rows:     []db.Message{stepMsg("a", db.StatusPending, 0, 0)},
			summary:  AttemptSummary{Outcome: OutcomeCancelled},
			wantRows: map[string]rowWant{"a": {status: db.StatusFailed, errText: "cancelled by operator"}},
		},
		{
			name: "CompletedAllTerminal", runs: 1,
			rows:     []db.Message{doneMsg("a", db.StatusCompleted, "", "ok")},
			summary:  AttemptSummary{Outcome: OutcomeCompleted},
			wantRows: map[string]rowWant{"a": {status: db.StatusCompleted}},
		},
		{
			name: "CompletedWithOpenRowIsRetryable", runs: 1, wantErr: true,
			rows:     []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)},
			summary:  AttemptSummary{Outcome: OutcomeCompleted},
			wantRows: map[string]rowWant{"a": {status: db.StatusProcessing}},
		},
		{
			name: "RescheduleLeavesRows", runs: 1,
			rows:     []db.Message{stepMsg("a", db.StatusPending, 1, 0)},
			summary:  AttemptSummary{Outcome: OutcomeReschedule},
			wantRows: map[string]rowWant{"a": {status: db.StatusPending, retry: 1}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &noticeRecorder{}
			pool, store := stepPool(t, stepFixture{noStore: tc.noStore, msgs: tc.rows, sabotageSQL: tc.sabotageSQL, closeDB: tc.closeDB}, WorkerPoolConfig{DeliveryFunc: rec.deliver, NotifierFunc: func(_, _, desc string) string { return desc }})
			ids := make([]string, len(tc.rows))
			for i, m := range tc.rows {
				ids[i] = m.ID
			}
			var err error
			for range tc.runs {
				err = pool.RecordBurstOutcome(context.Background(), ids, tc.summary)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("RecordBurstOutcome err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := rec.all(); strings.Join(got, "|") != strings.Join(tc.wantNotices, "|") {
				t.Errorf("notices = %q, want %q", got, tc.wantNotices)
			}
			assertRows(t, store, tc.wantRows)
		})
	}
}

func TestRecordDispatchOutcome(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		status      string
		summary     AttemptSummary
		wantErr     bool
		wantStatus  string
		wantErrMsg  string
		sabotageSQL string
		closeDB     bool
	}{
		{name: "FailWriteErrorRetryable", status: db.StatusProcessing, summary: AttemptSummary{Outcome: OutcomeFailed, Reason: "boom"}, sabotageSQL: failTriggerSQL("dispatches", "UPDATE", "40001"), wantErr: true, wantStatus: db.StatusProcessing},
		{name: "TelemetryWriteErrorRetryable", status: db.StatusProcessing, summary: AttemptSummary{Outcome: OutcomeFailed, Reason: "boom", DurationMs: 9}, sabotageSQL: faultTriggerSQL("dispatches", "UPDATE OF duration_ms", "40001", 1), wantErr: true, wantStatus: db.StatusFailed, wantErrMsg: "boom"},
		{name: "VerifyReadErrorRetryable", status: db.StatusCompleted, summary: AttemptSummary{Outcome: OutcomeCompleted}, closeDB: true, wantErr: true},
		{name: "RescheduleNoop", status: db.StatusPending, summary: AttemptSummary{Outcome: OutcomeReschedule}, wantStatus: db.StatusPending},
		{name: "FailedRecorded", status: db.StatusProcessing, summary: AttemptSummary{Outcome: OutcomeFailed, Reason: "boom", DurationMs: 50}, wantStatus: db.StatusFailed, wantErrMsg: "boom"},
		{name: "TimeoutDefaultReason", status: db.StatusProcessing, summary: AttemptSummary{Outcome: OutcomeTimeout}, wantStatus: db.StatusFailed, wantErrMsg: "timeout"},
		{name: "CompletedOpenRowRetryable", status: db.StatusProcessing, summary: AttemptSummary{Outcome: OutcomeCompleted}, wantErr: true, wantStatus: db.StatusProcessing},
		{name: "AlreadyCompletedUntouched", status: db.StatusCompleted, summary: AttemptSummary{Outcome: OutcomeFailed, Reason: "late"}, wantStatus: db.StatusCompleted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, store := stepPool(t, stepFixture{disps: []db.Dispatch{{ID: "d1", Source: "test", Prompt: "p", Status: tc.status}}, sabotageSQL: tc.sabotageSQL, closeDB: tc.closeDB}, WorkerPoolConfig{})
			err := pool.RecordDispatchOutcome(context.Background(), []string{"d1"}, tc.summary)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.closeDB {
				return
			}
			d, err := store.GetDispatch(context.Background(), "d1")
			if err != nil {
				t.Fatalf("GetDispatch: %v", err)
			}
			if d == nil || d.Status != tc.wantStatus || d.ErrorMessage != tc.wantErrMsg {
				t.Errorf("dispatch = %+v, want status=%s err=%q", d, tc.wantStatus, tc.wantErrMsg)
			}
		})
	}
}

func TestRunBurstPostTurnHook(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		rows         []db.Message
		attempted    bool
		outcome      TurnOutcome
		metadata     map[string]string
		wantCalled   bool
		wantStatus   string
		wantResponse string
		wantError    string
		wantMetaKey  string
		closeDB      bool
		wantErr      bool
	}{
		{name: "ReadErrorRetryable", attempted: true, outcome: OutcomeCompleted, rows: []db.Message{doneMsg("a", db.StatusCompleted, "", "r")}, closeDB: true, wantErr: true},
		{name: "NoRowsNoop", attempted: true, outcome: OutcomeFailed},
		{name: "FailedSendsFailed", attempted: true, outcome: OutcomeFailed, rows: []db.Message{doneMsg("a", db.StatusFailed, "boom", "")}, wantCalled: true, wantStatus: "failed", wantError: "boom"},
		{name: "CompletedSendsSuccess", attempted: true, outcome: OutcomeCompleted, metadata: map[string]string{"lane": "fast"}, rows: []db.Message{doneMsg("a", db.StatusCompleted, "", "the reply")}, wantCalled: true, wantStatus: "success", wantResponse: "the reply", wantMetaKey: "lane"},
		{name: "TimeoutSendsTimeout", attempted: true, outcome: OutcomeTimeout, rows: []db.Message{doneMsg("a", db.StatusFailed, "[watchdog] stalled", "")}, wantCalled: true, wantStatus: "timeout", wantError: "[watchdog] stalled"},
		{name: "NotAttemptedSkipsHook", attempted: false, outcome: OutcomeCompleted, rows: []db.Message{doneMsg("a", db.StatusCompleted, "[SKIPPED x]", "")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := openStepDB(t, stepFixture{msgs: tc.rows, closeDB: tc.closeDB}).store
			var mu sync.Mutex
			var captured *PostTurnRequest
			policy := config.ChannelPolicy{Mode: "thread", Hooks: config.ChannelHooksConfig{PostTurn: &config.WebhookEndpoint{URL: "http://sidecar.local/post", TimeoutMs: 1000}}}
			pool := NewWorkerPool(WorkerPoolConfig{
				Store:                store,
				ResolveChannelPolicy: func(string, string) config.ChannelPolicy { return policy },
				WebhookDispatcher: &mockWebhookDispatcher{callPostTurnHookFunc: func(_ context.Context, _ *config.WebhookEndpoint, req *PostTurnRequest) (*PostTurnResponse, error) {
					mu.Lock()
					captured = req
					mu.Unlock()
					return &PostTurnResponse{Acknowledged: true}, nil
				}},
			})
			err := pool.RunBurstPostTurnHook(context.Background(), "thread-steps", []string{"a"}, tc.attempted, tc.outcome, tc.metadata)
			assertStepErr(t, err, tc.wantErr, false)
			mu.Lock()
			defer mu.Unlock()
			if (captured != nil) != tc.wantCalled {
				t.Fatalf("hook called = %v, want %v", captured != nil, tc.wantCalled)
			}
			if captured == nil {
				return
			}
			if captured.Status != tc.wantStatus || captured.ResponseText != tc.wantResponse || captured.Error != tc.wantError {
				t.Errorf("post_turn = %+v, want status=%s response=%q error=%q", captured, tc.wantStatus, tc.wantResponse, tc.wantError)
			}
			if _, ok := captured.Metadata[tc.wantMetaKey]; tc.wantMetaKey != "" && !ok {
				t.Errorf("metadata %v missing %s", captured.Metadata, tc.wantMetaKey)
			}
		})
	}
}

func TestPublishDispatchOutcome(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		outcome      TurnOutcome
		runs         int
		wantSubjects []string
		dispatchID   string
		noBus        bool
		closeDB      bool
		wantErr      bool
	}{
		{name: "FailedPublishesWithError", outcome: OutcomeFailed, runs: 1, wantSubjects: []string{pubsub.SubjectAerialDispatchPrefix + "failed"}},
		{name: "ReplayPublishesSameMsgID", outcome: OutcomeCompleted, runs: 2, wantSubjects: []string{pubsub.SubjectAerialDispatchPrefix + "completed", pubsub.SubjectAerialDispatchPrefix + "completed"}},
		{name: "MissingDispatchSkipped", outcome: OutcomeCompleted, runs: 1, dispatchID: "nope"},
		{name: "NoBusSkipped", outcome: OutcomeCompleted, runs: 1, noBus: true},
		{name: "ReadErrorRetryable", outcome: OutcomeCompleted, runs: 1, closeDB: true, wantErr: true},
		{name: "CompletedPublishes", outcome: OutcomeCompleted, runs: 1, wantSubjects: []string{pubsub.SubjectAerialDispatchPrefix + "completed"}},
		{name: "TimeoutPublishes", outcome: OutcomeTimeout, runs: 1, wantSubjects: []string{pubsub.SubjectAerialDispatchPrefix + "timeout"}},
		{name: "CancelledSkipped", outcome: OutcomeCancelled, runs: 1},
		{name: "EmptySkipped", outcome: OutcomeEmpty, runs: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			memBus := pubsub.NewMemoryBus()
			defer memBus.Close()
			bus := &trackingBus{Bus: memBus}
			cfg := WorkerPoolConfig{EventBus: bus}
			if tc.noBus {
				cfg.EventBus = nil
			}
			cfg.Store = openStepDB(t, stepFixture{disps: []db.Dispatch{{ID: "d1", Source: "vanadis", Prompt: "p", Status: db.StatusCompleted, DurationMs: 10}}, closeDB: tc.closeDB}).store
			pool := NewWorkerPool(cfg)
			id := "d1"
			if tc.dispatchID != "" {
				id = tc.dispatchID
			}
			var err error
			for range tc.runs {
				err = pool.PublishDispatchOutcome(context.Background(), id, tc.outcome)
			}
			assertStepErr(t, err, tc.wantErr, false)
			bus.mu.Lock()
			defer bus.mu.Unlock()
			if strings.Join(bus.publishes, ",") != strings.Join(tc.wantSubjects, ",") {
				t.Errorf("published %v, want %v", bus.publishes, tc.wantSubjects)
			}
		})
	}
}

func TestStringifyHookMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   map[string]any
		want map[string]string
	}{
		{name: "Nil", in: nil, want: nil},
		{name: "Mixed", in: map[string]any{"s": "x", "n": 3, "b": true, "o": map[string]any{"k": "v"}}, want: map[string]string{"s": "x", "n": "3", "b": "true", "o": `{"k":"v"}`}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := stringifyHookMetadata(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("[%s] = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

func TestStopBurstTurn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		rows        []db.Message
		runs        int
		sabotageSQL string
		noStore     bool
		wantErr     bool
		wantStop    bool
		wantRows    map[string]rowWant
	}{
		{name: "NoStoreTerminal", runs: 1, noStore: true, wantErr: true},
		{name: "FailWriteErrorRetryable", runs: 1, sabotageSQL: failTriggerSQL("messages", "UPDATE", "40001"), wantErr: true, wantStop: true, rows: []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)}, wantRows: map[string]rowWant{"a": {status: db.StatusProcessing}}},
		{
			name: "ReplayCancelsActiveRowsOnceWithoutNotice", runs: 2, wantStop: true,
			rows:     []db.Message{stepMsg("a", db.StatusProcessing, 0, 0), stepMsg("b", db.StatusPending, 0, 0), doneMsg("c", db.StatusCompleted, "", "ok")},
			wantRows: map[string]rowWant{"a": {status: db.StatusFailed, errText: CancelledByOperatorReason}, "b": {status: db.StatusFailed, errText: CancelledByOperatorReason}, "c": {status: db.StatusCompleted}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &noticeRecorder{}
			pool, store := stepPool(t, stepFixture{noStore: tc.noStore, msgs: tc.rows, sabotageSQL: tc.sabotageSQL}, WorkerPoolConfig{DeliveryFunc: rec.deliver, NotifierFunc: func(_, _, desc string) string { return desc }})
			var err error
			for range tc.runs {
				err = pool.StopBurstTurn(context.Background(), "turn-s", []string{"a", "b", "c"})
			}
			assertStepErr(t, err, tc.wantErr, tc.noStore)
			if got := pool.Turns().Tombstoned("turn-s"); got != tc.wantStop {
				t.Errorf("tombstoned = %v, want %v", got, tc.wantStop)
			}
			if got := rec.all(); len(got) != 0 {
				t.Errorf("notices = %q, want none", got)
			}
			assertRows(t, store, tc.wantRows)
		})
	}
}

func TestFailBurstTurn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		rows        []db.Message
		runs        int
		noStore     bool
		wantErr     bool
		wantNotices []string
		wantRows    map[string]rowWant
	}{
		{name: "NoStoreTerminal", runs: 1, noStore: true, wantErr: true, rows: []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)}},
		{
			name: "ReplayNotifiesOnce", runs: 2,
			rows:        []db.Message{stepMsg("a", db.StatusProcessing, 0, 0), doneMsg("b", db.StatusCompleted, "", "ok")},
			wantNotices: []string{"execution failed: integrity violation"},
			wantRows:    map[string]rowWant{"a": {status: db.StatusFailed, errText: "integrity violation"}, "b": {status: db.StatusCompleted}},
		},
		{name: "AllTerminalNoNotice", runs: 1, rows: []db.Message{doneMsg("a", db.StatusCompleted, "", "ok")}, wantRows: map[string]rowWant{"a": {status: db.StatusCompleted}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &noticeRecorder{}
			pool, store := stepPool(t, stepFixture{noStore: tc.noStore, msgs: tc.rows}, WorkerPoolConfig{DeliveryFunc: rec.deliver, NotifierFunc: func(_, _, desc string) string { return desc }})
			ids := make([]string, len(tc.rows))
			for i, m := range tc.rows {
				ids[i] = m.ID
			}
			var err error
			for range tc.runs {
				err = pool.FailBurstTurn(context.Background(), "turn-f", ids, "integrity violation")
			}
			assertStepErr(t, err, tc.wantErr, tc.noStore)
			if got := rec.all(); strings.Join(got, "|") != strings.Join(tc.wantNotices, "|") {
				t.Errorf("notices = %q, want %q", got, tc.wantNotices)
			}
			if !tc.noStore && !pool.Turns().Tombstoned("turn-f") {
				t.Error("turn not tombstoned")
			}
			assertRows(t, store, tc.wantRows)
		})
	}
}

func TestStopDispatchTurn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		status     string
		noStore    bool
		wantErr    bool
		wantStatus string
		wantErrMsg string
	}{
		{name: "NoStoreTerminal", status: db.StatusProcessing, noStore: true, wantErr: true},
		{name: "ActiveRowCancelled", status: db.StatusProcessing, wantStatus: db.StatusFailed, wantErrMsg: CancelledByOperatorReason},
		{name: "CompletedRowUntouched", status: db.StatusCompleted, wantStatus: db.StatusCompleted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool, store := stepPool(t, stepFixture{noStore: tc.noStore, disps: []db.Dispatch{{ID: "d1", Source: "test", Prompt: "p", Status: tc.status}}}, WorkerPoolConfig{})
			err := pool.StopDispatchTurn(context.Background(), "turn-d", []string{"d1"})
			assertStepErr(t, err, tc.wantErr, tc.noStore)
			if tc.noStore {
				return
			}
			d, err := store.GetDispatch(context.Background(), "d1")
			if err != nil {
				t.Fatalf("GetDispatch: %v", err)
			}
			if d == nil || d.Status != tc.wantStatus || d.ErrorMessage != tc.wantErrMsg {
				t.Errorf("dispatch = %+v, want status=%s err=%q", d, tc.wantStatus, tc.wantErrMsg)
			}
		})
	}
}

func TestFailDispatchTurn(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		status       string
		ids          []string
		noStore      bool
		sabotageSQL  string
		wantErr      bool
		wantStatus   string
		wantSubjects []string
	}{
		{name: "NoStoreTerminal", status: db.StatusProcessing, ids: []string{"d1"}, noStore: true, wantErr: true},
		{name: "FailWriteErrorRetryable", status: db.StatusProcessing, ids: []string{"d1"}, sabotageSQL: failTriggerSQL("dispatches", "UPDATE", "40001"), wantErr: true, wantStatus: db.StatusProcessing},
		{name: "ActiveRowFailedAndPublished", status: db.StatusProcessing, ids: []string{"d1"}, wantStatus: db.StatusFailed, wantSubjects: []string{pubsub.SubjectAerialDispatchPrefix + "failed"}},
		{name: "CompletedRowPublishesCompleted", status: db.StatusCompleted, ids: []string{"d1"}, wantStatus: db.StatusCompleted, wantSubjects: []string{pubsub.SubjectAerialDispatchPrefix + "completed"}},
		{name: "MissingRowSkipped", status: db.StatusCompleted, ids: []string{"nope"}, wantStatus: db.StatusCompleted},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			memBus := pubsub.NewMemoryBus()
			defer memBus.Close()
			bus := &trackingBus{Bus: memBus}
			pool, store := stepPool(t, stepFixture{noStore: tc.noStore, disps: []db.Dispatch{{ID: "d1", Source: "vanadis", Prompt: "p", Status: tc.status}}, sabotageSQL: tc.sabotageSQL}, WorkerPoolConfig{EventBus: bus})
			err := pool.FailDispatchTurn(context.Background(), "turn-d", tc.ids, "integrity violation")
			assertStepErr(t, err, tc.wantErr, tc.noStore)
			if !tc.noStore {
				d, err := store.GetDispatch(context.Background(), "d1")
				if err != nil {
					t.Fatalf("GetDispatch: %v", err)
				}
				if d == nil || d.Status != tc.wantStatus {
					t.Errorf("dispatch = %+v, want status=%s", d, tc.wantStatus)
				}
			}
			bus.mu.Lock()
			defer bus.mu.Unlock()
			if strings.Join(bus.publishes, ",") != strings.Join(tc.wantSubjects, ",") {
				t.Errorf("published %v, want %v", bus.publishes, tc.wantSubjects)
			}
		})
	}
}

func TestRefreshBurstThreadSummary(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		rows    []db.Message
		outcome TurnOutcome
		closeDB bool
		noStore bool
		wantErr bool
	}{
		{name: "NoStoreTerminal", noStore: true, outcome: OutcomeCompleted, wantErr: true},
		{name: "ReadErrorRetryable", outcome: OutcomeCompleted, rows: []db.Message{doneMsg("a", db.StatusCompleted, "", "r")}, closeDB: true, wantErr: true},
		{name: "NoRowsNoop", outcome: OutcomeCompleted},
		{name: "FailedTurnSkipsRefresh", outcome: OutcomeFailed, rows: []db.Message{doneMsg("a", db.StatusFailed, "boom", "")}},
		{name: "CompletedWithoutSessionSkipsRefresh", outcome: OutcomeCompleted, rows: []db.Message{doneMsg("a", db.StatusCompleted, "", "r")}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := WorkerPoolConfig{Store: openStepDB(t, stepFixture{noStore: tc.noStore, msgs: tc.rows, closeDB: tc.closeDB}).store}
			pool := NewWorkerPool(cfg)
			err := pool.RefreshBurstThreadSummary(context.Background(), "thread-steps", []string{"a"}, tc.outcome)
			assertStepErr(t, err, tc.wantErr, tc.noStore)
		})
	}
}

// liveTurn is what driving one turn through the TurnRunner step functions returned.
type liveTurn struct {
	started StartedTurn
	prep    TurnPrep
	kinds   []string
	last    AttemptResult
	outcome TurnOutcome
	err     error
}

// runLiveTurn drives one turn through the TurnRunner step functions in the order executeTurn
// journals them: record-started, prep, run-1..n chained by nextAttempt, record-outcome and the
// post-turn steps. It runs without Restate and skips the backoff sleeps; a step error stops it.
func runLiveTurn(ctx context.Context, subj runnerSubject, maxAttempts int, turnID string) liveTurn {
	var lt liveTurn
	if lt.started, lt.err = subj.recordStarted(ctx); lt.err != nil || len(lt.started.IDs) == 0 {
		lt.outcome = OutcomeEmpty
		return lt
	}
	tp := turnProgress{ids: lt.started.IDs}
	if lt.prep, lt.err = subj.prep(ctx, turnID, lt.started.IDs); lt.err != nil {
		return lt
	}
	tp.hookMetadata = lt.prep.HookMetadata
	if decided, err := applyPrepDecision(lt.prep, &tp); err != nil || decided {
		lt.outcome, lt.err = tp.outcome, err
		return lt
	}
	in := subj.attemptInput(turnID, lt.started.IDs, lt.prep, lt.started.BaseRetry+1)
	for {
		res, err := subj.attempt(ctx, in)
		lt.kinds, lt.last = append(lt.kinds, res.Kind), res
		if err != nil {
			if restate.IsTerminalError(err) {
				lt.outcome = OutcomeFailed
				if failErr := subj.fail(ctx, turnID, tp.ids, err.Error(), tp.hookMetadata); failErr != nil {
					lt.err = failErr
				}
				return lt
			}
			lt.err = err
			return lt
		}
		tp.attempted, tp.last = true, res
		action, next, outcome, err := nextAttempt(in, res, maxAttempts)
		if err != nil {
			lt.err = err
			return lt
		}
		if action == actionDone {
			tp.outcome = outcome
			break
		}
		in = next
	}
	lt.outcome = tp.outcome
	if lt.err = subj.recordOutcome(ctx, tp.ids, outcomeSummary(tp)); lt.err != nil {
		return lt
	}
	for _, step := range subj.afterSteps(tp) {
		if lt.err = step.fn(ctx); lt.err != nil {
			return lt
		}
	}
	return lt
}

// runLiveBurst drives one burst turn of threadID over ids through the TurnRunner steps.
func runLiveBurst(ctx context.Context, pool *WorkerPool, threadID, turnID string, ids ...string) liveTurn {
	return runLiveTurn(ctx, burstRunner{steps: pool, threadID: threadID, ids: normalizeStepIDs(ids)}, pool.maxAttempts(), turnID)
}

// runLiveDispatch drives one dispatch turn through the TurnRunner steps.
func runLiveDispatch(ctx context.Context, pool *WorkerPool, sessionKey, turnID, dispatchID string) liveTurn {
	return runLiveTurn(ctx, dispatchRunner{steps: pool, sessionKey: sessionKey, dispatchID: dispatchID, ids: []string{dispatchID}}, pool.maxAttempts(), turnID)
}

func TestTurnRunner_QuotaPause_PreservesEffort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		effort         string
		expectedEffort string
	}{
		{name: "LowEffort", effort: "low", expectedEffort: "low"},
		{name: "MediumEffort", effort: "medium", expectedEffort: "medium"},
		{name: "HighEffort", effort: "high", expectedEffort: "high"},
		{name: "DefaultEffort", effort: "", expectedEffort: "high"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := testStore(t)
			tmpHome, tempData := t.TempDir(), t.TempDir()
			quotaErrSpawner := newScriptedDaemonSpawner("11111111-2222-3333-4444-555555555555",
				`{"event":"result","result":{"status":"ERROR","exit_code":1,"error":"RESOURCE_EXHAUSTED (code 429): Google Gemini API quota reached. Resets in 30m.","stderr":"RESOURCE_EXHAUSTED (code 429): Google Gemini API quota reached. Resets in 30m."}}`+"\n",
			)
			primaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, quotaErrSpawner)
			defer primaryPool.Close()
			lowPool := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, quotaErrSpawner)
			defer lowPool.Close()

			cfg := newTestConfig(func(d *config.ConfigData) { d.DataDir, d.LowEffortModel = tempData, "gemini-3.8-flash-low" })
			haroBus, getOneShots := newMockHaroOneShotBus(t)
			pool := New(cfg, WorkerPoolConfig{
				ProcessPool:          primaryPool,
				LowEffortProcessPool: lowPool,
				SessionManager:       session.New(tmpHome, tempData),
				Store:                store,
				MaxAttempts:          1,
				EventBus:             haroBus,
			})
			defer pool.Stop()

			threadID := "thread-runner-quota-" + tc.name
			msg := db.Message{ID: "msg-runner-quota-" + tc.name, ThreadID: threadID, Content: "run task", Effort: tc.effort, Status: db.StatusPending, CreatedAt: time.Now()}
			if err := store.InsertMessage(context.Background(), msg); err != nil {
				t.Fatalf("InsertMessage: %v", err)
			}

			lt := runLiveBurst(context.Background(), pool, threadID, "turn-1", msg.ID)
			if lt.outcome != OutcomeFailed {
				t.Fatalf("turn outcome = %s (err = %v), want failed", lt.outcome, lt.err)
			}
			schedules := getOneShots(threadID)
			if len(schedules) != 1 || schedules[0].Effort != tc.expectedEffort {
				t.Fatalf("expected 1 schedule with effort %q, got %v", tc.expectedEffort, schedules)
			}
		})
	}
}

func newTurnTraceMockSpawner(convID string, onTurn func()) *mockDaemonSpawner {
	return &mockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = outW.Write(fmt.Appendf(nil, `{"event":"init","conversation_id":%q}`+"\n", convID))
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = outW.Write([]byte(`{"event":"step_update","step_update":{"step_index":1,"type":"tool_call","tool_name":"run_command"}}` + "\n"))
					if onTurn != nil {
						onTurn()
					}
					_, _ = outW.Write([]byte(`{"event":"step_update","step_update":{"step_index":1,"type":"tool_call","state":"DONE"}}` + "\n"))
					_, _ = outW.Write([]byte(`{"event":"result","result":{"status":"SUCCESS","response":"all done","usage":{"input_tokens":7,"output_tokens":3,"total_tokens":10}}}` + "\n"))
				}
			}()
			return inW, outR, errR, newMockProcessHandle(12347), nil
		},
	}
}

type turnTraceCase struct {
	name        string
	isDispatch  bool
	traceparent string
	wantTraceID string
	wantParent  string
}

func runTraceTurn(t *testing.T, pool *WorkerPool, store db.Store, tc turnTraceCase, convID string) {
	t.Helper()
	if tc.isDispatch {
		d := db.Dispatch{
			ID:          "disp-trace-" + strings.ReplaceAll(tc.name, " ", "-"),
			SessionID:   convID,
			Prompt:      "run task",
			Status:      db.StatusPending,
			CreatedAt:   time.Now().Add(-500 * time.Millisecond),
			Source:      "cron",
			TargetID:    "default",
			Traceparent: tc.traceparent,
		}
		if _, err := store.InsertDispatch(context.Background(), d); err != nil {
			t.Fatalf("InsertDispatch: %v", err)
		}
		lt := runLiveDispatch(context.Background(), pool, convID, "turn-disp-1", d.ID)
		if lt.outcome != OutcomeCompleted {
			t.Fatalf("dispatch turn outcome = %s (err = %v), want completed", lt.outcome, lt.err)
		}
		return
	}
	msg := db.Message{
		ID:          "msg-trace-" + strings.ReplaceAll(tc.name, " ", "-"),
		ThreadID:    "thread-trace-" + strings.ReplaceAll(tc.name, " ", "-"),
		Content:     "run task",
		Status:      db.StatusPending,
		CreatedAt:   time.Now().Add(-500 * time.Millisecond),
		Traceparent: tc.traceparent,
	}
	if err := store.InsertMessage(context.Background(), msg); err != nil {
		t.Fatalf("InsertMessage: %v", err)
	}
	lt := runLiveBurst(context.Background(), pool, msg.ThreadID, "turn-burst-1", msg.ID)
	if lt.outcome != OutcomeCompleted {
		t.Fatalf("burst turn outcome = %s (err = %v), want completed", lt.outcome, lt.err)
	}
}

func assertTraceRootParent(t *testing.T, root sdktrace.ReadOnlySpan, tc turnTraceCase) {
	t.Helper()
	if tc.wantTraceID == "" {
		if root.Parent().IsValid() {
			t.Errorf("expected root span to have invalid parent for untraced turn, got %v", root.Parent())
		}
		return
	}
	if !root.Parent().IsValid() {
		t.Fatalf("root span parent is not valid, want remote parent %s", tc.wantTraceID)
	}
	if got := root.SpanContext().TraceID().String(); got != tc.wantTraceID {
		t.Errorf("root trace ID = %s, want %s", got, tc.wantTraceID)
	}
	if got := root.Parent().TraceID().String(); got != tc.wantTraceID {
		t.Errorf("parent trace ID = %s, want %s", got, tc.wantTraceID)
	}
	if got := root.Parent().SpanID().String(); got != tc.wantParent {
		t.Errorf("parent span ID = %s, want %s", got, tc.wantParent)
	}
}

func assertTraceChildren(t *testing.T, byName map[string]sdktrace.ReadOnlySpan, root sdktrace.ReadOnlySpan) {
	t.Helper()
	for _, childName := range []string{spanQueueWait, spanLease, runner.OperationChat, "execute_tool run_command"} {
		s, ok := byName[childName]
		if !ok {
			t.Errorf("missing expected child span %q", childName)
			continue
		}
		if s.SpanContext().TraceID() != root.SpanContext().TraceID() {
			t.Errorf("child span %q trace ID = %v, want root trace ID %v", childName, s.SpanContext().TraceID(), root.SpanContext().TraceID())
		}
	}
}

func assertTraceAttributes(t *testing.T, root sdktrace.ReadOnlySpan) {
	t.Helper()
	attrs := make(map[attribute.Key]attribute.Value, len(root.Attributes()))
	for _, kv := range root.Attributes() {
		attrs[kv.Key] = kv.Value
	}
	if attrs[runner.AttrGenAIUsageInputTokens].AsInt64() != 7 {
		t.Errorf("input tokens = %v, want 7", attrs[runner.AttrGenAIUsageInputTokens])
	}
	if attrs[runner.AttrGenAIUsageOutputTokens].AsInt64() != 3 {
		t.Errorf("output tokens = %v, want 3", attrs[runner.AttrGenAIUsageOutputTokens])
	}
	if inMsgs := attrs[tracing.AttrGenAIInputMessages].AsString(); inMsgs == "" {
		t.Errorf("gen_ai.input.messages attribute is missing or empty")
	}
	if outMsgs := attrs[tracing.AttrGenAIOutputMessages].AsString(); outMsgs == "" {
		t.Errorf("gen_ai.output.messages attribute is missing or empty")
	}
}

func assertActiveTurnStatus(t *testing.T, duringSpan trace.SpanContext, root sdktrace.ReadOnlySpan, daemon string, reg *runner.ActiveTurnRegistry) {
	t.Helper()
	if !duringSpan.IsValid() || !duringSpan.Equal(root.SpanContext()) {
		t.Errorf("during turn registered span = %v, want root %v", duringSpan, root.SpanContext())
	}
	if daemon == "" {
		t.Errorf("no daemon was registered in ActiveTurnRegistry during turn")
	} else if _, ok := reg.Get(daemon); ok {
		t.Errorf("daemon %q still registered in ActiveTurnRegistry after completion", daemon)
	}
}

func TestTurnRunner_EmitsTurnTraceAndRegistersActiveTurn(t *testing.T) {
	t.Parallel()
	tests := []turnTraceCase{
		{
			name:        "traced burst",
			isDispatch:  false,
			traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			wantTraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
			wantParent:  "00f067aa0ba902b7",
		},
		{
			name:        "untraced burst",
			isDispatch:  false,
			traceparent: "",
		},
		{
			name:        "dispatch",
			isDispatch:  true,
			traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			wantTraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
			wantParent:  "00f067aa0ba902b7",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := testStore(t)
			tmpHome, tempData := t.TempDir(), t.TempDir()
			registry := runner.NewActiveTurnRegistry()
			rec := tracetest.NewSpanRecorder()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

			var onTurnMu sync.Mutex
			var duringTurnSpan trace.SpanContext
			var capturedDaemon string
			convID := "c1111111-2222-3333-4444-555555555555"

			spawner := newTurnTraceMockSpawner(convID, func() {
				onTurnMu.Lock()
				defer onTurnMu.Unlock()
				if daemon, _, ok := registry.DaemonByConversation(convID); ok {
					capturedDaemon = daemon
					var ok bool
					duringTurnSpan, ok = registry.Get(daemon)
					if !ok {
						t.Errorf("expected daemon in registry")
					}
				}
			})

			procPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
				DefaultModel: "gemini-2.5-flash",
			}, spawner)
			defer procPool.Close()

			pool := New(newTestConfig(func(d *config.ConfigData) { d.DataDir = tempData }), WorkerPoolConfig{
				ProcessPool:    procPool,
				SessionManager: session.New(tmpHome, tempData),
				Store:          store,
				TracerProvider: tp,
				ActiveTurns:    registry,
				TraceContent:   true,
			})
			defer pool.Stop()

			runTraceTurn(t, pool, store, tc, convID)

			var root sdktrace.ReadOnlySpan
			byName := make(map[string]sdktrace.ReadOnlySpan)
			for _, s := range rec.Ended() {
				if s.Name() == runner.OperationInvokeAgent && (!s.Parent().IsValid() || s.Parent().IsRemote()) {
					root = s
				} else {
					byName[s.Name()] = s
				}
			}
			if root == nil {
				t.Fatalf("ended invoke_agent root span not found in %d spans: %v", len(rec.Ended()), rec.Ended())
			}

			assertTraceRootParent(t, root, tc)
			assertTraceChildren(t, byName, root)
			assertTraceAttributes(t, root)
			assertActiveTurnStatus(t, duringTurnSpan, root, capturedDaemon, registry)
		})
	}
}

func TestTurnExecution_ChildSpanTraceParentingWithoutTurnSpan(t *testing.T) {
	t.Parallel()

	const testTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	const wantTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const wantParentSpanID = "00f067aa0ba902b7"

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))

	pool := &WorkerPool{
		cfg: WorkerPoolConfig{
			TracerProvider: tp,
		},
	}

	burst := []db.Message{
		{
			ID:          "msg-trace-test",
			ThreadID:    "thread-trace",
			Traceparent: testTraceparent,
		},
	}

	te := pool.newStepTurnExecution(context.Background(), "thread-trace", burst)
	if te.turnSpan != nil {
		t.Fatalf("expected turnSpan to be nil")
	}

	spansToTest := []string{"turn.pre_turn_hook", "turn.post_turn_hook", "turn.summary_refresh"}
	for _, spanName := range spansToTest {
		_, span := te.startChildSpan(context.Background(), spanName)
		span.End()
	}

	ended := rec.Ended()
	if len(ended) != len(spansToTest) {
		t.Fatalf("expected %d ended spans, got %d", len(spansToTest), len(ended))
	}

	for _, s := range ended {
		if s.SpanContext().TraceID().String() != wantTraceID {
			t.Errorf("span %s traceID = %s, want %s", s.Name(), s.SpanContext().TraceID().String(), wantTraceID)
		}
		if s.Parent().SpanID().String() != wantParentSpanID {
			t.Errorf("span %s parent spanID = %s, want %s", s.Name(), s.Parent().SpanID().String(), wantParentSpanID)
		}
	}
}
