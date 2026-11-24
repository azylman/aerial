package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

const (
	attemptConvID       = "11111111-1111-4111-8111-111111111111"
	attemptTransientErr = "connection reset by peer"
	attemptWatchdogErr  = "[watchdog] inactivity timeout exceeded after 5m"
	attemptQuotaErr     = "RESOURCE_EXHAUSTED: quota reached. resets in 30m."
	attemptCapacityErr  = "RESOURCE_EXHAUSTED (code 429): You have exhausted your capacity on this model. Your quota will reset after 0s."
	attemptYieldReply   = "Still waiting on the build. Terminating 1 background task(s)"
)

func TestClassifyAttempt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   attemptClassInput
		want attemptClass
		// wantReason is the timeout reason a classTimeout row records.
		wantReason string
	}{
		{name: "Success", in: attemptClassInput{Ordinal: 1, MaxAttempts: 3}, want: classSuccess},
		{name: "SuccessErrTerminal", in: attemptClassInput{SuccessErr: "agent produced empty response", Ordinal: 1, MaxAttempts: 3}, want: classTerminal},
		{name: "YieldTrapResumes", in: attemptClassInput{YieldTrap: true, AutoResumes: 1, MaxAutoResumes: 2, Ordinal: 1, MaxAttempts: 3}, want: classResume},
		{name: "YieldTrapCircuitBreakerDelivers", in: attemptClassInput{YieldTrap: true, AutoResumes: 2, MaxAutoResumes: 2, Ordinal: 1, MaxAttempts: 3}, want: classSuccess},
		{name: "ParentCancelledAborted", in: attemptClassInput{Failed: true, ErrDetail: attemptTransientErr, Transient: true, ParentCancelled: true, Ordinal: 1, MaxAttempts: 3}, want: classAborted},
		{name: "InterruptedAbortedEvenOnSuccess", in: attemptClassInput{Interrupted: true, Ordinal: 1, MaxAttempts: 3}, want: classAborted},
		{name: "InterruptedBeatsDeadline", in: attemptClassInput{Failed: true, Interrupted: true, RunDeadline: true, Ordinal: 1, MaxAttempts: 3}, want: classAborted},
		{name: "RunDeadlineTimeout", in: attemptClassInput{Failed: true, ErrDetail: "context deadline exceeded", Transient: true, RunDeadline: true, Ordinal: 1, MaxAttempts: 3}, want: classTimeout, wantReason: "turn_cap"},
		{name: "RunDeadlineBeatsWatchdog", in: attemptClassInput{Failed: true, ErrDetail: attemptWatchdogErr, Transient: true, RunDeadline: true, Ordinal: 1, MaxAttempts: 3}, want: classTimeout, wantReason: "turn_cap"},
		{name: "WatchdogTimeoutNeverRetried", in: attemptClassInput{Failed: true, ErrDetail: attemptWatchdogErr, Transient: true, Ordinal: 1, MaxAttempts: 3}, want: classTimeout, wantReason: "watchdog"},
		{name: "QuotaPause", in: attemptClassInput{Failed: true, ErrDetail: attemptQuotaErr, Transient: true, Ordinal: 1, MaxAttempts: 3}, want: classQuotaPause},
		{name: "CapacityBlipRetry", in: attemptClassInput{Failed: true, ErrDetail: attemptCapacityErr, Transient: true, Ordinal: 1, MaxAttempts: 3}, want: classCapacityRetry},
		{name: "CapacityBlipLastAttemptExhausted", in: attemptClassInput{Failed: true, ErrDetail: attemptCapacityErr, Transient: true, Ordinal: 3, MaxAttempts: 3}, want: classCapacityExhausted},
		{name: "SessionCorruptionRetry", in: attemptClassInput{Failed: true, ErrDetail: "session corrupt", SessionCorruption: true, Ordinal: 1, MaxAttempts: 3}, want: classSessionCorrupt},
		{name: "SessionCorruptionLastAttemptExhausted", in: attemptClassInput{Failed: true, ErrDetail: "session corrupt", SessionCorruption: true, Ordinal: 3, MaxAttempts: 3}, want: classExhausted},
		{name: "TransientRetry", in: attemptClassInput{Failed: true, ErrDetail: attemptTransientErr, Transient: true, Ordinal: 2, MaxAttempts: 3}, want: classRetry},
		{name: "TransientLastAttemptExhausted", in: attemptClassInput{Failed: true, ErrDetail: attemptTransientErr, Transient: true, Ordinal: 3, MaxAttempts: 3}, want: classExhausted},
		{name: "NonTransientTerminal", in: attemptClassInput{Failed: true, ErrDetail: "invalid api key", Ordinal: 1, MaxAttempts: 3}, want: classTerminal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := classifyAttempt(tc.in)
			if got != tc.want {
				t.Errorf("classifyAttempt(%+v) = %d, want %d", tc.in, got, tc.want)
			}
			if got == classTimeout {
				if reason := timeoutReason(tc.in); reason != tc.wantReason {
					t.Errorf("timeoutReason(%+v) = %q, want %q", tc.in, reason, tc.wantReason)
				}
			}
		})
	}
}

func TestRecordedOutcome(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		statuses   []string
		failedErrs []string
		want       TurnOutcome
	}{
		{name: "NoRowsEmpty", want: OutcomeEmpty},
		{name: "AnyCompleted", statuses: []string{db.StatusFailed, db.StatusCompleted}, failedErrs: []string{"boom"}, want: OutcomeCompleted},
		{name: "RequeuedReschedule", statuses: []string{db.StatusPending}, want: OutcomeReschedule},
		{name: "CancelledByOperator", statuses: []string{db.StatusFailed}, failedErrs: []string{CancelledByOperatorReason}, want: OutcomeCancelled},
		{name: "TimedOut", statuses: []string{db.StatusFailed}, failedErrs: []string{"timeout"}, want: OutcomeTimeout},
		{name: "OtherFailure", statuses: []string{db.StatusFailed}, failedErrs: []string{"timeout", "boom"}, want: OutcomeFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := recordedOutcome(tc.statuses, tc.failedErrs); got != tc.want {
				t.Errorf("recordedOutcome(%v, %v) = %s, want %s", tc.statuses, tc.failedErrs, got, tc.want)
			}
		})
	}
}

// attemptScript is one scripted agent run: a reply, an error, or an action taken mid-run.
type attemptScript struct {
	reply  string
	err    string
	action string // "deadline" (run deadline fired), "cancelParent", "stop"
}

func replyScript(text string) attemptScript { return attemptScript{reply: text} }
func errScript(msg string) attemptScript    { return attemptScript{err: msg} }

// scriptedAttemptRunner runs the scripts in order, one per agent call.
type scriptedAttemptRunner struct {
	mu      sync.Mutex
	scripts []attemptScript
	calls   int
	prompts []string
	onStop  func()
	cancel  context.CancelFunc
}

func (r *scriptedAttemptRunner) run(ctx context.Context, _, prompt, _, _, _ string, _ int) (string, string, int, error) {
	r.mu.Lock()
	idx := r.calls
	r.calls++
	r.prompts = append(r.prompts, prompt)
	r.mu.Unlock()
	if idx >= len(r.scripts) {
		return "", "", 1, errors.New("unscripted agent run")
	}
	sc := r.scripts[idx]
	switch sc.action {
	case "deadline":
		return "", "", 1, fmt.Errorf("agent run: %w", context.DeadlineExceeded)
	case "cancelParent":
		r.cancel()
		<-ctx.Done()
		return "", "", 1, errors.New("signal: killed")
	case "stop":
		r.onStop()
		<-ctx.Done()
		return "", "", 1, errors.New("signal: killed")
	}
	if sc.err != "" {
		return "", "", 1, errors.New(sc.err)
	}
	return mockJSONResponseWithUsage(attemptConvID, sc.reply, runner.TokenUsage{InputTokens: 3, OutputTokens: 2, TotalTokens: 5}), "", 0, nil
}

func (r *scriptedAttemptRunner) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func newAttemptPool(t *testing.T, store db.Store, sr *scriptedAttemptRunner, notices *noticeRecorder, noAgent bool) *WorkerPool {
	t.Helper()
	cfg := WorkerPoolConfig{
		Store: store, MaxAttempts: 3, BackoffBase: 10e6, RetryDelayOverride: 250e6,
		ResolveChannelPolicy: func(string, string) config.ChannelPolicy { return config.ChannelPolicy{Mode: "thread"} },
		RunnerFunc:           sr.run,
		DeliveryFunc:         notices.deliver,
		NotifierFunc:         func(_, _, desc string) string { return "NOTICE: " + desc },
	}
	if noAgent {
		cfg.RunnerFunc = nil
	}
	pool := New(nil, cfg)
	pool.Start()
	t.Cleanup(pool.Stop)
	return pool
}

const codeCanceled = -2

// assertAttemptErr checks an attempt's error: none, a terminal with code, context.Canceled, or
// (wantCode -1) any retryable error.
func assertAttemptErr(t *testing.T, err error, wantCode int) {
	t.Helper()
	switch {
	case wantCode == -1:
		if err == nil || restate.IsTerminalError(err) {
			t.Errorf("err = %v, want a retryable error", err)
		}
	case wantCode == codeCanceled:
		if !errors.Is(err, context.Canceled) || restate.IsTerminalError(err) {
			t.Errorf("err = %v, want retryable context.Canceled", err)
		}
	case wantCode != 0:
		if te := restate.AsTerminalError(err); te == nil || int(te.Code()) != wantCode {
			t.Errorf("err = %v, want terminal code %d", err, wantCode)
		}
	case err != nil:
		t.Errorf("err = %v, want nil", err)
	}
}

func assertNotices(t *testing.T, got []string, want []string, wantAbsent string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("deliveries = %q, want %d matching %q", got, len(want), want)
	}
	for i, sub := range want {
		if !strings.Contains(got[i], sub) {
			t.Errorf("delivery %d = %q, want it to contain %q", i, got[i], sub)
		}
	}
	for _, n := range got {
		if wantAbsent != "" && strings.Contains(n, wantAbsent) {
			t.Errorf("delivery %q contains %q", n, wantAbsent)
		}
	}
}

func TestRunBurstAttempt_GuardsAndStoreFaults(t *testing.T) {
	t.Parallel()
	fresh := []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)}
	tests := []burstGuardCase{
		{
			name: "RecordedShortCircuit", rows: []db.Message{doneMsg("a", db.StatusCompleted, "", "done")},
			wantKind: AttemptKindRecorded, wantReason: string(OutcomeCompleted), wantRows: rowA(db.StatusCompleted, "", 0, 0),
		},
		{
			name: "OrphanRecordedWhileBeginWaited", rows: fresh, orphan: true, scripts: []attemptScript{replyScript("must not run")},
			wantKind: AttemptKindRecorded, wantReason: string(OutcomeCompleted), wantRows: rowA(db.StatusCompleted, "", 0, 0),
		},
		{
			name: "TombstonedRefused", rows: fresh, preStop: true,
			wantKind: AttemptKindAborted, wantCode: CodeQueueConflict, wantReason: CancelledByOperatorReason, wantRows: rowA(db.StatusProcessing, "", 0, 0),
		},
		{
			name: "CrashLoopCapFails", rows: []db.Message{stepMsg("a", db.StatusProcessing, 1, 2)}, scripts: []attemptScript{replyScript("must not run")},
			wantKind: AttemptKindTerminal, wantReason: crashLoopReason, wantRows: rowA(db.StatusFailed, crashLoopReason, 1, 3), wantNotices: []string{"NOTICE: execution failed: crash loop"},
		},
		{
			name: "ReadErrorRetryable", rows: fresh, closeDB: true,
			wantCode: -1,
		},
		{
			name: "BeginAttemptIntegrityTerminal", rows: fresh, sabotageSQL: failTriggerSQL("messages", "UPDATE", "23505"),
			wantCode: 500, wantRows: rowA(db.StatusProcessing, "", 0, 0),
		},
		{
			name: "BeginAttemptRowVanishedSkipped", rows: fresh, sabotageSQL: skipRowTriggerSQL("messages", "UPDATE", 1), scripts: []attemptScript{replyScript("still here")},
			wantKind: AttemptKindSuccess, wantRows: rowA(db.StatusCompleted, "", 0, 0), wantNotices: []string{"still here"},
		},
		{
			name: "LostCompletedWriteRepaired", rows: fresh, sabotageSQL: faultTriggerSQL("messages", "UPDATE OF response_text", "40001", 1), scripts: []attemptScript{replyScript("kept")},
			wantKind: AttemptKindSuccess, wantRows: rowA(db.StatusCompleted, "", 1, 0), wantNotices: []string{"kept"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runBurstGuardCase(t, tc)
		})
	}
}

type burstGuardCase struct {
	name        string
	rows        []db.Message
	scripts     []attemptScript
	orphan      bool
	preStop     bool
	sabotageSQL string
	closeDB     bool
	wantKind    string
	wantReason  string
	wantCode    int
	wantRows    map[string]rowWant
	wantNotices []string
}

func runBurstGuardCase(t *testing.T, tc burstGuardCase) {
	t.Helper()
	store := openStepDB(t, stepFixture{msgs: tc.rows, sabotageSQL: tc.sabotageSQL, closeDB: tc.closeDB}).store
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	notices := &noticeRecorder{}
	sr := &scriptedAttemptRunner{scripts: tc.scripts, cancel: cancel}
	pool := newAttemptPool(t, store, sr, notices, false)
	sr.onStop = func() { pool.Turns().Stop("turn-1", "operator stop") }
	if tc.preStop {
		pool.Turns().Stop("turn-1", "operator stop")
	}
	ids := make([]string, len(tc.rows))
	for i, m := range tc.rows {
		ids[i] = m.ID
	}
	in := AttemptInput{ThreadID: "thread-steps", TurnID: "turn-1", IDs: ids, Prep: TurnPrep{Decision: TurnDecisionRun, Prompt: "answer the user"}, Ordinal: 1}
	seq := runAttemptSequence(ctx, t, pool, store, in, 1, tc.orphan)
	kinds, last, err := seq.kinds, seq.last, seq.err
	assertAttemptErr(t, err, tc.wantCode)
	wantKinds := []string{tc.wantKind}
	if strings.Join(kinds, ",") != strings.Join(wantKinds, ",") || (tc.wantReason != "" && last.Reason != tc.wantReason) {
		t.Errorf("kinds = %v (last reason %q), want %v (reason %q)", kinds, last.Reason, wantKinds, tc.wantReason)
	}
	wantCalls := 0
	if len(tc.scripts) > 0 && tc.wantKind == AttemptKindSuccess {
		wantCalls = len(tc.scripts)
	}
	if got := sr.callCount(); got != wantCalls {
		t.Errorf("agent runs = %d, want %d", got, wantCalls)
	}
	assertRows(t, store, tc.wantRows)
	assertNotices(t, notices.all(), tc.wantNotices, "")
}

func TestRunBurstAttempt(t *testing.T) {
	t.Parallel()
	quotaReason := "[QUOTA_PAUSED reset_in=30m0s scheduled=false] " + attemptQuotaErr
	fresh := []db.Message{stepMsg("a", db.StatusProcessing, 0, 0)}
	tests := []burstAttemptCase{
		{
			name: "SuccessDeliversThenCompletes", rows: []db.Message{stepMsg("a", db.StatusProcessing, 0, 0), stepMsg("b", db.StatusProcessing, 0, 0)},
			scripts: []attemptScript{replyScript("hi there")}, wantKinds: []string{AttemptKindSuccess}, wantCalls: 1, wantNotices: []string{"hi there"},
			wantRows: map[string]rowWant{"a": {status: db.StatusCompleted, retry: 1}, "b": {status: db.StatusCompleted, retry: 1}},
		},
		{
			name: "CrashReentryCountsRestart", ordinal: 2, rows: []db.Message{stepMsg("a", db.StatusProcessing, 2, 1)}, scripts: []attemptScript{replyScript("back again")},
			wantKinds: []string{AttemptKindSuccess}, wantCalls: 1, wantRows: rowA(db.StatusCompleted, "", 2, 2), wantNotices: []string{"back again"},
		},
		{
			name: "TransientTransientSuccessOverOrdinals", rows: fresh, runs: 3, scripts: []attemptScript{errScript(attemptTransientErr), errScript(attemptTransientErr), replyScript("third time")},
			wantKinds: []string{AttemptKindRetry, AttemptKindRetry, AttemptKindSuccess}, wantBackoff: []int64{10, 20, 0}, wantCalls: 3,
			wantRows: rowA(db.StatusCompleted, "", 3, 0), wantNotices: []string{"third time"},
		},
		{
			name: "TransientLastOrdinalExhausted", ordinal: 3, rows: []db.Message{stepMsg("a", db.StatusProcessing, 2, 0)}, scripts: []attemptScript{errScript(attemptTransientErr)},
			wantKinds: []string{AttemptKindTerminal}, wantReason: attemptTransientErr, wantCalls: 1,
			wantRows: rowA(db.StatusFailed, attemptTransientErr, 3, 0), wantNotices: []string{"exhausting 3 attempts"},
		},
		{
			name: "CapacityBlipRetryBackoff", rows: fresh, scripts: []attemptScript{errScript(attemptCapacityErr)},
			wantKinds: []string{AttemptKindRetry}, wantBackoff: []int64{250}, wantCalls: 1, wantRows: rowA(db.StatusProcessing, "", 1, 0),
		},
		{
			name: "CapacityBlipLastOrdinalExhausted", ordinal: 3, rows: []db.Message{stepMsg("a", db.StatusProcessing, 2, 0)}, scripts: []attemptScript{errScript(attemptCapacityErr)},
			wantKinds: []string{AttemptKindTerminal}, wantReason: attemptCapacityErr, wantCalls: 1,
			wantRows: rowA(db.StatusFailed, "[CAPACITY_EXHAUSTED reset_in=0s] "+attemptCapacityErr, 3, 0), wantNotices: []string{"capacity"},
		},
		{
			name: "YieldTrapResumesThenSucceeds", rows: fresh, runs: 2, scripts: []attemptScript{replyScript(attemptYieldReply), replyScript("all done")},
			wantKinds: []string{AttemptKindResume, AttemptKindSuccess}, wantBackoff: []int64{0, 0}, wantCalls: 2,
			wantRows: rowA(db.StatusCompleted, "", 1, 1), wantNotices: []string{"all done"},
		},
		{
			name: "WatchdogTimeout", rows: fresh, scripts: []attemptScript{errScript(attemptWatchdogErr)},
			wantKinds: []string{AttemptKindTimeout}, wantReason: attemptTimeoutReason, wantCalls: 1,
			wantRows: rowA(db.StatusFailed, attemptTimeoutReason, 1, 0), wantNotices: []string{"execution timed out"},
		},
		{
			name: "OwnRunDeadlineTimeout", rows: fresh, scripts: []attemptScript{{action: "deadline"}},
			wantKinds: []string{AttemptKindTimeout}, wantReason: attemptTimeoutReason, wantCalls: 1,
			wantRows: rowA(db.StatusFailed, attemptTimeoutReason, 1, 0), wantNotices: []string{"execution timed out"},
		},
		{
			name: "ParentCancelledAborted", rows: fresh, scripts: []attemptScript{{action: "cancelParent"}},
			wantKinds: []string{AttemptKindAborted}, wantCode: codeCanceled, wantCalls: 1, wantRows: rowA(db.StatusProcessing, "", 1, 0),
		},
		{
			name: "StopMidAttemptAborted", rows: fresh, scripts: []attemptScript{{action: "stop"}},
			wantKinds: []string{AttemptKindAborted}, wantCode: CodeQueueConflict, wantReason: "operator stop", wantCalls: 1, wantRows: rowA(db.StatusProcessing, "", 1, 0),
		},
		{
			name: "QuotaPauseTerminal", rows: fresh, scripts: []attemptScript{errScript(attemptQuotaErr)},
			wantKinds: []string{AttemptKindTerminal}, wantReason: attemptQuotaErr, wantCalls: 1,
			wantRows: rowA(db.StatusFailed, quotaReason, 1, 0), wantNotices: []string{"quota"},
		},
		{
			name: "YieldTrapCircuitBreakerDelivers", rows: fresh, autoResumes: 2, scripts: []attemptScript{replyScript(attemptYieldReply)},
			wantKinds: []string{AttemptKindSuccess}, wantCalls: 1, wantRows: rowA(db.StatusCompleted, "", 1, 0), wantNotices: []string{"Still waiting"},
		},
		{
			name: "SessionCorruptionRotatesAndRetries", rows: fresh, scripts: []attemptScript{errScript("corrupted session")},
			wantKinds: []string{AttemptKindRetry}, wantBackoff: []int64{10}, wantCalls: 1,
			wantRows: rowA(db.StatusProcessing, "", 1, 0), wantNotices: []string{"session reset"},
		},
		{
			name: "NonTransientTerminal", rows: fresh, scripts: []attemptScript{errScript("invalid api key")},
			wantKinds: []string{AttemptKindTerminal}, wantReason: "invalid api key", wantCalls: 1,
			wantRows: rowA(db.StatusFailed, "invalid api key", 1, 0), wantNotices: []string{"non-transient error"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runBurstAttemptCase(t, tc)
		})
	}
}

// burstAttemptCase is one TestRunBurstAttempt row.
type burstAttemptCase struct {
	name        string
	rows        []db.Message
	ordinal     int
	runs        int
	scripts     []attemptScript
	autoResumes int
	wantKinds   []string
	wantBackoff []int64
	wantReason  string
	wantCode    int
	wantCalls   int
	wantRows    map[string]rowWant
	wantNotices []string
}

func runBurstAttemptCase(t *testing.T, tc burstAttemptCase) {
	t.Helper()
	store := openStepDB(t, stepFixture{msgs: tc.rows}).store
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	notices := &noticeRecorder{}
	sr := &scriptedAttemptRunner{scripts: tc.scripts, cancel: cancel}
	pool := newAttemptPool(t, store, sr, notices, false)
	sr.onStop = func() { pool.Turns().Stop("turn-1", "operator stop") }
	ids := make([]string, len(tc.rows))
	for i, m := range tc.rows {
		ids[i] = m.ID
	}
	in := AttemptInput{ThreadID: "thread-steps", TurnID: "turn-1", IDs: ids, Prep: TurnPrep{Decision: TurnDecisionRun, Prompt: "answer the user"}, Ordinal: max(tc.ordinal, 1), AutoResumes: tc.autoResumes}
	seq := runAttemptSequence(ctx, t, pool, store, in, max(tc.runs, 1), false)
	kinds, backoffs, last, err := seq.kinds, seq.backoffs, seq.last, seq.err
	assertAttemptErr(t, err, tc.wantCode)
	if strings.Join(kinds, ",") != strings.Join(tc.wantKinds, ",") || (tc.wantReason != "" && last.Reason != tc.wantReason) {
		t.Errorf("kinds = %v (last reason %q), want %v (reason %q)", kinds, last.Reason, tc.wantKinds, tc.wantReason)
	}
	if tc.wantBackoff != nil && fmt.Sprint(backoffs) != fmt.Sprint(tc.wantBackoff) {
		t.Errorf("backoffs = %v, want %v", backoffs, tc.wantBackoff)
	}
	if got := sr.callCount(); got != tc.wantCalls {
		t.Errorf("agent runs = %d, want %d", got, tc.wantCalls)
	}
	assertRows(t, store, tc.wantRows)
	assertNotices(t, notices.all(), tc.wantNotices, "")
}

// skipRowTriggerSQL makes the first count firings of event on table silently skip their row (a
// BEFORE trigger returning NULL), as if the row vanished under the statement.
func skipRowTriggerSQL(table, event string, count int) string {
	name := sabotageName("skip", table, event)
	return fmt.Sprintf(`CREATE SEQUENCE %[1]s_seq;
CREATE FUNCTION %[1]s() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
	IF nextval('%[1]s_seq') <= %[4]d THEN
		RETURN NULL;
	END IF;
	RETURN NEW;
END $$;
CREATE TRIGGER %[1]s BEFORE %[3]s ON %[2]s FOR EACH ROW EXECUTE FUNCTION %[1]s();`, name, table, event, count)
}

// rowA expects the single row "a" in the given state.
func rowA(status, errText string, retry, restart int) map[string]rowWant {
	return map[string]rowWant{"a": {status: status, errText: errText, retry: retry, restart: restart}}
}

// attemptSequence is what a run of consecutive attempts returned.
type attemptSequence struct {
	kinds    []string
	backoffs []int64
	last     AttemptResult
	err      error
}

// runAttemptSequence runs up to runs attempts the way the TurnRunner chains them: a resume repeats
// the ordinal with the resume prompt, anything else moves to the next ordinal; an error stops.
func runAttemptSequence(ctx context.Context, t *testing.T, pool *WorkerPool, store db.Store, in AttemptInput, runs int, orphan bool) attemptSequence {
	t.Helper()
	var seq attemptSequence
	for i := range runs {
		in.RunIndex = i + 1
		if orphan {
			seq.last, seq.err = runBehindOrphan(ctx, t, pool, store, in)
		} else {
			seq.last, seq.err = pool.RunBurstAttempt(ctx, in)
		}
		seq.kinds, seq.backoffs = append(seq.kinds, seq.last.Kind), append(seq.backoffs, seq.last.BackoffMs)
		if seq.err != nil {
			return seq
		}
		in.Resume = seq.last.Kind == AttemptKindResume
		in.AutoResumes, in.LastError = seq.last.AutoResumes, seq.last.Reason
		if !in.Resume {
			in.Ordinal++
		}
	}
	return seq
}

// runBehindOrphan runs the attempt while an orphaned closure of the same turn holds the
// registration; the orphan records the rows COMPLETED before it releases.
func runBehindOrphan(ctx context.Context, t *testing.T, pool *WorkerPool, store db.Store, in AttemptInput) (AttemptResult, error) {
	t.Helper()
	release, ok := pool.Turns().Begin(ctx, in.TurnID, &turnExecution{})
	if !ok {
		t.Fatal("orphan Begin refused")
	}
	type out struct {
		res AttemptResult
		err error
	}
	done := make(chan out, 1)
	go func() {
		res, err := pool.RunBurstAttempt(ctx, in)
		done <- out{res, err}
	}()
	for _, id := range in.IDs {
		if err := store.UpdateMessageCompleted(ctx, id, "orphan reply", ""); err != nil {
			t.Fatal(err)
		}
	}
	release()
	o := <-done
	return o.res, o.err
}

func TestRunDispatchAttempt_GuardsAndFaults(t *testing.T) {
	t.Parallel()
	tests := []dispatchGuardCase{
		{name: "RecordedShortCircuit", status: db.StatusFailed, wantKind: AttemptKindRecorded, wantStatus: db.StatusFailed},
		{name: "LostCompletedWriteRepaired", status: db.StatusProcessing, sabotageSQL: faultTriggerSQL("dispatches", "UPDATE OF response_text", "40001", 1), scripts: []attemptScript{replyScript("chunk done")}, wantKind: AttemptKindSuccess, wantStatus: db.StatusCompleted, wantRetry: 1},
		{name: "NoAgentPoolTerminal", status: db.StatusProcessing, noAgent: true, wantKind: AttemptKindTerminal, wantStatus: db.StatusFailed, wantRetry: 1},
		{name: "ReadErrorRetryable", status: db.StatusProcessing, closeDB: true, wantCode: -1},
		{name: "NoStoreTerminal", status: db.StatusProcessing, noStore: true, wantCode: 500},
		{name: "MissingRowRecordedEmpty", wantKind: AttemptKindRecorded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDispatchGuardCase(t, tc)
		})
	}
}

type dispatchGuardCase struct {
	name        string
	status      string
	scripts     []attemptScript
	wantKind    string
	wantCode    int
	wantStatus  string
	wantRetry   int
	sabotageSQL string
	closeDB     bool
	noStore     bool
	noAgent     bool
}

func runDispatchGuardCase(t *testing.T, tc dispatchGuardCase) {
	t.Helper()
	fx := stepFixture{noStore: tc.noStore, sabotageSQL: tc.sabotageSQL, closeDB: tc.closeDB}
	if tc.status != "" {
		fx.disps = []db.Dispatch{{ID: "d1", Source: "test", Prompt: "do the thing", Status: tc.status}}
	}
	store := openStepDB(t, fx).store
	notices := &noticeRecorder{}
	sr := &scriptedAttemptRunner{scripts: tc.scripts}
	pool := newAttemptPool(t, store, sr, notices, tc.noAgent)
	got, err := pool.RunDispatchAttempt(context.Background(), AttemptInput{SessionKey: "dispatch-d1", TurnID: "turn-d", IDs: []string{"d1"}, Prep: TurnPrep{Decision: TurnDecisionRun, Prompt: "do the thing"}, Ordinal: 1})
	assertAttemptErr(t, err, tc.wantCode)
	var d *db.Dispatch
	if !tc.noStore && !tc.closeDB {
		var err error
		d, err = store.GetDispatch(context.Background(), "d1")
		if err != nil {
			t.Fatalf("GetDispatch failed: %v", err)
		}
	}
	var status string
	var retry int
	if d != nil {
		status, retry = d.Status, d.RetryCount
	}
	if got.Kind != tc.wantKind || status != tc.wantStatus || retry != tc.wantRetry {
		t.Errorf("kind = %s, dispatch = %+v; want kind %s status %s retry %d", got.Kind, d, tc.wantKind, tc.wantStatus, tc.wantRetry)
	}
	wantCalls := 0
	if len(tc.scripts) > 0 && tc.wantKind == AttemptKindSuccess {
		wantCalls = len(tc.scripts)
	}
	if c := sr.callCount(); c != wantCalls {
		t.Errorf("agent runs = %d, want %d", c, wantCalls)
	}
	if n := notices.all(); len(n) != 0 {
		t.Errorf("dispatch deliveries = %q, want none", n)
	}
}

func TestRunDispatchAttempt(t *testing.T) {
	t.Parallel()
	tests := []dispatchAttemptCase{
		{name: "SuccessCompletesSilently", status: db.StatusProcessing, scripts: []attemptScript{replyScript("chunk done")}, wantKind: AttemptKindSuccess, wantStatus: db.StatusCompleted, wantRetry: 1, wantCalls: 1},
		{name: "CrashLoopCapFails", status: db.StatusProcessing, retry: 1, restart: 2, wantKind: AttemptKindTerminal, wantStatus: db.StatusFailed, wantRetry: 1},
		{name: "WatchdogTimeoutFails", status: db.StatusProcessing, scripts: []attemptScript{errScript(attemptWatchdogErr)}, wantKind: AttemptKindTimeout, wantStatus: db.StatusFailed, wantRetry: 1, wantCalls: 1},
		{name: "StopMidAttemptAborted", status: db.StatusProcessing, scripts: []attemptScript{{action: "stop"}}, wantKind: AttemptKindAborted, wantCode: CodeQueueConflict, wantStatus: db.StatusProcessing, wantRetry: 1, wantCalls: 1},
		{name: "VanadisChunkSuccess", status: db.StatusProcessing, chunkID: "build-1-c1", scripts: []attemptScript{replyScript("chunk done")}, wantKind: AttemptKindSuccess, wantStatus: db.StatusCompleted, wantRetry: 1, wantCalls: 1},
		{name: "HeldTurnContextDoneAborted", status: db.StatusProcessing, heldTurn: true, wantCode: codeCanceled, wantKind: AttemptKindAborted, wantStatus: db.StatusProcessing},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDispatchAttemptCase(t, tc)
		})
	}
}

type dispatchAttemptCase struct {
	name       string
	status     string
	retry      int
	restart    int
	scripts    []attemptScript
	wantKind   string
	wantCode   int
	wantStatus string
	wantRetry  int
	wantCalls  int
	chunkID    string
	heldTurn   bool
}

func runDispatchAttemptCase(t *testing.T, tc dispatchAttemptCase) {
	t.Helper()
	fx := stepFixture{}
	if tc.status != "" {
		fx.disps = []db.Dispatch{{ID: "d1", Source: "test", Prompt: "do the thing", Status: tc.status, RetryCount: tc.retry, RestartCount: tc.restart, VanadisChunkID: tc.chunkID}}
	}
	store := openStepDB(t, fx).store
	notices := &noticeRecorder{}
	sr := &scriptedAttemptRunner{scripts: tc.scripts}
	pool := newAttemptPool(t, store, sr, notices, false)
	sr.onStop = func() { pool.Turns().Stop("turn-d", "operator stop") }
	ctx := context.Background()
	if tc.heldTurn {
		release, ok := pool.Turns().Begin(ctx, "turn-d", &turnExecution{})
		if !ok {
			t.Fatal("expected turn begin to succeed")
		}
		defer release()
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		ctx = cctx
	}
	got, err := pool.RunDispatchAttempt(ctx, AttemptInput{SessionKey: "dispatch-d1", TurnID: "turn-d", IDs: []string{"d1"}, Prep: TurnPrep{Decision: TurnDecisionRun, Prompt: "do the thing"}, Ordinal: 1})
	assertAttemptErr(t, err, tc.wantCode)
	d, err := store.GetDispatch(context.Background(), "d1")
	if err != nil {
		t.Fatalf("GetDispatch failed: %v", err)
	}
	var status string
	var retry int
	if d != nil {
		status, retry = d.Status, d.RetryCount
	}
	if got.Kind != tc.wantKind || status != tc.wantStatus || retry != tc.wantRetry {
		t.Errorf("kind = %s, dispatch = %+v; want kind %s status %s retry %d", got.Kind, d, tc.wantKind, tc.wantStatus, tc.wantRetry)
	}
	if c := sr.callCount(); c != tc.wantCalls {
		t.Errorf("agent runs = %d, want %d", c, tc.wantCalls)
	}
	if n := notices.all(); len(n) != 0 {
		t.Errorf("dispatch deliveries = %q, want none", n)
	}
}

func TestInspectSuccess(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		session   string
		convID    string
		reply     string
		wantErr   string
		wantYield bool
	}{
		{name: "ColdStartUnlatchedFails", convID: "not-a-uuid", reply: "hi", wantErr: "failed to latch active session UUID on cold start"},
		{name: "ColdStartLatched", convID: attemptConvID, reply: "hi"},
		{name: "WarmSessionYieldTrap", session: attemptConvID, reply: attemptYieldReply, wantYield: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			te := &turnExecution{pool: NewWorkerPool(WorkerPoolConfig{}), threadID: "thread-steps", currentSessionID: tc.session, isEphemeral: true}
			st := attemptOutcomeState{stdout: tc.reply, resp: &runner.AgyResponse{ConversationID: tc.convID, Response: tc.reply}}
			gotErr, gotYield := te.inspectSuccess(st)
			if gotErr != tc.wantErr || gotYield != tc.wantYield {
				t.Errorf("inspectSuccess = (%q, %v), want (%q, %v)", gotErr, gotYield, tc.wantErr, tc.wantYield)
			}
		})
	}
}
