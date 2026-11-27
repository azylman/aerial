package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

var (
	errRunnerCancel    = restate.ToTerminalError(errors.New("cancelled"), restate.WithErrorCode(CodeRestateCancelled))
	errRunnerConflict  = restate.ToTerminalError(errors.New("stale turn"), restate.WithErrorCode(CodeQueueConflict))
	errRunnerTerminal  = restate.ToTerminalError(errors.New("integrity violation"), restate.WithErrorCode(500))
	errRunnerRecovered = restate.ToTerminalError(errors.New("panic: nil map"), restate.WithErrorCode(500))
	errRunnerRetryable = errors.New("connection reset")
)

// fakeAttempt is one scripted run-<n> result; panics makes the attempt panic (a panicking classifier).
type fakeAttempt struct {
	res    AttemptResult
	err    error
	panics bool
}

// fakeTurnSteps is a declarative turnSteps: errs pops one error per call of a method, panics
// makes a method panic, attempts scripts the run steps in order.
type fakeTurnSteps struct {
	mu       sync.Mutex
	started  StartedTurn
	prep     TurnPrep
	attempts []fakeAttempt
	errs     map[string][]error
	panics   map[string]bool
	max      int

	calls    []string
	prompts  []string
	attemptN int
}

func (f *fakeTurnSteps) call(method, detail string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, strings.TrimSpace(method+" "+detail))
	if f.panics[method] {
		panic("boom in " + method)
	}
	if q := f.errs[method]; len(q) > 0 {
		f.errs[method] = q[1:]
		return q[0]
	}
	return nil
}

func (f *fakeTurnSteps) attempt(in AttemptInput) (AttemptResult, error) {
	resume := ""
	if in.Resume {
		resume = " resume"
	}
	if err := f.call("attempt", fmt.Sprintf("ord=%d run=%d%s", in.Ordinal, in.RunIndex, resume)); err != nil {
		return AttemptResult{}, err
	}
	f.mu.Lock()
	f.prompts = append(f.prompts, in.Prep.Prompt)
	if f.attemptN >= len(f.attempts) {
		f.mu.Unlock()
		return AttemptResult{}, errors.New("unscripted attempt")
	}
	a := f.attempts[f.attemptN]
	f.attemptN++
	f.mu.Unlock()
	if a.panics {
		panic("classifier exploded")
	}
	return a.res, a.err
}

func (f *fakeTurnSteps) RecordBurstStarted(context.Context, string, []string) (StartedTurn, error) {
	return f.started, f.call("started", "")
}

func (f *fakeTurnSteps) PrepareBurstTurn(context.Context, string, string, []string) (TurnPrep, error) {
	return f.prep, f.call("prep", "")
}

func (f *fakeTurnSteps) RunBurstAttempt(_ context.Context, in AttemptInput) (AttemptResult, error) {
	return f.attempt(in)
}

func (f *fakeTurnSteps) RecordBurstOutcome(_ context.Context, _ []string, s AttemptSummary) error {
	return f.call("outcome", string(s.Outcome)+" "+s.Reason)
}

func (f *fakeTurnSteps) RunBurstPostTurnHook(_ context.Context, _ string, _ []string, _ bool, o TurnOutcome, _ map[string]string) error {
	return f.call("hook", string(o))
}

func (f *fakeTurnSteps) RefreshBurstThreadSummary(context.Context, string, []string, TurnOutcome) error {
	return f.call("summary", "")
}

func (f *fakeTurnSteps) StopBurstTurn(_ context.Context, _ string, ids []string) error {
	return f.call("stop", strings.Join(ids, ","))
}

func (f *fakeTurnSteps) FailBurstTurn(_ context.Context, _ string, ids []string, reason string, _ ...map[string]string) error {
	return f.call("fail", strings.Join(ids, ",")+" "+reason)
}

func (f *fakeTurnSteps) RecordDispatchStarted(context.Context, string, string) (StartedTurn, error) {
	return f.started, f.call("started", "")
}

func (f *fakeTurnSteps) PrepareDispatchTurn(context.Context, string, string, string) (TurnPrep, error) {
	return f.prep, f.call("prep", "")
}

func (f *fakeTurnSteps) RunDispatchAttempt(_ context.Context, in AttemptInput) (AttemptResult, error) {
	return f.attempt(in)
}

func (f *fakeTurnSteps) RecordDispatchOutcome(_ context.Context, _ []string, s AttemptSummary) error {
	return f.call("outcome", string(s.Outcome)+" "+s.Reason)
}

func (f *fakeTurnSteps) PublishDispatchOutcome(_ context.Context, _ string, o TurnOutcome) error {
	return f.call("publish", string(o))
}

func (f *fakeTurnSteps) StopDispatchTurn(_ context.Context, _ string, ids []string) error {
	return f.call("stop", strings.Join(ids, ","))
}

func (f *fakeTurnSteps) FailDispatchTurn(_ context.Context, _ string, ids []string, reason string) error {
	return f.call("fail", strings.Join(ids, ",")+" "+reason)
}

func (f *fakeTurnSteps) maxAttempts() int {
	if f.max > 0 {
		return f.max
	}
	return 3
}

func (f *fakeTurnSteps) callLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

// runnerJournal is the mock Restate context of one TurnRunner invocation. It records every
// Restate command; a step named in cancelSteps (or the run future of an index in cancelRuns, or
// a Sleep when cancelSleep) observes a Restate cancel (409). Retryable step errors re-run the
// step, as Restate's default retry policy would. A cancelled run future's closure is finished
// after stop-turn (the agent killed by TurnRegistry.Stop returns its 412 late) and its result
// is dropped.
type runnerJournal struct {
	t           *testing.T
	mu          sync.Mutex
	commands    []string
	cancelSteps map[string]bool
	cancelRuns  map[int]bool
	cancelSleep bool
	runAsyncN   int
	orphans     []func(restate.RunContext) (any, error)
}

func (j *runnerJournal) record(cmd string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.commands = append(j.commands, cmd)
}

func (j *runnerJournal) log() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]string(nil), j.commands...)
}

// execute runs a step closure to its final result, retrying retryable errors.
func (j *runnerJournal) execute(name string, fn func(restate.RunContext) (any, error), output any) restate.TerminalError {
	for range 5 {
		res, err := fn(mockRunContext{Context: context.Background()})
		if err == nil {
			if output != nil && res != nil {
				setJournalValue(output, res)
			}
			return nil
		}
		if restate.IsTerminalError(err) {
			return restate.AsTerminalError(err)
		}
		j.record("retry " + name)
	}
	j.t.Fatalf("step %s kept failing retryably", name)
	return nil
}

func setJournalValue(output, res any) {
	b, err := json.Marshal(res)
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(b, output); err != nil {
		panic(err)
	}
}

func (j *runnerJournal) context(t *testing.T) restate.Context {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.EXPECT().Value(mock.Anything).Return(nil).Maybe()
	mc.EXPECT().Request().Return(&restate.Request{ID: "inv-turn-1"}).Maybe()
	mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(fn func(restate.RunContext) (any, error), output any, opts ...restate.RunOption) restate.TerminalError {
		name := runStepName(opts)
		j.record("run " + name)
		if j.cancelSteps[name] {
			return errRunnerCancel
		}
		err := j.execute(name, fn, output)
		if name == StepStopTurn {
			j.finishOrphans()
		}
		return err
	}).Maybe()
	// The handler awaits every run future right after creating it, so one shared future serves
	// the latest RunAsync call.
	var current func(restate.RunContext) (any, error)
	var currentName string
	var currentCancelled bool
	fut := mocks.NewMockRunAsyncFuture(t)
	fut.EXPECT().Result(mock.Anything).RunAndReturn(func(output any) restate.TerminalError {
		j.mu.Lock()
		fn, name, cancelled := current, currentName, currentCancelled
		if cancelled {
			j.orphans = append(j.orphans, fn)
		}
		j.mu.Unlock()
		if cancelled {
			return errRunnerCancel
		}
		return j.execute(name, fn, output)
	}).Maybe()
	mc.EXPECT().RunAsync(mock.Anything, mock.Anything).Run(func(fn func(restate.RunContext) (any, error), opts ...restate.RunOption) {
		name := runStepName(opts)
		j.record("run " + name)
		j.mu.Lock()
		j.runAsyncN++
		current, currentName, currentCancelled = fn, name, j.cancelRuns[j.runAsyncN]
		j.mu.Unlock()
	}).Return(fut).Maybe()
	mc.EXPECT().Sleep(mock.Anything).RunAndReturn(func(d time.Duration, _ ...restate.SleepOption) restate.TerminalError {
		j.record("sleep " + d.String())
		if j.cancelSleep {
			return errRunnerCancel
		}
		return nil
	}).Maybe()
	var target string
	client := mocks.NewMockClient(t)
	client.EXPECT().Send(mock.Anything).RunAndReturn(func(input any, _ ...restate.SendOption) restate.Invocation {
		payload, err := json.Marshal(input)
		require.NoError(t, err)
		j.mu.Lock()
		to := target
		j.mu.Unlock()
		j.record(fmt.Sprintf("send %s %s", to, payload))
		return mocks.NewMockInvocation(t)
	}).Maybe()
	mc.EXPECT().Object(mock.Anything, mock.Anything, mock.Anything).Run(func(service, key, method string, _ ...restate.ClientOption) {
		j.mu.Lock()
		target = service + "/" + key + "/" + method
		j.mu.Unlock()
	}).Return(client).Maybe()
	return restate.WithMockContext(mc)
}

func (j *runnerJournal) finishOrphans() {
	j.mu.Lock()
	orphans := j.orphans
	j.orphans = nil
	j.mu.Unlock()
	for _, fn := range orphans {
		_, err := fn(mockRunContext{Context: context.Background()})
		j.record(fmt.Sprintf("late run result dropped (terminal=%v)", restate.IsTerminalError(err)))
	}
}

func doneSend(service, key string, outcome TurnOutcome, delayMs int64) string {
	return fmt.Sprintf(`send %s/%s/OnTurnDone {"turn_id":"turn-1","outcome":"%s","reschedule_delay_ms":%d}`, service, key, outcome, delayMs)
}

func runPrep() TurnPrep { return TurnPrep{Decision: TurnDecisionRun, Prompt: "p"} }

func claimed(baseRetry int, ids ...string) StartedTurn {
	return StartedTurn{IDs: ids, BaseRetry: baseRetry}
}

func attemptOf(kind string) fakeAttempt { return fakeAttempt{res: AttemptResult{Kind: kind}} }

func retryAfter(ms int64) fakeAttempt {
	return fakeAttempt{res: AttemptResult{Kind: AttemptKindRetry, BackoffMs: ms, Reason: "transient"}}
}

// turnRunnerBurstCase is one RunBurst row: the scripted steps and journal, and the expected
// Restate commands and step calls.
type turnRunnerBurstCase struct {
	name        string
	agent       string
	started     StartedTurn
	prep        TurnPrep
	attempts    []fakeAttempt
	errs        map[string][]error
	panics      map[string]bool
	cancelSteps map[string]bool
	cancelRuns  map[int]bool
	cancelSleep bool
	wantOutcome TurnOutcome
	wantErr     bool
	wantCmds    []string
	wantCalls   []string
}

const runnerTS = "ThreadSession"

var turnRunnerBurstCases = []turnRunnerBurstCase{
	{
		name: "HappyPathStepOrder", started: claimed(0, "m1", "m2"), prep: runPrep(), attempts: []fakeAttempt{attemptOf(AttemptKindSuccess)},
		wantOutcome: OutcomeCompleted,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "run record-outcome", "run post-turn-hook", "run refresh-thread-summary", doneSend(runnerTS, "thread-1", OutcomeCompleted, 0)},
		wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "outcome completed", "hook completed", "summary"},
	},
	{
		name: "EmptyClaimReportsEmpty", started: StartedTurn{},
		wantOutcome: OutcomeEmpty,
		wantCmds:    []string{"run record-started", doneSend(runnerTS, "thread-1", OutcomeEmpty, 0)},
		wantCalls:   []string{"started"},
	},
	{
		name: "PrepRescheduleCarriesDelay", started: claimed(0, "m1"), prep: TurnPrep{Decision: TurnDecisionReschedule, DelayMs: 5000},
		wantOutcome: OutcomeReschedule,
		wantCmds:    []string{"run record-started", "run prep", doneSend(runnerTS, "thread-1", OutcomeReschedule, 5000)},
		wantCalls:   []string{"started", "prep"},
	},
	{
		name: "PrepFinishedRunsNoAgent", started: claimed(0, "m1"), prep: TurnPrep{Decision: TurnDecisionFinished, Reason: "ignored"},
		wantOutcome: OutcomeCompleted,
		wantCmds:    []string{"run record-started", "run prep", doneSend(runnerTS, "thread-1", OutcomeCompleted, 0)},
		wantCalls:   []string{"started", "prep"},
	},
	{
		name: "RetrySleepsBackoffThenNextOrdinal", started: claimed(0, "m1"), prep: runPrep(), attempts: []fakeAttempt{retryAfter(2000), attemptOf(AttemptKindSuccess)},
		wantOutcome: OutcomeCompleted,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "sleep 2s", "run run-2", "run record-outcome", "run post-turn-hook", "run refresh-thread-summary", doneSend(runnerTS, "thread-1", OutcomeCompleted, 0)},
		wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "attempt ord=2 run=2", "outcome completed", "hook completed", "summary"},
	},
	{
		name: "OrdinalsStartAfterPersistedRetries", started: claimed(1, "m1"), prep: runPrep(), attempts: []fakeAttempt{attemptOf(AttemptKindSuccess)},
		wantOutcome: OutcomeCompleted,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "run record-outcome", "run post-turn-hook", "run refresh-thread-summary", doneSend(runnerTS, "thread-1", OutcomeCompleted, 0)},
		wantCalls:   []string{"started", "prep", "attempt ord=2 run=1", "outcome completed", "hook completed", "summary"},
	},
	{
		name: "ResumeKeepsOrdinal", started: claimed(0, "m1"), prep: runPrep(),
		attempts:    []fakeAttempt{{res: AttemptResult{Kind: AttemptKindResume, AutoResumes: 1}}, attemptOf(AttemptKindSuccess)},
		wantOutcome: OutcomeCompleted,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "run run-2", "run record-outcome", "run post-turn-hook", "run refresh-thread-summary", doneSend(runnerTS, "thread-1", OutcomeCompleted, 0)},
		wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "attempt ord=1 run=2 resume", "outcome completed", "hook completed", "summary"},
	},
	{
		name: "TimeoutEndsAfterOneOrdinal", started: claimed(0, "m1"), prep: runPrep(),
		attempts:    []fakeAttempt{{res: AttemptResult{Kind: AttemptKindTimeout, Reason: "timeout"}}},
		wantOutcome: OutcomeTimeout,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "run record-outcome", "run post-turn-hook", doneSend(runnerTS, "thread-1", OutcomeTimeout, 0)},
		wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "outcome timeout timeout", "hook timeout"},
	},
	{
		name: "CrashLoopIsRecordedFailure", started: claimed(0, "m1"), prep: runPrep(),
		attempts:    []fakeAttempt{{res: AttemptResult{Kind: AttemptKindTerminal, Reason: "crash loop"}}},
		wantOutcome: OutcomeFailed,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "run record-outcome", "run post-turn-hook", doneSend(runnerTS, "thread-1", OutcomeFailed, 0)},
		wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "outcome failed crash loop", "hook failed"},
	},
	{
		name: "RecordedRowsReportRecordedOutcome", started: claimed(0, "m1"), prep: runPrep(),
		attempts:    []fakeAttempt{{res: AttemptResult{Kind: AttemptKindRecorded, Reason: string(OutcomeCancelled)}}},
		wantOutcome: OutcomeCancelled,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "run record-outcome", "run post-turn-hook", doneSend(runnerTS, "thread-1", OutcomeCancelled, 0)},
		wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "outcome cancelled", "hook cancelled"},
	},
	{
		name: "RetryAtLastOrdinalNeverRunsPastMax", started: claimed(2, "m1"), prep: runPrep(), attempts: []fakeAttempt{retryAfter(1000)},
		wantOutcome: OutcomeFailed,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "run record-outcome", "run post-turn-hook", doneSend(runnerTS, "thread-1", OutcomeFailed, 0)},
		wantCalls:   []string{"started", "prep", "attempt ord=3 run=1", "outcome failed transient", "hook failed"},
	},
	{
		name: "RetryableStartRetriesUnderDefaultPolicy", started: claimed(0, "m1"), prep: TurnPrep{Decision: TurnDecisionFinished},
		errs:        map[string][]error{"started": {errRunnerRetryable}},
		wantOutcome: OutcomeCompleted,
		wantCmds:    []string{"run record-started", "retry record-started", "run prep", doneSend(runnerTS, "thread-1", OutcomeCompleted, 0)},
		wantCalls:   []string{"started", "started", "prep"},
	},
	{
		name: "M2CheckRetriesUntilRowsTerminal", started: claimed(0, "m1"), prep: runPrep(), attempts: []fakeAttempt{attemptOf(AttemptKindSuccess)},
		errs:        map[string][]error{"outcome": {errRunnerRetryable}},
		wantOutcome: OutcomeCompleted,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "run record-outcome", "retry record-outcome", "run post-turn-hook", "run refresh-thread-summary", doneSend(runnerTS, "thread-1", OutcomeCompleted, 0)},
		wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "outcome completed", "outcome completed", "hook completed", "summary"},
	},
	{
		name: "CancelAtPrepStopsTurn", started: claimed(0, "m1", "m2"), cancelSteps: map[string]bool{StepPrep: true},
		wantOutcome: OutcomeCancelled,
		wantCmds:    []string{"run record-started", "run prep", "run stop-turn", doneSend(runnerTS, "thread-1", OutcomeCancelled, 0)},
		wantCalls:   []string{"started", "stop m1,m2"},
	},
	{
		name: "CancelAtRunFutureIgnoresLate412", started: claimed(0, "m1"), prep: runPrep(), cancelRuns: map[int]bool{1: true},
		attempts:    []fakeAttempt{{err: attemptInterruptedError("turn-1")}},
		wantOutcome: OutcomeCancelled,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "run stop-turn", "late run result dropped (terminal=true)", doneSend(runnerTS, "thread-1", OutcomeCancelled, 0)},
		wantCalls:   []string{"started", "prep", "stop m1,m2", "attempt ord=1 run=1"},
	},
	{
		name: "CancelAtSleepStopsTurn", started: claimed(0, "m1"), prep: runPrep(), attempts: []fakeAttempt{retryAfter(1000)}, cancelSleep: true,
		wantOutcome: OutcomeCancelled,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "sleep 1s", "run stop-turn", doneSend(runnerTS, "thread-1", OutcomeCancelled, 0)},
		wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "stop m1,m2"},
	},
	{
		name: "CancelAtOutcomeStopsTurn", started: claimed(0, "m1"), prep: runPrep(), attempts: []fakeAttempt{attemptOf(AttemptKindSuccess)}, cancelSteps: map[string]bool{StepRecordOutcome: true},
		wantOutcome: OutcomeCancelled,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "run record-outcome", "run stop-turn", doneSend(runnerTS, "thread-1", OutcomeCancelled, 0)},
		wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "stop m1,m2"},
	},
	{
		name: "StopStepFailureStillReportsCancelled", started: claimed(0, "m1"), cancelSteps: map[string]bool{StepPrep: true},
		errs:        map[string][]error{"stop": {errRunnerTerminal}},
		wantOutcome: OutcomeCancelled, wantErr: true,
		wantCmds:  []string{"run record-started", "run prep", "run stop-turn", doneSend(runnerTS, "thread-1", OutcomeCancelled, 0)},
		wantCalls: []string{"started", "stop m1,m2"},
	},
	{
		name: "TerminalStoreErrorInStartFailsTurn", started: claimed(0, "m1"), errs: map[string][]error{"started": {errRunnerTerminal}},
		wantOutcome: OutcomeFailed,
		wantCmds:    []string{"run record-started", "run fail-turn", doneSend(runnerTS, "thread-1", OutcomeFailed, 0)},
		wantCalls:   []string{"started", "fail m1,m2 integrity violation"},
	},
	{
		name: "TerminalStoreErrorInRunFailsTurn", started: claimed(0, "m1"), prep: runPrep(), attempts: []fakeAttempt{{err: errRunnerTerminal}},
		wantOutcome: OutcomeFailed,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "run fail-turn", doneSend(runnerTS, "thread-1", OutcomeFailed, 0)},
		wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "fail m1,m2 integrity violation"},
	},
	{
		name: "TerminalStoreErrorInM2CheckFailsTurn", started: claimed(0, "m1"), prep: runPrep(), attempts: []fakeAttempt{attemptOf(AttemptKindSuccess)},
		errs:        map[string][]error{"outcome": {errRunnerTerminal}},
		wantOutcome: OutcomeFailed,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "run record-outcome", "run fail-turn", doneSend(runnerTS, "thread-1", OutcomeFailed, 0)},
		wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "outcome completed", "fail m1,m2 integrity violation"},
	},
	{
		name: "ConflictFromStepFailsNotCancels", started: claimed(0, "m1"), errs: map[string][]error{"prep": {errRunnerConflict}},
		wantOutcome: OutcomeFailed,
		wantCmds:    []string{"run record-started", "run prep", "run fail-turn", doneSend(runnerTS, "thread-1", OutcomeFailed, 0)},
		wantCalls:   []string{"started", "prep", "fail m1,m2 stale turn"},
	},
	{
		name: "RecoveredPanicFailsTurn", started: claimed(0, "m1"), prep: runPrep(), attempts: []fakeAttempt{{err: errRunnerRecovered}},
		wantOutcome: OutcomeFailed,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "run fail-turn", doneSend(runnerTS, "thread-1", OutcomeFailed, 0)},
		wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "fail m1,m2 panic: nil map"},
	},
	{
		name: "PanickingClassifierFailsOnce", started: claimed(0, "m1"), prep: runPrep(), attempts: []fakeAttempt{{panics: true}},
		wantOutcome: OutcomeFailed,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "run fail-turn", doneSend(runnerTS, "thread-1", OutcomeFailed, 0)},
		wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "fail m1,m2 turn runner panic: classifier exploded"},
	},
	{
		name: "PanickingBurstAssemblerFailsOnce", started: claimed(0, "m1"), panics: map[string]bool{"prep": true},
		wantOutcome: OutcomeFailed,
		wantCmds:    []string{"run record-started", "run prep", "run fail-turn", doneSend(runnerTS, "thread-1", OutcomeFailed, 0)},
		wantCalls:   []string{"started", "prep", "fail m1,m2 turn runner panic: boom in prep"},
	},
	{
		name: "UnknownAttemptKindFailsTurn", started: claimed(0, "m1"), prep: runPrep(), attempts: []fakeAttempt{attemptOf("bogus")},
		wantOutcome: OutcomeFailed,
		wantCmds:    []string{"run record-started", "run prep", "run run-1", "run fail-turn", doneSend(runnerTS, "thread-1", OutcomeFailed, 0)},
		wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", `fail m1,m2 queue: unexpected attempt kind "bogus"`},
	},
	{
		name: "UnknownPrepDecisionFailsTurn", started: claimed(0, "m1"), prep: TurnPrep{Decision: "maybe"},
		wantOutcome: OutcomeFailed,
		wantCmds:    []string{"run record-started", "run prep", "run fail-turn", doneSend(runnerTS, "thread-1", OutcomeFailed, 0)},
		wantCalls:   []string{"started", "prep", `fail m1,m2 queue: unknown prep decision "maybe"`},
	},
	{
		name: "OnTurnDoneTargetsAgentCoordinator", agent: "agent-alpha", started: StartedTurn{},
		wantOutcome: OutcomeEmpty,
		wantCmds:    []string{"run record-started", doneSend("ThreadSession-agent-alpha", "thread-1", OutcomeEmpty, 0)},
		wantCalls:   []string{"started"},
	},
}

func TestTurnRunnerRunBurst(t *testing.T) {
	t.Parallel()
	for _, tc := range turnRunnerBurstCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			steps := &fakeTurnSteps{started: tc.started, prep: tc.prep, attempts: tc.attempts, errs: maps.Clone(tc.errs), panics: tc.panics}
			j := &runnerJournal{t: t, cancelSteps: tc.cancelSteps, cancelRuns: tc.cancelRuns, cancelSleep: tc.cancelSleep}
			svc := NewTurnRunnerService(steps, tc.agent)
			res, err := svc.RunBurst(j.context(t), BurstTurnRequest{ThreadID: "thread-1", TurnID: "turn-1", MessageIDs: []string{"m1", "m2"}})
			assert.Equal(t, tc.wantErr, err != nil, "err = %v", err)
			assert.Equal(t, TurnRunResult{TurnID: "turn-1", Outcome: tc.wantOutcome}, res)
			assert.Equal(t, tc.wantCmds, j.log())
			assert.Equal(t, tc.wantCalls, steps.callLog())
		})
	}
}

func TestTurnRunnerRunDispatch(t *testing.T) {
	t.Parallel()
	const ds = "DispatchSession"
	tests := []struct {
		name        string
		agent       string
		started     StartedTurn
		prep        TurnPrep
		attempts    []fakeAttempt
		errs        map[string][]error
		cancelSteps map[string]bool
		wantOutcome TurnOutcome
		wantCmds    []string
		wantCalls   []string
	}{
		{
			name: "HappyPathPublishesCompleted", started: claimed(0, "d1"), prep: runPrep(), attempts: []fakeAttempt{attemptOf(AttemptKindSuccess)},
			wantOutcome: OutcomeCompleted,
			wantCmds:    []string{"run record-started", "run prep", "run run-1", "run record-outcome", "run publish-dispatch-outcome", doneSend(ds, "session-1", OutcomeCompleted, 0)},
			wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "outcome completed", "publish completed"},
		},
		{
			name: "HappyPathPublishesMultipleDrainedDispatches", started: claimed(0, "d1"), prep: runPrep(),
			attempts:    []fakeAttempt{{res: AttemptResult{Kind: AttemptKindSuccess, DrainedIDs: []string{"d2", "d3"}}}},
			wantOutcome: OutcomeCompleted,
			wantCmds:    []string{"run record-started", "run prep", "run run-1", "run record-outcome", "run publish-dispatch-outcome", "run publish-dispatch-outcome-2", "run publish-dispatch-outcome-3", doneSend(ds, "session-1", OutcomeCompleted, 0)},
			wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "outcome completed", "publish completed", "publish completed", "publish completed"},
		},
		{
			name: "TimeoutPublishesTimeout", started: claimed(0, "d1"), prep: runPrep(), attempts: []fakeAttempt{{res: AttemptResult{Kind: AttemptKindTimeout, Reason: "timeout"}}},
			wantOutcome: OutcomeTimeout,
			wantCmds:    []string{"run record-started", "run prep", "run run-1", "run record-outcome", "run publish-dispatch-outcome", doneSend(ds, "session-1", OutcomeTimeout, 0)},
			wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "outcome timeout timeout", "publish timeout"},
		},
		{
			name: "RecordedCancelPublishesNothing", started: claimed(0, "d1"), prep: runPrep(), attempts: []fakeAttempt{{res: AttemptResult{Kind: AttemptKindRecorded, Reason: string(OutcomeCancelled)}}},
			wantOutcome: OutcomeCancelled,
			wantCmds:    []string{"run record-started", "run prep", "run run-1", "run record-outcome", doneSend(ds, "session-1", OutcomeCancelled, 0)},
			wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "outcome cancelled"},
		},
		{
			name: "PrepFinishedPublishesNothing", started: claimed(0, "d1"), prep: TurnPrep{Decision: TurnDecisionFinished},
			wantOutcome: OutcomeCompleted,
			wantCmds:    []string{"run record-started", "run prep", doneSend(ds, "session-1", OutcomeCompleted, 0)},
			wantCalls:   []string{"started", "prep"},
		},
		{
			name: "CancelStopsWithoutPublishing", started: claimed(0, "d1"), cancelSteps: map[string]bool{StepPrep: true},
			wantOutcome: OutcomeCancelled,
			wantCmds:    []string{"run record-started", "run prep", "run stop-turn", doneSend(ds, "session-1", OutcomeCancelled, 0)},
			wantCalls:   []string{"started", "stop d1"},
		},
		{
			name: "TerminalErrorFailsTurn", started: claimed(0, "d1"), prep: runPrep(), attempts: []fakeAttempt{{err: errRunnerTerminal}},
			wantOutcome: OutcomeFailed,
			wantCmds:    []string{"run record-started", "run prep", "run run-1", "run fail-turn", doneSend(ds, "session-1", OutcomeFailed, 0)},
			wantCalls:   []string{"started", "prep", "attempt ord=1 run=1", "fail d1 integrity violation"},
		},
		{
			name: "AgentScopedCoordinator", agent: "agent-beta", started: StartedTurn{},
			wantOutcome: OutcomeEmpty,
			wantCmds:    []string{"run record-started", doneSend("DispatchSession-agent-beta", "session-1", OutcomeEmpty, 0)},
			wantCalls:   []string{"started"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			steps := &fakeTurnSteps{started: tc.started, prep: tc.prep, attempts: tc.attempts, errs: maps.Clone(tc.errs)}
			j := &runnerJournal{t: t, cancelSteps: tc.cancelSteps}
			svc := NewTurnRunnerService(steps, tc.agent)
			res, err := svc.RunDispatch(j.context(t), DispatchTurnRequest{SessionKey: "session-1", TurnID: "turn-1", DispatchID: "d1"})
			require.NoError(t, err)
			assert.Equal(t, TurnRunResult{TurnID: "turn-1", Outcome: tc.wantOutcome}, res)
			assert.Equal(t, tc.wantCmds, j.log())
			assert.Equal(t, tc.wantCalls, steps.callLog())
		})
	}
}

func TestReflectTurnRunner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		agent    string
		wantName string
	}{
		{name: "PrimaryAgent", agent: "", wantName: "TurnRunner"},
		{name: "SiblingAgent", agent: "agent-alpha", wantName: "TurnRunner-agent-alpha"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			def := ReflectTurnRunner(NewTurnRunnerService(&fakeTurnSteps{}, tc.agent))
			assert.Equal(t, tc.wantName, def.Name())
			handlers := make([]string, 0, len(def.Handlers()))
			for name := range def.Handlers() {
				handlers = append(handlers, name)
			}
			sort.Strings(handlers)
			assert.Equal(t, []string{HandlerRunBurst, HandlerRunDispatch}, handlers)
			opts := def.GetOptions()
			require.NotNil(t, opts.InactivityTimeout)
			require.NotNil(t, opts.AbortTimeout)
			assert.Equal(t, 255*time.Minute, *opts.InactivityTimeout)
			assert.Equal(t, 255*time.Minute, *opts.AbortTimeout)
			// No retry policy: the server default retries, then PAUSES; nothing configures KILL.
			assert.Nil(t, opts.InvocationRetryPolicy)
		})
	}
}

// runStepName reads the restate.WithName option of a Run call.
func runStepName(opts []restate.RunOption) string {
	for _, opt := range opts {
		m := reflect.ValueOf(opt).MethodByName("BeforeRun")
		arg := reflect.New(m.Type().In(0).Elem())
		m.Call([]reflect.Value{arg})
		if name := arg.Elem().FieldByName("Name").String(); name != "" {
			return name
		}
	}
	return ""
}

func TestTurnRunner_RunAttempts_PropagatesDrainedPrompts(t *testing.T) {
	t.Parallel()
	steps := &fakeTurnSteps{
		started: claimed(0, "m1"),
		prep:    TurnPrep{Decision: TurnDecisionRun, Prompt: "initial prompt"},
		attempts: []fakeAttempt{
			{res: AttemptResult{Kind: AttemptKindRetry, BackoffMs: 0, Reason: "transient", DrainedPrompts: []string{"steered extra prompt"}}},
			{res: AttemptResult{Kind: AttemptKindSuccess}},
		},
	}
	j := &runnerJournal{t: t}
	svc := NewTurnRunnerService(steps, "")
	res, err := svc.RunBurst(j.context(t), BurstTurnRequest{ThreadID: "thread-1", TurnID: "turn-1", MessageIDs: []string{"m1"}})
	require.NoError(t, err)
	assert.Equal(t, OutcomeCompleted, res.Outcome)
	require.Len(t, steps.prompts, 2)
	assert.Equal(t, "initial prompt", steps.prompts[0])
	assert.Equal(t, "initial prompt\n\nsteered extra prompt", steps.prompts[1])
}

func TestTurnRunner_EndOnTerminal_IncludesDrainedIDs(t *testing.T) {
	t.Parallel()
	steps := &fakeTurnSteps{
		started: claimed(0, "m1"),
		prep:    TurnPrep{Decision: TurnDecisionRun, Prompt: "initial prompt"},
		attempts: []fakeAttempt{
			{res: AttemptResult{Kind: AttemptKindRetry, BackoffMs: 0, Reason: "transient", DrainedIDs: []string{"m2"}}},
			{err: errRunnerTerminal},
		},
	}
	j := &runnerJournal{t: t}
	svc := NewTurnRunnerService(steps, "")
	res, err := svc.RunBurst(j.context(t), BurstTurnRequest{ThreadID: "thread-1", TurnID: "turn-1", MessageIDs: []string{"m1"}})
	require.NoError(t, err)
	assert.Equal(t, OutcomeFailed, res.Outcome)
	assert.Contains(t, steps.callLog(), "fail m1,m2 integrity violation")
}
