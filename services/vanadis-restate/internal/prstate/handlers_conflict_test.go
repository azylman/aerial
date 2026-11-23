package prstate

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/prwire"
)

// Hermetic fakes for the dark per-PR conflict check: no I/O, clocks, or
// sleeps.

type conflictCall struct {
	repo     string
	prNumber int
	headSHA  string
}

type fakeConflictChecker struct {
	conflicted bool
	err        error
	calls      []conflictCall
}

func (f *fakeConflictChecker) CheckConflict(ctx context.Context, repo string, prNumber int, headSHA string) (bool, error) {
	_ = ctx
	f.calls = append(f.calls, conflictCall{repo: repo, prNumber: prNumber, headSHA: headSHA})
	return f.conflicted, f.err
}

type recheckCall struct {
	delay time.Duration
	event PREvent
}

type fakeRescheduler struct {
	err       error
	failCalls int
	calls     []recheckCall
}

func (f *fakeRescheduler) ScheduleConflictRecheck(ctx context.Context, delay time.Duration, ev PREvent) error {
	_ = ctx
	f.calls = append(f.calls, recheckCall{delay: delay, event: ev})
	if f.failCalls == 0 {
		return nil
	}
	if f.failCalls > 0 {
		f.failCalls--
	}
	if f.err != nil {
		return f.err
	}
	return errFakeSchedule
}

var (
	errFakeCheck    = errors.New("fake conflict check failed")
	errFakeSchedule = errors.New("fake recheck schedule failed")
)

func seedConflictPR(store *MemPRStore, head string, gen, epoch uint64, terminal TerminalState, dispatched bool) string {
	key := prwire.PRObjectKey("azylman/aerial", 101)
	ps := NewPRState("azylman/aerial", 101)
	ps.HeadSHA = head
	ps.GateGeneration = gen
	ps.ReopenEpoch = epoch
	ps.Terminal = terminal
	if dispatched {
		ps.MarkEffectDispatched(ps.EffectKeyFor(prwire.EffectGitHubConflictCheck, head, "", ""))
	}
	store.SavePR(ps)
	return key
}

// conflictSeed is the stored PR one HandleConflictCheck row starts from; a
// zero head seeds nothing (unknown PR).
type conflictSeed struct {
	head       string
	gen, epoch uint64
	terminal   TerminalState
	dispatched bool
}

// TestHandleConflictCheck pins the journaled conflict-check effect: stale,
// terminal and malformed heads skip the checker, a dispatched key skips it
// too, clean checks record the effect key, conflicts record it only once the
// recheck is scheduled, and checker failures schedule a backoff recheck.
func TestHandleConflictCheck(t *testing.T) {
	t.Parallel()
	rateLimited := &GitHubError{Status: 403, RateLimited: true, RetryAfter: 5 * time.Minute, Message: "rate limit"}
	openSeed := conflictSeed{head: "abc123", gen: 2}
	tests := []struct {
		name             string
		seed             conflictSeed
		evHead           string
		evEpoch          uint64
		checker          fakeConflictChecker
		sched            fakeRescheduler
		wantErr          bool
		want             ConflictOutcome
		wantCheckerCalls int
		wantDelays       []time.Duration
		wantDispatched   bool
	}{
		{name: "stale head drops without effect", seed: conflictSeed{head: "abc123"}, evHead: "def456", want: ConflictOutcome{Disposition: DispositionDropStale}},
		{name: "terminal merged absorbs", seed: conflictSeed{head: "abc123", terminal: TerminalMerged}, evHead: "abc123", want: ConflictOutcome{Disposition: DispositionAbsorbTerminal}},
		{name: "terminal closed absorbs", seed: conflictSeed{head: "abc123", terminal: TerminalClosed}, evHead: "abc123", want: ConflictOutcome{Disposition: DispositionAbsorbTerminal}},
		{
			name: "reopen epoch applies after terminal", seed: conflictSeed{head: "abc123", epoch: 1, terminal: TerminalMerged}, evHead: "abc123", evEpoch: 2,
			want: ConflictOutcome{Disposition: DispositionApply}, wantCheckerCalls: 1, wantDispatched: true,
		},
		{name: "empty head drops", seed: conflictSeed{head: "abc123"}, evHead: "", want: ConflictOutcome{Disposition: DispositionDropStale}},
		{name: "zero head drops", seed: conflictSeed{head: "abc123"}, evHead: "0000000", want: ConflictOutcome{Disposition: DispositionDropStale}},
		{
			name: "already dispatched skips checker", seed: conflictSeed{head: "abc123", gen: 2, dispatched: true}, evHead: "abc123",
			want: ConflictOutcome{Disposition: DispositionApply, AlreadyDispatched: true}, wantDispatched: true,
		},
		{name: "missing pr errors", evHead: "abc123", wantErr: true},
		{
			name: "clean check records the effect key", seed: openSeed, evHead: "abc123",
			want: ConflictOutcome{Disposition: DispositionApply}, wantCheckerCalls: 1, wantDispatched: true,
		},
		{
			name: "conflict records the effect key and schedules recheck", seed: openSeed, evHead: "abc123", checker: fakeConflictChecker{conflicted: true},
			want: ConflictOutcome{Disposition: DispositionApply, Conflicted: true, RecheckScheduled: true}, wantCheckerCalls: 1, wantDelays: []time.Duration{ConflictRecheckDelay}, wantDispatched: true,
		},
		{
			name: "checker error schedules recheck and errors", seed: openSeed, evHead: "abc123", checker: fakeConflictChecker{err: errFakeCheck}, wantErr: true,
			want: ConflictOutcome{Disposition: DispositionApply, RecheckScheduled: true}, wantCheckerCalls: 1, wantDelays: []time.Duration{ConflictRecheckDelay},
		},
		{
			name: "rate limit error schedules recheck with backoff", seed: openSeed, evHead: "abc123", checker: fakeConflictChecker{err: rateLimited}, wantErr: true,
			want: ConflictOutcome{Disposition: DispositionApply, RecheckScheduled: true}, wantCheckerCalls: 1, wantDelays: []time.Duration{5 * time.Minute},
		},
		{
			name: "checker and scheduler both fail", seed: openSeed, evHead: "abc123", checker: fakeConflictChecker{err: errFakeCheck}, sched: fakeRescheduler{failCalls: -1}, wantErr: true,
			want: ConflictOutcome{Disposition: DispositionApply}, wantCheckerCalls: 1, wantDelays: []time.Duration{ConflictRecheckDelay},
		},
		{
			name: "conflicted scheduler failure persists nothing", seed: openSeed, evHead: "abc123", checker: fakeConflictChecker{conflicted: true}, sched: fakeRescheduler{failCalls: -1}, wantErr: true,
			want: ConflictOutcome{Disposition: DispositionApply, Conflicted: true}, wantCheckerCalls: 1, wantDelays: []time.Duration{ConflictRecheckDelay},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemPRStore()
			key := prwire.PRObjectKey("azylman/aerial", 101)
			if tc.seed.head != "" {
				seedConflictPR(store, tc.seed.head, tc.seed.gen, tc.seed.epoch, tc.seed.terminal, tc.seed.dispatched)
			}
			checker, sched := tc.checker, tc.sched
			h := testConflictHandlers(t, store, &checker, &sched)
			ev := PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: tc.evHead, ReopenEpoch: tc.evEpoch, GateGeneration: tc.seed.gen}
			out, err := h.HandleConflictCheck(context.Background(), key, ev)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleConflictCheck err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.seed.head == "" {
				return
			}
			if out != tc.want {
				t.Fatalf("outcome = %+v, want %+v", out, tc.want)
			}
			if len(checker.calls) != tc.wantCheckerCalls {
				t.Fatalf("checker calls = %d, want %d", len(checker.calls), tc.wantCheckerCalls)
			}
			var delays []time.Duration
			for _, c := range sched.calls {
				delays = append(delays, c.delay)
			}
			if !reflect.DeepEqual(delays, tc.wantDelays) {
				t.Fatalf("recheck delays = %v, want %v", delays, tc.wantDelays)
			}
			ps, ok := store.LoadPR(key)
			if !ok {
				t.Fatalf("PR %q missing after check", key)
			}
			if got := ps.WasEffectDispatched(ps.EffectKeyFor(prwire.EffectGitHubConflictCheck, ps.HeadSHA, "", "")); got != tc.wantDispatched {
				t.Fatalf("conflict check key dispatched = %v, want %v", got, tc.wantDispatched)
			}
		})
	}
}

func TestHandleConflictCheckConflictedRetry(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		failCalls      int
		wantFirstErr   bool
		wantSecondErr  bool
		wantDispatched bool
	}{
		{name: "schedule fails once then retry succeeds", failCalls: 1, wantFirstErr: true, wantDispatched: true},
		{name: "schedule keeps failing", failCalls: 9, wantFirstErr: true, wantSecondErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := NewMemPRStore()
			key := seedConflictPR(store, "abc123", 2, 0, TerminalNone, false)
			scheduler := &fakeRescheduler{err: errFakeSchedule, failCalls: tc.failCalls}
			h := testConflictHandlers(t, store, &fakeConflictChecker{conflicted: true}, scheduler)
			ev := PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "abc123", GateGeneration: 2}
			first, err := h.HandleConflictCheck(ctx, key, ev)
			if (err != nil) != tc.wantFirstErr {
				t.Fatalf("first check err = %v, wantErr %v", err, tc.wantFirstErr)
			}
			if first.RecheckScheduled {
				t.Fatalf("first check scheduled a recheck despite scheduler failure")
			}
			second, err := h.HandleConflictCheck(ctx, key, ev)
			if (err != nil) != tc.wantSecondErr {
				t.Fatalf("retry check err = %v, wantErr %v", err, tc.wantSecondErr)
			}
			if !tc.wantSecondErr && (!second.RecheckScheduled || second.AlreadyDispatched || !second.Conflicted) {
				t.Fatalf("retry outcome = %+v, want scheduled recheck on a fresh attempt", second)
			}
			if len(scheduler.calls) != 2 {
				t.Fatalf("scheduler calls = %d, want 2 attempts", len(scheduler.calls))
			}
			ps, ok := store.LoadPR(key)
			if !ok {
				t.Fatalf("PR %q missing after retry", key)
			}
			if got := ps.WasEffectDispatched(ps.EffectKeyFor(prwire.EffectGitHubConflictCheck, "abc123", "", "")); got != tc.wantDispatched {
				t.Fatalf("conflict check key dispatched = %v, want %v", got, tc.wantDispatched)
			}
		})
	}
}

// TestNewConflictHandlers pins that every ConflictHandlers dependency is
// required and the error names the missing one.
func TestNewConflictHandlers(t *testing.T) {
	t.Parallel()
	store, checker, resched := NewMemPRStore(), &fakeConflictChecker{}, &fakeRescheduler{}
	tests := []struct {
		name       string
		store      PRStateStore
		conflicts  ConflictChecker
		reschedule ConflictRescheduler
		wantErr    string
	}{
		{name: "every dependency builds", store: store, conflicts: checker, reschedule: resched},
		{name: "nil store fails", conflicts: checker, reschedule: resched, wantErr: "prstate: required dependency is nil: conflict handlers PR store"},
		{name: "nil checker fails", store: store, reschedule: resched, wantErr: "prstate: required dependency is nil: conflict handlers conflict checker"},
		{name: "nil rescheduler fails", store: store, conflicts: checker, wantErr: "prstate: required dependency is nil: conflict handlers rescheduler"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, err := NewConflictHandlers(tc.store, tc.conflicts, tc.reschedule)
			gotErr := ""
			if err != nil {
				gotErr = err.Error()
			}
			if gotErr != tc.wantErr || (h != nil) != (tc.wantErr == "") {
				t.Fatalf("NewConflictHandlers() = (%v, %q), want error %q", h, gotErr, tc.wantErr)
			}
		})
	}
}
