package prstate

import (
	"context"
	"errors"
	"testing"
	"time"
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

func seedConflictPR(store *MemPRStore, head string, gen, epoch uint64, terminal TerminalState, conflictMark string, dispatched bool) string {
	key := PRObjectKey("aerial", 101)
	ps := NewPRState("aerial", 101)
	ps.HeadSHA = head
	ps.GateGeneration = gen
	ps.ReopenEpoch = epoch
	ps.Terminal = terminal
	if conflictMark != "" {
		ps.MarkCollaboratorConflict(conflictMark)
	}
	if dispatched {
		ps.MarkEffectDispatched(ps.EffectKeyFor(EffectGitHubConflictCheck, head, "", ""))
	}
	store.SavePR(ps)
	return key
}

func countedCheckerCalls(checker *fakeConflictChecker, nilDep bool) int {
	if nilDep {
		return 0
	}
	return len(checker.calls)
}

func countedSchedulerCalls(scheduler *fakeRescheduler, nilDep bool) int {
	if nilDep {
		return 0
	}
	return len(scheduler.calls)
}

func runConflictEffect(t *testing.T, store *MemPRStore, seedMark string, conflicted, checkerErr, schedulerErr, nilChecker, nilScheduler bool) (string, ConflictOutcome, *fakeConflictChecker, *fakeRescheduler, error) {
	t.Helper()
	ctx := context.Background()
	key := seedConflictPR(store, "abc123", 2, 0, TerminalNone, seedMark, false)
	checker := &fakeConflictChecker{conflicted: conflicted}
	if checkerErr {
		checker.err = errFakeCheck
	}
	var checkerArg ConflictChecker = checker
	if nilChecker {
		checkerArg = nil
	}
	scheduler := &fakeRescheduler{}
	if schedulerErr {
		scheduler.err = errFakeSchedule
		scheduler.failCalls = -1
	}
	var schedulerArg ConflictRescheduler = scheduler
	if nilScheduler {
		schedulerArg = nil
	}
	h := NewConflictHandlers(store, checkerArg, schedulerArg)
	ev := PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", GateGeneration: 2}
	out, err := h.HandleConflictCheck(ctx, key, ev)
	return key, out, checker, scheduler, err
}

func checkConflictOutcome(t *testing.T, out ConflictOutcome, wantDisposition EventDisposition, wantConflicted, wantAlready, wantRecheck bool) {
	t.Helper()
	if out.Disposition != wantDisposition {
		t.Fatalf("disposition = %q, want %q", out.Disposition, wantDisposition)
	}
	if out.Conflicted != wantConflicted {
		t.Fatalf("conflicted = %v, want %v", out.Conflicted, wantConflicted)
	}
	if out.AlreadyDispatched != wantAlready {
		t.Fatalf("already = %v, want %v", out.AlreadyDispatched, wantAlready)
	}
	if out.RecheckScheduled != wantRecheck {
		t.Fatalf("recheck = %v, want %v", out.RecheckScheduled, wantRecheck)
	}
}

func TestHandleConflictCheckRoutes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		seedHead       string
		seedGen        uint64
		seedEpoch      uint64
		seedTerminal   TerminalState
		seedDispatched bool
		evHead         string
		evEpoch        uint64
		missingPR      bool
		wantErr        bool
		want           EventDisposition
		wantAlready    bool
		wantCalls      int
	}{
		{name: "stale head drops without effect", seedHead: "abc123", evHead: "def456", want: DispositionDropStale},
		{name: "terminal merged absorbs", seedHead: "abc123", evHead: "abc123", seedTerminal: TerminalMerged, want: DispositionAbsorbTerminal},
		{name: "terminal closed absorbs", seedHead: "abc123", evHead: "abc123", seedTerminal: TerminalClosed, want: DispositionAbsorbTerminal},
		{name: "reopen epoch applies after terminal", seedHead: "abc123", evHead: "abc123", seedTerminal: TerminalMerged, seedEpoch: 1, evEpoch: 2, want: DispositionApply, wantCalls: 1},
		{name: "empty head drops", seedHead: "abc123", evHead: "", want: DispositionDropStale},
		{name: "zero head drops", seedHead: "abc123", evHead: "0000000", want: DispositionDropStale},
		{name: "already dispatched skips checker", seedHead: "abc123", seedGen: 2, evHead: "abc123", seedDispatched: true, want: DispositionApply, wantAlready: true},
		{name: "missing pr errors", evHead: "abc123", missingPR: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := NewMemPRStore()
			key := PRObjectKey("aerial", 101)
			if !tc.missingPR {
				seedConflictPR(store, tc.seedHead, tc.seedGen, tc.seedEpoch, tc.seedTerminal, "", tc.seedDispatched)
			}
			checker := &fakeConflictChecker{}
			h := NewConflictHandlers(store, checker, nil)
			ev := PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: tc.evHead, ReopenEpoch: tc.evEpoch, GateGeneration: tc.seedGen}
			out, err := h.HandleConflictCheck(ctx, key, ev)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleConflictCheck err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			checkConflictOutcome(t, out, tc.want, false, tc.wantAlready, false)
			if len(checker.calls) != tc.wantCalls {
				t.Fatalf("checker calls = %d, want %d", len(checker.calls), tc.wantCalls)
			}
		})
	}
}

func TestHandleConflictCheckEffects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		seedConflictMark  string
		checkerConflicted bool
		checkerErr        bool
		schedulerErr      bool
		nilChecker        bool
		nilScheduler      bool
		wantErr           bool
		wantConflicted    bool
		wantRecheck       bool
		wantConflictMark  string
		wantCheckerCalls  int
		wantSchedCalls    int
	}{
		{name: "clean check clears mark", seedConflictMark: "abc123", wantConflictMark: "", wantCheckerCalls: 1},
		{name: "conflict marks and schedules recheck", checkerConflicted: true, wantConflicted: true, wantRecheck: true, wantConflictMark: "abc123", wantCheckerCalls: 1, wantSchedCalls: 1},
		{name: "checker error schedules recheck and errors", checkerErr: true, wantErr: true, wantRecheck: true, wantCheckerCalls: 1, wantSchedCalls: 1},
		{name: "checker and scheduler both fail", checkerErr: true, schedulerErr: true, wantErr: true, wantCheckerCalls: 1, wantSchedCalls: 1},
		{name: "conflicted scheduler failure persists nothing", checkerConflicted: true, schedulerErr: true, wantErr: true, wantConflicted: true, wantConflictMark: "", wantCheckerCalls: 1, wantSchedCalls: 1},
		{name: "nil checker defaults to clean", nilChecker: true, wantConflictMark: ""},
		{name: "nil scheduler with checker error still errors", checkerErr: true, nilScheduler: true, wantErr: true, wantRecheck: true, wantCheckerCalls: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemPRStore()
			key, out, checker, scheduler, err := runConflictEffect(t, store, tc.seedConflictMark, tc.checkerConflicted, tc.checkerErr, tc.schedulerErr, tc.nilChecker, tc.nilScheduler)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleConflictCheck err = %v, wantErr %v", err, tc.wantErr)
			}
			checkConflictOutcome(t, out, DispositionApply, tc.wantConflicted, false, tc.wantRecheck)
			if got := countedCheckerCalls(checker, tc.nilChecker); got != tc.wantCheckerCalls {
				t.Fatalf("checker calls = %d, want %d", got, tc.wantCheckerCalls)
			}
			if got := countedSchedulerCalls(scheduler, tc.nilScheduler); got != tc.wantSchedCalls {
				t.Fatalf("scheduler calls = %d, want %d", got, tc.wantSchedCalls)
			}
			if !tc.wantErr && !tc.nilScheduler && tc.wantRecheck && scheduler.calls[0].delay != ConflictRecheckDelay {
				t.Fatalf("recheck delay = %v, want %v", scheduler.calls[0].delay, ConflictRecheckDelay)
			}
			ps, ok := store.LoadPR(key)
			if !ok {
				t.Fatalf("PR %q missing after check", key)
			}
			if ps.CollaboratorConflict != tc.wantConflictMark {
				t.Fatalf("conflict mark = %q, want %q", ps.CollaboratorConflict, tc.wantConflictMark)
			}
		})
	}
}

func TestHandleConflictCheckConflictedRetry(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		failCalls     int
		wantFirstErr  bool
		wantSecondErr bool
		wantMark      string
	}{
		{name: "schedule fails once then retry succeeds", failCalls: 1, wantFirstErr: true, wantMark: "abc123"},
		{name: "schedule keeps failing", failCalls: 9, wantFirstErr: true, wantSecondErr: true, wantMark: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := NewMemPRStore()
			key := seedConflictPR(store, "abc123", 2, 0, TerminalNone, "", false)
			scheduler := &fakeRescheduler{err: errFakeSchedule, failCalls: tc.failCalls}
			h := NewConflictHandlers(store, &fakeConflictChecker{conflicted: true}, scheduler)
			ev := PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", GateGeneration: 2}
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
			if ps.CollaboratorConflict != tc.wantMark {
				t.Fatalf("conflict mark = %q, want %q", ps.CollaboratorConflict, tc.wantMark)
			}
		})
	}
}
