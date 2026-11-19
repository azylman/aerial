package prstate

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"
)

// Hermetic fakes for the dark RepoObject handlers. Production Restate
// bindings (journaled steps, delayed sends) land in chunk 07; these fakes
// prove the handler contracts without I/O, clocks, or sleeps.

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

type projectionCall struct {
	repo    string
	mainSHA string
}

type fakeProjector struct {
	calls []projectionCall
}

func (f *fakeProjector) SyncMainPush(ctx context.Context, repo, mainSHA string) {
	_ = ctx
	f.calls = append(f.calls, projectionCall{repo: repo, mainSHA: mainSHA})
}

type fanoutCall struct {
	prKey   string
	mainSHA string
}

type fakeFanoutSender struct {
	err     error
	failKey string
	calls   []fanoutCall
}

func (f *fakeFanoutSender) SendMainPush(ctx context.Context, prKey, mainSHA string) error {
	_ = ctx
	f.calls = append(f.calls, fanoutCall{prKey: prKey, mainSHA: mainSHA})
	if f.failKey == "" {
		return f.err
	}
	if prKey == f.failKey {
		if f.err != nil {
			return f.err
		}
		return errFakeSend
	}
	return nil
}

var (
	errFakeCheck    = errors.New("fake conflict check failed")
	errFakeSchedule = errors.New("fake recheck schedule failed")
	errFakeSend     = errors.New("fake fanout send failed")
)

func seedMainIndex50() []string {
	out := make([]string, 0, 50)
	for i := range 50 {
		out = append(out, fmt.Sprintf("base%02d", i))
	}
	return out
}

func seedMainPushStore(repo string, index []string, updatedPRs []int) *MemRepoStore {
	store := NewMemRepoStore()
	st := NewRepoState(repo)
	for _, sha := range index {
		st.RecordMainPushSHA(sha)
	}
	if len(index) > 0 || len(updatedPRs) > 0 {
		store.SaveRepo(st)
	}
	for _, pr := range updatedPRs {
		ps := NewPRState(repo, pr)
		ps.MarkUpdatedForMain()
		store.SavePR(ps)
	}
	return store
}

func mustLoadRepoState(t *testing.T, store *MemRepoStore, repo string) *RepoState {
	t.Helper()
	st, ok := store.LoadRepo(repo)
	if !ok {
		t.Fatalf("LoadRepo(%q) missing", repo)
	}
	return st
}

func seedConflictPR(store *MemRepoStore, head string, gen, epoch uint64, terminal TerminalState, conflictMark string, dispatched bool) string {
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
		ps.MarkEffectDispatched(ps.EffectKeyFor(EffectGitHubUpdateBranch, head, "", ""))
	}
	store.SavePR(ps)
	return key
}

type pushResult struct {
	latest      string
	indexLen    int
	first       string
	projections int
}

func repoPushSummary(t *testing.T, store *MemRepoStore, repo string, projections int) pushResult {
	t.Helper()
	got := mustLoadRepoState(t, store, repo)
	first := ""
	if len(got.MainPushIndex) > 0 {
		first = got.MainPushIndex[0]
	}
	return pushResult{latest: got.LatestMainSHA, indexLen: len(got.MainPushIndex), first: first, projections: projections}
}

func repoTouchedContains(store *MemRepoStore, repo, want string) bool {
	st, ok := store.LoadRepo(repo)
	if !ok || want == "" {
		return want == ""
	}
	return slices.Contains(st.PRKeys(), want)
}

func repoAllUpdatedCleared(store *MemRepoStore, repo string, prs []int) bool {
	for _, pr := range prs {
		ps, ok := store.LoadPR(PRObjectKey(repo, pr))
		if !ok || ps.WasUpdatedForMain() {
			return false
		}
	}
	return true
}

func seedFanoutStore(repo string, keys []string, latestSHA string) *MemRepoStore {
	store := NewMemRepoStore()
	st := NewRepoState(repo)
	if latestSHA != "" {
		st.RecordMainPushSHA(latestSHA)
	}
	for _, k := range keys {
		st.TouchPRKey(k)
	}
	store.SaveRepo(st)
	return store
}

func assertFanoutCalls(t *testing.T, sender *fakeFanoutSender, wantKeys []string, wantSHA string) {
	t.Helper()
	if len(sender.calls) != len(wantKeys) {
		t.Fatalf("fanout calls = %d, want %d", len(sender.calls), len(wantKeys))
	}
	for i, want := range wantKeys {
		if sender.calls[i].prKey != want || sender.calls[i].mainSHA != wantSHA {
			t.Fatalf("fanout call %d = %+v, want key %q sha %q", i, sender.calls[i], want, wantSHA)
		}
	}
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

func runConflictEffect(t *testing.T, store *MemRepoStore, seedMark string, conflicted, checkerErr, schedulerErr, nilChecker, nilScheduler bool) (string, ConflictOutcome, *fakeConflictChecker, *fakeRescheduler, error) {
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
	h := NewRepoHandlers(store, checkerArg, schedulerArg, nil, nil)
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

func TestHandleMainPushRecords(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		repo               string
		mainSHA            string
		prNumber           int
		seedIndex          []string
		seedUpdatedPRs     []int
		nilProjector       bool
		wantLatest         string
		wantIndexLen       int
		wantIndexFirst     string
		wantTouched        string
		wantUpdatedCleared bool
		wantProjections    int
	}{
		{name: "fresh push records sha", repo: "aerial", mainSHA: "mainsha1", wantLatest: "mainsha1", wantIndexLen: 1, wantIndexFirst: "mainsha1", wantProjections: 1},
		{name: "duplicate push keeps index", repo: "aerial", mainSHA: "mainsha1", seedIndex: []string{"mainsha1"}, seedUpdatedPRs: []int{90}, wantLatest: "mainsha1", wantIndexLen: 1, wantIndexFirst: "mainsha1", wantUpdatedCleared: false, wantProjections: 1},
		{name: "fresh push clears updated flags", repo: "aerial", mainSHA: "mainsha2", seedIndex: []string{"mainsha1"}, seedUpdatedPRs: []int{90, 91}, wantLatest: "mainsha2", wantIndexLen: 2, wantIndexFirst: "mainsha1", wantUpdatedCleared: true, wantProjections: 1},
		{name: "push touches pr key", repo: "aerial", mainSHA: "mainsha1", prNumber: 101, wantLatest: "mainsha1", wantIndexLen: 1, wantIndexFirst: "mainsha1", wantTouched: "azylman/aerial#101", wantProjections: 1},
		{name: "index bounds at fifty", repo: "aerial", mainSHA: "newsha99", seedIndex: seedMainIndex50(), wantLatest: "newsha99", wantIndexLen: 50, wantIndexFirst: "base01", wantProjections: 1},
		{name: "nil projector still commits", repo: "aerial", mainSHA: "mainsha2", seedIndex: []string{"mainsha1"}, nilProjector: true, wantLatest: "mainsha2", wantIndexLen: 2, wantIndexFirst: "mainsha1", wantProjections: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := seedMainPushStore(tc.repo, tc.seedIndex, tc.seedUpdatedPRs)
			projector := &fakeProjector{}
			var projectArg RepoProjector = projector
			if tc.nilProjector {
				projectArg = nil
			}
			h := NewRepoHandlers(store, nil, nil, projectArg, nil)
			if err := h.HandleMainPush(ctx, RepoEvent{Repo: tc.repo, MainSHA: tc.mainSHA, PRNumber: tc.prNumber}); err != nil {
				t.Fatalf("HandleMainPush err = %v, want nil", err)
			}
			want := pushResult{latest: tc.wantLatest, indexLen: tc.wantIndexLen, first: tc.wantIndexFirst, projections: tc.wantProjections}
			if got := repoPushSummary(t, store, tc.repo, len(projector.calls)); got != want {
				t.Fatalf("push summary = %+v, want %+v", got, want)
			}
			if !repoTouchedContains(store, tc.repo, tc.wantTouched) {
				t.Fatalf("touched keys missing %q", tc.wantTouched)
			}
			if !tc.wantUpdatedCleared && len(tc.seedUpdatedPRs) == 0 {
				return
			}
			if cleared := repoAllUpdatedCleared(store, tc.repo, tc.seedUpdatedPRs); cleared != tc.wantUpdatedCleared {
				t.Fatalf("updated cleared = %v, want %v", cleared, tc.wantUpdatedCleared)
			}
		})
	}
}

func TestHandleMainPushRejects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		repo    string
		mainSHA string
	}{
		{name: "empty sha errors", repo: "aerial", mainSHA: ""},
		{name: "zero sha errors", repo: "aerial", mainSHA: "0000000"},
		{name: "empty repo errors", repo: "", mainSHA: "mainsha1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := NewRepoHandlers(NewMemRepoStore(), nil, nil, nil, nil)
			if err := h.HandleMainPush(context.Background(), RepoEvent{Repo: tc.repo, MainSHA: tc.mainSHA}); err == nil {
				t.Fatalf("HandleMainPush with repo %q sha %q succeeded, want error", tc.repo, tc.mainSHA)
			}
		})
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
			store := NewMemRepoStore()
			key := PRObjectKey("aerial", 101)
			if !tc.missingPR {
				seedConflictPR(store, tc.seedHead, tc.seedGen, tc.seedEpoch, tc.seedTerminal, "", tc.seedDispatched)
			}
			checker := &fakeConflictChecker{}
			h := NewRepoHandlers(store, checker, nil, nil, nil)
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
			store := NewMemRepoStore()
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
			store := NewMemRepoStore()
			key := seedConflictPR(store, "abc123", 2, 0, TerminalNone, "", false)
			scheduler := &fakeRescheduler{err: errFakeSchedule, failCalls: tc.failCalls}
			h := NewRepoHandlers(store, &fakeConflictChecker{conflicted: true}, scheduler, nil, nil)
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

func TestHandleFanoutDelivers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		repo      string
		seedKeys  []string
		latestSHA string
		nilSender bool
		failKey   string
		wantErr   bool
	}{
		{name: "fans out to touched keys", repo: "aerial", seedKeys: []string{"azylman/aerial#101", "azylman/aerial#102"}, latestSHA: "mainsha1"},
		{name: "empty index sends nothing", repo: "aerial", latestSHA: "mainsha1"},
		{name: "missing repo sends nothing", repo: "ghost"},
		{name: "nil sender drops sends silently", repo: "aerial", seedKeys: []string{"azylman/aerial#101"}, latestSHA: "mainsha1", nilSender: true},
		{name: "failing key does not starve later keys", repo: "aerial", seedKeys: []string{"azylman/aerial#101", "azylman/aerial#102", "azylman/aerial#103"}, latestSHA: "mainsha1", failKey: "azylman/aerial#102", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemRepoStore()
			if NormalizeRepo(tc.repo) != "" {
				store = seedFanoutStore(tc.repo, tc.seedKeys, tc.latestSHA)
			}
			sender := &fakeFanoutSender{failKey: tc.failKey}
			var senderArg PRFanoutSender = sender
			if tc.nilSender {
				senderArg = nil
			}
			h := NewRepoHandlers(store, nil, nil, nil, senderArg)
			if err := h.HandleFanout(context.Background(), RepoEvent{Repo: tc.repo}); (err != nil) != tc.wantErr {
				t.Fatalf("HandleFanout err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.nilSender {
				return
			}
			assertFanoutCalls(t, sender, tc.seedKeys, tc.latestSHA)
		})
	}
}

func TestHandleFanoutRejects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		repo      string
		senderErr bool
	}{
		{name: "empty repo errors", repo: ""},
		{name: "sender error propagates", repo: "aerial", senderErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemRepoStore()
			if NormalizeRepo(tc.repo) != "" {
				st := NewRepoState(tc.repo)
				st.RecordMainPushSHA("mainsha1")
				st.TouchPRKey(PRObjectKey(tc.repo, 101))
				store.SaveRepo(st)
			}
			sender := &fakeFanoutSender{}
			if tc.senderErr {
				sender.err = errFakeSend
			}
			h := NewRepoHandlers(store, nil, nil, nil, sender)
			if err := h.HandleFanout(context.Background(), RepoEvent{Repo: tc.repo}); err == nil {
				t.Fatalf("HandleFanout with repo %q succeeded, want error", tc.repo)
			}
		})
	}
}

func TestHandleKustomizationStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		repo          string
		sha           string
		generation    uint64
		seedTombstone string
		seedGen       uint64
		wantErr       bool
		wantKept      bool
		wantRecorded  bool
	}{
		{name: "records tombstone", repo: "aerial", sha: "depasha1", generation: 3, wantRecorded: true},
		{name: "evicts older generations", repo: "aerial", sha: "depasha2", generation: 3, seedTombstone: "oldsha", seedGen: 1, wantRecorded: true},
		{name: "keeps same generation", repo: "aerial", sha: "depasha2", generation: 3, seedTombstone: "peersha", seedGen: 3, wantKept: true, wantRecorded: true},
		{name: "empty sha is noop", repo: "aerial", sha: ""},
		{name: "zero sha is noop", repo: "aerial", sha: "0000000"},
		{name: "empty repo errors", repo: "", sha: "depasha1", generation: 3, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := NewMemRepoStore()
			if tc.seedTombstone != "" {
				st := NewRepoState(tc.repo)
				st.RecordTombstone(tc.seedTombstone, tc.seedGen)
				store.SaveRepo(st)
			}
			h := NewRepoHandlers(store, nil, nil, nil, nil)
			if err := h.HandleKustomizationStatus(ctx, RepoEvent{Repo: tc.repo, HeadSHA: tc.sha, Generation: tc.generation}); (err != nil) != tc.wantErr {
				t.Fatalf("HandleKustomizationStatus err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr || tc.sha == "" || IsZeroSHA(tc.sha) {
				return
			}
			got := mustLoadRepoState(t, store, tc.repo)
			if got.HasTombstone(tc.sha) != tc.wantRecorded {
				t.Fatalf("HasTombstone(%q) = %v, want recorded %v", tc.sha, got.HasTombstone(tc.sha), tc.wantRecorded)
			}
			if tc.seedTombstone != "" && got.HasTombstone(tc.seedTombstone) != tc.wantKept {
				t.Fatalf("seed tombstone kept = %v, want %v", got.HasTombstone(tc.seedTombstone), tc.wantKept)
			}
		})
	}
}

func TestResolvePRBySHAHandler(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		entries    []SHAEntry
		repo       string
		sha        string
		wantPR     int
		wantBranch string
		wantOK     bool
	}{
		{name: "resolves hit through store entries", entries: []SHAEntry{{Repo: "aerial", SHA: "abc123", PRNumber: 101, Branch: "feat-x", UpdatedAt: 7}, {Repo: "aerial", SHA: "abc123", Branch: "junk"}}, repo: "aerial", sha: "abc123", wantPR: 101, wantBranch: "feat-x", wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := NewMemRepoStore()
			st := NewRepoState("aerial")
			store.SaveRepo(st)
			for _, e := range tc.entries {
				store.AppendSHAEntry(e)
			}
			h := NewRepoHandlers(store, nil, nil, nil, nil)
			pr, branch, ok := h.ResolvePRBySHA(ctx, tc.repo, tc.sha)
			if pr != tc.wantPR || branch != tc.wantBranch || ok != tc.wantOK {
				t.Fatalf("ResolvePRBySHA = (%d,%q,%v), want (%d,%q,%v)", pr, branch, ok, tc.wantPR, tc.wantBranch, tc.wantOK)
			}
		})
	}
}
