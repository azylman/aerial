package prstate

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"
)

// Hermetic fakes for RepoHandlers: no I/O, clocks, or sleeps. The Restate
// bindings for these seams are proven in service_test.go.

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

type sentPR struct {
	handler string
	ev      PREvent
}

// fakePRSender records every send; sends to failPR return errFakeSend.
type fakePRSender struct {
	failPR int
	calls  []sentPR
}

func (f *fakePRSender) SendPR(ctx context.Context, handler string, ev PREvent) error {
	_ = ctx
	f.calls = append(f.calls, sentPR{handler: handler, ev: ev})
	if f.failPR != 0 && ev.PRNumber == f.failPR {
		return errFakeSend
	}
	return nil
}

type selfCall struct {
	kind  string
	repo  string
	sha   string
	id    uint64
	delay time.Duration
}

// fakeSelfScheduler records every delayed self-send; fail makes each fail.
type fakeSelfScheduler struct {
	fail  bool
	calls []selfCall
}

func (f *fakeSelfScheduler) ScheduleHeldReplay(ctx context.Context, repo, sha string, delay time.Duration) error {
	_ = ctx
	f.calls = append(f.calls, selfCall{kind: "replay", repo: repo, sha: sha, delay: delay})
	if f.fail {
		return errFakeSchedule
	}
	return nil
}

func (f *fakeSelfScheduler) ScheduleHeldExpiry(ctx context.Context, repo string, id uint64, delay time.Duration) error {
	_ = ctx
	f.calls = append(f.calls, selfCall{kind: "expiry", repo: repo, id: id, delay: delay})
	if f.fail {
		return errFakeSchedule
	}
	return nil
}

var (
	errFakeSend = errors.New("fake pr send failed")
	errFakeLoad = errors.New("fake repo load failed")
)

// repoSeed describes the RepoState a row starts from; the zero value means
// no state was ever saved.
type repoSeed struct {
	index      []PRIndexEvent
	mains      []string
	held       []SHAEvent
	tombstones map[string]uint64
}

func (s repoSeed) empty() bool {
	return len(s.index) == 0 && len(s.mains) == 0 && len(s.held) == 0 && len(s.tombstones) == 0
}

func seedRepoStore(seed repoSeed, loadErr bool) *MemRepoStore {
	store := NewMemRepoStore()
	if !seed.empty() {
		st := NewRepoState("aerial")
		for _, sha := range seed.mains {
			st.RecordMainPushSHA(sha)
		}
		for _, ev := range seed.held {
			st.HoldSHAEvent(SHAKindCheckRun, ev)
		}
		for _, ev := range seed.index {
			st.ApplyPRIndex(ev)
		}
		for sha, gen := range seed.tombstones {
			st.RecordTombstone(sha, gen)
		}
		store.SaveRepo(st)
	}
	if loadErr {
		store.FailLoads(errFakeLoad)
	}
	return store
}

func loadRepoForTest(t *testing.T, store *MemRepoStore) *RepoState {
	t.Helper()
	st, err := store.LoadRepo("aerial")
	if err != nil {
		t.Fatalf("LoadRepo err = %v", err)
	}
	return st
}

func mainAdvanced(pr int, owner EffectsOwner) sentPR {
	return sentPR{handler: PRHandlerMainAdvanced, ev: PREvent{Event: "main_advanced", Repo: "azylman/aerial", PRNumber: pr, EffectsOwner: owner}}
}

func rolloutStatus(pr int, branch string, gen uint64) sentPR {
	return sentPR{handler: PRHandlerRolloutProgress, ev: PREvent{Event: "kustomization_status", Repo: "azylman/aerial", PRNumber: pr, Branch: branch, GateGeneration: gen, EffectsOwner: EffectsOwnerRestate, Kustomization: "eri"}}
}

func heldCheck(sha string) SHAEvent {
	return SHAEvent{Repo: "aerial", SHA: sha, Handler: PRHandlerCheckFailed, Event: PREvent{Event: "check_failed", CheckName: "ci"}}
}

func checkErr(t *testing.T, err error, wantErr bool, wantInvalid bool) {
	t.Helper()
	if (err != nil) != wantErr {
		t.Fatalf("err = %v, wantErr %v", err, wantErr)
	}
	if errors.Is(err, ErrInvalidRepoEvent) != wantInvalid {
		t.Fatalf("errors.Is(err, ErrInvalidRepoEvent) = %v, want %v (err %v)", errors.Is(err, ErrInvalidRepoEvent), wantInvalid, err)
	}
}

func assertSent(t *testing.T, got []sentPR, want []sentPR) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sent = %+v, want %+v", got, want)
	}
}

func assertSelf(t *testing.T, got []selfCall, want []selfCall) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("self sends = %+v, want %+v", got, want)
	}
}

func seedMainIndex50() []string {
	out := make([]string, 0, 50)
	for i := range 50 {
		out = append(out, fmt.Sprintf("base%02d", i))
	}
	return out
}

func TestRepoHandlers_HandleMainPush(t *testing.T) {
	t.Parallel()
	openTwoClosedOne := []PRIndexEvent{idxActive(101, "h1", "", 0), idxActive(102, "h2", "", 0), idxActive(103, "h3", "", 0), idxClosed(103, "h3")}
	tests := []struct {
		name            string
		seed            repoSeed
		loadErr         bool
		nilProjector    bool
		failPR          int
		ev              RepoEvent
		wantErr         bool
		wantInvalid     bool
		wantLatest      string
		wantIndexLen    int
		wantIndexFirst  string
		wantTouched     []string
		wantSent        []sentPR
		wantProjections []projectionCall
	}{
		{name: "fresh push records sha", ev: RepoEvent{Repo: "aerial", MainSHA: "MainSHA1"}, wantLatest: "mainsha1", wantIndexLen: 1, wantIndexFirst: "mainsha1", wantProjections: []projectionCall{{repo: "azylman/aerial", mainSHA: "mainsha1"}}},
		{name: "new sha fans out to open prs only", seed: repoSeed{mains: []string{"mainsha1"}, index: openTwoClosedOne}, ev: RepoEvent{Repo: "aerial", MainSHA: "mainsha2", EffectsOwner: EffectsOwnerRestate}, wantLatest: "mainsha2", wantIndexLen: 2, wantIndexFirst: "mainsha1", wantTouched: []string{"azylman/aerial#101", "azylman/aerial#102", "azylman/aerial#103"}, wantSent: []sentPR{mainAdvanced(101, EffectsOwnerRestate), mainAdvanced(102, EffectsOwnerRestate)}, wantProjections: []projectionCall{{repo: "azylman/aerial", mainSHA: "mainsha2"}}},
		{name: "duplicate sha sends and projects nothing", seed: repoSeed{mains: []string{"mainsha1"}, index: openTwoClosedOne}, ev: RepoEvent{Repo: "aerial", MainSHA: "mainsha1"}, wantLatest: "mainsha1", wantIndexLen: 1, wantIndexFirst: "mainsha1", wantTouched: []string{"azylman/aerial#101", "azylman/aerial#102", "azylman/aerial#103"}},
		{name: "duplicate sha still records a new pr key", seed: repoSeed{mains: []string{"mainsha1"}}, ev: RepoEvent{Repo: "aerial", MainSHA: "mainsha1", PRNumber: 104}, wantLatest: "mainsha1", wantIndexLen: 1, wantIndexFirst: "mainsha1", wantTouched: []string{"azylman/aerial#104"}},
		{name: "index bounds at fifty", seed: repoSeed{mains: seedMainIndex50()}, ev: RepoEvent{Repo: "aerial", MainSHA: "newsha99"}, wantLatest: "newsha99", wantIndexLen: 50, wantIndexFirst: "base01", wantProjections: []projectionCall{{repo: "azylman/aerial", mainSHA: "newsha99"}}},
		{name: "nil projector still commits", nilProjector: true, ev: RepoEvent{Repo: "aerial", MainSHA: "mainsha1"}, wantLatest: "mainsha1", wantIndexLen: 1, wantIndexFirst: "mainsha1"},
		{name: "failing send never starves later prs", seed: repoSeed{mains: []string{"mainsha1"}, index: openTwoClosedOne}, failPR: 101, ev: RepoEvent{Repo: "aerial", MainSHA: "mainsha2"}, wantErr: true, wantLatest: "mainsha2", wantIndexLen: 2, wantIndexFirst: "mainsha1", wantTouched: []string{"azylman/aerial#101", "azylman/aerial#102", "azylman/aerial#103"}, wantSent: []sentPR{mainAdvanced(101, EffectsOwnerLegacy), mainAdvanced(102, EffectsOwnerLegacy)}, wantProjections: []projectionCall{{repo: "azylman/aerial", mainSHA: "mainsha2"}}},
		{name: "load error surfaces", loadErr: true, ev: RepoEvent{Repo: "aerial", MainSHA: "mainsha1"}, wantErr: true},
		{name: "empty sha is invalid", ev: RepoEvent{Repo: "aerial"}, wantErr: true, wantInvalid: true},
		{name: "zero sha is invalid", ev: RepoEvent{Repo: "aerial", MainSHA: "0000000"}, wantErr: true, wantInvalid: true},
		{name: "empty repo is invalid", ev: RepoEvent{MainSHA: "mainsha1"}, wantErr: true, wantInvalid: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := seedRepoStore(tc.seed, tc.loadErr)
			projector := &fakeProjector{}
			var projectArg RepoProjector = projector
			if tc.nilProjector {
				projectArg = nil
			}
			sender := &fakePRSender{failPR: tc.failPR}
			err := NewRepoHandlers(store, projectArg, sender, nil).HandleMainPush(context.Background(), tc.ev)
			checkErr(t, err, tc.wantErr, tc.wantInvalid)
			assertSent(t, sender.calls, tc.wantSent)
			if !reflect.DeepEqual(projector.calls, tc.wantProjections) {
				t.Fatalf("projections = %+v, want %+v", projector.calls, tc.wantProjections)
			}
			if tc.wantLatest == "" {
				return
			}
			st := loadRepoForTest(t, store)
			if st.LatestMainSHA != tc.wantLatest || len(st.MainPushIndex) != tc.wantIndexLen || st.MainPushIndex[0] != tc.wantIndexFirst {
				t.Fatalf("index = (%q,%d,%q), want (%q,%d,%q)", st.LatestMainSHA, len(st.MainPushIndex), st.MainPushIndex[0], tc.wantLatest, tc.wantIndexLen, tc.wantIndexFirst)
			}
			if !slices.Equal(st.PRKeys(), tc.wantTouched) {
				t.Fatalf("touched = %v, want %v", st.PRKeys(), tc.wantTouched)
			}
		})
	}
}

func TestRepoHandlers_HandleFanout(t *testing.T) {
	t.Parallel()
	openTwo := []PRIndexEvent{idxActive(101, "h1", "", 0), idxActive(102, "h2", "", 0)}
	tests := []struct {
		name        string
		seed        repoSeed
		loadErr     bool
		failPR      int
		ev          RepoEvent
		wantErr     bool
		wantInvalid bool
		wantSent    []sentPR
	}{
		{name: "fans out latest main to open prs", seed: repoSeed{mains: []string{"mainsha1"}, index: openTwo}, ev: RepoEvent{Repo: "aerial", EffectsOwner: EffectsOwnerRestate}, wantSent: []sentPR{mainAdvanced(101, EffectsOwnerRestate), mainAdvanced(102, EffectsOwnerRestate)}},
		{name: "no main push sends nothing", seed: repoSeed{index: openTwo}, ev: RepoEvent{Repo: "aerial"}},
		{name: "missing repo state sends nothing", ev: RepoEvent{Repo: "aerial"}},
		{name: "failing pr never starves later prs", seed: repoSeed{mains: []string{"mainsha1"}, index: openTwo}, failPR: 101, ev: RepoEvent{Repo: "aerial"}, wantErr: true, wantSent: []sentPR{mainAdvanced(101, EffectsOwnerLegacy), mainAdvanced(102, EffectsOwnerLegacy)}},
		{name: "load error surfaces", loadErr: true, ev: RepoEvent{Repo: "aerial"}, wantErr: true},
		{name: "empty repo is invalid", ev: RepoEvent{}, wantErr: true, wantInvalid: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sender := &fakePRSender{failPR: tc.failPR}
			err := NewRepoHandlers(seedRepoStore(tc.seed, tc.loadErr), nil, sender, nil).HandleFanout(context.Background(), tc.ev)
			checkErr(t, err, tc.wantErr, tc.wantInvalid)
			assertSent(t, sender.calls, tc.wantSent)
		})
	}
}

func TestRepoHandlers_HandleKustomizationStatus(t *testing.T) {
	t.Parallel()
	deployingTwo := []PRIndexEvent{
		idxActive(101, "h1", "feat-a", 0), idxMerged(101, "h1", "m1"), idxDeploying(101, "m1", 2),
		idxActive(102, "h2", "feat-b", 0), idxMerged(102, "h2", "m2"), idxDeploying(102, "m2", 1),
		idxActive(103, "h3", "feat-c", 0), idxMerged(103, "h3", "m3"),
	}
	tests := []struct {
		name           string
		seed           repoSeed
		loadErr        bool
		failPR         int
		ev             RepoEvent
		wantErr        bool
		wantInvalid    bool
		wantSent       []sentPR
		wantStored     bool
		wantTombstones map[string]uint64
	}{
		{name: "records tombstone without deploying prs", ev: RepoEvent{Repo: "aerial", HeadSHA: "depsha1", Generation: 3}, wantStored: true, wantTombstones: map[string]uint64{"depsha1": 3}},
		{name: "evicts older generations", seed: repoSeed{tombstones: map[string]uint64{"oldsha": 1, "peersha": 3}}, ev: RepoEvent{Repo: "aerial", HeadSHA: "depsha2", Generation: 3}, wantStored: true, wantTombstones: map[string]uint64{"peersha": 3, "depsha2": 3}},
		{name: "forwards status to every deploying pr", seed: repoSeed{index: deployingTwo}, ev: RepoEvent{Repo: "aerial", Kustomization: " eri ", EffectsOwner: EffectsOwnerRestate}, wantSent: []sentPR{rolloutStatus(101, "feat-a", 2), rolloutStatus(102, "feat-b", 1)}, wantStored: true, wantTombstones: map[string]uint64{}},
		{name: "status without sha persists nothing", ev: RepoEvent{Repo: "aerial", Generation: 3}},
		{name: "zero sha persists nothing", ev: RepoEvent{Repo: "aerial", HeadSHA: "0000000", Generation: 3}},
		{name: "failing pr never starves later prs", seed: repoSeed{index: deployingTwo}, failPR: 101, ev: RepoEvent{Repo: "aerial", Kustomization: "eri", EffectsOwner: EffectsOwnerRestate}, wantErr: true, wantSent: []sentPR{rolloutStatus(101, "feat-a", 2), rolloutStatus(102, "feat-b", 1)}, wantStored: true, wantTombstones: map[string]uint64{}},
		{name: "load error surfaces", loadErr: true, ev: RepoEvent{Repo: "aerial", HeadSHA: "depsha1"}, wantErr: true},
		{name: "empty repo is invalid", ev: RepoEvent{HeadSHA: "depsha1", Generation: 3}, wantErr: true, wantInvalid: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := seedRepoStore(tc.seed, tc.loadErr)
			sender := &fakePRSender{failPR: tc.failPR}
			err := NewRepoHandlers(store, nil, sender, nil).HandleKustomizationStatus(context.Background(), tc.ev)
			checkErr(t, err, tc.wantErr, tc.wantInvalid)
			assertSent(t, sender.calls, tc.wantSent)
			if tc.loadErr {
				return
			}
			st := loadRepoForTest(t, store)
			if (st != nil) != tc.wantStored {
				t.Fatalf("stored = %v, want %v", st != nil, tc.wantStored)
			}
			if st != nil && !reflect.DeepEqual(st.Tombstones, tc.wantTombstones) {
				t.Fatalf("tombstones = %v, want %v", st.Tombstones, tc.wantTombstones)
			}
		})
	}
}

func TestRepoHandlers_HandlePRIndex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		seed        repoSeed
		loadErr     bool
		failSched   bool
		ev          PRIndexEvent
		probeSHA    string
		wantErr     bool
		wantInvalid bool
		wantPR      int
		wantSelf    []selfCall
	}{
		{name: "indexes head of a new pr", ev: idxActive(101, "h1", "feat", 0), probeSHA: "h1", wantPR: 101},
		{name: "held merge sha schedules a delayed replay", seed: repoSeed{held: []SHAEvent{heldCheck("m9")}}, ev: idxMerged(101, "h1", "M9"), probeSHA: "m9", wantPR: 101, wantSelf: []selfCall{{kind: "replay", repo: "azylman/aerial", sha: "m9", delay: time.Second}}},
		{name: "unchanged update keeps the index", seed: repoSeed{index: []PRIndexEvent{idxActive(101, "h1", "feat", 0)}}, ev: idxActive(101, "h1", "feat", 0), probeSHA: "h1", wantPR: 101},
		{name: "replay schedule failure persists nothing", seed: repoSeed{held: []SHAEvent{heldCheck("m9")}}, failSched: true, ev: idxMerged(101, "h1", "m9"), probeSHA: "m9", wantErr: true, wantSelf: []selfCall{{kind: "replay", repo: "azylman/aerial", sha: "m9", delay: time.Second}}},
		{name: "load error surfaces", loadErr: true, ev: idxActive(101, "h1", "feat", 0), wantErr: true},
		{name: "missing pr number is invalid", ev: idxActive(0, "h1", "feat", 0), wantErr: true, wantInvalid: true},
		{name: "empty repo is invalid", ev: PRIndexEvent{PRNumber: 101, HeadSHA: "h1"}, wantErr: true, wantInvalid: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := seedRepoStore(tc.seed, tc.loadErr)
			self := &fakeSelfScheduler{fail: tc.failSched}
			err := NewRepoHandlers(store, nil, nil, self).HandlePRIndex(context.Background(), tc.ev)
			checkErr(t, err, tc.wantErr, tc.wantInvalid)
			assertSelf(t, self.calls, tc.wantSelf)
			if tc.loadErr {
				return
			}
			pr, _, _ := NewRepoHandlers(store, nil, nil, nil).ResolvePRBySHA(context.Background(), "aerial", tc.probeSHA)
			if pr != tc.wantPR {
				t.Fatalf("ResolvePRBySHA(%q) = %d, want %d", tc.probeSHA, pr, tc.wantPR)
			}
		})
	}
}

func heldFill(n int) []SHAEvent {
	out := make([]SHAEvent, 0, n)
	for i := 1; i <= n; i++ {
		out = append(out, heldCheck(fmt.Sprintf("q%02d", i)))
	}
	return out
}

func TestRepoHandlers_RouteBySHA(t *testing.T) {
	t.Parallel()
	indexed := []PRIndexEvent{idxActive(101, "abc123", "feat-a", 0)}
	tests := []struct {
		name           string
		seed           repoSeed
		loadErr        bool
		failPR         int
		failSched      bool
		kind           string
		ev             SHAEvent
		wantErr        bool
		wantInvalid    bool
		wantForwarded  bool
		wantPR         int
		wantHeldID     uint64
		wantEvictedIDs []uint64
		wantSent       []sentPR
		wantSelf       []selfCall
		wantHeld       []string
	}{
		{name: "check run hit forwards with pr branch and head", seed: repoSeed{index: indexed}, kind: SHAKindCheckRun, ev: SHAEvent{Repo: "aerial", SHA: "ABC123", Handler: PRHandlerCheckFailed, Event: PREvent{Event: "check_failed", CheckName: "ci", EffectsOwner: EffectsOwnerRestate}}, wantForwarded: true, wantPR: 101, wantSent: []sentPR{{handler: PRHandlerCheckFailed, ev: PREvent{Event: "check_failed", Repo: "azylman/aerial", PRNumber: 101, Branch: "feat-a", HeadSHA: "abc123", CheckName: "ci", EffectsOwner: EffectsOwnerRestate}}}},
		{name: "workflow run hit keeps event branch and head", seed: repoSeed{index: indexed}, kind: SHAKindWorkflowRun, ev: SHAEvent{Repo: "aerial", SHA: "abc123", Handler: PRHandlerCheckPassed, Event: PREvent{Event: "check_passed", Branch: "own", HeadSHA: "abc123"}}, wantForwarded: true, wantPR: 101, wantSent: []sentPR{{handler: PRHandlerCheckPassed, ev: PREvent{Event: "check_passed", Repo: "azylman/aerial", PRNumber: 101, Branch: "own", HeadSHA: "abc123", EffectsOwner: EffectsOwnerLegacy}}}},
		{name: "push hit fills merge sha", seed: repoSeed{index: []PRIndexEvent{idxActive(101, "h1", "feat-a", 0), idxMerged(101, "h1", "m1")}}, kind: SHAKindPush, ev: SHAEvent{Repo: "aerial", SHA: "m1", Handler: PRHandlerMerged, Event: PREvent{Event: "merged"}}, wantForwarded: true, wantPR: 101, wantSent: []sentPR{{handler: PRHandlerMerged, ev: PREvent{Event: "merged", Repo: "azylman/aerial", PRNumber: 101, Branch: "feat-a", MergeSHA: "m1", EffectsOwner: EffectsOwnerLegacy}}}},
		{name: "miss holds and schedules expiry", kind: SHAKindImageReady, ev: SHAEvent{Repo: "aerial", SHA: "m2", Handler: PRHandlerRolloutProgress}, wantHeldID: 1, wantSelf: []selfCall{{kind: "expiry", repo: "azylman/aerial", id: 1, delay: 30 * time.Minute}}, wantHeld: []string{"m2"}},
		{name: "miss at capacity evicts oldest held", seed: repoSeed{held: heldFill(64)}, kind: SHAKindPush, ev: SHAEvent{Repo: "aerial", SHA: "m2", Handler: PRHandlerMerged}, wantHeldID: 65, wantEvictedIDs: []uint64{1}, wantSelf: []selfCall{{kind: "expiry", repo: "azylman/aerial", id: 65, delay: 30 * time.Minute}}, wantHeld: append(numberedSHAs(64)[1:], "m2")},
		{name: "expiry schedule failure persists nothing", failSched: true, kind: SHAKindCheckRun, ev: heldCheck("m2"), wantErr: true, wantSelf: []selfCall{{kind: "expiry", repo: "azylman/aerial", id: 1, delay: 30 * time.Minute}}},
		{name: "forward failure surfaces", seed: repoSeed{index: indexed}, failPR: 101, kind: SHAKindCheckRun, ev: heldCheck("abc123"), wantErr: true, wantSent: []sentPR{{handler: PRHandlerCheckFailed, ev: PREvent{Event: "check_failed", Repo: "azylman/aerial", PRNumber: 101, Branch: "feat-a", HeadSHA: "abc123", CheckName: "ci", EffectsOwner: EffectsOwnerLegacy}}}},
		{name: "load error surfaces", loadErr: true, kind: SHAKindCheckRun, ev: heldCheck("abc123"), wantErr: true},
		{name: "verdict handler is not routable", kind: SHAKindCheckRun, ev: SHAEvent{Repo: "aerial", SHA: "abc123", Handler: "HandleCIRecovery"}, wantErr: true, wantInvalid: true},
		{name: "handler outside the kind allowlist is invalid", kind: SHAKindImageReady, ev: SHAEvent{Repo: "aerial", SHA: "abc123", Handler: PRHandlerCheckFailed}, wantErr: true, wantInvalid: true},
		{name: "zero sha is invalid", kind: SHAKindCheckRun, ev: heldCheck("0000000"), wantErr: true, wantInvalid: true},
		{name: "empty repo is invalid", kind: SHAKindCheckRun, ev: SHAEvent{SHA: "abc123", Handler: PRHandlerCheckFailed}, wantErr: true, wantInvalid: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := seedRepoStore(tc.seed, tc.loadErr)
			sender := &fakePRSender{failPR: tc.failPR}
			self := &fakeSelfScheduler{fail: tc.failSched}
			out, err := NewRepoHandlers(store, nil, sender, self).RouteBySHA(context.Background(), tc.kind, tc.ev)
			checkErr(t, err, tc.wantErr, tc.wantInvalid)
			var evictedIDs []uint64
			for _, e := range out.Evicted {
				evictedIDs = append(evictedIDs, e.ID)
			}
			if out.Forwarded != tc.wantForwarded || out.PRNumber != tc.wantPR || out.Held != (tc.wantHeldID != 0) || out.HeldID != tc.wantHeldID || !slices.Equal(evictedIDs, tc.wantEvictedIDs) {
				t.Fatalf("outcome = %+v (evicted %v), want forwarded=%v pr=%d heldID=%d evicted=%v", out, evictedIDs, tc.wantForwarded, tc.wantPR, tc.wantHeldID, tc.wantEvictedIDs)
			}
			assertSent(t, sender.calls, tc.wantSent)
			assertSelf(t, self.calls, tc.wantSelf)
			if tc.loadErr {
				return
			}
			if st := loadRepoForTest(t, store); st != nil || len(tc.wantHeld) > 0 {
				if got := heldSHAs(st); !slices.Equal(got, tc.wantHeld) {
					t.Fatalf("held = %v, want %v", got, tc.wantHeld)
				}
			}
		})
	}
}

func TestRepoHandlers_ReplayHeld(t *testing.T) {
	t.Parallel()
	pushHeld := SHAEvent{Repo: "aerial", SHA: "m1", Handler: PRHandlerMerged, Event: PREvent{Event: "merged", EffectsOwner: EffectsOwnerRestate}}
	mergedIndex := []PRIndexEvent{idxActive(101, "h1", "feat-a", 0), idxMerged(101, "h1", "m1")}
	tests := []struct {
		name        string
		seed        repoSeed
		loadErr     bool
		failPR      int
		sha         string
		wantErr     bool
		wantInvalid bool
		wantSent    []sentPR
		wantHeld    []string
	}{
		{name: "resolved sha forwards its held events and keeps others", seed: repoSeed{held: []SHAEvent{pushHeld, heldCheck("zz")}, index: mergedIndex}, sha: "M1", wantSent: []sentPR{{handler: PRHandlerMerged, ev: PREvent{Event: "merged", Repo: "azylman/aerial", PRNumber: 101, Branch: "feat-a", HeadSHA: "m1", EffectsOwner: EffectsOwnerRestate}}}, wantHeld: []string{"zz"}},
		{name: "unresolved sha stays held", seed: repoSeed{held: []SHAEvent{heldCheck("zz")}}, sha: "zz", wantHeld: []string{"zz"}},
		{name: "resolved sha without held events is a no-op", seed: repoSeed{index: mergedIndex}, sha: "m1"},
		{name: "missing state is a no-op", sha: "m1"},
		{name: "forward failure keeps events held", seed: repoSeed{held: []SHAEvent{pushHeld}, index: mergedIndex}, failPR: 101, sha: "m1", wantErr: true, wantSent: []sentPR{{handler: PRHandlerMerged, ev: PREvent{Event: "merged", Repo: "azylman/aerial", PRNumber: 101, Branch: "feat-a", HeadSHA: "m1", EffectsOwner: EffectsOwnerRestate}}}, wantHeld: []string{"m1"}},
		{name: "load error surfaces", loadErr: true, sha: "m1", wantErr: true},
		{name: "zero sha is invalid", sha: "0000000", wantErr: true, wantInvalid: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := seedRepoStore(tc.seed, tc.loadErr)
			sender := &fakePRSender{failPR: tc.failPR}
			err := NewRepoHandlers(store, nil, sender, nil).ReplayHeld(context.Background(), "aerial", tc.sha)
			checkErr(t, err, tc.wantErr, tc.wantInvalid)
			assertSent(t, sender.calls, tc.wantSent)
			if tc.loadErr || tc.wantInvalid {
				return
			}
			if got := heldSHAs(loadRepoForTest(t, store)); !slices.Equal(got, tc.wantHeld) {
				t.Fatalf("held = %v, want %v", got, tc.wantHeld)
			}
		})
	}
}

func TestRepoHandlers_ExpireHeld(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		seed        repoSeed
		loadErr     bool
		repo        string
		id          uint64
		want        bool
		wantErr     bool
		wantInvalid bool
		wantHeld    []string
	}{
		{name: "still held event expires", seed: repoSeed{held: []SHAEvent{heldCheck("a1"), heldCheck("b2")}}, repo: "aerial", id: 1, want: true, wantHeld: []string{"b2"}},
		{name: "replayed id reports false", seed: repoSeed{held: []SHAEvent{heldCheck("a1")}}, repo: "aerial", id: 7, wantHeld: []string{"a1"}},
		{name: "missing state reports false", repo: "aerial", id: 1},
		{name: "load error surfaces", loadErr: true, repo: "aerial", id: 1, wantErr: true},
		{name: "empty repo is invalid", repo: "", id: 1, wantErr: true, wantInvalid: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := seedRepoStore(tc.seed, tc.loadErr)
			got, err := NewRepoHandlers(store, nil, nil, nil).ExpireHeld(context.Background(), tc.repo, tc.id)
			checkErr(t, err, tc.wantErr, tc.wantInvalid)
			if got != tc.want {
				t.Fatalf("ExpireHeld = %v, want %v", got, tc.want)
			}
			if tc.wantErr {
				return
			}
			if left := heldSHAs(loadRepoForTest(t, store)); !slices.Equal(left, tc.wantHeld) {
				t.Fatalf("held = %v, want %v", left, tc.wantHeld)
			}
		})
	}
}

func TestRepoHandlers_ResolvePRBySHA(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		seed       repoSeed
		loadErr    bool
		sha        string
		wantPR     int
		wantBranch string
		wantOK     bool
	}{
		{name: "hit resolves pr and branch", seed: repoSeed{index: []PRIndexEvent{idxActive(101, "abc123", "feat-x", 0)}}, sha: "abc123", wantPR: 101, wantBranch: "feat-x", wantOK: true},
		{name: "newest claim wins", seed: repoSeed{index: []PRIndexEvent{idxActive(101, "abc123", "old", 0), idxActive(102, "abc123", "new", 0)}}, sha: "abc123", wantPR: 102, wantBranch: "new", wantOK: true},
		{name: "miss reports not found", seed: repoSeed{index: []PRIndexEvent{idxActive(101, "abc123", "feat-x", 0)}}, sha: "zzz999"},
		{name: "missing state reports not found", sha: "abc123"},
		{name: "load error reports not found", loadErr: true, sha: "abc123"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pr, branch, ok := NewRepoHandlers(seedRepoStore(tc.seed, tc.loadErr), nil, nil, nil).ResolvePRBySHA(context.Background(), "aerial", tc.sha)
			if pr != tc.wantPR || branch != tc.wantBranch || ok != tc.wantOK {
				t.Fatalf("ResolvePRBySHA = (%d,%q,%v), want (%d,%q,%v)", pr, branch, ok, tc.wantPR, tc.wantBranch, tc.wantOK)
			}
		})
	}
}
