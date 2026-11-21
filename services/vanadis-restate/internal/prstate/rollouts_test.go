package prstate

import (
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
)

// deployingRollout seeds a merged PR deploying core and web at generation 1.
func deployingRollout(deployed ...string) *PRState {
	st := svcRow("h1", "m1", RegistryStatusDeploying, TerminalMerged)
	st.Agent = "agent-alpha"
	st.Branch = "feat/x"
	st.TargetID = "123456789012345678"
	st.GateGeneration = 1
	st.Jobs, st.StartedJobs = []string{"eri"}, []string{"eri"}
	st.KustomizationRequests = map[string]KustomizationRequest{
		"core": {Namespace: "flux-system", RequestedAt: "t1"},
		"web":  {Namespace: "flux-system", RequestedAt: "t1"},
	}
	st.DeployedKustomizations = deployed
	st.RolloutIndexed, st.RolloutIndexSeq = true, 1
	return st
}

func TestDeployViewOf(t *testing.T) {
	t.Parallel()
	merged := svcRow("h1", "m1", RegistryStatusMerged, TerminalMerged)
	tests := []struct {
		name string
		st   *PRState
		want DeployView
	}{
		{name: "unknown pr is not found", want: DeployView{}},
		{name: "merged pr without jobs expects nothing", st: merged, want: DeployView{Found: true, Repo: "azylman/aerial", PRNumber: 101, Status: RegistryStatusMerged, HeadSHA: "h1", MergeSHA: "m1", Agent: "aerial"}},
		{
			name: "deploying rollout carries its requests and progress", st: deployingRollout("core"),
			want: DeployView{
				Found: true, Repo: "azylman/aerial", PRNumber: 101, Status: RegistryStatusDeploying, Branch: "feat/x", HeadSHA: "h1", MergeSHA: "m1",
				TargetID: "123456789012345678", Agent: "agent-alpha", Jobs: []string{"eri"},
				KustomizationRequests:  map[string]KustomizationRequest{"core": {Namespace: "flux-system", RequestedAt: "t1"}, "web": {Namespace: "flux-system", RequestedAt: "t1"}},
				DeployedKustomizations: []string{"core"}, HasExpectedDeployJobs: true,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, DeployViewOf(tc.st))
		})
	}
}

func TestTakeRolloutIndexUpdate(t *testing.T) {
	t.Parallel()
	finished := deployingRollout("core", "web")
	finished.Status = RegistryStatusDeployed
	fresh := deployingRollout()
	fresh.RolloutIndexed, fresh.RolloutIndexSeq = false, 0
	tests := []struct {
		name    string
		st      *PRState
		want    RolloutIndexEvent
		wantOK  bool
		wantSeq uint64
	}{
		{name: "nil state owes nothing"},
		{name: "indexed rollout owes nothing", st: deployingRollout(), wantSeq: 1},
		{name: "new rollout activates", st: fresh, want: RolloutIndexEvent{Repo: "azylman/aerial", PRNumber: 101, Seq: 1, Active: true}, wantOK: true, wantSeq: 1},
		{name: "finished rollout deactivates", st: finished, want: RolloutIndexEvent{Repo: "azylman/aerial", PRNumber: 101, Seq: 2}, wantOK: true, wantSeq: 2},
		{name: "unindexed merged pr owes nothing", st: svcRow("h1", "m1", RegistryStatusMerged, TerminalMerged)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := tc.st.TakeRolloutIndexUpdate()
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.want, got)
			if tc.st != nil {
				require.Equal(t, tc.wantSeq, tc.st.RolloutIndexSeq)
			}
		})
	}
}

func TestRolloutIndexStateApply(t *testing.T) {
	t.Parallel()
	seeded := func() *RolloutIndexState {
		return &RolloutIndexState{Entries: map[string]RolloutIndexEntry{"azylman/aerial#101": {Seq: 2, Active: true, Order: 1}}, NextOrder: 1}
	}
	tests := []struct {
		name        string
		st          *RolloutIndexState
		ev          RolloutIndexEvent
		wantChanged bool
		wantActive  []RolloutRef
	}{
		{name: "first activation indexes the pr", st: &RolloutIndexState{}, ev: RolloutIndexEvent{Repo: "aerial", PRNumber: 7, Seq: 1, Active: true}, wantChanged: true, wantActive: []RolloutRef{{Repo: "azylman/aerial", PRNumber: 7}}},
		{name: "older sequence is dropped", st: seeded(), ev: RolloutIndexEvent{Repo: "aerial", PRNumber: 101, Seq: 1}, wantActive: []RolloutRef{{Repo: "azylman/aerial", PRNumber: 101}}},
		{name: "newer deactivation tombstones the pr", st: seeded(), ev: RolloutIndexEvent{Repo: "aerial", PRNumber: 101, Seq: 3}, wantChanged: true},
		{name: "a second pr sorts by repo then number", st: seeded(), ev: RolloutIndexEvent{Repo: "azylman/aerial-config", PRNumber: 5, Seq: 1, Active: true}, wantChanged: true, wantActive: []RolloutRef{{Repo: "azylman/aerial", PRNumber: 101}, {Repo: "azylman/aerial-config", PRNumber: 5}}},
		{name: "invalid repo is ignored", st: seeded(), ev: RolloutIndexEvent{PRNumber: 9, Seq: 1, Active: true}, wantActive: []RolloutRef{{Repo: "azylman/aerial", PRNumber: 101}}},
		{name: "nil index ignores updates", ev: RolloutIndexEvent{Repo: "aerial", PRNumber: 9, Seq: 1, Active: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.wantChanged, tc.st.Apply(tc.ev))
			require.Equal(t, tc.wantActive, tc.st.Active())
		})
	}
}

func TestRolloutIndexStateEvictsOldest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		extra     int
		wantLen   int
		wantFirst RolloutRef
	}{
		{name: "one over the bound evicts the first insert", extra: 1, wantLen: MaxRolloutIndexEntries, wantFirst: RolloutRef{Repo: "azylman/aerial", PRNumber: 2}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &RolloutIndexState{}
			for pr := 1; pr <= MaxRolloutIndexEntries+tc.extra; pr++ {
				st.Apply(RolloutIndexEvent{Repo: "aerial", PRNumber: pr, Seq: 1, Active: true})
			}
			active := st.Active()
			require.Len(t, active, tc.wantLen)
			require.NotContains(t, active, RolloutRef{Repo: "azylman/aerial", PRNumber: 1})
			require.Contains(t, active, tc.wantFirst)
		})
	}
}

func TestSplitPRObjectKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		key      string
		wantRepo string
		wantPR   int
		wantOK   bool
	}{
		{key: "azylman/aerial#101", wantRepo: "azylman/aerial", wantPR: 101, wantOK: true},
		{key: "azylman/aerial"},
		{key: "#5"},
		{key: "azylman/aerial#x"},
		{key: "azylman/aerial#0"},
	}
	for _, tc := range tests {
		t.Run(tc.key, func(t *testing.T) {
			t.Parallel()
			repo, pr, ok := splitPRObjectKey(tc.key)
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.wantRepo, repo)
			require.Equal(t, tc.wantPR, pr)
		})
	}
}

func TestApplyDeployObserved(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		st       *PRState
		ev       PREvent
		want     bool
		wantJobs []string
	}{
		{name: "started job transitions a merged pr", st: svcRow("h1", "m1", RegistryStatusMerged, TerminalMerged), ev: PREvent{JobName: "eri", ExpectedJobs: []string{"haro"}}, want: true, wantJobs: []string{"eri", "haro"}},
		{name: "expected jobs alone never transition", st: svcRow("h1", "m1", RegistryStatusMerged, TerminalMerged), ev: PREvent{ExpectedJobs: []string{"haro"}}, wantJobs: []string{"haro"}},
		{name: "already deployed job does not transition", st: func() *PRState {
			s := svcRow("h1", "m1", RegistryStatusDeployed, TerminalMerged)
			s.DeployedJobs = []string{"eri"}
			return s
		}(), ev: PREvent{JobName: "eri"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, tc.st.ApplyDeployObserved(tc.ev))
			require.Equal(t, tc.wantJobs, tc.st.Jobs)
		})
	}
}

func TestSplitDeployStartedFacts(t *testing.T) {
	t.Parallel()
	started := pubsub.PRLifecycleEvent{Event: "deploy_started", PRNumber: 1}
	synced := pubsub.PRLifecycleEvent{Event: "sync_success", PRNumber: 1}
	tests := []struct {
		name        string
		facts       []pubsub.PRLifecycleEvent
		wantStarted []pubsub.PRLifecycleEvent
		wantRest    []pubsub.PRLifecycleEvent
	}{
		{name: "no facts"},
		{name: "mixed facts split", facts: []pubsub.PRLifecycleEvent{synced, started}, wantStarted: []pubsub.PRLifecycleEvent{started}, wantRest: []pubsub.PRLifecycleEvent{synced}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotStarted, gotRest := SplitDeployStartedFacts(tc.facts)
			require.Equal(t, tc.wantStarted, gotStarted)
			require.Equal(t, tc.wantRest, gotRest)
		})
	}
}

func TestResolvePromotionPR(t *testing.T) {
	t.Parallel()
	indexed := func() *RepoState {
		st := NewRepoState("aerial")
		st.ApplyPRIndex(PRIndexEvent{Repo: "aerial", PRNumber: 42, Branch: "feat/y", MergeSHA: "m42", Lifecycle: IndexMerged})
		return st
	}
	fact := pubsub.PRLifecycleEvent{Event: "deploy_started"}
	tests := []struct {
		name       string
		ev         RepoEvent
		wantPR     int
		wantBranch string
		wantFactPR int
	}{
		{name: "restate sha-only promotion takes the indexed pr", ev: RepoEvent{HeadSHA: "m42", EffectsOwner: EffectsOwnerRestate, Facts: []pubsub.PRLifecycleEvent{fact}, DeployFailedFacts: []pubsub.PRLifecycleEvent{fact}}, wantPR: 42, wantBranch: "feat/y", wantFactPR: 42},
		{name: "legacy promotion keeps its pr", ev: RepoEvent{HeadSHA: "m42", Facts: []pubsub.PRLifecycleEvent{fact}}},
		{name: "unindexed sha stays untracked", ev: RepoEvent{HeadSHA: "m9", EffectsOwner: EffectsOwnerRestate, Facts: []pubsub.PRLifecycleEvent{fact}}},
		{name: "known pr is kept", ev: RepoEvent{HeadSHA: "m42", PRNumber: 7, Branch: "b", EffectsOwner: EffectsOwnerRestate, Facts: []pubsub.PRLifecycleEvent{fact}}, wantPR: 7, wantBranch: "b"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := resolvePromotionPR(indexed(), tc.ev)
			require.Equal(t, tc.wantPR, got.PRNumber)
			require.Equal(t, tc.wantBranch, got.Branch)
			require.Equal(t, tc.wantFactPR, got.Facts[0].PRNumber)
		})
	}
}

func TestPRObjectServiceGetDeployView(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		seed *PRState
		want DeployView
	}{
		{name: "unknown pr", want: DeployView{}},
		{name: "merged pr", seed: svcRow("h1", "m1", RegistryStatusMerged, TerminalMerged), want: DeployView{Found: true, Repo: "azylman/aerial", PRNumber: 101, Status: RegistryStatusMerged, HeadSHA: "h1", MergeSHA: "m1", Agent: "aerial"}},
		{name: "untracked deploying pr is not found", seed: untrackedResidue(), want: DeployView{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, _ := newStateMock(t, tc.seed)
			got, err := NewPRObjectService().GetDeployView(restate.WithMockContext(mc), DeployViewRequest{})
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestPRObjectServiceHandleDeployObserved(t *testing.T) {
	t.Parallel()
	started := pubsub.PRLifecycleEvent{Event: "deploy_started", Repo: "azylman/aerial", PRNumber: 101, MergeSHA: "m1", JobName: "eri"}
	synced := pubsub.PRLifecycleEvent{Event: "sync_success", Repo: "azylman/aerial", PRNumber: 101, MergeSHA: "m1", JobName: "git-sync"}
	deployed := svcRow("h1", "m1", RegistryStatusDeployed, TerminalMerged)
	deployed.DeployedJobs = []string{"eri"}
	tests := []struct {
		name       string
		seed       *PRState
		ev         PREvent
		wantEvents []string
		wantStatus string
	}{
		{name: "started job publishes deploy_started", seed: svcRow("h1", "m1", RegistryStatusMerged, TerminalMerged), ev: PREvent{Repo: "aerial", PRNumber: 101, MergeSHA: "m1", JobName: "eri", EffectsOwner: EffectsOwnerRestate, Facts: []pubsub.PRLifecycleEvent{started}}, wantEvents: []string{"deploy_started"}, wantStatus: RegistryStatusDeploying},
		{name: "already deployed job suppresses deploy_started", seed: deployed, ev: PREvent{Repo: "aerial", PRNumber: 101, MergeSHA: "m1", JobName: "eri", EffectsOwner: EffectsOwnerRestate, Facts: []pubsub.PRLifecycleEvent{started}}, wantStatus: RegistryStatusDeployed},
		{name: "other facts always publish", seed: svcRow("h1", "m1", RegistryStatusMerged, TerminalMerged), ev: PREvent{Repo: "aerial", PRNumber: 101, MergeSHA: "m1", ExpectedJobs: []string{"eri"}, EffectsOwner: EffectsOwnerRestate, Facts: []pubsub.PRLifecycleEvent{synced}}, wantEvents: []string{"sync_success"}, wantStatus: RegistryStatusMerged},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := newStateMock(t, tc.seed)
			stubRunKeepTerminal(mc)
			pub := &fakeFactPublisher{}
			require.NoError(t, NewPRObjectService().WithFactPublisher(pub).HandleDeployObserved(restate.WithMockContext(mc), tc.ev))
			require.Equal(t, tc.wantEvents, factEvents(pub.Calls()))
			require.Equal(t, tc.wantStatus, rec.saved.Status)
		})
	}
}

func TestPRObjectServiceHandleKustomizationRequestedStartedFact(t *testing.T) {
	t.Parallel()
	started := pubsub.PRLifecycleEvent{Event: "deploy_started", Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "m1"}
	alreadyDeployed := svcRow("h1", "m1", RegistryStatusDeployed, TerminalMerged)
	alreadyDeployed.DeployedJobs = []string{"eri"}
	tests := []struct {
		name       string
		seed       *PRState
		wantEvents []string
	}{
		{name: "transition publishes deploy_started", seed: svcRow("h1", "m1", RegistryStatusMerged, TerminalMerged), wantEvents: []string{"deploy_started"}},
		{name: "no transition suppresses deploy_started", seed: alreadyDeployed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, _ := newStateMock(t, tc.seed)
			stubRunKeepTerminal(mc)
			pub := &fakeFactPublisher{}
			ev := PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "m1", Jobs: []string{"eri"}, KustomizationRequests: coreRequests, EffectsOwner: EffectsOwnerRestate, Facts: []pubsub.PRLifecycleEvent{started}}
			require.NoError(t, NewPRObjectService().WithFactPublisher(pub).WithKube(&fakeKube{}).HandleKustomizationRequested(restate.WithMockContext(mc), ev))
			require.Equal(t, tc.wantEvents, factEvents(pub.Calls()))
		})
	}
}

func TestPRObjectServiceHandleReconcileLost(t *testing.T) {
	t.Parallel()
	lostKey := "azylman/aerial#101@:gen1:flux_reconcile:job=repatch:web@t0"
	ev := func(owner EffectsOwner, name string) PREvent {
		return PREvent{Repo: "aerial", PRNumber: 101, JobName: name, ObservedReconcileAt: "t0", EffectsOwner: owner}
	}
	tests := []struct {
		name         string
		seed         *PRState
		ev           PREvent
		wantCalls    []string
		wantIntended []string
	}{
		{name: "restate re-patches only the lost kustomization", seed: deployingRollout(), ev: ev(EffectsOwnerRestate, "web"), wantCalls: []string{"reconcile flux-system/web@t1"}},
		{name: "legacy records the intended re-patch", seed: deployingRollout(), ev: ev(EffectsOwnerLegacy, "web"), wantIntended: []string{lostKey}},
		{name: "unknown kustomization does nothing", seed: deployingRollout(), ev: ev(EffectsOwnerRestate, "api")},
		{name: "pr out of rollout does nothing", seed: deployedRollout(), ev: ev(EffectsOwnerRestate, "core")},
		{name: "missing name does nothing", seed: deployingRollout(), ev: ev(EffectsOwnerRestate, "")},
		{name: "untracked rollout does nothing", seed: untrackedResidue(), ev: ev(EffectsOwnerRestate, "gateway-api-crds")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := newStateMock(t, tc.seed)
			stubRunKeepTerminal(mc)
			kube := &fakeKube{}
			require.NoError(t, NewPRObjectService().WithKube(kube).HandleReconcileLost(restate.WithMockContext(mc), tc.ev))
			require.Equal(t, tc.wantCalls, kube.Calls())
			var intended []string
			if rec.saved != nil {
				for _, e := range rec.saved.IntendedEffects {
					intended = append(intended, e.EffectKey)
				}
			}
			require.Equal(t, tc.wantIntended, intended)
		})
	}
}

// newRolloutIndexMock stubs the rollout-index K/V slot on the object key.
func newRolloutIndexMock(t *testing.T, key string, existing *RolloutIndexState) (*mocks.MockContext, **RolloutIndexState) {
	t.Helper()
	mc := mocks.NewMockContext(t)
	saved := new(*RolloutIndexState)
	mc.On("Key").Return(key).Maybe()
	mc.On("Get", rolloutIndexStateKey, mock.Anything).Run(func(args mock.Arguments) {
		if pp, ok := args.Get(1).(**RolloutIndexState); ok && pp != nil {
			*pp = existing
		}
	}).Return(true, nil).Maybe()
	mc.On("Set", rolloutIndexStateKey, mock.Anything).Run(func(args mock.Arguments) {
		if st, ok := args.Get(1).(*RolloutIndexState); ok {
			*saved = st
		}
	}).Maybe()
	return mc, saved
}

func TestRepoObjectServiceRolloutIndex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		key      string
		existing *RolloutIndexState
		ev       RolloutIndexEvent
		wantList []RolloutRef
	}{
		{name: "index key records an activation", key: RolloutIndexKey, ev: RolloutIndexEvent{Repo: "aerial", PRNumber: 3, Seq: 1, Active: true}, wantList: []RolloutRef{{Repo: "azylman/aerial", PRNumber: 3}}},
		{name: "a repo key ignores index updates", key: "azylman/aerial", ev: RolloutIndexEvent{Repo: "aerial", PRNumber: 3, Seq: 1, Active: true}},
		{
			name: "stale update keeps the index", key: RolloutIndexKey,
			existing: &RolloutIndexState{Entries: map[string]RolloutIndexEntry{"azylman/aerial#3": {Seq: 5, Active: true, Order: 1}}, NextOrder: 1},
			ev:       RolloutIndexEvent{Repo: "aerial", PRNumber: 3, Seq: 4}, wantList: []RolloutRef{{Repo: "azylman/aerial", PRNumber: 3}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, saved := newRolloutIndexMock(t, tc.key, tc.existing)
			svc := NewRepoObjectService()
			require.NoError(t, svc.HandleRolloutIndex(restate.WithMockContext(mc), tc.ev))
			after := tc.existing
			if *saved != nil {
				after = *saved
			}
			listMC, _ := newRolloutIndexMock(t, tc.key, after)
			got, err := svc.ListRollouts(restate.WithMockContext(listMC), ListRolloutsRequest{})
			require.NoError(t, err)
			require.Equal(t, tc.wantList, got.Rollouts, "key "+tc.key)
		})
	}
}

// factEvents lists the published fact event names, nil when none fired.
func factEvents(calls []factCall) []string {
	if len(calls) == 0 {
		return nil
	}
	events := make([]string, 0, len(calls))
	for _, c := range calls {
		events = append(events, c.fact.Event)
	}
	return events
}

// untrackedResidue is the live kubernetes-sigs/gateway-api#5433 object a
// pre-guard HandleKustomizationRequested left behind: deploying, one
// recorded request, indexed as a rollout, with no head, merge, target or
// register (g3-shadow-soak-r5).
func untrackedResidue() *PRState {
	s := NewPRState("kubernetes-sigs/gateway-api", 5433)
	s.Agent = "aerial"
	s.Status = RegistryStatusDeploying
	s.Jobs, s.StartedJobs = []string{"gateway-api-crds"}, []string{"gateway-api-crds"}
	s.KustomizationRequests = map[string]KustomizationRequest{"gateway-api-crds": {Namespace: "flux-system", RequestedAt: "2026-11-21T04:55:46Z"}}
	s.GateGeneration, s.RolloutIndexed, s.RolloutIndexSeq = 1, true, 1
	return s
}

// untrackedCleaned is untrackedResidue after the cleanup: rollout fields
// cleared, generation kept, index membership dropped at the next sequence.
func untrackedCleaned() *PRState {
	s := NewPRState("kubernetes-sigs/gateway-api", 5433)
	s.Agent = "aerial"
	s.GateGeneration, s.RolloutIndexSeq = 1, 2
	return s
}

// untrackedCleanupSends are the index closes the cleanup sends.
var untrackedCleanupSends = []sentCall{
	{service: RepoObjectServiceName, key: RolloutIndexKey, method: RepoHandlerRolloutIndex, input: RolloutIndexEvent{Repo: "kubernetes-sigs/gateway-api", PRNumber: 5433, Seq: 2}},
	{service: RepoObjectServiceName, key: "kubernetes-sigs/gateway-api", method: RepoHandlerPRIndex, input: PRIndexEvent{Repo: "kubernetes-sigs/gateway-api", PRNumber: 5433, Generation: 1, Lifecycle: IndexDeployed}},
}

func TestPRObjectServiceDropUntracked(t *testing.T) {
	t.Parallel()
	tracked := svcRow("h1", "m1", RegistryStatusDeploying, TerminalMerged)
	tests := []struct {
		name      string
		seed      *PRState
		want      DropUntrackedResponse
		wantSaved *PRState
		wantSends []sentCall
	}{
		{name: "unknown pr is untouched"},
		{name: "tracked pr is untouched", seed: tracked},
		{name: "untracked rollout is cleaned and leaves both indexes", seed: untrackedResidue(), want: DropUntrackedResponse{Untracked: true, Cleaned: true}, wantSaved: untrackedCleaned(), wantSends: untrackedCleanupSends},
		{name: "cleaned untracked pr is idempotent", seed: untrackedCleaned(), want: DropUntrackedResponse{Untracked: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := newStateMock(t, tc.seed)
			got, err := NewPRObjectService().DropUntracked(restate.WithMockContext(mc), DropUntrackedRequest{})
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.wantSaved, rec.saved)
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
		})
	}
}

// TestPRObjectServiceHandleKustomizationRequestedUntracked pins the drop
// for an untracked PR: a fresh state persists and sends nothing, and a
// stored pre-guard rollout is cleaned on the next request instead of
// re-recorded or patched.
func TestPRObjectServiceHandleKustomizationRequestedUntracked(t *testing.T) {
	t.Parallel()
	started := pubsub.PRLifecycleEvent{Event: "deploy_started", Repo: "kubernetes-sigs/gateway-api", PRNumber: 5433, Status: "deploying"}
	requests := map[string]KustomizationRequest{"gateway-api-crds": {Namespace: "flux-system", RequestedAt: "2026-11-21T05:57:27Z"}}
	tests := []struct {
		name      string
		seed      *PRState
		owner     EffectsOwner
		wantSaved *PRState
		wantSends []sentCall
	}{
		{name: "fresh legacy request persists nothing", owner: EffectsOwnerLegacy},
		{name: "fresh restate request persists nothing", owner: EffectsOwnerRestate},
		{name: "stored pre-guard rollout is cleaned", seed: untrackedResidue(), owner: EffectsOwnerLegacy, wantSaved: untrackedCleaned(), wantSends: untrackedCleanupSends},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := newStateMock(t, tc.seed)
			stubRunKeepTerminal(mc)
			kube := &fakeKube{}
			pub := &fakeFactPublisher{}
			ev := PREvent{Repo: "kubernetes-sigs/gateway-api", PRNumber: 5433, Jobs: []string{"gateway-api-crds"}, KustomizationRequests: requests, EffectsOwner: tc.owner, Facts: []pubsub.PRLifecycleEvent{started}}
			require.NoError(t, NewPRObjectService().WithKube(kube).WithFactPublisher(pub).HandleKustomizationRequested(restate.WithMockContext(mc), ev))
			require.Equal(t, tc.wantSaved, rec.saved)
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
			require.Empty(t, kube.Calls())
			require.Empty(t, pub.Calls())
		})
	}
}
