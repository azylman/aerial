package prstate

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/pubsub"
)

const (
	testConflictKey  = "azylman/aerial#101@h1:gen0:pr_fact_conflict"
	testConflictIdem = "prstate-0e1440d980ffeb727b4c449454e78ef2"
	testDeployKey    = "azylman/aerial#101@m1:gen0:pr_fact_deploy_success:job=eri"
	testDeployIdem   = "prstate-42ee6b4f8424a360d332e93c0fca0597"
	testSyncKey      = "azylman/aerial#101@h1:gen0:pr_fact_synchronize"
	testSyncIdem     = "prstate-48ff1705266d6f67bd96deaf2b89ebb8"
	testFailedIdem   = "prstate-d7665dabf8bc19c3fe142a14cc6c3a50"
)

// conflictFact is the rendered conflict fact synapse attaches.
func conflictFact() pubsub.PRLifecycleEvent {
	return pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "h1", Status: "conflict", TargetID: testTarget}
}

// pinned returns f addressed to azylman/aerial#101, as PlanFacts pins it.
func pinned(f pubsub.PRLifecycleEvent) pubsub.PRLifecycleEvent {
	f.Repo, f.PRNumber = "azylman/aerial", 101
	return f
}

func deployFact() pubsub.PRLifecycleEvent {
	return pubsub.PRLifecycleEvent{Event: "deploy_success", MergeSHA: "m1", JobName: "eri", Status: "deployed"}
}

func syncFact() pubsub.PRLifecycleEvent {
	return pubsub.PRLifecycleEvent{Event: "synchronize", HeadSHA: "h1", Status: "open"}
}

func failedFact() pubsub.PRLifecycleEvent {
	return pubsub.PRLifecycleEvent{Event: "deploy_failed", MergeSHA: "m1", Status: "deploy_failed", Details: "boom"}
}

func factKey(kind, head, job string) EffectKey {
	return EffectKey{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: head, Kind: kind, JobName: job}
}

func checkFactKey(kind, head, state, check string) EffectKey {
	return EffectKey{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: head, Kind: kind, JobName: state, CheckName: check}
}

// factCall is one recorded FactPublisher publish.
type factCall struct {
	msgID string
	fact  pubsub.PRLifecycleEvent
}

// fakeFactPublisher is the hermetic FactPublisher boundary.
type fakeFactPublisher struct {
	mu    sync.Mutex
	err   error
	calls []factCall
}

func (f *fakeFactPublisher) PublishFact(_ context.Context, msgID string, fact pubsub.PRLifecycleEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, factCall{msgID: msgID, fact: fact})
	return f.err
}

func (f *fakeFactPublisher) Calls() []factCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]factCall(nil), f.calls...)
}

var errFakeBus = errors.New("nats unavailable")

// TestFactEffectKeyMatchKey pins the generation-free fact key against the
// golden fixture fact_match_keys.json.
func TestFactEffectKeyMatchKey(t *testing.T) {
	t.Parallel()
	var fx struct {
		Cases []struct {
			Name     string                  `json:"name"`
			Repo     string                  `json:"repo"`
			PRNumber int                     `json:"pr_number"`
			Fact     pubsub.PRLifecycleEvent `json:"fact"`
			Want     string                  `json:"want"`
		} `json:"cases"`
	}
	readPRLifecycleFixture(t, "fact_match_keys.json", &fx)
	require.NotEmpty(t, fx.Cases)
	for _, tc := range fx.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			st := NewPRState(tc.Repo, tc.PRNumber)
			st.GateGeneration = 4
			require.Equal(t, tc.Want, FactEffectKey(st, tc.Fact).MatchKey())
		})
	}
}

// TestPlanFacts pins fact normalization, keying with SHA fallbacks, owner
// addressing, collapse of same-key duplicates, and generation dedup.
func TestPlanFacts(t *testing.T) {
	t.Parallel()
	fired := effectPR()
	fired.MarkEffectDispatched(factKey("pr_fact_conflict", "h1", ""))
	pendingCheckFired := effectPR()
	pendingCheckFired.MarkEffectDispatched(checkFactKey("pr_fact_check_updated", "h1", "pending", "lint"))
	tests := []struct {
		name  string
		state *PRState
		facts []pubsub.PRLifecycleEvent
		want  []PlannedFact
	}{
		{name: "nil state plans nothing", facts: []pubsub.PRLifecycleEvent{conflictFact()}},
		{name: "no facts plan nothing", state: effectPR()},
		{
			name: "fact is pinned to the PR and keyed on its head", state: effectPR(),
			facts: []pubsub.PRLifecycleEvent{{Event: " Conflict ", Repo: "other/repo", PRNumber: 9, HeadSHA: "h1", Status: "conflict", TargetID: testTarget}},
			want:  []PlannedFact{{Fact: pinned(conflictFact()), Key: factKey("pr_fact_conflict", "h1", ""), Agent: "agent-alpha"}},
		},
		{
			name: "merge sha keys a headless fact", state: effectPR(), facts: []pubsub.PRLifecycleEvent{deployFact()},
			want: []PlannedFact{{Fact: pinned(deployFact()), Key: factKey("pr_fact_deploy_success", "m1", "eri"), Agent: "agent-alpha"}},
		},
		{
			name: "stored head keys a sha-less fact", state: effectPR(), facts: []pubsub.PRLifecycleEvent{{Event: "pr_updated"}},
			want: []PlannedFact{{Fact: pinned(pubsub.PRLifecycleEvent{Event: "pr_updated"}), Key: factKey("pr_fact_pr_updated", "h1", ""), Agent: "agent-alpha"}},
		},
		{name: "nameless fact is dropped", state: effectPR(), facts: []pubsub.PRLifecycleEvent{{Event: "  ", HeadSHA: "h1"}}},
		{
			name: "same-key duplicates collapse to the first", state: effectPR(), facts: []pubsub.PRLifecycleEvent{conflictFact(), {Event: "conflict", HeadSHA: "h1"}},
			want: []PlannedFact{{Fact: pinned(conflictFact()), Key: factKey("pr_fact_conflict", "h1", ""), Agent: "agent-alpha"}},
		},
		{name: "already fired key is skipped", state: fired, facts: []pubsub.PRLifecycleEvent{conflictFact()}},
		{
			name: "check_updated passed fact fires after pending fact for same check", state: pendingCheckFired,
			facts: []pubsub.PRLifecycleEvent{
				{Event: "check_updated", HeadSHA: "h1", CheckName: "lint", CheckState: "pending"},
				{Event: "check_updated", HeadSHA: "h1", CheckName: "lint", CheckState: "passed"},
			},
			want: []PlannedFact{{
				Fact:  pinned(pubsub.PRLifecycleEvent{Event: "check_updated", HeadSHA: "h1", CheckName: "lint", CheckState: "passed"}),
				Key:   checkFactKey("pr_fact_check_updated", "h1", "passed", "lint"),
				Agent: "agent-alpha",
			}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, PlanFacts(tc.state, tc.facts))
		})
	}
}

// TestFactFingerprints pins the eri and fact effect keys and their bounded
// idempotency keys against externally computed sha256 literals.
func TestFactFingerprints(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		key      EffectKey
		wantKey  string
		wantIdem string
	}{
		{name: "collaborator DM", key: dmKey("h1", 0, ""), wantKey: testDMKey, wantIdem: "prstate-54d500d389704715f4c085c744f8e1c8"},
		{name: "conflict", key: factKey("pr_fact_conflict", "h1", ""), wantKey: testConflictKey, wantIdem: testConflictIdem},
		{name: "deploy success", key: factKey("pr_fact_deploy_success", "m1", "eri"), wantKey: testDeployKey, wantIdem: testDeployIdem},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.wantKey, tc.key.String())
			require.Equal(t, tc.wantIdem, EffectIdempotencyKey(tc.key))
		})
	}
}

// recordingBus is the hermetic pubsub.Bus for BusFactPublisher; only
// Publish is exercised.
type recordingBus struct {
	pubsub.Bus

	err     error
	subject string
	msgID   string
	data    any
}

func (b *recordingBus) Publish(_ context.Context, subject, msgID string, data any) error {
	b.subject, b.msgID, b.data = subject, msgID, data
	return b.err
}

// TestBusFactPublisherPublishFact pins the aerial.pr.<event> subject, the
// message ID passthrough, error propagation, and the unconfigured bus.
func TestBusFactPublisherPublishFact(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		bus         *recordingBus
		fact        pubsub.PRLifecycleEvent
		wantErr     bool
		wantSubject string
	}{
		{name: "publishes on the event subject", bus: &recordingBus{}, fact: pinned(conflictFact()), wantSubject: "aerial.pr.conflict"},
		{name: "bus failure surfaces", bus: &recordingBus{err: errFakeBus}, fact: pinned(conflictFact()), wantErr: true, wantSubject: "aerial.pr.conflict"},
		{name: "missing bus fails", fact: pinned(conflictFact()), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pub := NewBusFactPublisher(nil)
			if tc.bus != nil {
				pub = NewBusFactPublisher(tc.bus)
			}
			err := pub.PublishFact(context.Background(), testConflictIdem, tc.fact)
			require.Equal(t, tc.wantErr, err != nil)
			if tc.bus != nil {
				require.Equal(t, tc.wantSubject, tc.bus.subject)
				require.Equal(t, testConflictIdem, tc.bus.msgID)
				require.Equal(t, tc.fact, tc.bus.data)
			}
		})
	}
}

// TestPRObjectServiceHandleConflict pins the merged conflict verdict: a new
// SHA commits the conflict SHA and row status, then publishes the fact with
// the EffectKey message ID; a failed publish keeps the commit and schedules a retry; deduped, terminal, and
// malformed events commit nothing.
func TestPRObjectServiceHandleConflict(t *testing.T) {
	t.Parallel()
	published := func() *PRState {
		st := effectPR()
		st.MarkConflictPublished("h1")
		return st
	}
	merged := effectPR()
	merged.Terminal = TerminalMerged
	retry := []sentCall{{service: "PRObject", key: "azylman/aerial", method: "RetryFactEffect", input: FactEffectRetry{Fact: pinned(conflictFact()), Attempt: 1}, delayed: true}}
	tests := []struct {
		name           string
		state          *PRState
		failGet        bool
		ev             PREvent
		pub            *fakeFactPublisher
		wantErr        bool
		want           ConflictDispatchDecision
		wantCalls      []factCall
		wantSet        bool
		wantStatus     string
		wantDispatched bool
		wantSends      []sentCall
	}{
		{
			name: "restate owner commits then publishes", state: effectPR(), pub: &fakeFactPublisher{},
			ev:   PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", Facts: []pubsub.PRLifecycleEvent{conflictFact()}},
			want: ConflictDispatchDecision{Disposition: DispositionApply, ShouldPublish: true}, wantCalls: []factCall{{msgID: testConflictIdem, fact: pinned(conflictFact())}},
			wantSet: true, wantStatus: RegistryStatusConflict, wantDispatched: true,
		},
		{
			name: "failed publish keeps the commit and schedules a retry", state: effectPR(), pub: &fakeFactPublisher{err: errFakeBus},
			ev:   PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", Facts: []pubsub.PRLifecycleEvent{conflictFact()}},
			want: ConflictDispatchDecision{Disposition: DispositionApply, ShouldPublish: true}, wantCalls: []factCall{{msgID: testConflictIdem, fact: pinned(conflictFact())}},
			wantSet: true, wantStatus: RegistryStatusConflict, wantSends: retry,
		},
		{
			name: "unconfigured publisher schedules a retry", state: effectPR(),
			ev:   PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", Facts: []pubsub.PRLifecycleEvent{conflictFact()}},
			want: ConflictDispatchDecision{Disposition: DispositionApply, ShouldPublish: true}, wantSet: true, wantStatus: RegistryStatusConflict, wantSends: retry,
		},
		{
			name: "published sha dedupes without a commit", state: published(), pub: &fakeFactPublisher{},
			ev:   PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", Facts: []pubsub.PRLifecycleEvent{conflictFact()}},
			want: ConflictDispatchDecision{Disposition: DispositionApply, Deduped: true},
		},
		{
			name: "terminal pr absorbs", state: merged, pub: &fakeFactPublisher{},
			ev:   PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", Facts: []pubsub.PRLifecycleEvent{conflictFact()}},
			want: ConflictDispatchDecision{Disposition: DispositionAbsorbTerminal},
		},
		{name: "malformed event is ignored", state: effectPR(), pub: &fakeFactPublisher{}, ev: PREvent{PRNumber: 101, HeadSHA: "h1"}},
		{name: "kv failure surfaces", failGet: true, pub: &fakeFactPublisher{}, ev: PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			svc := NewPRObjectService()
			if tc.pub != nil {
				svc = svc.WithFactPublisher(tc.pub)
			}
			mc, rec := prKV(t, tc.state, tc.failGet, false)
			stubRun(mc, false)
			got, err := svc.HandleConflict(restate.WithMockContext(mc), tc.ev)
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.want, got)
			if tc.pub != nil {
				require.Equal(t, tc.wantCalls, tc.pub.Calls())
			}
			require.Equal(t, tc.wantSet, rec.setCalled)
			if tc.wantSet {
				require.Equal(t, tc.wantStatus, rec.saved.Status)
				require.True(t, rec.saved.WasConflictPublished("h1"))
				require.Equal(t, tc.wantDispatched, rec.saved.WasEffectDispatched(factKey("pr_fact_conflict", "h1", "")))
			}
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
		})
	}
}

// deployingMergedPR seeds a merged PR deploying eri on merge m1.
func deployingMergedPR() *PRState {
	st := svcRow("h1", "m1", RegistryStatusDeploying, TerminalMerged)
	st.Agent = "agent-alpha"
	return st
}

// TestPRObjectServiceHandleDeploySuccess pins the merged deploy_success
// verdict: an undispatched deploy commits the dispatch key and both dedup
// slots (leaving an untracked rollout's status alone), disarms a pending failure deadline, indexes
// the PR as deployed, then publishes the fact; a fully deduped deploy
// commits nothing.
func TestPRObjectServiceHandleDeploySuccess(t *testing.T) {
	t.Parallel()
	armed := func() *PRState {
		st := deployingMergedPR()
		st.ArmDeployFailure(PREvent{MergeSHA: "m1", Banner: "boom"})
		return st
	}
	deduped := func() *PRState {
		st := deployingMergedPR()
		st.MarkEffectDispatched(st.EffectKeyFor(prwire.EffectDeployDispatch, "", "eri", ""))
		return st
	}
	ev := func() PREvent {
		return PREvent{Repo: "azylman/aerial", PRNumber: 101, MergeSHA: "m1", JobName: "eri", Facts: []pubsub.PRLifecycleEvent{deployFact()}}
	}
	indexed := []sentCall{{service: "RepoObject", key: "azylman/aerial", method: prwire.RepoHandlerPRIndex, input: PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", MergeSHA: "m1", Lifecycle: IndexDeployed}}}
	applied := DeployDispatchDecision{Disposition: DispositionApply, ShouldPublish: true, EffectKey: "azylman/aerial#101@:gen0:deploy_dispatch:job=eri"}
	tests := []struct {
		name           string
		state          *PRState
		failGet        bool
		ev             PREvent
		want           DeployDispatchDecision
		wantErr        bool
		wantCalls      []factCall
		wantSet        bool
		wantDispatched bool
		wantSends      []sentCall
	}{
		{
			name: "restate owner commits indexes and publishes", state: deployingMergedPR(), ev: ev(), want: applied,
			wantCalls: []factCall{{msgID: testDeployIdem, fact: pinned(deployFact())}}, wantSet: true, wantDispatched: true, wantSends: indexed,
		},
		{
			name: "pending failure deadline is disarmed", state: armed(), ev: ev(), want: applied,
			wantCalls: []factCall{{msgID: testDeployIdem, fact: pinned(deployFact())}}, wantSet: true, wantDispatched: true, wantSends: indexed,
		},
		{
			name: "fully deduped deploy commits nothing", state: deduped(), ev: ev(),
			want: DeployDispatchDecision{Disposition: DispositionApply, Deduped: true, EffectKey: "azylman/aerial#101@:gen0:deploy_dispatch:job=eri"},
		},
		{name: "stale sha drops", state: deployingMergedPR(), ev: PREvent{Repo: "azylman/aerial", PRNumber: 101, MergeSHA: "m9", JobName: "eri"}, want: DeployDispatchDecision{Disposition: DispositionDropStale}},
		{name: "malformed event is ignored", state: deployingMergedPR(), ev: PREvent{Repo: "azylman/aerial", MergeSHA: "m1"}},
		{name: "kv failure surfaces", failGet: true, ev: ev(), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pub := &fakeFactPublisher{}
			svc := NewPRObjectService().WithFactPublisher(pub)
			mc, rec := prKV(t, tc.state, tc.failGet, false)
			stubRun(mc, false)
			got, err := svc.HandleDeploySuccess(restate.WithMockContext(mc), tc.ev)
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.wantCalls, pub.Calls())
			require.Equal(t, tc.wantSet, rec.setCalled)
			if tc.wantSet {
				// No Kustomization requests: a direct-alert deploy never
				// flips the registry status, matching legacy synapse.
				require.Equal(t, RegistryStatusDeploying, rec.saved.Status)
				require.True(t, rec.saved.WasEffectDispatched(rec.saved.EffectKeyFor(prwire.EffectDeployDispatch, "", "eri", "")))
				require.Nil(t, rec.saved.PendingDeployFailure)
				require.Equal(t, tc.wantDispatched, rec.saved.WasEffectDispatched(factKey("pr_fact_deploy_success", "m1", "eri")))
			}
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
		})
	}
}

// ciFailedPR seeds an open PR whose head h1 failed CI on the given checks.
func ciFailedPR(checks ...string) *PRState {
	st := effectPR()
	st.Status = RegistryStatusCIFailed
	st.RecordFailedCISHA("h1")
	for _, c := range checks {
		st.RecordFailedCheckRunDetailed("h1", c)
	}
	return st
}

// TestPRObjectServiceHandleCIRecovered pins the merged CI-recovery verdict:
// a CI-failed head clears the recovered check (and the failed-CI SHA once
// no failed check remains), reopens the row, and publishes the recovery
// fact; a healthy PR commits and publishes nothing.
func TestPRObjectServiceHandleCIRecovered(t *testing.T) {
	t.Parallel()
	ev := func(check string) PREvent {
		return PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", CheckName: check, Facts: []pubsub.PRLifecycleEvent{syncFact()}}
	}
	recovered := CIRecoveryDecision{Disposition: DispositionApply, CIFailed: true}
	tests := []struct {
		name           string
		state          *PRState
		failGet        bool
		ev             PREvent
		want           CIRecoveryDecision
		wantErr        bool
		wantCalls      []factCall
		wantSet        bool
		wantFailedSHA  string
		wantDispatched bool
	}{
		{
			name: "restate owner recovers and publishes", state: ciFailedPR("lint"), ev: ev("lint"), want: recovered,
			wantCalls: []factCall{{msgID: testSyncIdem, fact: targeted(pinned(syncFact()))}}, wantSet: true, wantDispatched: true,
		},
		{
			name: "another failing check keeps the failed sha", state: ciFailedPR("lint", "test"), ev: ev("lint"), want: recovered,
			wantCalls: []factCall{{msgID: testSyncIdem, fact: targeted(pinned(syncFact()))}}, wantSet: true, wantFailedSHA: "h1", wantDispatched: true,
		},
		{name: "healthy pr records nothing", state: effectPR(), ev: ev("lint"), want: CIRecoveryDecision{Disposition: DispositionApply}},
		{name: "malformed event is ignored", state: ciFailedPR(), ev: PREvent{PRNumber: 101}},
		{name: "kv failure surfaces", failGet: true, ev: ev(""), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pub := &fakeFactPublisher{}
			svc := NewPRObjectService().WithFactPublisher(pub)
			mc, rec := prKV(t, tc.state, tc.failGet, false)
			stubRun(mc, false)
			got, err := svc.HandleCIRecovered(restate.WithMockContext(mc), tc.ev)
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.wantCalls, pub.Calls())
			require.Equal(t, tc.wantSet, rec.setCalled)
			if tc.wantSet {
				require.Equal(t, RegistryStatusOpen, rec.saved.Status)
				require.Equal(t, tc.wantFailedSHA, rec.saved.FailedCISHA)
				require.Equal(t, tc.wantDispatched, rec.saved.WasEffectDispatched(factKey("pr_fact_synchronize", "h1", "")))
			}
		})
	}
}

// TestPRObjectServiceRetryFactEffect pins the delayed re-publish: it fires
// and commits, re-schedules on failure until the budget is spent, and drops
// retries for gone PRs, moved generations, fired keys, and nameless facts.
func TestPRObjectServiceRetryFactEffect(t *testing.T) {
	t.Parallel()
	moved := effectPR()
	moved.GateGeneration = 1
	fired := effectPR()
	fired.MarkEffectDispatched(factKey("pr_fact_conflict", "h1", ""))
	retry := func(attempt int) FactEffectRetry {
		return FactEffectRetry{Fact: pinned(conflictFact()), Attempt: attempt}
	}
	tests := []struct {
		name           string
		state          *PRState
		failGet        bool
		req            FactEffectRetry
		pub            *fakeFactPublisher
		wantErr        bool
		wantCalls      int
		wantSet        bool
		wantDispatched bool
		wantSends      []sentCall
	}{
		{name: "kv failure surfaces", failGet: true, req: retry(1), pub: &fakeFactPublisher{}, wantErr: true},
		{name: "gone PR drops", req: retry(1), pub: &fakeFactPublisher{}},
		{name: "moved generation drops", state: moved, req: retry(1), pub: &fakeFactPublisher{}},
		{name: "fired key drops", state: fired, req: retry(1), pub: &fakeFactPublisher{}},
		{name: "nameless fact drops", state: effectPR(), req: FactEffectRetry{Fact: pubsub.PRLifecycleEvent{HeadSHA: "h1"}}, pub: &fakeFactPublisher{}},
		{name: "publish commits the key", state: effectPR(), req: retry(1), pub: &fakeFactPublisher{}, wantCalls: 1, wantSet: true, wantDispatched: true},
		{
			name: "failure re-schedules the next attempt", state: effectPR(), req: retry(2), pub: &fakeFactPublisher{err: errFakeBus}, wantCalls: 1,
			wantSends: []sentCall{{service: "PRObject", key: "azylman/aerial", method: PRHandlerRetryFactEffect, input: retry(3), delayed: true}},
		},
		{name: "spent budget drops", state: effectPR(), req: retry(10), pub: &fakeFactPublisher{err: errFakeBus}, wantCalls: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			svc := NewPRObjectService().WithFactPublisher(tc.pub)
			mc, rec := prKV(t, tc.state, tc.failGet, false)
			stubRun(mc, false)
			err := svc.RetryFactEffect(restate.WithMockContext(mc), tc.req)
			require.Equal(t, tc.wantErr, err != nil)
			require.Len(t, tc.pub.Calls(), tc.wantCalls)
			require.Equal(t, tc.wantSet, rec.setCalled)
			if tc.wantSet {
				require.Equal(t, tc.wantDispatched, rec.saved.WasEffectDispatched(factKey("pr_fact_conflict", "h1", "")))
			}
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
		})
	}
}

// TestPRStateArmDeployFailure pins arming: the first failure numbers a new
// deadline at the current generation, a repeat keeps its timer and refreshes
// the banner and facts, and a nil state arms nothing.
func TestPRStateArmDeployFailure(t *testing.T) {
	t.Parallel()
	armed := func() *PRState {
		st := deployingMergedPR()
		st.GateGeneration = 3
		st.ArmDeployFailure(PREvent{MergeSHA: "m1", Banner: "first"})
		return st
	}
	tests := []struct {
		name      string
		state     *PRState
		ev        PREvent
		wantSeq   uint64
		wantArmed bool
		want      *PendingDeployFailure
	}{
		{name: "nil state arms nothing", ev: PREvent{Banner: "x"}},
		{name: "rate limit banner arms nothing", state: deployingMergedPR(), ev: PREvent{Banner: "github api rate limit exceeded"}},
		{name: "rate limit facts arms nothing", state: deployingMergedPR(), ev: PREvent{Facts: []pubsub.PRLifecycleEvent{{Details: "rate limited by GitHub API"}}}},
		{
			name: "first failure arms a numbered deadline", state: deployingMergedPR(), ev: PREvent{HeadSHA: " h1 ", MergeSHA: " m1 ", Banner: " boom ", Facts: []pubsub.PRLifecycleEvent{failedFact()}},
			wantSeq: 1, wantArmed: true, want: &PendingDeployFailure{Seq: 1, HeadSHA: "h1", MergeSHA: "m1", Banner: "boom", Facts: []pubsub.PRLifecycleEvent{failedFact()}},
		},
		{
			name: "repeat failure refreshes banner and facts", state: armed(), ev: PREvent{Banner: "second", Facts: []pubsub.PRLifecycleEvent{failedFact()}},
			wantSeq: 1, want: &PendingDeployFailure{Seq: 1, MergeSHA: "m1", Generation: 3, Banner: "second", Facts: []pubsub.PRLifecycleEvent{failedFact()}},
		},
		{name: "blank repeat keeps the stored banner", state: armed(), ev: PREvent{}, wantSeq: 1, want: &PendingDeployFailure{Seq: 1, MergeSHA: "m1", Generation: 3, Banner: "first"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			seq, ok := tc.state.ArmDeployFailure(tc.ev)
			require.Equal(t, tc.wantSeq, seq)
			require.Equal(t, tc.wantArmed, ok)
			if tc.state != nil {
				require.Equal(t, tc.want, tc.state.PendingDeployFailure)
			}
		})
	}
}

// TestPRStateTakeDeployFailure pins taking a deadline: only the named seq
// fires, a moved generation disarms without firing, and a stale seq leaves
// the newer deadline armed.
func TestPRStateTakeDeployFailure(t *testing.T) {
	t.Parallel()
	armed := func(gen uint64) *PRState {
		st := deployingMergedPR()
		st.ArmDeployFailure(PREvent{MergeSHA: "m1"})
		st.GateGeneration = gen
		return st
	}
	tests := []struct {
		name        string
		state       *PRState
		seq         uint64
		wantTaken   bool
		wantCurrent bool
		wantArmed   bool
	}{
		{name: "nil state takes nothing", seq: 1},
		{name: "unarmed state takes nothing", state: deployingMergedPR(), seq: 1},
		{name: "stale seq leaves the deadline armed", state: armed(0), seq: 9, wantArmed: true},
		{name: "named seq fires", state: armed(0), seq: 1, wantTaken: true, wantCurrent: true},
		{name: "moved generation disarms without firing", state: armed(1), seq: 1, wantTaken: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p, current := tc.state.TakeDeployFailure(tc.seq)
			require.Equal(t, tc.wantTaken, p != nil)
			require.Equal(t, tc.wantCurrent, current)
			if tc.state != nil {
				require.Equal(t, tc.wantArmed, tc.state.PendingDeployFailure != nil)
			}
		})
	}
}

// TestPRObjectServiceHandleDeployAttemptFailed pins the restate-owned
// attempt: the first failure arms a deadline and sends the delayed timer,
// a repeat refreshes without a second timer, and legacy, stale, malformed
// attempts arm nothing.
func TestPRObjectServiceHandleDeployAttemptFailed(t *testing.T) {
	t.Parallel()
	armed := func() *PRState {
		st := deployingMergedPR()
		st.ArmDeployFailure(PREvent{MergeSHA: "m1", Banner: "first"})
		return st
	}
	attempt := func(merge string) PREvent {
		return PREvent{Repo: "azylman/aerial", PRNumber: 101, MergeSHA: merge, Banner: "boom", Facts: []pubsub.PRLifecycleEvent{failedFact()}}
	}
	tests := []struct {
		name       string
		state      *PRState
		failGet    bool
		ev         PREvent
		wantErr    bool
		wantSet    bool
		wantBanner string
		wantSends  []sentCall
	}{
		{
			name: "first restate failure arms the deadline", state: deployingMergedPR(), ev: attempt("m1"), wantSet: true, wantBanner: "boom",
			wantSends: []sentCall{{service: "PRObject", key: "azylman/aerial#101", method: prwire.PRHandlerDeployFailureDeadline, input: DeployFailureDeadlineRequest{Seq: 1}, delayed: true}},
		},
		{
			name: "rate limit failure never arms the deadline", state: deployingMergedPR(),
			ev: PREvent{Repo: "azylman/aerial", PRNumber: 101, MergeSHA: "m1", Banner: "github api rate limit exceeded", Facts: []pubsub.PRLifecycleEvent{failedFact()}},
		},
		{
			name: "fact details rate limit failure never arms the deadline", state: deployingMergedPR(),
			ev: PREvent{Repo: "azylman/aerial", PRNumber: 101, MergeSHA: "m1", Banner: "deploy failed", Facts: []pubsub.PRLifecycleEvent{{Details: "GitHub API rate limited"}}},
		},
		{name: "repeat failure refreshes without a second timer", state: armed(), ev: attempt("m1"), wantSet: true, wantBanner: "boom"},
		{name: "stale sha drops", state: deployingMergedPR(), ev: attempt("m9")},
		{name: "malformed attempt is ignored", state: deployingMergedPR(), ev: PREvent{Repo: "azylman/aerial"}},
		{name: "kv failure surfaces", failGet: true, ev: attempt("m1"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			svc := NewPRObjectService()
			mc, rec := prKV(t, tc.state, tc.failGet, false)
			err := svc.HandleDeployAttemptFailed(restate.WithMockContext(mc), tc.ev)
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.wantSet, rec.setCalled)
			if tc.wantSet {
				require.Equal(t, tc.wantBanner, rec.saved.PendingDeployFailure.Banner)
			}
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
		})
	}
}

// TestPRObjectServiceHandleDeployFailureDeadline pins the deadline: the
// current attempt applies deploy_failed and publishes the captured fact; a
// PR already in deploy_failed re-enters it and publishes the newest failure,
// while a fact whose effect key already fired is not sent again; a failed
// publish schedules a retry; stale, superseded, and unfailable deadlines
// only disarm or do nothing.
func TestPRObjectServiceHandleDeployFailureDeadline(t *testing.T) {
	t.Parallel()
	armed := func(status string, gen uint64) *PRState {
		st := deployingMergedPR()
		st.Status = status
		st.ArmDeployFailure(PREvent{MergeSHA: "m1", Banner: "boom", Facts: []pubsub.PRLifecycleEvent{failedFact()}})
		st.GateGeneration = gen
		return st
	}
	redelivered := armed(RegistryStatusDeployFailed, 0)
	redelivered.MarkEffectDispatched(factKey("pr_fact_deploy_failed", "m1", ""))
	legacyAcked := legacyPRState(t, armed(RegistryStatusDeployFailed, 0))
	tests := []struct {
		name       string
		state      *PRState
		failGet    bool
		seq        uint64
		pub        *fakeFactPublisher
		wantErr    bool
		wantCalls  []factCall
		wantSet    bool
		wantStatus string
		wantSends  []sentCall
	}{
		{
			name: "current attempt fails and publishes", state: armed(RegistryStatusDeploying, 0), seq: 1, pub: &fakeFactPublisher{},
			wantCalls: []factCall{{msgID: testFailedIdem, fact: pinned(failedFact())}}, wantSet: true, wantStatus: RegistryStatusDeployFailed,
		},
		{
			name: "repeated failure on a deploy failed pr publishes the newest failure", state: armed(RegistryStatusDeployFailed, 0), seq: 1, pub: &fakeFactPublisher{},
			wantCalls: []factCall{{msgID: testFailedIdem, fact: pinned(failedFact())}}, wantSet: true, wantStatus: RegistryStatusDeployFailed,
		},
		{name: "redelivered failure sends no duplicate notification", state: redelivered, seq: 1, pub: &fakeFactPublisher{}, wantSet: true, wantStatus: RegistryStatusDeployFailed},
		{
			name: "old blob with a published deploy failure still publishes the newest failure", state: legacyAcked, seq: 1, pub: &fakeFactPublisher{},
			wantCalls: []factCall{{msgID: testFailedIdem, fact: pinned(failedFact())}}, wantSet: true, wantStatus: RegistryStatusDeployFailed,
		},
		{
			name: "failed publish schedules a retry", state: armed(RegistryStatusDeploying, 0), seq: 1, pub: &fakeFactPublisher{err: errFakeBus},
			wantCalls: []factCall{{msgID: testFailedIdem, fact: pinned(failedFact())}}, wantSet: true, wantStatus: RegistryStatusDeployFailed,
			wantSends: []sentCall{{service: "PRObject", key: "azylman/aerial", method: PRHandlerRetryFactEffect, input: FactEffectRetry{Fact: pinned(failedFact()), Attempt: 1}, delayed: true}},
		},
		{name: "moved generation only disarms", state: armed(RegistryStatusDeploying, 1), seq: 1, pub: &fakeFactPublisher{}, wantSet: true, wantStatus: RegistryStatusDeploying},
		{name: "unfailable status only disarms", state: armed(RegistryStatusDeployed, 0), seq: 1, pub: &fakeFactPublisher{}, wantSet: true, wantStatus: RegistryStatusDeployed},
		{name: "stale seq does nothing", state: armed(RegistryStatusDeploying, 0), seq: 7, pub: &fakeFactPublisher{}},
		{name: "gone PR does nothing", seq: 1, pub: &fakeFactPublisher{}},
		{name: "kv failure surfaces", failGet: true, seq: 1, pub: &fakeFactPublisher{}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			svc := NewPRObjectService().WithFactPublisher(tc.pub)
			mc, rec := prKV(t, tc.state, tc.failGet, false)
			stubRun(mc, false)
			err := svc.HandleDeployFailureDeadline(restate.WithMockContext(mc), DeployFailureDeadlineRequest{Seq: tc.seq})
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.wantCalls, tc.pub.Calls())
			require.Equal(t, tc.wantSet, rec.setCalled)
			if tc.wantSet {
				require.Nil(t, rec.saved.PendingDeployFailure)
				require.Equal(t, tc.wantStatus, rec.saved.Status)
			}
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
		})
	}
}

// TestPRObjectServiceLifecycleFacts pins that lifecycle handlers run the
// facts their events carry after the state commit: the fact publishes and
// a dropped event fires nothing.
func TestPRObjectServiceLifecycleFacts(t *testing.T) {
	t.Parallel()
	merged := pubsub.PRLifecycleEvent{Event: "merged", HeadSHA: "h1", MergeSHA: "m1", Status: "merged"}
	carried := merged
	carried.TargetID = "333333333333333333"

	mainFact := merged
	mainFact.Branch = "main"

	refsMainFact := merged
	refsMainFact.Branch = "refs/heads/main"

	featureFact := merged
	featureFact.Branch = "feat/other-feature"
	featureFact.Title = "Carried Title"

	tests := []struct {
		name        string
		stateBranch string
		stateTitle  string
		ev          PREvent
		wantCalls   int
		wantErr     bool
		wantTarget  string
		wantBranch  string
		wantTitle   string
	}{
		{name: "restate owner publishes the fact with the stored target", ev: PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", MergeSHA: "m1", Facts: []pubsub.PRLifecycleEvent{merged}}, wantCalls: 1, wantTarget: testTarget},
		{name: "restate owner keeps a carried target", ev: PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", MergeSHA: "m1", Facts: []pubsub.PRLifecycleEvent{carried}}, wantCalls: 1, wantTarget: "333333333333333333"},
		{name: "stale event fires nothing", ev: PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h9", Facts: []pubsub.PRLifecycleEvent{merged}}},
		{
			name:        "empty fact branch and title take stored PR branch and title",
			stateBranch: "feat/aerial-pr-vanadis-c1-abcdef",
			stateTitle:  "Add Vanadis feature",
			ev:          PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", MergeSHA: "m1", Facts: []pubsub.PRLifecycleEvent{merged}},
			wantCalls:   1,
			wantTarget:  testTarget,
			wantBranch:  "feat/aerial-pr-vanadis-c1-abcdef",
			wantTitle:   "Add Vanadis feature",
		},
		{
			name:        "main branch on fact is replaced by stored PR head branch",
			stateBranch: "feat/aerial-pr-vanadis-c1-abcdef",
			stateTitle:  "Add Vanadis feature",
			ev:          PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", MergeSHA: "m1", Facts: []pubsub.PRLifecycleEvent{mainFact}},
			wantCalls:   1,
			wantTarget:  testTarget,
			wantBranch:  "feat/aerial-pr-vanadis-c1-abcdef",
			wantTitle:   "Add Vanadis feature",
		},
		{
			name:        "refs/heads/main branch on fact is replaced by stored PR head branch",
			stateBranch: "feat/aerial-pr-vanadis-c1-abcdef",
			stateTitle:  "Add Vanadis feature",
			ev:          PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", MergeSHA: "m1", Facts: []pubsub.PRLifecycleEvent{refsMainFact}},
			wantCalls:   1,
			wantTarget:  testTarget,
			wantBranch:  "feat/aerial-pr-vanadis-c1-abcdef",
			wantTitle:   "Add Vanadis feature",
		},
		{
			name:        "carried distinct feature branch and title are preserved",
			stateBranch: "feat/aerial-pr-vanadis-c1-abcdef",
			stateTitle:  "Stored Title",
			ev:          PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", MergeSHA: "m1", Facts: []pubsub.PRLifecycleEvent{featureFact}},
			wantCalls:   1,
			wantTarget:  testTarget,
			wantBranch:  "feat/other-feature",
			wantTitle:   "Carried Title",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pub := &fakeFactPublisher{}
			st := effectPR()
			if tc.stateBranch != "" {
				st.Branch = tc.stateBranch
			}
			if tc.stateTitle != "" {
				st.Title = tc.stateTitle
			}
			mc, rec := newStateMock(t, st)
			require.NotNil(t, rec)
			stubRun(mc, false)
			err := NewPRObjectService().WithFactPublisher(pub).HandleMerged(restate.WithMockContext(mc), tc.ev)
			require.Equal(t, tc.wantErr, restate.IsTerminalError(err), "err = %v", err)
			calls := pub.Calls()
			require.Len(t, calls, tc.wantCalls)
			if tc.wantCalls > 0 {
				require.Equal(t, tc.wantTarget, calls[0].fact.TargetID)
				require.Equal(t, tc.wantBranch, calls[0].fact.Branch)
				require.Equal(t, tc.wantTitle, calls[0].fact.Title)
			}
		})
	}
}

// targeted stamps the effectPR thread target a restate-owned publish fills
// into a fact that carried none.
func targeted(f pubsub.PRLifecycleEvent) pubsub.PRLifecycleEvent {
	f.TargetID = testTarget
	return f
}

// legacyPRState round-trips st through the Restate JSON codec with every
// field the old in-memory tracker stored, as an old stored blob carries
// them: removed fields must decode cleanly and change nothing.
func legacyPRState(t *testing.T, st *PRState) *PRState {
	t.Helper()
	//nolint:musttag // PRState is Restate-owned state with intentionally untagged fields; this mirrors the stored K/V blob.
	raw, err := json.Marshal(st)
	require.NoError(t, err)
	var blob map[string]any
	require.NoError(t, json.Unmarshal(raw, &blob))
	blob["NonRoboRevSHA"], blob["CancelledSHA"] = "h1", "h1"
	blob["CollaboratorAlerted"], blob["CollaboratorConflict"] = "h1", "h1"
	blob["UpdatedForMain"], blob["DeployPRDispatched"], blob["DeployFailedPublished"] = true, true, true
	blob["DeploySHAJob"] = map[string]bool{"sha:azylman/aerial:eri:h1": true}
	raw, err = json.Marshal(blob)
	require.NoError(t, err)
	var out PRState
	//nolint:musttag // PRState is Restate-owned state with intentionally untagged fields; this mirrors the stored K/V blob.
	require.NoError(t, json.Unmarshal(raw, &out))
	return &out
}

// TestPRObjectServiceHandleCheckStatus pins the facts-only check-status
// handler: an applied event commits the row and publishes the check_updated
// fact it carries (Vanadis records the check state from that fact), and a
// closed PR absorbs the event without publishing.
func TestPRObjectServiceHandleCheckStatus(t *testing.T) {
	t.Parallel()
	checkFact := pubsub.PRLifecycleEvent{Event: "check_updated", HeadSHA: "h1", CheckName: "lint", CheckState: "pending"}
	ev := PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", CheckName: "lint", CheckState: "pending", Facts: []pubsub.PRLifecycleEvent{checkFact}}
	closed := effectPR()
	closed.Terminal = TerminalClosed
	tests := []struct {
		name      string
		state     *PRState
		ev        PREvent
		wantCalls []factCall
		wantSet   bool
	}{
		{
			name: "open pr publishes the check_updated fact", state: effectPR(), ev: ev, wantSet: true,
			wantCalls: []factCall{{msgID: "prstate-644f18e690a8262e950d51cb18a18a40", fact: targeted(pinned(checkFact))}},
		},
		{name: "closed pr absorbs the event", state: closed, ev: ev},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pub := &fakeFactPublisher{}
			svc := NewPRObjectService().WithFactPublisher(pub)
			mc, rec := prKV(t, tc.state, false, false)
			stubRun(mc, false)
			require.NoError(t, svc.HandleCheckStatus(restate.WithMockContext(mc), tc.ev))
			require.Equal(t, tc.wantCalls, pub.Calls())
			require.Equal(t, tc.wantSet, rec.setCalled)
		})
	}
}
