package prstate

import (
	"context"
	"errors"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/prwire"
)

func TestJournalKube(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		kube         *fakeKube
		wantWorkload string
		wantStatus   int
		wantRetry    bool
		wantNilErr   bool
	}{
		{name: "success replays the value", kube: &fakeKube{}, wantWorkload: "eri", wantNilErr: true},
		{name: "server error exhausts step retries", kube: &fakeKube{promoteErrs: map[string]*KubeError{"eri": errKubeDown}}, wantRetry: true},
		{name: "terminal 4xx keeps its status", kube: &fakeKube{promoteErrs: map[string]*KubeError{"eri": errKubeForbidden}}, wantStatus: 403},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc := mocks.NewMockContext(t)
			stubRunKeepTerminal(mc)
			j := journaledKubeFor(restate.WithMockContext(mc), tc.kube)
			workload, kerr := j.PromoteImageTag(context.Background(), ImagePromotionRequest{Image: "eri", CommitSHA: "s1"})
			require.Equal(t, tc.wantWorkload, workload)
			if tc.wantNilErr {
				require.Nil(t, kerr)
				return
			}
			require.Equal(t, tc.wantStatus, kerr.Status)
			require.Equal(t, tc.wantRetry, kerr.Retryable())
		})
	}
}

func TestJournaledKubeCalls(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		kube      *fakeKube
		wantCalls []string
		wantPush  bool
		wantRecon bool
	}{
		{name: "push and reconcile journal through the client", kube: &fakeKube{}, wantCalls: []string{"push azylman/aerial refs/heads/main m1 ", "reconcile flux-system/core@t1"}},
		{name: "failures surface from both calls", kube: &fakeKube{pushErr: errKubeForbidden, reconcileErr: map[string]*KubeError{"core": errKubeForbidden}}, wantCalls: []string{"push azylman/aerial refs/heads/main m1 ", "reconcile flux-system/core@t1"}, wantPush: true, wantRecon: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc := mocks.NewMockContext(t)
			stubRunKeepTerminal(mc)
			j := journaledKubeFor(restate.WithMockContext(mc), tc.kube)
			pushErr := j.DispatchGitPush(context.Background(), GitPushRequest{Repo: "azylman/aerial", Ref: "refs/heads/main", Commit: "m1"})
			reconErr := j.ReconcileKustomization(context.Background(), "flux-system", "core", "t1")
			require.Equal(t, tc.wantPush, pushErr != nil)
			require.Equal(t, tc.wantRecon, reconErr != nil)
			require.Equal(t, tc.wantCalls, tc.kube.Calls())
		})
	}
}

func TestJournaledKubeForNilClient(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
	}{
		{name: "no client wires no journaled boundary"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Nil(t, journaledKubeFor(restate.WithMockContext(mocks.NewMockContext(t)), nil))
		})
	}
}

func TestKubeStepError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantRetry  bool
	}{
		{name: "terminal forbidden stays terminal", err: restate.ToTerminalError(errors.New("rbac"), restate.WithErrorCode(403)), wantStatus: 403},
		{name: "terminal conflict stays retryable", err: restate.ToTerminalError(errors.New("race"), restate.WithErrorCode(409)), wantStatus: 409, wantRetry: true},
		{name: "exhausted step is retryable", err: restate.TerminalErrorf("exhausted"), wantRetry: true},
		{name: "plain error is retryable", err: errors.New("boom"), wantRetry: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := kubeStepError(tc.err)
			require.Equal(t, tc.wantStatus, got.Status)
			require.Equal(t, tc.wantRetry, got.Retryable())
		})
	}
}

func TestRepoObjectServiceHandleGitPush(t *testing.T) {
	t.Parallel()
	push := func(attempt int) RepoEvent {
		return RepoEvent{Repo: "azylman/aerial", MainSHA: "m1", Ref: "refs/heads/main", Attempt: attempt}
	}
	tests := []struct {
		name      string
		ev        RepoEvent
		kube      *fakeKube
		wantErr   bool
		wantSet   bool
		wantRun   bool
		wantSends []sentCall
	}{
		{name: "restate push dispatches", ev: push(0), kube: &fakeKube{}, wantSet: true, wantRun: true},
		{
			name: "retryable failure schedules a delayed retry", ev: push(1), kube: &fakeKube{pushErr: errKubeDown}, wantRun: true,
			wantSends: []sentCall{{service: "RepoObject", key: "azylman/aerial", method: prwire.RepoHandlerGitPush, input: push(2), delayed: true}},
		},
		{name: "non-main ref fails terminally", ev: RepoEvent{Repo: "azylman/aerial", MainSHA: "m1", Ref: "refs/tags/v1"}, kube: &fakeKube{}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := newStateMock[*RepoState](t, nil)
			run := stubRunKeepTerminal(mc)
			err := NewRepoObjectService().WithKube(tc.kube).HandleGitPush(restate.WithMockContext(mc), tc.ev)
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.wantSet, rec.setCalled)
			require.Equal(t, tc.wantRun, run.ran)
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
		})
	}
}

func TestRepoObjectServiceHandleImagePromotion(t *testing.T) {
	t.Parallel()
	requests := map[string]KustomizationRequest{"core": {Namespace: "flux-system", RequestedAt: "t1"}}
	tests := []struct {
		name      string
		ev        RepoEvent
		kube      *fakeKube
		wantErr   bool
		wantSet   bool
		wantRun   bool
		wantCalls []string
		wantSends []sentCall
	}{
		{
			name: "unstamped untracked promotion promotes and reconciles", ev: RepoEvent{Repo: "azylman/aerial", HeadSHA: "s2", Images: []string{"eri"}, KustomizationRequests: requests}, kube: &fakeKube{},
			wantSet: true, wantRun: true, wantCalls: []string{"promote eri@s2", "reconcile flux-system/core@t1"},
		},
		{
			name: "restate pr promotion forwards the rollout", ev: RepoEvent{Repo: "azylman/aerial", HeadSHA: "s1", PRNumber: 7, Images: []string{"eri"}, KustomizationRequests: requests},
			kube: &fakeKube{}, wantSet: true, wantRun: true, wantCalls: []string{"promote eri@s1"},
			wantSends: []sentCall{{service: "PRObject", key: "azylman/aerial#7", method: prwire.PRHandlerKustomizationRequested, input: PREvent{
				Event: "kustomization_requested", Repo: "azylman/aerial", PRNumber: 7, HeadSHA: "s1", KustomizationRequests: requests,
			}}},
		},
		{name: "missing images fail terminally", ev: RepoEvent{Repo: "azylman/aerial", HeadSHA: "s1"}, kube: &fakeKube{}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := newStateMock[*RepoState](t, nil)
			run := stubRunKeepTerminal(mc)
			err := NewRepoObjectService().WithKube(tc.kube).HandleImagePromotion(restate.WithMockContext(mc), tc.ev)
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.wantSet, rec.setCalled)
			require.Equal(t, tc.wantRun, run.ran)
			require.Equal(t, tc.wantCalls, tc.kube.Calls())
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
		})
	}
}

// coreRequests is the single recorded reconcile request the PR rows use.
var coreRequests = map[string]KustomizationRequest{"core": {Namespace: "flux-system", RequestedAt: "t1"}}

// deployedRollout seeds a merged PR whose generation-2 rollout of core
// deployed.
func deployedRollout() *PRState {
	st := svcRow("h1", "m1", RegistryStatusDeployed, TerminalMerged)
	st.Agent = "agent-alpha"
	st.GateGeneration = 2
	st.KustomizationRequests = map[string]KustomizationRequest{"core": {Namespace: "flux-system", RequestedAt: "t1"}}
	st.DeployedKustomizations = []string{"core"}
	st.DeployedJobs = []string{"eri"}
	return st
}

func TestPRObjectServiceHandleKustomizationRequestedReconcile(t *testing.T) {
	t.Parallel()
	gen1Key := EffectKey{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "m1", GateGeneration: 1, Kind: prwire.EffectFluxReconcile}
	indexSend := prIndexSend(PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", MergeSHA: "m1", Generation: 1, Lifecycle: IndexDeploying})
	retrySend := sentCall{service: "PRObject", key: "azylman/aerial#101", method: prwire.PRHandlerRetryFluxReconcile, input: FluxReconcileRetryRequest{Generation: 1, HeadSHA: "m1", Attempt: 1}, delayed: true}
	ev := func(requests map[string]KustomizationRequest) PREvent {
		return PREvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "m1", KustomizationRequests: requests}
	}
	tests := []struct {
		name           string
		ev             PREvent
		kube           *fakeKube
		wantCalls      []string
		wantDispatched bool
		wantErr        bool
		wantSends      []sentCall
	}{
		{
			name: "restate request patches after the commit", ev: ev(coreRequests), kube: &fakeKube{},
			wantCalls: []string{"reconcile flux-system/core@t1"}, wantDispatched: true, wantSends: []sentCall{indexSend},
		},
		{
			name: "retryable patch failure schedules a delayed retry", ev: ev(coreRequests), kube: &fakeKube{reconcileErr: map[string]*KubeError{"core": errKubeDown}},
			wantCalls: []string{"reconcile flux-system/core@t1"}, wantSends: []sentCall{indexSend, retrySend},
		},
		{name: "request without kustomizations patches nothing", ev: ev(nil), kube: &fakeKube{}, wantSends: []sentCall{indexSend}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			seed := svcRow("h1", "m1", RegistryStatusMerged, TerminalMerged)
			seed.Agent = "agent-alpha"
			mc, rec := newStateMock(t, seed)
			stubRunKeepTerminal(mc)
			err := NewPRObjectService().WithKube(tc.kube).HandleKustomizationRequested(restate.WithMockContext(mc), tc.ev)
			require.Equal(t, tc.wantErr, restate.IsTerminalError(err), "err = %v", err)
			require.Equal(t, tc.wantCalls, tc.kube.Calls())
			require.Equal(t, tc.wantDispatched, rec.saved.WasEffectDispatched(gen1Key))
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
		})
	}
}

func TestPRObjectServiceRetryReadiness(t *testing.T) {
	t.Parallel()
	readinessKey := "azylman/aerial#101@m1:gen2:flux_reconcile:job=readiness-1"
	indexSend := prIndexSend(PRIndexEvent{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", MergeSHA: "m1", Generation: 2, Lifecycle: IndexDeploying})
	tests := []struct {
		name           string
		seed           *PRState
		req            RetryReadinessRequest
		kube           *fakeKube
		want           RetryReadinessResponse
		wantErr        bool
		wantSet        bool
		wantStatus     string
		wantCalls      []string
		wantDispatched bool
		wantSends      []sentCall
	}{
		{name: "unknown pr has no gate", req: RetryReadinessRequest{MergeSHA: "m1"}, kube: &fakeKube{}, want: RetryReadinessResponse{Status: 409}},
		{name: "mismatched merge sha has no gate", seed: deployedRollout(), req: RetryReadinessRequest{MergeSHA: "m9"}, kube: &fakeKube{}, want: RetryReadinessResponse{Status: 409}},
		{
			name: "unstamped retry re-requests the original reconcile", seed: deployedRollout(), req: RetryReadinessRequest{MergeSHA: "m1"}, kube: &fakeKube{},
			want:    RetryReadinessResponse{Status: 202, Requests: coreRequests, EffectKey: readinessKey},
			wantSet: true, wantStatus: RegistryStatusDeploying, wantCalls: []string{"reconcile flux-system/core@t1"}, wantDispatched: true, wantSends: []sentCall{indexSend, rolloutIndexSend(1, true)},
		},
		{
			name: "restate retry re-requests the original reconcile", seed: deployedRollout(), req: RetryReadinessRequest{MergeSHA: "m1"}, kube: &fakeKube{},
			want:    RetryReadinessResponse{Status: 202, Requests: coreRequests, EffectKey: readinessKey},
			wantSet: true, wantStatus: RegistryStatusDeploying, wantCalls: []string{"reconcile flux-system/core@t1"}, wantDispatched: true, wantSends: []sentCall{indexSend, rolloutIndexSend(1, true)},
		},
		{name: "missing merge sha fails terminally", req: RetryReadinessRequest{}, kube: &fakeKube{}, wantErr: true},
		{name: "fenced caller for another owner fails terminally", seed: deployedRollout(), req: RetryReadinessRequest{MergeSHA: "m1", EnforceOwner: true, ExpectedOwner: "agent-beta"}, kube: &fakeKube{}, wantErr: true},
		{
			name: "fenced caller for the stored owner is accepted", seed: deployedRollout(), req: RetryReadinessRequest{MergeSHA: "m1", EnforceOwner: true, ExpectedOwner: "agent-alpha"}, kube: &fakeKube{},
			want:    RetryReadinessResponse{Status: 202, Requests: coreRequests, EffectKey: readinessKey},
			wantSet: true, wantStatus: RegistryStatusDeploying, wantCalls: []string{"reconcile flux-system/core@t1"}, wantDispatched: true, wantSends: []sentCall{indexSend, rolloutIndexSend(1, true)},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := newStateMock(t, tc.seed)
			stubRunKeepTerminal(mc)
			got, err := NewPRObjectService().WithKube(tc.kube).RetryReadiness(restate.WithMockContext(mc), tc.req)
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.wantSet, rec.setCalled)
			require.Equal(t, tc.wantCalls, tc.kube.Calls())
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
			if !tc.wantSet {
				return
			}
			require.Equal(t, tc.wantStatus, rec.saved.Status)
			require.Empty(t, rec.saved.DeployedKustomizations)
			require.Equal(t, uint64(2), rec.saved.GateGeneration)
			require.Equal(t, tc.wantDispatched, rec.saved.WasEffectDispatched(EffectKey{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "m1", GateGeneration: 2, Kind: prwire.EffectFluxReconcile, JobName: "readiness-1"}))
		})
	}
}

func TestPRObjectServiceHandleReadinessRetry(t *testing.T) {
	t.Parallel()
	merged := func() *PRState { return svcRow("h1", "m1", RegistryStatusMerged, TerminalMerged) }
	tests := []struct {
		name       string
		seed       *PRState
		ev         PREvent
		wantErr    bool
		wantSet    bool
		wantStatus string
		wantCalls  []string
		wantSends  int
	}{
		{name: "retry applies the gate and re-requests the reconcile", seed: deployedRollout(), ev: PREvent{Repo: "azylman/aerial", PRNumber: 101, MergeSHA: "m1"}, wantSet: true, wantStatus: RegistryStatusDeploying, wantCalls: []string{"reconcile flux-system/core@t1"}, wantSends: 2},
		{name: "stale merge sha drops", seed: deployedRollout(), ev: PREvent{Repo: "azylman/aerial", PRNumber: 101, MergeSHA: "m9"}},
		{name: "merged pr without a rollout persists unchanged", seed: merged(), ev: PREvent{Repo: "azylman/aerial", PRNumber: 101, MergeSHA: "m1"}, wantSet: true, wantStatus: RegistryStatusMerged},
		{name: "missing merge sha does nothing", seed: deployedRollout(), ev: PREvent{Repo: "azylman/aerial", PRNumber: 101}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := newStateMock(t, tc.seed)
			stubRunKeepTerminal(mc)
			kube := &fakeKube{}
			err := NewPRObjectService().WithKube(kube).HandleReadinessRetry(restate.WithMockContext(mc), tc.ev)
			require.Equal(t, tc.wantErr, restate.IsTerminalError(err), "err = %v", err)
			require.Equal(t, tc.wantSet, rec.setCalled)
			require.Equal(t, tc.wantCalls, kube.Calls())
			require.Len(t, rec.sends.snapshot(), tc.wantSends)
			if !tc.wantSet {
				return
			}
			require.Equal(t, tc.wantStatus, rec.saved.Status)
		})
	}
}

func TestPRObjectServiceRetryFluxReconcile(t *testing.T) {
	t.Parallel()
	deploying := func() *PRState {
		st := deployedRollout()
		st.Status = RegistryStatusDeploying
		return st
	}
	tests := []struct {
		name           string
		seed           *PRState
		req            FluxReconcileRetryRequest
		kube           *fakeKube
		noKube         bool
		wantSet        bool
		wantCalls      []string
		wantDispatched bool
		wantSends      []sentCall
	}{
		{name: "unknown pr does nothing", req: FluxReconcileRetryRequest{Generation: 2, HeadSHA: "m1"}, kube: &fakeKube{}},
		{name: "moved generation does nothing", seed: deploying(), req: FluxReconcileRetryRequest{Generation: 1, HeadSHA: "m1"}, kube: &fakeKube{}},
		{
			name: "current generation re-requests", seed: deploying(), req: FluxReconcileRetryRequest{Generation: 2, HeadSHA: "m1", JobName: "readiness-1", Attempt: 2}, kube: &fakeKube{},
			wantSet: true, wantCalls: []string{"reconcile flux-system/core@t1"}, wantDispatched: true,
		},
		{
			name: "spent retry budget drops", seed: deploying(), req: FluxReconcileRetryRequest{Generation: 2, HeadSHA: "m1", JobName: "readiness-1", Attempt: maxRepoEffectRetries},
			kube: &fakeKube{reconcileErr: map[string]*KubeError{"core": errKubeDown}}, wantCalls: []string{"reconcile flux-system/core@t1"},
		},
		{
			name: "missing kube client reschedules", seed: deploying(), req: FluxReconcileRetryRequest{Generation: 2, HeadSHA: "m1", JobName: "readiness-1", Attempt: 1}, noKube: true,
			wantSends: []sentCall{{service: "PRObject", key: "azylman/aerial#101", method: prwire.PRHandlerRetryFluxReconcile, input: FluxReconcileRetryRequest{Generation: 2, HeadSHA: "m1", JobName: "readiness-1", Attempt: 2}, delayed: true}},
		},
		{
			name: "terminal failure commits", seed: deploying(), req: FluxReconcileRetryRequest{Generation: 2, HeadSHA: "m1", JobName: "readiness-1", Attempt: 1},
			kube: &fakeKube{reconcileErr: map[string]*KubeError{"core": errKubeForbidden}}, wantSet: true, wantCalls: []string{"reconcile flux-system/core@t1"}, wantDispatched: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := newStateMock(t, tc.seed)
			stubRunKeepTerminal(mc)
			svc := NewPRObjectService()
			if !tc.noKube {
				svc = svc.WithKube(tc.kube)
			}
			require.NoError(t, svc.RetryFluxReconcile(restate.WithMockContext(mc), tc.req))
			if tc.kube != nil {
				require.Equal(t, tc.wantCalls, tc.kube.Calls())
			}
			require.Equal(t, tc.wantSet, rec.setCalled)
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
			if tc.seed != nil {
				require.Equal(t, tc.wantDispatched, tc.seed.WasEffectDispatched(EffectKey{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "m1", GateGeneration: 2, Kind: prwire.EffectFluxReconcile, JobName: "readiness-1"}))
			}
		})
	}
}
