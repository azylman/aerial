package prstate

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
)

// fakeKube is the hermetic KubeEffector: canned failures per call kind
// (per image for promotions, per Kustomization for reconciles) and an
// ordered call log.
type fakeKube struct {
	mu           sync.Mutex
	pushErr      *KubeError
	promoteErrs  map[string]*KubeError
	reconcileErr map[string]*KubeError
	calls        []string
}

func (f *fakeKube) record(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
}

func (f *fakeKube) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeKube) DispatchGitPush(_ context.Context, req GitPushRequest) *KubeError {
	f.record("push " + req.Repo + " " + req.Ref + " " + req.Commit + " " + req.TargetID)
	return f.pushErr
}

func (f *fakeKube) PromoteImageTag(_ context.Context, req ImagePromotionRequest) (string, *KubeError) {
	f.record("promote " + req.Image + "@" + req.CommitSHA)
	if err := f.promoteErrs[req.Image]; err != nil {
		return "", err
	}
	return req.Image, nil
}

func (f *fakeKube) ReconcileKustomization(_ context.Context, namespace, name, requestedAt string) *KubeError {
	f.record("reconcile " + namespace + "/" + name + "@" + requestedAt)
	return f.reconcileErr[name]
}

var (
	errKubeDown      = &KubeError{Status: 503, Message: "unavailable"}
	errKubeForbidden = &KubeError{Status: 403, Message: "forbidden"}
)

func TestRepoHandlersHandleGitPush(t *testing.T) {
	t.Parallel()
	pushKey := EffectKey{Repo: "azylman/aerial", PRNumber: 7, HeadSHA: "m1", Kind: EffectFluxGitPush}
	pushKeyStr := "azylman/aerial#7@m1:gen0:flux_git_push"
	push := func(owner EffectsOwner) RepoEvent {
		return RepoEvent{Repo: "aerial", MainSHA: "m1", PRNumber: 7, Ref: "refs/heads/main", TargetID: "t1", EffectsOwner: owner}
	}
	tests := []struct {
		name        string
		ev          RepoEvent
		seed        *RepoState
		kube        *fakeKube
		noKube      bool
		wantErr     error
		wantOutcome RepoEffectOutcome
		wantCalls   []string
		wantEffects []string
	}{
		{
			name: "restate push dispatches once", ev: push(EffectsOwnerRestate), kube: &fakeKube{},
			wantOutcome: RepoEffectOutcome{Key: pushKey, Executed: true},
			wantCalls:   []string{"push azylman/aerial refs/heads/main m1 t1"},
			wantEffects: []string{pushKeyStr},
		},
		{
			name: "direct commit with no pr still dispatches", ev: RepoEvent{Repo: "aerial", MainSHA: "m2", Ref: "refs/heads/main", EffectsOwner: EffectsOwnerRestate}, kube: &fakeKube{},
			wantOutcome: RepoEffectOutcome{Key: EffectKey{Repo: "azylman/aerial", HeadSHA: "m2", Kind: EffectFluxGitPush}, Executed: true},
			wantCalls:   []string{"push azylman/aerial refs/heads/main m2 "},
			wantEffects: []string{"azylman/aerial#0@m2:gen0:flux_git_push"},
		},
		{
			name: "dispatched push dedupes", ev: push(EffectsOwnerRestate), kube: &fakeKube{},
			seed:        &RepoState{Repo: "azylman/aerial", RepoEffects: []string{pushKeyStr}},
			wantOutcome: RepoEffectOutcome{Key: pushKey, Deduped: true},
			wantEffects: []string{pushKeyStr},
		},
		{
			name: "retryable receiver failure leaves the key open", ev: push(EffectsOwnerRestate), kube: &fakeKube{pushErr: errKubeDown},
			wantOutcome: RepoEffectOutcome{Key: pushKey, Retry: true, Kube: errKubeDown},
			wantCalls:   []string{"push azylman/aerial refs/heads/main m1 t1"},
			wantEffects: []string{},
		},
		{
			name: "terminal receiver failure commits", ev: push(EffectsOwnerRestate), kube: &fakeKube{pushErr: errKubeForbidden},
			wantOutcome: RepoEffectOutcome{Key: pushKey, Kube: errKubeForbidden},
			wantCalls:   []string{"push azylman/aerial refs/heads/main m1 t1"},
			wantEffects: []string{pushKeyStr},
		},
		{
			name: "missing kube client retries", ev: push(EffectsOwnerRestate), noKube: true,
			wantOutcome: RepoEffectOutcome{Key: pushKey, Retry: true, Kube: errKubeUnconfigured},
			wantEffects: []string{},
		},
		{name: "non-main ref is invalid", ev: RepoEvent{Repo: "aerial", MainSHA: "m1", Ref: "refs/heads/feat"}, kube: &fakeKube{}, wantErr: ErrInvalidRepoEvent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemRepoStore()
			store.SaveRepo(&RepoState{Repo: "azylman/aerial", RepoEffects: []string{}})
			if tc.seed != nil {
				store.SaveRepo(tc.seed)
			}
			h := NewRepoHandlers(store, nil, &fakePRSender{}, nil)
			if !tc.noKube {
				h = h.WithKube(tc.kube)
			}
			out, err := h.HandleGitPush(context.Background(), tc.ev)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantOutcome, out)
			if tc.kube != nil {
				require.Equal(t, tc.wantCalls, tc.kube.Calls())
			}
			effects := storedRepoEffects(t, store)
			if tc.wantErr == nil {
				require.Equal(t, tc.wantEffects, effects)
			}
		})
	}
}

// Image promotion fixtures: core and apps requested at t1, the PR rollout's
// deploy_started fact, and the deploy_failed fact a failure fills in.
var (
	promotionRequests = map[string]KustomizationRequest{"core": {Namespace: "flux-system", RequestedAt: "t1"}, "apps": {Namespace: "flux-system", RequestedAt: "t1"}}
	promotionStarted  = pubsub.PRLifecycleEvent{Event: "deploy_started", Repo: "azylman/aerial", PRNumber: 7}
	promotionFailed   = pubsub.PRLifecycleEvent{Event: "deploy_failed", Repo: "azylman/aerial", PRNumber: 7}
)

// prPromotion is a tracked PR #7 promotion of eri and haro at s1.
func prPromotion(owner EffectsOwner) RepoEvent {
	return RepoEvent{
		Repo: "aerial", HeadSHA: "s1", PRNumber: 7, Branch: "feat", TargetID: "t9", Images: []string{"eri", "haro"}, Jobs: []string{"eri"},
		KustomizationRequests: promotionRequests, Facts: []pubsub.PRLifecycleEvent{promotionStarted}, DeployFailedFacts: []pubsub.PRLifecycleEvent{promotionFailed}, EffectsOwner: owner,
	}
}

// mainPromotion is an untracked direct-commit promotion of eri at s2.
func mainPromotion(owner EffectsOwner) RepoEvent {
	return RepoEvent{Repo: "aerial", HeadSHA: "s2", Images: []string{"eri"}, KustomizationRequests: promotionRequests, EffectsOwner: owner}
}

// promotionFailureForward is the HandleDeployAttemptFailed forward a failed
// PR #7 promotion sends.
func promotionFailureForward(kerr *KubeError) sentPR {
	fact := promotionFailed
	fact.Details = kerr.Error()
	return sentPR{handler: PRHandlerDeployAttemptFailed, ev: PREvent{
		Event: "deploy_attempt_failed", Repo: "azylman/aerial", PRNumber: 7, HeadSHA: "s1", MergeSHA: "s1", Branch: "feat",
		Banner: kerr.Error(), Facts: []pubsub.PRLifecycleEvent{fact}, EffectsOwner: EffectsOwnerRestate,
	}}
}

func TestRepoHandlersHandleImagePromotion(t *testing.T) {
	t.Parallel()
	requests := promotionRequests
	prKey := EffectKey{Repo: "azylman/aerial", PRNumber: 7, HeadSHA: "s1", Kind: EffectImagePromote}
	prKeyStr := "azylman/aerial#7@s1:gen0:image_promote"
	mainKey := EffectKey{Repo: "azylman/aerial", HeadSHA: "s2", Kind: EffectImagePromote}
	mainKeyStr := "azylman/aerial#0@s2:gen0:image_promote"
	mainReconcileStr := "azylman/aerial#0@s2:gen0:flux_reconcile"
	rolloutForward := sentPR{handler: PRHandlerKustomizationRequested, ev: PREvent{
		Event: "kustomization_requested", Repo: "azylman/aerial", PRNumber: 7, HeadSHA: "s1", Branch: "feat", TargetID: "t9", Jobs: []string{"eri"},
		KustomizationRequests: requests, Facts: []pubsub.PRLifecycleEvent{promotionStarted}, EffectsOwner: EffectsOwnerRestate,
	}}
	failureForward := promotionFailureForward
	tests := []struct {
		name        string
		ev          RepoEvent
		seed        *RepoState
		kube        *fakeKube
		noKube      bool
		wantErr     error
		wantOutcome RepoEffectOutcome
		wantCalls   []string
		wantSent    []sentPR
		wantEffects []string
	}{
		{
			name: "restate pr promotion writes every tag then forwards the rollout", ev: prPromotion(EffectsOwnerRestate), kube: &fakeKube{},
			wantOutcome: RepoEffectOutcome{Key: prKey, Executed: true, Forwards: 1, Promoted: []string{"eri", "haro"}},
			wantCalls:   []string{"promote eri@s1", "promote haro@s1"},
			wantSent:    []sentPR{rolloutForward},
			wantEffects: []string{prKeyStr},
		},
		{
			name: "restate direct commit promotes and reconciles with no pr", ev: mainPromotion(EffectsOwnerRestate), kube: &fakeKube{},
			wantOutcome: RepoEffectOutcome{Key: mainKey, Executed: true, Promoted: []string{"eri"}},
			wantCalls:   []string{"promote eri@s2", "reconcile flux-system/apps@t1", "reconcile flux-system/core@t1"},
			wantEffects: []string{mainKeyStr, mainReconcileStr},
		},
		{
			name: "retryable reconcile failure retries only the reconcile", ev: mainPromotion(EffectsOwnerRestate), kube: &fakeKube{reconcileErr: map[string]*KubeError{"apps": errKubeDown}},
			wantOutcome: RepoEffectOutcome{Key: mainKey, Executed: true, Retry: true, Kube: errKubeDown, Promoted: []string{"eri"}},
			wantCalls:   []string{"promote eri@s2", "reconcile flux-system/apps@t1", "reconcile flux-system/core@t1"},
			wantEffects: []string{mainKeyStr},
		},
		{
			name: "promotion retry after a promoted tag only reconciles", ev: mainPromotion(EffectsOwnerRestate), kube: &fakeKube{},
			seed:        &RepoState{Repo: "azylman/aerial", RepoEffects: []string{mainKeyStr}},
			wantOutcome: RepoEffectOutcome{Key: mainKey},
			wantCalls:   []string{"reconcile flux-system/apps@t1", "reconcile flux-system/core@t1"},
			wantEffects: []string{mainKeyStr, mainReconcileStr},
		},
		{
			name: "terminal reconcile failure commits", ev: mainPromotion(EffectsOwnerRestate), kube: &fakeKube{reconcileErr: map[string]*KubeError{"core": errKubeForbidden}},
			wantOutcome: RepoEffectOutcome{Key: mainKey, Executed: true, Kube: errKubeForbidden, Promoted: []string{"eri"}},
			wantCalls:   []string{"promote eri@s2", "reconcile flux-system/apps@t1", "reconcile flux-system/core@t1"},
			wantEffects: []string{mainKeyStr, mainReconcileStr},
		},
		{
			name: "mixed reconcile failures retry", ev: mainPromotion(EffectsOwnerRestate), kube: &fakeKube{reconcileErr: map[string]*KubeError{"apps": errKubeForbidden, "core": errKubeDown}},
			wantOutcome: RepoEffectOutcome{Key: mainKey, Executed: true, Retry: true, Kube: &KubeError{Message: "forbidden"}, Promoted: []string{"eri"}},
			wantCalls:   []string{"promote eri@s2", "reconcile flux-system/apps@t1", "reconcile flux-system/core@t1"},
			wantEffects: []string{mainKeyStr},
		},
		{
			name: "fully promoted and reconciled dedupes", ev: mainPromotion(EffectsOwnerRestate), kube: &fakeKube{},
			seed:        &RepoState{Repo: "azylman/aerial", RepoEffects: []string{mainKeyStr, mainReconcileStr}},
			wantOutcome: RepoEffectOutcome{Key: mainKey, Deduped: true},
			wantEffects: []string{mainKeyStr, mainReconcileStr},
		},
		{
			name: "retryable promotion failure reports the attempt and retries", ev: prPromotion(EffectsOwnerRestate), kube: &fakeKube{promoteErrs: map[string]*KubeError{"eri": errKubeDown}},
			wantOutcome: RepoEffectOutcome{Key: prKey, Retry: true, Kube: errKubeDown, Forwards: 1, Promoted: []string{"haro"}},
			wantCalls:   []string{"promote eri@s1", "promote haro@s1"},
			wantSent:    []sentPR{failureForward(errKubeDown)},
			wantEffects: []string{},
		},
		{
			name: "terminal promotion failure commits and reports", ev: prPromotion(EffectsOwnerRestate), kube: &fakeKube{promoteErrs: map[string]*KubeError{"haro": errKubeForbidden}},
			wantOutcome: RepoEffectOutcome{Key: prKey, Kube: errKubeForbidden, Forwards: 1, Promoted: []string{"eri"}},
			wantCalls:   []string{"promote eri@s1", "promote haro@s1"},
			wantSent:    []sentPR{failureForward(errKubeForbidden)},
			wantEffects: []string{prKeyStr},
		},
		{
			name: "terminal untracked promotion failure commits the reconcile too", ev: mainPromotion(EffectsOwnerRestate), kube: &fakeKube{promoteErrs: map[string]*KubeError{"eri": errKubeForbidden}},
			wantOutcome: RepoEffectOutcome{Key: mainKey, Kube: errKubeForbidden},
			wantCalls:   []string{"promote eri@s2"},
			wantEffects: []string{mainKeyStr, mainReconcileStr},
		},
		{
			name: "missing kube client retries and reports the attempt", ev: prPromotion(EffectsOwnerRestate), noKube: true,
			wantOutcome: RepoEffectOutcome{Key: prKey, Retry: true, Kube: errKubeUnconfigured, Forwards: 1},
			wantSent:    []sentPR{failureForward(errKubeUnconfigured)},
			wantEffects: []string{},
		},
		{name: "missing images are invalid", ev: RepoEvent{Repo: "aerial", HeadSHA: "s1"}, kube: &fakeKube{}, wantErr: ErrInvalidRepoEvent, wantEffects: []string{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemRepoStore()
			store.SaveRepo(&RepoState{Repo: "azylman/aerial", RepoEffects: []string{}})
			if tc.seed != nil {
				store.SaveRepo(tc.seed)
			}
			prs := &fakePRSender{}
			h := NewRepoHandlers(store, nil, prs, nil)
			if !tc.noKube {
				h = h.WithKube(tc.kube)
			}
			out, err := h.HandleImagePromotion(context.Background(), tc.ev)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantOutcome, out)
			if tc.kube != nil {
				require.Equal(t, tc.wantCalls, tc.kube.Calls())
			}
			require.Equal(t, tc.wantSent, prs.calls)
			effects := storedRepoEffects(t, store)
			require.Equal(t, tc.wantEffects, effects)
		})
	}
}

func TestRepoHandlersHandleImagePromotionLoadFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		wantErr error
	}{
		{name: "repo state load failure surfaces", wantErr: errFakeLoad},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemRepoStore()
			store.FailLoads(tc.wantErr)
			_, err := NewRepoHandlers(store, nil, nil, nil).HandleImagePromotion(context.Background(), RepoEvent{Repo: "aerial", HeadSHA: "s1", Images: []string{"eri"}})
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}
