package prstate

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
)

// stubRunKeepTerminal executes every restate.Run closure once like stubRun,
// but a terminal closure error keeps its code, matching Restate, which
// never retries a terminal error. Retryable closure errors stand in for
// exhausted retries (code 500).
func stubRunKeepTerminal(mc *mocks.MockContext) *runCapture {
	rec := &runCapture{}
	run := func(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
		rec.ran = true
		res, err := fn(projRunContext{Context: context.Background()})
		if restate.IsTerminalError(err) {
			return restate.AsTerminalError(err)
		}
		if err != nil {
			return restate.TerminalErrorf("retries exhausted: %v", err)
		}
		if out := reflect.ValueOf(output); res != nil && out.Kind() == reflect.Pointer && !out.IsNil() {
			out.Elem().Set(reflect.ValueOf(res))
		}
		return nil
	}
	// The mock flattens Run options into arguments: register every arity
	// the handlers use (one to three options).
	mc.On("Run", mock.Anything, mock.Anything, mock.Anything).Return(run).Maybe()
	mc.On("Run", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(run).Maybe()
	mc.On("Run", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(run).Maybe()
	return rec
}

func TestJournalGitHub(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		gh         *fakeGitHub
		want       UpdateBranchOutcome
		wantStatus int
		wantRetry  bool
		wantNilErr bool
	}{
		{name: "success replays the value", gh: &fakeGitHub{updates: map[int]updateResult{7: {outcome: UpdateBranchUpdated}}}, want: UpdateBranchUpdated, wantNilErr: true},
		{name: "rate limit returns as data", gh: &fakeGitHub{updates: map[int]updateResult{7: {err: errGHRateLimited}}}, wantStatus: 403, wantRetry: true},
		{name: "server error exhausts step retries", gh: &fakeGitHub{updates: map[int]updateResult{7: {err: errGHServer}}}, wantRetry: true},
		{name: "terminal 4xx keeps its status", gh: &fakeGitHub{updates: map[int]updateResult{7: {err: errGHInvalid}}}, wantStatus: 422},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc := mocks.NewMockContext(t)
			stubRunKeepTerminal(mc)
			j := journaled(restate.WithMockContext(mc), tc.gh)
			got, gerr := j.UpdateBranch(context.Background(), "aerial", 7)
			require.Equal(t, tc.want, got)
			if tc.wantNilErr {
				require.Nil(t, gerr)
				return
			}
			require.Equal(t, tc.wantStatus, gerr.Status)
			require.Equal(t, tc.wantRetry, gerr.Retryable())
		})
	}
}

func TestJournaledNilClient(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
	}{
		{name: "no client wires no journaled boundary"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Nil(t, journaled(restate.WithMockContext(mocks.NewMockContext(t)), nil))
		})
	}
}

func TestGitHubStepError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		err        error
		wantStatus int
	}{
		{name: "terminal 404 stays terminal", err: restate.ToTerminalError(errors.New("nf"), restate.WithErrorCode(404)), wantStatus: 404},
		{name: "terminal 500 is retryable", err: restate.TerminalErrorf("exhausted")},
		{name: "plain error is retryable", err: errors.New("boom")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.wantStatus, githubStepError(tc.err).Status)
		})
	}
}

func TestRepoObjectServiceGitHubEffects(t *testing.T) {
	t.Parallel()
	policy := &AutoUpdatePolicy{Authors: []string{"bot"}, DefaultOwner: true}
	retrySend := func(handler string, ev RepoEvent) []sentCall {
		return []sentCall{{service: "RepoObject", key: "azylman/aerial", method: handler, input: ev, delayed: true}}
	}
	conflictGH := func() *fakeGitHub {
		return &fakeGitHub{prs: []GitHubPR{openGitHubPR(4, "bot", "h4")}, updates: map[int]updateResult{4: {outcome: UpdateBranchConflict}}}
	}
	tests := []struct {
		name      string
		handler   string
		ev        RepoEvent
		seed      *RepoState
		failGet   bool
		gh        *fakeGitHub
		wantErr   bool
		wantSet   bool
		wantRun   bool
		wantSends []sentCall
	}{
		{
			name: "repo state load failure surfaces for the invocation retry", handler: RepoHandlerAutoUpdate, gh: &fakeGitHub{}, failGet: true,
			ev: RepoEvent{Repo: "aerial", MainSHA: "m1", EffectsOwner: EffectsOwnerRestate, AutoUpdate: policy}, wantErr: true,
		},
		{
			name: "unstamped cascade executes", handler: RepoHandlerAutoUpdate, gh: &fakeGitHub{},
			ev: RepoEvent{Repo: "aerial", MainSHA: "m1", AutoUpdate: policy}, wantSet: true, wantRun: true,
		},
		{
			name: "legacy cascade is rejected", handler: RepoHandlerAutoUpdate, gh: &fakeGitHub{},
			ev: RepoEvent{Repo: "aerial", MainSHA: "m1", EffectsOwner: EffectsOwnerLegacy, AutoUpdate: policy}, wantErr: true,
		},
		{
			name: "legacy rerun is rejected", handler: RepoHandlerWorkflowRerun, gh: &fakeGitHub{},
			ev: RepoEvent{Repo: "aerial", HeadSHA: "s1", RunID: 55, EffectsOwner: EffectsOwnerLegacy}, wantErr: true,
		},
		{
			name: "restate cascade executes and forwards conflicts", handler: RepoHandlerAutoUpdate, gh: conflictGH(),
			ev: RepoEvent{Repo: "aerial", MainSHA: "m1", EffectsOwner: EffectsOwnerRestate, AutoUpdate: policy}, wantSet: true, wantRun: true,
			wantSends: []sentCall{{
				service: "PRObject", key: "azylman/aerial#4", method: PRHandlerBranchConflict,
				input: PREvent{Event: "branch_conflict", Repo: "azylman/aerial", PRNumber: 4, HeadSHA: "h4", Branch: "b4", Title: "PR 4", EffectsOwner: EffectsOwnerRestate},
			}},
		},
		{
			name: "retryable failure schedules a delayed retry", handler: RepoHandlerAutoUpdate, gh: &fakeGitHub{listErr: errGHServer},
			ev: RepoEvent{Repo: "aerial", MainSHA: "m1", EffectsOwner: EffectsOwnerRestate, AutoUpdate: policy, Attempt: 2}, wantRun: true,
			wantSends: retrySend(RepoHandlerAutoUpdate, RepoEvent{Repo: "aerial", MainSHA: "m1", EffectsOwner: EffectsOwnerRestate, AutoUpdate: policy, Attempt: 3}),
		},
		{
			name: "spent retry budget drops", handler: RepoHandlerAutoUpdate, gh: &fakeGitHub{listErr: errGHServer},
			ev: RepoEvent{Repo: "aerial", MainSHA: "m1", EffectsOwner: EffectsOwnerRestate, AutoUpdate: policy, Attempt: maxRepoEffectRetries}, wantRun: true,
		},
		{
			name: "deduped trigger reports nothing", handler: RepoHandlerAutoUpdate, gh: &fakeGitHub{},
			seed: &RepoState{Repo: "azylman/aerial", RepoEffects: []string{autoUpdateKeyMain}},
			ev:   RepoEvent{Repo: "aerial", MainSHA: "m1", EffectsOwner: EffectsOwnerRestate, AutoUpdate: policy},
		},
		{name: "invalid trigger fails terminally", handler: RepoHandlerAutoUpdate, gh: &fakeGitHub{}, ev: RepoEvent{Repo: "aerial"}, wantErr: true},
		{
			name: "skipped cascade reports unconfigured", handler: RepoHandlerAutoUpdate, gh: &fakeGitHub{},
			ev: RepoEvent{Repo: "aerial", MainSHA: "m1", EffectsOwner: EffectsOwnerRestate}, wantSet: true, wantRun: true,
		},
		{
			name: "terminal rerun failure reports an error", handler: RepoHandlerWorkflowRerun, gh: &fakeGitHub{rerunErr: errGHNotFound},
			ev: RepoEvent{Repo: "aerial", HeadSHA: "s1", RunID: 55, EffectsOwner: EffectsOwnerRestate}, wantSet: true, wantRun: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mc *mocks.MockContext
			rec := &stateCapture[*RepoState]{sends: &sendCapture{}}
			if tc.failGet {
				mc = newStateErrorMock(t)
			} else {
				mc, rec = newStateMock(t, tc.seed)
			}
			run := stubRunKeepTerminal(mc)
			svc := NewRepoObjectService().WithGitHub(tc.gh)
			var err error
			if tc.handler == RepoHandlerWorkflowRerun {
				err = svc.HandleWorkflowRerun(restate.WithMockContext(mc), tc.ev)
			} else {
				err = svc.HandleAutoUpdate(restate.WithMockContext(mc), tc.ev)
			}
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.wantSet, rec.setCalled)
			require.Equal(t, tc.wantRun, run.ran)
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
		})
	}
}

// registeredPR seeds an open, registered PR on head h1 (it has a target).
func registeredPR() *PRState {
	st := effectPR()
	st.Branch = "feat"
	return st
}

// collaboratorPR seeds an open, unregistered PR on head h1.
func collaboratorPR() *PRState {
	return svcRow("h1", "", RegistryStatusOpen, TerminalNone)
}

func TestPRObjectServiceHandleConflictCheck(t *testing.T) {
	t.Parallel()
	conflicted := false
	dirty := fetchResult{pr: GitHubPR{Number: 101, State: "open", Mergeable: &conflicted, Head: GitHubPRRef{SHA: "h1"}}}
	clean := fetchResult{pr: GitHubPR{Number: 101, State: "open", MergeableState: "clean", Head: GitHubPRRef{SHA: "h1"}}}
	ev := func(owner EffectsOwner, head string) PREvent {
		return PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: head, Branch: "feat", EffectsOwner: owner, EriEffects: []EriEffect{dmEffect()}}
	}
	recheck := sentCall{service: "PRObject", key: "azylman/aerial#101", method: PRHandlerConflictCheck, input: ev(EffectsOwnerRestate, "h1"), delayed: true}
	tests := []struct {
		name          string
		state         *PRState
		failGet       bool
		ev            PREvent
		gh            *fakeGitHub
		wantErr       bool
		wantSet       bool
		wantGHCalls   []string
		wantFacts     []string
		wantDMs       int
		wantConflict  string
		wantSendCount int
		wantFirstSend *sentCall
	}{
		{name: "legacy owner is rejected", state: registeredPR(), ev: ev(EffectsOwnerLegacy, "h1"), gh: &fakeGitHub{}, wantErr: true},
		{
			name: "restate conflict on a registered pr publishes the conflict fact", state: registeredPR(), ev: ev(EffectsOwnerRestate, "h1"), gh: &fakeGitHub{mergeability: dirty},
			wantSet: true, wantGHCalls: []string{"mergeability 101"}, wantFacts: []string{"conflict"}, wantConflict: "h1", wantSendCount: 1, wantFirstSend: &recheck,
		},
		{
			name: "restate conflict on a collaborator pr fires the carried dm", state: collaboratorPR(), ev: ev(EffectsOwnerRestate, "h1"), gh: &fakeGitHub{mergeability: dirty},
			wantSet: true, wantGHCalls: []string{"mergeability 101"}, wantDMs: 1, wantConflict: "h1", wantSendCount: 1, wantFirstSend: &recheck,
		},
		{name: "restate clean head records nothing more", state: registeredPR(), ev: ev(EffectsOwnerRestate, "h1"), gh: &fakeGitHub{mergeability: clean}, wantSet: true, wantGHCalls: []string{"mergeability 101"}},
		{
			name: "restate retryable read schedules a recheck", state: registeredPR(), ev: ev(EffectsOwnerRestate, "h1"), gh: &fakeGitHub{mergeability: fetchResult{err: errGHServer}},
			wantGHCalls: []string{"mergeability 101"}, wantSendCount: 1, wantFirstSend: &recheck,
		},
		{name: "restate kv failure surfaces", failGet: true, ev: ev(EffectsOwnerRestate, "h1"), gh: &fakeGitHub{}, wantErr: true},
		{name: "missing head is ignored", state: registeredPR(), ev: ev(EffectsOwnerRestate, ""), gh: &fakeGitHub{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mc *mocks.MockContext
			rec := &stateCapture[*PRState]{}
			if tc.failGet {
				mc = newStateErrorMock(t)
			} else {
				mc, rec = newStateMock(t, tc.state)
			}
			stubRunKeepTerminal(mc)
			facts := &fakeFactPublisher{}
			eri := &fakeEriEffector{}
			svc := NewPRObjectService().WithFactPublisher(facts).WithEriEffector(eri)
			if tc.gh != nil {
				svc = svc.WithGitHub(tc.gh)
			}
			err := svc.HandleConflictCheck(restate.WithMockContext(mc), tc.ev)
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.wantSet, rec.setCalled)
			if tc.failGet {
				return
			}
			require.Equal(t, tc.wantGHCalls, tc.gh.Calls())
			var gotFacts []string
			for _, c := range facts.Calls() {
				gotFacts = append(gotFacts, c.fact.Event)
			}
			require.Equal(t, tc.wantFacts, gotFacts)
			require.Len(t, eri.Calls(), tc.wantDMs)
			require.Equal(t, tc.wantConflict, tc.state.CollaboratorConflict)
			sends := rec.sends.snapshot()
			require.Len(t, sends, tc.wantSendCount)
			if tc.wantFirstSend != nil {
				require.Equal(t, *tc.wantFirstSend, sends[0])
			}
		})
	}
}

func TestPRObjectServiceHandleBranchConflict(t *testing.T) {
	t.Parallel()
	published := func() *PRState {
		st := registeredPR()
		st.MarkConflictPublished("h1")
		return st
	}
	ev := func(head string) PREvent {
		return PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: head, Branch: "feat", EffectsOwner: EffectsOwnerRestate, EriEffects: []EriEffect{dmEffect()}}
	}
	at := time.Time{}
	tests := []struct {
		name       string
		state      *PRState
		failGet    bool
		ev         PREvent
		wantErr    bool
		wantSet    bool
		wantFacts  []pubsub.PRLifecycleEvent
		wantDMs    int
		wantStatus string
	}{
		{
			name: "registered pr publishes once", state: registeredPR(), ev: ev("h1"), wantSet: true, wantStatus: RegistryStatusConflict,
			wantFacts: []pubsub.PRLifecycleEvent{{Event: "conflict", Repo: "azylman/aerial", PRNumber: 101, Branch: "feat", HeadSHA: "h1", TargetID: testTarget, Status: "conflict", OccurredAt: at}},
		},
		{name: "published head dedupes", state: published(), ev: ev("h1"), wantSet: true, wantStatus: RegistryStatusOpen},
		{name: "collaborator pr fires the dm", state: collaboratorPR(), ev: ev(""), wantSet: true, wantDMs: 1, wantStatus: RegistryStatusOpen},
		{name: "stale head drops", state: registeredPR(), ev: ev("h0"), wantStatus: RegistryStatusOpen},
		{name: "kv failure surfaces", failGet: true, ev: ev("h1"), wantErr: true},
		{name: "invalid event ignored", state: registeredPR(), ev: PREvent{Repo: "aerial"}, wantStatus: RegistryStatusOpen},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mc *mocks.MockContext
			rec := &stateCapture[*PRState]{}
			if tc.failGet {
				mc = newStateErrorMock(t)
			} else {
				mc, rec = newStateMock(t, tc.state)
			}
			stubRunKeepTerminal(mc)
			facts := &fakeFactPublisher{}
			eri := &fakeEriEffector{}
			err := NewPRObjectService().WithFactPublisher(facts).WithEriEffector(eri).HandleBranchConflict(restate.WithMockContext(mc), tc.ev)
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.wantSet, rec.setCalled)
			if tc.failGet {
				return
			}
			var gotFacts []pubsub.PRLifecycleEvent
			for _, c := range facts.Calls() {
				c.fact.OccurredAt = at
				gotFacts = append(gotFacts, c.fact)
			}
			require.Equal(t, tc.wantFacts, gotFacts)
			require.Len(t, eri.Calls(), tc.wantDMs)
			require.Equal(t, tc.wantStatus, tc.state.Status)
		})
	}
}

func TestPRObjectServiceHandleHeadRefMissing(t *testing.T) {
	t.Parallel()
	ev := func(head string) PREvent {
		return PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: head, Branch: "dependabot/x", EffectsOwner: EffectsOwnerRestate}
	}
	tests := []struct {
		name         string
		state        *PRState
		failGet      bool
		ev           PREvent
		wantErr      bool
		wantSet      bool
		wantFacts    []pubsub.PRLifecycleEvent
		wantTerminal TerminalState
		wantIndex    bool
	}{
		{
			name: "stale head ref closes the pr", state: registeredPR(), ev: ev("h1"), wantSet: true, wantTerminal: TerminalClosed, wantIndex: true,
			wantFacts: []pubsub.PRLifecycleEvent{{Event: "closed_unmerged", Repo: "azylman/aerial", PRNumber: 101, Branch: "dependabot/x", HeadSHA: "h1", TargetID: testTarget, Status: "closed_unmerged"}},
		},
		{name: "moved head drops", state: registeredPR(), ev: ev("h0")},
		{name: "kv failure surfaces", failGet: true, ev: ev("h1"), wantErr: true},
		{name: "invalid event ignored", state: registeredPR(), ev: PREvent{Repo: "aerial"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mc *mocks.MockContext
			rec := &stateCapture[*PRState]{}
			if tc.failGet {
				mc = newStateErrorMock(t)
			} else {
				mc, rec = newStateMock(t, tc.state)
			}
			stubRunKeepTerminal(mc)
			facts := &fakeFactPublisher{}
			err := NewPRObjectService().WithFactPublisher(facts).HandleHeadRefMissing(restate.WithMockContext(mc), tc.ev)
			require.Equal(t, tc.wantErr, err != nil)
			require.Equal(t, tc.wantSet, rec.setCalled)
			if tc.failGet {
				return
			}
			var gotFacts []pubsub.PRLifecycleEvent
			for _, c := range facts.Calls() {
				c.fact.OccurredAt = time.Time{}
				gotFacts = append(gotFacts, c.fact)
			}
			require.Equal(t, tc.wantFacts, gotFacts)
			require.Equal(t, tc.wantTerminal, tc.state.Terminal)
			indexed := false
			for _, s := range rec.sends.snapshot() {
				indexed = indexed || s.method == RepoHandlerPRIndex
			}
			require.Equal(t, tc.wantIndex, indexed)
		})
	}
}

func TestRestateConflictCheckerCheckConflict(t *testing.T) {
	t.Parallel()
	no := false
	tests := []struct {
		name     string
		gh       GitHubEffector
		want     bool
		wantErr  bool
		wantCall bool
	}{
		{name: "dirty open head conflicts", gh: &fakeGitHub{mergeability: fetchResult{pr: GitHubPR{State: "open", Mergeable: &no, Head: GitHubPRRef{SHA: "H1"}}}}, want: true},
		{name: "merged is not a conflict", gh: &fakeGitHub{mergeability: fetchResult{pr: GitHubPR{Merged: true, Mergeable: &no}}}},
		{name: "closed is not a conflict", gh: &fakeGitHub{mergeability: fetchResult{pr: GitHubPR{State: "closed", Mergeable: &no}}}},
		{name: "moved head is not a conflict", gh: &fakeGitHub{mergeability: fetchResult{pr: GitHubPR{State: "open", Mergeable: &no, Head: GitHubPRRef{SHA: "h2"}}}}},
		{name: "terminal read is not a conflict", gh: &fakeGitHub{mergeability: fetchResult{err: errGHNotFound}}},
		{name: "retryable read fails", gh: &fakeGitHub{mergeability: fetchResult{err: errGHServer}}, wantErr: true},
		{name: "missing client fails", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := restateConflictChecker{gh: tc.gh}.CheckConflict(context.Background(), "aerial", 101, "h1")
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.wantErr, err != nil)
		})
	}
}
