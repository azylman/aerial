package prstate

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// updateResult is one canned update-branch answer.
type updateResult struct {
	outcome UpdateBranchOutcome
	err     *GitHubError
}

// fetchResult is one canned PR read.
type fetchResult struct {
	pr  GitHubPR
	err *GitHubError
}

// fakeGitHub is the hermetic GitHubEffector: canned answers per PR or SHA
// and an ordered call log.
type fakeGitHub struct {
	mu           sync.Mutex
	prs          []GitHubPR
	listErr      *GitHubError
	blocked      map[string]bool
	updates      map[int]updateResult
	fetches      map[int]fetchResult
	mergeability fetchResult
	rerunErr     *GitHubError
	calls        []string
}

func (f *fakeGitHub) record(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
}

func (f *fakeGitHub) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeGitHub) ListOpenPRs(_ context.Context, repo string) ([]GitHubPR, *GitHubError) {
	f.record("list " + repo)
	return f.prs, f.listErr
}

func (f *fakeGitHub) HeadChecksBlocked(_ context.Context, _ string, sha string) bool {
	f.record("checks " + sha)
	return f.blocked[sha]
}

func (f *fakeGitHub) UpdateBranch(_ context.Context, _ string, prNumber int) (UpdateBranchOutcome, *GitHubError) {
	f.record(fmt.Sprintf("update %d", prNumber))
	r := f.updates[prNumber]
	return r.outcome, r.err
}

func (f *fakeGitHub) FetchPR(_ context.Context, _ string, prNumber int) (GitHubPR, *GitHubError) {
	f.record(fmt.Sprintf("fetch %d", prNumber))
	r := f.fetches[prNumber]
	return r.pr, r.err
}

func (f *fakeGitHub) FetchPRMergeability(_ context.Context, _ string, prNumber int) (GitHubPR, *GitHubError) {
	f.record(fmt.Sprintf("mergeability %d", prNumber))
	return f.mergeability.pr, f.mergeability.err
}

func (f *fakeGitHub) RerunFailedJobs(_ context.Context, _ string, runID int64) *GitHubError {
	f.record(fmt.Sprintf("rerun %d", runID))
	return f.rerunErr
}

// openGitHubPR is one open PR by author on head sha.
func openGitHubPR(n int, author, sha string) GitHubPR {
	return GitHubPR{Number: n, State: "open", Title: fmt.Sprintf("PR %d", n), User: GitHubUser{Login: author}, Head: GitHubPRRef{Ref: fmt.Sprintf("b%d", n), SHA: sha}}
}

var (
	errGHRateLimited = &GitHubError{Status: 403, RateLimited: true, Message: "rate limited"}
	errGHServer      = &GitHubError{Status: 502, Message: "bad gateway"}
	errGHNotFound    = &GitHubError{Status: 404, Message: "not found"}
	errGHInvalid     = &GitHubError{Status: 422, Message: "invalid"}
)

const autoUpdateKeyMain = "azylman/aerial#0@m1:gen0:github_auto_update"

func TestRepoHandlersHandleAutoUpdate(t *testing.T) {
	t.Parallel()
	defaultPolicy := &AutoUpdatePolicy{Authors: []string{"bot"}, DefaultOwner: true, Collaborators: []CollaboratorAlert{{Author: "bot", ChannelID: testChannel, Mention: "<@1>"}}}
	indexedOnly := &AutoUpdatePolicy{Authors: []string{"bot"}}
	mainPush := func(policy *AutoUpdatePolicy) RepoEvent {
		return RepoEvent{Repo: "aerial", MainSHA: "m1", AutoUpdate: policy}
	}
	draft := openGitHubPR(3, "bot", "h3")
	draft.Draft = true
	conflictDM := EriEffect{
		Kind: EffectEriDirectMessage, ChannelID: testChannel,
		Content: "<@1> Merge conflict with `main` detected on [azylman/aerial#4](https://github.com/azylman/aerial/pull/4) (\"PR 4\", branch: b4, commit: h4) — automatic branch update could not resolve conflicts. Please rebase or merge `origin/main` and push.",
	}
	tests := []struct {
		name        string
		ev          RepoEvent
		seed        *RepoState
		gh          *fakeGitHub
		noGitHub    bool
		wantErr     error
		wantOutcome RepoEffectOutcome
		wantCalls   []string
		wantSent    []sentPR
		wantEffects []string
	}{
		{
			name: "fired trigger dedupes", ev: mainPush(defaultPolicy), gh: &fakeGitHub{},
			seed:        &RepoState{Repo: "azylman/aerial", RepoEffects: []string{autoUpdateKeyMain}},
			wantOutcome: RepoEffectOutcome{Key: EffectKey{Repo: "azylman/aerial", HeadSHA: "m1", Kind: EffectGitHubAutoUpdate}, Deduped: true},
			wantEffects: []string{autoUpdateKeyMain},
		},
		{
			name: "no allowlisted authors skips", ev: mainPush(&AutoUpdatePolicy{DefaultOwner: true}), gh: &fakeGitHub{},
			wantOutcome: RepoEffectOutcome{Key: EffectKey{Repo: "azylman/aerial", HeadSHA: "m1", Kind: EffectGitHubAutoUpdate}, Skipped: true},
			wantEffects: []string{autoUpdateKeyMain},
		},
		{
			name: "missing github client retries", ev: mainPush(defaultPolicy), noGitHub: true,
			wantOutcome: RepoEffectOutcome{Key: EffectKey{Repo: "azylman/aerial", HeadSHA: "m1", Kind: EffectGitHubAutoUpdate}, Retry: true, Err: errGitHubUnconfigured},
		},
		{
			name: "retryable list failure retries", ev: mainPush(defaultPolicy), gh: &fakeGitHub{listErr: errGHServer},
			wantOutcome: RepoEffectOutcome{Key: EffectKey{Repo: "azylman/aerial", HeadSHA: "m1", Kind: EffectGitHubAutoUpdate}, Retry: true, Err: errGHServer},
			wantCalls:   []string{"list azylman/aerial"},
		},
		{
			name: "terminal list failure commits", ev: mainPush(defaultPolicy), gh: &fakeGitHub{listErr: errGHNotFound},
			wantOutcome: RepoEffectOutcome{Key: EffectKey{Repo: "azylman/aerial", HeadSHA: "m1", Kind: EffectGitHubAutoUpdate}, Executed: true, Err: errGHNotFound},
			wantCalls:   []string{"list azylman/aerial"},
			wantEffects: []string{autoUpdateKeyMain},
		},
		{
			name: "cascade skips ineligible and blocked prs, forwards conflicts and stale closes, stops at the first update",
			ev:   RepoEvent{Repo: "aerial", PRNumber: 9, HeadSHA: "h9", CheckName: "CI Gate", AutoUpdate: defaultPolicy},
			gh: &fakeGitHub{
				prs:     []GitHubPR{openGitHubPR(1, "human", "h1"), openGitHubPR(2, "bot", "h2"), draft, openGitHubPR(4, "bot", "h4"), openGitHubPR(5, "bot", "h5"), openGitHubPR(6, "bot", "h6"), openGitHubPR(7, "bot", "h7"), openGitHubPR(8, "bot", "h8"), openGitHubPR(10, "bot", "h10")},
				blocked: map[string]bool{"h2": true},
				updates: map[int]updateResult{
					4: {outcome: UpdateBranchConflict},
					5: {outcome: UpdateBranchHeadMissing},
					6: {err: errGHInvalid},
					7: {outcome: UpdateBranchHeadMissing},
					8: {outcome: UpdateBranchUpToDate},
				},
				fetches: map[int]fetchResult{5: {pr: GitHubPR{Number: 5, State: "closed", Head: GitHubPRRef{Ref: "b5", SHA: "h5"}}}, 7: {pr: GitHubPR{Number: 7, State: "open"}}},
			},
			wantOutcome: RepoEffectOutcome{
				Key: EffectKey{Repo: "azylman/aerial", PRNumber: 9, HeadSHA: "h9", Kind: EffectGitHubAutoUpdate, CheckName: "CI Gate"}, Executed: true, Err: errGHInvalid, Forwards: 2,
				Updates: []BranchUpdate{{PRNumber: 4, HeadSHA: "h4", Outcome: UpdateBranchConflict}, {PRNumber: 5, HeadSHA: "h5", Outcome: UpdateBranchHeadMissing}, {PRNumber: 7, HeadSHA: "h7", Outcome: UpdateBranchHeadMissing}, {PRNumber: 8, HeadSHA: "h8", Outcome: UpdateBranchUpToDate}},
			},
			wantCalls: []string{"list azylman/aerial", "checks h2", "checks h4", "update 4", "checks h5", "update 5", "fetch 5", "checks h6", "update 6", "checks h7", "update 7", "fetch 7", "checks h8", "update 8"},
			wantSent: []sentPR{
				{handler: PRHandlerBranchConflict, ev: PREvent{Event: "branch_conflict", Repo: "azylman/aerial", PRNumber: 4, HeadSHA: "h4", Branch: "b4", Title: "PR 4", EriEffects: []EriEffect{conflictDM}}},
				{handler: PRHandlerHeadRefMissing, ev: PREvent{Event: "head_ref_missing", Repo: "azylman/aerial", PRNumber: 5, HeadSHA: "h5", Branch: "b5"}},
			},
			wantEffects: []string{"azylman/aerial#9@h9:gen0:github_auto_update:check=ci gate"},
		},
		{
			name: "non-default owner only updates indexed prs", ev: mainPush(indexedOnly),
			seed: &RepoState{Repo: "azylman/aerial", PRs: map[int]*RepoPRStatus{2: {}}},
			gh:   &fakeGitHub{prs: []GitHubPR{openGitHubPR(1, "bot", "h1"), openGitHubPR(2, "bot", "h2")}, updates: map[int]updateResult{2: {outcome: UpdateBranchUpdated}}},
			wantOutcome: RepoEffectOutcome{
				Key: EffectKey{Repo: "azylman/aerial", HeadSHA: "m1", Kind: EffectGitHubAutoUpdate}, Executed: true,
				Updates: []BranchUpdate{{PRNumber: 2, HeadSHA: "h2", Outcome: UpdateBranchUpdated}},
			},
			wantCalls:   []string{"list azylman/aerial", "checks h2", "update 2"},
			wantEffects: []string{autoUpdateKeyMain},
		},
		{
			name: "rate limit stops the cascade and retries", ev: mainPush(defaultPolicy),
			gh:          &fakeGitHub{prs: []GitHubPR{openGitHubPR(1, "bot", "h1"), openGitHubPR(2, "bot", "h2")}, updates: map[int]updateResult{1: {err: errGHRateLimited}}},
			wantOutcome: RepoEffectOutcome{Key: EffectKey{Repo: "azylman/aerial", HeadSHA: "m1", Kind: EffectGitHubAutoUpdate}, Retry: true, Err: errGHRateLimited},
			wantCalls:   []string{"list azylman/aerial", "checks h1", "update 1"},
		},
		{
			name: "stale read failure closes nothing", ev: mainPush(defaultPolicy),
			gh: &fakeGitHub{prs: []GitHubPR{openGitHubPR(1, "bot", "h1")}, updates: map[int]updateResult{1: {outcome: UpdateBranchHeadMissing}}, fetches: map[int]fetchResult{1: {err: errGHServer}}},
			wantOutcome: RepoEffectOutcome{
				Key: EffectKey{Repo: "azylman/aerial", HeadSHA: "m1", Kind: EffectGitHubAutoUpdate}, Executed: true,
				Updates: []BranchUpdate{{PRNumber: 1, HeadSHA: "h1", Outcome: UpdateBranchHeadMissing}},
			},
			wantCalls:   []string{"list azylman/aerial", "checks h1", "update 1", "fetch 1"},
			wantEffects: []string{autoUpdateKeyMain},
		},
		{
			name: "deleted pr closes as stale", ev: mainPush(defaultPolicy),
			gh: &fakeGitHub{prs: []GitHubPR{openGitHubPR(1, "bot", "h1")}, updates: map[int]updateResult{1: {outcome: UpdateBranchHeadMissing}}, fetches: map[int]fetchResult{1: {err: errGHNotFound}}},
			wantOutcome: RepoEffectOutcome{
				Key: EffectKey{Repo: "azylman/aerial", HeadSHA: "m1", Kind: EffectGitHubAutoUpdate}, Executed: true, Forwards: 1,
				Updates: []BranchUpdate{{PRNumber: 1, HeadSHA: "h1", Outcome: UpdateBranchHeadMissing}},
			},
			wantCalls:   []string{"list azylman/aerial", "checks h1", "update 1", "fetch 1"},
			wantSent:    []sentPR{{handler: PRHandlerHeadRefMissing, ev: PREvent{Event: "head_ref_missing", Repo: "azylman/aerial", PRNumber: 1}}},
			wantEffects: []string{autoUpdateKeyMain},
		},
		{name: "missing trigger sha is invalid", ev: RepoEvent{Repo: "aerial"}, gh: &fakeGitHub{}, wantErr: ErrInvalidRepoEvent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemRepoStore()
			if tc.seed != nil {
				store.SaveRepo(tc.seed)
			}
			prs := &fakePRSender{}
			h := testRepoHandlers(t, store, prs, nil)
			if !tc.noGitHub {
				h = h.WithGitHub(tc.gh)
			}
			out, err := h.HandleAutoUpdate(context.Background(), tc.ev)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantOutcome, out)
			if tc.gh != nil {
				require.Equal(t, tc.wantCalls, tc.gh.Calls())
			}
			require.Equal(t, tc.wantSent, prs.calls)
			effects := storedRepoEffects(t, store)
			require.Equal(t, tc.wantEffects, effects)
		})
	}
}

// storedRepoEffects loads the committed repo-effect keys.
func storedRepoEffects(t *testing.T, store *MemRepoStore) []string {
	t.Helper()
	st, err := store.LoadRepo("aerial")
	require.NoError(t, err)
	if st == nil {
		return nil
	}
	return st.RepoEffects
}

func TestRepoHandlersHandleAutoUpdateForwardFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		failPR      int
		wantErr     error
		wantForward int
		wantEffects []string
	}{
		{name: "failed forward surfaces after committing the key", failPR: 1, wantErr: errFakeSend, wantEffects: []string{autoUpdateKeyMain}},
		{name: "successful forward counts", failPR: 0, wantForward: 1, wantEffects: []string{autoUpdateKeyMain}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemRepoStore()
			gh := &fakeGitHub{prs: []GitHubPR{openGitHubPR(1, "bot", "h1")}, updates: map[int]updateResult{1: {outcome: UpdateBranchConflict}}}
			h := testRepoHandlers(t, store, &fakePRSender{failPR: tc.failPR}, nil).WithGitHub(gh)
			out, err := h.HandleAutoUpdate(context.Background(), RepoEvent{Repo: "aerial", MainSHA: "m1", AutoUpdate: &AutoUpdatePolicy{Authors: []string{"bot"}, DefaultOwner: true}})
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantForward, out.Forwards)
			st, lerr := store.LoadRepo("aerial")
			require.NoError(t, lerr)
			require.Equal(t, tc.wantEffects, st.RepoEffects)
		})
	}
}

func TestRepoHandlersHandleWorkflowRerun(t *testing.T) {
	t.Parallel()
	rerunKey := "azylman/aerial#12@s1:gen0:github_rerun:check=ci"
	key := EffectKey{Repo: "azylman/aerial", PRNumber: 12, HeadSHA: "s1", Kind: EffectGitHubRerun, CheckName: "CI"}
	ev := func() RepoEvent {
		return RepoEvent{Repo: "aerial", PRNumber: 12, HeadSHA: "s1", CheckName: "CI", RunID: 55}
	}
	tests := []struct {
		name        string
		ev          RepoEvent
		gh          *fakeGitHub
		noGitHub    bool
		storeErr    error
		wantErr     error
		wantOutcome RepoEffectOutcome
		wantCalls   []string
		wantEffects []string
	}{
		{name: "restate rerun executes", ev: ev(), gh: &fakeGitHub{}, wantOutcome: RepoEffectOutcome{Key: key, Executed: true}, wantCalls: []string{"rerun 55"}, wantEffects: []string{rerunKey}},
		{name: "retryable failure leaves the key open", ev: ev(), gh: &fakeGitHub{rerunErr: errGHServer}, wantOutcome: RepoEffectOutcome{Key: key, Retry: true, Err: errGHServer}, wantCalls: []string{"rerun 55"}},
		{name: "terminal failure commits the key", ev: ev(), gh: &fakeGitHub{rerunErr: errGHNotFound}, wantOutcome: RepoEffectOutcome{Key: key, Err: errGHNotFound}, wantCalls: []string{"rerun 55"}, wantEffects: []string{rerunKey}},
		{name: "missing client retries", ev: ev(), noGitHub: true, wantOutcome: RepoEffectOutcome{Key: key, Retry: true, Err: errGitHubUnconfigured}},
		{name: "missing run id is invalid", ev: RepoEvent{Repo: "aerial", HeadSHA: "s1"}, gh: &fakeGitHub{}, wantErr: ErrInvalidRepoEvent},
		{name: "store failure surfaces", ev: ev(), gh: &fakeGitHub{}, storeErr: errKVUnavailable, wantErr: errKVUnavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemRepoStore()
			h := testRepoHandlers(t, store, &fakePRSender{}, nil)
			if !tc.noGitHub {
				h = h.WithGitHub(tc.gh)
			}
			if tc.storeErr != nil {
				store.FailLoads(tc.storeErr)
			}
			out, err := h.HandleWorkflowRerun(context.Background(), tc.ev)
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantOutcome, out)
			if tc.gh != nil {
				require.Equal(t, tc.wantCalls, tc.gh.Calls())
			}
			if tc.storeErr != nil {
				return
			}
			st, lerr := store.LoadRepo("aerial")
			require.NoError(t, lerr)
			var effects []string
			if st != nil {
				effects = st.RepoEffects
			}
			require.Equal(t, tc.wantEffects, effects)
		})
	}
}

func TestRepoStateRepoEffectRing(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		marks       int
		wantEffects int
		wantFirst   string
	}{
		{name: "under the bound keeps everything", marks: 2, wantEffects: 2, wantFirst: "azylman/aerial#0@s0:gen0:github_rerun"},
		{name: "over the bound keeps the newest", marks: MaxRepoEffects + 3, wantEffects: MaxRepoEffects, wantFirst: "azylman/aerial#0@s3:gen0:github_rerun"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := NewRepoState("aerial")
			for i := range tc.marks {
				k := EffectKey{Repo: "aerial", HeadSHA: fmt.Sprintf("s%d", i), Kind: EffectGitHubRerun}
				st.MarkRepoEffect(k)
				st.MarkRepoEffect(k)
			}
			require.Len(t, st.RepoEffects, tc.wantEffects)
			require.Equal(t, tc.wantFirst, st.RepoEffects[0])
			var nilState *RepoState
			nilState.MarkRepoEffect(EffectKey{})
			require.False(t, nilState.WasRepoEffect(EffectKey{}))
		})
	}
}
