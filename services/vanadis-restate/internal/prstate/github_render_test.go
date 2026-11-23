package prstate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/pubsub"
)

func TestHasAerialPRMetadata(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
		want bool
	}{
		{name: "no trailer", body: "plain body"},
		{name: "bot id trailer", body: "x <!-- aerial-pr: {\"bot_id\":\"aerial\"} -->", want: true},
		{name: "agent trailer", body: "<!-- aerial-pr: {\"agent\":\"miorine\"} -->", want: true},
		{name: "snowflake target trailer", body: "<!-- aerial-pr: {\"target_id\":\"discord-123456789012345678\"} -->", want: true},
		{name: "headless target trailer", body: "<!-- aerial-pr: {\"target_id\":\"pool:dispatch-abc\"} -->", want: true},
		{name: "invalid target only", body: "<!-- aerial-pr: {\"target_id\":\"general\"} -->"},
		{name: "unterminated trailer", body: "<!-- aerial-pr: {\"bot_id\":\"aerial\"}"},
		{name: "malformed json", body: "<!-- aerial-pr: {bot} -->"},
		{name: "last trailer wins", body: "<!-- aerial-pr: {\"bot_id\":\"aerial\"} --> later <!-- aerial-pr: {} -->"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, HasAerialPRMetadata(tc.body))
		})
	}
}

func TestCollaboratorConflictMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		mention string
		repo    string
		title   string
		branch  string
		head    string
		want    string
	}{
		{
			name: "full target with mention", mention: " <@42> ", repo: "azylman/mirrormere", title: "Fix", branch: "refs/heads/feat", head: "abcdef1234",
			want: "<@42> Merge conflict with `main` detected on [azylman/mirrormere#9](https://github.com/azylman/mirrormere/pull/9) (\"Fix\", branch: feat, commit: abcdef1) — automatic branch update could not resolve conflicts. Please rebase or merge `origin/main` and push.",
		},
		{
			name: "bare target without repo or mention",
			want: "Merge conflict with `main` detected on PR #9 — automatic branch update could not resolve conflicts. Please rebase or merge `origin/main` and push.",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, CollaboratorConflictMessage(tc.mention, tc.repo, 9, tc.title, tc.branch, tc.head))
		})
	}
}

func TestCollaboratorConflictDM(t *testing.T) {
	t.Parallel()
	policy := &AutoUpdatePolicy{Collaborators: []CollaboratorAlert{
		{Author: "Octo", ChannelID: "111111111111111111", Mention: "<@42>"},
		{Author: "nochan", Mention: "<@43>"},
	}}
	pr := func(login, state, body string, merged bool) GitHubPR {
		return GitHubPR{Number: 7, Title: "T", Body: body, State: state, Merged: merged, User: GitHubUser{Login: login}, Head: GitHubPRRef{Ref: "b7", SHA: "h7"}}
	}
	wantDM := EriEffect{
		Kind: prwire.EffectEriDirectMessage, ChannelID: "111111111111111111",
		Content: "<@42> Merge conflict with `main` detected on [azylman/aerial#7](https://github.com/azylman/aerial/pull/7) (\"T\", branch: b7, commit: h7) — automatic branch update could not resolve conflicts. Please rebase or merge `origin/main` and push.",
	}
	tests := []struct {
		name   string
		policy *AutoUpdatePolicy
		pr     GitHubPR
		want   EriEffect
		wantOK bool
	}{
		{name: "matching collaborator", policy: policy, pr: pr("octo", "open", "", false), want: wantDM, wantOK: true},
		{name: "no policy", pr: pr("octo", "open", "", false)},
		{name: "unknown author", policy: policy, pr: pr("other", "open", "", false)},
		{name: "rule without channel", policy: policy, pr: pr("nochan", "open", "", false)},
		{name: "blank author", policy: policy, pr: pr(" ", "open", "", false)},
		{name: "bot-owned pr", policy: policy, pr: pr("octo", "open", "<!-- aerial-pr: {\"bot_id\":\"aerial\"} -->", false)},
		{name: "merged pr", policy: policy, pr: pr("octo", "open", "", true)},
		{name: "closed pr", policy: policy, pr: pr("octo", "closed", "", false)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := collaboratorConflictDM(tc.policy, "azylman/aerial", tc.pr)
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestAutoUpdatePolicyAuthorAllowed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		policy *AutoUpdatePolicy
		author string
		want   bool
	}{
		{name: "case-insensitive match", policy: &AutoUpdatePolicy{Authors: []string{" Aerial-Bot "}}, author: "aerial-bot", want: true},
		{name: "not listed", policy: &AutoUpdatePolicy{Authors: []string{"a"}}, author: "b"},
		{name: "blank author", policy: &AutoUpdatePolicy{Authors: []string{"a"}}, author: " "},
		{name: "nil policy", author: "a"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, authorAllowed(tc.policy, tc.author))
		})
	}
}

func TestPRFact(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	st := effectPR()
	st.Branch = "stored-branch"
	tests := []struct {
		name   string
		event  string
		branch string
		head   string
		want   pubsub.PRLifecycleEvent
	}{
		{
			name: "conflict on a dependabot branch is quiet", event: FactEventConflict, branch: "dependabot/go/x", head: " h2 ",
			want: pubsub.PRLifecycleEvent{Event: "conflict", Repo: "azylman/aerial", PRNumber: 101, Branch: "dependabot/go/x", HeadSHA: "h2", TargetID: testTarget, Status: "conflict", Quiet: true, OccurredAt: at},
		},
		{
			name: "empty branch falls back to the stored branch", event: FactEventClosedUnmerged, head: "h1",
			want: pubsub.PRLifecycleEvent{Event: "closed_unmerged", Repo: "azylman/aerial", PRNumber: 101, Branch: "stored-branch", HeadSHA: "h1", TargetID: testTarget, Status: "closed_unmerged", OccurredAt: at},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, prFact(st, tc.event, tc.branch, tc.head, at))
		})
	}
}

// TestRepoEffectKey pins the generation-free match key for the auto-update
// effect derived from a RepoEvent.
func TestRepoEffectKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		kind string
		ev   RepoEvent
		want string
	}{
		{name: "auto-update trigger", kind: prwire.EffectGitHubAutoUpdate, ev: RepoEvent{Repo: "azylman/mirrormere", PRNumber: 1010, HeadSHA: "def5678", CheckName: "CI Gate"}, want: "azylman/mirrormere#1010@def5678:github_auto_update:check=ci gate"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, RepoEffectKey(tc.kind, tc.ev).MatchKey())
		})
	}
}
