package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/synapse/internal/github"
)

func TestFormatCollaboratorPRTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		repo     string
		prNumber int
		prTitle  string
		branch   string
		headSHA  string
		want     string
	}{
		{
			name:     "full pr target formatted with markdown link and metadata",
			repo:     "azylman/aerial",
			prNumber: 42,
			prTitle:  "feat: add feature",
			branch:   "feat/something",
			headSHA:  "abcdef1234567890",
			want:     `[azylman/aerial#42](https://github.com/azylman/aerial/pull/42) ("feat: add feature", branch: feat/something, commit: abcdef1)`,
		},
		{
			name:     "missing repo falls back to plain pr format",
			repo:     "",
			prNumber: 42,
			prTitle:  "",
			branch:   "",
			headSHA:  "",
			want:     "PR #42",
		},
		{
			name:     "repo without extra metadata formats plain link",
			repo:     "azylman/aerial",
			prNumber: 42,
			prTitle:  "",
			branch:   "",
			headSHA:  "",
			want:     "[azylman/aerial#42](https://github.com/azylman/aerial/pull/42)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := github.FormatCollaboratorPRTarget(tc.repo, tc.prNumber, tc.prTitle, tc.branch, tc.headSHA)
			if got != tc.want {
				t.Errorf("formatCollaboratorPRTarget() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFormatCollaboratorCIFailureMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mention   string
		repo      string
		prNumber  int
		prTitle   string
		branch    string
		headSHA   string
		checkName string
		checkURL  string
		want      string
	}{
		{
			name:      "named check failure with url",
			mention:   "<@123>",
			repo:      "azylman/aerial",
			prNumber:  42,
			prTitle:   "fix: something",
			branch:    "fix/something",
			headSHA:   "1234567890abcdef",
			checkName: "Lint",
			checkURL:  "https://github.com/azylman/aerial/actions/runs/1",
			want:      "<@123> CI check \"Lint\" failed for [azylman/aerial#42](https://github.com/azylman/aerial/pull/42) (\"fix: something\", branch: fix/something, commit: 1234567).\nCheck URL: https://github.com/azylman/aerial/actions/runs/1",
		},
		{
			name:      "generic check failure without check name or url",
			mention:   "<@123>",
			repo:      "azylman/aerial",
			prNumber:  42,
			prTitle:   "",
			branch:    "",
			headSHA:   "",
			checkName: "",
			checkURL:  "",
			want:      "<@123> CI check failed for [azylman/aerial#42](https://github.com/azylman/aerial/pull/42).",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := formatCollaboratorCIFailureMessage(tc.mention, tc.repo, tc.prNumber, tc.prTitle, tc.branch, tc.headSHA, tc.checkName, tc.checkURL)
			if got != tc.want {
				t.Errorf("formatCollaboratorCIFailureMessage() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFormatCollaboratorConflictMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mention  string
		repo     string
		prNumber int
		prTitle  string
		branch   string
		headSHA  string
		want     string
	}{
		{
			name:     "conflict message formatted with mention and target",
			mention:  "<@123>",
			repo:     "azylman/aerial",
			prNumber: 42,
			prTitle:  "feat: cool thing",
			branch:   "feat/cool",
			headSHA:  "abcdef123456",
			want:     "<@123> Merge conflict with `main` detected on [azylman/aerial#42](https://github.com/azylman/aerial/pull/42) (\"feat: cool thing\", branch: feat/cool, commit: abcdef1) — automatic branch update could not resolve conflicts. Please rebase or merge `origin/main` and push.",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := formatCollaboratorConflictMessage(tc.mention, tc.repo, tc.prNumber, tc.prTitle, tc.branch, tc.headSHA)
			if got != tc.want {
				t.Errorf("formatCollaboratorConflictMessage() = %q, want %q", got, tc.want)
			}
		})
	}
}

type collaboratorCIFailureTestCase struct {
	name          string
	resRepo       string
	resPRNum      int
	resBranch     string
	resHeadSHA    string
	resTargetID   string
	resCheckName  string
	checkURL      string
	apiState      string
	apiMerged     bool
	apiBody       string
	apiAuthor     string
	apiHeadSHA    string
	alertRules    []CollaboratorCIAlertRule
	dmErr         error
	wantErr       bool
	wantDMCalls   int
	wantDMChannel string
	wantContains  string
}

func runCollaboratorCIFailureCase(ctx context.Context, t *testing.T, tc collaboratorCIFailureTestCase) {
	t.Helper()
	t.Parallel()

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, fmt.Sprintf("/pulls/%d", tc.resPRNum)) {
			sha := tc.apiHeadSHA
			if sha == "" {
				sha = tc.resHeadSHA
			}
			pr := GitHubPullRequest{
				Number: tc.resPRNum,
				Title:  "Test Collaborator PR",
				State:  tc.apiState,
				Merged: tc.apiMerged,
				Body:   tc.apiBody,
				User: struct {
					Login string `json:"login"`
				}{Login: tc.apiAuthor},
				Head: GitHubPRRef{
					Ref: tc.resBranch,
					SHA: sha,
				},
			}
			_ = json.NewEncoder(w).Encode(pr)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockServer.Close()

	disp := &mockOutboundDispatcher{
		directMessageErr: tc.dmErr,
	}
	cfg := Config{
		GitHubAPIURL:         mockServer.URL,
		CollaboratorCIAlerts: tc.alertRules,
	}
	srv := newTestRouter(cfg, mockServer.Client(), nil, disp)

	res := &ResolvedGitHubEvent{
		Repo:      tc.resRepo,
		PRNumber:  tc.resPRNum,
		Branch:    tc.resBranch,
		HeadSHA:   tc.resHeadSHA,
		TargetID:  tc.resTargetID,
		CheckName: tc.resCheckName,
	}

	err := srv.notifyCollaboratorCIFailure(ctx, res, tc.checkURL)
	if (err != nil) != tc.wantErr {
		t.Fatalf("notifyCollaboratorCIFailure() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErr {
		return
	}

	calls := disp.DirectMessageCalls()
	if len(calls) != tc.wantDMCalls {
		t.Fatalf("DirectMessageCalls() = %d, want %d", len(calls), tc.wantDMCalls)
	}
	if tc.wantDMCalls > 0 {
		if calls[0].ChannelID != tc.wantDMChannel {
			t.Errorf("channelID = %q, want %q", calls[0].ChannelID, tc.wantDMChannel)
		}
		if tc.wantContains != "" && !strings.Contains(calls[0].Content, tc.wantContains) {
			t.Errorf("content = %q, want contains %q", calls[0].Content, tc.wantContains)
		}

		// Without the deleted per-SHA mark, a duplicate redelivery resends
		// (at-least-once); the journaled Restate effect key owns dedup.
		if err := srv.notifyCollaboratorCIFailure(ctx, res, tc.checkURL); err != nil {
			t.Fatalf("second notifyCollaboratorCIFailure() err = %v", err)
		}
		if got := len(disp.DirectMessageCalls()); got != 2*tc.wantDMCalls {
			t.Errorf("DirectMessageCalls() after duplicate = %d, want %d", got, 2*tc.wantDMCalls)
		}
	}
}

func TestNotifyCollaboratorCIFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	standardRules := []CollaboratorCIAlertRule{
		{
			Repo:      "azylman/aerial",
			ChannelID: "111111111111111111",
			Authors: map[string]string{
				"collab-user": "<@123456>",
			},
		},
	}

	tests := []collaboratorCIFailureTestCase{
		{
			name:          "unregistered collaborator pr failure dispatches dm and resends on duplicate",
			resRepo:       "azylman/aerial",
			resPRNum:      42,
			resBranch:     "feat/collab",
			resHeadSHA:    "sha-collab-1",
			resCheckName:  "Test",
			checkURL:      "https://ci.example.com/build/1",
			apiState:      "open",
			apiMerged:     false,
			apiAuthor:     "collab-user",
			alertRules:    standardRules,
			wantDMCalls:   1,
			wantDMChannel: "111111111111111111",
			wantContains:  "<@123456> CI check \"Test\" failed",
		},
		{
			name:        "registered pr with target id skips dm",
			resRepo:     "azylman/aerial",
			resPRNum:    43,
			resBranch:   "feat/collab",
			resHeadSHA:  "sha-collab-2",
			resTargetID: "123456789012345678",
			apiState:    "open",
			apiAuthor:   "collab-user",
			alertRules:  standardRules,
			wantDMCalls: 0,
		},
		{
			name:        "pr with aerial trailer skips dm",
			resRepo:     "azylman/aerial",
			resPRNum:    44,
			resBranch:   "feat/collab",
			resHeadSHA:  "sha-collab-3",
			apiState:    "open",
			apiBody:     "PR body\n<!-- aerial-pr:{\"target_id\":\"100000000000000001\"} -->",
			apiAuthor:   "collab-user",
			alertRules:  standardRules,
			wantDMCalls: 0,
		},
		{
			name:        "unconfigured author skips dm",
			resRepo:     "azylman/aerial",
			resPRNum:    45,
			resBranch:   "feat/collab",
			resHeadSHA:  "sha-collab-4",
			apiState:    "open",
			apiAuthor:   "unknown-user",
			alertRules:  standardRules,
			wantDMCalls: 0,
		},
		{
			name:        "closed pr skips dm",
			resRepo:     "azylman/aerial",
			resPRNum:    46,
			resBranch:   "feat/collab",
			resHeadSHA:  "sha-collab-5",
			apiState:    "closed",
			apiAuthor:   "collab-user",
			alertRules:  standardRules,
			wantDMCalls: 0,
		},
		{
			name:        "api head sha mismatch skips dm",
			resRepo:     "azylman/aerial",
			resPRNum:    48,
			resBranch:   "feat/collab",
			resHeadSHA:  "sha-collab-7",
			apiHeadSHA:  "sha-collab-different",
			apiState:    "open",
			apiAuthor:   "collab-user",
			alertRules:  standardRules,
			wantDMCalls: 0,
		},
		{
			name:        "dm error propagates",
			resRepo:     "azylman/aerial",
			resPRNum:    49,
			resBranch:   "feat/collab",
			resHeadSHA:  "sha-collab-8",
			apiState:    "open",
			apiAuthor:   "collab-user",
			alertRules:  standardRules,
			dmErr:       errors.New("dm send failed"),
			wantErr:     true,
			wantDMCalls: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runCollaboratorCIFailureCase(ctx, t, tc)
		})
	}
}

type collaboratorConflictTestCase struct {
	name          string
	resRepo       string
	resPRNum      int
	resBranch     string
	resHeadSHA    string
	pr            GitHubPullRequest
	alertRules    []CollaboratorCIAlertRule
	dmErr         error
	apiErr        bool
	wantErr       bool
	wantDMCalls   int
	wantDMChannel string
	wantContains  string
}

func runCollaboratorConflictCase(ctx context.Context, t *testing.T, tc collaboratorConflictTestCase) {
	t.Helper()
	t.Parallel()

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tc.apiErr {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(tc.pr)
	}))
	defer mockServer.Close()

	disp := &mockOutboundDispatcher{directMessageErr: tc.dmErr}
	cfg := Config{
		GitHubAPIURL:         mockServer.URL,
		CollaboratorCIAlerts: tc.alertRules,
	}
	srv := newTestRouter(cfg, mockServer.Client(), nil, disp)

	err := srv.notifyCollaboratorConflict(ctx, tc.resRepo, tc.pr)
	if (err != nil) != tc.wantErr {
		t.Fatalf("notifyCollaboratorConflict() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErr {
		return
	}

	calls := disp.DirectMessageCalls()
	if len(calls) != tc.wantDMCalls {
		t.Fatalf("DirectMessageCalls() = %d, want %d", len(calls), tc.wantDMCalls)
	}
	if tc.wantDMCalls > 0 {
		if calls[0].ChannelID != tc.wantDMChannel {
			t.Errorf("channelID = %q, want %q", calls[0].ChannelID, tc.wantDMChannel)
		}
		if tc.wantContains != "" && !strings.Contains(calls[0].Content, tc.wantContains) {
			t.Errorf("content = %q, want contains %q", calls[0].Content, tc.wantContains)
		}

		// Without the deleted per-SHA mark, a duplicate redelivery resends
		// (at-least-once); the journaled Restate effect key owns dedup.
		if err := srv.notifyCollaboratorConflict(ctx, tc.resRepo, tc.pr); err != nil {
			t.Fatalf("second notifyCollaboratorConflict() err = %v", err)
		}
		if got := len(disp.DirectMessageCalls()); got != 2*tc.wantDMCalls {
			t.Errorf("DirectMessageCalls() after duplicate = %d, want %d", got, 2*tc.wantDMCalls)
		}
	}
}

func TestNotifyCollaboratorConflict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	standardRules := []CollaboratorCIAlertRule{
		{
			Repo:      "azylman/aerial",
			ChannelID: "111111111111111111",
			Authors: map[string]string{
				"collab-user": "<@123456>",
			},
		},
	}

	tests := []collaboratorConflictTestCase{
		{
			name:       "unregistered collaborator conflict dispatches dm and resends on duplicate",
			resRepo:    "azylman/aerial",
			resPRNum:   50,
			resBranch:  "feat/conflict",
			resHeadSHA: "sha-conflict-1",
			pr: GitHubPullRequest{
				Number: 50,
				Title:  "Conflict PR",
				State:  "open",
				User: struct {
					Login string `json:"login"`
				}{Login: "collab-user"},
				Head: GitHubPRRef{
					Ref: "feat/conflict",
					SHA: "sha-conflict-1",
				},
			},
			alertRules:    standardRules,
			wantDMCalls:   1,
			wantDMChannel: "111111111111111111",
			wantContains:  "<@123456> Merge conflict with `main` detected",
		},
		{
			name:       "pr with trailer skips conflict dm",
			resRepo:    "azylman/aerial",
			resPRNum:   51,
			resBranch:  "feat/conflict",
			resHeadSHA: "sha-conflict-2",
			pr: GitHubPullRequest{
				Number: 51,
				Title:  "Conflict PR",
				State:  "open",
				Body:   "<!-- aerial-pr:{\"target_id\":\"100000000000000001\"} -->",
				User: struct {
					Login string `json:"login"`
				}{Login: "collab-user"},
				Head: GitHubPRRef{
					Ref: "feat/conflict",
					SHA: "sha-conflict-2",
				},
			},
			alertRules:  standardRules,
			wantDMCalls: 0,
		},
		{
			name:       "dm error propagates and clears state",
			resRepo:    "azylman/aerial",
			resPRNum:   52,
			resBranch:  "feat/conflict",
			resHeadSHA: "sha-conflict-3",
			pr: GitHubPullRequest{
				Number: 52,
				Title:  "Conflict PR",
				State:  "open",
				User: struct {
					Login string `json:"login"`
				}{Login: "collab-user"},
				Head: GitHubPRRef{
					Ref: "feat/conflict",
					SHA: "sha-conflict-3",
				},
			},
			alertRules:    standardRules,
			dmErr:         errors.New("dm send failed"),
			wantErr:       true,
			wantDMCalls:   1,
			wantDMChannel: "111111111111111111",
		},
		{
			name:        "unconfigured repo skips conflict dm",
			resRepo:     "other/repo",
			resPRNum:    53,
			alertRules:  standardRules,
			wantDMCalls: 0,
		},
		{
			name:       "api error during resolve returns error",
			resRepo:    "azylman/aerial",
			resPRNum:   54,
			resBranch:  "feat/conflict",
			resHeadSHA: "sha-conflict-4",
			pr: GitHubPullRequest{
				Number: 54,
			},
			alertRules: standardRules,
			apiErr:     true,
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runCollaboratorConflictCase(ctx, t, tc)
		})
	}

	// Nil router server guard
	var nilSrv *RouterServer
	if err := nilSrv.notifyCollaboratorConflict(ctx, "azylman/aerial", GitHubPullRequest{}); err != nil {
		t.Errorf("expected nil error on nil router server")
	}
}

func TestSyncPRFallbackFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		res      *ResolvedGitHubEvent
		initial  GitHubPullRequest
		wantNum  int
		wantRef  string
		wantSHA  string
		wantName string
	}{
		{
			name:     "nil resolved event returns original unchanged",
			res:      nil,
			initial:  GitHubPullRequest{Title: "pr-unchanged"},
			wantName: "pr-unchanged",
		},
		{
			name:     "empty pr fields populated from resolved event",
			res:      &ResolvedGitHubEvent{PRNumber: 10, Branch: "feat/foo", HeadSHA: "sha10"},
			initial:  GitHubPullRequest{Title: "pr-updated"},
			wantNum:  10,
			wantRef:  "feat/foo",
			wantSHA:  "sha10",
			wantName: "pr-updated",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := syncPRFallbackFields(tc.res, tc.initial)
			if got.Title != tc.wantName || got.Number != tc.wantNum || got.Head.Ref != tc.wantRef || got.Head.SHA != tc.wantSHA {
				t.Errorf("syncPRFallbackFields(%+v, %+v) = %+v, want num=%d ref=%s sha=%s title=%s",
					tc.res, tc.initial, got, tc.wantNum, tc.wantRef, tc.wantSHA, tc.wantName)
			}
		})
	}
}

func TestDispatchCollaboratorDM_Guards(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	tests := []struct {
		name          string
		nilServer     bool
		nilDispatcher bool
		channelID     string
		sha           string
		owner         string
		wantDispatch  bool
	}{
		{
			name:         "restate-owned event skips the legacy send",
			channelID:    "111111111111111111",
			sha:          "abcdef1",
			owner:        "restate",
			wantDispatch: false,
		},
		{
			name:         "nil server returns nil",
			nilServer:    true,
			channelID:    "111111111111111111",
			sha:          "abcdef1",
			wantDispatch: false,
		},
		{
			name:          "nil dispatcher returns nil",
			nilDispatcher: true,
			channelID:     "111111111111111111",
			sha:           "abcdef1",
			wantDispatch:  false,
		},
		{
			name:         "empty channel id returns nil without dispatch",
			channelID:    "",
			sha:          "abcdef1",
			wantDispatch: false,
		},
		{
			name:         "empty sha returns nil without dispatch",
			channelID:    "111111111111111111",
			sha:          "",
			wantDispatch: false,
		},
		{
			name:         "zero sha returns nil without dispatch",
			channelID:    "111111111111111111",
			sha:          "0000000000000000000000000000000000000000",
			wantDispatch: false,
		},
		{
			name:         "successful dispatch sends DM",
			channelID:    "111111111111111111",
			sha:          "abcdef1",
			wantDispatch: true,
		},
	}

	// Every subtest owns its dispatcher mock: the guards assert exact dispatch
	// counts, so sharing one mock across parallel subtests would attribute a
	// concurrent successful dispatch to a guard case.
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDisp := &mockOutboundDispatcher{}
			srv := &RouterServer{dispatcher: mockDisp}
			if tc.nilServer {
				srv = nil
			}
			if tc.nilDispatcher {
				srv = &RouterServer{}
			}
			callCtx := ctx
			if tc.owner != "" {
				callCtx = withEffectsOwner(ctx, tc.owner)
			}
			if err := srv.dispatchCollaboratorDM(callCtx, "repo", 1, tc.sha, "", tc.channelID, "msg"); err != nil {
				t.Errorf("dispatchCollaboratorDM error = %v, want nil", err)
			}
			want := 0
			if tc.wantDispatch {
				want = 1
			}
			if got := len(mockDisp.DirectMessageCalls()); got != want {
				t.Errorf("direct message dispatches = %d, want %d", got, want)
			}
		})
	}

	dupDisp := &mockOutboundDispatcher{}
	dupSrv := &RouterServer{dispatcher: dupDisp}
	// Without the deleted per-SHA dedup map, repeat deliveries resend
	// (at-least-once); the Restate journal owns dedup.
	if err := dupSrv.dispatchCollaboratorDM(ctx, "repo", 1, "sha1", "", "111111111111111111", "msg"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	beforeCalls := len(dupDisp.DirectMessageCalls())
	if err := dupSrv.dispatchCollaboratorDM(ctx, "repo", 1, "sha1", "", "111111111111111111", "msg"); err != nil {
		t.Fatalf("unexpected error on duplicate: %v", err)
	}
	if len(dupDisp.DirectMessageCalls()) != beforeCalls+1 {
		t.Errorf("expected duplicate dispatch to resend without dedup")
	}

	failDisp := &mockOutboundDispatcher{directMessageErr: errors.New("dm failed")}
	srvFail := &RouterServer{dispatcher: failDisp}
	if err := srvFail.dispatchCollaboratorDM(ctx, "repo", 1, "sha2", "", "111111111111111111", "msg"); err == nil {
		t.Fatalf("expected error from failed dispatch")
	}
}

func TestResolveCollaboratorAlertTarget_Guards(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	srv := newTestRouter(Config{
		CollaboratorCIAlerts: []CollaboratorCIAlertRule{
			{
				Repo:      "azylman/aerial",
				ChannelID: "111111111111111111",
				Authors:   map[string]string{"collab-user": "<@123>"},
			},
		},
	}, nil, nil, nil)

	tests := []struct {
		name        string
		srv         *RouterServer
		prNum       int
		expectedSHA string
		pr          GitHubPullRequest
		wantOK      bool
	}{
		{
			name:        "nil server returns false",
			srv:         nil,
			prNum:       1,
			expectedSHA: "sha1",
			pr: GitHubPullRequest{Number: 1, User: struct {
				Login string `json:"login"`
			}{Login: "collab-user"}},
			wantOK: false,
		},
		{
			name:        "invalid pr number returns false",
			srv:         srv,
			prNum:       0,
			expectedSHA: "sha1",
			pr: GitHubPullRequest{Number: 0, User: struct {
				Login string `json:"login"`
			}{Login: "collab-user"}},
			wantOK: false,
		},
		{
			name:        "merged pr returns false",
			srv:         srv,
			prNum:       1,
			expectedSHA: "sha1",
			pr: GitHubPullRequest{
				Number: 1,
				Merged: true,
				User: struct {
					Login string `json:"login"`
				}{Login: "collab-user"},
			},
			wantOK: false,
		},
		{
			name:        "closed pr returns false",
			srv:         srv,
			prNum:       1,
			expectedSHA: "sha1",
			pr: GitHubPullRequest{
				Number: 1,
				State:  "closed",
				User: struct {
					Login string `json:"login"`
				}{Login: "collab-user"},
			},
			wantOK: false,
		},
		{
			name:        "mismatched expected sha returns false",
			srv:         srv,
			prNum:       1,
			expectedSHA: "sha-expected",
			pr: GitHubPullRequest{
				Number: 1,
				State:  "open",
				Head:   GitHubPRRef{SHA: "sha-actual"},
				User: struct {
					Login string `json:"login"`
				}{Login: "collab-user"},
			},
			wantOK: false,
		},
		{
			name:        "unconfigured author returns false",
			srv:         srv,
			prNum:       1,
			expectedSHA: "sha1",
			pr: GitHubPullRequest{
				Number: 1,
				State:  "open",
				Head:   GitHubPRRef{SHA: "sha1"},
				User: struct {
					Login string `json:"login"`
				}{Login: "unconfigured-user"},
			},
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, _, ok, err := tc.srv.resolveCollaboratorAlertTarget(ctx, "azylman/aerial", tc.prNum, tc.expectedSHA, tc.pr)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tc.wantOK {
				t.Errorf("resolveCollaboratorAlertTarget() ok = %v, want %v", ok, tc.wantOK)
			}
		})
	}
}
