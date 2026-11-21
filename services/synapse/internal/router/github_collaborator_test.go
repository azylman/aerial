package router

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/synapse/internal/github"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
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
	alertFails    bool
	wantErr       bool
	wantAlerts    int
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

	srv, ingress, disp := newCollaboratorRouter(t, mockServer, tc.alertRules, tc.alertFails)

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

	assertCollaboratorAlerts(t, ingress, disp, tc.wantAlerts, tc.wantDMChannel, tc.wantContains)
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
			wantAlerts:    1,
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
			wantAlerts:  0,
		},
		{
			name:       "pr with aerial trailer skips dm",
			resRepo:    "azylman/aerial",
			resPRNum:   44,
			resBranch:  "feat/collab",
			resHeadSHA: "sha-collab-3",
			apiState:   "open",
			apiBody:    "PR body\n<!-- aerial-pr:{\"target_id\":\"100000000000000001\"} -->",
			apiAuthor:  "collab-user",
			alertRules: standardRules,
			wantAlerts: 0,
		},
		{
			name:       "unconfigured author skips dm",
			resRepo:    "azylman/aerial",
			resPRNum:   45,
			resBranch:  "feat/collab",
			resHeadSHA: "sha-collab-4",
			apiState:   "open",
			apiAuthor:  "unknown-user",
			alertRules: standardRules,
			wantAlerts: 0,
		},
		{
			name:       "closed pr skips dm",
			resRepo:    "azylman/aerial",
			resPRNum:   46,
			resBranch:  "feat/collab",
			resHeadSHA: "sha-collab-5",
			apiState:   "closed",
			apiAuthor:  "collab-user",
			alertRules: standardRules,
			wantAlerts: 0,
		},
		{
			name:       "api head sha mismatch skips dm",
			resRepo:    "azylman/aerial",
			resPRNum:   48,
			resBranch:  "feat/collab",
			resHeadSHA: "sha-collab-7",
			apiHeadSHA: "sha-collab-different",
			apiState:   "open",
			apiAuthor:  "collab-user",
			alertRules: standardRules,
			wantAlerts: 0,
		},
		{
			name:       "dm error propagates",
			resRepo:    "azylman/aerial",
			resPRNum:   49,
			resBranch:  "feat/collab",
			resHeadSHA: "sha-collab-8",
			apiState:   "open",
			apiAuthor:  "collab-user",
			alertRules: standardRules,
			alertFails: true,
			wantErr:    true,
			wantAlerts: 1,
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
	alertFails    bool
	apiErr        bool
	wantErr       bool
	wantAlerts    int
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

	srv, ingress, disp := newCollaboratorRouter(t, mockServer, tc.alertRules, tc.alertFails)

	err := srv.notifyCollaboratorConflict(ctx, tc.resRepo, tc.pr)
	if (err != nil) != tc.wantErr {
		t.Fatalf("notifyCollaboratorConflict() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErr {
		return
	}

	assertCollaboratorAlerts(t, ingress, disp, tc.wantAlerts, tc.wantDMChannel, tc.wantContains)
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
			wantAlerts:    1,
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
			alertRules: standardRules,
			wantAlerts: 0,
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
			alertFails:    true,
			wantErr:       true,
			wantAlerts:    1,
			wantDMChannel: "111111111111111111",
		},
		{
			name:       "unconfigured repo skips conflict dm",
			resRepo:    "other/repo",
			resPRNum:   53,
			alertRules: standardRules,
			wantAlerts: 0,
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
	}, nil, nil)

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

// newCollaboratorRouter wires a router whose collaborator alerts go to a fake
// Restate ingress (failing every send when alertFails) and whose dispatcher
// records any DM synapse would wrongly send itself.
func newCollaboratorRouter(t *testing.T, gh *httptest.Server, rules []CollaboratorCIAlertRule, alertFails bool) (*RouterServer, *ownedIngress, *mockOutboundDispatcher) {
	t.Helper()
	ingress := &ownedIngress{sendCode: http.StatusAccepted}
	if alertFails {
		ingress.sendCode = http.StatusServiceUnavailable
	}
	disp := &mockOutboundDispatcher{}
	cfg := Config{GitHubAPIURL: gh.URL, CollaboratorCIAlerts: rules, RestateURL: ingress.serve(t)}
	return newTestRouter(cfg, gh.Client(), disp), ingress, disp
}

// assertCollaboratorAlerts checks the HandleCollaboratorAlert sends: their
// count, and the DM the first one carries for PRObject to send. synapse
// itself never sends the DM.
func assertCollaboratorAlerts(t *testing.T, ingress *ownedIngress, disp *mockOutboundDispatcher, wantAlerts int, wantChannel, wantContains string) {
	t.Helper()
	paths, bodies := ingress.sends()
	if len(paths) != wantAlerts {
		t.Fatalf("collaborator alert sends = %v, want %d", paths, wantAlerts)
	}
	if got := len(disp.DirectMessageCalls()); got != 0 {
		t.Errorf("synapse DirectMessageCalls() = %d, want 0", got)
	}
	if wantAlerts == 0 {
		return
	}
	if !strings.HasSuffix(paths[0], "/HandleCollaboratorAlert/send") {
		t.Errorf("send path = %s, want HandleCollaboratorAlert", paths[0])
	}
	var ev restateclient.PREvent
	if err := json.Unmarshal([]byte(bodies[0]), &ev); err != nil {
		t.Fatalf("decode alert: %v", err)
	}
	if len(ev.EriEffects) != 1 || ev.EriEffects[0].ChannelID != wantChannel || !strings.Contains(ev.EriEffects[0].Content, wantContains) {
		t.Errorf("eri_effects = %+v, want one DM to %s containing %q", ev.EriEffects, wantChannel, wantContains)
	}
}
