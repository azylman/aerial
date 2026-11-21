package router

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/azylman/aerial/services/synapse/internal/config"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// githubIngress is a hermetic Restate ingress fake that answers every
// request with one status (500 for paths naming failHandler) and records
// the path and body.
type githubIngress struct {
	mu     sync.Mutex
	paths  []string
	bodies []json.RawMessage
}

func newGitHubIngress(t *testing.T, status int, failHandler string) (*githubIngress, *httptest.Server) {
	t.Helper()
	rec := &githubIngress{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.paths = append(rec.paths, r.URL.EscapedPath())
		rec.bodies = append(rec.bodies, append(json.RawMessage(nil), raw...))
		rec.mu.Unlock()
		if failHandler != "" && strings.Contains(r.URL.Path, "/"+failHandler+"/") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return rec, srv
}

func (r *githubIngress) Paths() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.paths...)
}

func (r *githubIngress) Body(i int) json.RawMessage {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bodies[i]
}

// autoUpdateGitHub is a hermetic GitHub fake: one open PR by bot on head
// h1 with no check runs, accepting update-branch and rerun calls.
func autoUpdateGitHub(t *testing.T, puts, reruns *atomic.Int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/pulls"):
			_, _ = w.Write([]byte(`[{"number":7,"head":{"ref":"b7","sha":"h1"},"user":{"login":"bot"}}]`))
		case strings.HasSuffix(r.URL.Path, "/check-runs"):
			_, _ = w.Write([]byte(`{"check_runs":[]}`))
		case r.Method == http.MethodPut:
			puts.Add(1)
			w.WriteHeader(http.StatusAccepted)
		case strings.HasSuffix(r.URL.Path, "/rerun-failed-jobs"):
			reruns.Add(1)
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestAutoUpdatePolicy(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  Config
		repo string
		want *restateclient.AutoUpdatePolicy
	}{
		{
			name: "authors, default owner, and the repo's collaborators sorted by author",
			cfg: Config{AutoUpdatePRAuthors: []string{"bot"}, DefaultOwner: true, CollaboratorCIAlerts: []config.CollaboratorCIAlertRule{
				{Repo: "azylman/aerial", ChannelID: "111111111111111111", Authors: map[string]string{"zed": "<@2>", "amy": "<@1>", "blank": " "}},
				{Repo: "azylman/mirrormere", ChannelID: "333333333333333333", Authors: map[string]string{"other": "<@3>"}},
			}},
			repo: "azylman/aerial",
			want: &restateclient.AutoUpdatePolicy{Authors: []string{"bot"}, DefaultOwner: true, Collaborators: []restateclient.CollaboratorAlert{
				{Author: "amy", ChannelID: "111111111111111111", Mention: "<@1>"},
				{Author: "zed", ChannelID: "111111111111111111", Mention: "<@2>"},
			}},
		},
		{name: "empty config renders an empty policy", repo: "azylman/aerial", want: &restateclient.AutoUpdatePolicy{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := NewRouterServer(fixedConfig(tc.cfg), nil)
			if got := srv.autoUpdatePolicy(tc.repo); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("autoUpdatePolicy() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestAutoUpdateOpenPRs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		owner         string
		authoritative bool
		journalStatus int
		repo          string
		wantErr       bool
		wantPuts      int64
		wantPaths     []string
		wantBody      string
	}{
		{name: "legacy without restate runs the cascade", owner: restateclient.EffectsOwnerLegacy, repo: "aerial", wantPuts: 1},
		{
			name: "legacy journal failure still runs the cascade", owner: restateclient.EffectsOwnerLegacy, authoritative: true, journalStatus: http.StatusInternalServerError, repo: "aerial",
			wantPuts: 1, wantPaths: []string{"/RepoObject/azylman%2Faerial/HandleAutoUpdate/send"},
		},
		{
			name: "restate-owned trigger journals the policy and skips the cascade", owner: restateclient.EffectsOwnerRestate, authoritative: true, journalStatus: http.StatusAccepted, repo: "aerial",
			wantPaths: []string{"/RepoObject/azylman%2Faerial/HandleAutoUpdate/send"},
			wantBody:  `{"repo":"azylman/aerial","main_sha":"","generation":0,"pr_number":12,"head_sha":"h12","agent":"","effects_owner":"restate","check_name":"CI Gate","auto_update":{"authors":["bot"],"default_owner":true}}`,
		},
		{
			name: "restate-owned journal failure surfaces", owner: restateclient.EffectsOwnerRestate, authoritative: true, journalStatus: http.StatusInternalServerError, repo: "aerial",
			wantErr: true, wantPaths: []string{"/RepoObject/azylman%2Faerial/HandleAutoUpdate/send"},
		},
		{name: "blank repo is a no-op", owner: restateclient.EffectsOwnerRestate, authoritative: true, journalStatus: http.StatusAccepted, repo: " "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var puts, reruns atomic.Int64
			gh := autoUpdateGitHub(t, &puts, &reruns)
			cfg := Config{AutoUpdatePRAuthors: []string{"bot"}, DefaultOwner: true, GitHubAPIURL: gh.URL, GitHubToken: "t", RestateAuthoritative: tc.authoritative}
			var ingress *githubIngress
			if tc.journalStatus != 0 {
				var srv *httptest.Server
				ingress, srv = newGitHubIngress(t, tc.journalStatus, "")
				cfg.RestateURL = srv.URL
			}
			router := newTestRouter(cfg, gh.Client(), &mockPRRegistry{}, &mockOutboundDispatcher{}, &mockEventBus{})
			err := router.autoUpdateOpenPRs(withEffectsOwner(context.Background(), tc.owner), tc.repo, 12, "h12", "", "CI Gate")
			if (err != nil) != tc.wantErr {
				t.Fatalf("autoUpdateOpenPRs() err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := puts.Load(); got != tc.wantPuts {
				t.Errorf("update-branch calls = %d, want %d", got, tc.wantPuts)
			}
			var paths []string
			if ingress != nil {
				paths = ingress.Paths()
			}
			if !reflect.DeepEqual(paths, tc.wantPaths) {
				t.Errorf("journal paths = %v, want %v", paths, tc.wantPaths)
			}
			if tc.wantBody != "" {
				if got := string(ingress.Body(0)); got != tc.wantBody {
					t.Errorf("journal body = %s, want %s", got, tc.wantBody)
				}
			}
		})
	}
}

func TestLogAutoUpdateError(t *testing.T) {
	t.Parallel()
	owned := &restateOwnedSendError{handler: "HandleAutoUpdate", err: errors.New("down")}
	tests := []struct {
		name string
		err  error
		want error
	}{
		{name: "nil stays nil"},
		{name: "legacy cascade failure is logged", err: errors.New("rate limit")},
		{name: "restate-owned send failure surfaces", err: owned, want: owned},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := logAutoUpdateError(context.Background(), "azylman/aerial", "main_push", tc.err); !errors.Is(got, tc.want) {
				t.Errorf("logAutoUpdateError() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRouteConflictCheck(t *testing.T) {
	t.Parallel()
	known := true
	collaboratorPR := GitHubPullRequest{Number: 7, State: "open", Title: "Fix", Head: GitHubPRRef{Ref: "b7", SHA: "h7"}, User: struct {
		Login string `json:"login"`
	}{Login: "octo"}}
	knownPR := collaboratorPR
	knownPR.Mergeable = &known
	tests := []struct {
		name          string
		owner         string
		journalStatus int
		prNumber      int
		pr            GitHubPullRequest
		wantRouted    bool
		wantErr       bool
		wantPaths     []string
		wantEffects   int
	}{
		{name: "known mergeability needs no check", owner: restateclient.EffectsOwnerRestate, journalStatus: http.StatusAccepted, prNumber: 7, pr: knownPR},
		{name: "missing pr number needs no check", owner: restateclient.EffectsOwnerRestate, journalStatus: http.StatusAccepted, pr: collaboratorPR},
		{
			name: "legacy check journals and falls through to the fetch", owner: restateclient.EffectsOwnerLegacy, journalStatus: http.StatusAccepted, prNumber: 7, pr: collaboratorPR,
			wantPaths: []string{"/PRObject/azylman%2Faerial%237/HandleConflictCheck/send"},
		},
		{
			name: "legacy journal failure still falls through", owner: restateclient.EffectsOwnerLegacy, journalStatus: http.StatusInternalServerError, prNumber: 7, pr: collaboratorPR,
			wantPaths: []string{"/PRObject/azylman%2Faerial%237/HandleConflictCheck/send"},
		},
		{
			name: "restate-owned check hands off with the collaborator dm", owner: restateclient.EffectsOwnerRestate, journalStatus: http.StatusAccepted, prNumber: 7, pr: collaboratorPR,
			wantRouted: true, wantPaths: []string{"/PRObject/azylman%2Faerial%237/HandleConflictCheck/send"}, wantEffects: 1,
		},
		{
			name: "restate-owned journal failure surfaces", owner: restateclient.EffectsOwnerRestate, journalStatus: http.StatusInternalServerError, prNumber: 7, pr: collaboratorPR,
			wantRouted: true, wantErr: true, wantPaths: []string{"/PRObject/azylman%2Faerial%237/HandleConflictCheck/send"}, wantEffects: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress, srv := newGitHubIngress(t, tc.journalStatus, "")
			cfg := Config{RestateAuthoritative: true, RestateURL: srv.URL, CollaboratorCIAlerts: []config.CollaboratorCIAlertRule{
				{Repo: "azylman/aerial", ChannelID: "111111111111111111", Authors: map[string]string{"octo": "<@42>"}},
			}}
			router := newTestRouter(cfg, nil, &mockPRRegistry{}, &mockOutboundDispatcher{}, &mockEventBus{})
			res := &ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: tc.prNumber, HeadSHA: "h7", Branch: "b7"}
			routed, err := router.routeConflictCheck(withEffectsOwner(context.Background(), tc.owner), res, tc.pr)
			if routed != tc.wantRouted || (err != nil) != tc.wantErr {
				t.Fatalf("routeConflictCheck() = %v, %v; want %v, wantErr %v", routed, err, tc.wantRouted, tc.wantErr)
			}
			if got := ingress.Paths(); !reflect.DeepEqual(got, tc.wantPaths) {
				t.Fatalf("journal paths = %v, want %v", got, tc.wantPaths)
			}
			if len(tc.wantPaths) == 0 {
				return
			}
			var ev restateclient.PREvent
			if err := json.Unmarshal(ingress.Body(0), &ev); err != nil {
				t.Fatalf("decode journal body: %v", err)
			}
			if len(ev.EriEffects) != tc.wantEffects {
				t.Errorf("eri effects = %+v, want %d", ev.EriEffects, tc.wantEffects)
			}
		})
	}
}

func TestCollaboratorConflictEffects(t *testing.T) {
	t.Parallel()
	pr := GitHubPullRequest{Number: 7, State: "open", Title: "Fix", Head: GitHubPRRef{Ref: "b7", SHA: "abcdef1234"}, User: struct {
		Login string `json:"login"`
	}{Login: "octo"}}
	tests := []struct {
		name  string
		rules []config.CollaboratorCIAlertRule
		want  []restateclient.EriEffect
	}{
		{
			name:  "matching rule renders the conflict dm",
			rules: []config.CollaboratorCIAlertRule{{Repo: "azylman/aerial", ChannelID: "111111111111111111", Authors: map[string]string{"octo": "<@42>"}}},
			want: []restateclient.EriEffect{{
				Kind: restateclient.EffectEriDirectMessage, ChannelID: "111111111111111111",
				Content: "<@42> Merge conflict with `main` detected on [azylman/aerial#7](https://github.com/azylman/aerial/pull/7) (\"Fix\", branch: b7, commit: abcdef1) — automatic branch update could not resolve conflicts. Please rebase or merge `origin/main` and push.",
			}},
		},
		{name: "no rules for the repo renders nothing"},
		{name: "unknown author renders nothing", rules: []config.CollaboratorCIAlertRule{{Repo: "azylman/aerial", ChannelID: "111111111111111111", Authors: map[string]string{"someone": "<@1>"}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			router := NewRouterServer(fixedConfig(Config{CollaboratorCIAlerts: tc.rules}), nil)
			if got := router.collaboratorConflictEffects(context.Background(), "azylman/aerial", pr); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("collaboratorConflictEffects() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestTryRerunFailedMainWorkflowRunOwnership(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		owner       string
		failHandler string
		wantHandled bool
		wantErr     bool
		wantReruns  int64
		wantPaths   []string
	}{
		{
			name: "restate-owned rerun journal failure surfaces", owner: restateclient.EffectsOwnerRestate, failHandler: "HandleWorkflowRerun", wantErr: true,
			wantPaths: []string{"/PRObject/azylman%2Faerial%23224/HandleCheckRerunRequested/send", "/RepoObject/azylman%2Faerial/HandleWorkflowRerun/send"},
		},
		{
			name: "legacy rerun journal failure still reruns here", owner: restateclient.EffectsOwnerLegacy, failHandler: "HandleWorkflowRerun", wantHandled: true, wantReruns: 1,
			wantPaths: []string{"/PRObject/azylman%2Faerial%23224/HandleCheckRerunRequested/send", "/RepoObject/azylman%2Faerial/HandleWorkflowRerun/send"},
		},
		{
			name: "restate-owned rerun is handed to RepoObject", owner: restateclient.EffectsOwnerRestate, wantHandled: true,
			wantPaths: []string{"/PRObject/azylman%2Faerial%23224/HandleCheckRerunRequested/send", "/RepoObject/azylman%2Faerial/HandleWorkflowRerun/send"},
		},
		{
			name: "legacy rerun journals and reruns here", owner: restateclient.EffectsOwnerLegacy, wantHandled: true, wantReruns: 1,
			wantPaths: []string{"/PRObject/azylman%2Faerial%23224/HandleCheckRerunRequested/send", "/RepoObject/azylman%2Faerial/HandleWorkflowRerun/send"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var puts, reruns atomic.Int64
			gh := autoUpdateGitHub(t, &puts, &reruns)
			ingress, srv := newGitHubIngress(t, http.StatusAccepted, tc.failHandler)
			cfg := Config{RestateAuthoritative: true, RestateURL: srv.URL, GitHubAPIURL: gh.URL, GitHubToken: "t", DefaultOwner: true}
			reg := &mockPRRegistry{resolveTargetID: "100000000000000002", resolveBranch: "feat/restate", resolveHeadSHA: "premerge224", resolveMergeSHA: "merge224"}
			router := newTestRouter(cfg, gh.Client(), reg, &mockOutboundDispatcher{}, &mockEventBus{})
			var p GitHubWorkflowRunPayload
			p.Action = ghStatusCompleted
			p.WorkflowRun.ID = 803
			p.WorkflowRun.RunAttempt = 1
			p.WorkflowRun.HeadBranch = "main"
			p.WorkflowRun.HeadSHA = "merge224"
			res := &ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: 224, HeadSHA: "merge224", Branch: "main", Conclusion: conclusionFailure, CheckName: "Continuous Delivery"}
			handled, err := router.tryRerunFailedMainWorkflowRun(withEffectsOwner(context.Background(), tc.owner), p, res)
			if (err != nil) != tc.wantErr || handled != tc.wantHandled {
				t.Fatalf("tryRerunFailedMainWorkflowRun() = %v, %v; want %v, wantErr %v", handled, err, tc.wantHandled, tc.wantErr)
			}
			if got := reruns.Load(); got != tc.wantReruns {
				t.Errorf("rerun calls = %d, want %d", got, tc.wantReruns)
			}
			if got := ingress.Paths(); !reflect.DeepEqual(got, tc.wantPaths) {
				t.Errorf("journal paths = %v, want %v", got, tc.wantPaths)
			}
		})
	}
}

func TestRestateOwnedAutoUpdateFailureRetriesPush(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		failHandler string
		wantErr     bool
		wantPushes  int
		wantPaths   []string
	}{
		{
			name: "failed cascade journal fails the push job before the git_push dispatch", failHandler: "HandleAutoUpdate", wantErr: true,
			wantPaths: []string{"/RepoObject/azylman%2Faerial/HandleMainPush/send", "/RepoObject/azylman%2Faerial/HandleAutoUpdate/send"},
		},
		{
			name:      "journaled cascade hands the git_push dispatch to RepoObject",
			wantPaths: []string{"/RepoObject/azylman%2Faerial/HandleMainPush/send", "/RepoObject/azylman%2Faerial/HandleAutoUpdate/send", "/RepoObject/azylman%2Faerial/HandleGitPush/send"},
		},
		{
			name: "failed git_push journal fails the push job", failHandler: "HandleGitPush", wantErr: true,
			wantPaths: []string{"/RepoObject/azylman%2Faerial/HandleMainPush/send", "/RepoObject/azylman%2Faerial/HandleAutoUpdate/send", "/RepoObject/azylman%2Faerial/HandleGitPush/send"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress, srv := newGitHubIngress(t, http.StatusAccepted, tc.failHandler)
			disp := &mockOutboundDispatcher{}
			router := newTestRouter(Config{RestateAuthoritative: true, RestateURL: srv.URL, DefaultOwner: true}, nil, &mockPRRegistry{}, disp, &mockEventBus{})
			body := []byte(`{"ref":"refs/heads/main","after":"main123","head_commit":{"id":"main123","message":"chore: direct push"},"repository":{"full_name":"azylman/aerial"}}`)
			_, err := router.processGitHubPushEvent(withEffectsOwner(context.Background(), restateclient.EffectsOwnerRestate), body, &ResolvedGitHubEvent{})
			if (err != nil) != tc.wantErr {
				t.Fatalf("processGitHubPushEvent() err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := len(disp.GitPushCalls()); got != tc.wantPushes {
				t.Errorf("git pushes = %d, want %d", got, tc.wantPushes)
			}
			if got := ingress.Paths(); !reflect.DeepEqual(got, tc.wantPaths) {
				t.Errorf("journal paths = %v, want %v", got, tc.wantPaths)
			}
		})
	}
}

func TestRestateOwnedAutoUpdateFailureRetriesGateSuccess(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		failHandler string
		wantErr     bool
		wantPaths   []string
	}{
		{
			name: "failed cascade journal fails the check job", failHandler: "HandleAutoUpdate", wantErr: true,
			wantPaths: []string{"/RepoObject/azylman%2Faerial/HandleAutoUpdate/send"},
		},
		{
			name:      "journaled cascade then journals the check outcome",
			wantPaths: []string{"/RepoObject/azylman%2Faerial/HandleAutoUpdate/send", "/PRObject/azylman%2Faerial%2342/HandleCheckPassed/send"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress, srv := newGitHubIngress(t, http.StatusAccepted, tc.failHandler)
			router := newTestRouter(Config{RestateAuthoritative: true, RestateURL: srv.URL, DefaultOwner: true}, nil, &mockPRRegistry{}, &mockOutboundDispatcher{}, &mockEventBus{})
			var p GitHubCheckRunPayload
			p.Action = ghStatusCompleted
			res := &ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: 42, HeadSHA: "h42", CheckName: "CI Gate", Conclusion: conclusionSuccess}
			err := router.reportCheckRunGateSuccess(withEffectsOwner(context.Background(), restateclient.EffectsOwnerRestate), p, res, false)
			if (err != nil) != tc.wantErr {
				t.Fatalf("reportCheckRunGateSuccess() err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := ingress.Paths(); !reflect.DeepEqual(got, tc.wantPaths) {
				t.Errorf("journal paths = %v, want %v", got, tc.wantPaths)
			}
		})
	}
}

func TestRestateOwnedCIFailureRetriggersAutoUpdate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		owner     string
		wantCount int
	}{
		{name: "restate-owned redelivery re-sends the cascade trigger without a fresh transition", owner: restateclient.EffectsOwnerRestate, wantCount: 1},
		{name: "legacy failure without a transition never cascades", owner: restateclient.EffectsOwnerLegacy},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress, srv := newGitHubIngress(t, http.StatusAccepted, "")
			router := newTestRouter(Config{RestateAuthoritative: true, RestateURL: srv.URL, DefaultOwner: true}, nil, &mockPRRegistry{}, &mockOutboundDispatcher{}, &mockEventBus{})
			res := &ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: 42, HeadSHA: "h42", CheckName: "test", Conclusion: conclusionFailure, TargetID: "100000000000000002"}
			if err := router.handleCIFailureTransition(withEffectsOwner(context.Background(), tc.owner), res, "", ""); err != nil {
				t.Fatalf("handleCIFailureTransition() err = %v", err)
			}
			count := 0
			for _, p := range ingress.Paths() {
				if p == "/RepoObject/azylman%2Faerial/HandleAutoUpdate/send" {
					count++
				}
			}
			if count != tc.wantCount {
				t.Errorf("HandleAutoUpdate sends = %d, want %d (paths %v)", count, tc.wantCount, ingress.Paths())
			}
		})
	}
}
