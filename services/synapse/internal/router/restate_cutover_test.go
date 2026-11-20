package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// cutoverTestServer starts a hermetic Restate ingress fake that answers every
// request with status and counts invocations. Callers close it via cleanup.
func cutoverTestServer(t *testing.T, status int, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(status)
	}))
}

func TestRestateAuthoritativeFlag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cfg       Config
		nilServer bool
		want      bool
	}{
		{name: "nil server defaults to legacy", nilServer: true, want: false},
		{name: "empty config defaults to legacy", cfg: Config{}, want: false},
		{name: "explicit legacy with url stays legacy", cfg: Config{RestateURL: "http://127.0.0.1:8080"}, want: false},
		{name: "authoritative flag flips without url", cfg: Config{RestateAuthoritative: true}, want: true},
		{name: "authoritative flag flips with url", cfg: Config{RestateAuthoritative: true, RestateURL: "http://127.0.0.1:8080"}, want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var srv *RouterServer
			if !tc.nilServer {
				srv = NewRouterServer(fixedConfig(tc.cfg), nil)
			}
			if got := srv.restateAuthoritative(); got != tc.want {
				t.Fatalf("restateAuthoritative() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRoutePREventDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		authoritative bool
		capture       bool
		captureStatus int
		handler       string
		prNumber      int
		wantErr       bool
		wantCalls     int64
	}{
		{name: "shadow disabled without url sends nothing", authoritative: false, capture: false, handler: "HandleCheckPassed", prNumber: 7, wantErr: false, wantCalls: 0},
		{name: "shadow mode fails closed without error", authoritative: false, capture: true, captureStatus: 500, handler: "HandleCheckPassed", prNumber: 7, wantErr: false, wantCalls: 1},
		{name: "shadow mode drops missing pr without send", authoritative: false, capture: true, captureStatus: 202, handler: "HandleCheckPassed", prNumber: 0, wantErr: false, wantCalls: 0},
		{name: "authoritative success journals event", authoritative: true, capture: true, captureStatus: 202, handler: "HandleCheckPassed", prNumber: 7, wantErr: false, wantCalls: 1},
		{name: "authoritative ingress failure surfaces", authoritative: true, capture: true, captureStatus: 500, handler: "HandleCheckPassed", prNumber: 7, wantErr: true, wantCalls: 1},
		{name: "authoritative without url surfaces misconfiguration", authoritative: true, capture: false, handler: "HandleCheckPassed", prNumber: 7, wantErr: true, wantCalls: 0},
		{name: "authoritative rejects unknown handler", authoritative: true, capture: false, handler: "HandleNope", prNumber: 7, wantErr: true, wantCalls: 0},
		{name: "authoritative rejects missing pr", authoritative: true, capture: true, captureStatus: 202, handler: "HandleCheckPassed", prNumber: 0, wantErr: true, wantCalls: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			url := ""
			if tc.capture {
				srv := cutoverTestServer(t, tc.captureStatus, &calls)
				t.Cleanup(srv.Close)
				url = srv.URL
			}
			srv := NewRouterServer(fixedConfig(Config{RestateAuthoritative: tc.authoritative, RestateURL: url}), nil)
			ev := restateclient.PREvent{Repo: "azylman/aerial", PRNumber: tc.prNumber, HeadSHA: shadowTestHeadSHA()}
			err := srv.routePREvent(context.Background(), tc.handler, ev)
			if (err != nil) != tc.wantErr {
				t.Fatalf("routePREvent() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got := calls.Load(); got != tc.wantCalls {
				t.Fatalf("routePREvent() calls = %d, want %d", got, tc.wantCalls)
			}
		})
	}
}

func TestRouteRepoEventDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		authoritative bool
		capture       bool
		captureStatus int
		repo          string
		wantErr       bool
		wantCalls     int64
	}{
		{name: "shadow disabled without url sends nothing", authoritative: false, capture: false, repo: "azylman/aerial", wantErr: false, wantCalls: 0},
		{name: "authoritative success journals event", authoritative: true, capture: true, captureStatus: 202, repo: "azylman/aerial", wantErr: false, wantCalls: 1},
		{name: "authoritative ingress failure surfaces", authoritative: true, capture: true, captureStatus: 500, repo: "azylman/aerial", wantErr: true, wantCalls: 1},
		{name: "authoritative without url surfaces misconfiguration", authoritative: true, capture: false, repo: "azylman/aerial", wantErr: true, wantCalls: 0},
		{name: "authoritative rejects blank repo", authoritative: true, capture: true, captureStatus: 202, repo: "   ", wantErr: true, wantCalls: 0},
		{name: "shadow drops blank repo without send", authoritative: false, capture: true, captureStatus: 202, repo: "   ", wantErr: false, wantCalls: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			url := ""
			if tc.capture {
				srv := cutoverTestServer(t, tc.captureStatus, &calls)
				t.Cleanup(srv.Close)
				url = srv.URL
			}
			srv := NewRouterServer(fixedConfig(Config{RestateAuthoritative: tc.authoritative, RestateURL: url}), nil)
			ev := restateclient.RepoEvent{Repo: tc.repo, MainSHA: shadowTestHeadSHA()}
			err := srv.routeRepoEvent(context.Background(), "HandleMainPush", ev)
			if (err != nil) != tc.wantErr {
				t.Fatalf("routeRepoEvent() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got := calls.Load(); got != tc.wantCalls {
				t.Fatalf("routeRepoEvent() calls = %d, want %d", got, tc.wantCalls)
			}
		})
	}
}

// The cutover never changes legacy decision behavior: the trackers stay
// fully live during the soak so in-flight builds and PRs complete and
// webhook redelivery stays idempotent. Authority moves via the journal,
// not by mutating the trackers.
func TestRouteCheckOutcome(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		authoritative bool
		capture       bool
		captureStatus int
		conclusion    string
		prNumber      int
		headSHA       string
		wantErr       bool
		wantCalls     int64
	}{
		{name: "shadow pending conclusion journals nothing", authoritative: false, capture: true, captureStatus: 202, conclusion: "neutral", prNumber: 7, headSHA: shadowTestHeadSHA(), wantErr: false, wantCalls: 0},
		{name: "shadow success without url journals nothing", authoritative: false, capture: false, conclusion: "success", prNumber: 7, headSHA: shadowTestHeadSHA(), wantErr: false, wantCalls: 0},
		{name: "authoritative pending conclusion journals nothing", authoritative: true, capture: true, captureStatus: 202, conclusion: "neutral", prNumber: 7, headSHA: shadowTestHeadSHA(), wantErr: false, wantCalls: 0},
		{name: "authoritative success journals outcome", authoritative: true, capture: true, captureStatus: 202, conclusion: "success", prNumber: 7, headSHA: shadowTestHeadSHA(), wantErr: false, wantCalls: 1},
		{name: "authoritative ingress failure surfaces", authoritative: true, capture: true, captureStatus: 500, conclusion: "failure", prNumber: 7, headSHA: shadowTestHeadSHA(), wantErr: true, wantCalls: 1},
		{name: "authoritative missing head sha journals nothing", authoritative: true, capture: true, captureStatus: 202, conclusion: "success", prNumber: 7, headSHA: "", wantErr: false, wantCalls: 0},
		{name: "authoritative missing pr journals nothing", authoritative: true, capture: true, captureStatus: 202, conclusion: "success", prNumber: 0, headSHA: shadowTestHeadSHA(), wantErr: false, wantCalls: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			url := ""
			if tc.capture {
				srv := cutoverTestServer(t, tc.captureStatus, &calls)
				t.Cleanup(srv.Close)
				url = srv.URL
			}
			srv := NewRouterServer(fixedConfig(Config{RestateAuthoritative: tc.authoritative, RestateURL: url}), nil)
			res := &ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: tc.prNumber, HeadSHA: tc.headSHA, Conclusion: tc.conclusion}
			err := srv.routeCheckOutcome(context.Background(), res)
			if (err != nil) != tc.wantErr {
				t.Fatalf("routeCheckOutcome() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got := calls.Load(); got != tc.wantCalls {
				t.Fatalf("routeCheckOutcome() calls = %d, want %d", got, tc.wantCalls)
			}
		})
	}
}

func TestRouteConflictResolved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		authoritative bool
		capture       bool
		captureStatus int
		nilRes        bool
		wantErr       bool
		wantCalls     int64
	}{
		{name: "shadow nil res journals nothing", authoritative: false, capture: false, nilRes: true, wantErr: false, wantCalls: 0},
		{name: "authoritative nil res journals nothing", authoritative: true, capture: true, captureStatus: 202, nilRes: true, wantErr: false, wantCalls: 0},
		{name: "shadow recovery mirrors fail-closed", authoritative: false, capture: true, captureStatus: 202, wantErr: false, wantCalls: 1},
		{name: "authoritative recovery journals event", authoritative: true, capture: true, captureStatus: 202, wantErr: false, wantCalls: 1},
		{name: "authoritative ingress failure surfaces", authoritative: true, capture: true, captureStatus: 500, wantErr: true, wantCalls: 1},
		{name: "authoritative without url surfaces misconfiguration", authoritative: true, capture: false, wantErr: true, wantCalls: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			url := ""
			if tc.capture {
				srv := cutoverTestServer(t, tc.captureStatus, &calls)
				t.Cleanup(srv.Close)
				url = srv.URL
			}
			srv := NewRouterServer(fixedConfig(Config{RestateAuthoritative: tc.authoritative, RestateURL: url}), nil)
			var res *ResolvedGitHubEvent
			if !tc.nilRes {
				res = &ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: 7, HeadSHA: shadowTestHeadSHA(), Branch: "feat/x"}
			}
			err := srv.routeConflictResolved(context.Background(), res)
			if (err != nil) != tc.wantErr {
				t.Fatalf("routeConflictResolved() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got := calls.Load(); got != tc.wantCalls {
				t.Fatalf("routeConflictResolved() calls = %d, want %d", got, tc.wantCalls)
			}
		})
	}
}

func TestRouteDeployDispatched(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		authoritative bool
		capture       bool
		captureStatus int
		prNumber      int
		headSHA       string
		wantErr       bool
		wantCalls     int64
	}{
		{name: "shadow dispatch mirrors fail-closed", authoritative: false, capture: true, captureStatus: 202, prNumber: 7, headSHA: shadowTestHeadSHA(), wantErr: false, wantCalls: 1},
		{name: "shadow empty dispatch sends nothing", authoritative: false, capture: true, captureStatus: 202, prNumber: 0, wantErr: false, wantCalls: 0},
		{name: "authoritative dispatch journals event", authoritative: true, capture: true, captureStatus: 202, prNumber: 7, headSHA: shadowTestHeadSHA(), wantErr: false, wantCalls: 1},
		{name: "authoritative ingress failure surfaces", authoritative: true, capture: true, captureStatus: 500, prNumber: 7, headSHA: shadowTestHeadSHA(), wantErr: true, wantCalls: 1},
		{name: "authoritative empty dispatch journals nothing", authoritative: true, capture: true, captureStatus: 202, prNumber: 0, wantErr: false, wantCalls: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			url := ""
			if tc.capture {
				srv := cutoverTestServer(t, tc.captureStatus, &calls)
				t.Cleanup(srv.Close)
				url = srv.URL
			}
			srv := NewRouterServer(fixedConfig(Config{RestateAuthoritative: tc.authoritative, RestateURL: url}), nil)
			err := srv.routeDeployDispatched(context.Background(), "azylman/aerial", "api", tc.headSHA, "", tc.prNumber)
			if (err != nil) != tc.wantErr {
				t.Fatalf("routeDeployDispatched() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got := calls.Load(); got != tc.wantCalls {
				t.Fatalf("routeDeployDispatched() calls = %d, want %d", got, tc.wantCalls)
			}
		})
	}
}

func TestRouteRolloutProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		authoritative bool
		capture       bool
		captureStatus int
		prNumber      int
		wantErr       bool
		wantCalls     int64
	}{
		{name: "shadow progress mirrors fail-closed", authoritative: false, capture: true, captureStatus: 202, prNumber: 7, wantErr: false, wantCalls: 1},
		{name: "authoritative progress journals event", authoritative: true, capture: true, captureStatus: 202, prNumber: 7, wantErr: false, wantCalls: 1},
		{name: "authoritative ingress failure surfaces", authoritative: true, capture: true, captureStatus: 500, prNumber: 7, wantErr: true, wantCalls: 1},
		{name: "authoritative missing pr journals nothing", authoritative: true, capture: true, captureStatus: 202, prNumber: 0, wantErr: false, wantCalls: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			url := ""
			if tc.capture {
				srv := cutoverTestServer(t, tc.captureStatus, &calls)
				t.Cleanup(srv.Close)
				url = srv.URL
			}
			srv := NewRouterServer(fixedConfig(Config{RestateAuthoritative: tc.authoritative, RestateURL: url}), nil)
			err := srv.routeRolloutProgress(context.Background(), "azylman/aerial", tc.prNumber, shadowTestHeadSHA(), shadowTestMergeSHA(), "api")
			if (err != nil) != tc.wantErr {
				t.Fatalf("routeRolloutProgress() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got := calls.Load(); got != tc.wantCalls {
				t.Fatalf("routeRolloutProgress() calls = %d, want %d", got, tc.wantCalls)
			}
		})
	}
}

// flapRestateServer is a hermetic Restate ingress fake that answers the
// first journal attempt with 500 and every later attempt with 202,
// simulating a transient Restate outage followed by recovery across a
// webhook redelivery. It counts every attempt.
func flapRestateServer(t *testing.T, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
}

func TestAuthoritativeDeployJournalSurvivesRetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		prNumber      int
		wantFirstErr  bool
		wantSecondErr bool
		// wantCalls counts ingress attempts: the failed first attempt plus
		// the retry. The 500 commits nothing, so the retry must resend;
		// Restate dedups only committed effects.
		wantCalls     int64
		wantPublishes int
	}{
		{name: "deploy success journals on retry after flap", prNumber: 42, wantFirstErr: true, wantSecondErr: false, wantCalls: 2, wantPublishes: 1},
		// Non-PR dispatches have no PRObject key and no registry row, so a
		// redelivery republishes: at-least-once without the deleted map.
		{name: "non-pr dispatch republishes on redelivery at-least-once", prNumber: 0, wantFirstErr: false, wantSecondErr: false, wantCalls: 0, wantPublishes: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			flap := flapRestateServer(t, &calls)
			t.Cleanup(flap.Close)
			cfg := lifecycleCfg()
			cfg.RestateAuthoritative = true
			cfg.RestateURL = flap.URL
			bus := &mockEventBus{}
			reg := &mockPRRegistry{resolveTargetID: lifecycleTargetID}
			srv := NewRouterServer(fixedConfig(cfg), nil)
			srv.bus = bus
			srv.registry = reg
			srv.dispatcher = &mockOutboundDispatcher{}
			evt := DeployEvent{Event: eventDeploySuccess, Repo: "azylman/aerial", PRNumber: tc.prNumber, JobName: "brain", CommitSHA: "sha123", TargetID: lifecycleTargetID, Timestamp: time.Now().UTC()}
			if _, err := srv.publishUnownedDeployEvent(context.Background(), evt, false); (err != nil) != tc.wantFirstErr {
				t.Fatalf("first publishUnownedDeployEvent() error = %v, wantErr %v", err, tc.wantFirstErr)
			}
			if _, err := srv.publishUnownedDeployEvent(context.Background(), evt, false); (err != nil) != tc.wantSecondErr {
				t.Fatalf("second publishUnownedDeployEvent() error = %v, wantErr %v", err, tc.wantSecondErr)
			}
			if got := calls.Load(); got != tc.wantCalls {
				t.Fatalf("journal calls = %d, want %d", got, tc.wantCalls)
			}
			if got := len(bus.publishedPREvents); got != tc.wantPublishes {
				t.Fatalf("published lifecycle events = %d, want %d", got, tc.wantPublishes)
			}
		})
	}
}

func TestAuthoritativeConflictJournalSurvivesRetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		conflictUpdated bool
	}{
		{name: "updated conflict journals on retry after flap", conflictUpdated: true},
		{name: "fallback conflict journals on retry after flap", conflictUpdated: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			flap := flapRestateServer(t, &calls)
			t.Cleanup(flap.Close)
			cfg := lifecycleCfg()
			cfg.RestateAuthoritative = true
			cfg.RestateURL = flap.URL
			bus := &mockEventBus{}
			reg := &mockPRRegistry{conflictTargetID: lifecycleTargetID, conflictUpdated: tc.conflictUpdated, resolveTargetID: lifecycleTargetID}
			srv := newLifecycleServer(cfg, bus, reg, &mockOutboundDispatcher{})
			handled, err := srv.transitionRegisteredConflict(context.Background(), "azylman/aerial", 42, "feat/branch", "sha-conflict")
			if err == nil || handled {
				t.Fatalf("first transitionRegisteredConflict() handled = %v, err = %v, want failure", handled, err)
			}
			handled, err = srv.transitionRegisteredConflict(context.Background(), "azylman/aerial", 42, "feat/branch", "sha-conflict")
			if err != nil || !handled {
				t.Fatalf("second transitionRegisteredConflict() handled = %v, err = %v, want success", handled, err)
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("journal calls = %d, want 2", got)
			}
			assertPREvents(t, bus, []wantPREvent{{Event: "conflict", PRNumber: 42, HeadSHA: "sha-conflict", Status: "conflict"}})
		})
	}
}

func TestAuthoritativeCloseJournalSurvivesRetry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		merged    bool
		event     string
		status    string
		wantMerge string
	}{
		{name: "merged close journals on retry after flap", merged: true, event: "merged", status: "merged", wantMerge: "mergesha789"},
		{name: "unmerged close journals on retry after flap", merged: false, event: "closed_unmerged", status: "closed_unmerged"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			flap := flapRestateServer(t, &calls)
			t.Cleanup(flap.Close)
			cfg := lifecycleCfg()
			cfg.RestateAuthoritative = true
			cfg.RestateURL = flap.URL
			bus := &mockEventBus{}
			reg := &mockPRRegistry{resolveTargetID: lifecycleTargetID}
			srv := newLifecycleServer(cfg, bus, reg, &mockOutboundDispatcher{})
			res := &ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: 42, Branch: "feat/test", HeadSHA: "head123", MergeSHA: "mergesha789"}
			if err := srv.handlePRClosedAction(context.Background(), res, tc.merged); err == nil {
				t.Fatal("first handlePRClosedAction() err = nil, want journal failure")
			}
			if err := srv.handlePRClosedAction(context.Background(), res, tc.merged); err != nil {
				t.Fatalf("second handlePRClosedAction() err = %v, want success", err)
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("journal calls = %d, want 2", got)
			}
			assertPREvents(t, bus, []wantPREvent{{Event: tc.event, PRNumber: 42, HeadSHA: "head123", MergeSHA: tc.wantMerge, Status: tc.status}})
		})
	}
}

func TestAuthoritativePushJournalsBeforeDispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "journal failure emits no dispatch or backfill, retry completes all three"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			flap := flapRestateServer(t, &calls)
			t.Cleanup(flap.Close)
			cfg := Config{RestateAuthoritative: true, RestateURL: flap.URL, DefaultOwner: true}
			bus := &mockEventBus{}
			reg := &mockPRRegistry{}
			disp := &mockOutboundDispatcher{}
			srv := newTestRouter(cfg, nil, reg, disp, bus)
			body := []byte(`{"ref":"refs/heads/main","before":"prev111","after":"squash123","deleted":false,"head_commit":{"id":"squash123","message":"feat: merged feature (#999)"},"repository":{"full_name":"azylman/aerial"}}`)
			if _, err := srv.processGitHubPushEvent(context.Background(), body, &ResolvedGitHubEvent{}); err == nil {
				t.Fatal("first processGitHubPushEvent() err = nil, want journal failure")
			}
			if got := len(disp.GitPushCalls()); got != 0 {
				t.Fatalf("git pushes after failed journal = %d, want 0", got)
			}
			if got := len(reg.backfillCalls); got != 0 {
				t.Fatalf("backfill calls after failed journal = %d, want 0", got)
			}
			if _, err := srv.processGitHubPushEvent(context.Background(), body, &ResolvedGitHubEvent{}); err != nil {
				t.Fatalf("second processGitHubPushEvent() err = %v, want success", err)
			}
			if got := len(disp.GitPushCalls()); got != 1 {
				t.Fatalf("git pushes after retry = %d, want 1", got)
			}
			if got := len(reg.backfillCalls); got != 1 {
				t.Fatalf("backfill calls after retry = %d, want 1", got)
			}
			if got := calls.Load(); got != 3 {
				t.Fatalf("journal calls = %d, want 3", got)
			}
		})
	}
}

func TestAuthoritativeRerunRetriesAfterJournalSuccess(t *testing.T) {
	t.Parallel()
	var journalCalls, rerunCalls atomic.Int64
	journal := cutoverTestServer(t, http.StatusAccepted, &journalCalls)
	t.Cleanup(journal.Close)
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/actions/runs/803/rerun-failed-jobs") {
			if rerunCalls.Add(1) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(gh.Close)
	cfg := Config{RestateAuthoritative: true, RestateURL: journal.URL, GitHubAPIURL: gh.URL, GitHubToken: "test-token", DefaultOwner: true}
	reg := &mockPRRegistry{resolveTargetID: "100000000000000002", resolveBranch: "feat/restate", resolveHeadSHA: "premerge224", resolveMergeSHA: "merge224"}
	srv := newTestRouter(cfg, gh.Client(), reg, &mockOutboundDispatcher{}, &mockEventBus{})
	var p GitHubWorkflowRunPayload
	p.Action = ghStatusCompleted
	p.WorkflowRun.ID = 803
	p.WorkflowRun.RunAttempt = 1
	p.WorkflowRun.HeadBranch = "main"
	p.WorkflowRun.HeadSHA = "merge224"
	res := &ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: 224, HeadSHA: "merge224", Branch: "main", Conclusion: conclusionFailure, CheckName: "Continuous Delivery"}
	handled, err := srv.tryRerunFailedMainWorkflowRun(context.Background(), p, res)
	if err == nil || handled {
		t.Fatalf("first tryRerunFailedMainWorkflowRun() handled = %v, err = %v, want retryable failure", handled, err)
	}
	if got := journalCalls.Load(); got != 1 {
		t.Fatalf("journal calls after first attempt = %d, want 1", got)
	}
	if got := rerunCalls.Load(); got != 1 {
		t.Fatalf("rerun calls after first attempt = %d, want 1", got)
	}
	handled, err = srv.tryRerunFailedMainWorkflowRun(context.Background(), p, res)
	if err != nil || !handled {
		t.Fatalf("second tryRerunFailedMainWorkflowRun() handled = %v, err = %v, want success on redelivery", handled, err)
	}
	if got := journalCalls.Load(); got != 2 {
		t.Fatalf("journal calls after redelivery = %d, want 2", got)
	}
	if got := rerunCalls.Load(); got != 2 {
		t.Fatalf("rerun calls after redelivery = %d, want 2", got)
	}
}

func TestAuthoritativeKustomizationJournalsBeforeRecord(t *testing.T) {
	t.Parallel()
	var journalCalls atomic.Int64
	flap := flapRestateServer(t, &journalCalls)
	t.Cleanup(flap.Close)
	cfg := Config{RestateAuthoritative: true, RestateURL: flap.URL, DefaultOwner: true}
	reg := &mockPRRegistry{resolveTargetID: "100000000000000002"}
	disp := &mockOutboundDispatcher{}
	srv := newTestRouter(cfg, nil, reg, disp, &mockEventBus{})
	refs := []fluxKustomizationRef{{Namespace: "flux-system", Name: "aerial-core"}}
	if err := srv.recordAndReconcileKustomizations(context.Background(), "azylman/aerial", 42, "main", "headsha", "100000000000000002", refs, true, false); err == nil {
		t.Fatal("first recordAndReconcileKustomizations() err = nil, want journal failure")
	}
	if got := len(reg.kustomizationRequestCalls); got != 0 {
		t.Fatalf("recorded requests after failed journal = %d, want 0", got)
	}
	if got := len(disp.ReconcileCalls()); got != 0 {
		t.Fatalf("reconciles after failed journal = %d, want 0", got)
	}
	if err := srv.recordAndReconcileKustomizations(context.Background(), "azylman/aerial", 42, "main", "headsha", "100000000000000002", refs, true, false); err != nil {
		t.Fatalf("second recordAndReconcileKustomizations() err = %v, want success", err)
	}
	if got := len(reg.kustomizationRequestCalls); got != 1 {
		t.Fatalf("recorded requests after retry = %d, want 1", got)
	}
	if got := len(disp.ReconcileCalls()); got != 1 {
		t.Fatalf("reconciles after retry = %d, want 1", got)
	}
	if got := journalCalls.Load(); got != 2 {
		t.Fatalf("journal calls = %d, want 2", got)
	}
}

func TestAuthoritativeDeploySuppressHitStillJournals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		seeded        bool
		wantJournals  int64
		wantPublishes int
	}{
		{name: "fresh dispatch journals and publishes", seeded: false, wantJournals: 1, wantPublishes: 1},
		{name: "deployed projection suppresses republish and still journals", seeded: true, wantJournals: 1, wantPublishes: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			capture := cutoverTestServer(t, 202, &calls)
			t.Cleanup(capture.Close)
			cfg := lifecycleCfg()
			cfg.RestateAuthoritative = true
			cfg.RestateURL = capture.URL
			bus := &mockEventBus{}
			reg := &mockPRRegistry{resolveTargetID: lifecycleTargetID}
			srv := NewRouterServer(fixedConfig(cfg), nil)
			srv.bus = bus
			srv.registry = reg
			srv.dispatcher = &mockOutboundDispatcher{}
			if tc.seeded {
				reg.deployedRes = true
			}
			evt := DeployEvent{Event: eventDeploySuccess, Repo: "azylman/aerial", PRNumber: 42, JobName: "brain", CommitSHA: "sha123", TargetID: lifecycleTargetID, Timestamp: time.Now().UTC()}
			if _, err := srv.publishUnownedDeployEvent(context.Background(), evt, false); err != nil {
				t.Fatalf("publishUnownedDeployEvent() err = %v, want nil", err)
			}
			if got := calls.Load(); got != tc.wantJournals {
				t.Fatalf("journal calls = %d, want %d", got, tc.wantJournals)
			}
			if got := len(bus.publishedPREvents); got != tc.wantPublishes {
				t.Fatalf("published lifecycle events = %d, want %d", got, tc.wantPublishes)
			}
		})
	}
}

func TestAuthoritativeConflictSuppressHitStillJournals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		seeded bool
		want   []wantPREvent
	}{
		{name: "fresh conflict journals and publishes", seeded: false, want: []wantPREvent{{Event: "conflict", PRNumber: 42, HeadSHA: "sha-conflict", Status: "conflict"}}},
		{name: "projection-known conflict journals without republish", seeded: true, want: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			capture := cutoverTestServer(t, 202, &calls)
			t.Cleanup(capture.Close)
			cfg := lifecycleCfg()
			cfg.RestateAuthoritative = true
			cfg.RestateURL = capture.URL
			bus := &mockEventBus{}
			reg := &mockPRRegistry{resolveTargetID: lifecycleTargetID}
			srv := newLifecycleServer(cfg, bus, reg, &mockOutboundDispatcher{})
			if tc.seeded {
				reg.buildLifecycle = PRBuildLifecycle{Status: prConflict, HeadSHA: "sha-conflict"}
			}
			handled, err := srv.transitionRegisteredConflict(context.Background(), "azylman/aerial", 42, "feat/branch", "sha-conflict")
			if err != nil || !handled {
				t.Fatalf("transitionRegisteredConflict() handled = %v, err = %v, want success", handled, err)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("journal calls = %d, want 1", got)
			}
			assertPREvents(t, bus, tc.want)
		})
	}
}

func TestAuthoritativeCollaboratorSuppressHitStillJournals(t *testing.T) {
	t.Parallel()

	rules := []CollaboratorCIAlertRule{
		{
			Repo:      "azylman/aerial",
			ChannelID: "111111111111111111",
			Authors: map[string]string{
				"collab-user": "<@123456>",
			},
		},
	}
	pr := GitHubPullRequest{
		Number: 42,
		Title:  "Conflict PR",
		State:  "open",
		User: struct {
			Login string `json:"login"`
		}{Login: "collab-user"},
		Head: GitHubPRRef{
			Ref: "feat/conflict",
			SHA: "sha-conflict-9",
		},
	}

	// No per-SHA DM mark survives the purge: every delivery journals and
	// dispatches, and the journaled Restate effect key owns dedup.
	tests := []struct {
		name    string
		wantDMs int
	}{
		{name: "collaborator conflict journals and dispatches", wantDMs: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			capture := cutoverTestServer(t, 202, &calls)
			t.Cleanup(capture.Close)
			cfg := lifecycleCfg()
			cfg.RestateAuthoritative = true
			cfg.RestateURL = capture.URL
			cfg.CollaboratorCIAlerts = rules
			disp := &mockOutboundDispatcher{}
			srv := newLifecycleServer(cfg, &mockEventBus{}, nil, disp)
			if err := srv.notifyCollaboratorConflict(context.Background(), "azylman/aerial", pr); err != nil {
				t.Fatalf("notifyCollaboratorConflict() err = %v, want nil", err)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("journal calls = %d, want 1", got)
			}
			if got := len(disp.DirectMessageCalls()); got != tc.wantDMs {
				t.Fatalf("DirectMessageCalls() = %d, want %d", got, tc.wantDMs)
			}
		})
	}
}
