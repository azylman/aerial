package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/alerts"
)

const lifecycleTargetID = "123456789012345678"

// wantPREvent is the declarative projection of a published PR lifecycle event.
type wantPREvent struct {
	Event      string
	PRNumber   int
	HeadSHA    string
	MergeSHA   string
	Status     string
	Title      string
	CheckName  string
	CheckState string
}

// newLifecycleServer builds a router through the production constructor; nil
// mocks stay unset so typed-nil pointers never reach the interface fields.
func newLifecycleServer(cfg Config, bus *mockEventBus, reg *mockPRRegistry, disp *mockOutboundDispatcher) *RouterServer {
	srv := newTestRouter(cfg, nil, nil, nil, bus)
	if reg != nil {
		srv.SetRegistry(reg)
	}
	if disp != nil {
		srv.SetDispatcher(disp)
	}
	return srv
}

func lifecycleCfg() Config { return Config{SystemChannelID: lifecycleTargetID} }

func assertPREvents(t *testing.T, bus *mockEventBus, want []wantPREvent) {
	t.Helper()
	bus.mu.Lock()
	published := append([]pubsub.PRLifecycleEvent(nil), bus.publishedPREvents...)
	bus.mu.Unlock()
	got := make([]wantPREvent, 0, len(published))
	for _, ev := range published {
		if ev.OccurredAt.IsZero() {
			t.Errorf("published %q event has zero OccurredAt", ev.Event)
		}
		got = append(got, wantPREvent{
			Event: ev.Event, PRNumber: ev.PRNumber, HeadSHA: ev.HeadSHA, MergeSHA: ev.MergeSHA,
			Status: ev.Status, Title: ev.Title, CheckName: ev.CheckName, CheckState: ev.CheckState,
		})
	}
	if len(want) == 0 && len(got) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("published PR events = %+v, want %+v", got, want)
	}
}

func TestPublishPRLifecycleEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ev   pubsub.PRLifecycleEvent
		want []wantPREvent
	}{
		{
			name: "opened event published with fields and stamped OccurredAt",
			ev:   pubsub.PRLifecycleEvent{Event: "opened", Repo: "azylman/aerial", PRNumber: 42, Branch: "feat/something", HeadSHA: "abcdef123456", Status: "open"},
			want: []wantPREvent{{Event: "opened", PRNumber: 42, HeadSHA: "abcdef123456", Status: "open"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{}
			srv := newLifecycleServer(Config{}, bus, &mockPRRegistry{}, &mockOutboundDispatcher{})
			if err := srv.publishPRLifecycleEvent(context.Background(), tc.ev); err != nil {
				t.Fatalf("publishPRLifecycleEvent() err = %v", err)
			}
			assertPREvents(t, bus, tc.want)
		})
	}
}

func TestTransitionRegisteredConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		reg     *mockPRRegistry
		prNum   int
		branch  string
		headSHA string
		want    []wantPREvent
	}{
		{
			name:    "registered conflict publishes conflict without dispatching a prompt",
			reg:     &mockPRRegistry{conflictTargetID: lifecycleTargetID, conflictUpdated: true},
			prNum:   42,
			branch:  "feat/branch",
			headSHA: "sha-conflict",
			want:    []wantPREvent{{Event: "conflict", PRNumber: 42, HeadSHA: "sha-conflict", Status: "conflict"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{}
			disp := &mockOutboundDispatcher{}
			srv := newLifecycleServer(lifecycleCfg(), bus, tc.reg, disp)
			handled, err := srv.transitionRegisteredConflict(context.Background(), "azylman/aerial", tc.prNum, tc.branch, tc.headSHA)
			if err != nil || !handled {
				t.Fatalf("transitionRegisteredConflict() handled = %v, err = %v", handled, err)
			}
			assertPREvents(t, bus, tc.want)
			if n := len(disp.DispatchCalls()); n != 0 {
				t.Errorf("dispatches = %d, want 0", n)
			}
		})
	}
}

func TestProcessGitHubPullRequestEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want []wantPREvent
	}{
		{
			name: "opened payload publishes opened event",
			body: `{"action":"opened","number":42,"pull_request":{"number":42,"title":"test pr","head":{"ref":"feat/test","sha":"head123"},"user":{"login":"tester"}},"repository":{"full_name":"azylman/aerial"},"sender":{"login":"tester"}}`,
			want: []wantPREvent{{Event: "opened", PRNumber: 42, HeadSHA: "head123", Status: "open", Title: "test pr"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{}
			srv := newLifecycleServer(lifecycleCfg(), bus, nil, &mockOutboundDispatcher{})
			if _, err := srv.processGitHubPullRequestEvent(context.Background(), "del-open", []byte(tc.body), &ResolvedGitHubEvent{}); err != nil {
				t.Fatalf("processGitHubPullRequestEvent() err = %v", err)
			}
			assertPREvents(t, bus, tc.want)
		})
	}
}

func TestHandlePRClosedAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mergeSHA string
		merged   bool
		want     []wantPREvent
	}{
		{
			name:     "merged publishes merged event with merge sha",
			mergeSHA: "mergesha789",
			merged:   true,
			want:     []wantPREvent{{Event: "merged", PRNumber: 42, HeadSHA: "head123", MergeSHA: "mergesha789", Status: "merged"}},
		},
		{
			name:   "unmerged close publishes closed_unmerged event",
			merged: false,
			want:   []wantPREvent{{Event: "closed_unmerged", PRNumber: 42, HeadSHA: "head123", Status: "closed_unmerged"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{}
			reg := &mockPRRegistry{resolveTargetID: lifecycleTargetID}
			srv := newLifecycleServer(lifecycleCfg(), bus, reg, &mockOutboundDispatcher{})
			res := &ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: 42, Branch: "feat/test", HeadSHA: "head123", MergeSHA: tc.mergeSHA}
			if err := srv.handlePRClosedAction(context.Background(), res, tc.merged); err != nil {
				t.Fatalf("handlePRClosedAction(merged=%v) err = %v", tc.merged, err)
			}
			assertPREvents(t, bus, tc.want)
		})
	}
}

func TestRecordCheckRunStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		checkName  string
		status     string
		conclusion string
		want       []wantPREvent
	}{
		{
			name:       "successful check run publishes passed check_updated",
			checkName:  "Unit Tests",
			status:     "completed",
			conclusion: "success",
			want:       []wantPREvent{{Event: "check_updated", PRNumber: 42, HeadSHA: "head123", Status: "open", CheckName: "Unit Tests", CheckState: "passed"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{}
			reg := &mockPRRegistry{recordCheckUpdated: true}
			srv := newLifecycleServer(lifecycleCfg(), bus, reg, &mockOutboundDispatcher{})
			res := &ResolvedGitHubEvent{
				Repo: "azylman/aerial", PRNumber: 42, Branch: "feat/test", HeadSHA: "head123",
				CheckName: tc.checkName, Status: tc.status, Conclusion: tc.conclusion,
			}
			srv.recordCheckRunStatus(context.Background(), res)
			assertPREvents(t, bus, tc.want)
		})
	}
}

func TestHandleDeployFailedOrRollback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		evt  DeployEvent
		want []wantPREvent
	}{
		{
			name: "registered deploy failure publishes deploy_failed",
			evt: DeployEvent{
				Event: "deploy_failed", Repo: "azylman/aerial", PRNumber: 42, JobName: "eri", CommitSHA: "sha-deploy-fail",
				TargetID: lifecycleTargetID, Details: "crashloopbackoff", Timestamp: time.Now().UTC(),
			},
			want: []wantPREvent{{Event: "deploy_failed", PRNumber: 42, MergeSHA: "sha-deploy-fail", Status: "deploy_failed"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{}
			reg := &mockPRRegistry{deployFailedTargetID: lifecycleTargetID, deployFailedUpdated: true}
			srv := newLifecycleServer(lifecycleCfg(), bus, reg, &mockOutboundDispatcher{})
			if err := srv.handleDeployFailedOrRollback(context.Background(), tc.evt, ""); err != nil {
				t.Fatalf("handleDeployFailedOrRollback() err = %v", err)
			}
			assertPREvents(t, bus, tc.want)
		})
	}
}

func TestHandleDeploySyncSuccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		hasExpected bool
		want        []wantPREvent
	}{
		{
			name:        "sync with expected jobs publishes sync_success",
			hasExpected: true,
			want:        []wantPREvent{{Event: "sync_success", PRNumber: 42, MergeSHA: "sha-sync", Status: "synced"}},
		},
		{
			name:        "sync without expected jobs publishes deploy_success",
			hasExpected: false,
			want:        []wantPREvent{{Event: "deploy_success", PRNumber: 42, MergeSHA: "sha-sync", Status: "deployed"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{}
			reg := &mockPRRegistry{resolveTargetID: lifecycleTargetID}
			srv := newLifecycleServer(lifecycleCfg(), bus, reg, &mockOutboundDispatcher{})
			evt := DeployEvent{Event: "sync_success", Repo: "azylman/aerial", PRNumber: 42, CommitSHA: "sha-sync", TargetID: lifecycleTargetID, Timestamp: time.Now().UTC()}
			_ = srv.handleDeploySyncSuccess(context.Background(), evt, tc.hasExpected)
			assertPREvents(t, bus, tc.want)
		})
	}
}

func TestCompleteKustomizationRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		kustomization string
		transitioned  bool
		want          []wantPREvent
	}{
		{
			name:          "kustomization ready before transition publishes rollout_updated",
			kustomization: "aerial-root",
			transitioned:  false,
			want:          []wantPREvent{{Event: "rollout_updated", PRNumber: 42, MergeSHA: "sha-merge", Status: "deploying"}},
		},
		{
			name:          "last kustomization ready publishes deploy_success",
			kustomization: "aerial-services",
			transitioned:  true,
			want:          []wantPREvent{{Event: "deploy_success", PRNumber: 42, MergeSHA: "sha-merge", Status: "deployed"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{}
			reg := &mockPRRegistry{resolveTargetID: lifecycleTargetID, markKustomizationsRes: tc.transitioned}
			srv := newLifecycleServer(lifecycleCfg(), bus, reg, &mockOutboundDispatcher{})
			pr := DeployingPR{Repo: "azylman/aerial", PRNumber: 42, TargetID: lifecycleTargetID, MergeSHA: "sha-merge"}
			if err := srv.completeKustomizationRequest(context.Background(), pr, tc.kustomization); err != nil {
				t.Fatalf("completeKustomizationRequest() err = %v", err)
			}
			assertPREvents(t, bus, tc.want)
		})
	}
}

func TestCompleteNoDeployWorkflowRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		headSHA        string
		prNum          int
		nilRegistry    bool
		hasExpected    bool
		hasExpectedErr error
		wantErr        bool
		want           []wantPREvent
	}{
		{
			name:    "published deploy_success suppresses the deployment prompt",
			headSHA: "head123",
			prNum:   42,
			want:    []wantPREvent{{Event: "deploy_success", PRNumber: 42, HeadSHA: "head123", Status: "deployed"}},
		},
		{
			name:        "nil registry returns nil without action",
			headSHA:     "head123",
			prNum:       42,
			nilRegistry: true,
		},
		{
			name:    "zero prNum returns nil without action",
			headSHA: "head123",
			prNum:   0,
		},
		{
			name:        "has expected deploy jobs skips deploy_success",
			headSHA:     "head123",
			prNum:       42,
			hasExpected: true,
		},
		{
			name:           "has expected deploy jobs error propagates",
			headSHA:        "head123",
			prNum:          42,
			hasExpectedErr: errors.New("simulated has expected deploy jobs error"),
			wantErr:        true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{}
			disp := &mockOutboundDispatcher{}
			var reg *mockPRRegistry
			if !tc.nilRegistry {
				reg = &mockPRRegistry{
					resolveTargetID: lifecycleTargetID,
					hasExpectedRes:  tc.hasExpected,
					hasExpectedErr:  tc.hasExpectedErr,
				}
			}
			srv := newLifecycleServer(lifecycleCfg(), bus, reg, disp)
			err := srv.completeNoDeployWorkflowRun(context.Background(), "azylman/aerial", tc.headSHA, lifecycleTargetID, tc.prNum)
			if (err != nil) != tc.wantErr {
				t.Fatalf("completeNoDeployWorkflowRun() err = %v, wantErr = %v", err, tc.wantErr)
			}
			assertPREvents(t, bus, tc.want)
			if n := len(disp.PromptCalls()); n != 0 {
				t.Errorf("prompts = %d, want 0", n)
			}
		})
	}
}

func TestBuildPRRolloutState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		meta map[string]any
		want *pubsub.PRRolloutState
	}{
		{
			name: "all rollout fields extracted from metadata",
			meta: map[string]any{
				"jobs":                   []any{"eri", "coven"},
				"started_jobs":           []any{"eri"},
				"deployed_jobs":          []any{"eri"},
				"job_states":             map[string]any{"eri": "verifying_health"},
				"kustomization_statuses": map[string]any{"aerial-root": "Ready"},
			},
			want: &pubsub.PRRolloutState{
				ExpectedJobs:          []string{"eri", "coven"},
				StartedJobs:           []string{"eri"},
				DeployedJobs:          []string{"eri"},
				JobStates:             map[string]string{"eri": "verifying_health"},
				KustomizationStatuses: map[string]string{"aerial-root": "Ready"},
			},
		},
		{name: "nil metadata yields nil", meta: nil, want: nil},
		{name: "empty metadata yields nil", meta: map[string]any{}, want: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := buildPRRolloutState(tc.meta); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("buildPRRolloutState() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestHealDependabotPR(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sysChan      string
		defaultOwner bool
		ensureErr    error
		prNum        int
		branch       string
		wantEnsured  []prCall
	}{
		{
			name:         "non-dependabot branch is not registered",
			sysChan:      lifecycleTargetID,
			defaultOwner: true,
			prNum:        14,
			branch:       "feat/foo",
		},
		{
			name:         "dependabot branch is registered",
			sysChan:      lifecycleTargetID,
			defaultOwner: true,
			prNum:        15,
			branch:       "dependabot/pip/foo-1.0",
			wantEnsured:  []prCall{{repo: "azylman/aerial", prNumber: 15, headSHA: "sha123"}},
		},
		{
			name:         "non-default-owner router does not register",
			sysChan:      lifecycleTargetID,
			defaultOwner: false,
			prNum:        15,
			branch:       "dependabot/pip/foo-1.0",
		},
		{
			name:         "empty target id does not register",
			sysChan:      "",
			defaultOwner: true,
			prNum:        15,
			branch:       "dependabot/pip/foo-1.0",
		},
		{
			name:         "registry error still attempts registration",
			sysChan:      lifecycleTargetID,
			defaultOwner: true,
			ensureErr:    errors.New("db fail"),
			prNum:        15,
			branch:       "dependabot/pip/foo-1.0",
			wantEnsured:  []prCall{{repo: "azylman/aerial", prNumber: 15, headSHA: "sha123"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := &mockPRRegistry{resolveTargetID: tc.sysChan, ensureErr: tc.ensureErr}
			srv := newLifecycleServer(Config{SystemChannelID: tc.sysChan, DefaultOwner: tc.defaultOwner}, &mockEventBus{}, reg, &mockOutboundDispatcher{})
			srv.healDependabotPR(context.Background(), &ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: tc.prNum, Branch: tc.branch, HeadSHA: "sha123"})
			if (len(reg.ensureCalls) > 0 || len(tc.wantEnsured) > 0) && !reflect.DeepEqual(reg.ensureCalls, tc.wantEnsured) {
				t.Errorf("registered PRs = %+v, want %+v", reg.ensureCalls, tc.wantEnsured)
			}
		})
	}
}

func TestIsAutoUpdateAuthorAllowed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		author string
		want   bool
	}{
		{name: "configured app author allowed", author: "app/dependabot", want: true},
		{name: "configured user author allowed", author: "trusted-dev", want: true},
		{name: "unconfigured author rejected", author: "random-user", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newLifecycleServer(Config{AutoUpdatePRAuthors: []string{"app/dependabot", "trusted-dev"}}, &mockEventBus{}, nil, nil)
			if got := srv.isAutoUpdateAuthorAllowed(tc.author); got != tc.want {
				t.Errorf("isAutoUpdateAuthorAllowed(%q) = %v, want %v", tc.author, got, tc.want)
			}
		})
	}
}

func TestSanitizeSubjectToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		token string
		want  string
	}{
		{name: "blank token becomes unknown", token: "  ", want: "unknown"},
		{name: "separators replaced with underscores", token: "foo/bar:baz", want: "foo_bar_baz"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := alerts.SanitizeSubjectToken(tc.token); got != tc.want {
				t.Errorf("sanitizeSubjectToken(%q) = %q, want %q", tc.token, got, tc.want)
			}
		})
	}
}

type prAutoMergeDisabledTestCase struct {
	name                  string
	resRepo               string
	resPRNum              int
	resBranch             string
	resHeadSHA            string
	payloadMergeableState string
	payloadMergeable      *bool
	ghAPIStatus           int
	apiMergeableState     string
	apiHeadSHA            string
	apiState              string
	apiMerged             bool
	registryUpdated       bool
	registryTargetID      string
	wantConflictPublished int
	wantErr               bool
	wantFailedNonRoboRev  string
}

func newAutoMergeDisabledMockServer(tc prAutoMergeDisabledTestCase) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tc.ghAPIStatus != 0 {
			w.WriteHeader(tc.ghAPIStatus)
			return
		}
		if strings.Contains(r.URL.Path, fmt.Sprintf("/pulls/%d", tc.resPRNum)) {
			apiHeadSHA := tc.apiHeadSHA
			if apiHeadSHA == "" {
				apiHeadSHA = tc.resHeadSHA
			}
			pr := GitHubPullRequest{
				Number:         tc.resPRNum,
				State:          tc.apiState,
				Merged:         tc.apiMerged,
				MergeableState: tc.apiMergeableState,
				Head: GitHubPRRef{
					Ref: tc.resBranch,
					SHA: apiHeadSHA,
				},
			}
			_ = json.NewEncoder(w).Encode(pr)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
}

func runPRAutoMergeDisabledCase(ctx context.Context, t *testing.T, tc prAutoMergeDisabledTestCase) {
	t.Helper()
	t.Parallel()
	mockServer := newAutoMergeDisabledMockServer(tc)
	defer mockServer.Close()

	reg := &mockPRRegistry{
		conflictTargetID: tc.registryTargetID,
		conflictUpdated:  tc.registryUpdated,
	}
	bus := &mockEventBus{}
	cfg := Config{
		GitHubAPIURL: mockServer.URL,
	}
	srv := newTestRouter(cfg, mockServer.Client(), reg, nil, bus)

	res := &ResolvedGitHubEvent{
		Event:    "pull_request",
		Action:   "auto_merge_disabled",
		Repo:     tc.resRepo,
		PRNumber: tc.resPRNum,
		HeadSHA:  tc.resHeadSHA,
		Branch:   tc.resBranch,
	}
	pr := GitHubPullRequest{
		Number:         tc.resPRNum,
		MergeableState: tc.payloadMergeableState,
		Mergeable:      tc.payloadMergeable,
	}

	err := srv.handlePRAutoMergeDisabledAction(ctx, res, pr)
	if (err != nil) != tc.wantErr {
		t.Fatalf("handlePRAutoMergeDisabledAction err = %v, wantErr = %v", err, tc.wantErr)
	}
	conflictCount := 0
	for _, evt := range bus.publishedPREvents {
		if evt.Event == "conflict" {
			conflictCount++
		}
	}
	if conflictCount != tc.wantConflictPublished {
		t.Fatalf("published conflict events = %d, want %d", conflictCount, tc.wantConflictPublished)
	}
	if tc.wantFailedNonRoboRev != "" {
		val, ok := srv.GHState().GetNonRoboRevFailedSHA(tc.resRepo, tc.resPRNum)
		if !ok || val != tc.wantFailedNonRoboRev {
			t.Errorf("nonRoboRevFailedSHAs[%s#%d] = %v (ok=%v), want %q", tc.resRepo, tc.resPRNum, val, ok, tc.wantFailedNonRoboRev)
		}
	} else if val, ok := srv.GHState().GetNonRoboRevFailedSHA(tc.resRepo, tc.resPRNum); ok {
		t.Errorf("unexpected nonRoboRevFailedSHAs entry: %v", val)
	}
}

func TestHandlePRAutoMergeDisabledAction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	trueVal := true
	falseVal := false

	tests := []prAutoMergeDisabledTestCase{
		{
			name:                  "unknown state where API returns dirty open PR transitions conflict and enqueues job",
			resRepo:               "azylman/aerial",
			resPRNum:              102,
			resBranch:             "feat/fallback",
			resHeadSHA:            "sha102",
			payloadMergeableState: "unknown",
			apiMergeableState:     "dirty",
			apiHeadSHA:            "sha102",
			apiState:              "open",
			registryUpdated:       true,
			registryTargetID:      "100000000000000001",
			wantConflictPublished: 1,
			wantFailedNonRoboRev:  "sha102",
		},
		{
			name:                  "payload mergeable false with empty state is merge conflict without API lookup",
			resRepo:               "azylman/aerial",
			resPRNum:              103,
			resBranch:             "feat/unmergeable",
			resHeadSHA:            "sha103",
			payloadMergeable:      &falseVal,
			payloadMergeableState: "",
			registryUpdated:       true,
			registryTargetID:      "100000000000000001",
			wantConflictPublished: 1,
			wantFailedNonRoboRev:  "sha103",
		},
		{
			name:                  "payload mergeable true with empty state is clean and ignored",
			resRepo:               "azylman/aerial",
			resPRNum:              104,
			resBranch:             "feat/clean",
			resHeadSHA:            "sha104",
			payloadMergeable:      &trueVal,
			payloadMergeableState: "",
			wantConflictPublished: 0,
		},
		{
			name:                  "unknown state with empty repo normalization skips API lookup and ignores non-conflict",
			resRepo:               "",
			resPRNum:              107,
			resBranch:             "feat/unknown",
			resHeadSHA:            "sha107",
			payloadMergeableState: "unknown",
			ghAPIStatus:           http.StatusInternalServerError,
			wantConflictPublished: 0,
		},
		{
			name:                  "unknown state where API returns merged PR is ignored cleanly",
			resRepo:               "azylman/aerial",
			resPRNum:              109,
			resBranch:             "feat/api-merged",
			resHeadSHA:            "sha109",
			payloadMergeableState: "unknown",
			apiMergeableState:     "dirty",
			apiHeadSHA:            "sha109",
			apiMerged:             true,
			wantConflictPublished: 0,
		},
		{
			name:                  "unknown state where API returns closed PR is ignored cleanly",
			resRepo:               "azylman/aerial",
			resPRNum:              110,
			resBranch:             "feat/api-closed",
			resHeadSHA:            "sha110",
			payloadMergeableState: "unknown",
			apiMergeableState:     "dirty",
			apiHeadSHA:            "sha110",
			apiState:              "closed",
			wantConflictPublished: 0,
		},
		{
			name:                  "unknown state where API returns superseded head SHA is ignored cleanly",
			resRepo:               "azylman/aerial",
			resPRNum:              111,
			resBranch:             "feat/api-superseded",
			resHeadSHA:            "sha111",
			payloadMergeableState: "unknown",
			apiMergeableState:     "dirty",
			apiHeadSHA:            "sha111-newer",
			apiState:              "open",
			wantConflictPublished: 0,
		},
		{
			name:                  "unknown state where API returns HTTP 500 propagates error",
			resRepo:               "azylman/aerial",
			resPRNum:              112,
			resBranch:             "feat/api-err",
			resHeadSHA:            "sha112",
			payloadMergeableState: "unknown",
			ghAPIStatus:           http.StatusInternalServerError,
			wantErr:               true,
			wantConflictPublished: 0,
		},
		{
			name:                  "registry returns updated false skips enqueuing conflict job",
			resRepo:               "azylman/aerial",
			resPRNum:              115,
			resBranch:             "feat/not-updated",
			resHeadSHA:            "sha115",
			payloadMergeableState: "dirty",
			registryUpdated:       false,
			wantConflictPublished: 0,
			wantFailedNonRoboRev:  "sha115",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runPRAutoMergeDisabledCase(ctx, t, tc)
		})
	}
}
