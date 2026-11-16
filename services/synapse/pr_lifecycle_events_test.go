package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
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
			if err := srv.transitionRegisteredConflict(context.Background(), "azylman/aerial", tc.prNum, tc.branch, tc.headSHA); err != nil {
				t.Fatalf("transitionRegisteredConflict() err = %v", err)
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
			srv.handleDeploySyncSuccess(context.Background(), evt, tc.hasExpected)
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
			want:          []wantPREvent{{Event: "rollout_updated", PRNumber: 42, Status: "deploying"}},
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
		name    string
		headSHA string
		prNum   int
		want    []wantPREvent
	}{
		{
			name:    "published deploy_success suppresses the deployment prompt",
			headSHA: "head123",
			prNum:   42,
			want:    []wantPREvent{{Event: "deploy_success", PRNumber: 42, HeadSHA: "head123", Status: "deployed"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bus := &mockEventBus{}
			disp := &mockOutboundDispatcher{}
			reg := &mockPRRegistry{resolveTargetID: lifecycleTargetID}
			srv := newLifecycleServer(lifecycleCfg(), bus, reg, disp)
			if err := srv.completeNoDeployWorkflowRun(context.Background(), "azylman/aerial", tc.headSHA, lifecycleTargetID, tc.prNum); err != nil {
				t.Fatalf("completeNoDeployWorkflowRun() err = %v", err)
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
			if got := sanitizeSubjectToken(tc.token); got != tc.want {
				t.Errorf("sanitizeSubjectToken(%q) = %q, want %q", tc.token, got, tc.want)
			}
		})
	}
}
