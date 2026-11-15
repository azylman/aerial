package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestFireEventTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		targets          []string
		contextHeader    string
		wantFired        int
		wantHeaderSubstr string
	}{
		{
			name:    "empty targets is no-op",
			targets: nil,
		},
		{
			name:    "whitespace targets is no-op",
			targets: []string{"   ", ""},
		},
		{
			name:             "fires triggers via bus with context header",
			targets:          []string{"azylman/aerial"},
			contextHeader:    "GitHub ci_passed on azylman/aerial",
			wantFired:        1,
			wantHeaderSubstr: "GitHub ci_passed on azylman/aerial",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			recBus := &recordingBus{}
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			srv.SetEventBus(recBus)

			srv.fireEventTriggers(context.Background(), "github", tc.targets, "ci_passed", tc.contextHeader)

			if recBus.publishCount() != tc.wantFired {
				t.Fatalf("fireEventTriggers() fired %d times, want %d", recBus.publishCount(), tc.wantFired)
			}
			if tc.wantHeaderSubstr != "" && recBus.publishCount() > 0 {
				ev, ok := recBus.lastEvent()
				if !ok || !strings.Contains(ev.ContextHeader, tc.wantHeaderSubstr) {
					t.Fatalf("contextHeader = %q, want substr %q", ev.ContextHeader, tc.wantHeaderSubstr)
				}
			}
		})
	}
}

func TestEvaluateFluxEventTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		event          string
		repo           string
		details        string
		wantFired      bool
		wantTarget     string
		wantEventType  string
		wantPromptPart string
	}{
		{
			name:      "unsupported event ignored",
			event:     "deploy_started",
			repo:      "azylman/aerial",
			wantFired: false,
		},
		{
			name:      "empty repo ignored",
			event:     "sync_success",
			repo:      "",
			wantFired: false,
		},
		{
			name:           "sync_success with details fires flux trigger",
			event:          "sync_success",
			repo:           "aerial",
			details:        "synced 5 files",
			wantFired:      true,
			wantTarget:     "azylman/aerial",
			wantEventType:  "sync_success",
			wantPromptPart: "Flux git sync sync_success for azylman/aerial: synced 5 files",
		},
		{
			name:           "sync_failed without details fires flux trigger",
			event:          "sync_failed",
			repo:           "azylman/aerial-config",
			details:        "",
			wantFired:      true,
			wantTarget:     "azylman/aerial-config",
			wantEventType:  "sync_failed",
			wantPromptPart: "Flux git sync sync_failed for azylman/aerial-config",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			recBus := &recordingBus{}
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			srv.SetEventBus(recBus)

			srv.evaluateFluxEventTriggers(context.Background(), DeployEvent{
				Event:   tc.event,
				Repo:    tc.repo,
				Details: tc.details,
			})

			if !tc.wantFired {
				if recBus.publishCount() != 0 {
					t.Fatalf("expected 0 fire calls, got %d", recBus.publishCount())
				}
				return
			}
			if recBus.publishCount() != 1 {
				t.Fatalf("expected 1 fire call, got %d", recBus.publishCount())
			}
			ev, ok := recBus.lastEvent()
			if !ok {
				t.Fatalf("expected recorded event")
			}
			if ev.Source != "flux" || ev.EventType != tc.wantEventType || len(ev.Targets) != 1 || ev.Targets[0] != tc.wantTarget {
				t.Fatalf("unexpected fire call: %+v, want source=flux target=%s event=%s", ev, tc.wantTarget, tc.wantEventType)
			}
			if !strings.Contains(ev.ContextHeader, tc.wantPromptPart) {
				t.Fatalf("contextHeader %q does not contain %q", ev.ContextHeader, tc.wantPromptPart)
			}
		})
	}
}

func TestExtractCheckRunHeadBranch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantBranch string
	}{
		{
			name:       "empty body returns empty",
			body:       "",
			wantBranch: "",
		},
		{
			name:       "invalid json returns empty",
			body:       "{invalid",
			wantBranch: "",
		},
		{
			name:       "valid check_run body extracts branch with refs/heads/ stripped",
			body:       `{"check_run":{"check_suite":{"head_branch":"refs/heads/feat/awesome-feature"}}}`,
			wantBranch: "feat/awesome-feature",
		},
		{
			name:       "valid check_run body extracts bare branch",
			body:       `{"check_run":{"check_suite":{"head_branch":"main"}}}`,
			wantBranch: "main",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractCheckRunHeadBranch([]byte(tc.body))
			if got != tc.wantBranch {
				t.Fatalf("extractCheckRunHeadBranch() = %q, want %q", got, tc.wantBranch)
			}
		})
	}
}

func TestResolveGitHubTriggerEventType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		res       *ResolvedGitHubEvent
		wantEvent string
	}{
		{
			name:      "nil res returns empty",
			res:       nil,
			wantEvent: "",
		},
		{
			name: "pull_request closed with merged true returns pr_merged",
			res: &ResolvedGitHubEvent{
				Event:    "pull_request",
				Action:   "closed",
				Merged:   true,
				MergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
			},
			wantEvent: "pr_merged",
		},
		{
			name: "pull_request closed unmerged with merge_sha returns empty",
			res: &ResolvedGitHubEvent{
				Event:    "pull_request",
				Action:   "closed",
				Merged:   false,
				MergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
			},
			wantEvent: "",
		},
		{
			name: "pull_request closed without merge_sha returns empty",
			res: &ResolvedGitHubEvent{
				Event:    "pull_request",
				Action:   "closed",
				Merged:   false,
				MergeSHA: "",
			},
			wantEvent: "",
		},
		{
			name: "pull_request opened returns empty",
			res: &ResolvedGitHubEvent{
				Event:  "pull_request",
				Action: "opened",
			},
			wantEvent: "",
		},
		{
			name: "check_run completed success returns ci_passed",
			res: &ResolvedGitHubEvent{
				Event:      "check_run",
				Action:     "completed",
				Conclusion: "success",
			},
			wantEvent: "ci_passed",
		},
		{
			name: "check_run completed failure returns ci_failed",
			res: &ResolvedGitHubEvent{
				Event:      "check_run",
				Action:     "completed",
				Conclusion: "failure",
			},
			wantEvent: "ci_failed",
		},
		{
			name: "check_run completed timed_out returns ci_failed",
			res: &ResolvedGitHubEvent{
				Event:      "check_run",
				Action:     "completed",
				Conclusion: "timed_out",
			},
			wantEvent: "ci_failed",
		},
		{
			name: "check_run in_progress returns empty",
			res: &ResolvedGitHubEvent{
				Event:  "check_run",
				Action: "in_progress",
			},
			wantEvent: "",
		},
		{
			name: "workflow_run completed success returns ci_passed",
			res: &ResolvedGitHubEvent{
				Event:      "workflow_run",
				Action:     "completed",
				Conclusion: "success",
			},
			wantEvent: "ci_passed",
		},
		{
			name: "workflow_run completed cancelled returns empty",
			res: &ResolvedGitHubEvent{
				Event:      "workflow_run",
				Action:     "completed",
				Conclusion: "cancelled",
			},
			wantEvent: "",
		},
		{
			name: "ping event returns empty",
			res: &ResolvedGitHubEvent{
				Event: "ping",
			},
			wantEvent: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := resolveGitHubTriggerEventType(tc.res)
			if got != tc.wantEvent {
				t.Fatalf("resolveGitHubTriggerEventType() = %q, want %q", got, tc.wantEvent)
			}
		})
	}
}

func TestEvaluateGitHubEventTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		res           *ResolvedGitHubEvent
		body          string
		wantFired     bool
		wantSource    string
		wantEventType string
		wantTargets   []string
	}{
		{
			name:      "nil res is no-op",
			res:       nil,
			wantFired: false,
		},
		{
			name: "empty repo is no-op",
			res: &ResolvedGitHubEvent{
				Event:    "pull_request",
				Action:   "closed",
				MergeSHA: "123",
				Repo:     "",
			},
			wantFired: false,
		},
		{
			name: "unsupported event type is no-op",
			res: &ResolvedGitHubEvent{
				Event: "ping",
				Repo:  "azylman/aerial",
			},
			wantFired: false,
		},
		{
			name: "pull_request closed with branch builds repo and branch targets",
			res: &ResolvedGitHubEvent{
				Event:    "pull_request",
				Action:   "closed",
				Merged:   true,
				MergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
				Repo:     "azylman/aerial",
				Branch:   "refs/heads/feat/trigger-eval",
			},
			wantFired:     true,
			wantSource:    "github",
			wantEventType: "pr_merged",
			wantTargets:   []string{"azylman/aerial", "azylman/aerial:feat/trigger-eval"},
		},
		{
			name: "pull_request closed unmerged with test merge_sha does not fire",
			res: &ResolvedGitHubEvent{
				Event:    "pull_request",
				Action:   "closed",
				Merged:   false,
				MergeSHA: "0eb75b1f4eed46252d5fe0ac5041d1c1315bf092",
				Repo:     "azylman/aerial",
				Branch:   "refs/heads/feat/trigger-eval",
			},
			wantFired: false,
		},
		{
			name: "check_run extracts branch from body when res.Branch is empty",
			res: &ResolvedGitHubEvent{
				Event:      "check_run",
				Action:     "completed",
				Conclusion: "success",
				Repo:       "aerial",
				Branch:     "",
			},
			body:          `{"check_run":{"check_suite":{"head_branch":"refs/heads/feat/auto-extract"}}}`,
			wantFired:     true,
			wantSource:    "github",
			wantEventType: "ci_passed",
			wantTargets:   []string{"azylman/aerial", "azylman/aerial:feat/auto-extract"},
		},
		{
			name: "workflow_run completed failure with bare repo",
			res: &ResolvedGitHubEvent{
				Event:      "workflow_run",
				Action:     "completed",
				Conclusion: "failure",
				Repo:       "azylman/aerial",
				Branch:     "",
			},
			wantFired:     true,
			wantSource:    "github",
			wantEventType: "ci_failed",
			wantTargets:   []string{"azylman/aerial"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			recBus := &recordingBus{}
			mockReg := &mockPRRegistry{}
			mockDisp := &mockOutboundDispatcher{}
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			srv.SetEventBus(recBus)
			srv.SetRegistry(mockReg)
			srv.SetDispatcher(mockDisp)

			srv.evaluateGitHubEventTriggers(context.Background(), tc.res, []byte(tc.body))

			if !tc.wantFired {
				if recBus.publishCount() != 0 {
					t.Fatalf("expected 0 bus events, got %d", recBus.publishCount())
				}
				return
			}
			if recBus.publishCount() != 1 {
				t.Fatalf("expected 1 bus event, got %d", recBus.publishCount())
			}
			ev, ok := recBus.lastEvent()
			if !ok {
				t.Fatalf("expected recorded bus event")
			}
			if ev.Source != tc.wantSource || ev.EventType != tc.wantEventType {
				t.Fatalf("ev = %+v, want source=%s eventType=%s", ev, tc.wantSource, tc.wantEventType)
			}
			if len(ev.Targets) != len(tc.wantTargets) {
				t.Fatalf("ev.Targets = %v, want %v", ev.Targets, tc.wantTargets)
			}
			for i, tgt := range tc.wantTargets {
				if ev.Targets[i] != tgt {
					t.Fatalf("ev.Targets[%d] = %q, want %q", i, ev.Targets[i], tgt)
				}
			}
		})
	}
}

func TestIntegrations_EventTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		testScenario  string
		wantSource    string
		wantEventType string
	}{
		{
			name:          "ProcessDeployEvent sync_success fires flux triggers",
			testScenario:  "flux_sync_success",
			wantSource:    "flux",
			wantEventType: "sync_success",
		},
		{
			name:          "ProcessGitHubEvent pr closed merged fires github triggers",
			testScenario:  "github_pr_merged",
			wantSource:    "github",
			wantEventType: "pr_merged",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			recBus := &recordingBus{}
			mockReg := &mockPRRegistry{}
			mockDisp := &mockOutboundDispatcher{}
			srv := NewRouterServer(fixedConfig(Config{}), nil)
			srv.SetEventBus(recBus)
			srv.SetRegistry(mockReg)
			srv.SetDispatcher(mockDisp)

			ctx := context.Background()
			switch tc.testScenario {
			case "flux_sync_success":
				_, _ = srv.ProcessDeployEvent(ctx, DeployEvent{
					Event: "sync_success",
					Repo:  "azylman/aerial",
				})
			case "github_pr_merged":
				body := []byte(`{"action":"closed","number":1,"repository":{"full_name":"azylman/aerial"},"pull_request":{"number":1,"merged":true,"merge_commit_sha":"sha-merge-123","head":{"ref":"feat/test","sha":"sha-head-123"}}}`)
				_, _ = srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-merged", body)
			}

			recBus.mu.Lock()
			events := append([]pubsub.ExternalEvent(nil), recBus.events...)
			recBus.mu.Unlock()

			var found bool
			for _, ev := range events {
				if ev.Source == tc.wantSource && ev.EventType == tc.wantEventType {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("expected bus event for source=%s eventType=%s, events = %+v", tc.wantSource, tc.wantEventType, events)
			}
		})
	}
}

func TestProcessGitHubEvent_TriggersEvaluatedOnlyOnSuccess(t *testing.T) {
	t.Parallel()

	recBus := &recordingBus{}
	mockReg := &mockPRRegistry{
		err: errors.New("database connection lost"),
	}
	mockDisp := &mockOutboundDispatcher{}
	srv := NewRouterServer(fixedConfig(Config{}), nil)
	srv.SetEventBus(recBus)
	srv.SetRegistry(mockReg)
	srv.SetDispatcher(mockDisp)

	ctx := context.Background()
	body := []byte(`{"action":"closed","number":1,"repository":{"full_name":"azylman/aerial"},"pull_request":{"number":1,"merged":true,"merge_commit_sha":"sha-merge-123","head":{"ref":"feat/test","sha":"sha-head-123"}}}`)

	res, err := srv.ProcessGitHubEvent(ctx, "pull_request", "del-pr-merged-err", body)
	if err == nil {
		t.Fatalf("expected error from ProcessGitHubEvent, got res=%v", res)
	}

	if recBus.publishCount() != 0 {
		t.Fatalf("expected no bus events on ProcessGitHubEvent error, got %d events: %+v", recBus.publishCount(), recBus.events)
	}
}

func TestHandleEventsWebhook_MissingAndInvalidPayloads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		setBus     bool
		wantStatus int
	}{
		{
			name:       "invalid json",
			body:       "not-json",
			setBus:     true,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing source",
			body:       `{"event_type":"sync_success","target":"azylman/aerial"}`,
			setBus:     true,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing event_type",
			body:       `{"source":"flux","target":"azylman/aerial"}`,
			setBus:     true,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing targets",
			body:       `{"source":"flux","event_type":"sync_success"}`,
			setBus:     true,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "bus uninitialized",
			body:       `{"source":"flux","event_type":"sync_success","target":"azylman/aerial"}`,
			setBus:     false,
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:       "valid with custom context header",
			body:       `{"source":"flux","event_type":"sync_success","target":"azylman/aerial","context_header":"custom header"}`,
			setBus:     true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid with details generated header",
			body:       `{"source":"flux","event_type":"sync_success","target":"azylman/aerial","details":"5 files synced"}`,
			setBus:     true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "forward to peers",
			body:       `{"source":"flux","event_type":"sync_success","target":"azylman/aerial"}`,
			setBus:     true,
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			peerCalls := 0
			peerTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				peerCalls++
				w.WriteHeader(http.StatusOK)
			}))
			defer peerTs.Close()

			cfg := Config{}
			if tc.name == "forward to peers" {
				cfg.WebhookForwardURLs = map[string]string{"peer": peerTs.URL}
			}
			srv := NewRouterServer(fixedConfig(cfg), nil)
			if tc.setBus {
				srv.SetEventBus(&recordingBus{})
			}
			req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			srv.handleEventsWebhook(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("handleEventsWebhook() status = %d, want %d, body = %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.name == "forward to peers" && peerCalls == 0 {
				t.Errorf("expected peer forward call")
			}
		})
	}
}
