package alerts

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

type recordingBus struct {
	pubsub.Bus

	mu        sync.Mutex
	publishes []string
	events    []pubsub.ExternalEvent
	pubErr    error
}

func (b *recordingBus) Publish(ctx context.Context, subject, msgID string, payload any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pubErr != nil {
		return b.pubErr
	}
	b.publishes = append(b.publishes, subject)
	if ev, ok := payload.(pubsub.ExternalEvent); ok {
		b.events = append(b.events, ev)
	}
	return nil
}

func (b *recordingBus) publishCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.publishes)
}

func (b *recordingBus) lastEvent() (pubsub.ExternalEvent, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.events) == 0 {
		return pubsub.ExternalEvent{}, false
	}
	return b.events[len(b.events)-1], true
}

func TestTracker(t *testing.T) {
	t.Parallel()
	var tr Tracker
	fp := "test-fingerprint"

	if !tr.MarkFiring(fp) {
		t.Fatalf("first MarkFiring should return true")
	}
	if tr.MarkFiring(fp) {
		t.Fatalf("second MarkFiring should return false")
	}

	tr.ClearFiring(fp)
	if !tr.MarkFiring(fp) {
		t.Fatalf("MarkFiring after ClearFiring should return true")
	}
}

func TestFormatAndClassifyVMAlert(t *testing.T) {
	t.Parallel()

	t.Run("fingerprint", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name   string
			labels map[string]string
			want   string
		}{
			{name: "nil labels", labels: nil, want: "unknown"},
			{name: "empty labels", labels: map[string]string{}, want: "unknown"},
			{name: "single pair", labels: map[string]string{"alertname": "PostgresDeadlocks"}, want: "9:alertname=17:PostgresDeadlocks;"},
			{name: "multiple pairs sorted", labels: map[string]string{"severity": "critical", "alertname": "PostgresDeadlocks", "job": "postgres-cnpg"}, want: "9:alertname=17:PostgresDeadlocks;3:job=13:postgres-cnpg;8:severity=8:critical;"},
			{name: "collision resistance", labels: map[string]string{"alertname": "A", "instance": "x,job=y", "severity": "critical"}, want: "9:alertname=1:A;8:instance=7:x,job=y;8:severity=8:critical;"},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				if got := VMAlertFingerprint(tc.labels); got != tc.want {
					t.Fatalf("VMAlertFingerprint() = %q, want %q", got, tc.want)
				}
			})
		}
		if VMAlertFingerprint(map[string]string{"alertname": "A", "instance": "x,job=y", "severity": "critical"}) == VMAlertFingerprint(map[string]string{"alertname": "A", "instance": "x", "job": "y", "severity": "critical"}) {
			t.Fatalf("expected distinct fingerprints for distinct label sets")
		}
	})

	t.Run("isResolved", func(t *testing.T) {
		t.Parallel()
		now := time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)
		tests := []struct {
			name  string
			alert VMAlertNotification
			want  bool
		}{
			{name: "status resolved lowercase", alert: VMAlertNotification{Status: "resolved"}, want: true},
			{name: "status RESOLVED uppercase", alert: VMAlertNotification{Status: "RESOLVED"}, want: true},
			{name: "status firing is not resolved", alert: VMAlertNotification{Status: "firing"}, want: false},
			{name: "empty status with zero endsAt is not resolved", alert: VMAlertNotification{StartsAt: now.Add(-10 * time.Minute)}, want: false},
			{name: "empty status with endsAt in future is not resolved", alert: VMAlertNotification{StartsAt: now.Add(-10 * time.Minute), EndsAt: now.Add(10 * time.Minute)}, want: false},
			{name: "empty status with endsAt before startsAt is resolved", alert: VMAlertNotification{StartsAt: now.Add(10 * time.Minute), EndsAt: now}, want: true},
			{name: "empty status with endsAt equal to startsAt is resolved", alert: VMAlertNotification{StartsAt: now.Add(-5 * time.Minute), EndsAt: now.Add(-5 * time.Minute)}, want: true},
			{name: "empty status with endsAt in past is resolved", alert: VMAlertNotification{StartsAt: now.Add(-10 * time.Minute), EndsAt: now.Add(-1 * time.Minute)}, want: true},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				if got := IsVMAlertResolved(tc.alert, now); got != tc.want {
					t.Fatalf("IsVMAlertResolved() = %v, want %v", got, tc.want)
				}
			})
		}
	})

	t.Run("formatMessage", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name  string
			alert VMAlertNotification
			want  string
		}{
			{name: "defaults with no labels or annotations", alert: VMAlertNotification{}, want: "🚨 **[ALERT: UnnamedAlert]** (warning)"},
			{name: "custom alertname and severity with summary", alert: VMAlertNotification{Labels: map[string]string{"alertname": "PostgresDeadlocks", "severity": "critical"}, Annotations: map[string]string{"summary": "deadlocks detected"}}, want: "🚨 **[ALERT: PostgresDeadlocks]** (critical)\ndeadlocks detected"},
			{name: "falls back to description if summary empty", alert: VMAlertNotification{Labels: map[string]string{"alertname": "PostgresDeadlocks", "severity": "critical"}, Annotations: map[string]string{"description": "postgres deadlock description"}}, want: "🚨 **[ALERT: PostgresDeadlocks]** (critical)\npostgres deadlock description"},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				if got := FormatVMAlertMessage(tc.alert); got != tc.want {
					t.Fatalf("FormatVMAlertMessage() = %q, want %q", got, tc.want)
				}
			})
		}
	})
}

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
			FireEventTriggers(context.Background(), recBus, "github", tc.targets, "ci_passed", tc.contextHeader)

			if recBus.publishCount() != tc.wantFired {
				t.Fatalf("FireEventTriggers() fired %d times, want %d", recBus.publishCount(), tc.wantFired)
			}
			if tc.wantHeaderSubstr != "" && recBus.publishCount() > 0 {
				ev, ok := recBus.lastEvent()
				if !ok || !strings.Contains(ev.ContextHeader, tc.wantHeaderSubstr) {
					t.Fatalf("contextHeader = %q, want substr %q", ev.ContextHeader, tc.wantHeaderSubstr)
				}
			}
		})
	}

	// Test bus == nil and publish error branches
	FireEventTriggers(context.Background(), nil, "github", []string{"aerial"}, "ci_passed", "header")
	errBus := &recordingBus{pubErr: errors.New("bus fail")}
	FireEventTriggers(context.Background(), errBus, "github", []string{"aerial"}, "ci_passed", "header")
}

func TestEvaluateEventTriggers(t *testing.T) {
	t.Parallel()
	recBus := &recordingBus{}
	ctx := context.Background()

	// Empty source or eventType is no-op
	if err := EvaluateEventTriggers(ctx, recBus, "", "ci_passed", []string{"repo"}, "header"); err != nil {
		t.Fatal(err)
	}
	if err := EvaluateEventTriggers(ctx, recBus, "src", "", []string{"repo"}, "header"); err != nil {
		t.Fatal(err)
	}
	if err := EvaluateEventTriggers(ctx, nil, "src", "ci_passed", []string{"repo"}, "header"); err == nil {
		t.Fatal("expected error on nil bus")
	}
}

func TestEvaluateFluxEventTriggers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		evt           model.DeployEvent
		wantFired     bool
		wantTarget    string
		wantEventType string
	}{
		{
			name: "unsupported event ignored",
			evt: model.DeployEvent{
				Event: "sync_started",
				Repo:  "azylman/aerial",
			},
			wantFired: false,
		},
		{
			name: "empty repo ignored",
			evt: model.DeployEvent{
				Event: "sync_success",
				Repo:  "",
			},
			wantFired: false,
		},
		{
			name: "sync_success fires with generated prompt",
			evt: model.DeployEvent{
				Event: "sync_success",
				Repo:  "azylman/aerial",
			},
			wantFired:     true,
			wantTarget:    "azylman/aerial",
			wantEventType: "sync_success",
		},
		{
			name: "sync_failed with details includes details in prompt",
			evt: model.DeployEvent{
				Event:   "sync_failed",
				Repo:    "azylman/mirrormere",
				Details: "kustomization failed to apply",
			},
			wantFired:     true,
			wantTarget:    "azylman/mirrormere",
			wantEventType: "sync_failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			recBus := &recordingBus{}
			EvaluateFluxEventTriggers(context.Background(), recBus, tc.evt)

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
			if ev.Source != "flux" || ev.EventType != tc.wantEventType {
				t.Fatalf("ev = %+v, want source=flux eventType=%s", ev, tc.wantEventType)
			}
			if len(ev.Targets) != 1 || ev.Targets[0] != tc.wantTarget {
				t.Fatalf("ev.Targets = %v, want [%s]", ev.Targets, tc.wantTarget)
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
			got := ExtractCheckRunHeadBranch([]byte(tc.body))
			if got != tc.wantBranch {
				t.Fatalf("ExtractCheckRunHeadBranch() = %q, want %q", got, tc.wantBranch)
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
				Event:  "pull_request",
				Action: "closed",
				Merged: true,
			},
			wantEvent: "pr_merged",
		},
		{
			name: "pull_request closed unmerged with merge_sha returns empty",
			res: &ResolvedGitHubEvent{
				Event:  "pull_request",
				Action: "closed",
				Merged: false,
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
			got := ResolveGitHubTriggerEventType(tc.res)
			if got != tc.wantEvent {
				t.Fatalf("ResolveGitHubTriggerEventType() = %q, want %q", got, tc.wantEvent)
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
				Event:  "pull_request",
				Action: "closed",
				Repo:   "",
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
				Event:  "pull_request",
				Action: "closed",
				Merged: true,
				Repo:   "azylman/aerial",
				Branch: "refs/heads/feat/trigger-eval",
			},
			wantFired:     true,
			wantSource:    "github",
			wantEventType: "pr_merged",
			wantTargets:   []string{"azylman/aerial", "azylman/aerial:feat/trigger-eval"},
		},
		{
			name: "pull_request closed unmerged does not fire",
			res: &ResolvedGitHubEvent{
				Event:  "pull_request",
				Action: "closed",
				Merged: false,
				Repo:   "azylman/aerial",
				Branch: "refs/heads/feat/trigger-eval",
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
			EvaluateGitHubEventTriggers(context.Background(), recBus, tc.res, []byte(tc.body))

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
