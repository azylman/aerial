package alerts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

// EventWebhookRequest represents an inbound generic event trigger webhook payload.
type EventWebhookRequest struct {
	Source        string   `json:"source"`
	Target        string   `json:"target,omitempty"`
	Targets       []string `json:"targets,omitempty"`
	EventType     string   `json:"event_type"`
	ContextHeader string   `json:"context_header,omitempty"`
	Details       string   `json:"details,omitempty"`
}

// ResolvedGitHubEvent contains the subset of GitHub event fields needed for event trigger evaluation.
type ResolvedGitHubEvent struct {
	Event      string `json:"event"`
	Action     string `json:"action,omitempty"`
	Repo       string `json:"repo,omitempty"`
	Branch     string `json:"branch,omitempty"`
	Merged     bool   `json:"merged,omitempty"`
	Conclusion string `json:"conclusion,omitempty"`
}

// FireEventTriggers evaluates incoming event triggers and logs any evaluation error.
func FireEventTriggers(ctx context.Context, bus pubsub.Bus, source string, targets []string, eventType, contextHeader string) {
	if bus == nil || len(targets) == 0 {
		return
	}
	cleanTargets := model.DedupeTrimmedStrings(targets)
	if len(cleanTargets) == 0 {
		return
	}
	if err := EvaluateEventTriggers(ctx, bus, source, eventType, cleanTargets, contextHeader); err != nil {
		slog.ErrorContext(ctx, "[synapse] [event-triggers] error evaluating triggers", "source", source, "targets", cleanTargets, "event_type", eventType, "error", err)
	}
}

// EvaluateEventTriggers evaluates incoming event triggers by publishing an ExternalEvent to AERIAL_EVENTS on JetStream.
func EvaluateEventTriggers(ctx context.Context, bus pubsub.Bus, source, eventType string, targets []string, contextHeader string) error {
	source, eventType = strings.TrimSpace(source), strings.TrimSpace(eventType)
	cleanTargets := model.DedupeTrimmedStrings(targets)
	if len(cleanTargets) == 0 || source == "" || eventType == "" {
		return nil
	}
	if bus == nil {
		return errors.New("event bus not initialized")
	}

	ev := pubsub.ExternalEvent{
		EventID:       uuid.New().String(),
		Source:        source,
		EventType:     eventType,
		Targets:       cleanTargets,
		ContextHeader: contextHeader,
		OccurredAt:    time.Now().UTC(),
	}
	cleanSource := pubsub.SanitizeSubjectToken(source)
	cleanEventType := pubsub.SanitizeSubjectToken(eventType)
	subject := fmt.Sprintf("aerial.events.%s.%s", cleanSource, cleanEventType)
	msgID := fmt.Sprintf("event:%s:%s:%s", cleanSource, cleanEventType, ev.EventID)
	return bus.Publish(ctx, subject, msgID, ev)
}

// EvaluateFluxEventTriggers evaluates incoming Flux deployment events for event triggers.
func EvaluateFluxEventTriggers(ctx context.Context, bus pubsub.Bus, evt model.DeployEvent) {
	if evt.Event != "sync_success" && evt.Event != "sync_failed" {
		return
	}
	repo := model.NormalizeRepo(evt.Repo)
	if repo == "" {
		return
	}
	contextHeader := fmt.Sprintf("Flux git sync %s for %s", evt.Event, repo)
	if strings.TrimSpace(evt.Details) != "" {
		contextHeader = fmt.Sprintf("Flux git sync %s for %s: %s", evt.Event, repo, strings.TrimSpace(evt.Details))
	}
	FireEventTriggers(ctx, bus, "flux", []string{repo}, evt.Event, contextHeader)
}

// ExtractCheckRunHeadBranch parses a check_run JSON payload to extract check_suite.head_branch.
func ExtractCheckRunHeadBranch(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var crPayload struct {
		CheckRun struct {
			CheckSuite struct {
				HeadBranch string `json:"head_branch"`
			} `json:"check_suite"`
		} `json:"check_run"`
	}
	if err := json.Unmarshal(body, &crPayload); err != nil {
		return ""
	}
	return strings.TrimPrefix(strings.TrimSpace(crPayload.CheckRun.CheckSuite.HeadBranch), "refs/heads/")
}

// ResolveGitHubTriggerEventType classifies a resolved GitHub event into an event trigger type.
func ResolveGitHubTriggerEventType(res *ResolvedGitHubEvent) string {
	if res == nil {
		return ""
	}
	switch res.Event {
	case "pull_request":
		if res.Action == "closed" && res.Merged {
			return "pr_merged"
		}
	case "check_run", "workflow_run":
		if res.Action == "completed" {
			switch res.Conclusion {
			case "success":
				return "ci_passed"
			case "failure", "timed_out":
				return "ci_failed"
			}
		}
	}
	return ""
}

// EvaluateGitHubEventTriggers evaluates incoming GitHub events for event triggers.
func EvaluateGitHubEventTriggers(ctx context.Context, bus pubsub.Bus, res *ResolvedGitHubEvent, body []byte) {
	if res == nil {
		return
	}
	repo := model.NormalizeRepo(res.Repo)
	if repo == "" {
		return
	}

	eventType := ResolveGitHubTriggerEventType(res)
	if eventType == "" {
		return
	}

	branch := strings.TrimPrefix(strings.TrimSpace(res.Branch), "refs/heads/")
	if branch == "" && res.Event == "check_run" {
		branch = ExtractCheckRunHeadBranch(body)
	}

	targets := []string{repo}
	if branch != "" {
		targets = append(targets, repo+":"+branch)
	}

	contextHeader := fmt.Sprintf("GitHub %s on %s", eventType, repo)
	if branch != "" {
		contextHeader = fmt.Sprintf("GitHub %s on %s:%s", eventType, repo, branch)
	}
	FireEventTriggers(ctx, bus, "github", targets, eventType, contextHeader)
}
