package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
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

func (s *RouterServer) handleEventsWebhook(w http.ResponseWriter, r *http.Request) {
	body, ok := readWebhookBody(w, r, 1<<20)
	if !ok {
		return
	}
	if s != nil && s.cfgSrc != nil && len(s.cfgSrc.Current().WebhookForwardURLs) > 0 {
		reqPath := r.URL.RequestURI()
		if reqPath == "" {
			reqPath = r.URL.Path
		}
		s.forwardRawWebhookToPeers(r.Context(), reqPath, r.Header, body)
	}

	var req EventWebhookRequest
	if err := json.Unmarshal(body, &req); err != nil {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{fieldError: errMsgInvalidJSONPayload})
		return
	}
	source := strings.ToLower(strings.TrimSpace(req.Source))
	eventType := strings.ToLower(strings.TrimSpace(req.EventType))
	targets := dedupeTrimmedStrings(append([]string{req.Target}, req.Targets...))
	if source == "" || eventType == "" || len(targets) == 0 {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{fieldError: "missing required fields"})
		return
	}

	contextHeader := strings.TrimSpace(req.ContextHeader)
	if contextHeader == "" {
		contextHeader = fmt.Sprintf("External %s event %s on %s", source, eventType, strings.Join(targets, ", "))
		if details := strings.TrimSpace(req.Details); details != "" {
			contextHeader += ": " + details
		}
	}

	if s == nil || s.bus == nil {
		httpkit.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{fieldError: "event bus not initialized"})
		return
	}

	if err := s.evaluateEventTriggers(r.Context(), source, eventType, targets, contextHeader); err != nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{fieldError: "failed to evaluate triggers"})
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		fieldStatus:  statusAccepted,
		"source":     source,
		"targets":    targets,
		"event_type": eventType,
	})
}

func sanitizeSubjectToken(token string) string {
	token = strings.TrimSpace(token)
	var sb strings.Builder
	for _, r := range token {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('_')
		}
	}
	clean := sb.String()
	if clean == "" {
		return unknownLabel
	}
	return clean
}

func (s *RouterServer) fireEventTriggers(ctx context.Context, source string, targets []string, eventType, contextHeader string) {
	if s == nil || len(targets) == 0 {
		return
	}
	cleanTargets := dedupeTrimmedStrings(targets)
	if len(cleanTargets) == 0 {
		return
	}
	if err := s.evaluateEventTriggers(ctx, source, eventType, cleanTargets, contextHeader); err != nil {
		log.Printf("[synapse] [event-triggers] error evaluating triggers for %s %v %s: %v", source, cleanTargets, eventType, err)
	}
}

// evaluateEventTriggers evaluates incoming event triggers by publishing to AERIAL_EVENTS on JetStream.
func (s *RouterServer) evaluateEventTriggers(ctx context.Context, source, eventType string, targets []string, contextHeader string) error {
	source, eventType = strings.TrimSpace(source), strings.TrimSpace(eventType)
	cleanTargets := dedupeTrimmedStrings(targets)
	if len(cleanTargets) == 0 || source == "" || eventType == "" {
		return nil
	}
	if s == nil || s.bus == nil {
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
	cleanSource := sanitizeSubjectToken(source)
	cleanEventType := sanitizeSubjectToken(eventType)
	subject := fmt.Sprintf("aerial.events.%s.%s", cleanSource, cleanEventType)
	msgID := fmt.Sprintf("event:%s:%s:%s", cleanSource, cleanEventType, ev.EventID)
	return s.bus.Publish(ctx, subject, msgID, ev)
}

func (s *RouterServer) evaluateFluxEventTriggers(ctx context.Context, evt DeployEvent) {
	if evt.Event != eventSyncSuccess && evt.Event != eventSyncFailed {
		return
	}
	repo := normalizeRepo(evt.Repo)
	if repo == "" {
		return
	}
	contextHeader := fmt.Sprintf("Flux git sync %s for %s", evt.Event, repo)
	if strings.TrimSpace(evt.Details) != "" {
		contextHeader = fmt.Sprintf("Flux git sync %s for %s: %s", evt.Event, repo, strings.TrimSpace(evt.Details))
	}
	s.fireEventTriggers(ctx, "flux", []string{repo}, evt.Event, contextHeader)
}

func extractCheckRunHeadBranch(body []byte) string {
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

func resolveGitHubTriggerEventType(res *ResolvedGitHubEvent) string {
	if res == nil {
		return ""
	}
	switch res.Event {
	case ghEventPullRequest:
		// GitHub generates test merge commit SHAs for unmerged PRs; require res.Merged to prevent false pr_merged triggers.
		if res.Action == ghActionClosed && res.Merged {
			return "pr_merged"
		}
	case ghEventCheckRun, ghEventWorkflowRun:
		if res.Action == ghStatusCompleted {
			switch res.Conclusion {
			case conclusionSuccess:
				return "ci_passed"
			case conclusionFailure, conclusionTimedOut:
				return prCIFailed
			}
		}
	}
	return ""
}

func (s *RouterServer) evaluateGitHubEventTriggers(ctx context.Context, res *ResolvedGitHubEvent, body []byte) {
	if res == nil {
		return
	}
	repo := normalizeRepo(res.Repo)
	if repo == "" {
		return
	}

	eventType := resolveGitHubTriggerEventType(res)
	if eventType == "" {
		return
	}

	branch := strings.TrimPrefix(strings.TrimSpace(res.Branch), "refs/heads/")
	if branch == "" && res.Event == ghEventCheckRun {
		branch = extractCheckRunHeadBranch(body)
	}

	targets := []string{repo}
	if branch != "" {
		targets = append(targets, repo+":"+branch)
	}

	contextHeader := fmt.Sprintf("GitHub %s on %s", eventType, repo)
	if branch != "" {
		contextHeader = fmt.Sprintf("GitHub %s on %s:%s", eventType, repo, branch)
	}
	s.fireEventTriggers(ctx, "github", targets, eventType, contextHeader)
}

// VMAlertNotification represents an Alertmanager v2-compatible alert notification payload.
type VMAlertNotification struct {
	Status       string            `json:"status,omitempty"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL,omitempty"`
}

func vmAlertFingerprint(labels map[string]string) string {
	if len(labels) == 0 {
		return "unknown"
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&sb, "%d:%s=%d:%s;", len(k), k, len(labels[k]), labels[k])
	}
	return sb.String()
}

func isVMAlertResolved(a VMAlertNotification, now time.Time) bool {
	status := strings.TrimSpace(a.Status)
	if strings.EqualFold(status, "resolved") {
		return true
	}
	if status == "" && !a.EndsAt.IsZero() && (!a.EndsAt.After(a.StartsAt) || !a.EndsAt.After(now)) {
		return true
	}
	return false
}

func formatVMAlertMessage(a VMAlertNotification) string {
	alertname := "UnnamedAlert"
	if a.Labels != nil {
		if name := strings.TrimSpace(a.Labels["alertname"]); name != "" {
			alertname = name
		}
	}
	severity := "warning"
	if a.Labels != nil {
		if sev := strings.TrimSpace(a.Labels["severity"]); sev != "" {
			severity = sev
		}
	}
	var details string
	if a.Annotations != nil {
		if summ := strings.TrimSpace(a.Annotations["summary"]); summ != "" {
			details = summ
		} else if desc := strings.TrimSpace(a.Annotations["description"]); desc != "" {
			details = desc
		}
	}
	if details == "" {
		return fmt.Sprintf("🚨 **[ALERT: %s]** (%s)", alertname, severity)
	}
	return fmt.Sprintf("🚨 **[ALERT: %s]** (%s)\n%s", alertname, severity, details)
}

func (s *RouterServer) handleAlertsWebhook(w http.ResponseWriter, r *http.Request) {
	var alerts []VMAlertNotification
	if !readWebhookJSON(w, r, 1<<20, &alerts) {
		return
	}

	dispatchedCount := 0
	now := time.Now().UTC()
	for _, alert := range alerts {
		fp := vmAlertFingerprint(alert.Labels)
		if isVMAlertResolved(alert, now) {
			s.firingAlerts.Delete(fp)
			continue
		}
		if status := strings.TrimSpace(alert.Status); status != "" && !strings.EqualFold(status, "firing") {
			continue
		}

		if s.dispatcher == nil || s.cfgSrc == nil {
			httpkit.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{fieldError: "dispatcher or config not available"})
			return
		}
		channelID := NormalizeDiscordTargetID(s.cfgSrc.Current().SystemChannelID)
		if !IsValidDiscordSnowflake(channelID) {
			httpkit.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{fieldError: "system channel not configured or invalid"})
			return
		}

		if _, loaded := s.firingAlerts.LoadOrStore(fp, true); loaded {
			continue
		}
		msg := formatVMAlertMessage(alert)
		if err := s.dispatcher.DispatchDirectMessage(r.Context(), DirectMessageRequest{ChannelID: channelID, Content: msg}); err != nil {
			s.firingAlerts.Delete(fp)
			log.Printf("[synapse] [alerts] error dispatching alert %s: %v", fp, err)
			httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{fieldError: "failed to dispatch alert message"})
			return
		}
		dispatchedCount++
	}

	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		fieldStatus:  statusAccepted,
		"dispatched": dispatchedCount,
	})
}
