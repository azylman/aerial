package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/google/uuid"
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
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json payload"})
		return
	}
	source := strings.ToLower(strings.TrimSpace(req.Source))
	eventType := strings.ToLower(strings.TrimSpace(req.EventType))
	targets := dedupeTrimmedStrings(append([]string{req.Target}, req.Targets...))
	if source == "" || eventType == "" || len(targets) == 0 {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{"error": "missing required fields"})
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
		httpkit.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "event bus not initialized"})
		return
	}

	if err := s.evaluateEventTriggers(r.Context(), source, eventType, targets, contextHeader); err != nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to evaluate triggers"})
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		"status":     "accepted",
		"source":     source,
		"targets":    targets,
		"event_type": eventType,
	})
}

// FailVanadisChunkDetailsByPR marks a Vanadis chunk as failed by PR and returns the build ID and chunk ID.
func (r *PostgresPRRegistry) FailVanadisChunkDetailsByPR(ctx context.Context, repo string, prNumber int) (string, string, bool, error) {
	repo = normalizeRepo(repo)
	if r == nil || repo == "" || prNumber <= 0 {
		return "", "", false, nil
	}
	var res struct {
		BuildID string `json:"build_id"`
		ChunkID string `json:"chunk_id"`
		Failed  bool   `json:"failed"`
	}
	payload := map[string]any{
		"repo":      repo,
		"pr_number": prNumber,
	}
	if err := r.postVanadisJSON(ctx, "/chunks/fail-pr", "fail chunk", "fail vanadis chunk by pr", payload, &res); err != nil {
		return "", "", false, err
	}
	return res.BuildID, res.ChunkID, res.Failed, nil
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
		return "unknown"
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
	if evt.Event != "sync_success" && evt.Event != "sync_failed" {
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

func (s *RouterServer) evaluateVanadisAdvancementTriggers(ctx context.Context, repo string, prNumber int, res *VanadisAdvancementResult) {
	if res == nil || !res.Matched || !res.Advanced {
		return
	}
	chunkID := fmt.Sprintf("%s-chunk-%d", res.BuildID, res.MergedChunkIndex+1)
	for _, ch := range res.Chunks {
		if ch.ChunkIndex == res.MergedChunkIndex && ch.ID != "" {
			chunkID = ch.ID
			break
		}
	}
	if chunkID != "" {
		s.fireEventTriggers(ctx, "vanadis_chunk", []string{chunkID}, "merged",
			fmt.Sprintf("Vanadis chunk %s merged for %s#%d", chunkID, repo, prNumber))
	}
	if res.BuildCompleted && res.BuildID != "" {
		s.fireEventTriggers(ctx, "vanadis_build", []string{res.BuildID}, "completed",
			fmt.Sprintf("Vanadis build %s completed", res.BuildID))
	}
}

func (s *RouterServer) evaluateVanadisFailureTriggers(ctx context.Context, buildID, chunkID, reason string) {
	if chunkID != "" {
		s.fireEventTriggers(ctx, "vanadis_chunk", []string{chunkID}, "failed",
			fmt.Sprintf("Vanadis chunk %s failed: %s", chunkID, reason))
	}
	if buildID != "" {
		s.fireEventTriggers(ctx, "vanadis_build", []string{buildID}, "failed",
			fmt.Sprintf("Vanadis build %s failed: %s", buildID, reason))
	}
}

func (s *RouterServer) failVanadisChunk(ctx context.Context, repo string, prNumber int, reason string) error {
	if s.registry == nil || prNumber <= 0 {
		return nil
	}
	buildID, chunkID, failed, err := s.registry.FailVanadisChunkDetailsByPR(ctx, repo, prNumber)
	if err != nil {
		return err
	}
	if failed {
		s.evaluateVanadisFailureTriggers(ctx, buildID, chunkID, reason)
	}
	return nil
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
	case "pull_request":
		// GitHub generates test merge commit SHAs for unmerged PRs; require res.Merged to prevent false pr_merged triggers.
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
	if branch == "" && res.Event == "check_run" {
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
