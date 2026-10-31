package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
)

// ClaimedEventTrigger represents an event trigger claimed and fired atomically via Haro.
type ClaimedEventTrigger struct {
	ID        string `json:"id"`
	ChannelID string `json:"channel_id"`
	Source    string `json:"source"`
	Target    string `json:"target"`
	EventType string `json:"event_type"`
	Prompt    string `json:"prompt"`
	Effort    string `json:"effort"`
}

// EventWebhookRequest represents an inbound generic event trigger webhook payload.
type EventWebhookRequest struct {
	Source        string   `json:"source"`
	Target        string   `json:"target,omitempty"`
	Targets       []string `json:"targets,omitempty"`
	EventType     string   `json:"event_type"`
	ContextHeader string   `json:"context_header,omitempty"`
	Details       string   `json:"details,omitempty"`
}

// WithHaroClient configures the Haro URL and HTTP client for PostgresPRRegistry.
func WithHaroClient(haroURL string, client *http.Client) func(*PostgresPRRegistry) {
	return func(r *PostgresPRRegistry) {
		if r == nil {
			return
		}
		if haroURL != "" {
			r.haroURL = strings.TrimRight(haroURL, "/")
		}
		if client != nil {
			r.httpClient = client
		}
	}
}

func (r *PostgresPRRegistry) getHaroClient() (*http.Client, string) {
	if r == nil {
		return &http.Client{Timeout: 10 * time.Second}, "http://haro.aerial"
	}
	client := r.httpClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	hURL := strings.TrimRight(r.haroURL, "/")
	if hURL == "" {
		hURL = "http://haro.aerial"
	}
	return client, hURL
}

func (r *PostgresPRRegistry) postHaroEventTriggers(ctx context.Context, path, op string, source string, targets []string, eventType, contextHeader string, dst any) (bool, error) {
	source, eventType = strings.TrimSpace(source), strings.TrimSpace(eventType)
	cleanTargets := dedupeTrimmedStrings(targets)
	if r == nil || len(cleanTargets) == 0 || source == "" || eventType == "" {
		return false, nil
	}
	client, hURL := r.getHaroClient()
	payload := map[string]any{
		"source":     source,
		"targets":    cleanTargets,
		"event_type": eventType,
	}
	if strings.TrimSpace(contextHeader) != "" {
		payload["context_header"] = strings.TrimSpace(contextHeader)
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("marshal %s request: %w", op, err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, hURL+path, bytes.NewReader(bodyBytes))
	if err != nil {
		return false, fmt.Errorf("create %s request: %w", op, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(httpReq)
	if err != nil {
		return false, fmt.Errorf("%s: %w", op, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("%s: unexpected status %d: %s", op, resp.StatusCode, readBodyString(io.LimitReader(resp.Body, 1024)))
	}
	if err := json.NewDecoder(resp.Body).Decode(dst); err != nil {
		return false, fmt.Errorf("decode %s response: %w", op, err)
	}
	return true, nil
}

// CheckEventTriggers performs a lightweight read-only check on Haro to see if any active trigger matches.
func (r *PostgresPRRegistry) CheckEventTriggers(ctx context.Context, source string, targets []string, eventType string) (bool, error) {
	var res struct {
		Matched bool `json:"matched"`
	}
	ok, err := r.postHaroEventTriggers(ctx, "/event-triggers/check", "check event triggers", source, targets, eventType, "", &res)
	if !ok || err != nil {
		return false, err
	}
	return res.Matched, nil
}

// FireEventTriggers atomically claims and dispatches matching event triggers via Haro HTTP API.
func (r *PostgresPRRegistry) FireEventTriggers(ctx context.Context, source string, targets []string, eventType, contextHeader string) ([]ClaimedEventTrigger, error) {
	var res struct {
		Triggers []ClaimedEventTrigger `json:"triggers"`
	}
	ok, err := r.postHaroEventTriggers(ctx, "/event-triggers/fire", "fire event triggers", source, targets, eventType, contextHeader, &res)
	if !ok || err != nil {
		return nil, err
	}
	return res.Triggers, nil
}

// FailVanadisChunkDetailsByPR marks a Vanadis chunk as failed by PR and returns the build ID and chunk ID.
func (r *PostgresPRRegistry) FailVanadisChunkDetailsByPR(ctx context.Context, repo string, prNumber int) (string, string, bool, error) {
	repo = normalizeRepo(repo)
	if r == nil || repo == "" || prNumber <= 0 {
		return "", "", false, nil
	}
	client, vURL := r.getVanadisClient()
	bodyBytes, err := json.Marshal(map[string]any{
		"repo":      repo,
		"pr_number": prNumber,
	})
	if err != nil {
		return "", "", false, fmt.Errorf("marshal fail chunk request: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, vURL+"/chunks/fail-pr", bytes.NewReader(bodyBytes))
	if err != nil {
		return "", "", false, fmt.Errorf("create fail chunk request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(httpReq)
	if err != nil {
		return "", "", false, fmt.Errorf("fail vanadis chunk by pr: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if readErr != nil {
			return "", "", false, fmt.Errorf("fail vanadis chunk by pr: unexpected status %d (read error: %w)", resp.StatusCode, readErr)
		}
		return "", "", false, fmt.Errorf("fail vanadis chunk by pr: unexpected status %d: %s", resp.StatusCode, string(body))
	}
	var res struct {
		BuildID string `json:"build_id"`
		ChunkID string `json:"chunk_id"`
		Failed  bool   `json:"failed"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", "", false, fmt.Errorf("decode fail chunk response: %w", err)
	}
	return res.BuildID, res.ChunkID, res.Failed, nil
}

func (s *RouterServer) fireEventTriggers(ctx context.Context, source string, targets []string, eventType, contextHeader string) {
	if s.registry == nil || len(targets) == 0 {
		return
	}
	cleanTargets := dedupeTrimmedStrings(targets)
	if len(cleanTargets) == 0 {
		return
	}
	if _, err := s.registry.FireEventTriggers(ctx, source, cleanTargets, eventType, contextHeader); err != nil {
		log.Printf("[synapse] [event-triggers] error firing triggers for %s %v %s: %v", source, cleanTargets, eventType, err)
	}
}

func (s *RouterServer) handleEventsWebhook(w http.ResponseWriter, r *http.Request) {
	var req EventWebhookRequest
	if !readWebhookJSON(w, r, 1<<20, false, &req) {
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

	if s.registry != nil {
		matched, checkErr := s.registry.CheckEventTriggers(r.Context(), source, targets, eventType)
		if checkErr == nil && !matched {
			httpkit.WriteJSON(w, http.StatusOK, map[string]any{
				"status":     "dropped",
				"matched":    false,
				"source":     source,
				"event_type": eventType,
			})
			return
		}
		if checkErr != nil {
			log.Printf("[synapse] [events] warning: haro pre-check failed for %s %v %s (failing open to river): %v", source, targets, eventType, checkErr)
		}
	}

	jobArgs := EventTriggerWebhookArgs{
		Source:        source,
		Targets:       targets,
		EventType:     eventType,
		ContextHeader: contextHeader,
	}
	if s.riverClient != nil {
		if err := s.insertRiverJob(r.Context(), jobArgs); err != nil {
			log.Printf("[synapse] [events] ERROR: failed to insert event job into river: %v", err)
			httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to persist webhook"})
			return
		}
		httpkit.WriteJSON(w, http.StatusAccepted, map[string]any{
			"status":     "accepted",
			"source":     source,
			"targets":    targets,
			"event_type": eventType,
		})
		return
	}

	if err := s.ProcessEventTriggerWebhook(r.Context(), jobArgs); err != nil {
		httpkit.WriteJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		"status":     "accepted",
		"source":     source,
		"targets":    targets,
		"event_type": eventType,
	})
}

// ProcessEventTriggerWebhook fires matching event triggers via Haro, returning any error so River retries.
func (s *RouterServer) ProcessEventTriggerWebhook(ctx context.Context, args EventTriggerWebhookArgs) error {
	if s == nil || s.registry == nil {
		return nil
	}
	cleanTargets := dedupeTrimmedStrings(args.Targets)
	if len(cleanTargets) == 0 || strings.TrimSpace(args.Source) == "" || strings.TrimSpace(args.EventType) == "" {
		return nil
	}
	_, err := s.registry.FireEventTriggers(ctx, args.Source, cleanTargets, args.EventType, args.ContextHeader)
	return err
}

func (s *RouterServer) evaluateHangarEventTriggers(ctx context.Context, evt HangarDeployEvent) {
	if evt.Event != "sync_success" && evt.Event != "sync_failed" {
		return
	}
	repo := normalizeRepo(evt.Repo)
	if repo == "" {
		return
	}
	contextHeader := fmt.Sprintf("Hangar git sync %s for %s", evt.Event, repo)
	if strings.TrimSpace(evt.Details) != "" {
		contextHeader = fmt.Sprintf("Hangar git sync %s for %s: %s", evt.Event, repo, strings.TrimSpace(evt.Details))
	}
	s.fireEventTriggers(ctx, "hangar", []string{repo}, evt.Event, contextHeader)
}

func (s *RouterServer) evaluateNomadEventTriggers(ctx context.Context, jobID, eventType, refID, details string) {
	normJob := strings.ToLower(strings.TrimSpace(jobID))
	if normJob == "" {
		return
	}
	contextHeader := fmt.Sprintf("Nomad deployment %s for job %s (ref: %s)", eventType, normJob, refID)
	if strings.TrimSpace(details) != "" {
		contextHeader = fmt.Sprintf("Nomad deployment %s for job %s (ref: %s): %s", eventType, normJob, refID, strings.TrimSpace(details))
	}
	s.fireEventTriggers(ctx, "nomad", []string{normJob}, eventType, contextHeader)
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
