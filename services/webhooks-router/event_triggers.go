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
)

// ClaimedEventTrigger represents an event trigger claimed atomically from the database.
type ClaimedEventTrigger struct {
	ID        string `json:"id"`
	ChannelID string `json:"channel_id"`
	Source    string `json:"source"`
	Target    string `json:"target"`
	EventType string `json:"event_type"`
	Prompt    string `json:"prompt"`
	Effort    string `json:"effort"`
}

// ClaimEventTriggers claims matching event triggers atomically from event_triggers.
func (r *PostgresPRRegistry) ClaimEventTriggers(ctx context.Context, source string, targets []string, eventType string) ([]ClaimedEventTrigger, error) {
	if r == nil || r.pool == nil || len(targets) == 0 || source == "" || eventType == "" {
		return nil, nil
	}
	cleanTargets := dedupeTrimmedStrings(targets)
	if len(cleanTargets) == 0 {
		return nil, nil
	}

	qCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
DELETE FROM event_triggers
USING UNNEST($2::text[]) AS t(target_val)
WHERE event_triggers.source = $1
  AND event_triggers.target = t.target_val
  AND event_triggers.event_type = $3
RETURNING event_triggers.id, event_triggers.channel_id, event_triggers.source, event_triggers.target, event_triggers.event_type, event_triggers.prompt, COALESCE(NULLIF(event_triggers.effort, ''), 'medium');`

	rows, err := r.pool.Query(qCtx, query, source, cleanTargets, eventType)
	if err != nil {
		return nil, fmt.Errorf("claim event triggers: %w", err)
	}
	defer rows.Close()

	var claimed []ClaimedEventTrigger
	for rows.Next() {
		var trig ClaimedEventTrigger
		if err := rows.Scan(&trig.ID, &trig.ChannelID, &trig.Source, &trig.Target, &trig.EventType, &trig.Prompt, &trig.Effort); err != nil {
			return nil, fmt.Errorf("scan claimed event trigger: %w", err)
		}
		claimed = append(claimed, trig)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed event triggers: %w", err)
	}
	return claimed, nil
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

func formatEventTriggerPrompt(contextHeader, prompt string) string {
	header := strings.TrimSpace(contextHeader)
	body := strings.TrimSpace(prompt)
	if header == "" {
		return body
	}
	if body == "" {
		return header
	}
	return header + "\n\n" + body
}

func (s *RouterServer) fireEventTriggers(ctx context.Context, source string, targets []string, eventType, contextHeader string) {
	if s.registry == nil || s.dispatcher == nil || len(targets) == 0 {
		return
	}
	cleanTargets := dedupeTrimmedStrings(targets)
	if len(cleanTargets) == 0 {
		return
	}
	claimed, err := s.registry.ClaimEventTriggers(ctx, source, cleanTargets, eventType)
	if err != nil {
		log.Printf("[webhooks-router] [event-triggers] error claiming triggers for %s %v %s: %v", source, cleanTargets, eventType, err)
		return
	}
	for _, trig := range claimed {
		formattedPrompt := formatEventTriggerPrompt(contextHeader, trig.Prompt)
		req := PromptRequest{
			ChannelID: trig.ChannelID,
			Prompt:    formattedPrompt,
			Effort:    trig.Effort,
		}
		if err := s.dispatcher.DispatchPrompt(ctx, req); err != nil {
			log.Printf("[webhooks-router] [event-triggers] error dispatching trigger %s (%s) to %s: %v", trig.ID, trig.EventType, trig.ChannelID, err)
		}
	}
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
