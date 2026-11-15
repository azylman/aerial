package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/jackc/pgx/v5"
)

func isDependabotBranch(branch string) bool {
	clean := strings.ToLower(strings.TrimSpace(branch))
	clean = strings.TrimPrefix(clean, "refs/heads/")
	return strings.HasPrefix(clean, "dependabot/")
}

// RecordPRCheckStatus updates or inserts a check into the chunk's ci_checks jsonb map.
func (s *PostgresVanadisStore) RecordPRCheckStatus(ctx context.Context, chunkID, checkName, checkState string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	cID := strings.TrimSpace(chunkID)
	cName := strings.TrimSpace(checkName)
	cState := strings.TrimSpace(checkState)
	if cID == "" || cName == "" {
		return errors.New("chunk_id and check_name cannot be empty")
	}
	res, execErr := s.db.Exec(ctx, `UPDATE vanadis_chunks SET ci_checks = jsonb_set(COALESCE(ci_checks, '{}'::jsonb), ARRAY[$1], to_jsonb($2::text), true), updated_at = CURRENT_TIMESTAMP WHERE id = $3;`, cName, cState, cID)
	switch {
	case execErr != nil:
		return fmt.Errorf("record pr check status: %w", execErr)
	case res.RowsAffected() == 0:
		return ErrChunkNotFound
	default:
		return nil
	}
}

// RecordPRRolloutState persists the rollout_state jsonb and updates status (if non-empty) on a chunk.
func (s *PostgresVanadisStore) RecordPRRolloutState(ctx context.Context, chunkID, status string, rolloutState *pubsub.PRRolloutState) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return errors.New("chunk_id cannot be empty")
	}
	rsJSON := marshalJSONBOrEmpty(rolloutState)
	const q = `UPDATE vanadis_chunks SET rollout_state = $1::jsonb, status = CASE WHEN $2 != '' THEN $2 ELSE status END, updated_at = CURRENT_TIMESTAMP WHERE id = $3;`
	tag, err := s.db.Exec(ctx, q, rsJSON, strings.TrimSpace(status), chunkID)
	if err != nil {
		return fmt.Errorf("record pr rollout state: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrChunkNotFound
	}
	return nil
}

// ResetChunkForSynchronize resets ci_fail_count, clears ci_checks and banner, and recovers failed chunk/build.
// When reviving a chunk from 'failed', consumed awakeable IDs are cleared so subsequent PR events fall back to legacy advancement.
func (s *PostgresVanadisStore) ResetChunkForSynchronize(ctx context.Context, chunkID, buildID string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	buildID = strings.TrimSpace(buildID)
	if chunkID == "" || buildID == "" {
		return errors.New("chunk_id and build_id cannot be empty")
	}
	const chunkQ = `UPDATE vanadis_chunks
SET ci_fail_count = 0,
    ci_checks = '{}'::jsonb,
    banner = '',
    merge_awakeable_id = CASE WHEN status = 'failed' THEN '' ELSE merge_awakeable_id END,
    deploy_awakeable_id = CASE WHEN status = 'failed' THEN '' ELSE deploy_awakeable_id END,
    status = CASE WHEN status = 'failed' THEN 'pr_opened' ELSE status END,
    completed_at = CASE WHEN status = 'failed' THEN NULL ELSE completed_at END,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1;`
	tag, err := s.db.Exec(ctx, chunkQ, chunkID)
	if err != nil {
		return fmt.Errorf("reset chunk for synchronize: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrChunkNotFound
	}
	const buildQ = `UPDATE vanadis_builds SET status = CASE WHEN status = 'failed' THEN 'running' ELSE status END, updated_at = CURRENT_TIMESTAMP WHERE id = $1;`
	if _, err := s.db.Exec(ctx, buildQ, buildID); err != nil {
		return fmt.Errorf("reset build for synchronize: %w", err)
	}
	return nil
}

// IncrementChunkCIFailCount increments ci_fail_count, marks chunk as ci_failed, and sets banner.
// If the chunk is already terminal ('failed', 'completed', 'merged'), the counter is not incremented and status is preserved.
func (s *PostgresVanadisStore) IncrementChunkCIFailCount(ctx context.Context, chunkID, banner string) (int, error) {
	if s == nil || s.db == nil {
		return 0, errors.New("database connection is nil")
	}
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return 0, errors.New("chunk_id cannot be empty")
	}
	const q = `UPDATE vanadis_chunks
SET ci_fail_count = CASE WHEN status IN ('failed', 'completed', 'merged') THEN ci_fail_count ELSE ci_fail_count + 1 END,
    status = CASE WHEN status IN ('failed', 'completed', 'merged') THEN status ELSE 'ci_failed' END,
    banner = CASE WHEN status IN ('failed', 'completed', 'merged') THEN banner ELSE $1 END,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $2
RETURNING ci_fail_count;`
	var count int
	err := s.db.QueryRow(ctx, q, strings.TrimSpace(banner), chunkID).Scan(&count)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, ErrChunkNotFound
		}
		return 0, fmt.Errorf("increment chunk ci fail count: %w", err)
	}
	return count, nil
}

// UpdateChunkBannerAndStatus updates status and banner on a chunk.
func (s *PostgresVanadisStore) UpdateChunkBannerAndStatus(ctx context.Context, chunkID, status, banner string) error {
	if s == nil || s.db == nil {
		return errors.New("database connection is nil")
	}
	id := strings.TrimSpace(chunkID)
	if id == "" {
		return errors.New("chunk_id cannot be empty")
	}
	tag, err := s.db.Exec(ctx, `UPDATE vanadis_chunks SET status = CASE WHEN $1 != '' THEN $1 ELSE status END, banner = $2, updated_at = CURRENT_TIMESTAMP WHERE id = $3;`, strings.TrimSpace(status), strings.TrimSpace(banner), id)
	switch {
	case err != nil:
		return fmt.Errorf("update chunk banner and status: %w", err)
	case tag.RowsAffected() == 0:
		return ErrChunkNotFound
	default:
		return nil
	}
}

func (s *VanadisServer) publishExternalEvent(ctx context.Context, source, eventType string, targets []string, contextHeader string) {
	if s == nil || s.bus == nil {
		return
	}
	evID := fmt.Sprintf("%s-%s-%s", source, eventType, strings.Join(targets, "-"))
	payload := pubsub.ExternalEvent{
		EventID:       evID,
		Source:        source,
		EventType:     eventType,
		Targets:       targets,
		ContextHeader: contextHeader,
		OccurredAt:    time.Now().UTC(),
	}
	subj := fmt.Sprintf("aerial.events.%s.%s", source, eventType)
	if err := s.bus.Publish(ctx, subj, evID, payload); err != nil {
		log.Printf("[vanadis] warning: failed to publish %s event: %v", subj, err)
	}
}

func (s *VanadisServer) dispatchDirectMessage(ctx context.Context, channelID, content string) error {
	if s == nil || s.httpClient == nil {
		return nil
	}
	channelID = strings.TrimSpace(channelID)
	content = strings.TrimSpace(content)
	if channelID == "" || content == "" {
		return nil
	}
	payload := map[string]string{
		"channel_id": channelID,
		"content":    content,
	}
	_, err := s.postBrainJSON(ctx, "/discord/message", payload, 4<<10)
	return err
}

func (s *VanadisServer) formatCIFailurePrompt(ev pubsub.PRLifecycleEvent, details string) string {
	var extraParts []string
	cleanBranch := strings.TrimPrefix(strings.TrimSpace(ev.Branch), "refs/heads/")
	if cleanBranch != "" {
		extraParts = append(extraParts, "Branch: "+cleanBranch)
	}
	if cleanURL := strings.TrimSpace(ev.CheckURL); cleanURL != "" {
		extraParts = append(extraParts, "Check URL: "+cleanURL)
	}
	extraLine := strings.Join(extraParts, "\n")

	shortRepo := firstNonEmpty(cleanRepoName(ev.Repo), "aerial")
	branchArg := firstNonEmpty(cleanBranch, "<branch>")
	checkoutAndSubmit := fmt.Sprintf("Check out the existing PR branch via '/share/aerial/scripts/aerial-pr.sh init %s --branch %s' and push updates via '/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> \"<commit_msg>\"' (PR_DESCRIPTION.md is not required when updating an existing PR; never run bare 'git push').", shortRepo, branchArg)

	directive := "Please investigate and fix the failure by inspecting the failing logs, diagnosing the issue, verifying with local checks, and pushing a fix. " + checkoutAndSubmit
	if isRoboRevCheck(ev.CheckName) {
		directive = fmt.Sprintf("Please critically evaluate each RoboRev finding against codebase reality and architectural intent (RoboRev has read-only repo tools but lacks thread/build/cross-repo context: treat purely mechanical and objective findings like duplicate code, dead code, invariant violations, or concrete bugs/security issues as high-trust, while verifying subjective design suggestions or unverified caller assumptions before acting). Fix verified issues, add concise inline comments for non-obvious invariants (and consider adding general architectural principles to .roborev.toml only when a gap reflects a broad repo-wide pattern rather than a one-off fix), or—if the remaining findings are false positives or conflict with architectural invariants—run '/share/aerial/scripts/aerial-pr.sh --repo %s override-roborev %d '<technical_rebuttal>' to record your rebuttal on the PR and pass the gate. %s", shortRepo, ev.PRNumber, checkoutAndSubmit)
	}
	return formatErrorDetailsBlock(
		fmt.Sprintf("CI check %q failed for PR #%d on %s (head commit: %s).", ev.CheckName, ev.PRNumber, ev.Repo, ev.HeadSHA),
		details, extraLine,
		directive,
	)
}

func (s *VanadisServer) formatConflictPrompt(ev pubsub.PRLifecycleEvent) string {
	shortRepo := firstNonEmpty(cleanRepoName(ev.Repo), "aerial")
	cleanBranch := firstNonEmpty(strings.TrimPrefix(strings.TrimSpace(ev.Branch), "refs/heads/"), "<branch>")
	return fmt.Sprintf("Merge conflict detected on PR #%d on %s (branch: %s, head: %s). Please check out the branch via '/share/aerial/scripts/aerial-pr.sh init %s --branch %s', merge origin/main to resolve conflicts, verify locally, and push to update the PR via '/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> \"<commit_msg>\"' (PR_DESCRIPTION.md is not required when updating an existing PR; never run bare 'git push').", ev.PRNumber, ev.Repo, cleanBranch, ev.HeadSHA, shortRepo, cleanBranch)
}

func (s *VanadisServer) formatDeployFailedPrompt(ev pubsub.PRLifecycleEvent) string {
	jobName := firstNonEmpty(ev.JobName, "service")
	header := fmt.Sprintf("Deployment failed for job %s on %s (PR #%d, commit: %s).", jobName, ev.Repo, ev.PRNumber, ev.HeadSHA)
	return formatErrorDetailsBlock(header, ev.Details, "", "Please investigate and fix the deployment failure.")
}

func (s *VanadisServer) formatSyncFailedPrompt(ev pubsub.PRLifecycleEvent) string {
	header := fmt.Sprintf("Git sync failed for repo %s (commit: %s, PR #%d).", ev.Repo, ev.HeadSHA, ev.PRNumber)
	return formatErrorDetailsBlock(header, ev.Details, "", "Please investigate and fix the git sync failure.")
}

// HandlePRLifecycleEvent processes an incoming PRLifecycleEvent on aerial.pr.>.
func (s *VanadisServer) HandlePRLifecycleEvent(ctx context.Context, msg pubsub.Message) error {
	if s == nil || s.store == nil {
		return errors.New("vanadis store not configured")
	}
	var ev pubsub.PRLifecycleEvent
	if err := json.Unmarshal(msg.Data(), &ev); err != nil {
		log.Printf("[vanadis] warning: failed to unmarshal pr lifecycle event: %v", err)
		return nil
	}
	ev.Event = strings.ToLower(strings.TrimSpace(ev.Event))
	ev.Repo = normalizeRepo(ev.Repo)
	if ev.Repo == "" || ev.PRNumber <= 0 {
		return nil
	}
	return s.ProcessPRLifecycleEvent(ctx, ev)
}

// ProcessPRLifecycleEvent handles business logic for all PRLifecycleEvent transitions.
func (s *VanadisServer) ProcessPRLifecycleEvent(ctx context.Context, ev pubsub.PRLifecycleEvent) error {
	lookup, err := s.store.GetChunkByPR(ctx, ev.Repo, ev.PRNumber)
	if err != nil {
		log.Printf("[vanadis] warning getting chunk by pr %s#%d: %v", ev.Repo, ev.PRNumber, err)
		return err
	}

	if ev.Event == "opened" {
		return s.handlePROpened(ctx, ev, lookup)
	}

	if lookup == nil || !lookup.Matched {
		return nil
	}

	chunk, build, err := s.store.GetChunk(ctx, lookup.ChunkID)
	if err != nil {
		log.Printf("[vanadis] warning getting chunk %s for pr %s#%d: %v", lookup.ChunkID, ev.Repo, ev.PRNumber, err)
		return err
	}

	switch ev.Event {
	case "synchronize":
		return s.handlePRSynchronize(ctx, chunk, build)
	case "check_updated":
		return s.handlePRCheckUpdated(ctx, ev, chunk, build)
	case "deploy_started", "rollout_updated":
		return s.handlePRRolloutUpdated(ctx, ev, chunk, build)
	case "ci_failed":
		return s.handlePRCIFailed(ctx, ev, chunk, build)
	case "conflict":
		return s.handlePRConflict(ctx, ev, chunk, build)
	case "deploy_failed":
		return s.handlePRDeployFailed(ctx, ev, chunk, build)
	case "sync_failed":
		return s.handlePRSyncFailed(ctx, ev, chunk, build)
	case "merged":
		return s.handlePRMerged(ctx, ev, chunk, build)
	case "deploy_success", "sync_success":
		return s.handlePRDeploySuccess(ctx, chunk, build)
	case "closed_unmerged":
		return s.handlePRClosedUnmerged(ctx, chunk)
	default:
		return nil
	}
}

func (s *VanadisServer) handlePROpened(ctx context.Context, ev pubsub.PRLifecycleEvent, lookup *VanadisChunkPRLookup) error {
	if lookup != nil && lookup.Matched {
		s.syncBuildWidget(ctx, lookup.BuildID, "")
		return nil
	}
	cleanTargetID := normalizeDiscordThreadID(ev.TargetID)
	if !isNumericSnowflake(cleanTargetID) {
		return nil
	}
	buildID := generateBuildID()
	title := firstNonEmpty(strings.TrimSpace(ev.Title), fmt.Sprintf("PR #%d: %s", ev.PRNumber, cleanRepoName(ev.Repo)))
	objective := title
	quiet := ev.Quiet || isDependabotBranch(ev.Branch)
	now := time.Now().UTC()

	build := VanadisBuild{
		ID:        buildID,
		ThreadID:  cleanTargetID,
		Tier:      0,
		Title:     title,
		Objective: objective,
		Status:    "running",
		Quiet:     quiet,
		CreatedAt: now,
		UpdatedAt: now,
	}
	chunkID := buildID + "-chunk-1"
	chunk := VanadisChunk{
		ID:           chunkID,
		BuildID:      buildID,
		ChunkIndex:   0,
		Mode:         "pr",
		Repo:         normalizeRepo(ev.Repo),
		Title:        title,
		Instructions: objective,
		Status:       "pr_opened",
		PRNumber:     ev.PRNumber,
		Effort:       "medium",
		DependsOn:    []string{},
		StartedAt:    now,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := s.store.CreateBuild(ctx, build, []VanadisChunk{chunk}); err != nil {
		log.Printf("[vanadis] warning auto-creating 1-chunk build for pr %s#%d: %v", ev.Repo, ev.PRNumber, err)
		return err
	}
	RecordBuildTotal("created")
	s.syncBuildWidget(ctx, buildID, "")
	return nil
}

func (s *VanadisServer) handlePRSynchronize(ctx context.Context, chunk *VanadisChunk, build *VanadisBuild) error {
	if err := s.store.ResetChunkForSynchronize(ctx, chunk.ID, build.ID); err != nil {
		log.Printf("[vanadis] warning resetting chunk %s on synchronize: %v", chunk.ID, err)
		return err
	}
	s.syncBuildWidget(ctx, build.ID, "")
	return nil
}

func (s *VanadisServer) handlePRCheckUpdated(ctx context.Context, ev pubsub.PRLifecycleEvent, chunk *VanadisChunk, build *VanadisBuild) error {
	if ev.CheckName != "" && ev.CheckState != "" {
		if err := s.store.RecordPRCheckStatus(ctx, chunk.ID, ev.CheckName, ev.CheckState); err != nil {
			log.Printf("[vanadis] warning recording pr check status for %s: %v", chunk.ID, err)
		}
	}
	s.syncBuildWidget(ctx, build.ID, "")
	return nil
}

func (s *VanadisServer) handlePRRolloutUpdated(ctx context.Context, ev pubsub.PRLifecycleEvent, chunk *VanadisChunk, build *VanadisBuild) error {
	status := ""
	if ev.Event == "deploy_started" {
		status = "deploying"
	}
	if ev.RolloutState != nil {
		if err := s.store.RecordPRRolloutState(ctx, chunk.ID, status, ev.RolloutState); err != nil {
			log.Printf("[vanadis] warning recording rollout state for %s: %v", chunk.ID, err)
		}
	}
	s.syncBuildWidget(ctx, build.ID, "")
	return nil
}

func (s *VanadisServer) handlePRCIFailed(ctx context.Context, ev pubsub.PRLifecycleEvent, chunk *VanadisChunk, build *VanadisBuild) error {
	if chunk.Status == "failed" || chunk.Status == "completed" || chunk.Status == "merged" {
		return nil
	}
	if build.Status == "failed" || build.Status == "completed" || build.Status == "cancelled" {
		return nil
	}
	if isGateAggregatorCheck(ev.CheckName) {
		return nil
	}

	banner := fmt.Sprintf("CI check %q failed", ev.CheckName)
	failCount, err := s.store.IncrementChunkCIFailCount(ctx, chunk.ID, banner)
	if err != nil {
		log.Printf("[vanadis] warning incrementing ci fail count for %s: %v", chunk.ID, err)
		return err
	}
	s.syncBuildWidget(ctx, build.ID, banner)

	if failCount <= maxCIRemediationAttempts {
		details := s.resolveCIFailureDetails(ctx, ev.Repo, ev.PRNumber, ev.CheckName, ev.HeadSHA, ev.CheckURL, ev.Details)
		prompt := s.formatCIFailurePrompt(ev, details)
		if err := s.dispatchBrainDispatch(ctx, BrainDispatchRequest{
			ID:             ciRemediationDispatchID(chunk.ID, ev.HeadSHA, failCount),
			Source:         "vanadis",
			SessionID:      "vanadis-" + chunk.ID,
			TargetID:       build.ThreadID,
			VanadisChunkID: chunk.ID,
			Prompt:         prompt,
			Effort:         capChunkEffort(chunk.Effort),
			SkipIfActive:   true,
		}); err != nil {
			log.Printf("[vanadis] warning dispatching ci remediation for %s: %v", chunk.ID, err)
		}
		return nil
	}

	if failCount == maxCIRemediationAttempts+1 {
		if _, _, _, err := s.store.FailChunk(ctx, chunk.ID, "ci_remediation_cap_reached"); err != nil {
			log.Printf("[vanadis] warning failing chunk %s on cap reached: %v", chunk.ID, err)
		}
		capBanner := fmt.Sprintf("CI remediation cap (%d/%d) reached — awaiting human review", maxCIRemediationAttempts, maxCIRemediationAttempts)
		if err := s.store.UpdateChunkBannerAndStatus(ctx, chunk.ID, "failed", capBanner); err != nil {
			log.Printf("[vanadis] warning updating banner for chunk %s on cap reached: %v", chunk.ID, err)
		}
		s.syncBuildWidget(ctx, build.ID, capBanner)

		alertMsg := fmt.Sprintf("🛑 [%s#%d] CI check %q failed %d times (remediation cap %d/%d reached). Automatic fix loop paused for human review.", ev.Repo, ev.PRNumber, ev.CheckName, failCount, maxCIRemediationAttempts, maxCIRemediationAttempts)
		if err := s.dispatchDirectMessage(ctx, build.ThreadID, alertMsg); err != nil {
			log.Printf("[vanadis] warning sending cap direct message to thread %s: %v", build.ThreadID, err)
		}

		s.notifyRestateChunkFailed(ctx, chunk.ID)
		if chunk.MergeAwakeableID == "" && chunk.DeployAwakeableID == "" {
			s.publishExternalEvent(ctx, "vanadis_chunk", "failed", []string{chunk.ID}, "ci_remediation_cap_reached")
			s.publishExternalEvent(ctx, "vanadis_build", "failed", []string{build.ID}, "ci_remediation_cap_reached")
		}
	}
	return nil
}

func (s *VanadisServer) handlePRConflict(ctx context.Context, ev pubsub.PRLifecycleEvent, chunk *VanadisChunk, build *VanadisBuild) error {
	status := "conflict"
	banner := "Merge conflict detected"
	if err := s.store.UpdateChunkBannerAndStatus(ctx, chunk.ID, status, banner); err != nil {
		log.Printf("[vanadis] warning updating banner for chunk %s on conflict: %v", chunk.ID, err)
	}
	s.syncBuildWidget(ctx, build.ID, banner)
	prompt := s.formatConflictPrompt(ev)
	return s.dispatchWakePrompt(ctx, build.ThreadID, prompt)
}

func (s *VanadisServer) handlePRDeployFailed(ctx context.Context, ev pubsub.PRLifecycleEvent, chunk *VanadisChunk, build *VanadisBuild) error {
	status := "deploy_failed"
	banner := "Deployment failed for " + firstNonEmpty(ev.JobName, "service")
	if err := s.store.UpdateChunkBannerAndStatus(ctx, chunk.ID, status, banner); err != nil {
		log.Printf("[vanadis] warning updating banner for chunk %s on deploy failure: %v", chunk.ID, err)
	}
	s.syncBuildWidget(ctx, build.ID, banner)
	prompt := s.formatDeployFailedPrompt(ev)
	return s.dispatchWakePrompt(ctx, build.ThreadID, prompt)
}

func (s *VanadisServer) handlePRSyncFailed(ctx context.Context, ev pubsub.PRLifecycleEvent, chunk *VanadisChunk, build *VanadisBuild) error {
	status := "deploy_failed"
	banner := "Git file sync failed"
	if err := s.store.UpdateChunkBannerAndStatus(ctx, chunk.ID, status, banner); err != nil {
		log.Printf("[vanadis] warning updating banner for chunk %s on sync failure: %v", chunk.ID, err)
	}
	s.syncBuildWidget(ctx, build.ID, banner)
	prompt := s.formatSyncFailedPrompt(ev)
	return s.dispatchWakePrompt(ctx, build.ThreadID, prompt)
}

func (s *VanadisServer) advanceLegacyChunkByPR(ctx context.Context, repo string, prNumber int) {
	res, err := s.store.AdvanceChunkByPR(ctx, repo, prNumber)
	if err != nil || res == nil || !res.Matched {
		return
	}
	s.syncBuildWidget(ctx, res.BuildID, "")
	if !res.BuildCompleted && res.NextChunkIndex >= 0 {
		if qErr := s.enqueueExecuteChunk(ctx, ExecuteChunkArgs{BuildID: res.BuildID, ChunkIndex: res.NextChunkIndex}); qErr != nil {
			log.Printf("[vanadis] warning enqueueing next chunk for build %s: %v", res.BuildID, qErr)
		}
	}
}

func (s *VanadisServer) handlePRMerged(ctx context.Context, ev pubsub.PRLifecycleEvent, chunk *VanadisChunk, build *VanadisBuild) error {
	if err := s.store.UpdateChunkStatus(ctx, chunk.ID, "merged"); err != nil {
		log.Printf("[vanadis] warning updating chunk %s to merged: %v", chunk.ID, err)
	}
	RecordChunkTotal("merged")
	if chunk.MergeAwakeableID != "" {
		if err := s.resolveRestateAwakeable(ctx, chunk.MergeAwakeableID, "merged"); err != nil {
			log.Printf("[vanadis] warning resolving merge awakeable %s: %v; falling back to legacy advancement", chunk.MergeAwakeableID, err)
			s.advanceLegacyChunkByPR(ctx, ev.Repo, ev.PRNumber)
			s.publishExternalEvent(ctx, "vanadis_chunk", "merged", []string{chunk.ID}, "")
			return nil
		}
		s.syncBuildWidget(ctx, build.ID, "")
	} else {
		s.advanceLegacyChunkByPR(ctx, ev.Repo, ev.PRNumber)
		s.publishExternalEvent(ctx, "vanadis_chunk", "merged", []string{chunk.ID}, "")
	}
	return nil
}

func (s *VanadisServer) completeLegacyChunkDeploy(ctx context.Context, chunk *VanadisChunk, build *VanadisBuild) {
	wasCompleted := chunk.Status == "completed"
	if err := s.store.UpdateChunkStatus(ctx, chunk.ID, "completed"); err != nil {
		log.Printf("[vanadis] warning updating chunk %s to completed: %v", chunk.ID, err)
	}
	if wasCompleted {
		return
	}
	if err := s.completeBuildIfAllChunksFinished(ctx, build.ID); err != nil {
		log.Printf("[vanadis] warning completing build %s: %v", build.ID, err)
	}
	s.publishExternalEvent(ctx, "vanadis_chunk", "completed", []string{chunk.ID}, "")
}

func (s *VanadisServer) completeBuildIfAllChunksFinished(ctx context.Context, buildID string) error {
	b, allChunks, err := s.store.GetBuild(ctx, buildID)
	if err != nil || b == nil {
		return err
	}
	for _, c := range allChunks {
		finished := c.Status == "completed" || (c.Status == "merged" && b.Status == "completed")
		if !finished {
			return nil
		}
	}
	if b.Status != "completed" {
		if err := s.store.UpdateBuildStatus(ctx, b.ID, "completed"); err != nil {
			log.Printf("[vanadis] warning updating build status to completed for %s: %v", b.ID, err)
			return err
		}
	}
	RecordBuildTotal("completed")
	s.syncBuildWidget(ctx, b.ID, "Build completed successfully")
	s.publishExternalEvent(ctx, "vanadis_build", "completed", []string{b.ID}, "")
	if !b.Quiet {
		res := &VanadisAdvancementResult{
			BuildID:   b.ID,
			ThreadID:  b.ThreadID,
			Objective: b.Objective,
			Tier:      b.Tier,
			Chunks:    chunksToSummaries(allChunks),
		}
		s.dispatchCompletedBuildWake(ctx, res)
	}
	return nil
}

func (s *VanadisServer) handlePRDeploySuccess(ctx context.Context, chunk *VanadisChunk, build *VanadisBuild) error {
	if chunk.DeployAwakeableID != "" {
		if err := s.resolveRestateAwakeable(ctx, chunk.DeployAwakeableID, "deploy_success"); err != nil {
			log.Printf("[vanadis] warning resolving deploy awakeable %s: %v; falling back to legacy completion", chunk.DeployAwakeableID, err)
			s.completeLegacyChunkDeploy(ctx, chunk, build)
			return nil
		}
	} else {
		s.completeLegacyChunkDeploy(ctx, chunk, build)
	}
	return nil
}

func (s *VanadisServer) handlePRClosedUnmerged(ctx context.Context, chunk *VanadisChunk) error {
	_, _, _, err := s.failChunkInternal(ctx, chunk.ID, "pr_closed_unmerged")
	return err
}

func ciRemediationDispatchID(chunkID, headSHA string, failCount int) string {
	cleanSHA := strings.TrimSpace(headSHA)
	if cleanSHA != "" {
		return fmt.Sprintf("vanadis-%s-cifail-%s-%d", chunkID, cleanSHA, failCount)
	}
	return fmt.Sprintf("vanadis-%s-cifail-%d", chunkID, failCount)
}
