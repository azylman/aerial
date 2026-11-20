package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/github"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

const (
	statusFailed       = github.StatusFailed
	statusPassed       = github.StatusPassed
	statusPending      = github.StatusPending
	ghStatusCompleted  = github.GHStatusCompleted
	conclusionSuccess  = github.ConclusionSuccess
	conclusionFailure  = github.ConclusionFailure
	conclusionTimedOut = github.ConclusionTimedOut
)

func isRoboRevCheck(checkName string) bool {
	return github.IsRoboRevCheck(checkName)
}

func isGateAggregatorCheck(checkName string) bool {
	return github.IsGateAggregatorCheck(checkName)
}

func classifyCheckState(status, conclusion string) string {
	return github.ClassifyCheckState(status, conclusion)
}

func extractStringMapFromMetadata(meta map[string]any, key string) map[string]string {
	if meta == nil {
		return nil
	}
	rawVal, ok := meta[key]
	if !ok || rawVal == nil {
		return nil
	}
	out := make(map[string]string)
	switch m := rawVal.(type) {
	case map[string]any:
		for k, v := range m {
			cleanKey := strings.ToLower(strings.TrimSpace(k))
			if cleanKey == "" {
				continue
			}
			if s, ok := v.(string); ok {
				out[cleanKey] = strings.TrimSpace(s)
			}
		}
	case map[string]string:
		for k, v := range m {
			cleanKey := strings.ToLower(strings.TrimSpace(k))
			if cleanKey == "" {
				continue
			}
			out[cleanKey] = strings.TrimSpace(v)
		}
	}
	return out
}

func buildPRRolloutState(meta map[string]any) *pubsub.PRRolloutState {
	if meta == nil {
		return nil
	}
	expectedJobs := extractJobsFromMetadata(meta)
	startedJobs := extractStringSliceFromMetadata(meta, "started_jobs")
	deployedJobs := extractStringSliceFromMetadata(meta, "deployed_jobs")
	jobStates := extractStringMapFromMetadata(meta, "job_states")
	kustStatuses := extractStringMapFromMetadata(meta, "kustomization_statuses")
	if m, ok := metadataSnapshot(meta); ok {
		expectedJobs, jobStates = mergeStructuredBuildState(m, expectedJobs, jobStates)
	}
	if len(expectedJobs) == 0 && len(startedJobs) == 0 && len(deployedJobs) == 0 && len(jobStates) == 0 && len(kustStatuses) == 0 {
		return nil
	}
	return &pubsub.PRRolloutState{
		ExpectedJobs:          expectedJobs,
		StartedJobs:           startedJobs,
		DeployedJobs:          deployedJobs,
		JobStates:             jobStates,
		KustomizationStatuses: kustStatuses,
	}
}

// metadataSnapshot reads the identity-validated build snapshot that
// loadPRRolloutState placed in meta; raw registry JSON is revalidated.
func metadataSnapshot(meta map[string]any) (BuildMetadata, bool) {
	switch v := meta["build_metadata"].(type) {
	case BuildMetadata:
		return v, true
	case nil:
		return BuildMetadata{}, false
	default:
		raw, err := json.Marshal(v)
		if err != nil {
			return BuildMetadata{}, false
		}
		m, err := parseStoredBuildMetadata(raw)
		return m, err == nil
	}
}

// mergeStructuredBuildState adds every selected service to the expected jobs and
// derives its image card state, never overriding a deployment-reported state.
func mergeStructuredBuildState(m BuildMetadata, expectedJobs []string, jobStates map[string]string) ([]string, map[string]string) {
	if len(m.Selected) == 0 {
		return expectedJobs, jobStates
	}
	if jobStates == nil {
		jobStates = make(map[string]string, len(m.Selected))
	}
	for _, sel := range m.Selected {
		expectedJobs = append(expectedJobs, sel.Service)
		key := strings.ToLower(sel.Service)
		if jobStates[key] == "" {
			jobStates[key] = structuredServiceState(m, sel.Service)
		}
	}
	return dedupeTrimmedStrings(expectedJobs), jobStates
}

// structuredServiceState maps a service's snapshot result onto its card state.
// Services without a result in the snapshot (including inherited results while
// the current attempt is unfinished) are pending.
func structuredServiceState(m BuildMetadata, service string) string {
	for _, r := range m.Results {
		if r.Service != service {
			continue
		}
		switch {
		case r.State == buildResultRunning:
			return imageStateBuilding
		case r.State == conclusionSuccess && r.Pushed:
			return imageStateBuilt
		case r.State == conclusionSuccess:
			return imageStateUnpublished
		default:
			return imageStateFailed
		}
	}
	return imageStatePending
}

const (
	imageStatePending     = "image_pending"
	imageStateBuilding    = "image_building"
	imageStateBuilt       = "image_built"
	imageStateUnpublished = "image_unpublished"
	imageStateFailed      = "image_failed"
)

func (s *RouterServer) publishDeployLifecycleEvent(ctx context.Context, evt DeployEvent, eventName, status string) error {
	jobName := evt.JobName
	if (eventName == eventDeploySuccess || eventName == eventSyncSuccess) && jobName == "" {
		jobName = jobGitSync
	}
	return s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
		Event:        eventName,
		Repo:         evt.Repo,
		PRNumber:     evt.PRNumber,
		MergeSHA:     evt.CommitSHA,
		JobName:      jobName,
		Details:      evt.Details,
		TargetID:     evt.TargetID,
		Status:       status,
		RolloutState: s.loadPRRolloutState(ctx, evt.Repo, evt.PRNumber),
		Quiet:        s.isDependabotPRNumber(ctx, evt.Repo, evt.PRNumber),
		OccurredAt:   time.Now().UTC(),
	})
}

// recordFailedCheckRunDetailed reports granular per-check failure state.
// The in-memory failedCheckRuns index is deleted: Restate PRObject and the
// pr_registry projection own failure memory, so every call reports no active
// failure and lets the registry atomic transition decide.
func (*RouterServer) recordFailedCheckRunDetailed(_repo string, _prNumber int, _headSHA, _checkName string) (emitAdditional bool, wasActive bool, hadPriorEverFailed bool) {
	return false, false, false
}

func (s *RouterServer) hasFailedCIForSHA(ctx context.Context, repo string, prNumber int, headSHA string) bool {
	return github.HasPendingOrFailedChecksForSHA(ctx, s.ghClient(), repo, prNumber, headSHA)
}

// canRecoverRoboRevSuccess reports whether a roborev success recovers the PR.
// When authoritative, the Restate PRObject CI-failed verdict decides; the
// live roborev comment stays as the non-authoritative fallback signal.
func (s *RouterServer) canRecoverRoboRevSuccess(ctx context.Context, repo string, prNumber int, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if s == nil || prNumber <= 0 || headSHA == "" {
		return false
	}
	if s.isCIFailedForSHA(ctx, repo, prNumber, headSHA) {
		return true
	}
	comment := s.fetchLatestRoboRevComment(ctx, repo, prNumber, headSHA)
	return comment != "" && !strings.Contains(comment, "## roborev: Review Passed")
}

func (s *RouterServer) canRecoverCheckSuccess(ctx context.Context, repo string, prNumber int, checkName, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if s == nil || prNumber <= 0 || headSHA == "" {
		return false
	}
	if isGateAggregatorCheck(checkName) {
		return headSHA != "" && s.isCIFailedForSHA(ctx, repo, prNumber, headSHA)
	}
	if isRoboRevCheck(checkName) {
		return s.canRecoverRoboRevSuccess(ctx, repo, prNumber, headSHA)
	}
	return headSHA != "" && s.isCIFailedForSHA(ctx, repo, prNumber, headSHA)
}

func (s *RouterServer) isStaleOrIgnoredCIFailure(ctx context.Context, res *ResolvedGitHubEvent) bool {
	if s.registry == nil || (res.Conclusion != conclusionFailure && res.Conclusion != conclusionTimedOut) {
		return true
	}
	// Gate-aggregator failures stay ignored. The legacy cancelled-check
	// exception died with the tracker; Restate owns rerun decisions.
	if isGateAggregatorCheck(res.CheckName) {
		return true
	}
	if res.PRNumber <= 0 || res.HeadSHA == "" || isZeroSHA(res.HeadSHA) {
		return false
	}
	regTargetID, _, regHeadSHA, regMergeSHA, err := s.registry.ResolvePRByNumber(ctx, res.Repo, res.PRNumber)
	if err == nil && regTargetID != "" {
		res.TargetID = firstNonEmpty(res.TargetID, regTargetID)
	}
	if err == nil && regMergeSHA != "" {
		res.MergeSHA = regMergeSHA
	}
	trimmedSHA := strings.TrimSpace(res.HeadSHA)
	if err == nil && regHeadSHA != "" && !strings.EqualFold(regHeadSHA, trimmedSHA) && (regMergeSHA == "" || !strings.EqualFold(regMergeSHA, trimmedSHA)) {
		slog.InfoContext(ctx, "[synapse] [github] ignoring stale CI failure for superseded commit", "repo", res.Repo, "pr", res.PRNumber, "head_sha", res.HeadSHA, "current_head_sha", regHeadSHA)
		return true
	}
	return false
}

func (s *RouterServer) isMergeSHAFailure(ctx context.Context, res *ResolvedGitHubEvent) bool {
	if res == nil || res.PRNumber <= 0 || res.HeadSHA == "" || isZeroSHA(res.HeadSHA) {
		return false
	}
	trimmedSHA := strings.TrimSpace(res.HeadSHA)
	if res.MergeSHA != "" && strings.EqualFold(trimmedSHA, strings.TrimSpace(res.MergeSHA)) {
		return true
	}
	if s != nil && s.registry != nil {
		if _, _, _, regMergeSHA, err := s.registry.ResolvePRByNumber(ctx, normalizeRepo(res.Repo), res.PRNumber); err == nil && regMergeSHA != "" {
			res.MergeSHA = regMergeSHA
			return strings.EqualFold(trimmedSHA, strings.TrimSpace(regMergeSHA))
		}
	}
	return false
}

func (s *RouterServer) populateMissingBranch(ctx context.Context, res *ResolvedGitHubEvent) {
	if res == nil || s.registry == nil || res.PRNumber <= 0 || strings.TrimSpace(res.Branch) != "" {
		return
	}
	_, branch, _, _, err := s.registry.ResolvePRByNumber(ctx, normalizeRepo(res.Repo), res.PRNumber)
	if err == nil && branch != "" {
		res.Branch = branch
	}
}

func (s *RouterServer) completeCIFailureTransition(ctx context.Context, res *ResolvedGitHubEvent, details, checkURL string) error {
	if res == nil {
		return nil
	}
	s.populateMissingBranch(ctx, res)
	if strings.TrimSpace(details) == "" {
		if isRoboRevCheck(res.CheckName) {
			comment := s.fetchLatestRoboRevComment(ctx, res.Repo, res.PRNumber, res.HeadSHA)
			details = github.FormatRoboRevPrompt(res.Repo, res.PRNumber, res.HeadSHA, comment)
		} else {
			details = github.FormatCheckFailurePrompt(res.Repo, res.PRNumber, res.HeadSHA, res.CheckName, details, checkURL)
		}
	}
	return s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
		Event:        prCIFailed,
		Repo:         res.Repo,
		PRNumber:     res.PRNumber,
		Branch:       res.Branch,
		HeadSHA:      res.HeadSHA,
		MergeSHA:     res.MergeSHA,
		TargetID:     res.TargetID,
		Status:       prCIFailed,
		CheckName:    res.CheckName,
		CheckState:   statusFailed,
		CheckURL:     checkURL,
		Details:      details,
		RolloutState: s.loadPRRolloutState(ctx, res.Repo, res.PRNumber),
		Quiet:        isDependabotBranch(res.Branch),
		OccurredAt:   time.Now().UTC(),
	})
}

// resolveLegacyWorkflowRunImages is the bounded compatibility adapter for runs
// whose exact executed workflow blob is in the immutable legacy allowlist. It
// infers services from historical job and step names and must never be reached
// for metadata-capable producer runs.
func (s *RouterServer) resolveLegacyWorkflowRunImages(ctx context.Context, repo string, runID int64) ([]string, error) {
	repo = normalizeRepo(repo)
	if repo == "" || runID <= 0 {
		return nil, fmt.Errorf("invalid repo or runID for workflow run images resolution")
	}
	var jobsResp GitHubWorkflowJobsResponse
	if err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/actions/runs/%d/jobs?per_page=100", repo, runID), 5<<20, &jobsResp); err != nil {
		return nil, err
	}

	if hasSkippedImagePromotionJob(jobsResp.Jobs) {
		return nil, nil
	}

	owner := "azylman"
	if parts := strings.Split(repo, "/"); len(parts) == 2 {
		owner = strings.ToLower(parts[0])
	}

	var rawImages []string
	for _, job := range jobsResp.Jobs {
		if job.Status != ghStatusCompleted || job.Conclusion != conclusionSuccess || !hasExecutedBuildStep(job.Steps) {
			continue
		}
		if service, ok := buildJobService(job.Name); ok {
			rawImages = append(rawImages, fmt.Sprintf("ghcr.io/%s/%s:latest", owner, service))
		}
	}
	return dedupeTrimmedStrings(rawImages), nil
}

func (s *RouterServer) fetchLatestRoboRevComment(ctx context.Context, repo string, prNumber int, headSHA string) string {
	repo = normalizeRepo(repo)
	if s == nil || repo == "" || prNumber <= 0 {
		return ""
	}
	var comments []GitHubIssueComment
	path := fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100", repo, prNumber)
	if err := s.getGitHubJSON(ctx, path, 2<<20, &comments); err != nil {
		slog.WarnContext(ctx, "[synapse] [github] error fetching roborev comments", "repo", repo, "pr", prNumber, "error", err)
		return ""
	}
	shortSHA := strings.TrimSpace(headSHA)
	if len(shortSHA) > 7 {
		shortSHA = shortSHA[:7]
	}
	for _, comment := range slices.Backward(comments) {
		body := strings.TrimSpace(comment.Body)
		if !strings.Contains(body, "## roborev:") && !strings.Contains(body, "## roborev-override") {
			continue
		}
		if shortSHA != "" && !strings.Contains(body, "`"+shortSHA) && !strings.Contains(body, headSHA) {
			continue
		}
		return truncatePromptRunes(body, maxRoboRevCommentRunes)
	}
	return ""
}

func formatCollaboratorCIFailureMessage(mention, repo string, prNumber int, prTitle, branch, headSHA, checkName, checkURL string) string {
	return github.BuildCollaboratorAlertMessage(mention, repo, prNumber, prTitle, branch, headSHA, checkName, checkURL)
}

func formatCollaboratorConflictMessage(mention, repo string, prNumber int, prTitle, branch, headSHA string) string {
	return github.BuildCollaboratorConflictMessage(mention, repo, prNumber, prTitle, branch, headSHA)
}

func (s *RouterServer) resolveCollaboratorAlertTarget(ctx context.Context, repo string, prNumber int, expectedSHA string, pr GitHubPullRequest) (GitHubPullRequest, string, string, bool, error) {
	if s == nil || prNumber <= 0 {
		return pr, "", "", false, nil
	}
	if strings.TrimSpace(pr.User.Login) == "" {
		var fetched GitHubPullRequest
		path := fmt.Sprintf("/repos/%s/pulls/%d", normalizeRepo(repo), prNumber)
		if err := s.getGitHubJSON(ctx, path, 2<<20, &fetched); err != nil {
			return pr, "", "", false, err
		}
		pr = fetched
	}
	expectedSHA = strings.TrimSpace(expectedSHA)
	if pr.Merged {
		return pr, "", "", false, nil
	}
	if pr.State != "" && !strings.EqualFold(pr.State, "open") {
		return pr, "", "", false, nil
	}
	if expectedSHA != "" && pr.Head.SHA != "" && !strings.EqualFold(pr.Head.SHA, expectedSHA) {
		return pr, "", "", false, nil
	}
	if ParseAerialPRMetadata(pr.GetBody()).OK() {
		return pr, "", "", false, nil
	}
	channelID, mention, ok := s.lookupCollaboratorCIAlert(repo, pr.User.Login)
	if !ok || channelID == "" {
		return pr, "", "", false, nil
	}
	return pr, channelID, mention, true, nil
}

// dispatchCollaboratorDM sends one collaborator DM. Per-SHA dispatch dedup
// died with the tracker; redeliveries may resend (accepted at-least-once:
// the Restate journal and effect keys own dedup, delivery retries the send).
func (s *RouterServer) dispatchCollaboratorDM(ctx context.Context, _repo string, _prNumber int, sha, channelID, msg string) error {
	normSHA := strings.TrimSpace(sha)
	if s == nil || s.dispatcher == nil || channelID == "" || normSHA == "" || isZeroSHA(normSHA) {
		return nil
	}
	req := DirectMessageRequest{
		ChannelID: channelID,
		Content:   msg,
	}
	return s.dispatcher.DispatchDirectMessage(ctx, req)
}

func (s *RouterServer) notifyCollaboratorConflict(ctx context.Context, repo string, pr GitHubPullRequest) error {
	if s == nil || !s.hasCollaboratorCIAlertsForRepo(repo) {
		return nil
	}
	resolvedPR, channelID, mention, ok, err := s.resolveCollaboratorAlertTarget(ctx, repo, pr.Number, pr.Head.SHA, pr)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	sha := pr.Head.SHA
	if resolvedPR.Head.SHA != "" {
		sha = resolvedPR.Head.SHA
	}
	prNum := resolvedPR.Number
	branch := pr.Head.Ref
	if resolvedPR.Head.Ref != "" {
		branch = resolvedPR.Head.Ref
	}
	// Journal before the DM: the Restate journal is the ordered record and
	// owns alert dedup; the DM below is at-least-once on redelivery.
	if err := s.routePREvent(ctx, "HandleCollaboratorAlert", restateclient.PREvent{
		Repo: repo, PRNumber: prNum, HeadSHA: sha, Branch: branch,
		Title: resolvedPR.Title, Unmanaged: true,
	}); err != nil {
		return err
	}
	msg := formatCollaboratorConflictMessage(mention, repo, prNum, resolvedPR.Title, branch, sha)
	return s.dispatchCollaboratorDM(ctx, repo, prNum, sha, channelID, msg)
}

func (s *RouterServer) notifyCollaboratorCIFailure(ctx context.Context, res *ResolvedGitHubEvent, checkURL string) error {
	if s == nil || res == nil || res.TargetID != "" || res.PRNumber <= 0 || res.HeadSHA == "" || isZeroSHA(res.HeadSHA) || !s.hasCollaboratorCIAlertsForRepo(res.Repo) {
		return nil
	}
	resolvedPR, channelID, mention, ok, err := s.resolveCollaboratorAlertTarget(ctx, res.Repo, res.PRNumber, res.HeadSHA, GitHubPullRequest{
		Number: res.PRNumber,
		Head:   GitHubPRRef{Ref: res.Branch, SHA: res.HeadSHA},
	})
	if err != nil || !ok {
		return err
	}
	branch := firstNonEmpty(resolvedPR.Head.Ref, res.Branch)
	sha := firstNonEmpty(resolvedPR.Head.SHA, res.HeadSHA)
	// Journal before the DM, matching the conflict notifier above.
	if err := s.routePREvent(ctx, "HandleCollaboratorAlert", restateclient.PREvent{
		Repo: res.Repo, PRNumber: resolvedPR.Number, HeadSHA: sha, MergeSHA: res.MergeSHA,
		CheckName: res.CheckName, Branch: branch, Title: resolvedPR.Title, Unmanaged: true,
	}); err != nil {
		return err
	}
	msg := formatCollaboratorCIFailureMessage(mention, res.Repo, resolvedPR.Number, resolvedPR.Title, branch, sha, res.CheckName, checkURL)
	return s.dispatchCollaboratorDM(ctx, res.Repo, resolvedPR.Number, sha, channelID, msg)
}

func (s *RouterServer) handleMergeOrPromotionFailure(ctx context.Context, res *ResolvedGitHubEvent, details string) (bool, error) {
	if s.isMergeSHAFailure(ctx, res) || (res.Event == ghEventWorkflowRun && isMainBranch(res.Branch) && res.PRNumber > 0) {
		if res.Event == ghEventWorkflowRun {
			return true, s.handleImagePromotionDeployFailed(ctx, res.Repo, res.PRNumber, res.Branch, res.HeadSHA, res.TargetID, errors.New(details))
		}
		return true, nil
	}
	return false, nil
}

func (s *RouterServer) recordNonGateCheckFailureDetailed(res *ResolvedGitHubEvent) (emitAdditional, wasActive, hadPriorEverFailed bool) {
	if res == nil || res.PRNumber <= 0 || res.HeadSHA == "" {
		return false, false, false
	}
	if res.Event == ghEventWorkflowRun {
		return false, false, false
	}
	if res.Event != ghEventCheckRun || isGateAggregatorCheck(res.CheckName) || isBuildMetadataCheck(res.CheckName) {
		return false, false, false
	}
	return s.recordFailedCheckRunDetailed(res.Repo, res.PRNumber, res.HeadSHA, res.CheckName)
}

// isCIFailedForSHA reports whether the PR is CI-failed for the given head
// SHA. When authoritative, the Restate PRObject verdict decides and the
// Postgres projection is never consulted; a resolve failure falls back to
// the projection instead of losing the signal. Off the authoritative path
// it reads the projection directly.
func (s *RouterServer) isCIFailedForSHA(ctx context.Context, repo string, prNumber int, headSHA ...string) bool {
	if s.restateAuthoritative() && prNumber > 0 {
		var query string
		if len(headSHA) > 0 {
			query = headSHA[0]
		}
		decision, err := s.resolveCIRecovery(ctx, repo, prNumber, query)
		if err == nil {
			return decision.CIFailed
		}
		slog.WarnContext(ctx, "[synapse] [github] CI recovery resolve failed, falling back to projection", "repo", repo, "pr", prNumber, "error", err)
	}
	return s.isPRRegistryCIFailed(ctx, repo, prNumber, headSHA...)
}

// isPRRegistryCIFailed is the legacy fallback: it reads the Postgres
// ci_failed projection directly and runs only off the authoritative path or
// when the Restate resolve fails. Never called directly by effect gates;
// isCIFailedForSHA is the single entry point.
func (s *RouterServer) isPRRegistryCIFailed(ctx context.Context, repo string, prNumber int, headSHA ...string) bool {
	if s == nil || s.registry == nil || prNumber <= 0 || normalizeRepo(repo) == "" {
		return false
	}
	lifecycle, err := s.registry.LoadPRBuildLifecycle(ctx, repo, prNumber)
	if err != nil || !strings.EqualFold(strings.TrimSpace(lifecycle.Status), prCIFailed) {
		return false
	}
	if len(headSHA) > 0 {
		cleanSHA := strings.TrimSpace(headSHA[0])
		regHead := strings.TrimSpace(lifecycle.HeadSHA)
		if cleanSHA != "" && regHead != "" && !strings.EqualFold(regHead, cleanSHA) {
			return false
		}
	}
	return true
}

func (s *RouterServer) shouldRetryCIFailureCompletion(ctx context.Context, res *ResolvedGitHubEvent) bool {
	if res.PRNumber <= 0 || !s.isCIFailedForSHA(ctx, res.Repo, res.PRNumber) {
		return false
	}
	if res.TargetID == "" && s.registry != nil {
		if rTID, _, _, _, rErr := s.registry.ResolvePRByNumber(ctx, res.Repo, res.PRNumber); rErr == nil && rTID != "" {
			res.TargetID = rTID
		}
	}
	if res.TargetID == "" {
		res.TargetID = s.resolveHeadlessPRTargetID(ctx, res.Repo, res.PRNumber)
	}
	return res.TargetID != ""
}

func (s *RouterServer) handleCIFailureByPRNumber(ctx context.Context, res *ResolvedGitHubEvent, details, checkURL string, emitAdditional, wasActive bool) (bool, error) {
	s.healDependabotPR(ctx, res)
	targetID, updated, err := s.registry.AtomicTransitionCIFailed(ctx, res.Repo, res.PRNumber)
	if err != nil {
		return false, err
	}
	res.TargetID = firstNonEmpty(res.TargetID, targetID)
	shouldComplete := updated || (emitAdditional && res.TargetID != "")
	if !shouldComplete && !wasActive && s.shouldRetryCIFailureCompletion(ctx, res) {
		shouldComplete = true
	}
	if shouldComplete {
		return true, s.completeCIFailureTransition(ctx, res, details, checkURL)
	}
	return shouldComplete, nil
}

func (s *RouterServer) handleCIFailureByHeadSHA(ctx context.Context, res *ResolvedGitHubEvent, details, checkURL string) (bool, error) {
	targetID, prNum, updated, err := s.registry.AtomicTransitionCIFailedByHeadSHA(ctx, res.Repo, res.HeadSHA)
	if err != nil {
		return false, err
	}
	res.TargetID = firstNonEmpty(res.TargetID, targetID)
	if prNum > 0 && res.PRNumber == 0 {
		res.PRNumber = prNum
	}
	shouldComplete := updated
	if !shouldComplete && s.shouldRetryCIFailureCompletion(ctx, res) {
		shouldComplete = true
	}
	if !shouldComplete {
		return false, nil
	}
	if err := s.completeCIFailureTransition(ctx, res, details, checkURL); err != nil {
		return false, err
	}
	return true, nil
}

func syncPRFallbackFields(res *ResolvedGitHubEvent, pr GitHubPullRequest) GitHubPullRequest {
	if res == nil {
		return pr
	}
	if pr.Number == 0 && res.PRNumber > 0 {
		pr.Number = res.PRNumber
	}
	if pr.Head.Ref == "" && res.Branch != "" {
		pr.Head.Ref = res.Branch
	}
	if pr.Head.SHA == "" && res.HeadSHA != "" {
		pr.Head.SHA = res.HeadSHA
	}
	return pr
}

func (s *RouterServer) resolveAutoMergeDisabledPR(ctx context.Context, res *ResolvedGitHubEvent, pr GitHubPullRequest) (GitHubPullRequest, bool, error) {
	if !isPRMergeabilityUnknown(pr) || res.PRNumber <= 0 || normalizeRepo(res.Repo) == "" {
		return pr, true, nil
	}
	fetchedModel, err := github.FetchSinglePRWithMergeabilityRetry(ctx, s.ghClient(), res.Repo, res.PRNumber, 3)
	if err != nil {
		return pr, false, err
	}
	if fetchedModel.Merged || (fetchedModel.State != "" && !strings.EqualFold(fetchedModel.State, "open")) {
		return pr, false, nil
	}
	if res.HeadSHA != "" && fetchedModel.Head.SHA != "" && !strings.EqualFold(fetchedModel.Head.SHA, strings.TrimSpace(res.HeadSHA)) {
		return pr, false, nil
	}
	fetched := GitHubPullRequest{
		NodeID:         fetchedModel.NodeID,
		Number:         fetchedModel.Number,
		State:          fetchedModel.State,
		Title:          fetchedModel.Title,
		Body:           fetchedModel.Body,
		Head:           GitHubPRRef(fetchedModel.Head),
		Base:           GitHubPRRef(fetchedModel.Base),
		Draft:          fetchedModel.Draft,
		Merged:         fetchedModel.Merged,
		MergeCommitSHA: fetchedModel.MergeCommitSHA,
		Mergeable:      fetchedModel.Mergeable,
		MergeableState: fetchedModel.MergeableState,
		User:           fetchedModel.User,
	}
	return syncPRFallbackFields(res, fetched), true, nil
}

// resolvePRFromCommitSHA discovers the PR for a commit SHA via the GitHub
// API. The in-memory commitSHAPRs index is deleted; durable ResolvePRBySHA
// in pr_registry (checked first by ResolveSHA) is the lookup index.
func (s *RouterServer) resolvePRFromCommitSHA(ctx context.Context, repo, sha string) (int, string, error) {
	return github.ResolvePRFromCommitSHA(ctx, s.ghClient(), repo, sha)
}
