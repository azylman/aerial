package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/github"
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

func prCheckKey(repo string, prNumber int) string {
	return github.PRCheckKey(repo, prNumber)
}

func (s *RouterServer) recordFailedCISHA(repo string, prNumber int, headSHA string) bool {
	if s == nil {
		return false
	}
	return s.ghState.RecordFailedCISHA(repo, prNumber, headSHA)
}

func (s *RouterServer) clearFailedCISHA(repo string, prNumber int) {
	if s == nil {
		return
	}
	s.ghState.ClearFailedCISHA(repo, prNumber)
}

func (s *RouterServer) recordFailedCheckRunDetailed(repo string, prNumber int, headSHA, checkName string) (emitAdditional bool, wasActive bool, hadPriorEverFailed bool) {
	if s == nil {
		return false, false, false
	}
	return s.ghState.RecordFailedCheckRunDetailed(repo, prNumber, headSHA, checkName)
}

func (s *RouterServer) rollbackFailedCheckRun(repo string, prNumber int, headSHA, checkName string, wasActive, hadPriorEverFailed bool) {
	if s == nil {
		return
	}
	s.ghState.RollbackFailedCheckRun(repo, prNumber, headSHA, checkName, wasActive, hadPriorEverFailed)
}

func (s *RouterServer) clearActiveFailedCheckRun(repo string, prNumber int, headSHA, checkName string) {
	if s == nil {
		return
	}
	s.ghState.ClearActiveFailedCheckRun(repo, prNumber, headSHA, checkName)
}

func (s *RouterServer) hasActiveRoboRevFailure(repo string, prNumber int, headSHA string) bool {
	if s == nil {
		return false
	}
	return s.ghState.HasActiveRoboRevFailure(repo, prNumber, headSHA)
}

func (s *RouterServer) canRecoverNonGateCheckSuccess(repo string, prNumber int, checkName, headSHA string) bool {
	if s == nil {
		return false
	}
	return s.ghState.CanRecoverNonGateCheckSuccess(repo, prNumber, checkName, headSHA)
}

func (s *RouterServer) markUpdatedForCurrentMain(repo string, prNumber int) {
	if s == nil {
		return
	}
	s.ghState.MarkUpdatedForMain(repo, prNumber)
}

func (s *RouterServer) wasUpdatedForCurrentMain(repo string, prNumber int) bool {
	if s == nil {
		return false
	}
	return s.ghState.WasUpdatedForMain(repo, prNumber)
}

func (s *RouterServer) clearUpdatedPRsForMainCommit(repo, headSHA string) {
	if s == nil {
		return
	}
	s.ghState.ClearUpdatedPRsForMainCommit(repo, headSHA)
}

func (s *RouterServer) isCachedFailedCISHA(repo string, prNumber int, headSHA string) bool {
	if s == nil {
		return false
	}
	return s.ghState.IsCachedFailedCISHA(repo, prNumber, headSHA)
}

func (s *RouterServer) hasFailedCIForSHA(ctx context.Context, repo string, prNumber int, headSHA string) bool {
	return github.HasPendingOrFailedChecksForSHA(ctx, s.ghClient(), &s.ghState, repo, prNumber, headSHA)
}

func (s *RouterServer) hasCancelledCheckForSHA(repo string, prNumber int, headSHA string) bool {
	if s == nil {
		return false
	}
	return s.ghState.HasCancelledCheckSHA(repo, prNumber, headSHA)
}

func (s *RouterServer) canRecoverRoboRevSuccess(ctx context.Context, repo string, prNumber int, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if s == nil || prNumber <= 0 || headSHA == "" {
		return false
	}
	s.clearActiveFailedCheckRun(repo, prNumber, headSHA, "roborev")
	if s.ghState.HasNonRoboRevFailedSHA(repo, prNumber, headSHA) {
		return false
	}
	if s.ghState.HasEverFailedCheckRun(repo, prNumber, headSHA, "roborev") {
		return true
	}
	if s.isPRRegistryCIFailed(ctx, repo, prNumber, headSHA) {
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
		if s.hasActiveRoboRevFailure(repo, prNumber, headSHA) {
			return false
		}
		if s.ghState.HasNonRoboRevFailedSHA(repo, prNumber, headSHA) {
			return true
		}
		if s.hasCancelledCheckForSHA(repo, prNumber, headSHA) {
			return true
		}
		return headSHA != "" && s.isPRRegistryCIFailed(ctx, repo, prNumber, headSHA)
	}
	if isRoboRevCheck(checkName) {
		return s.canRecoverRoboRevSuccess(ctx, repo, prNumber, headSHA)
	}
	return s.canRecoverNonGateCheckSuccess(repo, prNumber, checkName, headSHA)
}

func (s *RouterServer) isStaleOrIgnoredCIFailure(ctx context.Context, res *ResolvedGitHubEvent) bool {
	if s.registry == nil || (res.Conclusion != conclusionFailure && res.Conclusion != conclusionTimedOut) {
		return true
	}
	if isGateAggregatorCheck(res.CheckName) && !s.hasCancelledCheckForSHA(res.Repo, res.PRNumber, res.HeadSHA) {
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

func recordCollaboratorSHAAlert(m *sync.Map, repo string, prNumber int, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if m == nil || prNumber <= 0 || headSHA == "" || isZeroSHA(headSHA) {
		return false
	}
	prev, loaded := m.Swap(prCheckKey(repo, prNumber), headSHA)
	return !loaded || !strings.EqualFold(fmt.Sprint(prev), headSHA)
}

func (s *RouterServer) dispatchCollaboratorDM(ctx context.Context, dedup *sync.Map, repo string, prNumber int, sha, channelID, msg string) error {
	normSHA := strings.TrimSpace(sha)
	if s == nil || s.dispatcher == nil || channelID == "" || normSHA == "" || isZeroSHA(normSHA) {
		return nil
	}
	if dedup != nil && !recordCollaboratorSHAAlert(dedup, repo, prNumber, normSHA) {
		return nil
	}
	req := DirectMessageRequest{
		ChannelID: channelID,
		Content:   msg,
	}
	if err := s.dispatcher.DispatchDirectMessage(ctx, req); err != nil {
		if dedup != nil {
			dedup.CompareAndDelete(prCheckKey(repo, prNumber), normSHA)
		}
		return err
	}
	return nil
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
	if !s.ghState.MarkCollaboratorConflict(repo, prNum, sha) {
		return nil
	}
	branch := pr.Head.Ref
	if resolvedPR.Head.Ref != "" {
		branch = resolvedPR.Head.Ref
	}
	msg := formatCollaboratorConflictMessage(mention, repo, prNum, resolvedPR.Title, branch, sha)
	if err := s.dispatchCollaboratorDM(ctx, nil, repo, prNum, sha, channelID, msg); err != nil {
		s.ghState.ClearCollaboratorConflict(repo, prNum, sha)
		return err
	}
	return nil
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
	if !s.ghState.MarkCollaboratorAlerted(res.Repo, resolvedPR.Number, sha) {
		return nil
	}
	msg := formatCollaboratorCIFailureMessage(mention, res.Repo, resolvedPR.Number, resolvedPR.Title, branch, sha, res.CheckName, checkURL)
	if err := s.dispatchCollaboratorDM(ctx, nil, res.Repo, resolvedPR.Number, sha, channelID, msg); err != nil {
		s.ghState.ClearCollaboratorAlerted(res.Repo, resolvedPR.Number, sha)
		return err
	}
	return nil
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
		if s.ghState.HasActiveFailedCheckRuns(res.Repo, res.PRNumber, res.HeadSHA) {
			return false, true, true
		}
		return false, false, false
	}
	if res.Event != ghEventCheckRun || isGateAggregatorCheck(res.CheckName) || isBuildMetadataCheck(res.CheckName) {
		return false, false, false
	}
	if !isRoboRevCheck(res.CheckName) {
		s.ghState.RecordNonRoboRevFailedSHA(res.Repo, res.PRNumber, res.HeadSHA)
	}
	return s.recordFailedCheckRunDetailed(res.Repo, res.PRNumber, res.HeadSHA, res.CheckName)
}

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
	if res.PRNumber <= 0 || !s.isPRRegistryCIFailed(ctx, res.Repo, res.PRNumber) {
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
	if !wasActive && res.Event == ghEventWorkflowRun && res.HeadSHA != "" && s.ghState.HasActiveFailedCheckRuns(res.Repo, res.PRNumber, res.HeadSHA) {
		wasActive = true
	}
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
	emitAdditional := false
	wasActive := false
	hadPriorEverFailed := false
	if prNum > 0 && res.PRNumber == 0 {
		res.PRNumber = prNum
		if res.Event == ghEventCheckRun && !isGateAggregatorCheck(res.CheckName) && !isBuildMetadataCheck(res.CheckName) {
			if !isRoboRevCheck(res.CheckName) {
				s.ghState.RecordNonRoboRevFailedSHA(res.Repo, res.PRNumber, res.HeadSHA)
			}
			emitAdditional, wasActive, hadPriorEverFailed = s.recordFailedCheckRunDetailed(res.Repo, res.PRNumber, res.HeadSHA, res.CheckName)
		}
	}
	shouldComplete := updated || (emitAdditional && res.TargetID != "")
	if !shouldComplete && !wasActive && s.shouldRetryCIFailureCompletion(ctx, res) {
		shouldComplete = true
	}
	if !shouldComplete {
		return false, nil
	}
	cErr := s.completeCIFailureTransition(ctx, res, details, checkURL)
	if cErr != nil {
		s.rollbackFailedCheckRun(res.Repo, res.PRNumber, res.HeadSHA, res.CheckName, wasActive, hadPriorEverFailed)
		return false, cErr
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

func (s *RouterServer) recordCommitSHAPR(repo, sha string, prNumber int, branch string) {
	if s == nil {
		return
	}
	s.ghState.RecordCommitSHAPR(repo, sha, prNumber, branch)
}

func (s *RouterServer) lookupCommitSHAPR(repo, sha string) (int, string, bool) {
	if s == nil {
		return 0, "", false
	}
	return s.ghState.LookupCommitSHAPR(repo, sha)
}

func (s *RouterServer) resolvePRFromCommitSHA(ctx context.Context, repo, sha string) (int, string, error) {
	if prNum, branch, ok := s.lookupCommitSHAPR(repo, sha); ok {
		return prNum, branch, nil
	}
	return github.ResolvePRFromCommitSHA(ctx, s.ghClient(), &s.ghState, repo, sha)
}
