package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

const (
	statusFailed       = "failed"
	statusPassed       = "passed"
	statusPending      = "pending"
	ghStatusCompleted  = "completed"
	conclusionSuccess  = "success"
	conclusionFailure  = "failure"
	conclusionTimedOut = "timed_out"
)

func isRoboRevCheck(checkName string) bool {
	trimmed := strings.TrimSpace(checkName)
	return strings.EqualFold(trimmed, "roborev") || strings.EqualFold(trimmed, "RoboRev Code Review")
}

func isGateAggregatorCheck(checkName string) bool {
	lower := strings.ToLower(strings.TrimSpace(checkName))
	return lower == "ci gate" || strings.HasSuffix(lower, " gate")
}

func classifyCheckState(status, conclusion string) string {
	s := strings.ToLower(strings.TrimSpace(status))
	c := strings.ToLower(strings.TrimSpace(conclusion))
	if s == ghStatusCompleted {
		switch c {
		case conclusionSuccess, "neutral", "skipped":
			return statusPassed
		case conclusionFailure, conclusionTimedOut, "cancelled", "action_required":
			return statusFailed
		default:
			return ""
		}
	}
	switch s {
	case "queued", "in_progress", "waiting", "requested", statusPending:
		return statusPending
	default:
		if s != "" {
			return statusPending
		}
		return ""
	}
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
	return fmt.Sprintf("%s#%d", normalizeRepo(repo), prNumber)
}

func (s *RouterServer) recordFailedCISHA(repo string, prNumber int, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	normRepo := normalizeRepo(repo)
	if s == nil || normRepo == "" || prNumber <= 0 || headSHA == "" || isZeroSHA(headSHA) {
		return false
	}
	prev, loaded := s.failedCheckSHAs.Swap(prCheckKey(normRepo, prNumber), headSHA)
	return !loaded || !strings.EqualFold(fmt.Sprint(prev), headSHA)
}

func (s *RouterServer) clearFailedCISHA(repo string, prNumber int) {
	normRepo := normalizeRepo(repo)
	if s == nil || normRepo == "" || prNumber <= 0 {
		return
	}
	key := prCheckKey(normRepo, prNumber)
	s.failedCheckSHAs.Delete(key)
	s.nonRoboRevFailedSHAs.Delete(key)
	s.cancelledCheckSHAs.Delete(key)
	s.failedCheckRuns.Delete(key)
	s.collaboratorAlertedSHAs.Delete(key)
	s.collaboratorConflictSHAs.Delete(key)
	s.conflictPublishedSHAs.Delete(key)
}

func canonicalFailedCheckKey(checkName string) string {
	if isRoboRevCheck(checkName) {
		return "roborev"
	}
	return strings.ToLower(strings.TrimSpace(checkName))
}

type failedCheckState struct {
	mu                  sync.Mutex
	headSHA             string
	everFailed          map[string]bool
	activeFailed        map[string]bool
	roboRevActiveFailed bool
}

func (s *RouterServer) recordFailedCheckRunDetailed(repo string, prNumber int, headSHA, checkName string) (emitAdditional bool, wasActive bool, hadPriorEverFailed bool) {
	normHeadSHA := strings.TrimSpace(headSHA)
	normCheck := canonicalFailedCheckKey(checkName)
	if s == nil || prNumber <= 0 || normHeadSHA == "" || isZeroSHA(normHeadSHA) || normCheck == "" {
		return false, false, false
	}
	key := prCheckKey(repo, prNumber)
	val, _ := s.failedCheckRuns.LoadOrStore(key, &failedCheckState{
		everFailed:   make(map[string]bool),
		activeFailed: make(map[string]bool),
	})
	entry, ok := val.(*failedCheckState)
	if !ok || entry == nil {
		return false, false, false
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if !strings.EqualFold(entry.headSHA, normHeadSHA) {
		entry.headSHA = normHeadSHA
		entry.everFailed = make(map[string]bool)
		entry.activeFailed = make(map[string]bool)
		entry.roboRevActiveFailed = false
	}
	hadPriorFailure := len(entry.everFailed) > 0 || entry.roboRevActiveFailed
	if isRoboRevCheck(checkName) {
		wasActive = entry.roboRevActiveFailed
		hadPriorEverFailed = entry.everFailed["roborev"]
		entry.everFailed["roborev"] = true
		entry.roboRevActiveFailed = true
		return hadPriorFailure && !wasActive, wasActive, hadPriorEverFailed
	}
	wasActive = entry.activeFailed[normCheck]
	hadPriorEverFailed = entry.everFailed[normCheck]
	entry.everFailed[normCheck] = true
	entry.activeFailed[normCheck] = true
	return hadPriorFailure && !wasActive, wasActive, hadPriorEverFailed
}

func (s *RouterServer) rollbackFailedCheckRun(repo string, prNumber int, headSHA, checkName string, wasActive, hadPriorEverFailed bool) {
	normCheck := canonicalFailedCheckKey(checkName)
	if s == nil || prNumber <= 0 || normCheck == "" || wasActive {
		return
	}
	entry := s.loadMatchingFailedCheckState(repo, prNumber, headSHA)
	if entry == nil {
		return
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if !strings.EqualFold(entry.headSHA, strings.TrimSpace(headSHA)) {
		return
	}
	if isRoboRevCheck(checkName) {
		entry.roboRevActiveFailed = false
		if !hadPriorEverFailed {
			delete(entry.everFailed, "roborev")
		}
		return
	}
	delete(entry.activeFailed, normCheck)
	if !hadPriorEverFailed {
		delete(entry.everFailed, normCheck)
	}
	if len(entry.activeFailed) == 0 {
		s.nonRoboRevFailedSHAs.Delete(prCheckKey(repo, prNumber))
	}
}

func (s *RouterServer) loadMatchingFailedCheckState(repo string, prNumber int, headSHA string) *failedCheckState {
	normHeadSHA := strings.TrimSpace(headSHA)
	if s == nil || prNumber <= 0 || normHeadSHA == "" || isZeroSHA(normHeadSHA) {
		return nil
	}
	val, ok := s.failedCheckRuns.Load(prCheckKey(repo, prNumber))
	if !ok {
		return nil
	}
	entry, ok := val.(*failedCheckState)
	if !ok || entry == nil {
		return nil
	}
	return entry
}

func (s *RouterServer) clearActiveFailedCheckRun(repo string, prNumber int, headSHA, checkName string) {
	normCheck := canonicalFailedCheckKey(checkName)
	if normCheck == "" {
		return
	}
	entry := s.loadMatchingFailedCheckState(repo, prNumber, headSHA)
	if entry == nil {
		return
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if strings.EqualFold(entry.headSHA, strings.TrimSpace(headSHA)) {
		if isRoboRevCheck(checkName) {
			entry.roboRevActiveFailed = false
		} else {
			delete(entry.activeFailed, normCheck)
		}
	}
}

func (s *RouterServer) hasActiveRoboRevFailure(repo string, prNumber int, headSHA string) bool {
	normHeadSHA := strings.TrimSpace(headSHA)
	if s == nil || prNumber <= 0 || normHeadSHA == "" || isZeroSHA(normHeadSHA) {
		return false
	}
	entry := s.loadMatchingFailedCheckState(repo, prNumber, normHeadSHA)
	if entry == nil {
		return false
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if strings.EqualFold(entry.headSHA, normHeadSHA) {
		return entry.roboRevActiveFailed
	}
	return false
}

func (s *RouterServer) canRecoverNonGateCheckSuccess(repo string, prNumber int, checkName, headSHA string) bool {
	normCheck := canonicalFailedCheckKey(checkName)
	if normCheck == "" {
		return false
	}
	entry := s.loadMatchingFailedCheckState(repo, prNumber, headSHA)
	if entry == nil {
		return false
	}
	entry.mu.Lock()
	if !strings.EqualFold(entry.headSHA, strings.TrimSpace(headSHA)) || !entry.everFailed[normCheck] {
		entry.mu.Unlock()
		return false
	}
	delete(entry.activeFailed, normCheck)
	allNonRoboRevCleared := len(entry.activeFailed) == 0
	roboRevActive := entry.roboRevActiveFailed
	entry.mu.Unlock()

	if allNonRoboRevCleared {
		s.nonRoboRevFailedSHAs.Delete(prCheckKey(repo, prNumber))
		return !roboRevActive
	}
	return false
}

func (s *RouterServer) markUpdatedForCurrentMain(repo string, prNumber int) {
	if s == nil || prNumber <= 0 {
		return
	}
	s.updatedForCurrentMain.Store(prCheckKey(repo, prNumber), true)
}

func (s *RouterServer) wasUpdatedForCurrentMain(repo string, prNumber int) bool {
	if s == nil || prNumber <= 0 {
		return false
	}
	v, ok := s.updatedForCurrentMain.Load(prCheckKey(repo, prNumber))
	return ok && v == true
}

func (s *RouterServer) clearUpdatedPRsForRepo(repo string) {
	if s == nil {
		return
	}
	prefix := normalizeRepo(repo) + "#"
	s.updatedForCurrentMain.Range(func(k, _ any) bool {
		if keyStr, ok := k.(string); ok && strings.HasPrefix(keyStr, prefix) {
			s.updatedForCurrentMain.Delete(k)
		}
		return true
	})
}

func (s *RouterServer) clearUpdatedPRsForMainCommit(repo, headSHA string) {
	normRepo := strings.ToLower(normalizeRepo(repo))
	cleanSHA := strings.ToLower(strings.TrimSpace(headSHA))
	if normRepo != "" && cleanSHA != "" && !isZeroSHA(cleanSHA) {
		if _, loaded := s.lastMainPushSHA.LoadOrStore(normRepo+"@"+cleanSHA, true); loaded {
			return
		}
	}
	s.clearUpdatedPRsForRepo(repo)
}

func (s *RouterServer) isCachedFailedCISHA(repo string, prNumber int, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	normRepo := normalizeRepo(repo)
	if s == nil || normRepo == "" || prNumber <= 0 || headSHA == "" || isZeroSHA(headSHA) {
		return false
	}
	key := prCheckKey(normRepo, prNumber)
	for _, m := range []*sync.Map{&s.failedCheckSHAs, &s.nonRoboRevFailedSHAs, &s.cancelledCheckSHAs} {
		if v, ok := m.Load(key); ok && strings.EqualFold(fmt.Sprint(v), headSHA) {
			return true
		}
	}
	return false
}

func (s *RouterServer) hasFailedCIForSHA(ctx context.Context, repo string, prNumber int, headSHA string) bool {
	if s.isCachedFailedCISHA(repo, prNumber, headSHA) {
		return true
	}
	headSHA = strings.TrimSpace(headSHA)
	normRepo := normalizeRepo(repo)
	if s == nil || normRepo == "" || prNumber <= 0 || headSHA == "" || isZeroSHA(headSHA) {
		return false
	}
	key := prCheckKey(normRepo, prNumber)
	var checkRuns GitHubCheckRunsResponse
	path := fmt.Sprintf("/repos/%s/commits/%s/check-runs?per_page=100", normRepo, url.PathEscape(headSHA))
	if err := s.getGitHubJSON(ctx, path, 2<<20, &checkRuns); err != nil {
		return !isGitHubNotFound(err)
	}
	hasPending := false
	for _, cr := range checkRuns.CheckRuns {
		if isBuildMetadataCheck(cr.Name) || isDependabotAppCheck(cr.App.Slug) {
			continue
		}
		state := classifyCheckState(cr.Status, cr.Conclusion)
		if !isGateAggregatorCheck(cr.Name) && state == statusFailed {
			s.failedCheckSHAs.Store(key, headSHA)
			return true
		}
		if state == statusPending {
			hasPending = true
		}
	}
	return hasPending
}

func (s *RouterServer) hasCancelledCheckForSHA(repo string, prNumber int, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if s == nil || prNumber <= 0 || headSHA == "" {
		return false
	}
	cancelledSHA, ok := s.cancelledCheckSHAs.Load(prCheckKey(repo, prNumber))
	return ok && cancelledSHA == headSHA
}

func (s *RouterServer) canRecoverRoboRevSuccess(ctx context.Context, repo string, prNumber int, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if s == nil || prNumber <= 0 || headSHA == "" {
		return false
	}
	s.clearActiveFailedCheckRun(repo, prNumber, headSHA, "roborev")
	if blockedSHA, ok := s.nonRoboRevFailedSHAs.Load(prCheckKey(repo, prNumber)); ok && blockedSHA == headSHA {
		return false
	}
	if entry := s.loadMatchingFailedCheckState(repo, prNumber, headSHA); entry != nil {
		entry.mu.Lock()
		match := strings.EqualFold(entry.headSHA, headSHA) && entry.everFailed["roborev"]
		entry.mu.Unlock()
		if match {
			return true
		}
	}
	comment := s.fetchLatestRoboRevComment(ctx, repo, prNumber, headSHA)
	return comment != "" && !strings.Contains(comment, "## roborev: Review Passed")
}

func (s *RouterServer) canRecoverCheckSuccess(ctx context.Context, repo string, prNumber int, checkName, headSHA string) bool {
	if isGateAggregatorCheck(checkName) {
		headSHA = strings.TrimSpace(headSHA)
		if s.hasActiveRoboRevFailure(repo, prNumber, headSHA) {
			return false
		}
		if failedSHA, ok := s.nonRoboRevFailedSHAs.Load(prCheckKey(repo, prNumber)); ok && headSHA != "" && failedSHA == headSHA {
			return true
		}
		return s.hasCancelledCheckForSHA(repo, prNumber, headSHA)
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
		if !strings.Contains(body, "## roborev:") {
			continue
		}
		if shortSHA != "" && !strings.Contains(body, "`"+shortSHA) && !strings.Contains(body, headSHA) {
			continue
		}
		return truncatePromptRunes(body, maxRoboRevCommentRunes)
	}
	return ""
}

func formatCollaboratorPRTarget(repo string, prNumber int, prTitle, branch, headSHA string) string {
	normRepo := normalizeRepo(repo)
	cleanTitle := strings.TrimSpace(prTitle)
	cleanBranch := strings.TrimPrefix(strings.TrimSpace(branch), "refs/heads/")
	shortSHA := strings.TrimSpace(headSHA)
	if len(shortSHA) > 7 {
		shortSHA = shortSHA[:7]
	}

	prLink := fmt.Sprintf("[%s#%d](https://github.com/%s/pull/%d)", normRepo, prNumber, normRepo, prNumber)
	if normRepo == "" {
		prLink = fmt.Sprintf("PR #%d", prNumber)
	}

	var meta []string
	if cleanTitle != "" {
		meta = append(meta, fmt.Sprintf("%q", cleanTitle))
	}
	if cleanBranch != "" {
		meta = append(meta, "branch: "+cleanBranch)
	}
	if shortSHA != "" {
		meta = append(meta, "commit: "+shortSHA)
	}
	if len(meta) > 0 {
		return fmt.Sprintf("%s (%s)", prLink, strings.Join(meta, ", "))
	}
	return prLink
}

func formatCollaboratorCIFailureMessage(mention, repo string, prNumber int, prTitle, branch, headSHA, checkName, checkURL string) string {
	cleanMention := strings.TrimSpace(mention)
	cleanCheckName := strings.TrimSpace(checkName)
	target := formatCollaboratorPRTarget(repo, prNumber, prTitle, branch, headSHA)

	var header strings.Builder
	if cleanMention != "" {
		header.WriteString(cleanMention)
		header.WriteString(" ")
	}
	if cleanCheckName != "" {
		fmt.Fprintf(&header, "CI check %q failed for %s.", cleanCheckName, target)
	} else {
		fmt.Fprintf(&header, "CI check failed for %s.", target)
	}

	if cleanURL := strings.TrimSpace(checkURL); cleanURL != "" {
		fmt.Fprintf(&header, "\nCheck URL: %s", cleanURL)
	}

	return header.String()
}

func formatCollaboratorConflictMessage(mention, repo string, prNumber int, prTitle, branch, headSHA string) string {
	target := formatCollaboratorPRTarget(repo, prNumber, prTitle, branch, headSHA)
	cleanMention := strings.TrimSpace(mention)
	if cleanMention != "" {
		return fmt.Sprintf("%s Merge conflict with `main` detected on %s — automatic branch update could not resolve conflicts. Please rebase or merge `origin/main` and push.", cleanMention, target)
	}
	return fmt.Sprintf("Merge conflict with `main` detected on %s — automatic branch update could not resolve conflicts. Please rebase or merge `origin/main` and push.", target)
}

func recordCollaboratorSHAAlert(m *sync.Map, repo string, prNumber int, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if m == nil || prNumber <= 0 || headSHA == "" || isZeroSHA(headSHA) {
		return false
	}
	prev, loaded := m.Swap(prCheckKey(repo, prNumber), headSHA)
	return !loaded || !strings.EqualFold(fmt.Sprint(prev), headSHA)
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

func (s *RouterServer) dispatchCollaboratorDM(ctx context.Context, dedup *sync.Map, repo string, prNumber int, sha, channelID, msg string) error {
	normSHA := strings.TrimSpace(sha)
	if s == nil || s.dispatcher == nil || channelID == "" || normSHA == "" || isZeroSHA(normSHA) {
		return nil
	}
	if !recordCollaboratorSHAAlert(dedup, repo, prNumber, normSHA) {
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
	if err != nil || !ok {
		return err
	}
	branch := firstNonEmpty(resolvedPR.Head.Ref, pr.Head.Ref)
	sha := firstNonEmpty(resolvedPR.Head.SHA, pr.Head.SHA)
	msg := formatCollaboratorConflictMessage(mention, repo, resolvedPR.Number, resolvedPR.Title, branch, sha)
	return s.dispatchCollaboratorDM(ctx, &s.collaboratorConflictSHAs, repo, resolvedPR.Number, sha, channelID, msg)
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
	msg := formatCollaboratorCIFailureMessage(mention, res.Repo, resolvedPR.Number, resolvedPR.Title, branch, sha, res.CheckName, checkURL)
	return s.dispatchCollaboratorDM(ctx, &s.collaboratorAlertedSHAs, res.Repo, resolvedPR.Number, sha, channelID, msg)
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
	if res == nil || res.Event != ghEventCheckRun || res.PRNumber <= 0 || res.HeadSHA == "" || isGateAggregatorCheck(res.CheckName) || isBuildMetadataCheck(res.CheckName) {
		return false, false, false
	}
	if !isRoboRevCheck(res.CheckName) {
		s.nonRoboRevFailedSHAs.Store(prCheckKey(res.Repo, res.PRNumber), strings.TrimSpace(res.HeadSHA))
	}
	return s.recordFailedCheckRunDetailed(res.Repo, res.PRNumber, res.HeadSHA, res.CheckName)
}

func (s *RouterServer) isPRRegistryCIFailed(ctx context.Context, repo string, prNumber int) bool {
	if s == nil || s.registry == nil || prNumber <= 0 || normalizeRepo(repo) == "" {
		return false
	}
	lifecycle, err := s.registry.LoadPRBuildLifecycle(ctx, repo, prNumber)
	return err == nil && strings.EqualFold(strings.TrimSpace(lifecycle.Status), prCIFailed)
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
				s.nonRoboRevFailedSHAs.Store(prCheckKey(res.Repo, res.PRNumber), strings.TrimSpace(res.HeadSHA))
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
	var fetched GitHubPullRequest
	path := fmt.Sprintf("/repos/%s/pulls/%d", normalizeRepo(res.Repo), res.PRNumber)
	if err := s.getGitHubJSON(ctx, path, 2<<20, &fetched); err != nil {
		return pr, false, err
	}
	if fetched.Merged || (fetched.State != "" && !strings.EqualFold(fetched.State, "open")) {
		return pr, false, nil
	}
	if res.HeadSHA != "" && fetched.Head.SHA != "" && !strings.EqualFold(fetched.Head.SHA, strings.TrimSpace(res.HeadSHA)) {
		return pr, false, nil
	}
	return syncPRFallbackFields(res, fetched), true, nil
}

func (s *RouterServer) recordCommitSHAPR(repo, sha string, prNumber int, branch string) {
	normRepo := strings.ToLower(normalizeRepo(repo))
	normSHA := strings.ToLower(strings.TrimSpace(sha))
	if s == nil || normRepo == "" || normSHA == "" || isZeroSHA(normSHA) || prNumber <= 0 {
		return
	}
	s.commitSHAPRs.Store(normRepo+"@"+normSHA, GitHubCheckRunPR{
		Number: prNumber,
		Head: GitHubPRRef{
			Ref: strings.TrimSpace(branch),
			SHA: normSHA,
		},
	})
}

func (s *RouterServer) lookupCommitSHAPR(repo, sha string) (int, string, bool) {
	if s == nil {
		return 0, "", false
	}
	normRepo := strings.ToLower(normalizeRepo(repo))
	normSHA := strings.ToLower(strings.TrimSpace(sha))
	if normRepo == "" || normSHA == "" || isZeroSHA(normSHA) {
		return 0, "", false
	}
	v, ok := s.commitSHAPRs.Load(normRepo + "@" + normSHA)
	if !ok {
		return 0, "", false
	}
	item, ok := v.(GitHubCheckRunPR)
	if !ok || item.Number <= 0 {
		return 0, "", false
	}
	return item.Number, item.Head.Ref, true
}

func (s *RouterServer) resolvePRFromCommitSHA(ctx context.Context, repo, sha string) (int, string, error) {
	repo = normalizeRepo(repo)
	if repo == "" || sha == "" || isZeroSHA(sha) {
		return 0, "", fmt.Errorf("invalid repo or sha for pr resolution")
	}
	if prNum, branch, ok := s.lookupCommitSHAPR(repo, sha); ok {
		return prNum, branch, nil
	}
	var prs []GitHubCommitPRItem
	if err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/commits/%s/pulls", repo, url.PathEscape(sha)), 1<<20, &prs); err != nil {
		return 0, "", err
	}
	if len(prs) == 0 {
		var openPRs []GitHubCommitPRItem
		if err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/pulls?state=open&per_page=100", repo), 2<<20, &openPRs); err == nil {
			for _, item := range openPRs {
				if strings.EqualFold(strings.TrimSpace(item.Head.SHA), strings.TrimSpace(sha)) {
					prs = append(prs, item)
				}
			}
		}
	}
	if len(prs) == 0 {
		return 0, "", fmt.Errorf("no pull requests associated with commit %s", sha)
	}
	selected := prs[0]
	for _, item := range prs[1:] {
		if (selected.State != ghActionClosed && item.State == ghActionClosed) || (selected.State == item.State && item.Number > selected.Number) {
			selected = item
		}
	}
	s.recordCommitSHAPR(repo, sha, selected.Number, selected.Head.Ref)
	return selected.Number, selected.Head.Ref, nil
}
