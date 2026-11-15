package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"sort"
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

func extractChecksFromJSONOrMetadata(ciChecksJSON string, meta map[string]any) map[string]string {
	var checks map[string]string
	if trimmedChecks := strings.TrimSpace(ciChecksJSON); trimmedChecks != "" && trimmedChecks != "{}" {
		if err := json.Unmarshal([]byte(trimmedChecks), &checks); err != nil {
			checks = nil
		}
	}
	if len(checks) == 0 && meta != nil {
		if rawChecks, ok := meta["ci_checks"]; ok && rawChecks != nil {
			switch c := rawChecks.(type) {
			case map[string]any:
				checks = make(map[string]string, len(c))
				for k, v := range c {
					if s, ok := v.(string); ok {
						checks[k] = s
					}
				}
			case map[string]string:
				checks = c
			}
		}
	}
	return checks
}

func analyzeBuildChecks(checks map[string]string) (checkJobs []string, passedBuildJobs map[string]bool, total, passed, failed int) {
	matchedServices := make(map[string]struct{})
	passedBuildJobs = make(map[string]bool)

	for checkName, state := range checks {
		s := strings.ToLower(strings.TrimSpace(state))
		switch s {
		case statusPassed:
			passed++
			total++
		case statusFailed:
			failed++
			total++
		default:
			if s != "" {
				total++
			}
		}

		if svc, ok := buildJobService(checkName); ok {
			matchedServices[svc] = struct{}{}
			if s == statusPassed {
				passedBuildJobs[strings.ToLower(svc)] = true
			}
		}
	}

	if len(matchedServices) > 0 {
		for svc := range matchedServices {
			checkJobs = append(checkJobs, svc)
		}
		sort.Strings(checkJobs)
		checkJobs = dedupeTrimmedStrings(checkJobs)
	}
	return checkJobs, passedBuildJobs, total, passed, failed
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

func (s *RouterServer) recordFailedCheckRun(repo string, prNumber int, headSHA, checkName string) bool {
	normHeadSHA := strings.TrimSpace(headSHA)
	normCheck := canonicalFailedCheckKey(checkName)
	if s == nil || prNumber <= 0 || normHeadSHA == "" || isZeroSHA(normHeadSHA) || normCheck == "" {
		return false
	}
	key := prCheckKey(repo, prNumber)
	val, _ := s.failedCheckRuns.LoadOrStore(key, &failedCheckState{
		everFailed:   make(map[string]bool),
		activeFailed: make(map[string]bool),
	})
	entry, ok := val.(*failedCheckState)
	if !ok || entry == nil {
		return false
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
		wasActive := entry.roboRevActiveFailed
		entry.everFailed["roborev"] = true
		entry.roboRevActiveFailed = true
		return hadPriorFailure && !wasActive
	}
	wasActive := entry.activeFailed[normCheck]
	entry.everFailed[normCheck] = true
	entry.activeFailed[normCheck] = true
	return hadPriorFailure && !wasActive
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
		return false
	}
	hasPending := false
	for _, cr := range checkRuns.CheckRuns {
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
	regTargetID, _, regHeadSHA, _, err := s.registry.ResolvePRByNumber(ctx, res.Repo, res.PRNumber)
	if err == nil && regTargetID != "" {
		res.TargetID = firstNonEmpty(res.TargetID, regTargetID)
	}
	if err == nil && regHeadSHA != "" && !strings.EqualFold(regHeadSHA, strings.TrimSpace(res.HeadSHA)) {
		log.Printf("[synapse] [github] ignoring stale CI failure on %s#%d for superseded commit %s (current head_sha=%s)", res.Repo, res.PRNumber, res.HeadSHA, regHeadSHA)
		return true
	}
	return false
}
