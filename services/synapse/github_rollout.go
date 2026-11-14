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
	if s == "completed" {
		switch c {
		case "success", "neutral", "skipped":
			return "passed"
		case "failure", "timed_out", "cancelled", "action_required":
			return "failed"
		default:
			return ""
		}
	}
	switch s {
	case "queued", "in_progress", "waiting", "requested", "pending":
		return "pending"
	default:
		if s != "" {
			return "pending"
		}
		return ""
	}
}

func formatCIChecksActiveStep(prStatus, ciChecksJSON string) string {
	if !strings.EqualFold(strings.TrimSpace(prStatus), "open") {
		return ""
	}
	trimmedJSON := strings.TrimSpace(ciChecksJSON)
	if trimmedJSON == "" || trimmedJSON == "{}" {
		return ""
	}
	var checks map[string]string
	if err := json.Unmarshal([]byte(trimmedJSON), &checks); err != nil || len(checks) == 0 {
		return ""
	}
	var total, passed, failed int
	for _, state := range checks {
		s := strings.ToLower(strings.TrimSpace(state))
		switch s {
		case "passed":
			passed++
			total++
		case "failed":
			failed++
			total++
		default:
			if s != "" {
				total++
			}
		}
	}
	if failed > 0 || total == 0 {
		return ""
	}
	return fmt.Sprintf("Waiting on CI (%d/%d passed)", passed, total)
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
		case "passed":
			passed++
			total++
		case "failed":
			failed++
			total++
		default:
			if s != "" {
				total++
			}
		}

		if svc, ok := buildJobService(checkName); ok {
			matchedServices[svc] = struct{}{}
			if s == "passed" {
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

func resolveRolloutCandidateJobs(meta map[string]any, checkJobs []string) []string {
	jobs := extractJobsFromMetadata(meta)
	startedJobs := extractStringSliceFromMetadata(meta, "started_jobs")
	deployedJobs := extractStringSliceFromMetadata(meta, "deployed_jobs")

	combined := append(append(append([]string(nil), jobs...), startedJobs...), deployedJobs...)
	if candidateJobs := dedupeTrimmedStrings(combined); len(candidateJobs) > 0 {
		sort.Strings(candidateJobs)
		return candidateJobs
	}
	return checkJobs
}

func formatSingleJobRolloutState(job, cleanStatus string, deployedSet, startedSet map[string]bool, jobStates map[string]string, passedBuildJobs map[string]bool, hasStartedJobs bool) string {
	jKey := strings.ToLower(strings.TrimSpace(job))
	switch {
	case deployedSet[jKey]:
		return fmt.Sprintf("`%s`: ✅ healthy", job)
	case startedSet[jKey] && strings.EqualFold(jobStates[jKey], "verifying_health"):
		return fmt.Sprintf("`%s`: 🔄 verifying health", job)
	case startedSet[jKey]:
		return fmt.Sprintf("`%s`: 🔄 starting", job)
	case (!hasStartedJobs && cleanStatus == "deploying") || passedBuildJobs[jKey]:
		return fmt.Sprintf("`%s`: 🔄 starting", job)
	default:
		return fmt.Sprintf("`%s`: ⏳ building image", job)
	}
}

func formatPerJobRolloutStatus(allJobs []string, cleanStatus string, startedJobs, deployedJobs []string, jobStates map[string]string, passedBuildJobs map[string]bool) string {
	deployedSet := make(map[string]bool, len(deployedJobs))
	for _, j := range deployedJobs {
		deployedSet[strings.ToLower(strings.TrimSpace(j))] = true
	}
	startedSet := make(map[string]bool, len(startedJobs))
	for _, j := range startedJobs {
		startedSet[strings.ToLower(strings.TrimSpace(j))] = true
	}
	hasStartedJobs := len(startedJobs) > 0

	lines := make([]string, 0, len(allJobs))
	for _, j := range allJobs {
		lines = append(lines, formatSingleJobRolloutState(j, cleanStatus, deployedSet, startedSet, jobStates, passedBuildJobs, hasStartedJobs))
	}
	return strings.Join(lines, "\n")
}

func formatPRWidgetActiveStep(prStatus, ciChecksJSON, metadataJSON string) string {
	cleanStatus := strings.ToLower(strings.TrimSpace(prStatus))
	if cleanStatus == "open" {
		return formatCIChecksActiveStep(prStatus, ciChecksJSON)
	}
	if cleanStatus != "merged" && cleanStatus != "synced" && cleanStatus != "deploying" {
		return ""
	}

	var meta map[string]any
	if trimmedMeta := strings.TrimSpace(metadataJSON); trimmedMeta != "" && trimmedMeta != "{}" {
		if err := json.Unmarshal([]byte(trimmedMeta), &meta); err != nil {
			meta = nil
		}
	}

	checks := extractChecksFromJSONOrMetadata(ciChecksJSON, meta)
	checkJobs, passedBuildJobs, total, passed, failed := analyzeBuildChecks(checks)

	if failed > 0 {
		return ""
	}
	if rollout, ok := formatKustomizationRolloutStatus(cleanStatus, meta); ok {
		return rollout
	}
	allJobs := resolveRolloutCandidateJobs(meta, checkJobs)
	if len(allJobs) > 0 {
		startedJobs := extractStringSliceFromMetadata(meta, "started_jobs")
		deployedJobs := extractStringSliceFromMetadata(meta, "deployed_jobs")
		jobStates := extractStringMapFromMetadata(meta, "job_states")
		return formatPerJobRolloutStatus(allJobs, cleanStatus, startedJobs, deployedJobs, jobStates, passedBuildJobs)
	}

	if (cleanStatus == "merged" || cleanStatus == "synced") && total > 0 && failed == 0 {
		return fmt.Sprintf("Building container images (%d/%d completed)", passed, total)
	}

	return ""
}

func prCheckKey(repo string, prNumber int) string {
	return fmt.Sprintf("%s#%d", normalizeRepo(repo), prNumber)
}

func (s *RouterServer) recordFailedCISHA(repo string, prNumber int, headSHA string) bool {
	headSHA = strings.TrimSpace(headSHA)
	if s == nil || prNumber <= 0 || headSHA == "" || isZeroSHA(headSHA) {
		return false
	}
	prev, loaded := s.failedCheckSHAs.Swap(prCheckKey(repo, prNumber), headSHA)
	return !loaded || !strings.EqualFold(fmt.Sprint(prev), headSHA)
}

func (s *RouterServer) clearFailedCISHA(repo string, prNumber int) {
	if s == nil || prNumber <= 0 {
		return
	}
	key := prCheckKey(repo, prNumber)
	s.failedCheckSHAs.Delete(key)
	s.nonRoboRevFailedSHAs.Delete(key)
	s.cancelledCheckSHAs.Delete(key)
	s.failedCheckRuns.Delete(key)
}

type failedCheckState struct {
	mu           sync.Mutex
	headSHA      string
	everFailed   map[string]bool
	activeFailed map[string]bool
}

func (s *RouterServer) recordFailedCheckRun(repo string, prNumber int, headSHA, checkName string) {
	normHeadSHA := strings.TrimSpace(headSHA)
	normCheck := strings.ToLower(strings.TrimSpace(checkName))
	if s == nil || prNumber <= 0 || normHeadSHA == "" || isZeroSHA(normHeadSHA) || normCheck == "" {
		return
	}
	key := prCheckKey(repo, prNumber)
	val, _ := s.failedCheckRuns.LoadOrStore(key, &failedCheckState{
		everFailed:   make(map[string]bool),
		activeFailed: make(map[string]bool),
	})
	entry, ok := val.(*failedCheckState)
	if !ok || entry == nil {
		return
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if !strings.EqualFold(entry.headSHA, normHeadSHA) {
		entry.headSHA = normHeadSHA
		entry.everFailed = make(map[string]bool)
		entry.activeFailed = make(map[string]bool)
	}
	entry.everFailed[normCheck] = true
	entry.activeFailed[normCheck] = true
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
	normCheck := strings.ToLower(strings.TrimSpace(checkName))
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
		delete(entry.activeFailed, normCheck)
	}
}

func (s *RouterServer) canRecoverNonGateCheckSuccess(repo string, prNumber int, checkName, headSHA string) bool {
	normCheck := strings.ToLower(strings.TrimSpace(checkName))
	if normCheck == "" {
		return false
	}
	entry := s.loadMatchingFailedCheckState(repo, prNumber, headSHA)
	if entry == nil {
		return false
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if !strings.EqualFold(entry.headSHA, strings.TrimSpace(headSHA)) || !entry.everFailed[normCheck] {
		return false
	}
	delete(entry.activeFailed, normCheck)
	return len(entry.activeFailed) == 0
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
		if !isGateAggregatorCheck(cr.Name) && state == "failed" {
			s.failedCheckSHAs.Store(key, headSHA)
			return true
		}
		if state == "pending" {
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
	if blockedSHA, ok := s.nonRoboRevFailedSHAs.Load(prCheckKey(repo, prNumber)); ok && blockedSHA == headSHA {
		return false
	}
	comment := s.fetchLatestRoboRevComment(ctx, repo, prNumber, headSHA)
	return comment != "" && !strings.Contains(comment, "## roborev: Review Passed")
}

func (s *RouterServer) canRecoverCheckSuccess(ctx context.Context, repo string, prNumber int, checkName, headSHA string) bool {
	if isGateAggregatorCheck(checkName) {
		headSHA = strings.TrimSpace(headSHA)
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
	if s.registry == nil || (res.Conclusion != "failure" && res.Conclusion != "timed_out") {
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
