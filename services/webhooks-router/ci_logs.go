package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	actionsJobIDRegex = regexp.MustCompile(`/actions/runs/[0-9]+/job/([0-9]+)`)
	ghaTimestampRegex = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9:.]+Z ?`)
	ansiEscapeRegex   = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)
	goCoordinateRegex = regexp.MustCompile(`\.go:[0-9]+`)

	ciPlumbingPrefixes = []string{
		"##[group]",
		"##[endgroup]",
		"[command]/usr/bin/git",
	}

	ciHighSignalPatterns = []string{
		"--- FAIL:",
		"FAIL\t",
		"panic:",
		"##[error]",
		"Error:",
		"🚨",
	}
)

func cleanCILogLine(line string) string {
	line = ansiEscapeRegex.ReplaceAllString(line, "")
	line = ghaTimestampRegex.ReplaceAllString(line, "")
	return strings.TrimSpace(line)
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(s, prefix) {
			return true
		}
	}
	return false
}

func containsAnySubstr(s string, substrs []string) bool {
	for _, sub := range substrs {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func isGoLinterCoordinate(line string) bool {
	return goCoordinateRegex.MatchString(line)
}

func isHighSignalLine(line string) bool {
	if containsAnySubstr(line, ciHighSignalPatterns) {
		return true
	}
	return isGoLinterCoordinate(line)
}

func extractActionsJobID(checkURL string) int64 {
	m := actionsJobIDRegex.FindStringSubmatch(checkURL)
	if len(m) < 2 {
		return 0
	}
	id, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil || id <= 0 {
		return 0
	}
	return id
}

func needsCIJobLogFetch(details string) bool {
	trimmed := strings.TrimSpace(details)
	if strings.Contains(trimmed, "\n") {
		return false
	}
	return len(trimmed) < 80
}

func ExtractCIFailureDetails(rawLog string) string {
	normalized := strings.ReplaceAll(rawLog, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")
	rawLines := strings.Split(normalized, "\n")

	var cleanedLines []string
	for _, rawLine := range rawLines {
		cleaned := cleanCILogLine(rawLine)
		if cleaned == "Post job cleanup." {
			break
		}
		if cleaned == "" || hasAnyPrefix(cleaned, ciPlumbingPrefixes) {
			continue
		}
		cleanedLines = append(cleanedLines, cleaned)
	}

	if len(cleanedLines) == 0 {
		return ""
	}

	tailStart := len(cleanedLines) - 25
	if tailStart < 0 {
		tailStart = 0
	}

	seen := make(map[string]struct{}, len(cleanedLines))
	var result []string
	for i, line := range cleanedLines {
		if i < tailStart && !isHighSignalLine(line) {
			continue
		}
		if _, exists := seen[line]; exists {
			continue
		}
		seen[line] = struct{}{}
		result = append(result, line)
	}

	return strings.Join(result, "\n")
}

func (s *RouterServer) fetchCheckJobFailureLogs(ctx context.Context, repo string, jobID int64) string {
	if s == nil || s.httpClient == nil {
		return ""
	}
	cleanRepo := normalizeRepo(repo)
	if cleanRepo == "" || jobID <= 0 {
		return ""
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	apiURL := firstNonEmpty(s.cfg.GitHubAPIURL, "https://api.github.com")
	reqURL := fmt.Sprintf("%s/repos/%s/actions/jobs/%d/logs", apiURL, cleanRepo, jobID)
	req, err := http.NewRequestWithContext(timeoutCtx, http.MethodGet, reqURL, nil)
	if err != nil {
		return ""
	}
	if s.cfg.GitHubToken != "" {
		req.Header.Set("Authorization", "token "+s.cfg.GitHubToken)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ""
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return ""
	}
	if len(body) > 256<<10 {
		body = body[len(body)-(256<<10):]
	}
	return ExtractCIFailureDetails(string(body))
}

func (s *RouterServer) fetchLatestRoboRevComment(ctx context.Context, repo string, prNumber int, headSHA string) string {
	repo = normalizeRepo(repo)
	if s == nil || repo == "" || prNumber <= 0 {
		return ""
	}
	var comments []GitHubIssueComment
	path := fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100", repo, prNumber)
	if err := s.getGitHubJSON(ctx, path, 2<<20, &comments); err != nil {
		log.Printf("[webhooks-router] [github] warning fetching roborev comments for %s#%d: %v", repo, prNumber, err)
		return ""
	}
	shortSHA := strings.TrimSpace(headSHA)
	if len(shortSHA) > 7 {
		shortSHA = shortSHA[:7]
	}
	for i := len(comments) - 1; i >= 0; i-- {
		body := strings.TrimSpace(comments[i].Body)
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

func (s *RouterServer) dispatchCIFailurePrompt(ctx context.Context, targetID, checkName, repo, headSHA string, prNum int, details, checkURL string, branchOpt ...string) error {
	if prNum > 0 && s.registry != nil {
		if _, isPg := s.registry.(*PostgresPRRegistry); isPg {
			banner := fmt.Sprintf("CI check %q failed", checkName)
			s.setPRWidgetState(ctx, repo, prNum, "", banner)
			if err := s.SyncPRWidget(ctx, repo, prNum); err != nil {
				log.Printf("[webhooks-router] [widget] warning syncing ci_failed widget: %v", err)
			}
		}
	}
	var extraParts []string
	if len(branchOpt) > 0 {
		if cleanBranch := strings.TrimPrefix(strings.TrimSpace(branchOpt[0]), "refs/heads/"); cleanBranch != "" {
			extraParts = append(extraParts, "Branch: "+cleanBranch)
		}
	}
	if cleanURL := strings.TrimSpace(checkURL); cleanURL != "" {
		extraParts = append(extraParts, "Check URL: "+cleanURL)
	}
	extraLine := strings.Join(extraParts, "\n")

	directive := "Please investigate and fix the failure by inspecting the failing logs, diagnosing the issue, verifying with local checks, and pushing a fix."
	if isRoboRevCheck(checkName) {
		directive = fmt.Sprintf("Please critically evaluate each RoboRev finding against codebase reality and architectural intent (RoboRev has read-only repo tools but lacks thread/build/cross-repo context: treat purely mechanical and objective findings like duplicate code, dead code, invariant violations, or concrete bugs/security issues as high-trust, while verifying subjective design suggestions or unverified caller assumptions before acting). Fix verified issues, add concise inline comments for non-obvious invariants (and consider adding general architectural principles to .roborev.toml only when a gap reflects a broad repo-wide pattern rather than a one-off fix), or—if the remaining findings are false positives or conflict with architectural invariants—run '/share/aerial/scripts/aerial-pr.sh --repo %s override-roborev %d '<technical_rebuttal>' to record your rebuttal on the PR and pass the gate.", firstNonEmpty(cleanRepoName(repo), "aerial"), prNum)
	}
	prompt := formatErrorDetailsBlock(
		fmt.Sprintf("CI check %q failed for PR #%d on %s (head commit: %s).", checkName, prNum, repo, headSHA),
		details, extraLine,
		directive,
	)
	opts := s.vanadisPromptOpts(ctx, repo, prNum)
	opts.silent = true
	return s.dispatchAlertWithPrompt(ctx, targetID, "ci_failed", "", prompt, opts)
}

func (s *RouterServer) completeCIFailureTransition(ctx context.Context, res *ResolvedGitHubEvent, details, checkURL string) error {
	if res == nil {
		return nil
	}
	if res.PRNumber > 0 && isRoboRevCheck(res.CheckName) {
		if comment := s.fetchLatestRoboRevComment(ctx, res.Repo, res.PRNumber, res.HeadSHA); comment != "" {
			details = comment
		}
	}
	if needsCIJobLogFetch(details) {
		if jobID := extractActionsJobID(checkURL); jobID > 0 {
			if logDetails := s.fetchCheckJobFailureLogs(ctx, res.Repo, jobID); logDetails != "" {
				details = logDetails
			}
		}
	}
	s.populateMissingBranch(ctx, res)
	if s.checkRemediationCapReached(ctx, res) {
		return nil
	}
	return s.dispatchCIFailurePrompt(ctx, res.TargetID, res.CheckName, res.Repo, res.HeadSHA, res.PRNumber, details, checkURL, res.Branch)
}

func (s *RouterServer) populateMissingBranch(ctx context.Context, res *ResolvedGitHubEvent) {
	if res == nil || s.registry == nil {
		return
	}
	if res.PRNumber <= 0 || strings.TrimSpace(res.Branch) != "" {
		return
	}
	_, branch, _, _, err := s.registry.ResolvePRByNumber(ctx, normalizeRepo(res.Repo), res.PRNumber)
	if err == nil && branch != "" {
		res.Branch = branch
	}
}

func (s *RouterServer) checkRemediationCapReached(ctx context.Context, res *ResolvedGitHubEvent) bool {
	if res == nil || res.PRNumber <= 0 || s.registry == nil {
		return false
	}
	failCount, fcErr := s.registry.IncrementCIFailCount(ctx, res.Repo, res.PRNumber)
	if fcErr != nil {
		log.Printf("[webhooks-router] [registry] warning incrementing ci_fail_count for %s#%d: %v", res.Repo, res.PRNumber, fcErr)
	}
	if failCount <= maxCIRemediationAttempts {
		return false
	}
	s.notifyRemediationCap(ctx, res, failCount)
	return true
}

func (s *RouterServer) notifyRemediationCap(ctx context.Context, res *ResolvedGitHubEvent, failCount int) {
	if s.registry != nil && res != nil && res.PRNumber > 0 {
		if err := s.failVanadisChunk(ctx, res.Repo, res.PRNumber, "ci_remediation_cap_reached"); err != nil {
			log.Printf("[webhooks-router] [vanadis] warning failing vanadis chunk on remediation cap for %s#%d: %v", res.Repo, res.PRNumber, err)
		}
	}
	banner := fmt.Sprintf("CI remediation cap (%d/%d) reached — awaiting human review", maxCIRemediationAttempts, maxCIRemediationAttempts)
	if res != nil {
		s.setPRWidgetState(ctx, res.Repo, res.PRNumber, "", banner)
		if err := s.SyncPRWidget(ctx, res.Repo, res.PRNumber); err != nil {
			log.Printf("[webhooks-router] [widget] sync pr widget error: %v", err)
		}
	}
	if s.dispatcher == nil || !IsValidDiscordSnowflake(res.TargetID) {
		return
	}
	msg := fmt.Sprintf("🛑 %sCI check %q failed %d times (remediation cap %d/%d reached). Automatic fix loop paused for human review.", formatDirectMessagePrefix(res.PRNumber, res.Repo), res.CheckName, failCount, maxCIRemediationAttempts, maxCIRemediationAttempts)
	if err := s.dispatcher.DispatchDirectMessage(ctx, DirectMessageRequest{
		ChannelID: res.TargetID,
		Content:   msg,
	}); err != nil {
		log.Printf("[webhooks-router] [dispatcher] error dispatching cap direct message: %v", err)
	}
}
