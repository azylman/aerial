// Package cilog fetches GitHub Actions job logs, extracts high-signal CI failure
// details, and looks up RoboRev PR review comments.
package cilog

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/githubapi"
	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/strutil"
)

const (
	maxRoboRevCommentRunes = 4000
	// maxRunFailedJobLogs caps how many failed jobs' logs a run-level failure fetches.
	maxRunFailedJobLogs = 3
)

var (
	actionsJobIDRegex = regexp.MustCompile(`/actions/runs/\d+/job/(\d+)`)
	actionsRunIDRegex = regexp.MustCompile(`/actions/runs/(\d+)`)
	// workflowConclusionRegex matches synapse's generic workflow_run details,
	// e.g. `Workflow "CI" concluded with failure`.
	workflowConclusionRegex     = regexp.MustCompile(`^Workflow ".*" concluded with [a-z_]+$`)
	checkFailureHeaderOnlyRegex = regexp.MustCompile(`^CI check ".*" failed on [^\n]+ \(commit [^\n]+\)\.(?:\nCheck URL: [^\n]+)?$`)
	ghaTimestampRegex           = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T[0-9:.]+Z ?`)
	ansiEscapeRegex             = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)
	goCoordinateRegex           = regexp.MustCompile(`\.go:\d+`)

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

// Client fetches CI logs and RoboRev comments from the GitHub API.
type Client struct {
	GitHubToken  string
	GitHubAPIURL string
	HTTPClient   *http.Client
	gitHubOwner  string
}

// NewClient builds a CI log client with the default GitHub API URL.
// gitHubOwner qualifies bare repo names.
func NewClient(gitHubToken, gitHubOwner string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{
		GitHubToken:  gitHubToken,
		GitHubAPIURL: githubapi.DefaultAPIURL,
		HTTPClient:   httpClient,
		gitHubOwner:  gitHubOwner,
	}
}

// ResolveCIFailureDetailsChecked extracts failing logs or RoboRev findings for a PR check,
// returning a retryable error when a GitHub API call fails with a transport error or retryable HTTP status (>= 500, 429, 403).
func (c *Client) ResolveCIFailureDetailsChecked(ctx context.Context, repo string, prNumber int, checkName, headSHA, checkURL, details string) (string, error) {
	if c == nil {
		return details, nil
	}
	if prNumber > 0 && prwire.IsRoboRevCheck(checkName) {
		comment, err := c.fetchLatestRoboRevCommentChecked(ctx, repo, prNumber, headSHA)
		if err != nil {
			return details, err
		}
		if comment != "" {
			return comment, nil
		}
	}
	if !NeedsCIJobLogFetch(details) {
		return details, nil
	}
	var logDetails string
	var err error
	if jobID := ExtractActionsJobID(checkURL); jobID > 0 {
		logDetails, err = c.fetchCheckJobFailureLogsChecked(ctx, repo, jobID)
	} else if runID := ExtractActionsRunID(checkURL); runID > 0 {
		logDetails, err = c.fetchRunFailureLogsChecked(ctx, repo, runID)
	} else {
		return details, nil
	}
	if err != nil {
		return details, err
	}
	if logDetails != "" {
		return logDetails, nil
	}
	return details, nil
}

func (c *Client) fetchCheckJobFailureLogsChecked(ctx context.Context, repo string, jobID int64) (string, error) {
	if c == nil || c.HTTPClient == nil {
		return "", nil
	}
	repoPath := githubapi.QualifyRepo(c.gitHubOwner, repo)
	if repoPath == "" || jobID <= 0 {
		return "", nil
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := githubapi.NewRequest(timeoutCtx, c.GitHubAPIURL, c.GitHubToken, http.MethodGet, fmt.Sprintf("/repos/%s/actions/jobs/%d/logs", repoPath, jobID), nil)
	if err != nil {
		return "", err
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if githubapi.IsRetryable(resp) {
		return "", fmt.Errorf("github api error (%d): %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	}
	if resp.StatusCode != http.StatusOK {
		return "", nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return "", err
	}
	if len(body) > 256<<10 {
		body = body[len(body)-(256<<10):]
	}
	return ExtractCIFailureDetails(string(body)), nil
}

type gitHubRunJob struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
}

type gitHubRunJobs struct {
	Jobs []gitHubRunJob `json:"jobs"`
}

// fetchRunFailureLogsChecked lists a workflow run's latest jobs and joins the
// extracted logs of up to maxRunFailedJobLogs failed or timed-out jobs, skipping
// gate aggregators. It returns "" when no failed job yields a log.
func (c *Client) fetchRunFailureLogsChecked(ctx context.Context, repo string, runID int64) (string, error) {
	if c == nil || c.HTTPClient == nil {
		return "", nil
	}
	repoPath := githubapi.QualifyRepo(c.gitHubOwner, repo)
	if repoPath == "" || runID <= 0 {
		return "", nil
	}
	jobs, err := c.listRunJobsChecked(ctx, repoPath, runID)
	if err != nil {
		return "", err
	}
	var sections []string
	fetched := 0
	for _, job := range jobs {
		if fetched >= maxRunFailedJobLogs {
			break
		}
		if !isFailedJobConclusion(job.Conclusion) || isGateAggregatorJob(job.Name) || job.ID <= 0 {
			continue
		}
		fetched++
		logDetails, err := c.fetchCheckJobFailureLogsChecked(ctx, repoPath, job.ID)
		if err != nil {
			return "", err
		}
		if logDetails == "" {
			continue
		}
		sections = append(sections, "### Job: "+job.Name+"\n"+logDetails)
	}
	return strings.Join(sections, "\n\n"), nil
}

func (c *Client) listRunJobsChecked(ctx context.Context, repoPath string, runID int64) ([]gitHubRunJob, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	path := fmt.Sprintf("/repos/%s/actions/runs/%d/jobs?filter=latest&per_page=100", repoPath, runID)
	req, err := githubapi.NewRequest(timeoutCtx, c.GitHubAPIURL, c.GitHubToken, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if githubapi.IsRetryable(resp) {
		return nil, fmt.Errorf("github api error (%d): %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil, err
	}
	var listing gitHubRunJobs
	if json.Unmarshal(body, &listing) != nil {
		return nil, nil
	}
	return listing.Jobs, nil
}

func isFailedJobConclusion(conclusion string) bool {
	return strings.EqualFold(conclusion, "failure") || strings.EqualFold(conclusion, "timed_out")
}

// isGateAggregatorJob mirrors executor.IsGateAggregatorCheck (executor imports
// cilog, so it cannot be imported here).
func isGateAggregatorJob(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	return lower == "ci gate" || strings.HasSuffix(lower, " gate")
}

type gitHubIssueComment struct {
	ID   int64  `json:"id"`
	Body string `json:"body"`
}

// fetchIssueCommentsChecked reads the first page of PR issue comments. It
// returns a retryable error for transport failures and retryable statuses, and
// no comments for any other non-200 response or malformed body.
func (c *Client) fetchIssueCommentsChecked(ctx context.Context, repo string, prNumber int) ([]gitHubIssueComment, error) {
	if c == nil || c.HTTPClient == nil {
		return nil, nil
	}
	repoPath := githubapi.QualifyRepo(c.gitHubOwner, repo)
	if repoPath == "" || prNumber <= 0 {
		return nil, nil
	}
	path := fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100", repoPath, prNumber)
	req, err := githubapi.NewRequest(ctx, c.GitHubAPIURL, c.GitHubToken, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if githubapi.IsRetryable(resp) {
		return nil, fmt.Errorf("github api error (%d): %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil
	}
	respBody, rErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if rErr != nil {
		return nil, rErr
	}
	var comments []gitHubIssueComment
	if uErr := json.Unmarshal(respBody, &comments); uErr != nil {
		return nil, nil
	}
	return comments, nil
}

// HasRoboRevOverrideChecked reports whether the PR carries the override comment
// `aerial-pr.sh override-roborev` posts for headSHA: a "## roborev-override"
// comment naming the head's 7-character short SHA.
func (c *Client) HasRoboRevOverrideChecked(ctx context.Context, repo string, prNumber int, headSHA string) (bool, error) {
	shortSHA := strings.ToLower(strings.TrimSpace(headSHA))
	if len(shortSHA) > 7 {
		shortSHA = shortSHA[:7]
	}
	if shortSHA == "" {
		return false, nil
	}
	comments, err := c.fetchIssueCommentsChecked(ctx, repo, prNumber)
	if err != nil {
		return false, err
	}
	for _, comment := range comments {
		if strings.Contains(comment.Body, "## roborev-override") && strings.Contains(comment.Body, shortSHA) {
			return true, nil
		}
	}
	return false, nil
}

func (c *Client) fetchLatestRoboRevCommentChecked(ctx context.Context, repo string, prNumber int, headSHA string) (string, error) {
	comments, err := c.fetchIssueCommentsChecked(ctx, repo, prNumber)
	if err != nil {
		return "", err
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
		return strutil.TruncateRunes(body, maxRoboRevCommentRunes), nil
	}
	return "", nil
}

// ExtractCIFailureDetails filters and extracts high-signal lines from raw CI logs.
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
		if cleaned == "" || slices.ContainsFunc(ciPlumbingPrefixes, func(p string) bool { return strings.HasPrefix(cleaned, p) }) {
			continue
		}
		cleanedLines = append(cleanedLines, cleaned)
	}

	if len(cleanedLines) == 0 {
		return ""
	}

	tailStart := max(len(cleanedLines)-25, 0)
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

func cleanCILogLine(line string) string {
	line = ghaTimestampRegex.ReplaceAllString(line, "")
	line = ansiEscapeRegex.ReplaceAllString(line, "")
	return strings.TrimSpace(line)
}

func isHighSignalLine(line string) bool {
	for _, pattern := range ciHighSignalPatterns {
		if strings.Contains(line, pattern) {
			return true
		}
	}
	return goCoordinateRegex.MatchString(line)
}

// NeedsCIJobLogFetch reports whether details require a CI job log fetch: empty,
// a bare failure conclusion, or synapse's generic workflow-level summary.
func NeedsCIJobLogFetch(details string) bool {
	trimmed := strings.TrimSpace(details)
	return trimmed == "" || strings.EqualFold(details, "failure") || strings.EqualFold(details, "timed_out") ||
		workflowConclusionRegex.MatchString(trimmed) ||
		checkFailureHeaderOnlyRegex.MatchString(trimmed)
}

// CIFailureResolution describes the coalesced failure outcome of one or more jobs in a workflow run.
type CIFailureResolution struct {
	Pending      bool
	CheckName    string
	Details      string
	FailedChecks []string
}

// IsWorkflowRunFailure reports whether checkURL and details represent a workflow-level run failure rather than a single job check-run.
func IsWorkflowRunFailure(checkURL, details string) bool {
	return workflowConclusionRegex.MatchString(strings.TrimSpace(details)) ||
		(ExtractActionsRunID(checkURL) > 0 && ExtractActionsJobID(checkURL) == 0)
}

type jobSection struct {
	name string
	body string
}

func (c *Client) fallbackCIFailureResolution(ctx context.Context, repo string, prNumber int, checkName, headSHA, checkURL, details string) (CIFailureResolution, error) {
	res := CIFailureResolution{CheckName: strings.TrimSpace(checkName)}
	resolved, err := c.ResolveCIFailureDetailsChecked(ctx, repo, prNumber, checkName, headSHA, checkURL, details)
	res.Details = resolved
	if !IsWorkflowRunFailure(checkURL, details) && res.CheckName != "" {
		res.FailedChecks = []string{res.CheckName}
	}
	return res, err
}

func hasPendingJobs(jobs []gitHubRunJob, triggerJobID int64) bool {
	for _, job := range jobs {
		if job.ID != triggerJobID && strings.TrimSpace(job.Status) != "" && !strings.EqualFold(strings.TrimSpace(job.Status), "completed") {
			return true
		}
	}
	return false
}

func filterFailedRunJobs(jobs []gitHubRunJob, triggerJobID int64) []gitHubRunJob {
	var failed []gitHubRunJob
	for _, job := range jobs {
		if !isGateAggregatorJob(job.Name) && (isFailedJobConclusion(job.Conclusion) || (triggerJobID > 0 && job.ID == triggerJobID)) {
			failed = append(failed, job)
		}
	}
	return failed
}

func (c *Client) fetchRoboRevJobBody(ctx context.Context, repo, repoPath, headSHA string, prNumber int, jobID int64) (string, error) {
	if prNumber > 0 {
		comment, cErr := c.fetchLatestRoboRevCommentChecked(ctx, repo, prNumber, headSHA)
		if cErr != nil {
			return "", cErr
		}
		if comment != "" {
			return comment, nil
		}
	}
	if jobID > 0 {
		return c.fetchCheckJobFailureLogsChecked(ctx, repoPath, jobID)
	}
	return "", nil
}

func (c *Client) fetchFailedJobSections(ctx context.Context, repo, repoPath, headSHA, details string, prNumber int, triggerJobID int64, failedJobs []gitHubRunJob) ([]jobSection, error) {
	if len(failedJobs) == 1 && triggerJobID > 0 && failedJobs[0].ID == triggerJobID && !prwire.IsRoboRevCheck(failedJobs[0].Name) && !NeedsCIJobLogFetch(details) {
		if trimmed := strings.TrimSpace(details); trimmed != "" {
			return []jobSection{{name: failedJobs[0].Name, body: trimmed}}, nil
		}
		return nil, nil
	}
	var sections []jobSection
	nonRoboRevFetched := 0
	for _, job := range failedJobs {
		if prwire.IsRoboRevCheck(job.Name) {
			body, err := c.fetchRoboRevJobBody(ctx, repo, repoPath, headSHA, prNumber, job.ID)
			if err != nil {
				return nil, err
			}
			if body != "" {
				sections = append(sections, jobSection{name: job.Name, body: body})
			}
			continue
		}
		if nonRoboRevFetched >= maxRunFailedJobLogs || job.ID <= 0 {
			continue
		}
		nonRoboRevFetched++
		logBody, err := c.fetchCheckJobFailureLogsChecked(ctx, repoPath, job.ID)
		if err != nil {
			return nil, err
		}
		if logBody != "" {
			sections = append(sections, jobSection{name: job.Name, body: logBody})
		}
	}
	return sections, nil
}

func formatCIFailureSections(sections []jobSection, failedCount int, triggerJobID int64, defaultDetails string) string {
	if failedCount == 1 && triggerJobID > 0 && len(sections) == 1 {
		if sections[0].body != "" {
			return sections[0].body
		}
		return defaultDetails
	}
	if len(sections) == 0 {
		return defaultDetails
	}
	var parts []string
	for _, s := range sections {
		parts = append(parts, "### Job: "+s.name+"\n"+s.body)
	}
	return strings.Join(parts, "\n\n")
}

// ResolveCIFailureChecked resolves CI failure details and failed checks across all jobs in a workflow run.
func (c *Client) ResolveCIFailureChecked(ctx context.Context, repo string, prNumber int, checkName, headSHA, checkURL, details string) (CIFailureResolution, error) {
	res := CIFailureResolution{CheckName: strings.TrimSpace(checkName), Details: details}
	if c == nil || c.HTTPClient == nil {
		return c.fallbackCIFailureResolution(ctx, repo, prNumber, checkName, headSHA, checkURL, details)
	}
	runID := ExtractActionsRunID(checkURL)
	if runID <= 0 {
		return c.fallbackCIFailureResolution(ctx, repo, prNumber, checkName, headSHA, checkURL, details)
	}
	repoPath := githubapi.QualifyRepo(c.gitHubOwner, repo)
	jobs, err := c.listRunJobsChecked(ctx, repoPath, runID)
	if err != nil {
		return res, err
	}
	if len(jobs) == 0 {
		return c.fallbackCIFailureResolution(ctx, repo, prNumber, checkName, headSHA, checkURL, details)
	}

	triggerJobID := ExtractActionsJobID(checkURL)
	if !IsWorkflowRunFailure(checkURL, details) && hasPendingJobs(jobs, triggerJobID) {
		return CIFailureResolution{Pending: true, CheckName: res.CheckName, Details: details}, nil
	}

	failedJobs := filterFailedRunJobs(jobs, triggerJobID)
	if len(failedJobs) == 0 {
		return res, nil
	}

	for _, job := range failedJobs {
		if name := strings.TrimSpace(job.Name); name != "" {
			res.FailedChecks = append(res.FailedChecks, name)
		}
	}
	if len(res.FailedChecks) == 1 {
		res.CheckName = res.FailedChecks[0]
	} else if len(res.FailedChecks) > 1 {
		res.CheckName = strings.Join(res.FailedChecks, ", ")
	}

	sections, err := c.fetchFailedJobSections(ctx, repo, repoPath, headSHA, details, prNumber, triggerJobID, failedJobs)
	if err != nil {
		return res, err
	}
	res.Details = formatCIFailureSections(sections, len(res.FailedChecks), triggerJobID, details)
	return res, nil
}

// ExtractActionsJobID parses a GitHub Actions job ID from a check URL.
func ExtractActionsJobID(url string) int64 {
	m := actionsJobIDRegex.FindStringSubmatch(url)
	if len(m) < 2 {
		return 0
	}
	id, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0
	}
	return id
}

// ExtractActionsRunID parses a GitHub Actions workflow run ID from a check URL.
func ExtractActionsRunID(url string) int64 {
	m := actionsRunIDRegex.FindStringSubmatch(url)
	if len(m) < 2 {
		return 0
	}
	id, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0
	}
	return id
}
