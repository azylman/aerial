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

	"github.com/azylman/aerial/pkg/githubauth"
	"github.com/azylman/aerial/services/vanadis-restate/internal/prompt"
)

const maxRoboRevCommentRunes = 4000

var (
	actionsJobIDRegex = regexp.MustCompile(`/actions/runs/\d+/job/(\d+)`)
	ghaTimestampRegex = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T[0-9:.]+Z ?`)
	ansiEscapeRegex   = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)
	goCoordinateRegex = regexp.MustCompile(`\.go:\d+`)

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
}

// NewClient builds a CI log client with the default GitHub API URL.
func NewClient(gitHubToken string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Client{
		GitHubToken:  gitHubToken,
		GitHubAPIURL: "https://api.github.com",
		HTTPClient:   httpClient,
	}
}

// IsRetryableGitHubResponse reports whether a GitHub response warrants a
// retry: a 5xx or a rate limit as githubauth.IsRateLimited classifies it (a
// permission 403 is not retryable).
func IsRetryableGitHubResponse(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	return resp.StatusCode >= 500 || githubauth.IsRateLimited(resp)
}

// ResolveCIFailureDetailsChecked extracts failing logs or RoboRev findings for a PR check,
// returning a retryable error when a GitHub API call fails with a transport error or retryable HTTP status (>= 500, 429, 403).
func (c *Client) ResolveCIFailureDetailsChecked(ctx context.Context, repo string, prNumber int, checkName, headSHA, checkURL, details string) (string, error) {
	if c == nil {
		return details, nil
	}
	if prNumber > 0 && IsRoboRevCheck(checkName) {
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
	jobID := ExtractActionsJobID(checkURL)
	if jobID <= 0 {
		return details, nil
	}
	logDetails, err := c.fetchCheckJobFailureLogsChecked(ctx, repo, jobID)
	if err != nil {
		return details, err
	}
	if logDetails != "" {
		return logDetails, nil
	}
	return details, nil
}

// ResolveCIFailureDetails extracts failing logs or RoboRev findings for a PR check.
func (c *Client) ResolveCIFailureDetails(ctx context.Context, repo string, prNumber int, checkName, headSHA, checkURL, details string) string {
	res, err := c.ResolveCIFailureDetailsChecked(ctx, repo, prNumber, checkName, headSHA, checkURL, details)
	if err != nil {
		return details
	}
	return res
}

func (c *Client) newGitHubRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	apiURL := strings.TrimSpace(c.GitHubAPIURL)
	if apiURL == "" {
		apiURL = "https://api.github.com"
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(apiURL, "/")+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	if token := strings.TrimSpace(c.GitHubToken); token != "" {
		req.Header.Set("Authorization", "token "+token)
	}
	return req, nil
}

// ResolveGitHubRepoPath normalizes a repo slug to an owner/repo path.
func ResolveGitHubRepoPath(repo string) string {
	clean := strings.Trim(strings.TrimSpace(repo), "/")
	if clean == "" {
		return ""
	}
	if strings.Contains(clean, "/") {
		return clean
	}
	return "azylman/" + clean
}

func (c *Client) fetchCheckJobFailureLogsChecked(ctx context.Context, repo string, jobID int64) (string, error) {
	if c == nil || c.HTTPClient == nil {
		return "", nil
	}
	repoPath := ResolveGitHubRepoPath(repo)
	if repoPath == "" || jobID <= 0 {
		return "", nil
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := c.newGitHubRequest(timeoutCtx, http.MethodGet, fmt.Sprintf("/repos/%s/actions/jobs/%d/logs", repoPath, jobID), nil)
	if err != nil {
		return "", err
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if IsRetryableGitHubResponse(resp) {
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

type gitHubIssueComment struct {
	ID   int64  `json:"id"`
	Body string `json:"body"`
}

func (c *Client) fetchLatestRoboRevCommentChecked(ctx context.Context, repo string, prNumber int, headSHA string) (string, error) {
	repoPath := ResolveGitHubRepoPath(repo)
	if c == nil || repoPath == "" || prNumber <= 0 || c.HTTPClient == nil {
		return "", nil
	}
	var comments []gitHubIssueComment
	path := fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100", repoPath, prNumber)
	req, err := c.newGitHubRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return "", err
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if IsRetryableGitHubResponse(resp) {
		return "", fmt.Errorf("github api error (%d): %s", resp.StatusCode, http.StatusText(resp.StatusCode))
	}
	if resp.StatusCode != http.StatusOK {
		return "", nil
	}
	respBody, rErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if rErr != nil {
		return "", rErr
	}
	if uErr := json.Unmarshal(respBody, &comments); uErr != nil {
		return "", nil
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
		return prompt.TruncatePromptRunes(body, maxRoboRevCommentRunes), nil
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
		if cleaned == "" || hasAnyPrefix(cleaned, ciPlumbingPrefixes) {
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

// IsRoboRevCheck reports whether a check name is a RoboRev review.
func IsRoboRevCheck(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	return strings.Contains(lower, "roborev")
}

// NeedsCIJobLogFetch reports whether details require a CI job log fetch.
func NeedsCIJobLogFetch(details string) bool {
	return strings.TrimSpace(details) == "" || strings.EqualFold(details, "failure") || strings.EqualFold(details, "timed_out")
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

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}
