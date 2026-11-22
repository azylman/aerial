package prstate

// GitHub REST client for restate-owned GitHub effects: update-branch, open
// PR listing, head check-run reads, single-PR mergeability reads, and
// workflow rerun-failed-jobs. Ported from services/synapse/internal/github
// (conflicts.go, rollout.go) without importing it: synapse is a separate
// module and its packages are internal.
//
// Error classification is data, not error types, so a journaled result
// replays identically: GitHubResult carries the HTTP status and whether a
// retry can help. Rate limits (429, or a 403 githubauth.IsRateLimited
// classifies as primary or secondary limit) and 5xx/transport failures are
// retryable; every other 4xx, including a permission 403, is terminal.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/azylman/aerial/pkg/githubauth"
	"github.com/azylman/aerial/pkg/prwire"
)

// DefaultGitHubAPIURL is the public GitHub REST endpoint.
const DefaultGitHubAPIURL = "https://api.github.com"

// mergeabilityRefetches bounds the immediate refetches while GitHub is still
// computing mergeability, matching synapse FetchSinglePRWithMergeabilityRetry.
const mergeabilityRefetches = 3

// GitHubPRRef is a pull request head or base ref.
type GitHubPRRef struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

// GitHubUser is a pull request author.
type GitHubUser struct {
	Login string `json:"login"`
}

// GitHubPR is the pull request subset the GitHub effects read.
type GitHubPR struct {
	Number         int         `json:"number"`
	State          string      `json:"state"`
	Title          string      `json:"title"`
	Body           string      `json:"body"`
	Head           GitHubPRRef `json:"head"`
	Draft          bool        `json:"draft"`
	Merged         bool        `json:"merged"`
	Mergeable      *bool       `json:"mergeable,omitempty"`
	MergeableState string      `json:"mergeable_state,omitempty"`
	User           GitHubUser  `json:"user"`
}

// GitHubError is one failed GitHub call. Status is 0 for transport errors.
type GitHubError struct {
	Status      int    `json:"status"`
	Message     string `json:"message"`
	RateLimited bool   `json:"rate_limited"`
}

func (e *GitHubError) Error() string {
	return e.Message
}

// Retryable reports whether a later attempt can succeed: rate limits,
// transport failures, and 5xx. Every other 4xx is terminal.
func (e *GitHubError) Retryable() bool {
	return e != nil && (e.RateLimited || e.Status == 0 || e.Status >= http.StatusInternalServerError)
}

// NotFound reports a GitHub 404.
func (e *GitHubError) NotFound() bool {
	return e != nil && e.Status == http.StatusNotFound
}

// UpdateBranchOutcome is the classified update-branch result.
type UpdateBranchOutcome string

const (
	// UpdateBranchUpdated means GitHub accepted the update (200/202).
	UpdateBranchUpdated UpdateBranchOutcome = "updated"
	// UpdateBranchUpToDate means the base had no new commits.
	UpdateBranchUpToDate UpdateBranchOutcome = "up_to_date"
	// UpdateBranchConflict means the update could not merge cleanly.
	UpdateBranchConflict UpdateBranchOutcome = "conflict"
	// UpdateBranchHeadMissing means the PR head ref no longer exists.
	UpdateBranchHeadMissing UpdateBranchOutcome = "head_missing"
)

// GitHubClient is the production GitHub REST client.
type GitHubClient struct {
	http   *http.Client
	apiURL string
	token  string
}

// NewGitHubClient builds a client; an empty apiURL targets api.github.com
// and a nil httpClient uses http.DefaultClient.
func NewGitHubClient(httpClient *http.Client, apiURL, token string) *GitHubClient {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	apiURL = strings.TrimRight(strings.TrimSpace(apiURL), "/")
	if apiURL == "" {
		apiURL = DefaultGitHubAPIURL
	}
	return &GitHubClient{http: httpClient, apiURL: apiURL, token: strings.TrimSpace(token)}
}

// do sends one request and returns the status and up to maxBytes of body.
// A transport failure is a retryable GitHubError with status 0.
func (c *GitHubClient) do(ctx context.Context, method, path string, body io.Reader, maxBytes int64) (int, []byte, *GitHubError) {
	req, err := http.NewRequestWithContext(ctx, method, c.apiURL+path, body)
	if err != nil {
		return 0, nil, &GitHubError{Message: fmt.Sprintf("github request %s %s: %v", method, path, err)}
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "token "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, &GitHubError{Message: fmt.Sprintf("github request %s %s failed: %v", method, path, err)}
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if err != nil {
		return resp.StatusCode, nil, &GitHubError{Message: fmt.Sprintf("read github response %s %s: %v", method, path, err)}
	}
	if githubauth.IsRateLimited(resp) {
		return resp.StatusCode, raw, &GitHubError{
			Status:      resp.StatusCode,
			RateLimited: true,
			Message:     fmt.Sprintf("github api rate limited (status %d, remaining: %s): %s", resp.StatusCode, resp.Header.Get("X-Ratelimit-Remaining"), gitHubMessage(raw)),
		}
	}
	if resp.StatusCode == http.StatusForbidden {
		return resp.StatusCode, raw, &GitHubError{
			Status:  resp.StatusCode,
			Message: fmt.Sprintf("github api forbidden %s %s (status %d): %s", method, path, resp.StatusCode, gitHubMessage(raw)),
		}
	}
	return resp.StatusCode, raw, nil
}

// maxGitHubMessage caps the GitHub error message carried in a GitHubError.
const maxGitHubMessage = 200

// gitHubMessage extracts GitHub's JSON "message" field from an error body,
// capped at maxGitHubMessage runes. GitHubError.Message is journaled, so it
// carries only this field: never the raw body, a JWT, a key or a token.
func gitHubMessage(raw []byte) string {
	var body struct {
		Message string `json:"message"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return "no message"
	}
	msg := strings.Join(strings.Fields(body.Message), " ")
	if msg == "" {
		return "no message"
	}
	if r := []rune(msg); len(r) > maxGitHubMessage {
		msg = string(r[:maxGitHubMessage]) + "…"
	}
	return msg
}

// getJSON GETs path and decodes a 200 body into out.
func (c *GitHubClient) getJSON(ctx context.Context, path string, maxBytes int64, out any) *GitHubError {
	status, raw, gerr := c.do(ctx, http.MethodGet, path, nil, maxBytes)
	if gerr != nil {
		return gerr
	}
	if status != http.StatusOK {
		return &GitHubError{Status: status, Message: fmt.Sprintf("github api GET %s returned status %d", path, status)}
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return &GitHubError{Status: status, Message: fmt.Sprintf("unmarshal github response %s: %v", path, err)}
	}
	return nil
}

// UpdateBranch asks GitHub to merge the base into the PR branch, matching
// synapse UpdatePRBranch: 200/202 updated, 409 or a 422 mentioning a
// conflict is a conflict, a 422 with no new base commits is up to date, a
// missing head ref is reported so the caller can close the stale PR.
func (c *GitHubClient) UpdateBranch(ctx context.Context, repo string, prNumber int) (UpdateBranchOutcome, *GitHubError) {
	repo = NormalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return "", &GitHubError{Status: http.StatusBadRequest, Message: "invalid repo or pr number for update-branch"}
	}
	path := fmt.Sprintf("/repos/%s/pulls/%d/update-branch", repo, prNumber)
	status, raw, gerr := c.do(ctx, http.MethodPut, path, strings.NewReader(`{}`), 64<<10)
	if gerr != nil {
		return "", gerr
	}
	return classifyUpdateBranch(status, string(raw))
}

// classifyUpdateBranch maps one non-rate-limited update-branch response.
func classifyUpdateBranch(status int, body string) (UpdateBranchOutcome, *GitHubError) {
	lower := strings.ToLower(body)
	switch {
	case status == http.StatusOK || status == http.StatusAccepted:
		return UpdateBranchUpdated, nil
	case status == http.StatusConflict || (status == http.StatusUnprocessableEntity && strings.Contains(lower, "conflict")):
		return UpdateBranchConflict, nil
	case status == http.StatusUnprocessableEntity && strings.Contains(lower, "no new commits on the base branch"):
		return UpdateBranchUpToDate, nil
	case strings.Contains(lower, "head ref does not exist"):
		return UpdateBranchHeadMissing, nil
	default:
		return "", &GitHubError{Status: status, Message: fmt.Sprintf("update-branch returned status %d: %s", status, strings.TrimSpace(body))}
	}
}

// ListOpenPRs lists open PRs targeting main in ascending creation order.
func (c *GitHubClient) ListOpenPRs(ctx context.Context, repo string) ([]GitHubPR, *GitHubError) {
	repo = NormalizeRepo(repo)
	if repo == "" {
		return nil, &GitHubError{Status: http.StatusBadRequest, Message: "invalid repo for open pr list"}
	}
	var prs []GitHubPR
	path := fmt.Sprintf("/repos/%s/pulls?state=open&base=main&sort=created&direction=asc&per_page=100", repo)
	if gerr := c.getJSON(ctx, path, 5<<20, &prs); gerr != nil {
		return nil, gerr
	}
	return prs, nil
}

// FetchPR reads one pull request.
func (c *GitHubClient) FetchPR(ctx context.Context, repo string, prNumber int) (GitHubPR, *GitHubError) {
	repo = NormalizeRepo(repo)
	if repo == "" || prNumber <= 0 {
		return GitHubPR{}, &GitHubError{Status: http.StatusBadRequest, Message: "invalid repo or pr number for pr fetch"}
	}
	var pr GitHubPR
	if gerr := c.getJSON(ctx, fmt.Sprintf("/repos/%s/pulls/%d", repo, prNumber), 2<<20, &pr); gerr != nil {
		return GitHubPR{}, gerr
	}
	return pr, nil
}

// FetchPRMergeability reads one PR and refetches up to mergeabilityRefetches
// times while GitHub still reports mergeability unknown. There is no sleep
// between refetches, matching synapse.
func (c *GitHubClient) FetchPRMergeability(ctx context.Context, repo string, prNumber int) (GitHubPR, *GitHubError) {
	pr, gerr := c.FetchPR(ctx, repo, prNumber)
	for range mergeabilityRefetches {
		if gerr != nil || !IsMergeabilityUnknown(pr) {
			return pr, gerr
		}
		if ctx.Err() != nil {
			return pr, &GitHubError{Message: ctx.Err().Error()}
		}
		pr, gerr = c.FetchPR(ctx, repo, prNumber)
	}
	return pr, gerr
}

// gitHubCheckRun is the check-run subset the head check read classifies.
type gitHubCheckRun struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	App        struct {
		Slug string `json:"slug"`
	} `json:"app"`
}

// HeadChecksBlocked reports whether the commit has pending or failed check
// runs, matching synapse HasPendingOrFailedChecksForSHA: a read failure other
// than 404 counts as blocked so the auto-update never races unknown CI.
func (c *GitHubClient) HeadChecksBlocked(ctx context.Context, repo, sha string) bool {
	repo = NormalizeRepo(repo)
	sha = strings.TrimSpace(sha)
	if repo == "" || sha == "" || prwire.IsZeroSHA(sha) {
		return false
	}
	var resp struct {
		CheckRuns []gitHubCheckRun `json:"check_runs"`
	}
	path := fmt.Sprintf("/repos/%s/commits/%s/check-runs?per_page=100", repo, url.PathEscape(sha))
	if gerr := c.getJSON(ctx, path, 2<<20, &resp); gerr != nil {
		return !gerr.NotFound()
	}
	return checkRunsBlocked(resp.CheckRuns)
}

// checkRunsBlocked dedupes runs to the newest per canonical name (skipping
// build-metadata and dependabot runs) and reports any non-gate failure or
// any pending run.
func checkRunsBlocked(runs []gitHubCheckRun) bool {
	pending := false
	for _, cr := range dedupeLatestCheckRuns(runs) {
		state := ClassifyCheckState(cr.Status, cr.Conclusion)
		if state == CheckStateFailed && !IsGateAggregatorCheck(cr.Name) {
			return true
		}
		if state == CheckStatePending {
			pending = true
		}
	}
	return pending
}

func dedupeLatestCheckRuns(runs []gitHubCheckRun) []gitHubCheckRun {
	latest := make(map[string]int, len(runs))
	var out []gitHubCheckRun
	for _, cr := range runs {
		if isBuildMetadataCheckName(cr.Name) || strings.EqualFold(strings.TrimSpace(cr.App.Slug), "dependabot") {
			continue
		}
		key := CanonicalFailedCheckKey(cr.Name)
		if key == "" {
			continue
		}
		if idx, ok := latest[key]; ok {
			if cr.ID >= out[idx].ID {
				out[idx] = cr
			}
			continue
		}
		latest[key] = len(out)
		out = append(out, cr)
	}
	return out
}

func isBuildMetadataCheckName(name string) bool {
	return name == "build-metadata" || name == "build-metadata-result"
}

// RerunFailedJobs reruns the failed jobs of one workflow run.
func (c *GitHubClient) RerunFailedJobs(ctx context.Context, repo string, runID int64) *GitHubError {
	repo = NormalizeRepo(repo)
	if repo == "" || runID <= 0 {
		return &GitHubError{Status: http.StatusBadRequest, Message: "invalid repo or run id for rerun-failed-jobs"}
	}
	path := fmt.Sprintf("/repos/%s/actions/runs/%d/rerun-failed-jobs", repo, runID)
	status, raw, gerr := c.do(ctx, http.MethodPost, path, strings.NewReader(`{}`), 64<<10)
	if gerr != nil {
		return gerr
	}
	if status >= http.StatusOK && status < http.StatusMultipleChoices {
		return nil
	}
	return &GitHubError{Status: status, Message: fmt.Sprintf("rerun-failed-jobs returned status %d: %s", status, strings.TrimSpace(string(raw)))}
}

// ClassifyCheckState maps a GitHub check status and conclusion to the
// canonical CheckStatePassed/Failed/Pending values (or ""), matching synapse
// ClassifyCheckState.
func ClassifyCheckState(status, conclusion string) string {
	s := strings.ToLower(strings.TrimSpace(status))
	c := strings.ToLower(strings.TrimSpace(conclusion))
	if s == "completed" {
		switch c {
		case "success", "neutral", "skipped":
			return CheckStatePassed
		case "failure", "timed_out", "cancelled", "action_required":
			return CheckStateFailed
		default:
			return ""
		}
	}
	if s == "" {
		return ""
	}
	return CheckStatePending
}

// IsGateAggregatorCheck reports an overarching gate check ("CI Gate", "* gate").
func IsGateAggregatorCheck(checkName string) bool {
	lower := strings.ToLower(strings.TrimSpace(checkName))
	return lower == "ci gate" || strings.HasSuffix(lower, " gate")
}

// IsMergeabilityUnknown reports whether GitHub has not computed
// mergeability yet.
func IsMergeabilityUnknown(pr GitHubPR) bool {
	if pr.Mergeable != nil {
		return false
	}
	state := strings.TrimSpace(pr.MergeableState)
	return state == "" || strings.EqualFold(state, "unknown")
}

// AutoMergeBlockReason maps mergeability to a block reason, matching
// synapse ExtractAutoMergeBlockReason.
func AutoMergeBlockReason(mergeableState string, mergeable *bool) string {
	state := strings.ToLower(strings.TrimSpace(mergeableState))
	switch {
	case state == "dirty" || (mergeable != nil && !*mergeable):
		return "merge_conflict"
	case state == "blocked":
		return "checks_or_reviews_blocked"
	case state == "behind":
		return "branch_behind_base"
	case slices.Contains([]string{"", "clean", "unstable", "has_hooks"}, state):
		return ""
	default:
		return state
	}
}

// IsMergeConflict reports whether the PR's mergeability is an actionable
// auto-merge block (conflict or behind base), matching synapse
// isPRMergeConflict.
func IsMergeConflict(pr GitHubPR) bool {
	reason := AutoMergeBlockReason(pr.MergeableState, pr.Mergeable)
	return reason == "merge_conflict" || reason == "dirty" || strings.Contains(reason, "conflict") || reason == "branch_behind_base"
}
