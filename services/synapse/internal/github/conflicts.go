package github

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/azylman/aerial/services/synapse/internal/model"
)

// IsAutoUpdateAuthorAllowed checks if the author login is authorized for automated PR branch updates.
func IsAutoUpdateAuthorAllowed(author string, allowedAuthors []string) bool {
	clean := strings.TrimSpace(author)
	if clean == "" || len(allowedAuthors) == 0 {
		return false
	}
	return slices.ContainsFunc(allowedAuthors, func(a string) bool {
		return strings.EqualFold(clean, strings.TrimSpace(a))
	})
}

// UpdatePRBranch sends an update-branch request to GitHub for the given pull request.
// Returns (true, nil) if a merge conflict was detected, (false, nil) if updated successfully
// or no new commits exist on the base branch, and an error on API failure.
func UpdatePRBranch(ctx context.Context, client *Client, repo string, prNumber int) (bool, error) {
	normRepo := model.NormalizeRepo(repo)
	if normRepo == "" || prNumber <= 0 {
		return false, fmt.Errorf("invalid repo or prNumber for update-branch")
	}
	path := fmt.Sprintf("/repos/%s/pulls/%d/update-branch", normRepo, prNumber)
	req, err := client.NewRequest(ctx, http.MethodPut, path, strings.NewReader(`{}`))
	if err != nil {
		return false, fmt.Errorf("create update-branch request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	httpClient := http.DefaultClient
	if client != nil && client.HTTPClient != nil {
		httpClient = client.HTTPClient
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("update-branch request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
		return false, nil
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if readErr != nil {
		body = nil
	}
	bodyLower := strings.ToLower(string(body))
	if resp.StatusCode == http.StatusForbidden {
		remaining := resp.Header.Get("X-Ratelimit-Remaining")
		return false, fmt.Errorf("github api rate limit or forbidden (remaining: %s)", remaining)
	}
	if resp.StatusCode == http.StatusConflict || (resp.StatusCode == http.StatusUnprocessableEntity && strings.Contains(bodyLower, "conflict")) {
		return true, nil
	}
	if resp.StatusCode == http.StatusUnprocessableEntity && strings.Contains(bodyLower, "no new commits on the base branch") {
		return false, nil
	}
	return false, fmt.Errorf("update-branch returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

type checkRunsResponse struct {
	TotalCount int                         `json:"total_count"`
	CheckRuns  []model.GitHubMetadataCheck `json:"check_runs"`
}

func dedupeLatestCheckRuns(runs []model.GitHubMetadataCheck) []model.GitHubMetadataCheck {
	if len(runs) == 0 {
		return nil
	}
	latestIdx := make(map[string]int, len(runs))
	out := make([]model.GitHubMetadataCheck, 0, len(runs))
	for _, cr := range runs {
		if model.IsBuildMetadataCheck(cr.Name) || model.IsDependabotAppCheck(cr.App.Slug) {
			continue
		}
		key := CanonicalFailedCheckKey(cr.Name)
		if key == "" {
			continue
		}
		if idx, ok := latestIdx[key]; ok {
			if cr.ID >= out[idx].ID {
				out[idx] = cr
			}
			continue
		}
		latestIdx[key] = len(out)
		out = append(out, cr)
	}
	return out
}

// HasPendingOrFailedChecksForSHA queries check runs for a commit SHA and records any failures in tracker.
// Returns true if there are pending or failed checks for the commit.
func HasPendingOrFailedChecksForSHA(ctx context.Context, client *Client, tracker *StateTracker, repo string, prNumber int, headSHA string) bool {
	if tracker != nil && tracker.IsCachedFailedCISHA(repo, prNumber, headSHA) {
		return true
	}
	headSHA = strings.TrimSpace(headSHA)
	normRepo := model.NormalizeRepo(repo)
	if normRepo == "" || prNumber <= 0 || headSHA == "" || model.IsZeroSHA(headSHA) || client == nil {
		return false
	}
	var checkRuns checkRunsResponse
	path := fmt.Sprintf("/repos/%s/commits/%s/check-runs?per_page=100", normRepo, url.PathEscape(headSHA))
	if err := client.GetJSON(ctx, path, 2<<20, &checkRuns); err != nil {
		return !IsNotFound(err)
	}
	hasPending := false
	for _, cr := range dedupeLatestCheckRuns(checkRuns.CheckRuns) {
		state := ClassifyCheckState(cr.Status, cr.Conclusion)
		if !IsGateAggregatorCheck(cr.Name) && state == StatusFailed {
			if tracker != nil {
				tracker.RecordFailedCISHA(normRepo, prNumber, headSHA)
			}
			return true
		}
		if state == StatusPending {
			hasPending = true
		}
	}
	return hasPending
}

// FetchSinglePR retrieves a single pull request by number.
func FetchSinglePR(ctx context.Context, client *Client, repo string, prNumber int) (model.GitHubPullRequest, error) {
	normRepo := model.NormalizeRepo(repo)
	if normRepo == "" || prNumber <= 0 || client == nil {
		return model.GitHubPullRequest{}, fmt.Errorf("invalid arguments for fetch single pr")
	}
	var pr model.GitHubPullRequest
	path := fmt.Sprintf("/repos/%s/pulls/%d", normRepo, prNumber)
	if err := client.GetJSON(ctx, path, 2<<20, &pr); err != nil {
		return model.GitHubPullRequest{}, err
	}
	return pr, nil
}

// FetchSinglePRWithMergeabilityRetry retrieves a single PR and retries if mergeability is not yet calculated.
func FetchSinglePRWithMergeabilityRetry(ctx context.Context, client *Client, repo string, prNumber int, maxRetries int) (model.GitHubPullRequest, error) {
	pr, err := FetchSinglePR(ctx, client, repo, prNumber)
	if err != nil {
		return pr, err
	}
	if maxRetries <= 0 {
		maxRetries = 1
	}
	for range maxRetries {
		if pr.Mergeable != nil || (pr.MergeableState != "" && !strings.EqualFold(pr.MergeableState, "unknown")) {
			return pr, nil
		}
		if ctx.Err() != nil {
			return pr, ctx.Err()
		}
		fetched, fErr := FetchSinglePR(ctx, client, repo, prNumber)
		if fErr != nil {
			return pr, fErr
		}
		pr = fetched
	}
	return pr, nil
}

// FetchOpenPullRequests retrieves all open pull requests targeting main in ascending creation order.
func FetchOpenPullRequests(ctx context.Context, client *Client, repo string) ([]model.GitHubPullRequest, error) {
	normRepo := model.NormalizeRepo(repo)
	if normRepo == "" || client == nil {
		return nil, fmt.Errorf("invalid arguments for fetch open prs")
	}
	var openPRs []model.GitHubPullRequest
	path := fmt.Sprintf("/repos/%s/pulls?state=open&base=main&sort=created&direction=asc&per_page=100", normRepo)
	if err := client.GetJSON(ctx, path, 5<<20, &openPRs); err != nil {
		return nil, fmt.Errorf("list open prs: %w", err)
	}
	return openPRs, nil
}

type commitPRItem struct {
	Number int               `json:"number"`
	State  string            `json:"state"`
	Head   model.GitHubPRRef `json:"head"`
}

// ResolvePRFromCommitSHA discovers the pull request number and head branch ref associated with a commit SHA.
func ResolvePRFromCommitSHA(ctx context.Context, client *Client, tracker *StateTracker, repo, sha string) (int, string, error) {
	repo = model.NormalizeRepo(repo)
	if repo == "" || sha == "" || model.IsZeroSHA(sha) {
		return 0, "", fmt.Errorf("invalid repo or sha for pr resolution")
	}
	if tracker != nil {
		if prNum, branch, ok := tracker.LookupCommitSHAPR(repo, sha); ok {
			return prNum, branch, nil
		}
	}
	if client == nil {
		return 0, "", fmt.Errorf("github client required for resolving pr from commit")
	}
	var prs []commitPRItem
	path := fmt.Sprintf("/repos/%s/commits/%s/pulls", repo, url.PathEscape(sha))
	if err := client.GetJSON(ctx, path, 1<<20, &prs); err != nil {
		return 0, "", err
	}
	if len(prs) == 0 {
		var openPRs []commitPRItem
		if err := client.GetJSON(ctx, fmt.Sprintf("/repos/%s/pulls?state=open&per_page=100", repo), 2<<20, &openPRs); err == nil {
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
		if (selected.State != "closed" && item.State == "closed") || (selected.State == item.State && item.Number > selected.Number) {
			selected = item
		}
	}
	if tracker != nil {
		tracker.RecordCommitSHAPR(repo, sha, selected.Number, selected.Head.Ref)
	}
	return selected.Number, selected.Head.Ref, nil
}
