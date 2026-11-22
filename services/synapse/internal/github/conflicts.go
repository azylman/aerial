package github

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

// FetchSinglePR retrieves a single pull request by number.
func FetchSinglePR(ctx context.Context, client *Client, repo string, prNumber int) (model.GitHubPullRequest, error) {
	normRepo := strings.TrimSpace(repo)
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

type commitPRItem struct {
	Number         int               `json:"number"`
	State          string            `json:"state"`
	Head           model.GitHubPRRef `json:"head"`
	MergedAt       string            `json:"merged_at"`
	MergeCommitSHA string            `json:"merge_commit_sha"`
}

// CommitPR is the pull request a commit resolves to through the GitHub
// commits API. Merged and MergeCommitSHA come from the same response:
// GitHub fills merge_commit_sha with a test merge commit on open PRs, so
// MergeCommitSHA is only the real merge commit when Merged is true.
type CommitPR struct {
	Number         int
	Branch         string
	HeadSHA        string
	Merged         bool
	MergeCommitSHA string
}

// MergedOn reports whether the PR merged with sha as its merge commit.
func (p CommitPR) MergedOn(sha string) bool {
	merge, sha := strings.TrimSpace(p.MergeCommitSHA), strings.TrimSpace(sha)
	return p.Number > 0 && p.Merged && merge != "" && sha != "" && strings.EqualFold(merge, sha)
}

// ResolvePRFromCommitSHA discovers the pull request number and head branch ref associated with a commit SHA.
// It is synapse's stateless fallback when the RepoObject SHA index has no answer.
func ResolvePRFromCommitSHA(ctx context.Context, client *Client, repo, sha string) (int, string, error) {
	pr, err := ResolveCommitPR(ctx, client, repo, sha)
	return pr.Number, pr.Branch, err
}

// ResolveCommitPR is ResolvePRFromCommitSHA with the selected PR's head SHA
// and merge state.
func ResolveCommitPR(ctx context.Context, client *Client, repo, sha string) (CommitPR, error) {
	repo = strings.TrimSpace(repo)
	if repo == "" || sha == "" || prwire.IsZeroSHA(sha) {
		return CommitPR{}, fmt.Errorf("invalid repo or sha for pr resolution")
	}
	if client == nil {
		return CommitPR{}, fmt.Errorf("github client required for resolving pr from commit")
	}
	var prs []commitPRItem
	path := fmt.Sprintf("/repos/%s/commits/%s/pulls", repo, url.PathEscape(sha))
	if err := client.GetJSON(ctx, path, 1<<20, &prs); err != nil {
		return CommitPR{}, err
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
		return CommitPR{}, fmt.Errorf("no pull requests associated with commit %s", sha)
	}
	selected := prs[0]
	for _, item := range prs[1:] {
		if (selected.State != "closed" && item.State == "closed") || (selected.State == item.State && item.Number > selected.Number) {
			selected = item
		}
	}
	return CommitPR{
		Number:         selected.Number,
		Branch:         selected.Head.Ref,
		HeadSHA:        strings.TrimSpace(selected.Head.SHA),
		Merged:         strings.TrimSpace(selected.MergedAt) != "",
		MergeCommitSHA: strings.TrimSpace(selected.MergeCommitSHA),
	}, nil
}
