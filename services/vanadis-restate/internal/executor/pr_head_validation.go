package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/githubapi"
	"github.com/azylman/aerial/pkg/githubauth"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// GitHubPullRequest represents GitHub pull request API response fields.
type GitHubPullRequest struct {
	NodeID            string `json:"node_id"`
	Number            int    `json:"number"`
	Title             string `json:"title"`
	State             string `json:"state"`
	Merged            *bool  `json:"merged"`
	MergeCommitSHA    string `json:"merge_commit_sha"`
	Body              string `json:"body"`
	AuthorAssociation string `json:"author_association"`
	User              struct {
		Login string `json:"login"`
	} `json:"user"`
	Head struct {
		SHA  string `json:"sha"`
		Ref  string `json:"ref"`
		Repo *struct {
			Fork     bool   `json:"fork"`
			FullName string `json:"full_name"`
		} `json:"repo"`
	} `json:"head"`
	Base struct {
		Repo *struct {
			FullName string `json:"full_name"`
		} `json:"repo"`
	} `json:"base"`
}

type gitHubPullRequest = GitHubPullRequest

func (e *Executor) fetchGitHubPullRequest(ctx context.Context, repo string, number int) (*gitHubPullRequest, string, string, error) {
	if e == nil || e.HTTPClient == nil {
		return nil, "", "", fmt.Errorf("PR head verification unavailable")
	}
	repo = githubapi.QualifyRepo(e.GitHubOwner(), repo)
	parts := strings.Split(repo, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" || number <= 0 {
		return nil, "", "", fmt.Errorf("PR head verification requires repository and PR number")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := githubapi.NewRequest(ctx, e.GitHubAPIURL, e.gitHubToken, http.MethodGet, fmt.Sprintf("/repos/%s/%s/pulls/%d", url.PathEscape(parts[0]), url.PathEscape(parts[1]), number), nil)
	if err != nil {
		return nil, "", "", err
	}
	var pr gitHubPullRequest
	if err := githubapi.GetJSON(ctx, e.HTTPClient, req, 2<<20, &pr); err != nil {
		var se *githubapi.StatusError
		if errors.As(err, &se) && !se.RateLimited && (se.StatusCode == http.StatusNotFound || se.StatusCode == http.StatusGone) {
			return nil, "", "", store.ErrPRTransitionSkipped
		}
		return nil, "", "", err
	}
	return &pr, parts[0], parts[0] + "/" + parts[1], nil
}

// RateLimitDelay returns the remaining duration until the current GitHub rate limit resets, or 0 if not rate-limited.
func (e *Executor) RateLimitDelay(now time.Time) time.Duration {
	if e == nil || e.HTTPClient == nil {
		return 0
	}
	resetAt := githubauth.RateLimitResetAt(e.HTTPClient.Transport)
	return max(0, resetAt.Sub(now))
}

// ValidatePRHead checks GitHub's current PR rather than trusting webhook delivery
// order. API errors remain retryable; non-existent PRs (404/410) or superseded events are safely skipped.
func (e *Executor) ValidatePRHead(ctx context.Context, repo string, number int, head string) error {
	pr, _, _, err := e.fetchGitHubPullRequest(ctx, repo, number)
	if err != nil {
		return err
	}
	// A matching head does not prove that the PR remains eligible to run.
	if pr.State == "closed" || (pr.Merged != nil && *pr.Merged) {
		return store.ErrPRTransitionSkipped
	}
	if pr.State != "open" || pr.Merged == nil {
		return fmt.Errorf("verify PR head: incomplete or invalid lifecycle")
	}
	if strings.TrimSpace(pr.Head.SHA) == "" {
		return fmt.Errorf("verify PR head: empty head")
	}
	if !strings.EqualFold(pr.Head.SHA, head) {
		return store.ErrPRTransitionSkipped
	}
	return nil
}

// LivePRState reads GitHub's current head SHA and whether the PR is still open
// and unmerged. A missing PR (404/410) reports closed without an error; other
// API failures are returned for the caller to classify.
func (e *Executor) LivePRState(ctx context.Context, repo string, number int) (string, bool, error) {
	pr, _, _, err := e.fetchGitHubPullRequest(ctx, repo, number)
	if errors.Is(err, store.ErrPRTransitionSkipped) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	open := pr.State == "open" && (pr.Merged == nil || !*pr.Merged)
	return strings.TrimSpace(pr.Head.SHA), open, nil
}

// MergedPRState reads GitHub's current merge state, head SHA, and merge commit SHA.
// A missing PR (404/410) reports unmerged without an error; other API failures are returned.
func (e *Executor) MergedPRState(ctx context.Context, repo string, number int) (bool, string, string, error) {
	pr, _, _, err := e.fetchGitHubPullRequest(ctx, repo, number)
	if errors.Is(err, store.ErrPRTransitionSkipped) {
		return false, "", "", nil
	}
	if err != nil {
		return false, "", "", err
	}
	merged := pr.Merged != nil && *pr.Merged
	return merged, strings.TrimSpace(pr.Head.SHA), strings.TrimSpace(pr.MergeCommitSHA), nil
}

// HasRoboRevOverride reports whether `aerial-pr.sh override-roborev` recorded
// an override for headSHA on the PR.
func (e *Executor) HasRoboRevOverride(ctx context.Context, repo string, number int, headSHA string) (bool, error) {
	return e.ciClient().HasRoboRevOverrideChecked(ctx, repo, number, headSHA)
}

// HasActiveOrRecoveredChecks reports whether the commit has active or passing CI checks.
func (e *Executor) HasActiveOrRecoveredChecks(ctx context.Context, repo, headSHA string) (bool, error) {
	return e.ciClient().HasActiveOrRecoveredChecksChecked(ctx, repo, headSHA)
}

func isTrustedPRAuthor(login, association, owner string) bool {
	if cleanLogin := strings.TrimSpace(login); cleanLogin != "" && strings.EqualFold(cleanLogin, owner) {
		return true
	}
	switch strings.ToUpper(strings.TrimSpace(association)) {
	case "OWNER", "MEMBER", "COLLABORATOR":
		return true
	default:
		return false
	}
}

func hasDispatchedChunkAssociation(body string, build model.VanadisBuild, chunk model.VanadisChunk) bool {
	_, rest, ok := strings.CutLast(body, "<!-- aerial-pr:")
	if !ok {
		return false
	}
	raw, _, ok := strings.Cut(rest, "-->")
	if !ok {
		return false
	}
	var trailer struct {
		TargetID       string `json:"target_id"`
		VanadisChunkID string `json:"vanadis_chunk_id"`
	}
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &trailer); err != nil {
		return false
	}
	if strings.TrimSpace(trailer.VanadisChunkID) != chunk.ID {
		return false
	}
	buildThread := model.NormalizeDiscordThreadID(build.ThreadID)
	return buildThread == "" || model.NormalizeDiscordThreadID(trailer.TargetID) == buildThread
}

func validatePRRepoBinding(pr *gitHubPullRequest, fullRepo string) bool {
	if pr.Head.Repo == nil || pr.Base.Repo == nil || pr.Head.Repo.Fork {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(pr.Head.Repo.FullName), fullRepo) &&
		strings.EqualFold(strings.TrimSpace(pr.Base.Repo.FullName), fullRepo)
}

func validatePRBranchBinding(pr *gitHubPullRequest, ev pubsub.PRLifecycleEvent, build model.VanadisBuild, chunk model.VanadisChunk, fullRepo string) bool {
	headRef := strings.TrimPrefix(strings.TrimSpace(pr.Head.Ref), "refs/heads/")
	evBranch := strings.TrimPrefix(strings.TrimSpace(ev.Branch), "refs/heads/")
	branchChunkID := model.ExtractChunkIDFromBranch(fullRepo, headRef)
	if headRef == "" || (evBranch != "" && headRef != evBranch) || (branchChunkID != "" && branchChunkID != chunk.ID) {
		return false
	}
	if evHead := strings.TrimSpace(ev.HeadSHA); evHead != "" && (strings.TrimSpace(pr.Head.SHA) == "" || !strings.EqualFold(pr.Head.SHA, evHead)) {
		return false
	}
	if branchChunkID == chunk.ID {
		branchTargetID := model.ExtractTargetIDFromBranch(fullRepo, headRef)
		buildThread := model.NormalizeDiscordThreadID(build.ThreadID)
		return branchTargetID == "" || buildThread == "" || branchTargetID == buildThread
	}
	return hasDispatchedChunkAssociation(pr.Body, build, chunk)
}

func (e *Executor) ValidateChunkPRAttachment(ctx context.Context, build model.VanadisBuild, chunk model.VanadisChunk, ev pubsub.PRLifecycleEvent) error {
	pr, owner, fullRepo, err := e.fetchGitHubPullRequest(ctx, ev.Repo, ev.PRNumber)
	if err != nil {
		return err
	}
	if !validatePRRepoBinding(pr, fullRepo) || !isTrustedPRAuthor(pr.User.Login, pr.AuthorAssociation, owner) {
		return store.ErrPRTransitionSkipped
	}
	if !validatePRBranchBinding(pr, ev, build, chunk, fullRepo) {
		return store.ErrPRTransitionSkipped
	}
	return nil
}

// FormatVanadisBuildContext formats the Markdown build context trailer for multi-chunk builds.
func FormatVanadisBuildContext(build model.VanadisBuild, chunks []model.VanadisChunk, chunk model.VanadisChunk) string {
	total := len(chunks)
	idx := chunk.ChunkIndex + 1
	buildTitle := strings.TrimSpace(build.Title)
	if buildTitle == "" {
		buildTitle = "Untitled Build"
	}
	chunkTitle := strings.TrimSpace(chunk.Title)
	if chunkTitle == "" {
		chunkTitle = "Untitled Chunk"
	}
	lines := []string{
		"## Vanadis Build Context",
		fmt.Sprintf("- **Build**: %s — %s", build.ID, buildTitle),
	}
	if obj := strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(build.Objective, "\r\n", " "), "\n", " ")); obj != "" {
		lines = append(lines, "- **Build Objective**: "+obj)
	}
	lines = append(lines, fmt.Sprintf("- **Active Chunk (%d of %d)**: %s — %s", idx, total, chunk.ID, chunkTitle))
	if len(chunk.TargetFiles) > 0 {
		lines = append(lines, "- **Active Chunk Target Files**: "+strings.Join(chunk.TargetFiles, ", "))
	}
	lines = append(lines, "- **Build Chunks Roadmap**:")
	for _, c := range chunks {
		cIdx := c.ChunkIndex + 1
		cTitle := strings.TrimSpace(c.Title)
		if cTitle == "" {
			cTitle = "Untitled"
		}
		cMode := strings.TrimSpace(c.Mode)
		if cMode == "" {
			cMode = "unknown"
		}
		cStatus := strings.TrimSpace(c.Status)
		if cStatus == "" {
			cStatus = "unknown"
		}
		lines = append(lines, fmt.Sprintf("- [Chunk %d/%d] %s: %s (%s, %s)", cIdx, total, c.ID, cTitle, cMode, cStatus))
	}
	lines = append(lines, "- **Scope Boundary (RoboRev & Review Guidance)**: This PR implements ONLY the active chunk listed above. Caller cutover, downstream lifecycle wiring, and legacy code removal assigned to downstream chunks in the roadmap are intentionally deferred to their respective chunks and must not be flagged as incomplete or dead wiring in this PR.")
	return strings.Join(lines, "\n")
}

func (e *Executor) enrichPRContext(ctx context.Context, build model.VanadisBuild, chunks []model.VanadisChunk, chunk model.VanadisChunk, pr *gitHubPullRequest, fullRepo string, prNumber int) error {
	if len(chunks) <= 1 && strings.HasPrefix(chunk.ID, build.ID+"-chunk-") {
		return nil
	}
	if strings.Contains(pr.Body, "## Vanadis Build Context") {
		return nil
	}
	vanadisCtx := FormatVanadisBuildContext(build, chunks, chunk)
	newBody := vanadisCtx
	if trimmed := strings.TrimSpace(pr.Body); trimmed != "" {
		newBody = trimmed + "\n\n" + vanadisCtx
	}
	patchPayload, mErr := json.Marshal(map[string]string{"body": newBody})
	if mErr != nil {
		return mErr
	}
	parts := strings.Split(fullRepo, "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid repository %s", fullRepo)
	}
	patchPath := fmt.Sprintf("/repos/%s/%s/pulls/%d", url.PathEscape(parts[0]), url.PathEscape(parts[1]), prNumber)
	pReq, pErr := githubapi.NewRequest(ctx, e.GitHubAPIURL, e.gitHubToken, http.MethodPatch, patchPath, bytes.NewReader(patchPayload))
	if pErr != nil {
		return pErr
	}
	pReq.Header.Set("Content-Type", "application/json")
	pResp, pDoErr := e.HTTPClient.Do(pReq)
	if pDoErr != nil {
		return pDoErr
	}
	defer pResp.Body.Close()
	if pResp.StatusCode >= 400 {
		return fmt.Errorf("patch PR context returned status %d", pResp.StatusCode)
	}
	return nil
}

func isLoopbackAPIURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	h := strings.ToLower(u.Hostname())
	return h == "127.0.0.1" || h == "localhost" || h == "::1"
}

func (e *Executor) enablePRAutoMerge(ctx context.Context, nodeID string) error {
	if nodeID == "" {
		return nil
	}
	if e.gitHubToken == "" && !isLoopbackAPIURL(e.GitHubAPIURL) {
		return nil
	}
	graphqlURL := strings.TrimRight(e.GitHubAPIURL, "/") + "/graphql"
	mutationPayload, mErr := json.Marshal(map[string]any{
		"query": `mutation($input: EnablePullRequestAutoMergeInput!) {
			enablePullRequestAutoMerge(input: $input) {
				pullRequest {
					autoMergeRequest {
						enabledAt
						mergeMethod
					}
				}
			}
		}`,
		"variables": map[string]any{
			"input": map[string]string{
				"pullRequestId": nodeID,
				"mergeMethod":   "SQUASH",
			},
		},
	})
	if mErr != nil {
		return mErr
	}
	gReq, gErr := http.NewRequestWithContext(ctx, http.MethodPost, graphqlURL, bytes.NewReader(mutationPayload))
	if gErr != nil {
		return gErr
	}
	gReq.Header.Set("Content-Type", "application/json")
	gReq.Header.Set("Accept", "application/vnd.github+json")
	if e.gitHubToken != "" {
		gReq.Header.Set("Authorization", "Bearer "+e.gitHubToken)
	}
	gResp, gDoErr := e.HTTPClient.Do(gReq)
	if gDoErr != nil {
		slog.WarnContext(ctx, "[vanadis-restate] enable pull request auto-merge failed", "error", gDoErr)
		return gDoErr
	}
	defer func() {
		if cErr := gResp.Body.Close(); cErr != nil {
			slog.WarnContext(ctx, "[vanadis-restate] close response body failed", "error", cErr)
		}
	}()
	if gResp.StatusCode >= 400 {
		err := fmt.Errorf("enable auto-merge returned HTTP status %d", gResp.StatusCode)
		slog.WarnContext(ctx, "[vanadis-restate] enable pull request auto-merge HTTP error", "status", gResp.StatusCode)
		return err
	}
	var res struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.NewDecoder(io.LimitReader(gResp.Body, 64<<10)).Decode(&res); err == nil && len(res.Errors) > 0 {
		err := fmt.Errorf("enable auto-merge GraphQL error: %s", res.Errors[0].Message)
		slog.WarnContext(ctx, "[vanadis-restate] enable pull request auto-merge GraphQL error", "error", res.Errors[0].Message)
		return err
	}
	return nil
}

// EnsurePRAutoMergeAndContext enriches multi-chunk PR descriptions with Vanadis build context
// and enables squash auto-merge on GitHub.
func (e *Executor) EnsurePRAutoMergeAndContext(ctx context.Context, build model.VanadisBuild, chunks []model.VanadisChunk, chunk model.VanadisChunk, prNumber int) error {
	repo := strings.TrimSpace(chunk.Repo)
	if repo == "" {
		for _, c := range chunks {
			if strings.TrimSpace(c.Repo) != "" {
				repo = strings.TrimSpace(c.Repo)
				break
			}
		}
	}
	pr, _, fullRepo, err := e.fetchGitHubPullRequest(ctx, repo, prNumber)
	if err != nil {
		return err
	}
	if err := e.enrichPRContext(ctx, build, chunks, chunk, pr, fullRepo, prNumber); err != nil {
		return err
	}
	if aErr := e.enablePRAutoMerge(ctx, pr.NodeID); aErr != nil {
		slog.WarnContext(ctx, "[vanadis-restate] enable pull request auto-merge warning", "error", aErr)
	}
	return nil
}

// ListOpenPullRequests queries open pull requests across pages for repo.
func (e *Executor) ListOpenPullRequests(ctx context.Context, repo string) ([]GitHubPullRequest, error) {
	if e == nil || e.HTTPClient == nil {
		return nil, fmt.Errorf("list open PRs unavailable: nil client")
	}
	repo = githubapi.QualifyRepo(e.GitHubOwner(), repo)
	parts := strings.Split(repo, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("list open PRs requires repository")
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	const perPage = 100
	const maxPages = 10
	var allPRs []GitHubPullRequest
	for page := 1; page <= maxPages; page++ {
		path := fmt.Sprintf("/repos/%s/%s/pulls?state=open&per_page=%d&page=%d", url.PathEscape(parts[0]), url.PathEscape(parts[1]), perPage, page)
		req, err := githubapi.NewRequest(ctx, e.GitHubAPIURL, e.gitHubToken, http.MethodGet, path, nil)
		if err != nil {
			return nil, err
		}
		var prs []GitHubPullRequest
		if err := githubapi.GetJSON(ctx, e.HTTPClient, req, 5<<20, &prs); err != nil {
			return nil, err
		}
		allPRs = append(allPRs, prs...)
		if len(prs) < perPage {
			break
		}
	}
	return allPRs, nil
}
