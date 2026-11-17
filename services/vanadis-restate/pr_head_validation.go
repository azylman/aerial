package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

type gitHubPullRequest struct {
	State             string `json:"state"`
	Merged            *bool  `json:"merged"`
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

func (e *Executor) fetchGitHubPullRequest(ctx context.Context, repo string, number int) (*gitHubPullRequest, string, string, error) {
	if e == nil || e.httpClient == nil {
		return nil, "", "", fmt.Errorf("PR head verification unavailable")
	}
	repo = strings.TrimSpace(repo)
	if repo != "" && !strings.Contains(repo, "/") {
		repo = defaultRepoOwner + "/" + repo
	}
	parts := strings.Split(repo, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" || number <= 0 {
		return nil, "", "", fmt.Errorf("PR head verification requires repository and PR number")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := e.newGitHubRequest(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/%s/pulls/%d", url.PathEscape(parts[0]), url.PathEscape(parts[1]), number), nil)
	if err != nil {
		return nil, "", "", err
	}
	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, "", "", fmt.Errorf("verify PR head: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "", "", fmt.Errorf("verify PR head: HTTP %d", resp.StatusCode)
	}
	var pr gitHubPullRequest
	if err = json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, "", "", fmt.Errorf("decode PR head: %w", err)
	}
	return &pr, parts[0], parts[0] + "/" + parts[1], nil
}

// validatePRHead checks GitHub's current PR rather than trusting webhook delivery
// order. API errors remain retryable; a superseded event is safely skipped.
func (e *Executor) validatePRHead(ctx context.Context, repo string, number int, head string) error {
	pr, _, _, err := e.fetchGitHubPullRequest(ctx, repo, number)
	if err != nil {
		return err
	}
	// A matching head does not prove that the PR remains eligible to run.
	if pr.State == "closed" || (pr.Merged != nil && *pr.Merged) {
		return errPRTransitionSkipped
	}
	if pr.State != "open" || pr.Merged == nil {
		return fmt.Errorf("verify PR head: incomplete or invalid lifecycle")
	}
	if strings.TrimSpace(pr.Head.SHA) == "" {
		return fmt.Errorf("verify PR head: empty head")
	}
	if !strings.EqualFold(pr.Head.SHA, head) {
		return errPRTransitionSkipped
	}
	return nil
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

func hasDispatchedChunkAssociation(body string, build VanadisBuild, chunk VanadisChunk) bool {
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
	buildThread := normalizeDiscordThreadID(build.ThreadID)
	return buildThread == "" || normalizeDiscordThreadID(trailer.TargetID) == buildThread
}

func (e *Executor) validateChunkPRAttachment(ctx context.Context, build VanadisBuild, chunk VanadisChunk, ev pubsub.PRLifecycleEvent) error {
	pr, owner, fullRepo, err := e.fetchGitHubPullRequest(ctx, ev.Repo, ev.PRNumber)
	if err != nil {
		return err
	}
	if pr.Head.Repo == nil || pr.Base.Repo == nil || pr.Head.Repo.Fork || !strings.EqualFold(strings.TrimSpace(pr.Head.Repo.FullName), fullRepo) || !strings.EqualFold(strings.TrimSpace(pr.Base.Repo.FullName), fullRepo) {
		return errPRTransitionSkipped
	}
	if !isTrustedPRAuthor(pr.User.Login, pr.AuthorAssociation, owner) {
		return errPRTransitionSkipped
	}
	headRef := strings.TrimPrefix(strings.TrimSpace(pr.Head.Ref), "refs/heads/")
	evBranch := strings.TrimPrefix(strings.TrimSpace(ev.Branch), "refs/heads/")
	branchChunkID := extractChunkIDFromBranch(fullRepo, headRef)
	if headRef == "" || (evBranch != "" && headRef != evBranch) || (branchChunkID != "" && branchChunkID != chunk.ID) {
		return errPRTransitionSkipped
	}
	if evHead := strings.TrimSpace(ev.HeadSHA); evHead != "" && (strings.TrimSpace(pr.Head.SHA) == "" || !strings.EqualFold(pr.Head.SHA, evHead)) {
		return errPRTransitionSkipped
	}
	if !hasDispatchedChunkAssociation(pr.Body, build, chunk) {
		return errPRTransitionSkipped
	}
	return nil
}
