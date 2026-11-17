package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// validatePRHead checks GitHub's current PR rather than trusting webhook delivery
// order. API errors remain retryable; a superseded event is safely skipped.
func (e *Executor) validatePRHead(ctx context.Context, repo string, number int, head string) error {
	if e == nil || e.httpClient == nil {
		return fmt.Errorf("PR head verification unavailable")
	}
	repo = strings.TrimSpace(repo)
	if repo != "" && !strings.Contains(repo, "/") {
		repo = defaultRepoOwner + "/" + repo
	}
	parts := strings.Split(repo, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" || number <= 0 {
		return fmt.Errorf("PR head verification requires repository and PR number")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := e.newGitHubRequest(ctx, http.MethodGet, fmt.Sprintf("/repos/%s/%s/pulls/%d", url.PathEscape(parts[0]), url.PathEscape(parts[1]), number), nil)
	if err != nil {
		return err
	}
	resp, err := e.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("verify PR head: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("verify PR head: HTTP %d", resp.StatusCode)
	}
	var pr struct {
		State  string `json:"state"`
		Merged *bool  `json:"merged"`
		Head   struct {
			SHA string `json:"sha"`
		} `json:"head"`
	}
	if err = json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return fmt.Errorf("decode PR head: %w", err)
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
