package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type GitHubRun struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	RepoObj *struct {
		FullName string `json:"full_name"`
	} `json:"repository,omitempty"`
	Repository string `json:"-"`
	HeadSHA    string `json:"head_sha"`
	HeadBranch string `json:"head_branch"`
	Event      string `json:"event,omitempty"`
	HeadCommit *struct {
		Message   string    `json:"message"`
		Timestamp time.Time `json:"timestamp"`
	} `json:"head_commit,omitempty"`
	Status     string    `json:"status"`     // "queued", "in_progress", "completed"
	Conclusion string    `json:"conclusion"` // "success", "failure", "cancelled"
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
	HTMLURL    string    `json:"html_url"`
}

type GitHubJob struct {
	ID          int64     `json:"id"`
	RunID       int64     `json:"run_id"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`     // "queued", "in_progress", "completed"
	Conclusion  string    `json:"conclusion"` // "success", "failure", "skipped"
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	HTMLURL     string    `json:"html_url"`
}

type GitHubRunsResponse struct {
	TotalCount   int         `json:"total_count"`
	WorkflowRuns []GitHubRun `json:"workflow_runs"`
}

type GitHubJobsResponse struct {
	TotalCount int         `json:"total_count"`
	Jobs       []GitHubJob `json:"jobs"`
}

type GitHubPoller struct {
	repo         string
	repos        []string
	token        string
	client       *http.Client
	configPath   string
	runsETag     string
	runsETagMap  map[string]string
	jobsETagMap  map[int64]string
	apiBaseURL   string
	pollInterval time.Duration

	mu               sync.RWMutex
	cachedRunsByRepo map[string][]GitHubRun
	cachedRuns       []GitHubRun
	cachedJobs       map[int64][]GitHubJob
	lastPollTime     time.Time
	lastError        error
	stopCh           chan struct{}
	hangarURL        string
	dynamicRepos     []string
}

var globalGHPoller *GitHubPoller

func NewMultiGitHubPoller(repos []string, token string) *GitHubPoller {
	var cleaned []string
	seen := make(map[string]bool)
	for _, r := range repos {
		trimmed := strings.TrimSpace(r)
		if trimmed != "" && !seen[trimmed] {
			seen[trimmed] = true
			cleaned = append(cleaned, trimmed)
		}
	}
	primary := ""
	if len(cleaned) > 0 {
		primary = cleaned[0]
	}

	return &GitHubPoller{
		repo:             primary,
		repos:            cleaned,
		token:            token,
		runsETagMap:      make(map[string]string),
		jobsETagMap:      make(map[int64]string),
		cachedRunsByRepo: make(map[string][]GitHubRun),
		cachedJobs:       make(map[int64][]GitHubJob),
		client: &http.Client{
			Timeout: 4 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        10,
				IdleConnTimeout:     60 * time.Second,
				TLSHandshakeTimeout: 3 * time.Second,
			},
		},
		stopCh: make(chan struct{}),
	}
}

// fetchHangarRepos queries Hangar's GET /status endpoint and extracts valid github_repo slugs.
func fetchHangarRepos(ctx context.Context, hangarURL string, client *http.Client) []string {
	if hangarURL == "" {
		return nil
	}
	reqURL := strings.TrimRight(hangarURL, "/") + "/status"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil
	}

	httpClient := client
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 2 * time.Second}
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	defer drainAndClose(resp.Body, "hangar repos response body")

	if resp.StatusCode != http.StatusOK {
		return nil
	}

	var status GitSyncStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil
	}

	var repos []string
	seen := make(map[string]bool)
	for _, repoStatus := range status.Repos {
		slug := strings.TrimSpace(repoStatus.GitHubRepo)
		if slug != "" {
			lower := strings.ToLower(slug)
			if !seen[lower] {
				seen[lower] = true
				repos = append(repos, lower)
			}
		}
	}
	sort.Strings(repos)
	return repos
}

func (p *GitHubPoller) getActiveRepos() []string {
	if p == nil {
		return nil
	}
	var baseRepos []string
	if p.configPath != "" {
		baseRepos = loadDashboardRepos(p.configPath)
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(baseRepos) == 0 {
		if len(p.repos) > 0 {
			baseRepos = p.repos
		} else if p.repo != "" {
			baseRepos = []string{p.repo}
		}
	}

	seen := make(map[string]bool)
	var result []string
	for _, r := range baseRepos {
		trimmed := strings.TrimSpace(r)
		lower := strings.ToLower(trimmed)
		if trimmed != "" && !seen[lower] {
			seen[lower] = true
			result = append(result, trimmed)
		}
	}
	for _, r := range p.dynamicRepos {
		trimmed := strings.TrimSpace(r)
		lower := strings.ToLower(trimmed)
		if trimmed != "" && !seen[lower] {
			seen[lower] = true
			result = append(result, trimmed)
		}
	}
	return result
}

func (p *GitHubPoller) Start(ctx context.Context) {
	if p == nil {
		return
	}
	p.mu.RLock()
	hasHangar := p.hangarURL != ""
	p.mu.RUnlock()
	repos := p.getActiveRepos()
	if len(repos) == 0 && !hasHangar {
		return
	}
	go func() {
		// Initial immediate poll
		p.pollOnce(ctx)

		interval := p.pollInterval
		if interval <= 0 {
			interval = 30 * time.Second
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-p.stopCh:
				return
			case <-ticker.C:
				hasActive := p.pollOnce(ctx)
				if p.pollInterval <= 0 {
					if hasActive {
						ticker.Reset(15 * time.Second)
					} else {
						ticker.Reset(45 * time.Second)
					}
				}
			}
		}
	}()
}

func (p *GitHubPoller) refreshHangarDynamicRepos(ctx context.Context) {
	p.mu.RLock()
	hURL := p.hangarURL
	p.mu.RUnlock()

	if hURL == "" {
		return
	}
	hCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	dynRepos := fetchHangarRepos(hCtx, hURL, p.client)
	cancel()
	if len(dynRepos) > 0 {
		p.mu.Lock()
		p.dynamicRepos = dynRepos
		p.mu.Unlock()
	}
}

func (p *GitHubPoller) commitPolledRunsAndPruneJobs(repos []string, allActiveRunIDs map[int64]bool) bool {
	p.mu.Lock()
	var allRuns []GitHubRun
	activeRepoMap := make(map[string]bool)
	for _, repo := range repos {
		activeRepoMap[repo] = true
		allRuns = append(allRuns, p.cachedRunsByRepo[repo]...)
	}
	// Prune any removed repositories from cache
	for r := range p.cachedRunsByRepo {
		if !activeRepoMap[r] {
			delete(p.cachedRunsByRepo, r)
			delete(p.runsETagMap, r)
		}
	}

	// Sort runs: active runs first (newest CreatedAt first), then completed runs (newest CreatedAt first)
	sort.SliceStable(allRuns, func(i, j int) bool {
		iActive := allRuns[i].Status == "queued" || allRuns[i].Status == "in_progress"
		jActive := allRuns[j].Status == "queued" || allRuns[j].Status == "in_progress"
		if iActive != jActive {
			return iActive
		}
		return allRuns[i].CreatedAt.After(allRuns[j].CreatedAt)
	})

	hasActive := false
	for _, r := range allRuns {
		if r.Status == "in_progress" || r.Status == "queued" {
			hasActive = true
		}
	}

	p.cachedRuns = allRuns
	p.lastPollTime = time.Now().UTC()
	p.lastError = nil

	// Global job and job ETag pruning across all active run IDs
	for id := range p.cachedJobs {
		if !allActiveRunIDs[id] {
			delete(p.cachedJobs, id)
		}
	}
	for id := range p.jobsETagMap {
		if !allActiveRunIDs[id] {
			delete(p.jobsETagMap, id)
		}
	}
	p.mu.Unlock()

	return hasActive
}

func (p *GitHubPoller) pollOnce(ctx context.Context) bool {
	if p == nil {
		return false
	}
	p.refreshHangarDynamicRepos(ctx)

	repos := p.getActiveRepos()
	if len(repos) == 0 {
		return false
	}

	baseURL := p.apiBaseURL
	if baseURL == "" {
		baseURL = "https://api.github.com"
	}
	allActiveRunIDs := make(map[int64]bool)
	hasActive := false

	for _, repo := range repos {
		if p.pollRepo(ctx, repo, baseURL, allActiveRunIDs) {
			hasActive = true
		}
	}

	if p.commitPolledRunsAndPruneJobs(repos, allActiveRunIDs) {
		hasActive = true
	}

	return hasActive
}

func sanitizeWorkflowRunCommits(runs []GitHubRun, repo string) {
	// Sanitize commit messages and tag repository
	for i := range runs {
		runs[i].Repository = repo
		if runs[i].HeadCommit != nil {
			rawMsg := runs[i].HeadCommit.Message
			firstLine := strings.SplitN(rawMsg, "\n", 2)[0]
			// Truncate to 72 runes
			runes := []rune(firstLine)
			if len(runes) > 72 {
				firstLine = string(runes[:72]) + "…"
			}
			// Strip any token-like substrings
			for _, sens := range sensitiveKeys {
				firstLine = strings.ReplaceAll(firstLine, sens, "[REDACTED]")
			}
			runs[i].HeadCommit.Message = firstLine
		}
	}
}

func (p *GitHubPoller) processPolledWorkflowRuns(ctx context.Context, repo, newETag string, runs []GitHubRun, allActiveRunIDs map[int64]bool) bool {
	p.mu.Lock()
	if newETag != "" {
		p.runsETagMap[repo] = newETag
		if repo == p.repo {
			p.runsETag = newETag
		}
	}
	p.cachedRunsByRepo[repo] = runs
	p.mu.Unlock()

	hasActive := false
	for _, r := range runs {
		allActiveRunIDs[r.ID] = true
		if r.Status == "in_progress" || r.Status == "queued" || time.Since(r.UpdatedAt) < 10*time.Minute {
			p.fetchJobsForRun(ctx, r.ID, repo)
		}
		if r.Status == "in_progress" || r.Status == "queued" {
			hasActive = true
		}
	}
	return hasActive
}

func (p *GitHubPoller) pollRepo(ctx context.Context, repo, baseURL string, allActiveRunIDs map[int64]bool) bool {
	reqURL := fmt.Sprintf("%s/repos/%s/actions/runs?per_page=10", baseURL, repo)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return false
	}

	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-Github-Api-Version", "2022-11-28")
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}

	p.mu.RLock()
	etag := p.runsETagMap[repo]
	if etag == "" && repo == p.repo && p.runsETag != "" {
		etag = p.runsETag
	}
	p.mu.RUnlock()

	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		p.mu.Lock()
		p.lastError = err
		p.mu.Unlock()
		return false
	}
	defer resp.Body.Close()
	defer drainAndClose(resp.Body, "runs response body")

	if resp.StatusCode == http.StatusNotModified {
		hasActive := false
		p.mu.RLock()
		for _, r := range p.cachedRunsByRepo[repo] {
			allActiveRunIDs[r.ID] = true
			if r.Status == "in_progress" || r.Status == "queued" {
				hasActive = true
			}
		}
		p.mu.RUnlock()
		return hasActive
	}

	if resp.StatusCode == http.StatusOK {
		newETag := resp.Header.Get("ETag")
		var runData GitHubRunsResponse
		decodeErr := json.NewDecoder(resp.Body).Decode(&runData)
		if decodeErr != nil {
			log.Printf("[Dashboard:GitHub] Error decoding runs for %s: %v", repo, decodeErr)
			return false
		}
		sanitizeWorkflowRunCommits(runData.WorkflowRuns, repo)
		return p.processPolledWorkflowRuns(ctx, repo, newETag, runData.WorkflowRuns, allActiveRunIDs)
	}

	if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusTooManyRequests {
		log.Printf("[Dashboard:GitHub] Warning: rate limited on %s: HTTP %d", repo, resp.StatusCode)
	} else {
		log.Printf("[Dashboard:GitHub] Warning: unexpected HTTP %d querying %s", resp.StatusCode, repo)
	}
	return false
}

func (p *GitHubPoller) fetchJobsForRun(ctx context.Context, runID int64, optionalRepo ...string) {
	baseURL := p.apiBaseURL
	if baseURL == "" {
		baseURL = "https://api.github.com"
	}
	repo := ""
	if len(optionalRepo) > 0 && optionalRepo[0] != "" {
		repo = optionalRepo[0]
	} else {
		repos := p.getActiveRepos()
		if len(repos) > 0 {
			repo = repos[0]
		}
	}
	if repo == "" {
		return
	}

	reqURL := fmt.Sprintf("%s/repos/%s/actions/runs/%d/jobs", baseURL, repo, runID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return
	}

	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-Github-Api-Version", "2022-11-28")
	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}

	p.mu.RLock()
	etag := p.jobsETagMap[runID]
	p.mu.RUnlock()

	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	defer drainAndClose(resp.Body, "jobs response body")

	if resp.StatusCode == http.StatusNotModified {
		return
	}

	if resp.StatusCode == http.StatusOK {
		jobETag := resp.Header.Get("ETag")
		var jobData GitHubJobsResponse
		if err := json.NewDecoder(resp.Body).Decode(&jobData); err == nil {
			p.mu.Lock()
			if jobETag != "" {
				p.jobsETagMap[runID] = jobETag
			}
			p.cachedJobs[runID] = jobData.Jobs
			p.mu.Unlock()
		}
	}
}

func (p *GitHubPoller) GetSnapshot() ([]GitHubRun, map[int64][]GitHubJob) {
	if p == nil {
		return nil, nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()

	runs := make([]GitHubRun, len(p.cachedRuns))
	copy(runs, p.cachedRuns)

	jobs := make(map[int64][]GitHubJob, len(p.cachedJobs))
	for k, v := range p.cachedJobs {
		jobsCopy := make([]GitHubJob, len(v))
		copy(jobsCopy, v)
		jobs[k] = jobsCopy
	}
	return runs, jobs
}

func mergeClusterDeploymentsWithHangar(
	rawContainers []DockerContainerJSON,
	runs []GitHubRun,
	jobs map[int64][]GitHubJob,
	gitSync GitSyncStatusResponse,
	currentCommit string,
) []DeploymentStatus {
	return MergeClusterDeploymentsWithHangar(runs, jobs, gitSync, rawContainers, currentCommit, time.Time{})
}
