package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strings"
	"time"
)

var validSkillRelPathRegex = regexp.MustCompile(`^custom-skills/(common|discord)/[a-z0-9-]+/SKILL\.md$`)

// SkillPublisher defines the contract for publishing skill updates to a remote repository.
type SkillPublisher interface {
	PublishSkillChanges(ctx context.Context, changes []SkillFileChange) (prNumber int, err error)
}

// GitHubSkillPublisherConfig configures GitHubSkillPublisher.
type GitHubSkillPublisherConfig struct {
	Token              string
	Owner              string
	Repo               string
	BrainPRRegisterURL string
	SystemChannelID    string
	BaseURL            string
	HTTPClient         *http.Client
	Now                func() time.Time
}

// GitHubSkillPublisher publishes procedural skill changes directly via GitHub HTTP API without git subprocesses.
type GitHubSkillPublisher struct {
	token              string
	owner              string
	repo               string
	brainPRRegisterURL string
	systemChannelID    string
	baseURL            string
	httpClient         *http.Client
	now                func() time.Time
}

// NewGitHubSkillPublisher initializes a GitHubSkillPublisher instance.
func NewGitHubSkillPublisher(cfg GitHubSkillPublisherConfig) *GitHubSkillPublisher {
	baseURL := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	if baseURL == "" {
		baseURL = "https://api.github.com"
	}
	httpClient := cfg.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	owner := strings.TrimSpace(cfg.Owner)
	if owner == "" {
		owner = DefaultConfigRepoOwner
	}
	repo := strings.TrimSpace(cfg.Repo)
	if repo == "" {
		repo = DefaultConfigRepoName
	}
	brainURL := strings.TrimSpace(cfg.BrainPRRegisterURL)
	if brainURL == "" {
		brainURL = DefaultBrainPRRegisterURL
	}
	return &GitHubSkillPublisher{
		token:              strings.TrimSpace(cfg.Token),
		owner:              owner,
		repo:               repo,
		brainPRRegisterURL: brainURL,
		systemChannelID:    strings.TrimSpace(cfg.SystemChannelID),
		baseURL:            baseURL,
		httpClient:         httpClient,
		now:                now,
	}
}

type ghPullRequest struct {
	Number int    `json:"number"`
	NodeID string `json:"node_id"`
	Head   struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"head"`
}

type ghRefObject struct {
	SHA  string `json:"sha"`
	Type string `json:"type"`
}

type ghRef struct {
	Ref    string      `json:"ref"`
	Object ghRefObject `json:"object"`
}

type ghCommitTree struct {
	SHA string `json:"sha"`
}

type ghCommit struct {
	SHA  string       `json:"sha"`
	Tree ghCommitTree `json:"tree"`
}

type ghTreeEntry struct {
	Path    string `json:"path"`
	Mode    string `json:"mode"`
	Type    string `json:"type"`
	Content string `json:"content"`
}

type ghCreateTreeRequest struct {
	BaseTree string        `json:"base_tree"`
	Tree     []ghTreeEntry `json:"tree"`
}

type ghCreateTreeResponse struct {
	SHA string `json:"sha"`
}

type ghCreateCommitRequest struct {
	Message string   `json:"message"`
	Tree    string   `json:"tree"`
	Parents []string `json:"parents"`
}

type ghCreateCommitResponse struct {
	SHA string `json:"sha"`
}

type ghCreateRefRequest struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

type ghCreatePRRequest struct {
	Title string `json:"title"`
	Head  string `json:"head"`
	Base  string `json:"base"`
	Body  string `json:"body"`
}

type ghCreatePRResponse struct {
	Number int    `json:"number"`
	NodeID string `json:"node_id"`
}

func (p *GitHubSkillPublisher) doRequest(ctx context.Context, method, endpoint string, payload any) ([]byte, error) {
	var bodyReader io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request payload: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	url := endpoint
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		url = p.baseURL + endpoint
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}

	if p.token != "" {
		req.Header.Set("Authorization", "Bearer "+p.token)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request %s %s failed: %w", method, url, err)
	}
	defer closeWarn(resp.Body, "github response body")

	limited := io.LimitReader(resp.Body, 2<<20)
	respBytes, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return respBytes, fmt.Errorf("github api %s %s returned %d: %s", method, url, resp.StatusCode, strings.TrimSpace(string(respBytes)))
	}

	return respBytes, nil
}

// PublishSkillChanges publishes compiled skill changes to the configuration repository as an auto-merging PR.
func (p *GitHubSkillPublisher) PublishSkillChanges(ctx context.Context, changes []SkillFileChange) (int, error) {
	if p.token == "" {
		return 0, fmt.Errorf("github: missing GITHUB_PAT")
	}
	if p.repo == "aerial" {
		return 0, fmt.Errorf("github: publishing directly to aerial repository is forbidden")
	}
	if len(changes) == 0 {
		return 0, nil
	}

	for _, c := range changes {
		if !validSkillRelPathRegex.MatchString(c.RelPath) {
			return 0, fmt.Errorf("github: invalid skill relative path %q", c.RelPath)
		}
	}

	// 1. Check open PRs for an existing update/coven-skills- branch
	listPullsEndpoint := fmt.Sprintf("/repos/%s/%s/pulls?state=open&per_page=30", p.owner, p.repo)
	pullsData, err := p.doRequest(ctx, http.MethodGet, listPullsEndpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("github: failed to list open pull requests: %w", err)
	}
	var openPRs []ghPullRequest
	if err := json.Unmarshal(pullsData, &openPRs); err != nil {
		return 0, fmt.Errorf("github: failed to parse open pull requests: %w", err)
	}
	for _, pr := range openPRs {
		if strings.HasPrefix(pr.Head.Ref, "update/coven-skills-") {
			log.Printf("[coven-github] open coven PR already exists (#%d, ref %s), skipping publication", pr.Number, pr.Head.Ref)
			return 0, nil
		}
	}

	// 2. Resolve main branch HEAD ref and base commit
	mainRefEndpoint := fmt.Sprintf("/repos/%s/%s/git/ref/heads/main", p.owner, p.repo)
	mainRefData, err := p.doRequest(ctx, http.MethodGet, mainRefEndpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("github: failed to get heads/main: %w", err)
	}
	var mainRef ghRef
	if err := json.Unmarshal(mainRefData, &mainRef); err != nil {
		return 0, fmt.Errorf("github: failed to parse heads/main: %w", err)
	}
	mainCommitSHA := strings.TrimSpace(mainRef.Object.SHA)
	if mainCommitSHA == "" {
		return 0, fmt.Errorf("github: empty SHA for heads/main")
	}

	commitEndpoint := fmt.Sprintf("/repos/%s/%s/git/commits/%s", p.owner, p.repo, mainCommitSHA)
	commitData, err := p.doRequest(ctx, http.MethodGet, commitEndpoint, nil)
	if err != nil {
		return 0, fmt.Errorf("github: failed to get commit %s: %w", mainCommitSHA, err)
	}
	var commitInfo ghCommit
	if err := json.Unmarshal(commitData, &commitInfo); err != nil {
		return 0, fmt.Errorf("github: failed to parse commit %s: %w", mainCommitSHA, err)
	}
	baseTreeSHA := strings.TrimSpace(commitInfo.Tree.SHA)
	if baseTreeSHA == "" {
		return 0, fmt.Errorf("github: empty tree SHA for commit %s", mainCommitSHA)
	}

	// 3. Create tree with changes
	treeEntries := make([]ghTreeEntry, len(changes))
	for i, c := range changes {
		treeEntries[i] = ghTreeEntry{
			Path:    c.RelPath,
			Mode:    "100644",
			Type:    "blob",
			Content: c.Content,
		}
	}
	treeReq := ghCreateTreeRequest{
		BaseTree: baseTreeSHA,
		Tree:     treeEntries,
	}
	createTreeEndpoint := fmt.Sprintf("/repos/%s/%s/git/trees", p.owner, p.repo)
	treeData, err := p.doRequest(ctx, http.MethodPost, createTreeEndpoint, treeReq)
	if err != nil {
		return 0, fmt.Errorf("github: failed to create tree: %w", err)
	}
	var treeResp ghCreateTreeResponse
	if err := json.Unmarshal(treeData, &treeResp); err != nil {
		return 0, fmt.Errorf("github: failed to parse created tree: %w", err)
	}
	newTreeSHA := strings.TrimSpace(treeResp.SHA)
	if newTreeSHA == "" {
		return 0, fmt.Errorf("github: empty SHA for created tree")
	}

	// 4. Create commit
	commitMsg := formatSkillCommitMessage(changes)
	commitReq := ghCreateCommitRequest{
		Message: commitMsg,
		Tree:    newTreeSHA,
		Parents: []string{mainCommitSHA},
	}
	createCommitEndpoint := fmt.Sprintf("/repos/%s/%s/git/commits", p.owner, p.repo)
	commitRespData, err := p.doRequest(ctx, http.MethodPost, createCommitEndpoint, commitReq)
	if err != nil {
		return 0, fmt.Errorf("github: failed to create commit: %w", err)
	}
	var commitResp ghCreateCommitResponse
	if err := json.Unmarshal(commitRespData, &commitResp); err != nil {
		return 0, fmt.Errorf("github: failed to parse created commit: %w", err)
	}
	newCommitSHA := strings.TrimSpace(commitResp.SHA)
	if newCommitSHA == "" {
		return 0, fmt.Errorf("github: empty SHA for created commit")
	}

	// 5. Create branch ref: refs/heads/update/coven-skills-<unix_ts>
	ts := p.now().Unix()
	branchRef := fmt.Sprintf("refs/heads/update/coven-skills-%d", ts)
	branchName := fmt.Sprintf("update/coven-skills-%d", ts)

	createRefEndpoint := fmt.Sprintf("/repos/%s/%s/git/refs", p.owner, p.repo)
	refReq := ghCreateRefRequest{
		Ref: branchRef,
		SHA: newCommitSHA,
	}
	if _, err := p.doRequest(ctx, http.MethodPost, createRefEndpoint, refReq); err != nil {
		return 0, fmt.Errorf("github: failed to create ref %s: %w", branchRef, err)
	}

	// 6. Create PR
	prTitle := formatSkillPRTitle(changes)
	prBody := formatSkillPRBody(changes, p.systemChannelID)
	prReq := ghCreatePRRequest{
		Title: prTitle,
		Head:  branchName,
		Base:  "main",
		Body:  prBody,
	}
	createPREndpoint := fmt.Sprintf("/repos/%s/%s/pulls", p.owner, p.repo)
	prData, err := p.doRequest(ctx, http.MethodPost, createPREndpoint, prReq)
	if err != nil {
		// Clean up branch ref on PR creation failure
		delEndpoint := fmt.Sprintf("/repos/%s/%s/git/refs/heads/%s", p.owner, p.repo, branchName)
		if _, delErr := p.doRequest(context.Background(), http.MethodDelete, delEndpoint, nil); delErr != nil {
			log.Printf("[coven-github] warning deleting orphan branch %s: %v", branchName, delErr)
		}
		return 0, fmt.Errorf("github: failed to create pull request: %w", err)
	}
	var prResp ghCreatePRResponse
	if err := json.Unmarshal(prData, &prResp); err != nil {
		return 0, fmt.Errorf("github: failed to parse created pull request: %w", err)
	}
	prNumber := prResp.Number
	nodeID := strings.TrimSpace(prResp.NodeID)

	// 7. Best-effort GraphQL enablePullRequestAutoMerge (SQUASH)
	if nodeID != "" {
		if err := p.enableAutoMerge(ctx, nodeID); err != nil {
			log.Printf("[coven-github] warning enabling auto-merge for PR #%d: %v", prNumber, err)
		}
	}

	// 8. Best-effort register PR with Brain if SystemChannelID is configured
	if p.systemChannelID != "" && p.brainPRRegisterURL != "" {
		if err := p.registerPRWithBrain(ctx, prNumber, branchName, newCommitSHA, prTitle); err != nil {
			log.Printf("[coven-github] warning registering PR #%d in brain: %v", prNumber, err)
		}
	}

	return prNumber, nil
}

func (p *GitHubSkillPublisher) enableAutoMerge(ctx context.Context, nodeID string) error {
	graphqlEndpoint := fmt.Sprintf("%s/graphql", p.baseURL)
	payload := map[string]any{
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
	}

	_, err := p.doRequest(ctx, http.MethodPost, graphqlEndpoint, payload)
	return err
}

func (p *GitHubSkillPublisher) registerPRWithBrain(ctx context.Context, prNum int, branch, headSHA, title string) error {
	payload := map[string]any{
		"repo":             p.repo,
		"pr_number":        prNum,
		"branch":           branch,
		"head_sha":         headSHA,
		"target_id":        p.systemChannelID,
		"title":            title,
		"metadata":         "{}",
		"vanadis_chunk_id": "",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal registration payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.brainPRRegisterURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create registration request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("brain registration request failed: %w", err)
	}
	defer closeWarn(resp.Body, "brain registration response")

	if resp.StatusCode >= 400 {
		limited := io.LimitReader(resp.Body, 2<<20)
		body, err := io.ReadAll(limited)
		if err != nil {
			return fmt.Errorf("brain registration returned status %d (failed to read body: %w)", resp.StatusCode, err)
		}
		return fmt.Errorf("brain registration returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return nil
}

func formatSkillCommitMessage(changes []SkillFileChange) string {
	if len(changes) == 1 {
		return fmt.Sprintf("feat(skills): %s\n\n%s", changes[0].CommitSummary, changes[0].RelPath)
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("feat(skills): update %d procedural skills\n\n", len(changes)))
	for _, c := range changes {
		sb.WriteString(fmt.Sprintf("- %s: %s\n", c.RelPath, c.CommitSummary))
	}
	return sb.String()
}

func formatSkillPRTitle(changes []SkillFileChange) string {
	if len(changes) == 1 {
		return fmt.Sprintf("feat(skills): %s", changes[0].CommitSummary)
	}
	return fmt.Sprintf("feat(skills): evolve %d procedural skills", len(changes))
}

func formatSkillPRBody(changes []SkillFileChange, targetID string) string {
	var sb strings.Builder
	sb.WriteString("## Summary\n")
	sb.WriteString("Autonomous procedural skill updates compiled by Coven from runtime trajectory friction and execution signals.\n\n")

	sb.WriteString("## Changes\n")
	for _, c := range changes {
		sb.WriteString(fmt.Sprintf("- **%s** `%s`: %s\n", c.Action, c.RelPath, c.CommitSummary))
	}
	sb.WriteString("\n## Verification\n")
	sb.WriteString("- Frontmatter schema and length boundaries verified by Coven validator.\n")
	sb.WriteString("- Relative paths verified against allowed custom skill domain directories.\n\n")

	trailer := fmt.Sprintf(`<!-- aerial-pr: {"target_id":%q,"vanadis_chunk_id":""} -->`, targetID)
	sb.WriteString(trailer)
	return sb.String()
}

func parseDesignerResponse(raw string) (*DesignerSkillResponse, error) {
	clean := strings.TrimSpace(raw)
	if clean == "" {
		return nil, fmt.Errorf("empty designer response")
	}

	firstBrace := strings.Index(clean, "{")
	if firstBrace == -1 {
		return nil, fmt.Errorf("no json object found in designer output")
	}

	var resp DesignerSkillResponse
	dec := json.NewDecoder(strings.NewReader(clean[firstBrace:]))
	if err := dec.Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to decode designer response: %w", err)
	}
	return &resp, nil
}

// ConsolidateProceduralSkills scans pending procedural friction candidates, clusters and compiles
// them into validated SKILL.md changes, and publishes an auto-merging PR to aerial-config.
func ConsolidateProceduralSkills(ctx context.Context, store Store, ollama OllamaClient, llmFunc LLMClientFunc, publisher SkillPublisher, customSkillsDir string) (int, error) {
	if !extractionMutex.TryLock() {
		return 0, nil
	}
	defer extractionMutex.Unlock()

	if store == nil {
		return 0, fmt.Errorf("store cannot be nil")
	}
	if publisher == nil {
		return 0, fmt.Errorf("publisher cannot be nil")
	}
	if llmFunc == nil {
		return 0, fmt.Errorf("llmFunc cannot be nil")
	}

	pending, err := store.GetPendingProceduralCandidates(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get pending procedural candidates: %w", err)
	}
	if len(pending) == 0 {
		return 0, nil
	}

	cleanDir := strings.TrimSpace(customSkillsDir)
	if cleanDir == "" {
		cleanDir = DefaultCustomSkillsDir
	}

	diskSkills, err := ScanCustomSkills(ctx, cleanDir, ollama)
	if err != nil {
		return 0, fmt.Errorf("failed to scan custom skills from %s: %w", cleanDir, err)
	}

	batches, skippedIDs := PlanSkillConsolidation(pending, diskSkills)
	if len(skippedIDs) > 0 {
		if err := store.MarkProceduralCandidatesSkipped(ctx, skippedIDs); err != nil {
			log.Printf("[coven-skills] warning marking blocked candidates skipped: %v", err)
		}
	}

	if len(batches) == 0 {
		return 0, nil
	}

	var changes []SkillFileChange
	var allPublishedIDs []int64
	seenPaths := make(map[string]bool)
	seenNames := make(map[string]bool)

	for _, batch := range batches {
		batchIDs := make([]int64, len(batch.Candidates))
		for i, c := range batch.Candidates {
			batchIDs[i] = c.ID
		}

		prompt := buildSkillDesignerPrompt(batch)
		rawResp, err := llmFunc(ctx, prompt)
		if err != nil {
			log.Printf("[coven-skills] error invoking skill designer LLM: %v", err)
			continue
		}

		designerResp, err := parseDesignerResponse(rawResp)
		if err != nil {
			log.Printf("[coven-skills] error parsing designer response: %v", err)
			continue
		}

		action := strings.ToUpper(strings.TrimSpace(designerResp.Action))
		if action == "NOOP" {
			if err := store.MarkProceduralCandidatesSkipped(ctx, batchIDs); err != nil {
				log.Printf("[coven-skills] warning marking NOOP candidates skipped: %v", err)
			}
			continue
		}

		if action != "EDIT" && action != "CREATE" {
			log.Printf("[coven-skills] unknown designer action %q, skipping batch", designerResp.Action)
			continue
		}

		domain := batch.Domain
		if domain == "" {
			domain = designerResp.Domain
		}
		if domain == "" {
			domain = "discord"
		}

		name, relPath, cleanedMD, err := ValidateCompiledSkillMarkdown(action, domain, designerResp.Markdown, batch.TargetSkill, diskSkills)
		if err != nil {
			log.Printf("[coven-skills] validation failed for %s %s: %v", action, designerResp.Name, err)
			continue
		}

		if seenPaths[relPath] || seenNames[name] {
			log.Printf("[coven-skills] colliding skill name %q or path %q already staged in current pass, deferring batch", name, relPath)
			continue
		}

		seenPaths[relPath] = true
		seenNames[name] = true

		// Update in-memory diskSkills slice so subsequent batches validate and compile against the latest staged state.
		found := false
		for i := range diskSkills {
			if diskSkills[i].RelPath == relPath || diskSkills[i].Name == name {
				diskSkills[i].RawContent = cleanedMD
				diskSkills[i].Name = name
				found = true
				break
			}
		}
		if !found {
			normDomain := strings.ToLower(strings.TrimSpace(domain))
			if normDomain != "common" {
				normDomain = "discord"
			}
			diskSkills = append(diskSkills, DiskSkill{
				Name:       name,
				RelPath:    relPath,
				Domain:     normDomain,
				RawContent: cleanedMD,
			})
		}

		commitSummary := strings.TrimSpace(designerResp.CommitSummary)
		if commitSummary == "" {
			commitSummary = fmt.Sprintf("%s skill %s", strings.ToLower(action), name)
		}

		changes = append(changes, SkillFileChange{
			RelPath:       relPath,
			Content:       cleanedMD,
			Action:        action,
			SkillName:     name,
			CommitSummary: commitSummary,
			CandidateIDs:  batchIDs,
		})
		allPublishedIDs = append(allPublishedIDs, batchIDs...)
	}

	if len(changes) == 0 {
		return 0, nil
	}

	prNum, err := publisher.PublishSkillChanges(ctx, changes)
	if err != nil {
		return 0, fmt.Errorf("failed to publish skill changes: %w", err)
	}

	if prNum > 0 && len(allPublishedIDs) > 0 {
		if err := store.MarkProceduralCandidatesConsolidated(ctx, allPublishedIDs, prNum); err != nil {
			log.Printf("[coven-skills] warning marking candidates consolidated: %v", err)
		}
	}

	return prNum, nil
}
