package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/githubapi"
	"github.com/azylman/aerial/services/coven/internal/config"
	"github.com/azylman/aerial/services/coven/internal/store"
)

var validSkillRelPathRegex = regexp.MustCompile(`^custom-skills/(common|discord)/[a-z0-9-]+/SKILL\.md$`)

// SkillPublisher defines the contract for publishing skill updates to a remote repository.
type SkillPublisher interface {
	PublishSkillChanges(ctx context.Context, changes []SkillFileChange) (prNumber int, err error)
}

// FrictionAlertNotifier is an optional capability on SkillPublisher for delivering 5x-repeat short friction alerts to Discord.
type FrictionAlertNotifier interface {
	NotifyShortFriction(ctx context.Context, content string) error
}

// GitHubSkillPublisherConfig configures GitHubSkillPublisher.
type GitHubSkillPublisherConfig struct {
	Token           string
	Owner           string
	Repo            string
	PRRegisterURL   string
	SystemChannelID string
	BaseURL         string
	HTTPClient      *http.Client
	Now             func() time.Time
	EriURL          string
}

// GitHubSkillPublisher publishes procedural skill changes directly via GitHub HTTP API without git subprocesses.
type GitHubSkillPublisher struct {
	token           string
	owner           string
	repo            string
	prRegisterURL   string
	systemChannelID string
	baseURL         string
	httpClient      *http.Client
	now             func() time.Time
	eriURL          string
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
		owner = config.DefaultConfigRepoOwner
	}
	repo := strings.TrimSpace(cfg.Repo)
	if repo == "" {
		repo = config.DefaultConfigRepoName
	}
	prRegisterURL := strings.TrimSpace(cfg.PRRegisterURL)
	return &GitHubSkillPublisher{
		token:           strings.TrimSpace(cfg.Token),
		owner:           owner,
		repo:            repo,
		prRegisterURL:   prRegisterURL,
		systemChannelID: strings.TrimSpace(cfg.SystemChannelID),
		baseURL:         baseURL,
		httpClient:      httpClient,
		now:             now,
		eriURL:          strings.TrimRight(strings.TrimSpace(cfg.EriURL), "/"),
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

	apiURL, path := p.baseURL, endpoint
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		apiURL, path = endpoint, ""
	}

	req, err := githubapi.NewRequest(ctx, apiURL, p.token, method, path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request: %w", err)
	}
	url := req.URL.String()

	req.Header.Set("X-Github-Api-Version", "2022-11-28")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request %s %s failed: %w", method, url, err)
	}
	defer resp.Body.Close()

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

func (p *GitHubSkillPublisher) hasOpenCovenSkillPR(ctx context.Context) (bool, error) {
	listPullsEndpoint := fmt.Sprintf("/repos/%s/%s/pulls?state=open&per_page=30", p.owner, p.repo)
	pullsData, err := p.doRequest(ctx, http.MethodGet, listPullsEndpoint, nil)
	if err != nil {
		return false, fmt.Errorf("github: failed to list open pull requests: %w", err)
	}
	var openPRs []ghPullRequest
	if err := json.Unmarshal(pullsData, &openPRs); err != nil {
		return false, fmt.Errorf("github: failed to parse open pull requests: %w", err)
	}
	for _, pr := range openPRs {
		if strings.HasPrefix(pr.Head.Ref, "update/coven-skills-") {
			log.Printf("[coven-github] open coven PR already exists (#%d, ref %s), skipping publication", pr.Number, pr.Head.Ref)
			return true, nil
		}
	}
	return false, nil
}

func (p *GitHubSkillPublisher) resolveMainCommitAndTree(ctx context.Context) (mainCommitSHA, baseTreeSHA string, err error) {
	mainRefEndpoint := fmt.Sprintf("/repos/%s/%s/git/ref/heads/main", p.owner, p.repo)
	mainRefData, err := p.doRequest(ctx, http.MethodGet, mainRefEndpoint, nil)
	if err != nil {
		return "", "", fmt.Errorf("github: failed to get heads/main: %w", err)
	}
	var mainRef ghRef
	if unmarshalErr := json.Unmarshal(mainRefData, &mainRef); unmarshalErr != nil {
		return "", "", fmt.Errorf("github: failed to parse heads/main: %w", unmarshalErr)
	}
	mainCommitSHA = strings.TrimSpace(mainRef.Object.SHA)
	if mainCommitSHA == "" {
		return "", "", fmt.Errorf("github: empty SHA for heads/main")
	}

	commitEndpoint := fmt.Sprintf("/repos/%s/%s/git/commits/%s", p.owner, p.repo, mainCommitSHA)
	commitData, err := p.doRequest(ctx, http.MethodGet, commitEndpoint, nil)
	if err != nil {
		return "", "", fmt.Errorf("github: failed to get commit %s: %w", mainCommitSHA, err)
	}
	var commitInfo ghCommit
	if err := json.Unmarshal(commitData, &commitInfo); err != nil {
		return "", "", fmt.Errorf("github: failed to parse commit %s: %w", mainCommitSHA, err)
	}
	baseTreeSHA = strings.TrimSpace(commitInfo.Tree.SHA)
	if baseTreeSHA == "" {
		return "", "", fmt.Errorf("github: empty tree SHA for commit %s", mainCommitSHA)
	}
	return mainCommitSHA, baseTreeSHA, nil
}

func (p *GitHubSkillPublisher) createSkillCommitAndBranch(ctx context.Context, mainCommitSHA, baseTreeSHA string, changes []SkillFileChange) (branchName, newCommitSHA string, err error) {
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
		return "", "", fmt.Errorf("github: failed to create tree: %w", err)
	}
	var treeResp ghCreateTreeResponse
	if unmarshalErr := json.Unmarshal(treeData, &treeResp); unmarshalErr != nil {
		return "", "", fmt.Errorf("github: failed to parse created tree: %w", unmarshalErr)
	}
	newTreeSHA := strings.TrimSpace(treeResp.SHA)
	if newTreeSHA == "" {
		return "", "", fmt.Errorf("github: empty SHA for created tree")
	}

	commitMsg := formatSkillCommitMessage(changes)
	commitReq := ghCreateCommitRequest{
		Message: commitMsg,
		Tree:    newTreeSHA,
		Parents: []string{mainCommitSHA},
	}
	createCommitEndpoint := fmt.Sprintf("/repos/%s/%s/git/commits", p.owner, p.repo)
	commitRespData, err := p.doRequest(ctx, http.MethodPost, createCommitEndpoint, commitReq)
	if err != nil {
		return "", "", fmt.Errorf("github: failed to create commit: %w", err)
	}
	var commitResp ghCreateCommitResponse
	if err := json.Unmarshal(commitRespData, &commitResp); err != nil {
		return "", "", fmt.Errorf("github: failed to parse created commit: %w", err)
	}
	newCommitSHA = strings.TrimSpace(commitResp.SHA)
	if newCommitSHA == "" {
		return "", "", fmt.Errorf("github: empty SHA for created commit")
	}

	ts := p.now().Unix()
	branchRef := fmt.Sprintf("refs/heads/update/coven-skills-%d", ts)
	branchName = fmt.Sprintf("update/coven-skills-%d", ts)

	createRefEndpoint := fmt.Sprintf("/repos/%s/%s/git/refs", p.owner, p.repo)
	refReq := ghCreateRefRequest{
		Ref: branchRef,
		SHA: newCommitSHA,
	}
	if _, err := p.doRequest(ctx, http.MethodPost, createRefEndpoint, refReq); err != nil {
		return "", "", fmt.Errorf("github: failed to create ref %s: %w", branchRef, err)
	}
	return branchName, newCommitSHA, nil
}

// PublishSkillChanges publishes compiled skill changes to the configuration repository as an auto-merging PR.
func (p *GitHubSkillPublisher) PublishSkillChanges(ctx context.Context, changes []SkillFileChange) (int, error) {
	if p.token == "" {
		return 0, fmt.Errorf("github: missing GITHUB_PAT")
	}
	if p.repo == config.DefaultStack {
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

	hasOpen, err := p.hasOpenCovenSkillPR(ctx)
	if err != nil {
		return 0, err
	}
	if hasOpen {
		return 0, nil
	}

	mainCommitSHA, baseTreeSHA, err := p.resolveMainCommitAndTree(ctx)
	if err != nil {
		return 0, err
	}

	branchName, newCommitSHA, err := p.createSkillCommitAndBranch(ctx, mainCommitSHA, baseTreeSHA, changes)
	if err != nil {
		return 0, err
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
		// Cleanup must run even when ctx is what failed the PR creation.
		if _, delErr := p.doRequest(context.WithoutCancel(ctx), http.MethodDelete, delEndpoint, nil); delErr != nil {
			log.Printf("[coven-github] warning deleting orphan branch %s: %v", branchName, delErr)
		}
		return 0, fmt.Errorf("github: failed to create pull request: %w", err)
	}
	var prResp ghCreatePRResponse
	if err := json.Unmarshal(prData, &prResp); err != nil {
		return 0, fmt.Errorf("github: failed to parse created pull request: %w", err)
	}
	prNumber := prResp.Number
	p.finalizePublishedSkillPR(ctx, prNumber, strings.TrimSpace(prResp.NodeID), branchName, newCommitSHA, prTitle, changes)
	return prNumber, nil
}

func (p *GitHubSkillPublisher) finalizePublishedSkillPR(ctx context.Context, prNumber int, nodeID, branchName, commitSHA, prTitle string, changes []SkillFileChange) {
	if changesRequireReview(changes) {
		if err := p.NotifyShortFriction(ctx, formatManualReviewPRAlert(prNumber, p.owner, p.repo, changes)); err != nil {
			log.Printf("[coven-github] warning sending manual review PR alert for PR #%d: %v", prNumber, err)
		}
		return
	}
	if nodeID != "" {
		if err := p.enableAutoMerge(ctx, nodeID); err != nil {
			log.Printf("[coven-github] warning enabling auto-merge for PR #%d: %v", prNumber, err)
		}
	}
	if p.systemChannelID != "" && p.prRegisterURL != "" {
		if err := p.registerPRWithBrain(ctx, prNumber, branchName, commitSHA, prTitle); err != nil {
			log.Printf("[coven-github] warning registering PR #%d in brain: %v", prNumber, err)
		}
	}
}

func (p *GitHubSkillPublisher) enableAutoMerge(ctx context.Context, nodeID string) error {
	graphqlEndpoint := p.baseURL + "/graphql"
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

func (p *GitHubSkillPublisher) postJSON(ctx context.Context, targetURL, operationName string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal %s payload: %w", operationName, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create %s request: %w", operationName, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s request failed: %w", operationName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		limited := io.LimitReader(resp.Body, 2<<20)
		body, readErr := io.ReadAll(limited)
		if readErr != nil {
			return fmt.Errorf("%s returned status %d (failed to read body: %w)", operationName, resp.StatusCode, readErr)
		}
		return fmt.Errorf("%s returned status %d: %s", operationName, resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return nil
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
	return p.postJSON(ctx, p.prRegisterURL, "brain registration", payload)
}

// notifyShortFrictionHTTP posts a short-friction alert via Eri's POST /discord/message HTTP endpoint.
func (p *GitHubSkillPublisher) notifyShortFrictionHTTP(ctx context.Context, content string) error {
	payload := map[string]string{
		"channel_id": p.systemChannelID,
		"content":    content,
	}
	return p.postJSON(ctx, p.eriURL+"/discord/message", "discord message", payload)
}

// NotifyShortFriction posts a recurring short-friction alert to the configured Discord system channel via Eri HTTP.
func (p *GitHubSkillPublisher) NotifyShortFriction(ctx context.Context, content string) error {
	if p.systemChannelID == "" || strings.TrimSpace(content) == "" {
		return nil
	}
	if p.eriURL == "" {
		return errors.New("eri URL is not configured")
	}
	return p.notifyShortFrictionHTTP(ctx, content)
}

func formatShortFrictionAlert(batch SkillConsolidationBatch) string {
	ids := make([]string, 0, len(batch.Candidates))
	var steps, avoid []string
	seenSteps := make(map[string]bool)
	seenAvoid := make(map[string]bool)
	trigger := ""
	for _, c := range batch.Candidates {
		ids = append(ids, fmt.Sprintf("#%d", c.ID))
		if trigger == "" && strings.TrimSpace(c.Trigger) != "" {
			trigger = strings.TrimSpace(c.Trigger)
		}
		for _, s := range c.Steps {
			if t := strings.TrimSpace(s); t != "" && !seenSteps[t] {
				seenSteps[t] = true
				steps = append(steps, t)
			}
		}
		for _, a := range c.Avoid {
			if t := strings.TrimSpace(a); t != "" && !seenAvoid[t] {
				seenAvoid[t] = true
				avoid = append(avoid, t)
			}
		}
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "⚡ **Recurring 1-Liner Friction Alert (%dx Repeats: %s)**\n", len(batch.Candidates), strings.Join(ids, ", "))
	fmt.Fprintf(&sb, "- **Pattern**: %s\n", trigger)
	if len(steps) > 0 {
		fmt.Fprintf(&sb, "- **Observed Workaround**: %s\n", strings.Join(steps, "; "))
	}
	if len(avoid) > 0 {
		fmt.Fprintf(&sb, "- **Friction / Avoid**: %s\n", strings.Join(avoid, "; "))
	}
	sb.WriteString("Should we fix the underlying container/tooling or codify a rule?")
	return sb.String()
}

func changesRequireReview(changes []SkillFileChange) bool {
	for _, c := range changes {
		if c.RequireReview {
			return true
		}
	}
	return false
}

func appendUniqueTrimmed(dst []string, seen map[string]bool, vals ...string) []string {
	for _, v := range vals {
		if t := strings.TrimSpace(v); t != "" && !seen[t] {
			seen[t] = true
			dst = append(dst, t)
		}
	}
	return dst
}

func writeBulletIfPresent(sb *strings.Builder, label string, items []string) {
	if len(items) > 0 {
		fmt.Fprintf(sb, "- **%s**: %s\n", label, strings.Join(items, "; "))
	}
}

func formatManualReviewPRAlert(prNumber int, owner, repo string, changes []SkillFileChange) string {
	prURL := fmt.Sprintf("https://github.com/%s/%s/pull/%d", owner, repo, prNumber)
	proposals := make([]string, 0, len(changes))
	var ids, skills, triggers, steps, avoid []string
	seenIDs := make(map[string]bool)
	seenSkills := make(map[string]bool)
	seenTriggers := make(map[string]bool)
	seenSteps := make(map[string]bool)
	seenAvoid := make(map[string]bool)

	for _, ch := range changes {
		proposals = append(proposals, fmt.Sprintf("**%s** `%s` (%s)", ch.Action, ch.RelPath, ch.CommitSummary))
		for _, id := range ch.CandidateIDs {
			ids = appendUniqueTrimmed(ids, seenIDs, fmt.Sprintf("#%d", id))
		}
		for _, c := range ch.Candidates {
			if c.ID > 0 {
				ids = appendUniqueTrimmed(ids, seenIDs, fmt.Sprintf("#%d", c.ID))
			}
			if sk := strings.TrimSpace(c.LoadedSkill); sk != "" {
				skills = appendUniqueTrimmed(skills, seenSkills, fmt.Sprintf("`%s`", sk))
			}
			triggers = appendUniqueTrimmed(triggers, seenTriggers, c.Trigger)
			steps = appendUniqueTrimmed(steps, seenSteps, c.Steps...)
			avoid = appendUniqueTrimmed(avoid, seenAvoid, c.Avoid...)
		}
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "🛡️ **Manual Review Required: Procedural Skill PR [#%d](%s)** (auto-merge disabled)\n", prNumber, prURL)
	if len(ids) > 0 || len(skills) > 0 {
		fmt.Fprintf(&sb, "- **Source Candidates**: %s (Protected Skill Context: %s)\n", strings.Join(ids, ", "), strings.Join(skills, ", "))
	}
	writeBulletIfPresent(&sb, "Issue / Friction Pattern", triggers)
	writeBulletIfPresent(&sb, "Observed Steps", steps)
	writeBulletIfPresent(&sb, "Avoid Guardrails", avoid)
	writeBulletIfPresent(&sb, "Proposed PR Changes", proposals)
	sb.WriteString("Review and merge manually on GitHub if approved.")
	out := sb.String()
	if len(out) > 1800 {
		out = out[:1797] + "..."
	}
	return out
}

func formatSkillCommitMessage(changes []SkillFileChange) string {
	if len(changes) == 1 {
		return fmt.Sprintf("feat(skills): %s\n\n%s", changes[0].CommitSummary, changes[0].RelPath)
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "feat(skills): update %d procedural skills\n\n", len(changes))
	for _, c := range changes {
		fmt.Fprintf(&sb, "- %s: %s\n", c.RelPath, c.CommitSummary)
	}
	return sb.String()
}

func formatSkillPRTitle(changes []SkillFileChange) string {
	if len(changes) == 1 {
		return "feat(skills): " + changes[0].CommitSummary
	}
	return "feat(skills): evolve " + strconv.Itoa(len(changes)) + " procedural skills"
}

func formatSkillPRBody(changes []SkillFileChange, targetID string) string {
	var sb strings.Builder
	sb.WriteString("## Summary\n")
	sb.WriteString("Autonomous procedural skill updates compiled by Coven from runtime trajectory friction and execution signals.\n\n")
	effectiveTargetID := targetID
	if changesRequireReview(changes) {
		sb.WriteString("> [!IMPORTANT]\n> Manual review required: includes procedural candidates extracted while a protected core skill was active. Auto-merge is disabled.\n\n")
		effectiveTargetID = ""
	}

	sb.WriteString("## Changes\n")
	for _, c := range changes {
		fmt.Fprintf(&sb, "- **%s** `%s`: %s\n", c.Action, c.RelPath, c.CommitSummary)
	}
	sb.WriteString("\n## Verification\n")
	sb.WriteString("- Frontmatter schema and length boundaries verified by Coven validator.\n")
	sb.WriteString("- Relative paths verified against allowed custom skill domain directories.\n\n")

	trailer := fmt.Sprintf(`<!-- aerial-pr: {"target_id":%q,"vanadis_chunk_id":""} -->`, effectiveTargetID)
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

func stageCompiledSkillInMemory(diskSkills []DiskSkill, name, relPath, domain, cleanedMD string) []DiskSkill {
	for i := range diskSkills {
		if diskSkills[i].RelPath == relPath || diskSkills[i].Name == name {
			diskSkills[i].RawContent = cleanedMD
			diskSkills[i].Name = name
			return diskSkills
		}
	}
	normDomain := strings.ToLower(strings.TrimSpace(domain))
	if normDomain != domainCommon {
		normDomain = domainDiscord
	}
	return append(diskSkills, DiskSkill{
		Name:       name,
		RelPath:    relPath,
		Domain:     normDomain,
		RawContent: cleanedMD,
	})
}

func handleAlertBatch(ctx context.Context, s store.Store, publisher SkillPublisher, batch SkillConsolidationBatch, batchIDs []int64) bool {
	if batch.Action != "ALERT" {
		return false
	}
	if notifier, ok := publisher.(FrictionAlertNotifier); ok {
		if err := notifier.NotifyShortFriction(ctx, formatShortFrictionAlert(batch)); err != nil {
			log.Printf("[coven-skills] warning sending short friction alert: %v", err)
			return true
		}
	}
	if err := s.MarkProceduralCandidatesSkipped(ctx, batchIDs); err != nil {
		log.Printf("[coven-skills] warning marking alerted candidates skipped: %v", err)
	}
	return true
}

type skillCompilationState struct {
	diskSkills      []DiskSkill
	changes         []SkillFileChange
	allPublishedIDs []int64
	seenPaths       map[string]bool
	seenNames       map[string]bool
}

func compileSkillBatch(ctx context.Context, s store.Store, llmFunc LLMClientFunc, publisher SkillPublisher, batch SkillConsolidationBatch, state *skillCompilationState) {
	batchIDs := make([]int64, len(batch.Candidates))
	for i, c := range batch.Candidates {
		batchIDs[i] = c.ID
	}

	if handleAlertBatch(ctx, s, publisher, batch, batchIDs) {
		return
	}

	prompt := buildSkillDesignerPrompt(batch)
	sortedIDs := slices.Sorted(slices.Values(batchIDs))
	idParts := make([]string, len(sortedIDs))
	for i, id := range sortedIDs {
		idParts[i] = strconv.FormatInt(id, 10)
	}
	// Skill synthesis runs across all agents, so the agent segment is "all".
	rawResp, err := llmFunc(withLLMKey(ctx, "coven:skill:all:"+hash16(strings.Join(idParts, ","))), prompt)
	if err != nil {
		log.Printf("[coven-skills] error invoking skill designer LLM: %v", err)
		return
	}

	designerResp, err := parseDesignerResponse(rawResp)
	if err != nil {
		log.Printf("[coven-skills] error parsing designer response: %v", err)
		return
	}

	action := strings.ToUpper(strings.TrimSpace(designerResp.Action))
	if action == actionNoop {
		if err = s.MarkProceduralCandidatesSkipped(ctx, batchIDs); err != nil {
			log.Printf("[coven-skills] warning marking NOOP candidates skipped: %v", err)
		}
		return
	}

	if action != actionEdit && action != "CREATE" {
		log.Printf("[coven-skills] unknown designer action %q, skipping batch", designerResp.Action)
		return
	}

	domain := batch.Domain
	if domain == "" {
		domain = designerResp.Domain
	}
	if domain == "" {
		domain = domainDiscord
	}

	name, relPath, cleanedMD, err := ValidateCompiledSkillMarkdown(action, domain, designerResp.Markdown, batch.TargetSkill, state.diskSkills)
	if err != nil {
		log.Printf("[coven-skills] validation failed for %s %s: %v", action, designerResp.Name, err)
		return
	}

	if state.seenPaths[relPath] || state.seenNames[name] {
		log.Printf("[coven-skills] colliding skill name %q or path %q already staged in current pass, deferring batch", name, relPath)
		return
	}

	state.seenPaths[relPath] = true
	state.seenNames[name] = true
	state.diskSkills = stageCompiledSkillInMemory(state.diskSkills, name, relPath, domain, cleanedMD)

	commitSummary := strings.TrimSpace(designerResp.CommitSummary)
	if commitSummary == "" {
		commitSummary = fmt.Sprintf("%s skill %s", strings.ToLower(action), name)
	}

	state.changes = append(state.changes, SkillFileChange{
		RelPath:       relPath,
		Content:       cleanedMD,
		Action:        action,
		SkillName:     name,
		CommitSummary: commitSummary,
		CandidateIDs:  batchIDs,
		Candidates:    batch.Candidates,
		RequireReview: batch.RequireReview,
	})
	state.allPublishedIDs = append(state.allPublishedIDs, batchIDs...)
}

// ConsolidateProceduralSkills scans pending procedural friction candidates, clusters and compiles
// them into validated SKILL.md changes, and publishes an auto-merging PR to aerial-config.
func ConsolidateProceduralSkills(ctx context.Context, s store.Store, ollama store.OllamaClient, llmFunc LLMClientFunc, publisher SkillPublisher, customSkillsDir string) (int, error) {
	if !extractionMutex.TryLock() {
		return 0, nil
	}
	defer extractionMutex.Unlock()

	if s == nil || publisher == nil || llmFunc == nil {
		return 0, fmt.Errorf("store, publisher, and llmFunc cannot be nil")
	}

	pending, err := s.GetPendingProceduralCandidates(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get pending procedural candidates: %w", err)
	}
	if len(pending) == 0 {
		return 0, nil
	}

	cleanDir := strings.TrimSpace(customSkillsDir)
	if cleanDir == "" {
		cleanDir = config.DefaultCustomSkillsDir
	}

	diskSkills, err := ScanCustomSkills(ctx, cleanDir, ollama)
	if err != nil {
		return 0, fmt.Errorf("failed to scan custom skills from %s: %w", cleanDir, err)
	}

	batches, _ := PlanSkillConsolidation(pending, diskSkills)
	if len(batches) == 0 {
		return 0, nil
	}

	state := &skillCompilationState{
		diskSkills: diskSkills,
		seenPaths:  make(map[string]bool),
		seenNames:  make(map[string]bool),
	}

	for _, batch := range batches {
		compileSkillBatch(ctx, s, llmFunc, publisher, batch, state)
	}

	if len(state.changes) == 0 {
		return 0, nil
	}

	prNum, err := publisher.PublishSkillChanges(ctx, state.changes)
	if err != nil {
		return 0, fmt.Errorf("failed to publish skill changes: %w", err)
	}

	if prNum > 0 && len(state.allPublishedIDs) > 0 {
		if err := s.MarkProceduralCandidatesConsolidated(ctx, state.allPublishedIDs, prNum); err != nil {
			log.Printf("[coven-skills] warning marking candidates consolidated: %v", err)
		}
	}

	return prNum, nil
}
