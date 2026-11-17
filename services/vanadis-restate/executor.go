package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

const (
	defaultShareRoot       = "/share"
	maxRoboRevCommentRunes = 4000
	maxPromptRunes         = 6000
)

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

	declKeywordPrefixes = []string{"func ", "type ", "var ", "const ", "export ", "function ", "class "}
)

// BrainDispatchRequest represents the JSON payload dispatched to Eri's POST /dispatch endpoint.
type BrainDispatchRequest struct {
	ID             string `json:"id,omitempty"`
	Source         string `json:"source"`
	SessionID      string `json:"session_id,omitempty"`
	TargetID       string `json:"target_id,omitempty"`
	VanadisChunkID string `json:"vanadis_chunk_id,omitempty"`
	Prompt         string `json:"prompt"`
	Effort         string `json:"effort,omitempty"`
	SkipIfActive   bool   `json:"skip_if_active,omitempty"`
}

// StatusWidgetChunk represents a chunk in a Vanadis build or a PR task for widget rendering.
type StatusWidgetChunk struct {
	ID            string    `json:"id,omitempty"`
	Index         int       `json:"index"`
	Repo          string    `json:"repo"`
	Title         string    `json:"title"`
	Status        string    `json:"status"`
	ActiveStep    string    `json:"active_step,omitempty"`
	PRNumber      int       `json:"pr_number,omitempty"`
	DeployingJobs []string  `json:"deploying_jobs,omitempty"`
	StartedAt     time.Time `json:"started_at,omitempty"`
	CompletedAt   time.Time `json:"completed_at,omitempty"`
	UpdatedAt     time.Time `json:"updated_at,omitempty"`
}

// StatusWidgetRequest represents a request to deliver or update a Discord status tracker widget.
type StatusWidgetRequest struct {
	ChannelID string              `json:"channel_id"`
	MessageID string              `json:"message_id,omitempty"`
	ID        string              `json:"id"`
	Tier      int                 `json:"tier,omitempty"`
	Title     string              `json:"title"`
	Status    string              `json:"status"`
	Banner    string              `json:"banner,omitempty"`
	SkipSink  bool                `json:"skip_sink,omitempty"`
	Chunks    []StatusWidgetChunk `json:"chunks"`
	StartedAt time.Time           `json:"started_at,omitempty"`
}

// SyncChunkPRWidgetRequest carries optional streaming state for a single PR chunk.
type SyncChunkPRWidgetRequest struct {
	PRNumber   int    `json:"pr_number"`
	PRStatus   string `json:"pr_status"`
	PRBanner   string `json:"pr_banner"`
	ActiveStep string `json:"active_step,omitempty"`
	PRFound    bool   `json:"pr_found"`
}

// EriClient describes the outbound communication interface to Eri.
type EriClient interface {
	Dispatch(ctx context.Context, req BrainDispatchRequest) error
	DispatchAccepted(ctx context.Context, req BrainDispatchRequest) error
	CancelDispatch(ctx context.Context, chunkID string) error
	Prompt(ctx context.Context, threadID, prompt string) error
	Widget(ctx context.Context, req StatusWidgetRequest) (string, error)
	Message(ctx context.Context, channelID, content string) error
}

// HTTPEriClient implements EriClient against Eri's HTTP endpoints.
type HTTPEriClient struct {
	eriURL     string
	httpClient *http.Client
}

// NewHTTPEriClient builds an Eri client pointing at eriURL.
func NewHTTPEriClient(eriURL string, httpClient *http.Client) *HTTPEriClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &HTTPEriClient{
		eriURL:     strings.TrimRight(strings.TrimSpace(eriURL), "/"),
		httpClient: httpClient,
	}
}

func (c *HTTPEriClient) postEriJSON(ctx context.Context, path string, payload any, maxBytes int64) ([]byte, error) {
	if c == nil || c.httpClient == nil {
		return nil, errors.New("eri http client is nil")
	}
	if c.eriURL == "" {
		return nil, errors.New("eri url not configured")
	}
	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	url := c.eriURL + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("create eri request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("post eri request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if readErr != nil {
			return nil, fmt.Errorf("eri returned status %d (failed to read body: %w)", resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("eri returned status %d: %s", resp.StatusCode, string(respBody))
	}
	return io.ReadAll(io.LimitReader(resp.Body, maxBytes))
}

func (c *HTTPEriClient) Dispatch(ctx context.Context, req BrainDispatchRequest) error {
	_, err := c.postEriJSON(ctx, "/dispatch", req, 64<<10)
	return err
}

// DispatchAccepted requires durable acceptance of the exact requested generation.
// skipped_active is a retryable rejection, even though Eri returns HTTP 200.
func (c *HTTPEriClient) DispatchAccepted(ctx context.Context, req BrainDispatchRequest) error {
	body, err := c.postEriJSON(ctx, "/dispatch", req, 64<<10)
	if err != nil {
		return err
	}
	var result struct {
		Status     string `json:"status"`
		DispatchID string `json:"dispatch_id"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return fmt.Errorf("decode eri dispatch acceptance: %w", err)
	}
	if result.Status == "skipped_active" {
		return errDispatchActive
	}
	if req.ID == "" || result.DispatchID != req.ID || (result.Status != "accepted" && result.Status != "already_exists") {
		return fmt.Errorf("eri did not accept dispatch %q (status %q, identity %q)", req.ID, result.Status, result.DispatchID)
	}
	return nil
}

func (c *HTTPEriClient) CancelDispatch(ctx context.Context, chunkID string) error {
	chunkID = strings.TrimSpace(chunkID)
	if chunkID == "" {
		return nil
	}
	payload := map[string]string{"chunk_id": chunkID}
	_, err := c.postEriJSON(ctx, "/dispatch/cancel", payload, 4<<10)
	return err
}

func (c *HTTPEriClient) Prompt(ctx context.Context, threadID, prompt string) error {
	cleanThreadID, cleanPrompt := strings.TrimSpace(threadID), strings.TrimSpace(prompt)
	if cleanThreadID == "" || cleanPrompt == "" {
		return nil
	}
	payload := map[string]string{
		"channel_id": cleanThreadID,
		"prompt":     cleanPrompt,
	}
	_, err := c.postEriJSON(ctx, "/prompt", payload, 4<<10)
	return err
}

func (c *HTTPEriClient) Widget(ctx context.Context, req StatusWidgetRequest) (string, error) {
	respBody, err := c.postEriJSON(ctx, "/discord/widget", req, 4<<10)
	if err != nil {
		return "", err
	}
	var res struct {
		MessageID string `json:"message_id"`
	}
	if uErr := json.Unmarshal(respBody, &res); uErr != nil {
		return "", fmt.Errorf("unmarshal eri widget response: %w", uErr)
	}
	return res.MessageID, nil
}

func (c *HTTPEriClient) Message(ctx context.Context, channelID, content string) error {
	cleanChannelID, cleanContent := strings.TrimSpace(channelID), strings.TrimSpace(content)
	if cleanChannelID == "" || cleanContent == "" {
		return nil
	}
	payload := map[string]string{
		"channel_id": cleanChannelID,
		"content":    cleanContent,
	}
	_, err := c.postEriJSON(ctx, "/discord/message", payload, 4<<10)
	return err
}

// Executor coordinates AST symbol resolution, prompt building, CI logs, widget sync, chunk dispatch, and event publishing.
type Executor struct {
	shareRoot    string
	eriClient    EriClient
	bus          pubsub.Bus
	gitHubToken  string
	gitHubAPIURL string
	httpClient   *http.Client
	deployUIURL  string
}

// NewExecutor creates a new Executor instance.
func NewExecutor(shareRoot string, eriClient EriClient, bus pubsub.Bus, gitHubToken string, httpClient *http.Client, deployUIURL ...string) *Executor {
	if shareRoot == "" {
		shareRoot = defaultShareRoot
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	var dURL string
	if len(deployUIURL) > 0 {
		dURL = deployUIURL[0]
	}
	return &Executor{
		shareRoot:    shareRoot,
		eriClient:    eriClient,
		bus:          bus,
		gitHubToken:  gitHubToken,
		gitHubAPIURL: "https://api.github.com",
		httpClient:   httpClient,
		deployUIURL:  dURL,
	}
}

// FormatChunkPrompt renders a structured prompt for executing a specific chunk.
func (e *Executor) FormatChunkPrompt(build VanadisBuild, chunks []VanadisChunk, activeIdx int) (string, error) {
	root := defaultShareRoot
	if e != nil && e.shareRoot != "" {
		root = e.shareRoot
	}
	return formatChunkPromptWithShareRoot(build, chunks, activeIdx, root)
}

// ExecuteLaunchChunk reserves the chunk in PostgreSQL and dispatches it directly to Eri via POST /dispatch.
// It explicitly DOES NOT publish to aerial.jobs.vanadis.chunk.
func (e *Executor) ExecuteLaunchChunk(ctx context.Context, store VanadisStore, build VanadisBuild, chunks []VanadisChunk, activeIdx int, notice string) error {
	if e == nil || e.eriClient == nil {
		return errors.New("executor or eri client is nil")
	}
	if activeIdx < 0 || activeIdx >= len(chunks) {
		return fmt.Errorf("chunk index %d out of bounds", activeIdx)
	}
	chunk := chunks[activeIdx]

	var reserveErr error
	if store != nil {
		reserveErr = store.ReserveChunkForLaunch(ctx, chunk.ID)
	}
	if reserveErr != nil && chunk.Mode == chunkModeAction && chunk.StallNudges > 0 {
		return fmt.Errorf("reserve action generation: %w", reserveErr)
	}
	if reserveErr != nil && !errors.Is(reserveErr, ErrChunkNotFound) {
		log.Printf("[vanadis-restate] warning: failed reserving chunk %s for launch: %v", chunk.ID, reserveErr)
	}

	prompt, err := e.FormatChunkPrompt(build, chunks, activeIdx)
	if err != nil {
		return fmt.Errorf("format chunk prompt: %w", err)
	}

	req := BrainDispatchRequest{
		ID:             actionDispatchID(chunk),
		Source:         "vanadis-restate",
		SessionID:      "vanadis-" + strings.TrimSpace(chunk.ID),
		TargetID:       strings.TrimSpace(build.ThreadID),
		VanadisChunkID: strings.TrimSpace(chunk.ID),
		Prompt:         notice + prompt,
		Effort:         capChunkEffort(chunk.Effort),
		SkipIfActive:   true,
	}

	if chunk.Mode == chunkModeAction && chunk.StallNudges > 0 {
		return e.eriClient.DispatchAccepted(ctx, req)
	}
	return e.eriClient.Dispatch(ctx, req)
}

// CancelChunkDispatch calls Eri's POST /dispatch/cancel for a chunk.
func (e *Executor) CancelChunkDispatch(ctx context.Context, chunkID string) error {
	if e == nil || e.eriClient == nil {
		return nil
	}
	return e.eriClient.CancelDispatch(ctx, chunkID)
}

// DispatchWakePrompt dispatches a prompt to a Discord thread.
func (e *Executor) DispatchWakePrompt(ctx context.Context, threadID, prompt string) error {
	if e == nil || e.eriClient == nil {
		return nil
	}
	return e.eriClient.Prompt(ctx, threadID, prompt)
}

// DispatchDirectMessage posts a message to a Discord channel.
func (e *Executor) DispatchDirectMessage(ctx context.Context, channelID, content string) error {
	if e == nil || e.eriClient == nil {
		return nil
	}
	return e.eriClient.Message(ctx, channelID, content)
}

// SyncBuildWidget formats the Discord tracker widget and dispatches it directly to Eri via POST /discord/widget.
// It explicitly DOES NOT publish to aerial.coalesced.widget_sync.>.
func (e *Executor) SyncBuildWidget(ctx context.Context, store VanadisStore, build VanadisBuild, chunks []VanadisChunk, notice string) (string, error) {
	if e == nil || e.eriClient == nil {
		return "", nil
	}
	widgetReq := buildSyncWidgetRequest(build, chunks, e.deployUIURL)
	if notice != "" {
		widgetReq.Banner = notice
	}
	msgID, err := e.eriClient.Widget(ctx, widgetReq)
	if err != nil {
		return "", fmt.Errorf("sync widget to eri: %w", err)
	}
	if msgID != "" && msgID != build.StatusMessageID && store != nil {
		if uErr := store.UpdateBuildStatusMessageID(ctx, build.ID, msgID); uErr != nil {
			log.Printf("[vanadis-restate] warning: update status message id: %v", uErr)
		}
	}
	return msgID, nil
}

// PublishExternalEvent publishes external events strictly for haro / audit consumers.
// It publishes to aerial.events.<source>.<eventType>.
func (e *Executor) PublishExternalEvent(ctx context.Context, source, eventType string, targets []string, contextHeader string) error {
	if e == nil || e.bus == nil {
		return nil
	}
	evID := fmt.Sprintf("%s-%s-%s", source, eventType, strings.Join(targets, "-"))
	payload := pubsub.ExternalEvent{
		EventID:       evID,
		Source:        source,
		EventType:     eventType,
		Targets:       targets,
		ContextHeader: contextHeader,
		OccurredAt:    time.Now().UTC(),
	}
	subj := fmt.Sprintf("aerial.events.%s.%s", source, eventType)
	return e.bus.Publish(ctx, subj, evID, payload)
}

// ResolveCIFailureDetails extracts failing logs or RoboRev findings for a PR check.
func (e *Executor) ResolveCIFailureDetails(ctx context.Context, repo string, prNumber int, checkName, headSHA, checkURL, details string) string {
	if prNumber > 0 && isRoboRevCheck(checkName) {
		if comment := e.fetchLatestRoboRevComment(ctx, repo, prNumber, headSHA); comment != "" {
			return comment
		}
	}
	if needsCIJobLogFetch(details) {
		if jobID := extractActionsJobID(checkURL); jobID > 0 {
			if logDetails := e.fetchCheckJobFailureLogs(ctx, repo, jobID); logDetails != "" {
				return logDetails
			}
		}
	}
	return details
}

func (e *Executor) newGitHubRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	apiURL := firstNonEmpty(e.gitHubAPIURL, "https://api.github.com")
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(apiURL, "/")+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	if token := strings.TrimSpace(e.gitHubToken); token != "" {
		req.Header.Set("Authorization", "token "+token)
	}
	return req, nil
}

func resolveGitHubRepoPath(repo string) string {
	clean := strings.Trim(strings.TrimSpace(repo), "/")
	if clean == "" {
		return ""
	}
	if strings.Contains(clean, "/") {
		return clean
	}
	return defaultRepoOwner + "/" + clean
}

func (e *Executor) fetchCheckJobFailureLogs(ctx context.Context, repo string, jobID int64) string {
	if e == nil || e.httpClient == nil {
		return ""
	}
	repoPath := resolveGitHubRepoPath(repo)
	if repoPath == "" || jobID <= 0 {
		return ""
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := e.newGitHubRequest(timeoutCtx, http.MethodGet, fmt.Sprintf("/repos/%s/actions/jobs/%d/logs", repoPath, jobID), nil)
	if err != nil {
		return ""
	}
	resp, err := e.httpClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ""
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return ""
	}
	if len(body) > 256<<10 {
		body = body[len(body)-(256<<10):]
	}
	return ExtractCIFailureDetails(string(body))
}

type gitHubIssueComment struct {
	ID   int64  `json:"id"`
	Body string `json:"body"`
}

func (e *Executor) fetchLatestRoboRevComment(ctx context.Context, repo string, prNumber int, headSHA string) string {
	repoPath := resolveGitHubRepoPath(repo)
	if e == nil || repoPath == "" || prNumber <= 0 || e.httpClient == nil {
		return ""
	}
	var comments []gitHubIssueComment
	path := fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100", repoPath, prNumber)
	req, err := e.newGitHubRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return ""
	}
	resp, err := e.httpClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		if resp != nil {
			resp.Body.Close()
		}
		return ""
	}
	defer resp.Body.Close()
	respBody, rErr := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if rErr != nil {
		return ""
	}
	if uErr := json.Unmarshal(respBody, &comments); uErr != nil {
		return ""
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
		return truncatePromptRunes(body, maxRoboRevCommentRunes)
	}
	return ""
}

// FormatCIFailurePrompt formats the failure prompt when a CI check fails.
func FormatCIFailurePrompt(ev pubsub.PRLifecycleEvent, details string) string {
	var extraParts []string
	cleanBranch := strings.TrimPrefix(strings.TrimSpace(ev.Branch), "refs/heads/")
	if cleanBranch != "" {
		extraParts = append(extraParts, "Branch: "+cleanBranch)
	}
	if cleanURL := strings.TrimSpace(ev.CheckURL); cleanURL != "" {
		extraParts = append(extraParts, "Check URL: "+cleanURL)
	}
	extraLine := strings.Join(extraParts, "\n")

	shortRepo := firstNonEmpty(cleanRepoName(ev.Repo), "aerial")
	branchArg := firstNonEmpty(cleanBranch, "<branch>")
	checkoutAndSubmit := fmt.Sprintf("Check out the existing PR branch via '/share/aerial/scripts/aerial-pr.sh init %s --branch %s' and push updates via '/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> \"<commit_msg>\"' (PR_DESCRIPTION.md is not required when updating an existing PR; never run bare 'git push').", shortRepo, branchArg)

	directive := "Please investigate and fix the failure by inspecting the failing logs, diagnosing the issue, verifying with local checks, and pushing a fix. " + checkoutAndSubmit
	if isRoboRevCheck(ev.CheckName) {
		directive = fmt.Sprintf("Please critically evaluate each RoboRev finding against codebase reality and architectural intent. Fix verified issues, or record rebuttal via '/share/aerial/scripts/aerial-pr.sh --repo %s override-roborev %d '<technical_rebuttal>''. %s", shortRepo, ev.PRNumber, checkoutAndSubmit)
	}
	return formatErrorDetailsBlock(
		fmt.Sprintf("CI check %q failed for PR #%d on %s (head commit: %s).", ev.CheckName, ev.PRNumber, ev.Repo, ev.HeadSHA),
		details, extraLine,
		directive,
	)
}

// FormatConflictPrompt formats the conflict prompt when merge conflict is detected.
func FormatConflictPrompt(ev pubsub.PRLifecycleEvent) string {
	shortRepo := firstNonEmpty(cleanRepoName(ev.Repo), "aerial")
	cleanBranch := firstNonEmpty(strings.TrimPrefix(strings.TrimSpace(ev.Branch), "refs/heads/"), "<branch>")
	return fmt.Sprintf("Merge conflict detected on PR #%d on %s (branch: %s, head: %s). Please check out the branch via '/share/aerial/scripts/aerial-pr.sh init %s --branch %s', merge origin/main to resolve conflicts, verify locally, and push to update the PR via '/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> \"<commit_msg>\"' (PR_DESCRIPTION.md is not required when updating an existing PR; never run bare 'git push').", ev.PRNumber, ev.Repo, cleanBranch, ev.HeadSHA, shortRepo, cleanBranch)
}

// FormatDeployFailureBanner formats the chunk banner when deployment or git sync fails.
func FormatDeployFailureBanner(ev pubsub.PRLifecycleEvent) string {
	var banner string
	if strings.EqualFold(strings.TrimSpace(ev.Event), "sync_failed") {
		banner = "Git sync failed"
	} else if cleanJob := strings.TrimSpace(ev.JobName); cleanJob != "" {
		banner = "Deployment failed for " + cleanJob
	} else {
		banner = "Deployment failed"
	}
	if cleanDetails := strings.TrimSpace(ev.Details); cleanDetails != "" {
		banner = fmt.Sprintf("%s: %s", banner, cleanDetails)
	}
	return banner
}

func formatDeployFailureHeader(repo, commit, jobName string, prNumber int) string {
	if cleanJob := strings.TrimSpace(jobName); cleanJob != "" {
		return fmt.Sprintf("Deployment failed for job %s on %s (PR #%d, commit: %s).", cleanJob, repo, prNumber, commit)
	}
	return fmt.Sprintf("Deployment failed for PR #%d on %s (commit: %s).", prNumber, repo, commit)
}

// FormatDeployFailurePrompt formats the failure prompt when deployment or git sync fails.
func FormatDeployFailurePrompt(ev pubsub.PRLifecycleEvent) string {
	commit := firstNonEmpty(ev.MergeSHA, ev.HeadSHA)
	cleanEvent := strings.ToLower(strings.TrimSpace(ev.Event))
	var extraLine string
	cleanBranch := strings.TrimPrefix(strings.TrimSpace(ev.Branch), "refs/heads/")
	if cleanBranch != "" {
		extraLine = "Branch: " + cleanBranch
	}
	if cleanEvent == "sync_failed" {
		header := fmt.Sprintf("Git sync failed for PR #%d on %s (commit: %s).", ev.PRNumber, ev.Repo, commit)
		return formatErrorDetailsBlock(header, ev.Details, extraLine, "Please investigate and fix the git sync failure.")
	}
	header := formatDeployFailureHeader(ev.Repo, commit, ev.JobName, ev.PRNumber)
	return formatErrorDetailsBlock(header, ev.Details, extraLine, "Please investigate and fix the deployment failure.")
}

func formatErrorDetailsBlock(header, details, extraLine, footer string) string {
	lines := []string{header}
	if clean := strings.TrimSpace(details); clean != "" {
		lines = append(lines, fmt.Sprintf("Error details:\n```\n%s\n```", truncatePromptDetails(clean)))
	}
	if cleanExtra := strings.TrimSpace(extraLine); cleanExtra != "" {
		lines = append(lines, cleanExtra)
	}
	if footer != "" {
		lines = append(lines, footer)
	}
	return strings.Join(lines, "\n")
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

func isRoboRevCheck(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	return strings.Contains(lower, "roborev")
}

func needsCIJobLogFetch(details string) bool {
	return strings.TrimSpace(details) == "" || strings.EqualFold(details, "failure") || strings.EqualFold(details, "timed_out")
}

func extractActionsJobID(url string) int64 {
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

func truncatePromptRunes(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "... (truncated)"
}

func truncatePromptDetails(s string) string {
	return truncatePromptRunes(strings.ReplaceAll(s, "```", "'''"), maxPromptRunes)
}

// Widget formatting helpers
func buildSyncWidgetRequest(build VanadisBuild, chunks []VanadisChunk, deployUIURL ...string) StatusWidgetRequest {
	widgetStatus := firstNonEmpty(normalizeWidgetStatus(build.Status), statusRunning)
	baseURL := firstNonEmpty(deployUIURL...)
	wChunks := make([]StatusWidgetChunk, len(chunks))
	for i, c := range chunks {
		wChunks[i] = buildStatusWidgetChunk(c, baseURL)
	}
	return StatusWidgetRequest{
		ChannelID: build.ThreadID,
		MessageID: build.StatusMessageID,
		ID:        build.ID,
		Tier:      build.Tier,
		Title:     firstNonEmpty(strings.TrimSpace(build.Title), build.Objective),
		Status:    widgetStatus,
		Banner:    build.Objective,
		SkipSink:  build.Quiet,
		Chunks:    wChunks,
		StartedAt: build.CreatedAt,
	}
}

func buildStatusWidgetChunk(c VanadisChunk, deployUIURL ...string) StatusWidgetChunk {
	baseURL := firstNonEmpty(deployUIURL...)
	chunkRepo := firstNonEmpty(c.Repo, "aerial")
	chunkStatus := normalizeWidgetStatus(c.Status)
	activeStep := FormatChunkActiveStep(c, baseURL)
	return StatusWidgetChunk{
		ID:          c.ID,
		Index:       c.ChunkIndex,
		Repo:        chunkRepo,
		Title:       c.Title,
		Status:      chunkStatus,
		ActiveStep:  activeStep,
		PRNumber:    c.PRNumber,
		StartedAt:   c.StartedAt,
		CompletedAt: c.CompletedAt,
		UpdatedAt:   c.UpdatedAt,
	}
}

func normalizeWidgetStatus(status string) string {
	s := strings.ToLower(strings.TrimSpace(status))
	switch s {
	case "coding", statusRunning:
		return statusRunning
	case statusPROpened, statusOpen:
		return statusOpen
	case statusCompleted, "deployed":
		return statusCompleted
	case statusMerged:
		return statusMerged
	case statusFailed, "crashed", "timeout":
		return statusFailed
	case statusCancelled:
		return statusCancelled
	default:
		return s
	}
}

var buildJobRegex = regexp.MustCompile(`(?i)Build\s*(?:&|and)\s*(?:Push|Publish)(?:\s+(?:Sidecar\s+)?(?:Container\s+)?Images?(?:\s+to\s+GHCR)?)?\s*\(([^)]+)\)`)

func buildJobService(checkName string) (string, bool) {
	svc := strings.TrimSpace(append(buildJobRegex.FindStringSubmatch(checkName), "", "")[1])
	return svc, svc != "" && !strings.Contains(svc, "${{")
}

func dedupeTrimmedStrings(items []string) []string {
	seen := make(map[string]bool, len(items))
	out := make([]string, 0, len(items))
	for _, it := range items {
		clean := strings.TrimSpace(it)
		if clean == "" {
			continue
		}
		lower := strings.ToLower(clean)
		if seen[lower] {
			continue
		}
		seen[lower] = true
		out = append(out, clean)
	}
	return out
}

const statusPassed = "passed"

func formatOpenCIChecks(checks map[string]string) string {
	var total, passed, failed int
	for _, state := range checks {
		s := strings.ToLower(strings.TrimSpace(state))
		switch s {
		case statusPassed, dispatchStatusSuccess:
			passed++
			total++
		case statusFailed, "failure":
			failed++
			total++
		default:
			if s != "" {
				total++
			}
		}
	}
	if failed > 0 || total == 0 {
		return ""
	}
	return fmt.Sprintf("Waiting on CI (%d/%d passed)", passed, total)
}

func analyzeChunkBuildChecks(checks map[string]string) (checkJobs []string, passedBuildJobs map[string]bool, total, passed, failed int) {
	matchedServices := make(map[string]struct{})
	passedBuildJobs = make(map[string]bool)

	for checkName, state := range checks {
		s := strings.ToLower(strings.TrimSpace(state))
		switch s {
		case statusPassed, dispatchStatusSuccess:
			passed++
			total++
		case statusFailed, "failure":
			failed++
			total++
		default:
			if s != "" {
				total++
			}
		}

		if svc, ok := buildJobService(checkName); ok {
			matchedServices[svc] = struct{}{}
			if s == statusPassed || s == dispatchStatusSuccess {
				passedBuildJobs[strings.ToLower(svc)] = true
			}
		}
	}

	if len(matchedServices) > 0 {
		for svc := range matchedServices {
			checkJobs = append(checkJobs, svc)
		}
		sort.Strings(checkJobs)
		checkJobs = dedupeTrimmedStrings(checkJobs)
	}
	return checkJobs, passedBuildJobs, total, passed, failed
}

func formatDeployTargetLabel(name, kind, deployUIURL string) string {
	base := strings.TrimRight(strings.TrimSpace(deployUIURL), "/")
	if base == "" {
		return fmt.Sprintf("`%s`", name)
	}
	cleanName := strings.TrimSpace(name)
	var route string
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "kustomization":
		route = fmt.Sprintf("%s/c/main/flux/kustomize/kustomizations/flux-system/%s", base, cleanName)
	default:
		switch strings.ToLower(cleanName) {
		case "git-sync":
			route = base + "/c/main/flux/sources"
		case "github-runner":
			route = base + "/c/main/flux/helmreleases"
		default:
			route = fmt.Sprintf("%s/c/main/deployments/default/%s", base, cleanName)
		}
	}
	return fmt.Sprintf("[`%s`](%s)", name, route)
}

func formatSingleJobRolloutState(job, cleanStatus string, deployedSet, startedSet map[string]bool, jobStates map[string]string, passedBuildJobs map[string]bool, hasStartedJobs bool, deployUIURL ...string) string {
	baseURL := firstNonEmpty(deployUIURL...)
	label := formatDeployTargetLabel(job, "job", baseURL)
	jKey := strings.ToLower(strings.TrimSpace(job))
	switch {
	case deployedSet[jKey]:
		return label + ": ✅ healthy"
	case startedSet[jKey] && strings.EqualFold(jobStates[jKey], "verifying_health"):
		return label + ": 🔄 verifying health"
	case startedSet[jKey]:
		return label + ": 🔄 starting"
	case (!hasStartedJobs && cleanStatus == statusDeploying) || passedBuildJobs[jKey]:
		return label + ": 🔄 starting"
	default:
		return label + ": ⏳ building image"
	}
}

func formatPerJobRolloutStatus(allJobs []string, cleanStatus string, startedJobs, deployedJobs []string, jobStates map[string]string, passedBuildJobs map[string]bool, deployUIURL ...string) string {
	baseURL := firstNonEmpty(deployUIURL...)
	deployedSet := make(map[string]bool, len(deployedJobs))
	for _, j := range deployedJobs {
		deployedSet[strings.ToLower(strings.TrimSpace(j))] = true
	}
	startedSet := make(map[string]bool, len(startedJobs))
	for _, j := range startedJobs {
		startedSet[strings.ToLower(strings.TrimSpace(j))] = true
	}
	hasStartedJobs := len(startedJobs) > 0

	lines := make([]string, 0, len(allJobs))
	for _, j := range allJobs {
		lines = append(lines, formatSingleJobRolloutState(j, cleanStatus, deployedSet, startedSet, jobStates, passedBuildJobs, hasStartedJobs, baseURL))
	}
	return strings.Join(lines, "\n")
}

func formatKustomizationRolloutStatus(cleanStatus string, kustomizationStatuses map[string]string, deployUIURL ...string) (string, bool) {
	if cleanStatus != statusDeploying {
		return "", false
	}
	baseURL := firstNonEmpty(deployUIURL...)
	names := make([]string, 0, len(kustomizationStatuses))
	for name := range kustomizationStatuses {
		names = append(names, name)
	}
	sort.Strings(names)
	lines := make([]string, 0, len(names))
	for _, name := range names {
		status := strings.ToLower(strings.TrimSpace(kustomizationStatuses[name]))
		state := "🔄 rolling out"
		if status == statusDeployed || status == "healthy" || status == "rolled_out" {
			state = "✅ rolled out"
		}
		label := formatDeployTargetLabel(name, "kustomization", baseURL)
		lines = append(lines, label+": "+state)
	}
	return strings.Join(lines, "\n"), true
}

func resolveCandidateJobs(rs *pubsub.PRRolloutState, checkJobs []string) []string {
	if rs == nil {
		return checkJobs
	}
	combined := append(append(append([]string(nil), rs.ExpectedJobs...), rs.StartedJobs...), rs.DeployedJobs...)
	if candidateJobs := dedupeTrimmedStrings(combined); len(candidateJobs) > 0 {
		sort.Strings(candidateJobs)
		return candidateJobs
	}
	return checkJobs
}

// FormatChunkActiveStep formats the step string for a chunk in a widget.
func FormatChunkActiveStep(chunk VanadisChunk, deployUIURL ...string) string {
	baseURL := firstNonEmpty(deployUIURL...)
	cleanStatus := strings.ToLower(strings.TrimSpace(chunk.Status))
	if cleanStatus == statusOpen || cleanStatus == statusPROpened {
		if len(chunk.CIChecks) > 0 {
			return formatOpenCIChecks(chunk.CIChecks)
		}
		return ""
	}
	if cleanStatus != statusMerged && cleanStatus != statusSynced && cleanStatus != statusDeploying {
		return ""
	}

	checkJobs, passedBuildJobs, total, passed, failed := analyzeChunkBuildChecks(chunk.CIChecks)
	if failed > 0 {
		return ""
	}

	if chunk.RolloutState != nil && len(chunk.RolloutState.KustomizationStatuses) > 0 {
		if rollout, ok := formatKustomizationRolloutStatus(cleanStatus, chunk.RolloutState.KustomizationStatuses, baseURL); ok {
			return rollout
		}
	}

	allJobs := resolveCandidateJobs(chunk.RolloutState, checkJobs)
	if len(allJobs) > 0 {
		var startedJobs, deployedJobs []string
		var jobStates map[string]string
		if chunk.RolloutState != nil {
			startedJobs = chunk.RolloutState.StartedJobs
			deployedJobs = chunk.RolloutState.DeployedJobs
			jobStates = chunk.RolloutState.JobStates
		}
		return formatPerJobRolloutStatus(allJobs, cleanStatus, startedJobs, deployedJobs, jobStates, passedBuildJobs, baseURL)
	}

	if (cleanStatus == statusMerged || cleanStatus == statusSynced) && total > 0 && failed == 0 {
		return fmt.Sprintf("Building container images (%d/%d completed)", passed, total)
	}

	if cleanStatus == statusDeploying {
		return "Deploying to cluster"
	}

	return ""
}

// AST Symbol Spans resolution
func repoCheckoutDir(shareRoot, repo string) (string, bool) {
	name := strings.TrimSpace(repo)
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	if name == "" || name == "." || name == ".." {
		return "", false
	}
	return filepath.Join(strings.TrimSpace(shareRoot), name), true
}

func normalizeSymbolName(symbol string) string {
	sym := strings.TrimSpace(symbol)
	sym = strings.TrimSuffix(sym, "()")
	if idx := strings.LastIndex(sym, "."); idx >= 0 {
		sym = sym[idx+1:]
	}
	return strings.TrimSpace(sym)
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

func isIdentByte(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

func isBoundedIdent(line string, start, end int) bool {
	if start > 0 && isIdentByte(line[start-1]) {
		return false
	}
	if end < len(line) && isIdentByte(line[end]) {
		return false
	}
	return true
}

func containsIdent(line, ident string) bool {
	if ident == "" {
		return false
	}
	idx := 0
	for {
		pos := strings.Index(line[idx:], ident)
		if pos < 0 {
			return false
		}
		start := idx + pos
		end := start + len(ident)
		if isBoundedIdent(line, start, end) {
			return true
		}
		idx = start + 1
	}
}

func stripLineComment(line string) string {
	if before, _, found := strings.Cut(line, "//"); found {
		return before
	}
	return line
}

// FindSymbolSpan locates the 1-based start and end line for symbol in content.
func FindSymbolSpan(content, symbol string) (int, int, bool) {
	sym := normalizeSymbolName(symbol)
	if sym == "" || content == "" {
		return 0, 0, false
	}
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	for idx, line := range lines {
		clean := strings.TrimSpace(stripLineComment(line))
		if hasAnyPrefix(clean, declKeywordPrefixes) && containsIdent(clean, sym) {
			end := idx + 1
			for j := idx + 1; j < len(lines); j++ {
				if strings.HasPrefix(lines[j], "}") {
					end = j + 1
					break
				}
			}
			return idx + 1, end, true
		}
	}
	for idx, line := range lines {
		clean := strings.TrimSpace(stripLineComment(line))
		if clean != "" && containsIdent(clean, sym) {
			return idx + 1, idx + 1, true
		}
	}
	return 0, 0, false
}

// ResolveSymbolSpans resolves start and end lines for target symbols in repo checkout under shareRoot.
func ResolveSymbolSpans(shareRoot, repo string, targetFiles, targetSymbols []string) ([]string, bool) {
	repoDir, ok := repoCheckoutDir(shareRoot, repo)
	if !ok {
		return nil, false
	}
	var spans []string
	allResolved := true
	for _, f := range targetFiles {
		cleanFile := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(f), "./"), "/")
		fullPath := filepath.Join(repoDir, cleanFile)
		data, err := os.ReadFile(fullPath)
		if err != nil {
			allResolved = false
			continue
		}
		content := string(data)
		var fileSpans []string
		for _, sym := range targetSymbols {
			start, end, found := FindSymbolSpan(content, sym)
			if found {
				fileSpans = append(fileSpans, fmt.Sprintf("%s (L%d-L%d)", sym, start, end))
			} else {
				allResolved = false
			}
		}
		if len(fileSpans) > 0 {
			spans = append(spans, fmt.Sprintf("- %s: %s", cleanFile, strings.Join(fileSpans, ", ")))
		}
	}
	return spans, allResolved && len(spans) > 0
}

func formatChunkPromptWithShareRoot(build VanadisBuild, chunks []VanadisChunk, activeIdx int, shareRoot string) (string, error) {
	if len(chunks) == 0 {
		return "", errors.New("chunks slice cannot be empty")
	}
	if activeIdx < 0 || activeIdx >= len(chunks) {
		return "", fmt.Errorf("active chunk index %d out of range [0, %d)", activeIdx, len(chunks))
	}
	total := len(chunks)
	active := chunks[activeIdx]
	isActionChunk := active.Mode == chunkModeAction
	activeRepo := ""
	if !isActionChunk {
		activeRepo = firstNonEmpty(cleanRepoName(active.Repo), "aerial")
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: %s\n", build.ID)
	if bTitle := strings.TrimSpace(build.Title); bTitle != "" {
		fmt.Fprintf(&sb, "Build Title: %s\n", bTitle)
	}
	if isActionChunk {
		fmt.Fprintf(&sb, "Dispatch ID: %s\nInclude this dispatch_id in action completion and failure calls.\n", actionDispatchID(active))
	}
	if !isActionChunk {
		fmt.Fprintf(&sb, "Repository: %s\n", activeRepo)
	}
	fmt.Fprintf(&sb, "Tier: %d\n\n=== Build Objective ===\n%s\n\n", build.Tier, strings.TrimSpace(build.Objective))
	if sharedCtx := strings.TrimSpace(build.SharedContext); sharedCtx != "" {
		fmt.Fprintf(&sb, "=== Shared Build Context ===\n%s\n\n", sharedCtx)
	}

	sb.WriteString("=== Completed Chunks Ledger ===\n")
	if activeIdx == 0 {
		sb.WriteString("(none — this is the first chunk)\n\n")
	} else {
		for k, c := range chunks[:activeIdx] {
			fmt.Fprintf(&sb, "- [Chunk %d/%d] %s\n", k+1, total, c.Title)
		}
		sb.WriteString("\n")
	}

	targetFilesStr := firstNonEmpty(strings.Join(active.TargetFiles, ", "), "(none specified)")
	fmt.Fprintf(&sb, "=== Active Chunk (Chunk %d of %d) ===\nChunk ID: %s\nTitle: %s\n", activeIdx+1, total, active.ID, active.Title)
	if isActionChunk {
		fmt.Fprintf(&sb, "Dispatch ID: %s\nInclude this dispatch_id in action completion and failure calls.\n", actionDispatchID(active))
	}
	if !isActionChunk {
		fmt.Fprintf(&sb, "Repository: %s\nTarget Files: %s\n", activeRepo, targetFilesStr)
		if len(active.TargetSymbols) > 0 {
			fmt.Fprintf(&sb, "Target Symbols: %s\n", strings.Join(active.TargetSymbols, ", "))
		}
		spans, _ := ResolveSymbolSpans(shareRoot, activeRepo, active.TargetFiles, active.TargetSymbols)
		if len(spans) > 0 {
			fmt.Fprintf(&sb, "Target Symbol Spans:\n%s\n", strings.Join(spans, "\n"))
		}
	}
	fmt.Fprintf(&sb, "Instructions:\n%s\n\n", active.Instructions)

	sb.WriteString("=== Remaining Chunks ===\n")
	if activeIdx == total-1 {
		sb.WriteString("(none — this is the final chunk)\n\n")
	} else {
		for k, c := range chunks[activeIdx+1:] {
			fmt.Fprintf(&sb, "- [Chunk %d/%d] %s\n", activeIdx+2+k, total, c.Title)
		}
		sb.WriteString("\n")
	}

	fmt.Fprintf(&sb, "=== Mandatory Execution Contract ===\n"+
		"1. Execute chunk instructions directly in this turn.\n"+
		"2. Submit via `/share/aerial/scripts/aerial-pr.sh submit \"$SCRATCH_DIR\" \"<commit_msg>\" --vanadis-chunk %s`.\n", active.ID)

	return sb.String(), nil
}

var errDispatchActive = errors.New("dispatch active; reconciliation deferred")
