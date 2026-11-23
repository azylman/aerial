package router

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"regexp"
	"slices"
	"strings"

	"github.com/azylman/aerial/pkg/githubapi"
	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/services/synapse/internal/buildmeta"
	"github.com/azylman/aerial/services/synapse/internal/github"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

const (
	prCIFailed          = "ci_failed"
	prClosedUnmerged    = "closed_unmerged"
	prMerged            = "merged"
	prOpen              = "open"
	statusDeploying     = "deploying"
	statusDeployed      = "deployed"
	ghEventPullRequest  = "pull_request"
	ghEventCheckRun     = "check_run"
	ghEventWorkflowRun  = "workflow_run"
	ghActionSynchronize = "synchronize"
	ghActionOpened      = "opened"
	ghActionClosed      = "closed"
	ghActionReopened    = "reopened"
)

// GitHubIssueComment represents a comment on a GitHub issue or pull request.
type GitHubIssueComment struct {
	ID   int64  `json:"id"`
	Body string `json:"body"`
}

func (s *RouterServer) ghClient() *github.Client {
	if s == nil {
		return nil
	}
	// Empty token: s.githubClient's githubauth.Transport is the only place
	// a credential is attached.
	return github.NewClient(s.githubClient, s.cfgSrc.Current().GitHubAPIURL, "")
}

// newGitHubRequest builds a GitHub REST request against the live config
// snapshot's API URL. s.githubClient's githubauth.Transport attaches the
// credential, so requests must be sent through it.
func (s *RouterServer) newGitHubRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	return githubapi.NewRequest(ctx, s.cfgSrc.Current().GitHubAPIURL, "", method, path, body)
}

func (s *RouterServer) getGitHubJSON(ctx context.Context, path string, maxBytes int64, out any) error {
	req, err := s.newGitHubRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return fmt.Errorf("create github api request: %w", err)
	}
	return githubapi.GetJSON(ctx, s.githubClient, req, maxBytes, out)
}

// GitHub Webhook Payloads and Resolution Types

type (
	GitHubPRRef       = model.GitHubPRRef
	GitHubPullRequest = model.GitHubPullRequest
	AerialPRMetadata  = model.AerialPRMetadata
)

func isPRMergeConflict(pr GitHubPullRequest) bool {
	reason := prwire.AutoMergeBlockReason(pr.MergeableState, pr.Mergeable)
	return prwire.IsActionableAutoMergeBlock(reason)
}

func isPRMergeabilityUnknown(pr GitHubPullRequest) bool {
	if pr.Mergeable != nil {
		return false
	}
	trimmedState := strings.TrimSpace(pr.MergeableState)
	return trimmedState == "" || strings.EqualFold(trimmedState, "unknown")
}

const aerialPRMetadataPrefix = "<!-- aerial-pr:"

// ParseAerialPRMetadata extracts the Aerial PR metadata JSON payload from a markdown body.
func ParseAerialPRMetadata(body string) AerialPRMetadata {
	_, after0, ok0 := strings.CutLast(body, aerialPRMetadataPrefix)
	if !ok0 {
		return AerialPRMetadata{}
	}
	rest := after0
	before, _, ok := strings.Cut(rest, "-->")
	if !ok {
		return AerialPRMetadata{}
	}
	var trailer AerialPRMetadata
	if err := json.Unmarshal([]byte(strings.TrimSpace(before)), &trailer); err != nil {
		return AerialPRMetadata{}
	}
	targetID := model.NormalizeDiscordTargetID(trailer.TargetID)
	if !model.IsValidDiscordSnowflake(targetID) && !model.IsHeadlessSessionKey(targetID) {
		targetID = ""
	}
	return AerialPRMetadata{
		TargetID: targetID,
		BotID:    strings.TrimSpace(trailer.BotID),
		Agent:    strings.TrimSpace(trailer.Agent),
	}
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

func isValidChunkID(id string) bool {
	id = strings.TrimSpace(id)
	if len(id) < 1 || len(id) > 120 {
		return false
	}
	for i := range len(id) {
		c := id[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return false
		}
	}
	return true
}

func parseAerialBranchRemainder(repo, branch, prefix string) (string, bool) {
	repoName := strings.TrimSuffix(model.CleanRepoName(repo), ".git")
	if repoName == "" {
		return "", false
	}
	clean := strings.TrimPrefix(strings.TrimSpace(branch), "refs/heads/")
	after, ok := strings.CutPrefix(clean, "feat/"+repoName+"-pr-"+prefix)
	if !ok {
		return "", false
	}
	rem, suffix, hasSuffix := strings.CutLast(after, "-")
	if !hasSuffix || len(suffix) != 6 {
		return "", false
	}
	for i := range suffix {
		if !isHexDigit(suffix[i]) {
			return "", false
		}
	}
	return rem, true
}

// ExtractChunkIDFromBranch recovers the build chunk ID encoded in a
// feat/<repo>-pr-<chunkTag>-<chunk>[-t<target_id>]-<6hex> branch name, or "" when absent.
func ExtractChunkIDFromBranch(repo, branch string) string {
	rem, ok := parseAerialBranchRemainder(repo, branch, ("van"+"adis")+"-")
	if !ok {
		return ""
	}
	chunkID := rem
	if left, target, hasTarget := strings.CutLast(rem, "-t"); hasTarget && model.IsValidDiscordSnowflake(target) {
		chunkID = left
	}
	if !isValidChunkID(chunkID) {
		return ""
	}
	return chunkID
}

// ExtractTargetIDFromBranch recovers the Discord target snowflake encoded in a
// feat/<repo>-pr-t<target_id>-<6hex> or feat/<repo>-pr-...-t<target_id>-<6hex>
// branch name, or "" when absent.
func ExtractTargetIDFromBranch(repo, branch string) string {
	rem, ok := parseAerialBranchRemainder(repo, branch, "")
	if !ok {
		return ""
	}
	if strings.HasPrefix(rem, "t") && model.IsValidDiscordSnowflake(rem[1:]) {
		return rem[1:]
	}
	if _, candidate, hasT := strings.CutLast(rem, "-t"); hasT && model.IsValidDiscordSnowflake(candidate) {
		return candidate
	}
	return ""
}

// ResolveAerialPRMetadata extracts the Aerial PR metadata from a PR body,
// falling back to branch-encoded target metadata if the body has no target.
func ResolveAerialPRMetadata(repo, branch, body string) AerialPRMetadata {
	meta := ParseAerialPRMetadata(body)
	if meta.TargetID == "" {
		meta.TargetID = ExtractTargetIDFromBranch(repo, branch)
	}
	return meta
}

// isForeignBotPR reports whether meta specifies a non-empty BotID/Agent that differs
// from the local synapse instance's configured BotID.
func isForeignBotPR(meta AerialPRMetadata, cfgBotID string) bool {
	metaBotID := strings.TrimSpace(meta.OwningAgent())
	cfgBotID = strings.TrimSpace(cfgBotID)
	return metaBotID != "" && cfgBotID != "" && !strings.EqualFold(metaBotID, cfgBotID)
}

type GitHubPingPayload struct {
	Zen    string `json:"zen"`
	HookID int64  `json:"hook_id"`
	Repo   *struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

type GitHubPullRequestPayload struct {
	Action      string            `json:"action"`
	Number      int               `json:"number"`
	Before      string            `json:"before,omitempty"`
	PullRequest GitHubPullRequest `json:"pull_request"`
	Repository  struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
	Sender struct {
		Login string `json:"login"`
	} `json:"sender"`
}

type GitHubCheckRunPR struct {
	Number int         `json:"number"`
	Head   GitHubPRRef `json:"head"`
	Base   GitHubPRRef `json:"base"`
}

type GitHubCheckRunPayload struct {
	Action   string `json:"action"`
	CheckRun struct {
		ID         int64  `json:"id"`
		Name       string `json:"name"`
		HeadSHA    string `json:"head_sha"`
		Status     string `json:"status"`
		Conclusion string `json:"conclusion"`
		DetailsURL string `json:"details_url"`
		HTMLURL    string `json:"html_url"`
		Output     struct {
			Title   string `json:"title"`
			Summary string `json:"summary"`
			Text    string `json:"text"`
		} `json:"output"`
		PullRequests []GitHubCheckRunPR `json:"pull_requests"`
	} `json:"check_run"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

type GitHubCommit struct {
	ID      string `json:"id"`
	Message string `json:"message"`
}

type GitHubPushPayload struct {
	Ref        string        `json:"ref"`
	Before     string        `json:"before"`
	After      string        `json:"after"`
	Deleted    bool          `json:"deleted"`
	HeadCommit *GitHubCommit `json:"head_commit"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

type ResolvedGitHubEvent struct {
	Event            string `json:"event"`
	Delivery         string `json:"delivery"`
	Action           string `json:"action,omitempty"`
	Repo             string `json:"repo,omitempty"`
	PRNumber         int    `json:"pr_number,omitempty"`
	Branch           string `json:"branch,omitempty"`
	HeadSHA          string `json:"head_sha,omitempty"`
	MergeSHA         string `json:"merge_sha,omitempty"`
	Merged           bool   `json:"merged,omitempty"`
	TargetID         string `json:"target_id,omitempty"`
	CheckName        string `json:"check_name,omitempty"`
	Status           string `json:"status,omitempty"`
	Conclusion       string `json:"conclusion,omitempty"`
	Sender           string `json:"sender,omitempty"`
	ResolutionSource string `json:"resolution_source,omitempty"`
	// BeforeSHA is the head a pull_request synchronize moved away from.
	BeforeSHA string `json:"before_sha,omitempty"`
}

type GitHubCommitPRItem struct {
	Number int    `json:"number"`
	State  string `json:"state"`
	Head   struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"head"`
}

var (
	squashCommitPRRegex = regexp.MustCompile(`\(#([1-9]\d*)\)\s*$`)
	mergeCommitPRRegex  = regexp.MustCompile(`^Merge pull request #([1-9]\d*)\b`)
)

func extractPRNumberFromCommitMessage(msg string) int {
	firstLine := strings.TrimSpace(strings.SplitN(strings.TrimSpace(msg), "\n", 2)[0])
	for _, re := range []*regexp.Regexp{squashCommitPRRegex, mergeCommitPRRegex} {
		if m := re.FindStringSubmatch(firstLine); len(m) == 2 {
			prNum := 0
			for _, ch := range m[1] {
				prNum = prNum*10 + int(ch-'0')
			}
			return prNum
		}
	}
	return 0
}

func verifyGitHubWebhookSignature(secret, signatureHeader string, body []byte) bool {
	return github.VerifySignature(secret, signatureHeader, body)
}

func (s *RouterServer) handleGitHubWebhook(w http.ResponseWriter, r *http.Request) {
	event := r.Header.Get("X-Github-Event")
	if strings.TrimSpace(event) == "" {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{fieldError: "missing X-GitHub-Event header"})
		return
	}
	delivery := r.Header.Get("X-Github-Delivery")

	body, ok := readWebhookBody(w, r, 5<<20)
	if !ok {
		return
	}
	var webhookSecret string
	if s != nil && s.cfgSrc != nil && s.cfgSrc.Current() != nil {
		webhookSecret = s.cfgSrc.Current().GitHubWebhookSecret
	}
	if !verifyGitHubWebhookSignature(webhookSecret, r.Header.Get("X-Hub-Signature-256"), body) {
		httpkit.WriteJSON(w, http.StatusUnauthorized, map[string]string{fieldError: "invalid github webhook signature"})
		return
	}
	if s.bus == nil {
		slog.ErrorContext(r.Context(), "[synapse] [github] event bus not initialized")
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{fieldError: "event bus not initialized"})
		return
	}
	if err := s.publishJetStreamJob(r.Context(), GitHubWebhookArgs{Event: event, Delivery: delivery, Body: body}); err != nil {
		slog.ErrorContext(r.Context(), "[synapse] [github] failed to publish job to jetstream", "error", err)
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{fieldError: "failed to persist webhook"})
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		fieldStatus: statusAccepted,
		fieldEvent:  event,
		"delivery":  delivery,
	})
}

func (*RouterServer) processGitHubPingEvent(delivery string, body []byte, res *ResolvedGitHubEvent) (*ResolvedGitHubEvent, error) {
	var p GitHubPingPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("unmarshal ping payload: %w", err)
	}
	if p.Repo != nil {
		res.Repo = p.Repo.FullName
	}
	log.Printf("[synapse] [github] [ping] hook_id=%d repo=%s zen=%q delivery=%s", p.HookID, res.Repo, p.Zen, delivery)
	return res, nil
}

func populatePullRequestEvent(p *GitHubPullRequestPayload, res *ResolvedGitHubEvent) {
	res.Repo = p.Repository.FullName
	res.Action = p.Action
	res.PRNumber = p.Number
	if res.PRNumber == 0 {
		res.PRNumber = p.PullRequest.Number
	}
	res.Branch = p.PullRequest.Head.Ref
	res.HeadSHA = p.PullRequest.Head.SHA
	res.BeforeSHA = p.Before
	res.Merged = p.PullRequest.Merged
	// GitHub pull_request webhooks set merge_commit_sha for test merges even on unmerged PRs; only set MergeSHA when merged.
	if p.PullRequest.Merged {
		res.MergeSHA = p.PullRequest.MergeCommitSHA
	}
	res.Sender = p.Sender.Login
	res.ResolutionSource = "pull_request_payload"
}

func (s *RouterServer) isForeignBotEvent(meta AerialPRMetadata) bool {
	if s.cfgSrc == nil {
		return false
	}
	cfg := s.cfgSrc.Current()
	if cfg == nil {
		return false
	}
	agent := meta.OwningAgent()
	if agent == "" {
		return false
	}
	if len(cfg.Bots) > 0 {
		for b := range cfg.Bots {
			if strings.EqualFold(b, agent) {
				return false
			}
		}
		return true
	}
	return isForeignBotPR(meta, cfg.BotID)
}

func (s *RouterServer) ProcessGitHubEvent(ctx context.Context, event, delivery string, body []byte) (res *ResolvedGitHubEvent, err error) {
	if s.onProcessed != nil {
		defer s.onProcessed(event, delivery)
	}
	res = &ResolvedGitHubEvent{Event: event, Delivery: delivery}
	// Triggers are only evaluated after successful processing to prevent destructive trigger claiming during transient errors/JetStream retries.
	defer func() {
		if err == nil && (res == nil || res.Event != ghEventCheckRun || !model.IsBuildMetadataCheck(res.CheckName)) {
			s.evaluateGitHubEventTriggers(ctx, res, body)
		}
	}()

	// Normalize and send to Restate; synapse performs no lifecycle effect.
	return s.processRestateOwnedGitHubEvent(ctx, event, delivery, body, res)
}

// autoUpdatePolicy renders the stateless auto-update config RepoObject's
// cascade needs for repo: the allowlisted authors, the default-owner flag,
// and the repo's collaborator alert rules sorted by author.
func (s *RouterServer) autoUpdatePolicy(repo string) *prwire.AutoUpdatePolicy {
	cfg := s.currentConfig()
	policy := &prwire.AutoUpdatePolicy{Authors: slices.Clone(cfg.AutoUpdatePRAuthors), DefaultOwner: cfg.DefaultOwner}
	for _, rule := range cfg.CollaboratorCIAlerts {
		if !strings.EqualFold(rule.Repo, repo) {
			continue
		}
		authors := make([]string, 0, len(rule.Authors))
		for author := range rule.Authors {
			authors = append(authors, author)
		}
		slices.Sort(authors)
		for _, author := range authors {
			if mention := strings.TrimSpace(rule.Authors[author]); mention != "" {
				policy.Collaborators = append(policy.Collaborators, prwire.CollaboratorAlert{Author: author, ChannelID: rule.ChannelID, Mention: mention})
			}
		}
	}
	return policy
}

func isDependabotBranch(branch string) bool {
	return strings.HasPrefix(strings.TrimSpace(branch), "dependabot/")
}

func isDependabotPR(p GitHubPullRequestPayload) bool {
	return isDependabotBranch(p.PullRequest.Head.Ref) &&
		(strings.EqualFold(strings.TrimSpace(p.PullRequest.User.Login), "dependabot[bot]") ||
			strings.EqualFold(strings.TrimSpace(p.Sender.Login), "dependabot[bot]"))
}

func isMainBranch(branch string) bool {
	clean := strings.TrimSpace(branch)
	return clean == "main" || strings.HasPrefix(clean, "refs/heads/main")
}

func resolveImagesWorkloadsAndNames(repo string, images []string) ([]string, []string) {
	var imageNames, workloads []string
	for _, img := range images {
		if _, imageName, workload := prwire.ResolveImageTag(repo, img); workload != "" {
			imageNames = append(imageNames, imageName)
			workloads = append(workloads, workload)
		}
	}
	return model.DedupeTrimmedStrings(imageNames), model.DedupeTrimmedStrings(workloads)
}

type GitHubWorkflowRunPayload struct {
	Action      string `json:"action"`
	WorkflowRun struct {
		ID           int64              `json:"id"`
		Name         string             `json:"name"`
		Path         string             `json:"path"`
		Event        string             `json:"event"`
		HeadSHA      string             `json:"head_sha"`
		HeadBranch   string             `json:"head_branch"`
		RunAttempt   int                `json:"run_attempt"`
		Status       string             `json:"status"`
		Conclusion   string             `json:"conclusion"`
		HTMLURL      string             `json:"html_url"`
		PullRequests []GitHubCheckRunPR `json:"pull_requests"`
	} `json:"workflow_run"`
	Repository struct {
		FullName string `json:"full_name"`
	} `json:"repository"`
}

type (
	workflowRunEvent = buildmeta.WorkflowRunEvent
)

var (
	errStaleWorkflowEvent = buildmeta.ErrStaleWorkflowEvent
	errNotImageWorkflow   = buildmeta.ErrNotImageWorkflow
)

func logBuildMetadataWarning(ctx context.Context, msg, repo string, pr int, err error) {
	slog.WarnContext(ctx, "[synapse] [build_metadata] "+msg, "repo", repo, "pr", pr, "error", err)
}

const (
	statusFailed       = github.StatusFailed
	statusPassed       = github.StatusPassed
	statusPending      = github.StatusPending
	ghStatusCompleted  = github.GHStatusCompleted
	conclusionSuccess  = github.ConclusionSuccess
	conclusionFailure  = github.ConclusionFailure
	conclusionTimedOut = github.ConclusionTimedOut
)

func (s *RouterServer) fetchLatestRoboRevComment(ctx context.Context, repo string, prNumber int, headSHA string) string {
	repo = githubapi.QualifyRepo(s.currentConfig().GitHubOwner, repo)
	if s == nil || repo == "" || prNumber <= 0 {
		return ""
	}
	var comments []GitHubIssueComment
	path := fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100", repo, prNumber)
	if err := s.getGitHubJSON(ctx, path, 2<<20, &comments); err != nil {
		slog.WarnContext(ctx, "[synapse] [github] error fetching roborev comments", "repo", repo, "pr", prNumber, "error", err)
		return ""
	}
	shortSHA := strings.TrimSpace(headSHA)
	if len(shortSHA) > 7 {
		shortSHA = shortSHA[:7]
	}
	for _, comment := range slices.Backward(comments) {
		body := strings.TrimSpace(comment.Body)
		if !strings.Contains(body, "## roborev:") && !strings.Contains(body, "## roborev-override") {
			continue
		}
		if shortSHA != "" && !strings.Contains(body, "`"+shortSHA) && !strings.Contains(body, headSHA) {
			continue
		}
		return strutil.TruncateRunes(body, 4000)
	}
	return ""
}

func formatCollaboratorCIFailureMessage(mention, repo string, prNumber int, prTitle, branch, headSHA, checkName, checkURL string) string {
	return github.BuildCollaboratorAlertMessage(mention, repo, prNumber, prTitle, branch, headSHA, checkName, checkURL)
}

func formatCollaboratorConflictMessage(mention, repo string, prNumber int, prTitle, branch, headSHA string) string {
	return github.BuildCollaboratorConflictMessage(mention, repo, prNumber, prTitle, branch, headSHA)
}

func (s *RouterServer) resolveCollaboratorAlertTarget(ctx context.Context, repo string, prNumber int, expectedSHA string, pr GitHubPullRequest) (GitHubPullRequest, string, string, bool, error) {
	if s == nil || prNumber <= 0 {
		return pr, "", "", false, nil
	}
	if strings.TrimSpace(pr.User.Login) == "" {
		var fetched GitHubPullRequest
		path := fmt.Sprintf("/repos/%s/pulls/%d", githubapi.QualifyRepo(s.currentConfig().GitHubOwner, repo), prNumber)
		if err := s.getGitHubJSON(ctx, path, 2<<20, &fetched); err != nil {
			return pr, "", "", false, err
		}
		pr = fetched
	}
	expectedSHA = strings.TrimSpace(expectedSHA)
	if pr.Merged {
		return pr, "", "", false, nil
	}
	if pr.State != "" && !strings.EqualFold(pr.State, "open") {
		return pr, "", "", false, nil
	}
	if expectedSHA != "" && pr.Head.SHA != "" && !strings.EqualFold(pr.Head.SHA, expectedSHA) {
		return pr, "", "", false, nil
	}
	if ResolveAerialPRMetadata(repo, pr.Head.Ref, pr.Body).OK() || ExtractChunkIDFromBranch(repo, pr.Head.Ref) != "" {
		return pr, "", "", false, nil
	}
	channelID, mention, ok := s.lookupCollaboratorCIAlert(repo, pr.User.Login)
	if !ok || channelID == "" {
		return pr, "", "", false, nil
	}
	return pr, channelID, mention, true, nil
}

// collaboratorDMEffect renders the collaborator DM as the eri effect the
// HandleCollaboratorAlert event carries to PRObject.
func collaboratorDMEffect(channelID, msg string) []prwire.EriEffect {
	return []prwire.EriEffect{{Kind: prwire.EffectEriDirectMessage, ChannelID: channelID, Content: msg}}
}

func (s *RouterServer) notifyCollaboratorConflict(ctx context.Context, repo string, pr GitHubPullRequest) error {
	if s == nil || !s.hasCollaboratorCIAlertsForRepo(repo) {
		return nil
	}
	resolvedPR, channelID, mention, ok, err := s.resolveCollaboratorAlertTarget(ctx, repo, pr.Number, pr.Head.SHA, pr)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	sha := pr.Head.SHA
	if resolvedPR.Head.SHA != "" {
		sha = resolvedPR.Head.SHA
	}
	prNum := resolvedPR.Number
	branch := pr.Head.Ref
	if resolvedPR.Head.Ref != "" {
		branch = resolvedPR.Head.Ref
	}
	msg := formatCollaboratorConflictMessage(mention, repo, prNum, resolvedPR.Title, branch, sha)
	// The event carries the rendered DM; PRObject owns alert dedup and
	// sends it.
	return s.sendOwnedPR(ctx, prwire.PRHandlerCollaboratorAlert, prwire.PREvent{
		Repo: repo, PRNumber: prNum, HeadSHA: sha, Branch: branch,
		Title: resolvedPR.Title, Unmanaged: true,
		EriEffects: collaboratorDMEffect(channelID, msg),
	})
}

func (s *RouterServer) notifyCollaboratorCIFailure(ctx context.Context, res *ResolvedGitHubEvent, checkURL string) error {
	if s == nil || res == nil || res.TargetID != "" || res.PRNumber <= 0 || res.HeadSHA == "" || prwire.IsZeroSHA(res.HeadSHA) || !s.hasCollaboratorCIAlertsForRepo(res.Repo) {
		return nil
	}
	resolvedPR, channelID, mention, ok, err := s.resolveCollaboratorAlertTarget(ctx, res.Repo, res.PRNumber, res.HeadSHA, GitHubPullRequest{
		Number: res.PRNumber,
		Head:   GitHubPRRef{Ref: res.Branch, SHA: res.HeadSHA},
	})
	if err != nil || !ok {
		return err
	}
	branch := strutil.FirstNonEmpty(resolvedPR.Head.Ref, res.Branch)
	sha := strutil.FirstNonEmpty(resolvedPR.Head.SHA, res.HeadSHA)
	msg := formatCollaboratorCIFailureMessage(mention, res.Repo, resolvedPR.Number, resolvedPR.Title, branch, sha, res.CheckName, checkURL)
	return s.sendOwnedPR(ctx, prwire.PRHandlerCollaboratorAlert, prwire.PREvent{
		Repo: res.Repo, PRNumber: resolvedPR.Number, HeadSHA: sha, MergeSHA: res.MergeSHA,
		CheckName: res.CheckName, Branch: branch, Title: resolvedPR.Title, Unmanaged: true,
		EriEffects: collaboratorDMEffect(channelID, msg),
	})
}

func syncPRFallbackFields(res *ResolvedGitHubEvent, pr GitHubPullRequest) GitHubPullRequest {
	if res == nil {
		return pr
	}
	if pr.Number == 0 && res.PRNumber > 0 {
		pr.Number = res.PRNumber
	}
	if pr.Head.Ref == "" && res.Branch != "" {
		pr.Head.Ref = res.Branch
	}
	if pr.Head.SHA == "" && res.HeadSHA != "" {
		pr.Head.SHA = res.HeadSHA
	}
	return pr
}

func (s *RouterServer) resolveAutoMergeDisabledPR(ctx context.Context, res *ResolvedGitHubEvent, pr GitHubPullRequest) (GitHubPullRequest, bool, error) {
	if !isPRMergeabilityUnknown(pr) || res.PRNumber <= 0 || strings.TrimSpace(res.Repo) == "" {
		return pr, true, nil
	}
	fetchedModel, err := github.FetchSinglePRWithMergeabilityRetry(ctx, s.ghClient(), res.Repo, res.PRNumber, 3)
	if err != nil {
		return pr, false, err
	}
	if fetchedModel.Merged || (fetchedModel.State != "" && !strings.EqualFold(fetchedModel.State, "open")) {
		return pr, false, nil
	}
	if res.HeadSHA != "" && fetchedModel.Head.SHA != "" && !strings.EqualFold(fetchedModel.Head.SHA, strings.TrimSpace(res.HeadSHA)) {
		return pr, false, nil
	}
	fetched := fetchedModel
	return syncPRFallbackFields(res, fetched), true, nil
}

// resolvePRFromCommitSHA discovers the PR for a commit SHA via the GitHub
// API, the stateless fallback when the RepoObject SHA index has no answer.
func (s *RouterServer) resolvePRFromCommitSHA(ctx context.Context, repo, sha string) (int, string, error) {
	return github.ResolvePRFromCommitSHA(ctx, s.ghClient(), repo, sha)
}
