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
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/github"
	"github.com/azylman/aerial/services/synapse/internal/model"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
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
)

// GitHubIssueComment represents a comment on a GitHub issue or pull request.
type GitHubIssueComment struct {
	ID   int64  `json:"id"`
	Body string `json:"body"`
}

func (s *RouterServer) publishPRLifecycleEvent(ctx context.Context, ev pubsub.PRLifecycleEvent) error {
	if s == nil || s.bus == nil {
		return nil
	}
	ev.Repo = normalizeRepo(ev.Repo)
	if ev.OccurredAt.IsZero() {
		ev.OccurredAt = time.Now().UTC()
	}
	subject := pubsub.SubjectAerialPRPrefix + strings.TrimSpace(ev.Event)
	msgID := fmt.Sprintf("pr-life-%s-%d-%s-%d", strings.ReplaceAll(ev.Repo, "/", "-"), ev.PRNumber, ev.Event, ev.OccurredAt.UnixNano())
	if err := s.bus.Publish(ctx, subject, msgID, ev); err != nil {
		slog.ErrorContext(ctx, "[synapse] [pr_lifecycle] error publishing event", "event", ev.Event, "repo", ev.Repo, "pr", ev.PRNumber, "error", err)
		return err
	}
	return nil
}

func (s *RouterServer) loadPRRolloutState(ctx context.Context, repo string, prNum int) *pubsub.PRRolloutState {
	if s == nil || s.registry == nil || prNum <= 0 || strings.TrimSpace(repo) == "" {
		return nil
	}
	meta, exists, err := s.registry.LoadRawPRMetadata(ctx, repo, prNum)
	if err != nil || !exists || len(meta) == 0 {
		return nil
	}
	// Only the identity-validated snapshot for the PR's current revision renders.
	// Projection-safe: read-model render for rollout state display, never an
	// effect decision.
	delete(meta, "build_metadata")
	if m, found, err := s.registry.LoadPRBuildMetadata(ctx, repo, prNum); err == nil && found {
		meta["build_metadata"] = m
	}
	return buildPRRolloutState(meta)
}

func (s *RouterServer) ghClient() *github.Client {
	if s == nil {
		return nil
	}
	cfg := s.cfgSrc.Current()
	return github.NewClient(s.githubClient, cfg.GitHubAPIURL, cfg.GitHubToken)
}

// newGitHubRequest builds an authenticated GitHub REST request against the
// live config snapshot's API URL and token.
func (s *RouterServer) newGitHubRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	cfg := s.cfgSrc.Current()
	return github.NewRequest(ctx, cfg.GitHubAPIURL, cfg.GitHubToken, method, path, body)
}

func isGitHubNotFound(err error) bool {
	return github.IsNotFound(err)
}

func (s *RouterServer) getGitHubJSON(ctx context.Context, path string, maxBytes int64, out any) error {
	req, err := s.newGitHubRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return fmt.Errorf("create github api request: %w", err)
	}
	return github.GetJSON(ctx, s.githubClient, req, maxBytes, out)
}

func (s *RouterServer) dispatchGitPush(ctx context.Context, repo, ref, commit, targetID string, prNum int) error {
	if s.dispatcher == nil || !strings.HasPrefix(ref, "refs/heads/main") || isZeroSHA(commit) || (!s.cfgSrc.Current().DefaultOwner && targetID == "") {
		return nil
	}
	if err := s.dispatcher.DispatchGitPush(ctx, GitPushEventRequest{
		Repo:     repo,
		Ref:      ref,
		Commit:   commit,
		TargetID: targetID,
		PRNumber: prNum,
	}); err != nil {
		slog.ErrorContext(ctx, "[synapse] [dispatcher] error dispatching git_push", "repo", repo, "error", err)
		return fmt.Errorf("dispatch git_push: %w", err)
	}
	return nil
}

// GitHub Webhook Payloads and Resolution Types

type (
	GitHubPRRef       = model.GitHubPRRef
	GitHubPullRequest = model.GitHubPullRequest
	AerialPRMetadata  = model.AerialPRMetadata
)

func isPRMergeConflict(pr GitHubPullRequest) bool {
	reason := github.ExtractAutoMergeBlockReason(pr.MergeableState, pr.Mergeable)
	return github.IsActionableAutoMergeBlock(reason)
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
	targetID := NormalizeDiscordTargetID(trailer.TargetID)
	if !IsValidDiscordSnowflake(targetID) && !IsHeadlessSessionKey(targetID) {
		targetID = ""
	}
	return AerialPRMetadata{
		TargetID: targetID,
		BotID:    strings.TrimSpace(trailer.BotID),
		Agent:    strings.TrimSpace(trailer.Agent),
	}
}

// isForeignBotPR reports whether meta specifies a non-empty BotID/Agent that differs
// from the local synapse instance's configured BotID.
func isForeignBotPR(meta AerialPRMetadata, cfgBotID string) bool {
	metaBotID := strings.TrimSpace(meta.OwningAgent())
	cfgBotID = strings.TrimSpace(cfgBotID)
	return metaBotID != "" && cfgBotID != "" && !strings.EqualFold(metaBotID, cfgBotID)
}

func (s *RouterServer) isPROwnedLocally(ctx context.Context, repo string, prNum int) bool {
	if s == nil || s.registry == nil || prNum <= 0 {
		return false
	}
	targetID, branch, _, _, err := s.registry.ResolvePRByNumber(ctx, repo, prNum)
	if err != nil {
		return false
	}
	return strings.TrimSpace(targetID) != "" || strings.TrimSpace(branch) != ""
}

func (s *RouterServer) resolveOwningAgent(ctx context.Context, repo string, prNum int, prBody string) string {
	defaultOwner := defaultOwnerAgent
	if s != nil && s.cfgSrc != nil && s.cfgSrc.Current() != nil {
		defaultOwner = s.cfgSrc.Current().DefaultOwnerAgent()
	}
	if prBody != "" {
		if agent := ParseAerialPRMetadata(prBody).OwningAgent(); agent != "" {
			return agent
		}
	}
	if s == nil || s.registry == nil || prNum <= 0 || repo == "" {
		return defaultOwner
	}
	meta, _, err := s.registry.LoadRawPRMetadata(ctx, repo, prNum)
	if err != nil || len(meta) == 0 {
		return defaultOwner
	}
	if a, ok := meta["agent"].(string); ok && strings.TrimSpace(a) != "" {
		return strings.TrimSpace(a)
	}
	if b, ok := meta["bot_id"].(string); ok && strings.TrimSpace(b) != "" {
		return strings.TrimSpace(b)
	}
	return defaultOwner
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

func isZeroSHA(sha string) bool {
	return strings.Trim(sha, "0") == ""
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
	if err := s.publishJetStreamJob(r.Context(), GitHubWebhookArgs{Event: event, Delivery: delivery, Body: body, EffectsOwner: newEventEffectsOwner()}); err != nil {
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

func (s *RouterServer) resolveEventPR(ctx context.Context, res *ResolvedGitHubEvent, prs []GitHubCheckRunPR, payloadSource string) {
	if len(prs) > 0 {
		res.PRNumber = prs[0].Number
		if res.Branch == "" {
			res.Branch = prs[0].Head.Ref
		}
		res.ResolutionSource = payloadSource
		if s.registry != nil && res.HeadSHA != "" && !isZeroSHA(res.HeadSHA) {
			if _, _, targetID, err := s.registry.ResolvePRBySHA(ctx, res.Repo, res.HeadSHA); err == nil && targetID != "" {
				res.TargetID = targetID
			}
		}
		return
	}
	if res.HeadSHA == "" || isZeroSHA(res.HeadSHA) {
		return
	}
	if s.registry != nil {
		if prNum, branch, targetID, err := s.registry.ResolvePRBySHA(ctx, res.Repo, res.HeadSHA); err != nil {
			slog.WarnContext(ctx, "[synapse] [registry] failed resolving PR by SHA locally", "event", res.Event, "head_sha", res.HeadSHA, "error", err)
		} else if prNum > 0 {
			res.PRNumber = prNum
			if res.Branch == "" || strings.HasPrefix(res.Branch, "refs/heads/") {
				res.Branch = firstNonEmpty(branch, res.Branch)
			}
			res.TargetID = targetID
			res.ResolutionSource = "local_pr_registry"
			return
		}
	}
	if prNum, branch, err := s.resolvePRFromCommitSHA(ctx, res.Repo, res.HeadSHA); err != nil {
		slog.WarnContext(ctx, "[synapse] [github] failed resolving PR for commit", "event", res.Event, "head_sha", res.HeadSHA, "error", err)
	} else {
		res.PRNumber = prNum
		if res.Branch == "" || strings.HasPrefix(res.Branch, "refs/heads/") {
			res.Branch = firstNonEmpty(branch, res.Branch)
		}
		res.ResolutionSource = "commit_sha_lookup"
	}
}

func (s *RouterServer) handleCIFailureTransition(ctx context.Context, res *ResolvedGitHubEvent, details, checkURL string) error {
	if s.isStaleOrIgnoredCIFailure(ctx, res) {
		return nil
	}
	if handled, err := s.handleMergeOrPromotionFailure(ctx, res, details); handled {
		return err
	}
	emitAdditional, wasActive, _ := s.recordNonGateCheckFailureDetailed(res)
	var (
		updated bool
		err     error
	)
	if res.PRNumber > 0 {
		updated, err = s.handleCIFailureByPRNumber(ctx, res, details, checkURL, emitAdditional, wasActive)
	} else if res.HeadSHA != "" && !isZeroSHA(res.HeadSHA) {
		updated, err = s.handleCIFailureByHeadSHA(ctx, res, details, checkURL)
	}
	if err != nil {
		return err
	}
	if !updated && res.TargetID == "" {
		if notifyErr := s.notifyCollaboratorCIFailure(ctx, res, checkURL); notifyErr != nil {
			return notifyErr
		}
	}
	// The registry atomic transition is the once-per-failure gate for the
	// open-PR conflict cascade the deleted failed-CISHA mark used to own.
	if updated {
		if cascadeErr := s.checkOpenPRConflicts(ctx, res.Repo); cascadeErr != nil {
			slog.ErrorContext(ctx, "[synapse] [github] error checking open pr conflicts on ci failure", "repo", res.Repo, "error", cascadeErr)
		}
	}
	return nil
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

func (s *RouterServer) processGitHubPullRequestEvent(ctx context.Context, delivery string, body []byte, res *ResolvedGitHubEvent) (*ResolvedGitHubEvent, error) {
	var p GitHubPullRequestPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("unmarshal pull_request payload: %w", err)
	}
	populatePullRequestEvent(&p, res)
	slog.InfoContext(ctx, "[synapse] [github] [pull_request] event", "repo", res.Repo, "pr", res.PRNumber, "action", res.Action, "head_sha", res.HeadSHA, "merge_sha", res.MergeSHA, "branch", res.Branch, "merged", p.PullRequest.Merged, "sender", res.Sender, "delivery", delivery)

	if s.isForeignBotEvent(ParseAerialPRMetadata(p.PullRequest.GetBody())) {
		return res, nil
	}

	s.handleDependabotPROpen(ctx, res, p)

	if s.registry != nil && res.PRNumber > 0 {
		if err := s.handlePullRequestRegistryAction(ctx, res, p.PullRequest); err != nil {
			return nil, err
		}
	}
	if res.Action == "reopened" && res.PRNumber > 0 && strings.TrimSpace(res.HeadSHA) != "" {
		if err := s.routePREvent(ctx, "HandleReopened", restateclient.PREvent{
			Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA,
			Branch: res.Branch, Title: p.PullRequest.Title,
		}); err != nil {
			return nil, err
		}
	}
	if res.Action == ghActionOpened && res.PRNumber > 0 {
		if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        ghActionOpened,
			Repo:         res.Repo,
			PRNumber:     res.PRNumber,
			Branch:       res.Branch,
			HeadSHA:      res.HeadSHA,
			Title:        p.PullRequest.Title,
			TargetID:     res.TargetID,
			Status:       prOpen,
			RolloutState: s.loadPRRolloutState(ctx, res.Repo, res.PRNumber),
			Quiet:        isDependabotBranch(res.Branch),
			OccurredAt:   time.Now().UTC(),
		}); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func (s *RouterServer) recordCheckRunStatus(ctx context.Context, res *ResolvedGitHubEvent) {
	if s.registry == nil || res.PRNumber <= 0 || res.HeadSHA == "" || (isGateAggregatorCheck(res.CheckName) || isBuildMetadataCheck(res.CheckName)) {
		return
	}
	if isDependabotBranch(res.Branch) {
		return
	}
	checkState := classifyCheckState(res.Status, res.Conclusion)
	if checkState == "" || checkState == statusFailed {
		return
	}
	// Journal the check status into PRObject before the legacy write; the
	// legacy registry stays authoritative, so a journal failure only logs.
	if err := s.routePREvent(ctx, "HandleCheckStatus", restateclient.PREvent{
		Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, Branch: res.Branch,
		CheckName: res.CheckName, CheckState: checkState,
	}); err != nil {
		slog.WarnContext(ctx, "[synapse] [restate-cutover] failed to route check status", "repo", res.Repo, "pr", res.PRNumber, "check", res.CheckName, "error", err)
	}
	updated, err := s.registry.RecordPRCheckStatus(ctx, res.Repo, res.PRNumber, res.HeadSHA, res.CheckName, checkState)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [registry] error recording check status", "repo", res.Repo, "pr", res.PRNumber, "check", res.CheckName, "error", err)
		return
	}
	if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
		Event:        "check_updated",
		Repo:         res.Repo,
		PRNumber:     res.PRNumber,
		Branch:       res.Branch,
		HeadSHA:      res.HeadSHA,
		TargetID:     res.TargetID,
		Status:       prOpen,
		CheckName:    res.CheckName,
		CheckState:   checkState,
		RolloutState: s.loadPRRolloutState(ctx, res.Repo, res.PRNumber),
		Quiet:        isDependabotBranch(res.Branch),
		OccurredAt:   time.Now().UTC(),
	}); err != nil {
		slog.WarnContext(ctx, "[synapse] [pr_lifecycle] failed publishing lifecycle event", "error", err)
	}
	_ = updated
}

func (s *RouterServer) processGitHubCheckRunEvent(ctx context.Context, delivery string, body []byte, res *ResolvedGitHubEvent) (*ResolvedGitHubEvent, error) {
	var p GitHubCheckRunPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("unmarshal check_run payload: %w", err)
	}
	res.Repo = p.Repository.FullName
	res.Action = p.Action
	res.CheckName = p.CheckRun.Name
	res.HeadSHA = p.CheckRun.HeadSHA
	res.Status = p.CheckRun.Status
	res.Conclusion = p.CheckRun.Conclusion
	if isBuildMetadataCheck(res.CheckName) {
		if err := s.handleBuildMetadataCheck(ctx, p, res); err != nil {
			return nil, err
		}
		return res, nil
	}
	s.resolveEventPR(ctx, res, p.CheckRun.PullRequests, "check_run_payload")
	slog.InfoContext(ctx, "[synapse] [github] [check_run] event", "repo", res.Repo, "check", res.CheckName, "head_sha", res.HeadSHA, "pr", res.PRNumber, "branch", res.Branch, "status", res.Status, "conclusion", res.Conclusion, "source", res.ResolutionSource, "target_id", res.TargetID, "delivery", delivery)

	recovered := false
	if p.Action == ghStatusCompleted && res.Conclusion == conclusionSuccess && res.PRNumber > 0 && s.registry != nil && s.canRecoverCheckSuccess(ctx, res.Repo, res.PRNumber, res.CheckName, res.HeadSHA) {
		targetID, err := s.registry.UpdatePRSync(ctx, res.Repo, res.PRNumber, res.HeadSHA)
		if err != nil {
			return nil, err
		}
		recovered = true
		res.TargetID = firstNonEmpty(res.TargetID, targetID)
		s.populateMissingBranch(ctx, res)
		if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        ghActionSynchronize,
			Repo:         res.Repo,
			PRNumber:     res.PRNumber,
			Branch:       res.Branch,
			HeadSHA:      res.HeadSHA,
			TargetID:     res.TargetID,
			Status:       prOpen,
			RolloutState: s.loadPRRolloutState(ctx, res.Repo, res.PRNumber),
			Quiet:        isDependabotBranch(res.Branch),
			OccurredAt:   time.Now().UTC(),
		}); err != nil {
			slog.WarnContext(ctx, "[synapse] [pr_lifecycle] failed publishing recovery synchronize event", "repo", res.Repo, "pr", res.PRNumber, "error", err)
		}
	}

	s.recordCheckRunStatus(ctx, res)

	details := firstNonEmpty(strings.TrimSpace(p.CheckRun.Output.Text), strings.TrimSpace(p.CheckRun.Output.Summary), strings.TrimSpace(p.CheckRun.Output.Title))
	if lines := github.ExtractLogErrorLines(details, 10); len(lines) > 0 {
		details = strings.Join(lines, "\n")
	}
	details = github.SanitizeDetailsForPrompt(details, 2000)
	checkURL := firstNonEmpty(strings.TrimSpace(p.CheckRun.HTMLURL), strings.TrimSpace(p.CheckRun.DetailsURL))
	if err := s.handleCIFailureTransition(ctx, res, details, checkURL); err != nil {
		return nil, err
	}
	if err := s.reportCheckRunGateSuccess(ctx, p, res, recovered); err != nil {
		return nil, err
	}
	return res, nil
}

// reportCheckRunGateSuccess re-checks open-PR conflicts when a gate check
// (or a recovered success) lands, then journals the check outcome to the
// Restate journal in authoritative mode or the shadow mirror otherwise.
func (s *RouterServer) reportCheckRunGateSuccess(ctx context.Context, p GitHubCheckRunPayload, res *ResolvedGitHubEvent, recovered bool) error {
	if p.Action == ghStatusCompleted && res.Conclusion == conclusionSuccess && (isGateAggregatorCheck(res.CheckName) || isRoboRevCheck(res.CheckName) || recovered) && res.PRNumber > 0 {
		if err := s.checkOpenPRConflicts(ctx, res.Repo); err != nil {
			slog.ErrorContext(ctx, "[synapse] [github] error checking open pr conflicts on gate success", "repo", res.Repo, "error", err)
		}
	}
	return s.routeCheckOutcome(ctx, res)
}

func (s *RouterServer) resolveAndBackfillPushPR(ctx context.Context, res *ResolvedGitHubEvent, p GitHubPushPayload) error {
	s.resolveEventPR(ctx, res, nil, "")
	if res.PRNumber == 0 && p.HeadCommit != nil {
		if prNum := extractPRNumberFromCommitMessage(p.HeadCommit.Message); prNum > 0 {
			res.PRNumber = prNum
			res.ResolutionSource = "commit_message"
		}
	}
	if res.PRNumber > 0 {
		res.MergeSHA = res.HeadSHA
	}
	slog.InfoContext(ctx, "[synapse] [github] [push] event", "repo", res.Repo, "ref", p.Ref, "head_sha", res.HeadSHA, "resolved_pr", res.PRNumber, "branch", res.Branch, "source", res.ResolutionSource, "target_id", res.TargetID, "delivery", res.Delivery)

	if s.registry != nil && res.PRNumber > 0 && res.MergeSHA != "" {
		targetID, err := s.registry.BackfillPushMergeSHA(ctx, res.Repo, res.PRNumber, res.MergeSHA)
		if err != nil {
			return err
		}
		res.TargetID = firstNonEmpty(res.TargetID, targetID)
		// Journal before the legacy publish: a journal failure leaves no
		// merged event behind, so the redelivery retries the journal.
		if err := s.routePREvent(ctx, "HandleMerged", restateclient.PREvent{
			Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA,
			MergeSHA: res.MergeSHA, Branch: res.Branch,
		}); err != nil {
			return err
		}
		if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        prMerged,
			Repo:         res.Repo,
			PRNumber:     res.PRNumber,
			Branch:       res.Branch,
			HeadSHA:      res.HeadSHA,
			MergeSHA:     res.MergeSHA,
			TargetID:     res.TargetID,
			Status:       prMerged,
			RolloutState: s.loadPRRolloutState(ctx, res.Repo, res.PRNumber),
			Quiet:        isDependabotBranch(res.Branch),
			OccurredAt:   time.Now().UTC(),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *RouterServer) processGitHubPushEvent(ctx context.Context, body []byte, res *ResolvedGitHubEvent) (*ResolvedGitHubEvent, error) {
	var p GitHubPushPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("unmarshal push payload: %w", err)
	}
	res.Repo = p.Repository.FullName
	res.Branch = p.Ref

	if p.Deleted || isZeroSHA(p.After) {
		res.ResolutionSource = "branch_deleted"
		return res, nil
	}
	if !strings.HasPrefix(p.Ref, "refs/heads/") {
		res.ResolutionSource = "non_branch_ref"
		return res, nil
	}
	res.HeadSHA = p.After
	if p.HeadCommit != nil && p.HeadCommit.ID != "" {
		res.HeadSHA = p.HeadCommit.ID
	}
	if !strings.HasPrefix(p.Ref, "refs/heads/main") {
		res.ResolutionSource = "non_main_branch"
		return res, nil
	}

	// Journal before every legacy side effect on the push path
	// (backfill, conflict checks, git_push dispatch): a journal failure
	// leaves nothing behind, so the redelivery retries the journal instead
	// of re-emitting the dispatch or duplicating downstream work.
	if err := s.routeRepoEvent(ctx, "HandleMainPush", restateclient.RepoEvent{
		Repo: res.Repo, MainSHA: res.HeadSHA,
	}); err != nil {
		return nil, err
	}

	if err := s.resolveAndBackfillPushPR(ctx, res, p); err != nil {
		return nil, err
	}

	if err := s.checkOpenPRConflicts(ctx, res.Repo); err != nil {
		slog.ErrorContext(ctx, "[synapse] [github] error checking open pr conflicts", "repo", res.Repo, "error", err)
	}
	if err := s.dispatchGitPush(ctx, res.Repo, p.Ref, res.HeadSHA, res.TargetID, res.PRNumber); err != nil {
		return nil, err
	}
	return res, nil
}

func (s *RouterServer) ProcessGitHubEvent(ctx context.Context, event, delivery string, body []byte) (res *ResolvedGitHubEvent, err error) {
	if s.onProcessed != nil {
		defer s.onProcessed(event, delivery)
	}
	res = &ResolvedGitHubEvent{Event: event, Delivery: delivery}
	// Triggers are only evaluated after successful processing to prevent destructive trigger claiming during transient errors/JetStream retries.
	defer func() {
		if err == nil && (res == nil || res.Event != ghEventCheckRun || !isBuildMetadataCheck(res.CheckName)) {
			s.evaluateGitHubEventTriggers(ctx, res, body)
		}
	}()

	switch event {
	case "ping":
		return s.processGitHubPingEvent(delivery, body, res)
	case ghEventPullRequest:
		return s.processGitHubPullRequestEvent(ctx, delivery, body, res)
	case ghEventCheckRun:
		return s.processGitHubCheckRunEvent(ctx, delivery, body, res)
	case ghEventWorkflowRun:
		return s.processGitHubWorkflowRunEvent(ctx, delivery, body, res)
	case "push":
		return s.processGitHubPushEvent(ctx, body, res)
	default:
		slog.InfoContext(ctx, "[synapse] [github] [unhandled] event", "event", event, "delivery", delivery, "payload_size_bytes", len(body))
		res.ResolutionSource = "unhandled_event"
		return res, nil
	}
}
