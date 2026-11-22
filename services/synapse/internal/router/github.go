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

	"github.com/azylman/aerial/pkg/githubapi"
	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
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
		if err == nil && (res == nil || res.Event != ghEventCheckRun || !isBuildMetadataCheck(res.CheckName)) {
			s.evaluateGitHubEventTriggers(ctx, res, body)
		}
	}()

	// Normalize and send to Restate; synapse performs no lifecycle effect.
	return s.processRestateOwnedGitHubEvent(ctx, event, delivery, body, res)
}
