package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
)

const (
	prCIFailed          = "ci_failed"
	prClosedUnmerged    = "closed_unmerged"
	prMerged            = "merged"
	prOpen              = "open"
	statusDeploying     = "deploying"
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
		log.Printf("[synapse] [pr_lifecycle] error publishing event %s for %s#%d: %v", ev.Event, ev.Repo, ev.PRNumber, err)
		return err
	}
	return nil
}

func (s *RouterServer) loadPRRolloutState(ctx context.Context, repo string, prNum int) *pubsub.PRRolloutState {
	if s == nil || s.registry == nil || prNum <= 0 || strings.TrimSpace(repo) == "" {
		return nil
	}
	pgReg, ok := s.registry.(*PostgresPRRegistry)
	if !ok || pgReg == nil || pgReg.pool == nil {
		return nil
	}
	qCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var metaBytes []byte
	err := pgReg.pool.QueryRow(qCtx, `SELECT COALESCE(metadata, '{}'::jsonb) FROM pr_registry WHERE repo = $1 AND pr_number = $2 LIMIT 1;`, normalizeRepo(repo), prNum).Scan(&metaBytes)
	if err != nil || len(metaBytes) == 0 {
		return nil
	}
	var meta map[string]any
	if err := json.Unmarshal(metaBytes, &meta); err != nil || meta == nil {
		return nil
	}
	return buildPRRolloutState(meta)
}

// newGitHubRequest builds an authenticated GitHub REST request against the
// live config snapshot's API URL and token.
func (s *RouterServer) newGitHubRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	cfg := s.cfgSrc.Current()
	req, err := http.NewRequestWithContext(ctx, method, firstNonEmpty(cfg.GitHubAPIURL, "https://api.github.com")+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	if cfg.GitHubToken != "" {
		req.Header.Set("Authorization", "token "+cfg.GitHubToken)
	}
	return req, nil
}

func (s *RouterServer) getGitHubJSON(ctx context.Context, path string, maxBytes int64, out any) error {
	req, err := s.newGitHubRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return fmt.Errorf("create github api request: %w", err)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("github api request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("github api rate limit or forbidden (remaining: %s)", resp.Header.Get("X-Ratelimit-Remaining"))
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("github api returned status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if err != nil {
		return fmt.Errorf("read github api response: %w", err)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("unmarshal github api response: %w", err)
	}
	return nil
}

func (s *RouterServer) isAutoUpdateAuthorAllowed(author string) bool {
	clean := strings.TrimSpace(author)
	return clean != "" && s != nil && slices.ContainsFunc(s.cfgSrc.Current().AutoUpdatePRAuthors, func(a string) bool {
		return strings.EqualFold(clean, strings.TrimSpace(a))
	})
}

func (s *RouterServer) transitionRegisteredConflict(ctx context.Context, repo string, prNumber int, branch, headSHA string) error {
	if s.registry == nil {
		return nil
	}
	targetID, updated, err := s.registry.AtomicTransitionConflict(ctx, repo, prNumber)
	if err != nil {
		return err
	}
	if updated {
		_ = s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        "conflict",
			Repo:         repo,
			PRNumber:     prNumber,
			Branch:       branch,
			HeadSHA:      headSHA,
			TargetID:     targetID,
			Status:       "conflict",
			RolloutState: s.loadPRRolloutState(ctx, repo, prNumber),
			Quiet:        isDependabotBranch(branch),
			OccurredAt:   time.Now().UTC(),
		})
		return nil
	}
	return nil
}

func (s *RouterServer) handleDirtyPRConflict(ctx context.Context, repo string, pr GitHubPullRequest) {
	if pr.Head.SHA != "" {
		s.nonRoboRevFailedSHAs.Store(prCheckKey(repo, pr.Number), strings.TrimSpace(pr.Head.SHA))
	}
	s.recordFailedCISHA(repo, pr.Number, pr.Head.SHA)
	if err := s.transitionRegisteredConflict(ctx, repo, pr.Number, pr.Head.Ref, pr.Head.SHA); err != nil {
		log.Printf("[synapse] [registry] error transitioning PR #%d to conflict: %v", pr.Number, err)
	}
}

func (s *RouterServer) updatePRBranch(ctx context.Context, repo string, prNumber int) (bool, error) {
	req, err := s.newGitHubRequest(ctx, http.MethodPut, fmt.Sprintf("/repos/%s/pulls/%d/update-branch", normalizeRepo(repo), prNumber), strings.NewReader(`{}`))
	if err != nil {
		return false, fmt.Errorf("create update-branch request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("update-branch request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK {
		return false, nil
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if readErr != nil {
		body = nil
	}
	bodyLower := strings.ToLower(string(body))
	if resp.StatusCode == http.StatusForbidden {
		remaining := resp.Header.Get("X-Ratelimit-Remaining")
		return false, fmt.Errorf("github api rate limit or forbidden (remaining: %s)", remaining)
	}
	if resp.StatusCode == http.StatusConflict || (resp.StatusCode == http.StatusUnprocessableEntity && strings.Contains(bodyLower, "conflict")) {
		return true, nil
	}
	if resp.StatusCode == http.StatusUnprocessableEntity && strings.Contains(bodyLower, "no new commits on the base branch") {
		return false, nil
	}
	return false, fmt.Errorf("update-branch returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

func (s *RouterServer) handleStaleOpenPR(ctx context.Context, repo string, prNumber int) {
	s.clearFailedCISHA(repo, prNumber)
	s.updatedForCurrentMain.Delete(prCheckKey(repo, prNumber))
	if s.registry == nil {
		return
	}
	if targetID, closeErr := s.registry.UpdatePRClosedUnmerged(ctx, repo, prNumber); closeErr != nil {
		log.Printf("[synapse] [registry] warning closing stale PR #%d with missing head ref: %v", prNumber, closeErr)
	} else {
		_ = s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        prClosedUnmerged,
			Repo:         repo,
			PRNumber:     prNumber,
			TargetID:     targetID,
			Status:       prClosedUnmerged,
			RolloutState: s.loadPRRolloutState(ctx, repo, prNumber),
			OccurredAt:   time.Now().UTC(),
		})
	}
}

func (s *RouterServer) isEligibleAutoUpdatePR(ctx context.Context, repo string, pr GitHubPullRequest) bool {
	if pr.Draft || !s.isAutoUpdateAuthorAllowed(pr.User.Login) {
		return false
	}
	if !s.cfgSrc.Current().DefaultOwner && !s.isPROwnedLocally(ctx, repo, pr.GetNumber()) {
		return false
	}
	return true
}

func (s *RouterServer) checkOpenPRConflicts(ctx context.Context, repo string) error {
	normRepo := normalizeRepo(repo)
	if len(s.cfgSrc.Current().AutoUpdatePRAuthors) == 0 || normRepo == "" {
		return nil
	}
	var openPRs []GitHubPullRequest
	if err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/pulls?state=open&base=main&sort=created&direction=asc&per_page=100", normRepo), 5<<20, &openPRs); err != nil {
		return fmt.Errorf("list open prs: %w", err)
	}
	for _, pr := range openPRs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !s.isEligibleAutoUpdatePR(ctx, normRepo, pr) {
			continue
		}
		hasFailedOrPending := s.hasFailedCIForSHA(ctx, normRepo, pr.Number, pr.Head.SHA)
		if s.wasUpdatedForCurrentMain(normRepo, pr.Number) {
			// If the in-flight PR updated for current main actually failed CI, continue to update the next clean PR.
			// Otherwise (checks are still pending, passed, or initializing), break to prevent concurrent branch updates.
			if hasFailedOrPending && s.isCachedFailedCISHA(normRepo, pr.Number, pr.Head.SHA) {
				continue
			}
			break
		}
		if hasFailedOrPending {
			continue
		}
		conflict, err := s.updatePRBranch(ctx, normRepo, pr.Number)
		if err != nil {
			errLower := strings.ToLower(err.Error())
			if strings.Contains(err.Error(), "403") || strings.Contains(errLower, "rate limit") {
				return err
			}
			if strings.Contains(errLower, "head ref does not exist") {
				s.handleStaleOpenPR(ctx, normRepo, pr.Number)
				continue
			}
			log.Printf("[synapse] [github] warning updating open PR #%d branch: %v", pr.Number, err)
			continue
		}
		if conflict {
			s.handleDirtyPRConflict(ctx, normRepo, pr)
			continue
		}
		s.markUpdatedForCurrentMain(normRepo, pr.Number)
		break
	}
	return nil
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
		log.Printf("[synapse] [dispatcher] error dispatching git_push for %s: %v", repo, err)
		return fmt.Errorf("dispatch git_push: %w", err)
	}
	return nil
}

func (s *RouterServer) completeNoDeployWorkflowRun(ctx context.Context, repo, headSHA, targetID string, prNum int) error {
	if s.registry == nil || prNum <= 0 {
		return nil
	}
	hasExpected, err := s.registry.HasExpectedDeployJobs(ctx, repo, prNum)
	if err != nil {
		return fmt.Errorf("failed to check expected deploy jobs: %w", err)
	}
	if hasExpected {
		return nil
	}
	if deferred, err := s.deferPRToWatcher(ctx, repo, prNum); deferred || err != nil {
		return err
	}
	s.recordDeploySuccessDispatched(repo, "git-merge", headSHA, prNum)
	_ = s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
		Event:        eventDeploySuccess,
		Repo:         repo,
		PRNumber:     prNum,
		HeadSHA:      headSHA,
		TargetID:     targetID,
		Status:       "deployed",
		JobName:      "git-merge",
		RolloutState: s.loadPRRolloutState(ctx, repo, prNum),
		Quiet:        s.isDependabotPRNumber(ctx, repo, prNum),
		OccurredAt:   time.Now().UTC(),
	})
	return nil
}

func isMainBranch(branch string) bool {
	clean := strings.TrimSpace(branch)
	return clean == "main" || strings.HasPrefix(clean, "refs/heads/main")
}

func (s *RouterServer) ownsWorkflowRunEvent(ctx context.Context, repo string, prNum int) bool {
	cfg := s.cfgSrc.Current()
	if prNum > 0 {
		return s.isPROwnedLocally(ctx, repo, prNum) || (cfg.DefaultOwner && len(cfg.WebhookForwardURLs) == 0)
	}
	return cfg.DefaultOwner || s.registry == nil
}

func (s *RouterServer) shouldDispatchWorkflowRunImages(ctx context.Context, repo string, headBranch, conclusion string, prNum int) bool {
	if s.dispatcher == nil || conclusion != conclusionSuccess || !isMainBranch(headBranch) {
		return false
	}
	return s.ownsWorkflowRunEvent(ctx, repo, prNum)
}

func resolveImagesWorkloadsAndNames(repo string, images []string) ([]string, []string) {
	var imageNames, workloads []string
	for _, img := range images {
		if _, imageName, workload := resolveImageTag(repo, img); workload != "" {
			imageNames = append(imageNames, imageName)
			workloads = append(workloads, workload)
		}
	}
	return dedupeTrimmedStrings(imageNames), dedupeTrimmedStrings(workloads)
}

func (s *RouterServer) startWorkflowRunPRDeploying(ctx context.Context, repo string, prNum int, headSHA, targetID string, workloads []string) error {
	tID, err := s.registry.TransitionDeploying(ctx, repo, prNum, workloads)
	if err != nil {
		log.Printf("[synapse] [dispatcher] error transitioning pr %s#%d to deploying: %v", repo, prNum, err)
		return fmt.Errorf("transition pr deploying: %w", err)
	}
	if tID != "" {
		_ = s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        eventDeployStarted,
			Repo:         repo,
			PRNumber:     prNum,
			HeadSHA:      headSHA,
			TargetID:     firstNonEmpty(targetID, tID),
			Status:       statusDeploying,
			RolloutState: s.loadPRRolloutState(ctx, repo, prNum),
			Quiet:        s.isDependabotPRNumber(ctx, repo, prNum),
			OccurredAt:   time.Now().UTC(),
		})
	}
	return nil
}

func (s *RouterServer) dispatchWorkflowRunImages(ctx context.Context, repo string, runID int64, headBranch, conclusion, headSHA, targetID string, prNum int) error {
	if !s.shouldDispatchWorkflowRunImages(ctx, repo, headBranch, conclusion, prNum) {
		return nil
	}
	finalAttempt := isFinalAttemptFromContext(ctx)
	images, err := s.resolveWorkflowRunImages(ctx, repo, runID)
	if err != nil {
		log.Printf("[synapse] [dispatcher] error resolving images for workflow run %d (%s): %v", runID, repo, err)
		return fmt.Errorf("resolve workflow run images: %w", err)
	}

	// Phase a: resolve image names and discover every owning Kustomization once.
	imageNames, workloads := resolveImagesWorkloadsAndNames(repo, images)
	if len(workloads) == 0 {
		return s.completeNoDeployWorkflowRun(ctx, repo, headSHA, targetID, prNum)
	}
	refs, err := s.dispatcher.DiscoverImageKustomizations(ctx, imageNames)
	if err != nil {
		log.Printf("[synapse] [dispatcher] error discovering kustomizations for %s images %v: %v", repo, imageNames, err)
		return fmt.Errorf("discover image kustomizations: %w", err)
	}
	trackPR := s.registry != nil && prNum > 0

	// Phase b: mark the PR deploying before any tag can roll out.
	if len(refs) > 0 && trackPR {
		if err := s.startWorkflowRunPRDeploying(ctx, repo, prNum, headSHA, targetID, workloads); err != nil {
			return err
		}
	}

	// Phase c: write every image tag before any reconcile so one reconcile renders all of them.
	if err := s.promoteWorkflowRunImages(ctx, images, ImageReadyEventRequest{Repo: repo, CommitSHA: headSHA, PRNumber: prNum, TargetID: targetID}); err != nil {
		if finalAttempt && trackPR {
			s.handleFinalAttemptDeployFailure(ctx, repo, prNum, headBranch, headSHA, targetID, err)
		}
		return err
	}
	if len(refs) == 0 {
		return s.completeNoDeployWorkflowRun(ctx, repo, headSHA, targetID, prNum)
	}

	// Phases d+e: durably record every reconcile request before patching any of them, so a watch event can
	// never observe a request that is not yet recorded. A crash or failure between record and patch is
	// repaired by the watcher re-patching the recorded value. Sync the widget after recording (rollout lines).
	return s.recordAndReconcileKustomizations(ctx, repo, prNum, headBranch, headSHA, targetID, refs, trackPR, finalAttempt)
}

func (s *RouterServer) hasUnrecordedKustomizationRequests(ctx context.Context, repo string, prNum int) bool {
	hasReqs, err := s.registry.HasKustomizationRequests(ctx, repo, prNum)
	return err == nil && !hasReqs
}

func (s *RouterServer) handleFinalAttemptDeployFailure(ctx context.Context, repo string, prNum int, headBranch, headSHA, targetID string, causeErr error) {
	if fErr := s.handleImagePromotionDeployFailed(ctx, repo, prNum, headBranch, headSHA, targetID, causeErr); fErr != nil {
		log.Printf("[synapse] [dispatcher] error handling deploy failure for %s#%d: %v", repo, prNum, fErr)
	}
}

func (s *RouterServer) recordAndReconcileKustomizations(ctx context.Context, repo string, prNum int, headBranch, headSHA, targetID string, refs []fluxKustomizationRef, trackPR, finalAttempt bool) error {
	requests := newKustomizationRequests(refs, time.Now().UTC())
	if trackPR {
		if err := s.registry.RecordKustomizationRequests(ctx, repo, prNum, requests); err != nil {
			log.Printf("[synapse] [dispatcher] error recording kustomization requests for %s#%d: %v", repo, prNum, err)
			if finalAttempt {
				s.handleFinalAttemptDeployFailure(ctx, repo, prNum, headBranch, headSHA, targetID, err)
			}
			return fmt.Errorf("record kustomization requests: %w", err)
		}
	}
	if err := s.reconcileImageKustomizations(ctx, refs, requests); err != nil {
		if finalAttempt && trackPR && s.hasUnrecordedKustomizationRequests(ctx, repo, prNum) {
			s.handleFinalAttemptDeployFailure(ctx, repo, prNum, headBranch, headSHA, targetID, err)
		}
		return err
	}
	return nil
}

func (s *RouterServer) handleImagePromotionDeployFailed(ctx context.Context, repo string, prNum int, headBranch, headSHA, targetID string, causeErr error) error {
	if s == nil || s.registry == nil || prNum <= 0 {
		return nil
	}
	tID, branch, hSHA, mSHA, err := s.registry.ResolvePRByNumber(ctx, repo, prNum)
	if err != nil {
		log.Printf("[synapse] [dispatcher] warning: failed resolving pr %s#%d: %v", repo, prNum, err)
	}
	if targetID == "" {
		targetID = tID
	}
	if headSHA == "" {
		headSHA = hSHA
	}
	if branch == "" {
		branch = headBranch
	}
	mergeSHA := mSHA
	if mergeSHA == "" {
		mergeSHA = headSHA
	}

	newTID, updated, fErr := s.registry.AtomicTransitionDeployFailed(ctx, repo, prNum)
	if fErr != nil {
		log.Printf("[synapse] [dispatcher] error transitioning pr %s#%d to deploy_failed: %v", repo, prNum, fErr)
		return fmt.Errorf("transition deploy failed: %w", fErr)
	}
	if !updated {
		return nil
	}
	if newTID != "" {
		targetID = newTID
	}
	if targetID == "" && s.cfgSrc != nil {
		targetID = strings.TrimSpace(s.cfgSrc.Current().SystemChannelID)
	}

	errMsg := ""
	if causeErr != nil {
		errMsg = causeErr.Error()
	}

	if pErr := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
		Event:        eventDeployFailed,
		Repo:         repo,
		PRNumber:     prNum,
		Branch:       branch,
		HeadSHA:      headSHA,
		MergeSHA:     mergeSHA,
		TargetID:     targetID,
		Status:       eventDeployFailed,
		Details:      errMsg,
		RolloutState: s.loadPRRolloutState(ctx, repo, prNum),
		Quiet:        isDependabotBranch(branch) || s.isDependabotPRNumber(ctx, repo, prNum),
		OccurredAt:   time.Now().UTC(),
	}); pErr != nil {
		return fmt.Errorf("publish deploy failed event: %w", pErr)
	}
	if ackErr := s.registry.AcknowledgeDeployFailedPublished(ctx, repo, prNum); ackErr != nil {
		log.Printf("[synapse] [dispatcher] warning: failed acknowledging deploy_failed_published for %s#%d: %v", repo, prNum, ackErr)
	}
	return nil
}

// promoteWorkflowRunImages promotes every image concurrently and waits for all of them.
func (s *RouterServer) promoteWorkflowRunImages(ctx context.Context, images []string, base ImageReadyEventRequest) error {
	var wg sync.WaitGroup
	errs := make([]error, len(images))
	for i, img := range images {
		wg.Go(func() {
			req := base
			req.Image = img
			if _, err := s.dispatcher.PromoteImageTag(ctx, req); err != nil {
				log.Printf("[synapse] [dispatcher] error promoting image %s: %v", img, err)
				errs[i] = fmt.Errorf("promote image %s: %w", img, err)
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// GitHub Webhook Payloads and Resolution Types

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

type GitHubCheckRunsResponse struct {
	CheckRuns []struct {
		Name       string `json:"name"`
		Status     string `json:"status"`
		Conclusion string `json:"conclusion"`
	} `json:"check_runs"`
}

type GitHubWorkflowRunPayload struct {
	Action      string `json:"action"`
	WorkflowRun struct {
		ID           int64              `json:"id"`
		Name         string             `json:"name"`
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

type GitHubWorkflowJobStep struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	Number     int    `json:"number"`
}

type GitHubWorkflowJobItem struct {
	ID         int64                   `json:"id"`
	Name       string                  `json:"name"`
	Status     string                  `json:"status"`
	Conclusion string                  `json:"conclusion"`
	Steps      []GitHubWorkflowJobStep `json:"steps,omitempty"`
}

// hasExecutedBuildStep verifies that if steps are present and a build/push step is defined,
// that step actually concluded with success rather than being skipped.
func hasExecutedBuildStep(steps []GitHubWorkflowJobStep) bool {
	hasPushStep := false
	for _, step := range steps {
		nameLower := strings.ToLower(step.Name)
		isPush := (strings.Contains(nameLower, "build") && (strings.Contains(nameLower, "push") || strings.Contains(nameLower, "publish"))) ||
			strings.Contains(nameLower, "docker push") || strings.Contains(nameLower, "push image") || strings.Contains(nameLower, "push docker")
		if isPush {
			hasPushStep = true
			if step.Conclusion == conclusionSuccess {
				return true
			}
		}
	}
	return !hasPushStep
}

// hasSkippedImagePromotionJob returns true if any job whose name contains both "promote" and "latest"
// has completed with a conclusion other than "success" (e.g. skipped or failed).
func hasSkippedImagePromotionJob(jobs []GitHubWorkflowJobItem) bool {
	for _, job := range jobs {
		nameLower := strings.ToLower(job.Name)
		if strings.Contains(nameLower, "promote") && strings.Contains(nameLower, "latest") && job.Status == ghStatusCompleted && job.Conclusion != conclusionSuccess {
			return true
		}
	}
	return false
}

type GitHubWorkflowJobsResponse struct {
	TotalCount int                     `json:"total_count"`
	Jobs       []GitHubWorkflowJobItem `json:"jobs"`
}

var buildJobRegex = regexp.MustCompile(`(?i)Build\s*(?:&|and)\s*(?:Push|Publish)(?:\s+(?:Sidecar\s+)?(?:Container\s+)?Images?(?:\s+to\s+GHCR)?)?\s*\(([^)]+)\)`)

// buildJobService returns a build check's service name, rejecting empty or unexpanded "${{ }}" captures.
func buildJobService(checkName string) (string, bool) {
	svc := strings.TrimSpace(append(buildJobRegex.FindStringSubmatch(checkName), "", "")[1])
	return svc, svc != "" && !strings.Contains(svc, "${{")
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
	if len(s.cfgSrc.Current().WebhookForwardURLs) > 0 {
		reqPath := r.URL.RequestURI()
		if reqPath == "" {
			reqPath = r.URL.Path
		}
		s.teeGitHubWebhook(r.Context(), reqPath, r, event, body)
	}
	if s.bus == nil {
		log.Printf("[synapse] [github] ERROR: event bus not initialized")
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{fieldError: "event bus not initialized"})
		return
	}
	if err := s.publishJetStreamJob(r.Context(), GitHubWebhookArgs{Event: event, Delivery: delivery, Body: body}); err != nil {
		log.Printf("[synapse] [github] ERROR: failed to publish job to jetstream: %v", err)
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
			log.Printf("[synapse] [registry] %s failed resolving PR by SHA %s locally: %v", res.Event, res.HeadSHA, err)
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
		log.Printf("[synapse] [github] %s failed resolving PR for commit %s: %v", res.Event, res.HeadSHA, err)
	} else {
		res.PRNumber = prNum
		if res.Branch == "" || strings.HasPrefix(res.Branch, "refs/heads/") {
			res.Branch = firstNonEmpty(branch, res.Branch)
		}
		res.ResolutionSource = "commit_sha_lookup"
	}
}

func (s *RouterServer) healDependabotPR(ctx context.Context, res *ResolvedGitHubEvent) {
	if s == nil || !s.cfgSrc.Current().DefaultOwner || !strings.HasPrefix(strings.TrimSpace(res.Branch), "dependabot/") {
		return
	}
	targetID := s.resolveHeadlessPRTargetID(ctx, res.Repo, res.PRNumber)
	if targetID == "" {
		return
	}
	if _, err := s.registry.EnsurePRRegistered(ctx, res.Repo, res.PRNumber, res.Branch, res.HeadSHA, "", targetID); err != nil {
		log.Printf("[synapse] [registry] warning self-healing dependabot PR %s#%d on CI failure: %v", res.Repo, res.PRNumber, err)
	}
}

func (s *RouterServer) handleCIFailureByPRNumber(ctx context.Context, res *ResolvedGitHubEvent, details, checkURL string, emitAdditional bool) error {
	s.healDependabotPR(ctx, res)
	targetID, updated, err := s.registry.AtomicTransitionCIFailed(ctx, res.Repo, res.PRNumber)
	if err != nil {
		return err
	}
	res.TargetID = firstNonEmpty(res.TargetID, targetID)
	if updated || (emitAdditional && res.TargetID != "") {
		return s.completeCIFailureTransition(ctx, res, details, checkURL)
	}
	return nil
}

func (s *RouterServer) handleCIFailureByHeadSHA(ctx context.Context, res *ResolvedGitHubEvent, details, checkURL string) error {
	targetID, prNum, updated, err := s.registry.AtomicTransitionCIFailedByHeadSHA(ctx, res.Repo, res.HeadSHA)
	if err != nil {
		return err
	}
	res.TargetID = firstNonEmpty(res.TargetID, targetID)
	emitAdditional := false
	if prNum > 0 && res.PRNumber == 0 {
		res.PRNumber = prNum
		if res.Event == ghEventCheckRun && !isGateAggregatorCheck(res.CheckName) {
			if !isRoboRevCheck(res.CheckName) {
				s.nonRoboRevFailedSHAs.Store(prCheckKey(res.Repo, res.PRNumber), strings.TrimSpace(res.HeadSHA))
			}
			emitAdditional = s.recordFailedCheckRun(res.Repo, res.PRNumber, res.HeadSHA, res.CheckName)
		}
	}
	if updated || (emitAdditional && res.TargetID != "") {
		return s.completeCIFailureTransition(ctx, res, details, checkURL)
	}
	return nil
}

func (s *RouterServer) handleCIFailureTransition(ctx context.Context, res *ResolvedGitHubEvent, details, checkURL string) error {
	if s.isStaleOrIgnoredCIFailure(ctx, res) {
		return nil
	}
	if s.isMergeSHAFailure(ctx, res) || (res.Event == ghEventWorkflowRun && isMainBranch(res.Branch) && res.PRNumber > 0) {
		if res.Event == ghEventWorkflowRun {
			return s.handleImagePromotionDeployFailed(ctx, res.Repo, res.PRNumber, res.Branch, res.HeadSHA, res.TargetID, errors.New(details))
		}
		return nil
	}
	emitAdditional := false
	if res.Event == ghEventCheckRun && res.PRNumber > 0 && res.HeadSHA != "" && !isGateAggregatorCheck(res.CheckName) {
		if !isRoboRevCheck(res.CheckName) {
			s.nonRoboRevFailedSHAs.Store(prCheckKey(res.Repo, res.PRNumber), strings.TrimSpace(res.HeadSHA))
		}
		emitAdditional = s.recordFailedCheckRun(res.Repo, res.PRNumber, res.HeadSHA, res.CheckName)
	}
	var err error
	if res.PRNumber > 0 {
		err = s.handleCIFailureByPRNumber(ctx, res, details, checkURL, emitAdditional)
	} else if res.HeadSHA != "" && !isZeroSHA(res.HeadSHA) {
		err = s.handleCIFailureByHeadSHA(ctx, res, details, checkURL)
	}
	if res.PRNumber > 0 && res.HeadSHA != "" && s.recordFailedCISHA(res.Repo, res.PRNumber, res.HeadSHA) {
		if cascadeErr := s.checkOpenPRConflicts(ctx, res.Repo); cascadeErr != nil {
			log.Printf("[synapse] [github] error checking open pr conflicts on ci failure for %s: %v", res.Repo, cascadeErr)
		}
	}
	return err
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

func (s *RouterServer) rerunWorkflowRunFailedJobs(ctx context.Context, repo string, runID int64) error {
	path := fmt.Sprintf("/repos/%s/actions/runs/%d/rerun-failed-jobs", normalizeRepo(repo), runID)
	req, err := s.newGitHubRequest(ctx, http.MethodPost, path, strings.NewReader(`{}`))
	if err != nil {
		return fmt.Errorf("create rerun-failed-jobs request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("rerun-failed-jobs request failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return nil
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return fmt.Errorf("rerun-failed-jobs returned status %d", resp.StatusCode)
	}
	return fmt.Errorf("rerun-failed-jobs returned status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

func (s *RouterServer) tryRerunFailedMainWorkflowRun(ctx context.Context, p GitHubWorkflowRunPayload, res *ResolvedGitHubEvent) bool {
	if p.Action != ghStatusCompleted || p.WorkflowRun.ID <= 0 || p.WorkflowRun.RunAttempt != 1 ||
		s.isStaleOrIgnoredCIFailure(ctx, res) ||
		(!isMainBranch(p.WorkflowRun.HeadBranch) && !s.isMergeSHAFailure(ctx, res)) ||
		!s.ownsWorkflowRunEvent(ctx, res.Repo, res.PRNumber) {
		return false
	}
	if err := s.rerunWorkflowRunFailedJobs(ctx, res.Repo, p.WorkflowRun.ID); err != nil {
		log.Printf("[synapse] [github] warning: failed auto-rerunning failed jobs for workflow run %d on %s: %v", p.WorkflowRun.ID, res.Repo, err)
		return false
	}
	log.Printf("[synapse] [github] successfully triggered rerun-failed-jobs for workflow run %d on %s (attempt 1)", p.WorkflowRun.ID, res.Repo)
	return true
}

func (s *RouterServer) handlePRClosedAction(ctx context.Context, res *ResolvedGitHubEvent, merged bool) error {
	s.clearFailedCISHA(res.Repo, res.PRNumber)
	s.updatedForCurrentMain.Delete(prCheckKey(res.Repo, res.PRNumber))
	if merged {
		targetID, err := s.registry.UpdatePRMerged(ctx, res.Repo, res.PRNumber, res.MergeSHA)
		if err != nil {
			return err
		}
		res.TargetID = firstNonEmpty(res.TargetID, targetID)
		_ = s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
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
		})
		return nil
	}
	targetID, err := s.registry.UpdatePRClosedUnmerged(ctx, res.Repo, res.PRNumber)
	if err != nil {
		return err
	}
	res.TargetID = firstNonEmpty(res.TargetID, targetID)
	_ = s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
		Event:        prClosedUnmerged,
		Repo:         res.Repo,
		PRNumber:     res.PRNumber,
		Branch:       res.Branch,
		HeadSHA:      res.HeadSHA,
		TargetID:     res.TargetID,
		Status:       prClosedUnmerged,
		RolloutState: s.loadPRRolloutState(ctx, res.Repo, res.PRNumber),
		Quiet:        isDependabotBranch(res.Branch),
		OccurredAt:   time.Now().UTC(),
	})
	return nil
}

func (s *RouterServer) handlePRAutoMergeDisabledAction(ctx context.Context, res *ResolvedGitHubEvent, pr GitHubPullRequest) error {
	if isPRMergeabilityUnknown(pr) && res.PRNumber > 0 && normalizeRepo(res.Repo) != "" {
		var fetched GitHubPullRequest
		if err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/pulls/%d", normalizeRepo(res.Repo), res.PRNumber), 2<<20, &fetched); err != nil {
			return err
		}
		if fetched.Merged || (fetched.State != "" && !strings.EqualFold(fetched.State, "open")) || (res.HeadSHA != "" && fetched.Head.SHA != "" && !strings.EqualFold(fetched.Head.SHA, strings.TrimSpace(res.HeadSHA))) {
			return nil
		}
		pr = fetched
	}

	if !isPRMergeConflict(pr) {
		log.Printf("[synapse] [github] ignoring auto_merge_disabled on %s#%d (mergeable_state=%q)", res.Repo, res.PRNumber, pr.MergeableState)
		return nil
	}

	if res.HeadSHA != "" {
		s.nonRoboRevFailedSHAs.Store(prCheckKey(res.Repo, res.PRNumber), strings.TrimSpace(res.HeadSHA))
	}
	s.recordFailedCISHA(res.Repo, res.PRNumber, res.HeadSHA)
	return s.transitionRegisteredConflict(ctx, res.Repo, res.PRNumber, res.Branch, res.HeadSHA)
}

func (s *RouterServer) handlePullRequestRegistryAction(ctx context.Context, res *ResolvedGitHubEvent, pr GitHubPullRequest) error {
	cfg := s.cfgSrc.Current()
	meta := ParseAerialPRMetadata(pr.GetBody())
	shouldRegister := true
	if meta.BotID != "" && cfg.BotID != "" && !strings.EqualFold(meta.BotID, cfg.BotID) {
		shouldRegister = false
	} else if meta.BotID == "" && !cfg.DefaultOwner {
		shouldRegister = false
	}
	targetID := meta.TargetID
	if IsHeadlessSessionKey(targetID) {
		targetID = s.resolveHeadlessPRTargetID(ctx, res.Repo, res.PRNumber)
	}
	if shouldRegister && s.registry != nil && targetID != "" && (res.Action != ghActionClosed || pr.Merged) {
		// Self-heal DB registration only; initial Discord widget delivery on opened is owned by Brain's /internal/pr/register to avoid duplicate message creation races.
		resolvedTargetID, err := s.registry.EnsurePRRegistered(ctx, res.Repo, res.PRNumber, res.Branch, res.HeadSHA, pr.Title, targetID)
		if err != nil {
			return err
		}
		res.TargetID = firstNonEmpty(res.TargetID, resolvedTargetID, targetID)
	}
	switch res.Action {
	case ghActionClosed:
		return s.handlePRClosedAction(ctx, res, pr.Merged)
	case ghActionSynchronize:
		s.clearFailedCISHA(res.Repo, res.PRNumber)
		s.nonRoboRevFailedSHAs.Delete(prCheckKey(res.Repo, res.PRNumber))
		s.cancelledCheckSHAs.Delete(prCheckKey(res.Repo, res.PRNumber))
		s.failedCheckRuns.Delete(prCheckKey(res.Repo, res.PRNumber))
		targetID, err := s.registry.UpdatePRSync(ctx, res.Repo, res.PRNumber, res.HeadSHA)
		if err != nil {
			return err
		}
		res.TargetID = firstNonEmpty(res.TargetID, targetID)
		_ = s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        ghActionSynchronize,
			Repo:         res.Repo,
			PRNumber:     res.PRNumber,
			Branch:       res.Branch,
			HeadSHA:      res.HeadSHA,
			Title:        pr.Title,
			TargetID:     res.TargetID,
			Status:       prOpen,
			RolloutState: s.loadPRRolloutState(ctx, res.Repo, res.PRNumber),
			OccurredAt:   time.Now().UTC(),
		})
	case "auto_merge_disabled":
		return s.handlePRAutoMergeDisabledAction(ctx, res, pr)
	}
	return nil
}

func isDependabotBranch(branch string) bool {
	return strings.HasPrefix(strings.TrimSpace(branch), "dependabot/")
}

func isDependabotPR(p GitHubPullRequestPayload) bool {
	return isDependabotBranch(p.PullRequest.Head.Ref) &&
		(strings.EqualFold(strings.TrimSpace(p.PullRequest.User.Login), "dependabot[bot]") ||
			strings.EqualFold(strings.TrimSpace(p.Sender.Login), "dependabot[bot]"))
}

func (s *RouterServer) handleDependabotPROpen(ctx context.Context, res *ResolvedGitHubEvent, p GitHubPullRequestPayload) {
	if s == nil || !s.cfgSrc.Current().DefaultOwner || !isDependabotPR(p) || (res.Action != ghActionOpened && res.Action != "reopened" && res.Action != ghActionSynchronize) {
		return
	}
	if s.registry == nil || res.PRNumber <= 0 {
		return
	}
	targetID := s.resolveHeadlessPRTargetID(ctx, res.Repo, res.PRNumber)
	if targetID == "" {
		return
	}
	resolvedTargetID, err := s.registry.EnsurePRRegistered(ctx, res.Repo, res.PRNumber, res.Branch, res.HeadSHA, p.PullRequest.Title, targetID)
	if err != nil {
		log.Printf("[synapse] [registry] warning registering dependabot PR %s#%d: %v", res.Repo, res.PRNumber, err)
		return
	}
	res.TargetID = firstNonEmpty(res.TargetID, resolvedTargetID, targetID)
}

func (s *RouterServer) processGitHubPullRequestEvent(ctx context.Context, delivery string, body []byte, res *ResolvedGitHubEvent) (*ResolvedGitHubEvent, error) {
	var p GitHubPullRequestPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("unmarshal pull_request payload: %w", err)
	}
	res.Repo = p.Repository.FullName
	res.Action = p.Action
	res.PRNumber = p.Number
	if res.PRNumber == 0 {
		res.PRNumber = p.PullRequest.Number
	}
	res.Branch = p.PullRequest.Head.Ref
	res.HeadSHA = p.PullRequest.Head.SHA
	res.Merged = p.PullRequest.Merged
	// GitHub pull_request webhooks set merge_commit_sha for test merges even on unmerged PRs; only set MergeSHA when merged.
	if p.PullRequest.Merged {
		res.MergeSHA = p.PullRequest.MergeCommitSHA
	}
	res.Sender = p.Sender.Login
	res.ResolutionSource = "pull_request_payload"
	log.Printf("[synapse] [github] [pull_request] repo=%s pr=%d action=%s head_sha=%s merge_sha=%s branch=%s merged=%v sender=%s delivery=%s",
		res.Repo, res.PRNumber, res.Action, res.HeadSHA, res.MergeSHA, res.Branch, p.PullRequest.Merged, res.Sender, delivery)

	s.handleDependabotPROpen(ctx, res, p)

	if res.Action == ghActionSynchronize && res.PRNumber > 0 {
		s.clearFailedCISHA(res.Repo, res.PRNumber)
	}

	if res.PRNumber > 0 && res.HeadSHA != "" {
		s.recordCommitSHAPR(res.Repo, res.HeadSHA, res.PRNumber, res.Branch)
	}

	if s.registry != nil && res.PRNumber > 0 {
		if err := s.handlePullRequestRegistryAction(ctx, res, p.PullRequest); err != nil {
			return nil, err
		}
	}
	if res.Action == ghActionOpened && res.PRNumber > 0 {
		_ = s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
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
		})
	}
	return res, nil
}

func (s *RouterServer) recordCheckRunStatus(ctx context.Context, res *ResolvedGitHubEvent) {
	if s.registry == nil || res.PRNumber <= 0 || res.HeadSHA == "" || isGateAggregatorCheck(res.CheckName) {
		return
	}
	if strings.EqualFold(strings.TrimSpace(res.Status), "completed") && strings.EqualFold(strings.TrimSpace(res.Conclusion), "cancelled") {
		s.cancelledCheckSHAs.Store(prCheckKey(res.Repo, res.PRNumber), strings.TrimSpace(res.HeadSHA))
	}
	if isDependabotBranch(res.Branch) {
		return
	}
	checkState := classifyCheckState(res.Status, res.Conclusion)
	if checkState == "" || checkState == statusFailed {
		return
	}
	if checkState == statusPending {
		s.clearActiveFailedCheckRun(res.Repo, res.PRNumber, res.HeadSHA, res.CheckName)
	}
	updated, err := s.registry.RecordPRCheckStatus(ctx, res.Repo, res.PRNumber, res.HeadSHA, res.CheckName, checkState)
	if err != nil {
		log.Printf("[synapse] [registry] warning recording check status for %s#%d check %q: %v", res.Repo, res.PRNumber, res.CheckName, err)
		return
	}
	_ = s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
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
	})
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
	s.resolveEventPR(ctx, res, p.CheckRun.PullRequests, "check_run_payload")
	log.Printf("[synapse] [github] [check_run] repo=%s check=%q head_sha=%s pr=%d branch=%s status=%s conclusion=%s source=%s target_id=%s delivery=%s",
		res.Repo, res.CheckName, res.HeadSHA, res.PRNumber, res.Branch, res.Status, res.Conclusion, res.ResolutionSource, res.TargetID, delivery)

	if p.Action == ghStatusCompleted && res.Conclusion == conclusionSuccess && res.PRNumber > 0 && s.registry != nil && s.canRecoverCheckSuccess(ctx, res.Repo, res.PRNumber, res.CheckName, res.HeadSHA) {
		s.clearFailedCISHA(res.Repo, res.PRNumber)
		s.nonRoboRevFailedSHAs.Delete(prCheckKey(res.Repo, res.PRNumber))
		s.cancelledCheckSHAs.Delete(prCheckKey(res.Repo, res.PRNumber))
		s.failedCheckRuns.Delete(prCheckKey(res.Repo, res.PRNumber))
		targetID, err := s.registry.UpdatePRSync(ctx, res.Repo, res.PRNumber, res.HeadSHA)
		if err != nil {
			return nil, err
		}
		res.TargetID = firstNonEmpty(res.TargetID, targetID)
	}

	s.recordCheckRunStatus(ctx, res)

	details := firstNonEmpty(strings.TrimSpace(p.CheckRun.Output.Text), strings.TrimSpace(p.CheckRun.Output.Summary), strings.TrimSpace(p.CheckRun.Output.Title))
	checkURL := firstNonEmpty(strings.TrimSpace(p.CheckRun.HTMLURL), strings.TrimSpace(p.CheckRun.DetailsURL))
	if err := s.handleCIFailureTransition(ctx, res, details, checkURL); err != nil {
		return nil, err
	}
	if p.Action == ghStatusCompleted && res.Conclusion == conclusionSuccess && isGateAggregatorCheck(res.CheckName) && res.PRNumber > 0 {
		if err := s.checkOpenPRConflicts(ctx, res.Repo); err != nil {
			log.Printf("[synapse] [github] error checking open pr conflicts on gate success for %s: %v", res.Repo, err)
		}
	}
	return res, nil
}

func (s *RouterServer) processGitHubWorkflowRunEvent(ctx context.Context, delivery string, body []byte, res *ResolvedGitHubEvent) (*ResolvedGitHubEvent, error) {
	var p GitHubWorkflowRunPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("unmarshal workflow_run payload: %w", err)
	}
	res.Repo = p.Repository.FullName
	res.Action = p.Action
	res.CheckName = p.WorkflowRun.Name
	res.HeadSHA = p.WorkflowRun.HeadSHA
	res.Branch = p.WorkflowRun.HeadBranch
	res.Status = p.WorkflowRun.Status
	res.Conclusion = p.WorkflowRun.Conclusion
	s.resolveEventPR(ctx, res, p.WorkflowRun.PullRequests, "workflow_run_payload")
	log.Printf("[synapse] [github] [workflow_run] repo=%s workflow=%q head_sha=%s pr=%d branch=%s status=%s conclusion=%s source=%s target_id=%s delivery=%s",
		res.Repo, res.CheckName, res.HeadSHA, res.PRNumber, res.Branch, res.Status, res.Conclusion, res.ResolutionSource, res.TargetID, delivery)

	if s.tryRerunFailedMainWorkflowRun(ctx, p, res) {
		return res, nil
	}

	details := fmt.Sprintf("Workflow %q concluded with %s", p.WorkflowRun.Name, p.WorkflowRun.Conclusion)
	if err := s.handleCIFailureTransition(ctx, res, details, strings.TrimSpace(p.WorkflowRun.HTMLURL)); err != nil {
		return nil, err
	}
	if p.Action == ghStatusCompleted && p.WorkflowRun.Status == ghStatusCompleted {
		if err := s.dispatchWorkflowRunImages(ctx, res.Repo, p.WorkflowRun.ID, p.WorkflowRun.HeadBranch, p.WorkflowRun.Conclusion, res.HeadSHA, res.TargetID, res.PRNumber); err != nil {
			return nil, err
		}
	}
	return res, nil
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
	log.Printf("[synapse] [github] [push] repo=%s ref=%s head_sha=%s resolved_pr=%d branch=%s source=%s target_id=%s delivery=%s",
		res.Repo, p.Ref, res.HeadSHA, res.PRNumber, res.Branch, res.ResolutionSource, res.TargetID, res.Delivery)

	if s.registry != nil && res.PRNumber > 0 && res.MergeSHA != "" {
		targetID, err := s.registry.BackfillPushMergeSHA(ctx, res.Repo, res.PRNumber, res.MergeSHA)
		if err != nil {
			return err
		}
		res.TargetID = firstNonEmpty(res.TargetID, targetID)
		_ = s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
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
		})
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

	if err := s.resolveAndBackfillPushPR(ctx, res, p); err != nil {
		return nil, err
	}

	if err := s.dispatchGitPush(ctx, res.Repo, p.Ref, res.HeadSHA, res.TargetID, res.PRNumber); err != nil {
		return nil, err
	}
	s.clearUpdatedPRsForRepo(res.Repo)
	if err := s.checkOpenPRConflicts(ctx, res.Repo); err != nil {
		log.Printf("[synapse] [github] error checking open pr conflicts for %s: %v", res.Repo, err)
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
		if err == nil {
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
		log.Printf("[synapse] [github] [unhandled] event=%s delivery=%s payload_size=%d bytes", event, delivery, len(body))
		res.ResolutionSource = "unhandled_event"
		return res, nil
	}
}

func (s *RouterServer) recordCommitSHAPR(repo, sha string, prNumber int, branch string) {
	normRepo := strings.ToLower(normalizeRepo(repo))
	normSHA := strings.ToLower(strings.TrimSpace(sha))
	if s == nil || normRepo == "" || normSHA == "" || isZeroSHA(normSHA) || prNumber <= 0 {
		return
	}
	s.commitSHAPRs.Store(normRepo+"@"+normSHA, GitHubCheckRunPR{
		Number: prNumber,
		Head: GitHubPRRef{
			Ref: strings.TrimSpace(branch),
			SHA: normSHA,
		},
	})
}

func (s *RouterServer) lookupCommitSHAPR(repo, sha string) (int, string, bool) {
	if s == nil {
		return 0, "", false
	}
	normRepo := strings.ToLower(normalizeRepo(repo))
	normSHA := strings.ToLower(strings.TrimSpace(sha))
	if normRepo == "" || normSHA == "" || isZeroSHA(normSHA) {
		return 0, "", false
	}
	v, ok := s.commitSHAPRs.Load(normRepo + "@" + normSHA)
	if !ok {
		return 0, "", false
	}
	item, ok := v.(GitHubCheckRunPR)
	if !ok || item.Number <= 0 {
		return 0, "", false
	}
	return item.Number, item.Head.Ref, true
}

func (s *RouterServer) resolvePRFromCommitSHA(ctx context.Context, repo, sha string) (int, string, error) {
	repo = normalizeRepo(repo)
	if repo == "" || sha == "" || isZeroSHA(sha) {
		return 0, "", fmt.Errorf("invalid repo or sha for pr resolution")
	}
	if prNum, branch, ok := s.lookupCommitSHAPR(repo, sha); ok {
		return prNum, branch, nil
	}
	var prs []GitHubCommitPRItem
	if err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/commits/%s/pulls", repo, url.PathEscape(sha)), 1<<20, &prs); err != nil {
		return 0, "", err
	}
	if len(prs) == 0 {
		var openPRs []GitHubCommitPRItem
		if err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/pulls?state=open&per_page=100", repo), 2<<20, &openPRs); err == nil {
			for _, item := range openPRs {
				if strings.EqualFold(strings.TrimSpace(item.Head.SHA), strings.TrimSpace(sha)) {
					prs = append(prs, item)
				}
			}
		}
	}
	if len(prs) == 0 {
		return 0, "", fmt.Errorf("no pull requests associated with commit %s", sha)
	}
	selected := prs[0]
	for _, item := range prs[1:] {
		if (selected.State != ghActionClosed && item.State == ghActionClosed) || (selected.State == item.State && item.Number > selected.Number) {
			selected = item
		}
	}
	s.recordCommitSHAPR(repo, sha, selected.Number, selected.Head.Ref)
	return selected.Number, selected.Head.Ref, nil
}

func (s *RouterServer) resolveWorkflowRunImages(ctx context.Context, repo string, runID int64) ([]string, error) {
	repo = normalizeRepo(repo)
	if repo == "" || runID <= 0 {
		return nil, fmt.Errorf("invalid repo or runID for workflow run images resolution")
	}
	var jobsResp GitHubWorkflowJobsResponse
	if err := s.getGitHubJSON(ctx, fmt.Sprintf("/repos/%s/actions/runs/%d/jobs?per_page=100", repo, runID), 5<<20, &jobsResp); err != nil {
		return nil, err
	}

	if hasSkippedImagePromotionJob(jobsResp.Jobs) {
		return nil, nil
	}

	owner := "azylman"
	if parts := strings.Split(repo, "/"); len(parts) == 2 {
		owner = strings.ToLower(parts[0])
	}

	var rawImages []string
	for _, job := range jobsResp.Jobs {
		if job.Status != ghStatusCompleted || job.Conclusion != conclusionSuccess || !hasExecutedBuildStep(job.Steps) {
			continue
		}
		if service, ok := buildJobService(job.Name); ok {
			rawImages = append(rawImages, fmt.Sprintf("ghcr.io/%s/%s:latest", owner, service))
		}
	}
	return dedupeTrimmedStrings(rawImages), nil
}

func (s *RouterServer) fetchLatestRoboRevComment(ctx context.Context, repo string, prNumber int, headSHA string) string {
	repo = normalizeRepo(repo)
	if s == nil || repo == "" || prNumber <= 0 {
		return ""
	}
	var comments []GitHubIssueComment
	path := fmt.Sprintf("/repos/%s/issues/%d/comments?per_page=100", repo, prNumber)
	if err := s.getGitHubJSON(ctx, path, 2<<20, &comments); err != nil {
		log.Printf("[synapse] [github] warning fetching roborev comments for %s#%d: %v", repo, prNumber, err)
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
