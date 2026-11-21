package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/buildmeta"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

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
	// Journal before the legacy publish: a journal failure leaves nothing
	// recorded, so the redelivery retries the journal.
	fact := pubsub.PRLifecycleEvent{
		Event:        eventDeploySuccess,
		Repo:         repo,
		PRNumber:     prNum,
		HeadSHA:      headSHA,
		TargetID:     targetID,
		Status:       statusDeployed,
		JobName:      jobGitMerge,
		RolloutState: s.loadPRRolloutState(ctx, repo, prNum),
		Quiet:        s.isDependabotPRNumber(ctx, repo, prNum),
		OccurredAt:   time.Now().UTC(),
	}
	if err := s.routeDeployDispatched(ctx, repo, jobGitMerge, headSHA, "", prNum, fact); err != nil {
		return err
	}
	if restateOwnsEffects(ctx) {
		return nil
	}
	return s.publishLegacyFact(ctx, fact)
}

func isMainBranch(branch string) bool {
	clean := strings.TrimSpace(branch)
	return clean == "main" || strings.HasPrefix(clean, "refs/heads/main")
}

func (s *RouterServer) ownsWorkflowRunEvent(ctx context.Context, repo string, prNum int) bool {
	cfg := s.cfgSrc.Current()
	if prNum > 0 {
		return s.isPROwnedLocally(ctx, repo, prNum) || cfg.DefaultOwner
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
		slog.ErrorContext(ctx, "[synapse] [dispatcher] error transitioning pr to deploying", "repo", repo, "pr", prNum, "error", err)
		return fmt.Errorf("transition pr deploying: %w", err)
	}
	if tID != "" {
		if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        eventDeployStarted,
			Repo:         repo,
			PRNumber:     prNum,
			HeadSHA:      headSHA,
			TargetID:     firstNonEmpty(targetID, tID),
			Status:       statusDeploying,
			RolloutState: s.loadPRRolloutState(ctx, repo, prNum),
			Quiet:        s.isDependabotPRNumber(ctx, repo, prNum),
			OccurredAt:   time.Now().UTC(),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (s *RouterServer) dispatchWorkflowRunImages(ctx context.Context, repo string, run workflowRunEvent, headBranch, conclusion, targetID string, prNum int) error {
	if !s.shouldDispatchWorkflowRunImages(ctx, repo, headBranch, conclusion, prNum) {
		return nil
	}
	finalAttempt := isFinalAttemptFromContext(ctx)
	headSHA := run.HeadSHA
	trackPR := s.registry != nil && prNum > 0
	images, err := s.resolveWorkflowRunImages(ctx, repo, run, prNum)
	if errors.Is(err, errStaleWorkflowEvent) || errors.Is(err, errNotImageWorkflow) {
		slog.InfoContext(ctx, "[synapse] [dispatcher] skipping workflow run image promotion", "run_id", run.ID, "attempt", run.Attempt, "repo", repo, "reason", err)
		return nil
	}
	if err != nil {
		slog.ErrorContext(ctx, "[synapse] [dispatcher] error resolving images for workflow run", "run_id", run.ID, "attempt", run.Attempt, "repo", repo, "error", err)
		if trackPR {
			s.handleDeployAttemptFailure(ctx, repo, prNum, headBranch, headSHA, targetID, finalAttempt, err)
		}
		return fmt.Errorf("resolve workflow run images: %w", err)
	}

	// Phase a: resolve image names and discover every owning Kustomization once.
	imageNames, workloads := resolveImagesWorkloadsAndNames(repo, images)
	if len(workloads) == 0 {
		return s.completeNoDeployWorkflowRun(ctx, repo, headSHA, targetID, prNum)
	}
	refs, err := s.dispatcher.DiscoverImageKustomizations(ctx, imageNames)
	if err != nil {
		slog.ErrorContext(ctx, "[synapse] [dispatcher] error discovering kustomizations for images", "repo", repo, "images", imageNames, "error", err)
		return fmt.Errorf("discover image kustomizations: %w", err)
	}

	// Phase b: mark the PR deploying before any tag can roll out.
	if len(refs) > 0 && trackPR {
		if err := s.startWorkflowRunPRDeploying(ctx, repo, prNum, headSHA, targetID, workloads); err != nil {
			return err
		}
	}

	var requests map[string]kustomizationRequest
	if len(refs) > 0 {
		requests = newKustomizationRequests(refs, time.Now().UTC())
	}
	effectPR := 0
	if trackPR {
		effectPR = prNum
	}
	s.journalImagePromotion(ctx, restateclient.RepoEvent{
		Repo: repo, HeadSHA: headSHA, PRNumber: effectPR, Branch: headBranch, TargetID: targetID,
		Images: images, Jobs: workloads, KustomizationRequests: requests,
	})

	// Phase c: write every image tag before any reconcile so one reconcile renders all of them.
	if err := s.promoteWorkflowRunImages(ctx, images, ImageReadyEventRequest{Repo: repo, CommitSHA: headSHA, PRNumber: prNum, TargetID: targetID}); err != nil {
		if trackPR {
			s.handleDeployAttemptFailure(ctx, repo, prNum, headBranch, headSHA, targetID, finalAttempt, err)
		}
		return err
	}
	recordLegacyEffect(ctx, restateclient.EffectImagePromote,
		restateclient.EffectMatchKey(repo, effectPR, headSHA, restateclient.EffectImagePromote, "", ""), normalizeRepo(repo), effectPR)
	if len(refs) == 0 {
		return s.completeNoDeployWorkflowRun(ctx, repo, headSHA, targetID, prNum)
	}

	// Phases d+e: durably record every reconcile request before patching any of them, so a watch event can
	// never observe a request that is not yet recorded. A crash or failure between record and patch is
	// repaired by the watcher re-patching the recorded value. Sync the widget after recording (rollout lines).
	return s.recordAndReconcileRequests(ctx, repo, prNum, headBranch, headSHA, targetID, workloads, refs, requests, trackPR, finalAttempt)
}

// journalImagePromotion journals one legacy image promotion to
// RepoObject.HandleImagePromotion, which records the promotion (and, for an
// untracked rollout, the reconcile) as intended; synapse promotes and
// reconciles below exactly as before, so a journal failure is only logged.
func (s *RouterServer) journalImagePromotion(ctx context.Context, ev restateclient.RepoEvent) {
	if err := s.routeRepoEvent(ctx, "HandleImagePromotion", ev); err != nil {
		slog.WarnContext(ctx, "[synapse] [restate-cutover] image promotion journal failed; legacy promotion continues", "repo", ev.Repo, "sha", ev.HeadSHA, "error", err)
	}
}

// hasUnrecordedKustomizationRequests is projection-safe: pure workflow
// routing (whether reconcile requests still need recording), never an
// effect dedup decision.
func (s *RouterServer) hasUnrecordedKustomizationRequests(ctx context.Context, repo string, prNum int) bool {
	hasReqs, err := s.registry.HasKustomizationRequests(ctx, repo, prNum)
	return err == nil && !hasReqs
}

func (s *RouterServer) handleFinalAttemptDeployFailure(ctx context.Context, repo string, prNum int, headBranch, headSHA, targetID string, causeErr error) {
	if fErr := s.handleImagePromotionDeployFailed(ctx, repo, prNum, headBranch, headSHA, targetID, causeErr); fErr != nil {
		slog.ErrorContext(ctx, "[synapse] [dispatcher] error handling deploy failure", "repo", repo, "pr", prNum, "error", fErr)
	}
}

// recordAndReconcileRequests records requests (journal, then registry) and
// patches every ref, logging the reconcile as [synapse-effect] under the
// match key PRObject (tracked PR) or RepoObject (untracked) records.
func (s *RouterServer) recordAndReconcileRequests(ctx context.Context, repo string, prNum int, headBranch, headSHA, targetID string, jobs []string, refs []fluxKustomizationRef, requests map[string]kustomizationRequest, trackPR, finalAttempt bool) error {
	// Journal before recording or reconciling: a journal failure leaves
	// nothing recorded, so redelivery re-journals (idempotent) and retries
	// the record/reconcile instead of repeating an unjournaled effect. The
	// event carries the deploying jobs and the exact requests so PRObject
	// records the same row the registry does.
	if trackPR {
		if err := s.routePREvent(ctx, "HandleKustomizationRequested", restateclient.PREvent{
			Repo: repo, PRNumber: prNum, HeadSHA: headSHA, Branch: headBranch,
			Jobs: jobs, KustomizationRequests: requests,
		}); err != nil {
			return err
		}
	}
	if trackPR {
		if err := s.registry.RecordKustomizationRequests(ctx, repo, prNum, requests); err != nil {
			slog.ErrorContext(ctx, "[synapse] [dispatcher] error recording kustomization requests", "repo", repo, "pr", prNum, "error", err)
			s.handleDeployAttemptFailure(ctx, repo, prNum, headBranch, headSHA, targetID, finalAttempt, err)
			return fmt.Errorf("record kustomization requests: %w", err)
		}
	}
	if err := s.reconcileImageKustomizations(ctx, refs, requests); err != nil {
		if trackPR && s.hasUnrecordedKustomizationRequests(ctx, repo, prNum) {
			s.handleDeployAttemptFailure(ctx, repo, prNum, headBranch, headSHA, targetID, finalAttempt, err)
		}
		return err
	}
	effectPR := 0
	if trackPR {
		effectPR = prNum
	}
	recordLegacyEffect(ctx, restateclient.EffectFluxReconcile,
		restateclient.EffectMatchKey(repo, effectPR, headSHA, restateclient.EffectFluxReconcile, "", ""), normalizeRepo(repo), effectPR)
	return nil
}

func (s *RouterServer) handleImagePromotionDeployFailed(ctx context.Context, repo string, prNum int, headBranch, headSHA, targetID string, causeErr error) error {
	if s == nil || s.registry == nil || prNum <= 0 {
		return nil
	}
	tID, branch, hSHA, mSHA, err := s.registry.ResolvePRByNumber(ctx, repo, prNum)
	if err != nil {
		slog.WarnContext(ctx, "[synapse] [dispatcher] failed resolving pr", "repo", repo, "pr", prNum, "error", err)
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

	if restateOwnsEffects(ctx) {
		// PRObject applies deploy_failed and publishes the carried fact.
		fact := s.imageDeployFailedFact(ctx, repo, prNum, branch, headSHA, mergeSHA, s.deployFailedTarget(targetID), causeErr)
		return s.routeDeployFailed(ctx, repo, prNum, headSHA, mergeSHA, false, fact)
	}
	if err := s.routeDeployFailed(ctx, repo, prNum, headSHA, mergeSHA, false); err != nil {
		return err
	}
	newTID, updated, fErr := s.registry.AtomicTransitionDeployFailed(ctx, repo, prNum)
	if fErr != nil {
		slog.ErrorContext(ctx, "[synapse] [dispatcher] error transitioning pr to deploy_failed", "repo", repo, "pr", prNum, "error", fErr)
		return fmt.Errorf("transition deploy failed: %w", fErr)
	}
	if !updated {
		return nil
	}
	if newTID != "" {
		targetID = newTID
	}
	fact := s.imageDeployFailedFact(ctx, repo, prNum, branch, headSHA, mergeSHA, s.deployFailedTarget(targetID), causeErr)
	if pErr := s.publishLegacyFact(ctx, fact); pErr != nil {
		return fmt.Errorf("publish deploy failed event: %w", pErr)
	}
	if ackErr := s.routeDeployFailed(ctx, repo, prNum, headSHA, mergeSHA, true, fact); ackErr != nil {
		slog.WarnContext(ctx, "[synapse] [dispatcher] failed journaling deploy_failed ack", "repo", repo, "pr", prNum, "error", ackErr)
	}
	if ackErr := s.registry.AcknowledgeDeployFailedPublished(ctx, repo, prNum); ackErr != nil {
		slog.WarnContext(ctx, "[synapse] [dispatcher] failed acknowledging deploy_failed_published", "repo", repo, "pr", prNum, "error", ackErr)
	}
	return nil
}

// deployFailedTarget falls back to the system channel when the PR has no
// Discord target.
func (s *RouterServer) deployFailedTarget(targetID string) string {
	if targetID == "" && s.cfgSrc != nil {
		return strings.TrimSpace(s.cfgSrc.Current().SystemChannelID)
	}
	return targetID
}

// imageDeployFailedFact renders the deploy_failed fact for one failed image
// promotion.
func (s *RouterServer) imageDeployFailedFact(ctx context.Context, repo string, prNum int, branch, headSHA, mergeSHA, targetID string, causeErr error) pubsub.PRLifecycleEvent {
	errMsg := ""
	if causeErr != nil {
		errMsg = causeErr.Error()
	}
	return pubsub.PRLifecycleEvent{
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
	}
}

// handleDeployAttemptFailure routes one failed deploy attempt for a tracked
// PR. A restate-owned attempt reports to PRObject, whose bounded
// deploy-failure deadline decides deploy_failed; a legacy attempt keeps the
// final-attempt rule (deploy_failed only on the last JetStream delivery).
func (s *RouterServer) handleDeployAttemptFailure(ctx context.Context, repo string, prNum int, headBranch, headSHA, targetID string, finalAttempt bool, causeErr error) {
	if restateOwnsEffects(ctx) {
		fact := s.imageDeployFailedFact(ctx, repo, prNum, headBranch, headSHA, headSHA, s.deployFailedTarget(targetID), causeErr)
		if err := s.routeDeployAttemptFailed(ctx, repo, prNum, headSHA, headSHA, fact); err != nil {
			slog.ErrorContext(ctx, "[synapse] [dispatcher] error reporting deploy attempt failure", "repo", repo, "pr", prNum, "error", err)
		}
		return
	}
	if finalAttempt {
		s.handleFinalAttemptDeployFailure(ctx, repo, prNum, headBranch, headSHA, targetID, causeErr)
	}
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
				slog.ErrorContext(ctx, "[synapse] [dispatcher] error promoting image", "image", img, "error", err)
				errs[i] = fmt.Errorf("promote image %s: %w", img, err)
			}
		})
	}
	wg.Wait()
	return errors.Join(errs...)
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

func (s *RouterServer) rerunWorkflowRunFailedJobs(ctx context.Context, repo string, runID int64) error {
	path := fmt.Sprintf("/repos/%s/actions/runs/%d/rerun-failed-jobs", normalizeRepo(repo), runID)
	req, err := s.newGitHubRequest(ctx, http.MethodPost, path, strings.NewReader(`{}`))
	if err != nil {
		return fmt.Errorf("create rerun-failed-jobs request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.githubClient.Do(req)
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

func (s *RouterServer) tryRerunFailedMainWorkflowRun(ctx context.Context, p GitHubWorkflowRunPayload, res *ResolvedGitHubEvent) (bool, error) {
	if p.Action != ghStatusCompleted || p.WorkflowRun.ID <= 0 || p.WorkflowRun.RunAttempt != 1 ||
		s.isStaleOrIgnoredCIFailure(ctx, res) ||
		(!isMainBranch(p.WorkflowRun.HeadBranch) && !s.isMergeSHAFailure(ctx, res)) ||
		!s.ownsWorkflowRunEvent(ctx, res.Repo, res.PRNumber) {
		return false, nil
	}
	// Journal before the external rerun: a journal failure fires nothing,
	// so redelivery retries the journal. A rerun failure after a committed
	// journal also returns an error when authoritative, so the webhook is
	// answered 5xx and redelivery re-journals (idempotent) and retries the
	// rerun. No success ack or dedup mark lands until the rerun succeeds;
	// legacy mode keeps the deploy_failed fallback.
	if err := s.routePREvent(ctx, "HandleCheckRerunRequested", restateclient.PREvent{
		Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA,
		MergeSHA: res.MergeSHA, CheckName: res.CheckName, Branch: res.Branch,
	}); err != nil {
		return false, err
	}
	// The rerun itself journals to RepoObject.HandleWorkflowRerun: a
	// restate-owned run is rerun there (a send failure retries the job); a
	// legacy-owned run reruns here as today and logs [synapse-effect].
	if err := s.routeRepoEvent(ctx, "HandleWorkflowRerun", restateclient.RepoEvent{
		Repo: res.Repo, PRNumber: res.PRNumber, HeadSHA: res.HeadSHA, CheckName: res.CheckName, RunID: p.WorkflowRun.ID,
	}); err != nil {
		if restateOwnsEffects(ctx) {
			return false, err
		}
		slog.WarnContext(ctx, "[synapse] [restate-cutover] workflow rerun journal failed; legacy rerun continues", "run_id", p.WorkflowRun.ID, "repo", res.Repo, "error", err)
	}
	if restateOwnsEffects(ctx) {
		return true, nil
	}
	if err := s.rerunWorkflowRunFailedJobs(ctx, res.Repo, p.WorkflowRun.ID); err != nil {
		slog.WarnContext(ctx, "[synapse] [github] failed auto-rerunning failed jobs for workflow run", "run_id", p.WorkflowRun.ID, "repo", res.Repo, "error", err)
		if s.restateAuthoritative() {
			return false, fmt.Errorf("rerun failed jobs for workflow run %d: %w", p.WorkflowRun.ID, err)
		}
		return false, nil
	}
	recordLegacyEffect(ctx, restateclient.EffectGitHubRerun,
		restateclient.EffectMatchKey(res.Repo, res.PRNumber, res.HeadSHA, restateclient.EffectGitHubRerun, "", res.CheckName), normalizeRepo(res.Repo), res.PRNumber)
	slog.InfoContext(ctx, "[synapse] [github] successfully triggered rerun-failed-jobs for workflow run (attempt 1)", "run_id", p.WorkflowRun.ID, "repo", res.Repo)
	return true, nil
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
	slog.InfoContext(ctx, "[synapse] [github] [workflow_run] event", "repo", res.Repo, "workflow", res.CheckName, "head_sha", res.HeadSHA, "pr", res.PRNumber, "branch", res.Branch, "status", res.Status, "conclusion", res.Conclusion, "source", res.ResolutionSource, "target_id", res.TargetID, "delivery", delivery)

	s.handleWorkflowRunBuildMetadata(ctx, p, res)
	rerunHandled, err := s.tryRerunFailedMainWorkflowRun(ctx, p, res)
	if err != nil {
		return nil, err
	}
	if rerunHandled {
		return res, nil
	}

	details := fmt.Sprintf("Workflow %q concluded with %s", p.WorkflowRun.Name, p.WorkflowRun.Conclusion)
	if err := s.handleCIFailureTransition(ctx, res, details, strings.TrimSpace(p.WorkflowRun.HTMLURL)); err != nil {
		return nil, err
	}
	if err := s.routeCheckOutcome(ctx, res); err != nil {
		return nil, err
	}
	if p.Action == ghStatusCompleted && p.WorkflowRun.Status == ghStatusCompleted {
		if err := s.dispatchWorkflowRunImages(ctx, res.Repo, workflowRunEvent{ID: p.WorkflowRun.ID, Attempt: p.WorkflowRun.RunAttempt, HeadSHA: res.HeadSHA}, p.WorkflowRun.HeadBranch, p.WorkflowRun.Conclusion, res.TargetID, res.PRNumber); err != nil {
			return nil, err
		}
	}
	return res, nil
}

type (
	metadataWorkflowRun = buildmeta.MetadataWorkflowRun
	workflowRunEvent    = buildmeta.WorkflowRunEvent
)

var (
	errStaleWorkflowEvent = buildmeta.ErrStaleWorkflowEvent
	errNotImageWorkflow   = buildmeta.ErrNotImageWorkflow
)

func (s *RouterServer) resolveWorkflowRunImages(ctx context.Context, repo string, ev workflowRunEvent, prNum int) ([]string, error) {
	return s.buildConsumer().ResolveWorkflowRunImages(ctx, repo, ev, prNum)
}

func (s *RouterServer) refreshPRBuildMetadata(ctx context.Context, repo string, pr int, sha string) (bool, error) {
	return s.buildConsumer().RefreshPRBuildMetadata(ctx, repo, pr, sha)
}

func (s *RouterServer) supersedeStaleBuildAttempt(ctx context.Context, repo string, pr int, ev workflowRunEvent) {
	s.buildConsumer().SupersedeStaleBuildAttempt(ctx, repo, pr, ev)
}

func (s *RouterServer) handleWorkflowRunBuildMetadata(ctx context.Context, p GitHubWorkflowRunPayload, res *ResolvedGitHubEvent) {
	repo := normalizeRepo(res.Repo)
	if s.registry == nil || res.PRNumber <= 0 || metadataWorkflowPath(repo) == "" || (p.WorkflowRun.Path != "" && p.WorkflowRun.Path != metadataWorkflowPath(repo)) || !s.ownsWorkflowRunEvent(ctx, repo, res.PRNumber) {
		return
	}
	s.supersedeStaleBuildAttempt(ctx, repo, res.PRNumber, workflowRunEvent{ID: p.WorkflowRun.ID, Attempt: p.WorkflowRun.RunAttempt, HeadSHA: p.WorkflowRun.HeadSHA})
	if _, err := s.refreshPRBuildMetadata(ctx, repo, res.PRNumber, p.WorkflowRun.HeadSHA); err != nil {
		logBuildMetadataWarning(ctx, "refreshing build metadata on workflow run", repo, res.PRNumber, err)
	}
}

// handleBuildMetadataCheck ingests a transport check from authoritative APIs,
// independently of Dependabot CI filtering, preserving owner routing.
func (s *RouterServer) handleBuildMetadataCheck(ctx context.Context, p GitHubCheckRunPayload, res *ResolvedGitHubEvent) error {
	if s.registry == nil || metadataWorkflowPath(normalizeRepo(res.Repo)) == "" {
		return nil
	}
	s.resolveEventPR(ctx, res, p.CheckRun.PullRequests, "build_metadata")
	if res.PRNumber <= 0 || !s.ownsWorkflowRunEvent(ctx, res.Repo, res.PRNumber) {
		return nil
	}
	updated, err := s.refreshPRBuildMetadata(ctx, res.Repo, res.PRNumber, res.HeadSHA)
	if err != nil {
		return fmt.Errorf("refresh build metadata: %w", err)
	}
	if updated {
		if err := s.publishPRLifecycleEvent(ctx, pubsub.PRLifecycleEvent{
			Event:        "check_updated",
			Repo:         res.Repo,
			PRNumber:     res.PRNumber,
			Branch:       res.Branch,
			HeadSHA:      res.HeadSHA,
			TargetID:     res.TargetID,
			RolloutState: s.loadPRRolloutState(ctx, res.Repo, res.PRNumber),
			Quiet:        isDependabotBranch(res.Branch),
			OccurredAt:   time.Now().UTC(),
		}); err != nil {
			slog.WarnContext(ctx, "[synapse] [build_metadata] publishing pr lifecycle event", "repo", res.Repo, "pr", res.PRNumber, "error", err)
		}
	}
	return nil
}

func logBuildMetadataWarning(ctx context.Context, msg, repo string, pr int, err error) {
	slog.WarnContext(ctx, "[synapse] [build_metadata] "+msg, "repo", repo, "pr", pr, "error", err)
}
