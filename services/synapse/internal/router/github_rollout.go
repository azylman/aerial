package router

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/services/synapse/internal/github"
)

const (
	statusFailed       = github.StatusFailed
	statusPassed       = github.StatusPassed
	statusPending      = github.StatusPending
	ghStatusCompleted  = github.GHStatusCompleted
	conclusionSuccess  = github.ConclusionSuccess
	conclusionFailure  = github.ConclusionFailure
	conclusionTimedOut = github.ConclusionTimedOut
)

func isRoboRevCheck(checkName string) bool {
	return github.IsRoboRevCheck(checkName)
}

func isGateAggregatorCheck(checkName string) bool {
	return github.IsGateAggregatorCheck(checkName)
}

func classifyCheckState(status, conclusion string) string {
	return github.ClassifyCheckState(status, conclusion)
}

// resolveLegacyWorkflowRunImages is the bounded compatibility adapter for runs
// whose exact executed workflow blob is in the immutable legacy allowlist. It
// infers services from historical job and step names and must never be reached
// for metadata-capable producer runs.
func (s *RouterServer) resolveLegacyWorkflowRunImages(ctx context.Context, repo string, runID int64) ([]string, error) {
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
		return truncatePromptRunes(body, maxRoboRevCommentRunes)
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
		path := fmt.Sprintf("/repos/%s/pulls/%d", normalizeRepo(repo), prNumber)
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
	if ParseAerialPRMetadata(pr.GetBody()).OK() {
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
	branch := firstNonEmpty(resolvedPR.Head.Ref, res.Branch)
	sha := firstNonEmpty(resolvedPR.Head.SHA, res.HeadSHA)
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
	if !isPRMergeabilityUnknown(pr) || res.PRNumber <= 0 || normalizeRepo(res.Repo) == "" {
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
