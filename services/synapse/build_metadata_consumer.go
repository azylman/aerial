package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/buildmeta"
)

type (
	metadataWorkflowRun = buildmeta.MetadataWorkflowRun
	workflowRunEvent    = buildmeta.WorkflowRunEvent
)

const (
	prConflict = "conflict"
	prSynced   = "synced"
)

var (
	errStaleWorkflowEvent = buildmeta.ErrStaleWorkflowEvent
	errNotImageWorkflow   = buildmeta.ErrNotImageWorkflow
)

func metadataWorkflowPath(repo string) string {
	return buildmeta.MetadataWorkflowPath(repo)
}

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
