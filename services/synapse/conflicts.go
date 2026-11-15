package main

import (
	"context"
	"fmt"
	"log"
	"strings"
)

func (s *RouterServer) enqueueMergeConflict(ctx context.Context, args MergeConflictArgs) error {
	if s == nil {
		return nil
	}
	args.Repo = normalizeRepo(args.Repo)
	if s.bus != nil {
		if err := s.publishJetStreamJob(ctx, args); err != nil {
			log.Printf("[synapse] [github] error enqueuing merge conflict into jetstream: %v", err)
			return fmt.Errorf("enqueue merge conflict: %w", err)
		}
		return nil
	}
	return s.ProcessMergeConflict(ctx, args)
}

func (s *RouterServer) ProcessMergeConflict(ctx context.Context, args MergeConflictArgs) error {
	if s == nil {
		return nil
	}
	return s.dispatchConflictPrompt(ctx, args.TargetID, args.Repo, args.Branch, args.HeadSHA, args.PRNumber)
}

func (s *RouterServer) dispatchConflictPrompt(ctx context.Context, targetID, repo, branch, headSHA string, prNum int) error {
	if s.bus != nil && s.registry != nil && prNum > 0 {
		if suppressed, err := s.registry.ShouldSuppressConflictPrompt(ctx, repo, prNum); err == nil && suppressed {
			log.Printf("[synapse] [conflict] suppressing conflict /prompt dispatch for %s#%d (handled by build orchestrator via aerial.pr.conflict)", repo, prNum)
			return nil
		}
	}
	shortRepo := firstNonEmpty(cleanRepoName(repo), "aerial")
	cleanBranch := firstNonEmpty(strings.TrimPrefix(strings.TrimSpace(branch), "refs/heads/"), "<branch>")
	prompt := fmt.Sprintf("Merge conflict detected on PR #%d on %s (branch: %s, head: %s). Please check out the branch via '/share/aerial/scripts/aerial-pr.sh init %s --branch %s', merge origin/main to resolve conflicts, verify locally, and push to update the PR via '/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> \"<commit_msg>\"' (PR_DESCRIPTION.md is not required when updating an existing PR; never run bare 'git push').", prNum, repo, branch, headSHA, shortRepo, cleanBranch)
	opts := alertPromptOpts{silent: true, skipIfActive: true, effort: "medium", dispatchID: fmt.Sprintf("conflict-%s-%d", strings.ReplaceAll(repo, "/", "-"), prNum)}
	if cleanSHA := strings.TrimSpace(headSHA); cleanSHA != "" {
		opts.dispatchID += "-" + cleanSHA
	}
	return s.dispatchAlertWithPrompt(ctx, targetID, "conflict", "", prompt, opts)
}
