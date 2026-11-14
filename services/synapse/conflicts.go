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
	if s.riverClient != nil {
		if err := s.insertRiverJob(ctx, args); err != nil {
			log.Printf("[synapse] [github] error enqueuing merge conflict into river: %v", err)
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
	if prNum > 0 && s.registry != nil {
		if _, isPg := s.registry.(*PostgresPRRegistry); isPg {
			s.setPRWidgetState(ctx, repo, prNum, "conflict", "Merge conflict detected")
			if err := s.SyncPRWidget(ctx, repo, prNum); err != nil {
				log.Printf("[synapse] [widget] warning syncing conflict widget: %v", err)
			}
		}
	}
	shortRepo := firstNonEmpty(cleanRepoName(repo), "aerial")
	cleanBranch := firstNonEmpty(strings.TrimPrefix(strings.TrimSpace(branch), "refs/heads/"), "<branch>")
	prompt := fmt.Sprintf("Merge conflict detected on PR #%d on %s (branch: %s, head: %s). Please check out the branch via '/share/aerial/scripts/aerial-pr.sh init %s --branch %s', merge origin/main to resolve conflicts, verify locally, and push to update the PR via '/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> \"<commit_msg>\"' (PR_DESCRIPTION.md is not required when updating an existing PR; never run bare 'git push').", prNum, repo, branch, headSHA, shortRepo, cleanBranch)
	opts := s.vanadisPromptOpts(ctx, repo, prNum)
	opts.silent, opts.skipIfActive, opts.effort = true, true, firstNonEmpty(opts.effort, "medium")
	opts.dispatchID = fmt.Sprintf("conflict-%s-%d", strings.ReplaceAll(repo, "/", "-"), prNum)
	if cleanSHA := strings.TrimSpace(headSHA); cleanSHA != "" {
		opts.dispatchID += "-" + cleanSHA
	}
	return s.dispatchAlertWithPrompt(ctx, targetID, "conflict", "", prompt, opts)
}
