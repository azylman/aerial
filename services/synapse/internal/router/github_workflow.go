package router

import (
	"context"
	"log/slog"
	"strings"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/services/synapse/internal/buildmeta"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

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
