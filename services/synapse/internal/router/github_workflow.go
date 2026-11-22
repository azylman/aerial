package router

import (
	"context"
	"log/slog"
	"regexp"
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

type (
	metadataWorkflowRun = buildmeta.MetadataWorkflowRun
	workflowRunEvent    = buildmeta.WorkflowRunEvent
)

var (
	errStaleWorkflowEvent = buildmeta.ErrStaleWorkflowEvent
	errNotImageWorkflow   = buildmeta.ErrNotImageWorkflow
)

func logBuildMetadataWarning(ctx context.Context, msg, repo string, pr int, err error) {
	slog.WarnContext(ctx, "[synapse] [build_metadata] "+msg, "repo", repo, "pr", pr, "error", err)
}
