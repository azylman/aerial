// Package widget renders the Discord build card and Flux/Kustomization/job rollout status.
package widget

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// Status vocabulary mirrored from the executor frozen copies; values stay byte-identical.
const (
	chunkModeAction       = "action"
	statusRunning         = "running"
	statusOpen            = "open"
	statusPROpened        = "pr_opened"
	statusMerged          = "merged"
	statusSynced          = "synced"
	statusDeploying       = "deploying"
	statusDeployed        = "deployed"
	statusCompleted       = "completed"
	statusFailed          = "failed"
	statusCancelled       = "cancelled"
	dispatchStatusSuccess = "success"
)

// BuildSyncWidgetRequest formats the Discord tracker widget request for a build.
func BuildSyncWidgetRequest(build model.VanadisBuild, chunks []model.VanadisChunk, deployUIURL ...string) ericlient.StatusWidgetRequest {
	widgetStatus := store.FirstNonEmpty(NormalizeWidgetStatus(build.Status), statusRunning)
	baseURL := store.FirstNonEmpty(deployUIURL...)
	wChunks := make([]ericlient.StatusWidgetChunk, len(chunks))
	for i, c := range chunks {
		wChunks[i] = BuildStatusWidgetChunk(c, baseURL)
	}
	return ericlient.StatusWidgetRequest{
		ChannelID: build.ThreadID,
		MessageID: build.StatusMessageID,
		ID:        build.ID,
		Tier:      build.Tier,
		Title:     store.FirstNonEmpty(strings.TrimSpace(build.Title), build.Objective),
		Status:    widgetStatus,
		Banner:    build.Objective,
		SkipSink:  build.Quiet,
		Chunks:    wChunks,
		StartedAt: build.CreatedAt,
	}
}

// BuildStatusWidgetChunk formats a single chunk for the Discord tracker widget.
func BuildStatusWidgetChunk(c model.VanadisChunk, deployUIURL ...string) ericlient.StatusWidgetChunk {
	baseURL := store.FirstNonEmpty(deployUIURL...)
	// Action chunks have no repository; only PR chunks fall back to the default repo.
	chunkRepo := ""
	if c.Mode != chunkModeAction {
		chunkRepo = store.FirstNonEmpty(c.Repo, "aerial")
	}
	chunkStatus := NormalizeWidgetStatus(c.Status)
	activeStep := FormatChunkActiveStep(c, baseURL)
	if activeStep == "" && !isChunkTerminal(chunkStatus) {
		activeStep = GetChunkActiveStep(c.ID)
	}
	return ericlient.StatusWidgetChunk{
		ID:          c.ID,
		Index:       c.ChunkIndex,
		Mode:        c.Mode,
		Repo:        chunkRepo,
		Title:       c.Title,
		Status:      chunkStatus,
		ActiveStep:  activeStep,
		PRNumber:    c.PRNumber,
		StartedAt:   c.StartedAt,
		CompletedAt: c.CompletedAt,
		UpdatedAt:   c.UpdatedAt,
	}
}

// NormalizeWidgetStatus maps lifecycle states to widget display statuses.
func NormalizeWidgetStatus(status string) string {
	s := strings.ToLower(strings.TrimSpace(status))
	switch s {
	case "coding", statusRunning:
		return statusRunning
	case statusPROpened, statusOpen:
		return statusOpen
	case statusCompleted, "deployed":
		return statusCompleted
	case statusMerged:
		return statusMerged
	case statusFailed, "crashed", "timeout":
		return statusFailed
	case statusCancelled:
		return statusCancelled
	default:
		return s
	}
}

var buildJobRegex = regexp.MustCompile(`(?i)Build\s*(?:&|and)\s*(?:Push|Publish)(?:\s+(?:Sidecar\s+)?(?:Container\s+)?Images?(?:\s+to\s+GHCR)?)?\s*\(([^)]+)\)`)

func buildJobService(checkName string) (string, bool) {
	svc := strings.TrimSpace(append(buildJobRegex.FindStringSubmatch(checkName), "", "")[1])
	return svc, svc != "" && !strings.Contains(svc, "${{")
}

func dedupeTrimmedStrings(items []string) []string {
	seen := make(map[string]bool, len(items))
	out := make([]string, 0, len(items))
	for _, it := range items {
		clean := strings.TrimSpace(it)
		if clean == "" {
			continue
		}
		lower := strings.ToLower(clean)
		if seen[lower] {
			continue
		}
		seen[lower] = true
		out = append(out, clean)
	}
	return out
}

const statusPassed = "passed"

func formatOpenCIChecks(checks map[string]string) string {
	var total, passed, failed int
	for _, state := range checks {
		s := strings.ToLower(strings.TrimSpace(state))
		switch s {
		case statusPassed, dispatchStatusSuccess:
			passed++
			total++
		case statusFailed, "failure":
			failed++
			total++
		default:
			if s != "" {
				total++
			}
		}
	}
	if failed > 0 || total == 0 {
		return ""
	}
	return fmt.Sprintf("Waiting on CI (%d/%d passed)", passed, total)
}

func analyzeChunkBuildChecks(checks map[string]string) (checkJobs []string, passedBuildJobs map[string]bool, total, passed, failed int) {
	matchedServices := make(map[string]struct{})
	passedBuildJobs = make(map[string]bool)

	for checkName, state := range checks {
		s := strings.ToLower(strings.TrimSpace(state))
		switch s {
		case statusPassed, dispatchStatusSuccess:
			passed++
			total++
		case statusFailed, "failure":
			failed++
			total++
		default:
			if s != "" {
				total++
			}
		}

		if svc, ok := buildJobService(checkName); ok {
			matchedServices[svc] = struct{}{}
			if s == statusPassed || s == dispatchStatusSuccess {
				passedBuildJobs[strings.ToLower(svc)] = true
			}
		}
	}

	if len(matchedServices) > 0 {
		for svc := range matchedServices {
			checkJobs = append(checkJobs, svc)
		}
		sort.Strings(checkJobs)
		checkJobs = dedupeTrimmedStrings(checkJobs)
	}
	return checkJobs, passedBuildJobs, total, passed, failed
}

func formatDeployTargetLabel(name, kind, deployUIURL string) string {
	base := strings.TrimRight(strings.TrimSpace(deployUIURL), "/")
	if base == "" {
		return fmt.Sprintf("`%s`", name)
	}
	cleanName := strings.TrimSpace(name)
	var route string
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "kustomization":
		route = fmt.Sprintf("%s/c/main/flux/kustomize/kustomizations/flux-system/%s", base, cleanName)
	default:
		switch strings.ToLower(cleanName) {
		case "git-sync":
			route = base + "/c/main/flux/sources"
		case "github-runner":
			route = base + "/c/main/flux/helmreleases"
		default:
			route = fmt.Sprintf("%s/c/main/deployments/default/%s", base, cleanName)
		}
	}
	return fmt.Sprintf("[`%s`](%s)", name, route)
}

func formatSingleJobRolloutState(job, cleanStatus string, deployedSet, startedSet map[string]bool, jobStates map[string]string, passedBuildJobs map[string]bool, hasStartedJobs bool, deployUIURL ...string) string {
	baseURL := store.FirstNonEmpty(deployUIURL...)
	label := formatDeployTargetLabel(job, "job", baseURL)
	jKey := strings.ToLower(strings.TrimSpace(job))
	switch {
	case deployedSet[jKey]:
		return label + ": ✅ healthy"
	case startedSet[jKey] && strings.EqualFold(jobStates[jKey], "verifying_health"):
		return label + ": 🔄 verifying health"
	case startedSet[jKey]:
		return label + ": 🔄 starting"
	case (!hasStartedJobs && cleanStatus == statusDeploying) || passedBuildJobs[jKey]:
		return label + ": 🔄 starting"
	default:
		return label + ": ⏳ building image"
	}
}

// FormatPerJobRolloutStatus renders per-job rollout lines for a deploying chunk.
func FormatPerJobRolloutStatus(allJobs []string, cleanStatus string, startedJobs, deployedJobs []string, jobStates map[string]string, passedBuildJobs map[string]bool, deployUIURL ...string) string {
	baseURL := store.FirstNonEmpty(deployUIURL...)
	deployedSet := make(map[string]bool, len(deployedJobs))
	for _, j := range deployedJobs {
		deployedSet[strings.ToLower(strings.TrimSpace(j))] = true
	}
	startedSet := make(map[string]bool, len(startedJobs))
	for _, j := range startedJobs {
		startedSet[strings.ToLower(strings.TrimSpace(j))] = true
	}
	hasStartedJobs := len(startedJobs) > 0

	lines := make([]string, 0, len(allJobs))
	for _, j := range allJobs {
		lines = append(lines, formatSingleJobRolloutState(j, cleanStatus, deployedSet, startedSet, jobStates, passedBuildJobs, hasStartedJobs, baseURL))
	}
	return strings.Join(lines, "\n")
}

// FormatKustomizationRolloutStatus renders kustomization rollout lines for a deploying chunk.
func FormatKustomizationRolloutStatus(cleanStatus string, kustomizationStatuses map[string]string, deployUIURL ...string) (string, bool) {
	if cleanStatus != statusDeploying {
		return "", false
	}
	baseURL := store.FirstNonEmpty(deployUIURL...)
	names := make([]string, 0, len(kustomizationStatuses))
	for name := range kustomizationStatuses {
		names = append(names, name)
	}
	sort.Strings(names)
	lines := make([]string, 0, len(names))
	for _, name := range names {
		status := strings.ToLower(strings.TrimSpace(kustomizationStatuses[name]))
		state := "🔄 rolling out"
		if status == statusDeployed || status == "healthy" || status == "rolled_out" {
			state = "✅ rolled out"
		}
		label := formatDeployTargetLabel(name, "kustomization", baseURL)
		lines = append(lines, label+": "+state)
	}
	return strings.Join(lines, "\n"), true
}

func resolveCandidateJobs(rs *pubsub.PRRolloutState, checkJobs []string) []string {
	if rs == nil {
		return checkJobs
	}
	combined := append(append(append([]string(nil), rs.ExpectedJobs...), rs.StartedJobs...), rs.DeployedJobs...)
	if candidateJobs := dedupeTrimmedStrings(combined); len(candidateJobs) > 0 {
		sort.Strings(candidateJobs)
		return candidateJobs
	}
	return checkJobs
}

func formatRecoverableFailureStep(cleanStatus, banner string) (string, bool) {
	if cleanStatus != statusCIFailed && cleanStatus != statusConflict {
		return "", false
	}
	if trimmed := strings.TrimSpace(banner); trimmed != "" {
		return trimmed + " — remediation in progress", true
	}
	return "Remediation in progress", true
}

// FormatChunkActiveStep formats the step string for a chunk in a widget.
func FormatChunkActiveStep(chunk model.VanadisChunk, deployUIURL ...string) string {
	baseURL := store.FirstNonEmpty(deployUIURL...)
	cleanStatus := strings.ToLower(strings.TrimSpace(chunk.Status))
	if step, ok := formatRecoverableFailureStep(cleanStatus, chunk.Banner); ok {
		return step
	}
	if cleanStatus == statusOpen || cleanStatus == statusPROpened {
		if len(chunk.CIChecks) > 0 {
			return formatOpenCIChecks(chunk.CIChecks)
		}
		return ""
	}
	if cleanStatus != statusMerged && cleanStatus != statusSynced && cleanStatus != statusDeploying {
		return ""
	}

	checkJobs, passedBuildJobs, total, passed, failed := analyzeChunkBuildChecks(chunk.CIChecks)
	if failed > 0 {
		return ""
	}

	if chunk.RolloutState != nil && len(chunk.RolloutState.KustomizationStatuses) > 0 {
		if rollout, ok := FormatKustomizationRolloutStatus(cleanStatus, chunk.RolloutState.KustomizationStatuses, baseURL); ok {
			return rollout
		}
	}

	allJobs := resolveCandidateJobs(chunk.RolloutState, checkJobs)
	if len(allJobs) > 0 {
		var startedJobs, deployedJobs []string
		var jobStates map[string]string
		if chunk.RolloutState != nil {
			startedJobs = chunk.RolloutState.StartedJobs
			deployedJobs = chunk.RolloutState.DeployedJobs
			jobStates = chunk.RolloutState.JobStates
		}
		return FormatPerJobRolloutStatus(allJobs, cleanStatus, startedJobs, deployedJobs, jobStates, passedBuildJobs, baseURL)
	}

	if (cleanStatus == statusMerged || cleanStatus == statusSynced) && total > 0 && failed == 0 {
		return fmt.Sprintf("Building container images (%d/%d completed)", passed, total)
	}

	if cleanStatus == statusDeploying {
		return "Deploying to cluster"
	}

	return ""
}
