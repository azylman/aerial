package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"

	"github.com/azylman/aerial/pkg/pubsub"
)

var buildJobRegex = regexp.MustCompile(`(?i)Build\s*(?:&|and)\s*(?:Push|Publish)(?:\s+(?:Sidecar\s+)?(?:Container\s+)?Images?(?:\s+to\s+GHCR)?)?\s*\(([^)]+)\)`)

// buildJobService returns a build check's service name, rejecting empty or unexpanded "${{ }}" captures.
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

// formatCIChecksActiveStep renders active step progress for open PR checks.
func formatCIChecksActiveStep(prStatus, ciChecksJSON string) string {
	if !strings.EqualFold(strings.TrimSpace(prStatus), "open") {
		return ""
	}
	trimmedJSON := strings.TrimSpace(ciChecksJSON)
	if trimmedJSON == "" || trimmedJSON == "{}" {
		return ""
	}
	var checks map[string]string
	if err := json.Unmarshal([]byte(trimmedJSON), &checks); err != nil || len(checks) == 0 {
		return ""
	}
	var total, passed, failed int
	for _, state := range checks {
		s := strings.ToLower(strings.TrimSpace(state))
		switch s {
		case statusPassed:
			passed++
			total++
		case statusFailed:
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

func (s *VanadisServer) deployUIURL() string {
	if s == nil {
		return ""
	}
	if u := strings.TrimRight(strings.TrimSpace(s.cfg.DeployUIURL), "/"); u != "" {
		return u
	}
	if strings.TrimSpace(s.cfg.ConfigPath) != "" {
		return strings.TrimRight(strings.TrimSpace(LoadConfig(s.cfg.ConfigPath).DeployUIURL), "/")
	}
	return ""
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
	baseURL := firstNonEmpty(deployUIURL...)
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

// formatPerJobRolloutStatus formats per-job deployment rollout states.
func formatPerJobRolloutStatus(allJobs []string, cleanStatus string, startedJobs, deployedJobs []string, jobStates map[string]string, passedBuildJobs map[string]bool, deployUIURL ...string) string {
	baseURL := firstNonEmpty(deployUIURL...)
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

// formatKustomizationRolloutStatus renders a deploying PR's recorded Kustomizations one per line.
func formatKustomizationRolloutStatus(cleanStatus string, kustomizationStatuses map[string]string, deployUIURL ...string) (string, bool) {
	if cleanStatus != statusDeploying || len(kustomizationStatuses) == 0 {
		return "", false
	}
	baseURL := firstNonEmpty(deployUIURL...)
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

func analyzeChunkBuildChecks(checks map[string]string) (checkJobs []string, passedBuildJobs map[string]bool, total, passed, failed int) {
	matchedServices := make(map[string]struct{})
	passedBuildJobs = make(map[string]bool)

	for checkName, state := range checks {
		s := strings.ToLower(strings.TrimSpace(state))
		switch s {
		case statusPassed:
			passed++
			total++
		case statusFailed:
			failed++
			total++
		default:
			if s != "" {
				total++
			}
		}

		if svc, ok := buildJobService(checkName); ok {
			matchedServices[svc] = struct{}{}
			if s == statusPassed {
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

// FormatChunkActiveStep formats the dynamic active_step string for a VanadisChunk with optional Headlamp deep-link URLs.
func FormatChunkActiveStep(chunk VanadisChunk, deployUIURL ...string) string {
	baseURL := firstNonEmpty(deployUIURL...)
	cleanStatus := strings.ToLower(strings.TrimSpace(chunk.Status))
	if cleanStatus == statusOpen || cleanStatus == statusPROpened {
		if len(chunk.CIChecks) > 0 {
			if rawJSON, err := json.Marshal(chunk.CIChecks); err == nil {
				return formatCIChecksActiveStep("open", string(rawJSON))
			}
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
		if rollout, ok := formatKustomizationRolloutStatus(cleanStatus, chunk.RolloutState.KustomizationStatuses, baseURL); ok {
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
		return formatPerJobRolloutStatus(allJobs, cleanStatus, startedJobs, deployedJobs, jobStates, passedBuildJobs, baseURL)
	}

	if (cleanStatus == statusMerged || cleanStatus == statusSynced) && total > 0 && failed == 0 {
		return fmt.Sprintf("Building container images (%d/%d completed)", passed, total)
	}

	return ""
}

func (s *VanadisServer) resolveCoalescedWidgetBuildID(ctx context.Context, payload pubsub.WidgetSyncJobArgs) (string, error) {
	buildID := strings.TrimSpace(payload.BuildID)
	if buildID != "" {
		return buildID, nil
	}
	if strings.TrimSpace(payload.Repo) == "" || payload.PRNumber <= 0 {
		return "", nil
	}
	lookup, err := s.store.GetChunkByPR(ctx, strings.TrimSpace(payload.Repo), payload.PRNumber)
	if err != nil {
		return "", err
	}
	if lookup != nil && lookup.Matched {
		return strings.TrimSpace(lookup.BuildID), nil
	}
	return "", nil
}

// HandleCoalescedWidgetMessage processes an aerial.coalesced.widget_sync.> message from JetStream,
// as published by syncBuildWidget or PR widget sync events, and posts or edits the build's Discord status card.
func (s *VanadisServer) HandleCoalescedWidgetMessage(ctx context.Context, msg pubsub.Message) error {
	if s == nil || s.store == nil {
		if termErr := msg.Term(); termErr != nil {
			log.Printf("[vanadis] warning: error terming coalesced widget message: %v", termErr)
		}
		return nil
	}
	var payload pubsub.WidgetSyncJobArgs
	if err := json.Unmarshal(msg.Data(), &payload); err != nil {
		log.Printf("[vanadis] failed to unmarshal coalesced widget message: %v", err)
		if termErr := msg.Term(); termErr != nil {
			log.Printf("[vanadis] warning: error terming invalid widget message: %v", termErr)
		}
		return nil
	}
	buildID, err := s.resolveCoalescedWidgetBuildID(ctx, payload)
	if err != nil {
		return err
	}
	if buildID == "" {
		if ackErr := msg.Ack(); ackErr != nil {
			log.Printf("[vanadis] warning: failed acknowledging empty build widget message: %v", ackErr)
		}
		return nil
	}
	build, chunks, err := s.store.GetBuild(ctx, buildID)
	if err != nil {
		if errors.Is(err, ErrBuildNotFound) {
			if ackErr := msg.Ack(); ackErr != nil {
				log.Printf("[vanadis] warning: failed acknowledging not-found build widget message: %v", ackErr)
			}
			return nil
		}
		return err
	}
	syncReq := SyncChunkPRWidgetRequest{
		BuildID:  buildID,
		PRBanner: payload.Banner,
	}
	widgetReq := buildSyncWidgetRequest(*build, chunks, syncReq, s.deployUIURL())
	if err := s.dispatchAndPersistWidget(ctx, build.ID, build.StatusMessageID, widgetReq); err != nil {
		log.Printf("[vanadis] warning: dispatchAndPersistWidget failed for build %s: %v", buildID, err)
		if msg.NumDelivered() >= 3 {
			if termErr := msg.Term(); termErr != nil {
				log.Printf("[vanadis] warning: error terming failed widget message: %v", termErr)
			}
		}
		return err
	}
	if ackErr := msg.Ack(); ackErr != nil {
		log.Printf("[vanadis] warning: failed acknowledging widget message: %v", ackErr)
	}
	return nil
}
