package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

type pipelineEntry struct {
	dep       *DeploymentStatus
	canonical string
}

type pipelineRegistry struct {
	entries []*pipelineEntry
}

func isHexSHA(s string) bool {
	trimmed := strings.TrimSpace(s)
	if len(trimmed) < 7 || len(trimmed) > 40 {
		return false
	}
	for _, ch := range trimmed {
		if !((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F')) {
			return false
		}
	}
	return true
}

func normalizeSHA(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

func shasMatchPrefix(a, b string) bool {
	return len(a) >= 7 && len(b) >= 7 && (strings.HasPrefix(a, b) || strings.HasPrefix(b, a))
}

func (r *pipelineRegistry) find(sha string) *pipelineEntry {
	if sha == "" {
		return nil
	}
	norm := normalizeSHA(sha)
	for _, e := range r.entries {
		if e.canonical == norm {
			return e
		}
		if isHexSHA(e.canonical) && isHexSHA(norm) && shasMatchPrefix(e.canonical, norm) {
			return e
		}
	}
	return nil
}

func (r *pipelineRegistry) getOrCreate(sha, fallbackID string) *pipelineEntry {
	if e := r.find(sha); e != nil {
		return e
	}
	norm := normalizeSHA(sha)
	short := shortenSHA(norm)
	if short == "" {
		short = fallbackID
	}
	dep := &DeploymentStatus{
		ID:      fallbackID,
		Service: "aerial-stack",
		Commit:  short,
	}
	entry := &pipelineEntry{
		dep:       dep,
		canonical: norm,
	}
	r.entries = append(r.entries, entry)
	return entry
}

func reconciliationMatchesCommit(recon *ReconciliationStatus, depCommit string) bool {
	return recon != nil && len(recon.TargetServices) > 0 &&
		(recon.CommitSHA == "" || strings.HasPrefix(depCommit, shortenSHA(recon.CommitSHA)) || strings.HasPrefix(shortenSHA(recon.CommitSHA), depCommit))
}

func attachContainerChips(dep *DeploymentStatus, groupContainers []DockerContainerJSON, gitSync GitSyncStatusResponse, refTime time.Time) {
	for _, chip := range dep.MatrixJobs {
		if chip.Type == "container" {
			return
		}
	}
	if reconciliationMatchesCommit(gitSync.Reconciliation, dep.Commit) {
		containerChips := BuildTargetContainerChips(groupContainers, gitSync.Reconciliation.TargetServices, gitSync.Reconciliation.StartedAt, refTime)
		dep.MatrixJobs = append(dep.MatrixJobs, containerChips...)
	} else {
		containerChips := BuildContainerChips(groupContainers, refTime)
		dep.MatrixJobs = append(dep.MatrixJobs, containerChips...)
	}
}

func extractRunCommitMetadata(run GitHubRun) (string, *time.Time) {
	commitMsg := ""
	var commitTime *time.Time
	if run.HeadCommit != nil {
		commitMsg = run.HeadCommit.Message
		if !run.HeadCommit.Timestamp.IsZero() {
			t := run.HeadCommit.Timestamp
			commitTime = &t
		}
	}
	if commitTime == nil && !run.CreatedAt.IsZero() {
		t := run.CreatedAt
		commitTime = &t
	}
	return commitMsg, commitTime
}

func hasActiveContainerBuildJob(runJobs []GitHubJob) bool {
	for _, j := range runJobs {
		if strings.Contains(j.Name, "Build & Push") && strings.Contains(j.Name, "Images") && j.Conclusion != "skipped" {
			return true
		}
	}
	return false
}

func detectRecentContainerSwap(run GitHubRun, shortSHA, currentCommit string, aerialContainers []DockerContainerJSON) bool {
	if currentCommit != "" && (currentCommit == shortSHA || strings.HasPrefix(run.HeadSHA, currentCommit)) {
		return true
	}
	containerCommit := GetContainerCommit(aerialContainers)
	if containerCommit != "" && (containerCommit == shortSHA || strings.HasPrefix(run.HeadSHA, containerCommit)) {
		return true
	}
	for _, c := range aerialContainers {
		createdAt := time.Unix(c.Created, 0).UTC()
		if createdAt.After(run.CreatedAt.Add(-30*time.Second)) || (c.Health != nil && c.Health.Status == "starting") {
			return true
		}
	}
	return false
}

func evaluateSuccessfulCIRun(
	run GitHubRun,
	runJobs []GitHubJob,
	shortSHA, currentCommit string,
	gitSync GitSyncStatusResponse,
	aerialContainers []DockerContainerJSON,
	refTime time.Time,
) (stage, ciStatus, commitMsgOverride string, progress int, skip bool) {
	isPRRun := (run.HeadBranch != "" && run.HeadBranch != "main" && run.HeadBranch != "master") || run.Event == "pull_request"
	if isPRRun {
		return "", "", "", 0, true
	}
	hasContainerBuilds := hasActiveContainerBuildJob(runJobs)
	hasRecentContainerSwap := detectRecentContainerSwap(run, shortSHA, currentCommit, aerialContainers)
	if run.Repository != "" && !strings.HasSuffix(run.Repository, "/aerial") && !hasContainerBuilds {
		hasRecentContainerSwap = true
	}
	isHangarActive := gitSync.Reconciliation != nil && gitSync.Reconciliation.Active
	ciElapsed := refTime.Sub(run.UpdatedAt)
	if ciElapsed < 0 {
		ciElapsed = refTime.Sub(run.CreatedAt)
	}
	if len(runJobs) > 0 && !hasContainerBuilds {
		return "live", "completed", "", 100, false
	}
	if hasContainerBuilds && !hasRecentContainerSwap && !isHangarActive && ciElapsed > 120*time.Second {
		return "failed", "completed", "Hangar reconciliation timed out (>120s)", 55, false
	}
	return "awaiting_pull", "completed", "", 55, false
}

func evaluateCompletedCIRun(
	run GitHubRun,
	runJobs []GitHubJob,
	shortSHA, currentCommit string,
	gitSync GitSyncStatusResponse,
	aerialContainers []DockerContainerJSON,
	refTime time.Time,
) (stage, ciStatus, commitMsgOverride string, progress int, skip bool) {
	if refTime.Sub(run.UpdatedAt) >= 30*time.Minute && refTime.Sub(run.CreatedAt) >= 30*time.Minute {
		return "", "pending", "", 0, false
	}
	switch run.Conclusion {
	case "failure", "cancelled":
		return "failed", "failed", "", 40, false
	case "success":
		return evaluateSuccessfulCIRun(run, runJobs, shortSHA, currentCommit, gitSync, aerialContainers, refTime)
	default:
		return "", "pending", "", 0, false
	}
}

func evaluateCIRunStage(
	run GitHubRun,
	runJobs []GitHubJob,
	ciChips []MatrixJobChip,
	shortSHA, currentCommit string,
	gitSync GitSyncStatusResponse,
	aerialContainers []DockerContainerJSON,
	refTime time.Time,
) (stage, ciStatus, commitMsgOverride string, progress int) {
	switch run.Status {
	case "queued":
		return "queued", "pending", "", 15
	case "in_progress":
		progress := 25
		if len(ciChips) > 0 {
			doneCount := 0
			for _, c := range ciChips {
				if c.Status == "completed" {
					doneCount++
				}
			}
			progress = 20 + int(float64(doneCount)/float64(len(ciChips))*30)
		}
		return "building", "active", "", progress
	case "completed":
		stage, ciStatus, commitMsgOverride, progress, skip := evaluateCompletedCIRun(
			run, runJobs, shortSHA, currentCommit, gitSync, aerialContainers, refTime,
		)
		if skip {
			return "", "", "", 0
		}
		return stage, ciStatus, commitMsgOverride, progress
	default:
		return "", "pending", "", 0
	}
}

func shouldUpdateDeploymentFromRun(dep *DeploymentStatus, stage string, runCreatedAt time.Time) bool {
	return dep.Stage == "" || stageRank[stage] > stageRank[dep.Stage] ||
		(stageRank[stage] == stageRank[dep.Stage] && runCreatedAt.After(dep.StartedAt))
}

func applyRepositoryService(dep *DeploymentStatus, repo string) {
	if repo == "" {
		return
	}
	dep.Repository = repo
	parts := strings.Split(repo, "/")
	if len(parts) == 2 && parts[1] != "aerial" {
		dep.Service = parts[1]
	}
}

func applyCIRunToDeployment(
	dep *DeploymentStatus,
	run GitHubRun,
	shortSHA, commitMsg string,
	commitTime *time.Time,
	stage, ciStatus string,
	progress int,
	ciChips []MatrixJobChip,
) {
	dep.ID = fmt.Sprintf("gh-run-%d", run.ID)
	dep.Commit = shortSHA
	applyRepositoryService(dep, run.Repository)
	if commitMsg != "" {
		dep.CommitMsg = commitMsg
	}
	if commitTime != nil {
		dep.CommitTime = commitTime
	}
	if run.HTMLURL != "" {
		dep.HTMLURL = run.HTMLURL
	}
	if !run.CreatedAt.IsZero() {
		dep.StartedAt = run.CreatedAt
	}

	hangarStatus := "pending"
	if stage == "awaiting_pull" {
		hangarStatus = "active"
	} else if stage == "failed" && commitMsg == "Hangar reconciliation timed out (>120s)" {
		hangarStatus = "failed"
	} else if stage == "live" {
		hangarStatus = "completed"
	}

	step4Status := "pending"
	step5Status := "pending"
	if stage == "live" {
		step4Status = "completed"
		step5Status = "completed"
	}

	dep.Stage = stage
	dep.Progress = progress
	dep.Steps = buildDeploymentSteps("completed", ciStatus, hangarStatus, step4Status, step5Status)
	dep.MatrixJobs = ciChips
}

func ingestGitHubRun(
	reg *pipelineRegistry,
	run GitHubRun,
	jobs map[int64][]GitHubJob,
	gitSync GitSyncStatusResponse,
	aerialContainers []DockerContainerJSON,
	currentCommit string,
	refTime time.Time,
) {
	normSHA := normalizeSHA(run.HeadSHA)
	shortSHA := shortenSHA(normSHA)
	if shortSHA == "" {
		shortSHA = fmt.Sprintf("gh-run-%d", run.ID)
	}

	commitMsg, commitTime := extractRunCommitMetadata(run)

	var runJobs []GitHubJob
	if jobs != nil {
		runJobs = jobs[run.ID]
	}
	ciChips := ParseMatrixJobChips(runJobs, refTime)

	stage, ciStatus, commitMsgOverride, progress := evaluateCIRunStage(
		run, runJobs, ciChips, shortSHA, currentCommit, gitSync, aerialContainers, refTime,
	)
	if stage == "" {
		return
	}
	if commitMsgOverride != "" {
		commitMsg = commitMsgOverride
	}

	entry := reg.getOrCreate(normSHA, fmt.Sprintf("gh-run-%d", run.ID))
	dep := entry.dep
	applyRepositoryService(dep, run.Repository)

	if shouldUpdateDeploymentFromRun(dep, stage, run.CreatedAt) {
		applyCIRunToDeployment(dep, run, shortSHA, commitMsg, commitTime, stage, ciStatus, progress, ciChips)
	}
}

func isReconciliationReportable(recon *ReconciliationStatus) bool {
	return recon != nil && (recon.Active || recon.State == "failed" || recon.State == "pulling" || recon.State == "swapping" || recon.State == "healthy")
}

func resolveReconciliationSHA(recon *ReconciliationStatus, runs []GitHubRun, currentCommit string) string {
	if recon.CommitSHA != "" {
		return recon.CommitSHA
	}
	if len(runs) > 0 && runs[0].HeadSHA != "" {
		return runs[0].HeadSHA
	}
	return currentCommit
}

func applyReconciliationState(dep *DeploymentStatus, recon *ReconciliationStatus) {
	switch recon.State {
	case "healthy":
		if stageRank[dep.Stage] < stageRank["live"] {
			dep.Stage = "live"
			dep.Progress = 100
		}
		if len(dep.Steps) < 5 {
			dep.Steps = buildDeploymentSteps("completed", "completed", "completed", "completed", "completed")
		} else {
			for i := range dep.Steps {
				dep.Steps[i].Status = "completed"
			}
		}
	case "pulling":
		if stageRank[dep.Stage] < stageRank["pulling"] {
			dep.Stage = "pulling"
			dep.Progress = 55
		}
		if len(dep.Steps) < 5 {
			dep.Steps = buildDeploymentSteps("completed", "completed", "active", "pending", "pending")
		} else {
			dep.Steps[1].Status = "completed"
			dep.Steps[2].Status = "active"
		}
	case "swapping":
		if stageRank[dep.Stage] < stageRank["swapping"] {
			dep.Stage = "swapping"
			dep.Progress = 60
		}
		if len(dep.Steps) < 5 {
			dep.Steps = buildDeploymentSteps("completed", "completed", "completed", "active", "active")
		} else {
			dep.Steps[1].Status = "completed"
			dep.Steps[2].Status = "completed"
			dep.Steps[3].Status = "active"
			dep.Steps[4].Status = "active"
		}
	case "failed":
		dep.Stage = "failed"
		dep.Progress = 55
		failMsg := "Hangar reconciliation failed"
		if recon.Error != "" {
			failMsg = "Hangar reconciliation failed: " + recon.Error
		}
		dep.CommitMsg = failMsg
		if len(dep.Steps) < 5 {
			dep.Steps = buildDeploymentSteps("completed", "completed", "failed", "pending", "pending")
		} else {
			dep.Steps[2].Status = "failed"
		}
	}
}

func ingestHangarReconciliation(
	reg *pipelineRegistry,
	runs []GitHubRun,
	gitSync GitSyncStatusResponse,
	aerialContainers []DockerContainerJSON,
	currentCommit string,
	refTime time.Time,
) {
	recon := gitSync.Reconciliation
	if !isReconciliationReportable(recon) {
		return
	}
	reconSHA := resolveReconciliationSHA(recon, runs, currentCommit)
	normReconSHA := normalizeSHA(reconSHA)
	shortReconSHA := shortenSHA(normReconSHA)
	if shortReconSHA == "" {
		shortReconSHA = "hangar"
	}

	entry := reg.getOrCreate(normReconSHA, "dep-aerial-stack-"+shortReconSHA)
	dep := entry.dep

	if dep.StartedAt.IsZero() && !recon.StartedAt.IsZero() {
		dep.StartedAt = recon.StartedAt
	}

	applyReconciliationState(dep, recon)

	if len(recon.TargetServices) > 0 {
		containerChips := BuildTargetContainerChips(aerialContainers, recon.TargetServices, recon.StartedAt, refTime)
		dep.MatrixJobs = append(dep.MatrixJobs, containerChips...)
	}
}

func resolveContainerCommit(c DockerContainerJSON, recon *ReconciliationStatus, currentCommit string) string {
	commit := ExtractSingleContainerCommit(c)
	svc, isStackContainer := ExtractAerialServiceName(c)
	if commit == "" && recon != nil && len(recon.TargetServices) > 0 && svc != "" {
		for _, ts := range recon.TargetServices {
			if ts == svc {
				if recon.CommitSHA != "" {
					commit = recon.CommitSHA
				}
				break
			}
		}
	}
	if commit == "" && isStackContainer {
		commit = currentCommit
	}
	return commit
}

func groupContainersByCommit(aerialContainers []DockerContainerJSON, gitSync GitSyncStatusResponse, currentCommit string) map[string][]DockerContainerJSON {
	groupsByCommit := make(map[string][]DockerContainerJSON)
	for _, c := range aerialContainers {
		commit := resolveContainerCommit(c, gitSync.Reconciliation, currentCommit)
		if commit == "" {
			continue
		}
		norm := normalizeSHA(commit)
		matchedKey := ""
		for gk := range groupsByCommit {
			if shasMatchPrefix(gk, norm) {
				matchedKey = gk
				break
			}
		}
		if matchedKey != "" {
			groupsByCommit[matchedKey] = append(groupsByCommit[matchedKey], c)
		} else {
			groupsByCommit[norm] = append(groupsByCommit[norm], c)
		}
	}
	return groupsByCommit
}

func summarizeContainerGroup(groupContainers []DockerContainerJSON, refTime time.Time) (latestCreatedAt time.Time, minUptimeSec int64, hasStarting, hasDegraded bool, healthyCount int) {
	minUptimeSec = int64(999999999)
	for _, c := range groupContainers {
		createdAt := time.Unix(c.Created, 0).UTC()
		if createdAt.After(latestCreatedAt) {
			latestCreatedAt = createdAt
		}
		uptimeSec := int64(refTime.Sub(createdAt).Seconds())
		if uptimeSec < 0 {
			uptimeSec = 0
		}
		if uptimeSec < minUptimeSec {
			minUptimeSec = uptimeSec
		}
		if c.Health != nil && c.Health.Status == "starting" {
			hasStarting = true
		} else if c.State != "running" || (c.Health != nil && c.Health.Status == "unhealthy") {
			hasDegraded = true
		} else {
			healthyCount++
		}
	}
	return latestCreatedAt, minUptimeSec, hasStarting, hasDegraded, healthyCount
}

func isHangarActiveForGroup(recon *ReconciliationStatus, groupSHA string) bool {
	if recon == nil || !recon.Active {
		return false
	}
	if recon.CommitSHA == "" {
		return true
	}
	normRecon := normalizeSHA(recon.CommitSHA)
	return strings.HasPrefix(groupSHA, normRecon) || strings.HasPrefix(normRecon, groupSHA)
}

func applyGroupDegradedState(dep *DeploymentStatus) {
	dep.Stage = "degraded"
	dep.Progress = 85
	if len(dep.Steps) < 5 {
		dep.Steps = buildDeploymentSteps("completed", "completed", "completed", "completed", "failed")
	} else {
		dep.Steps[3].Status = "completed"
		dep.Steps[4].Status = "failed"
	}
}

func applyGroupSwappingState(dep *DeploymentStatus, healthyCount, totalCount int) {
	if stageRank[dep.Stage] < stageRank["swapping"] {
		dep.Stage = "swapping"
		progress := 60
		if totalCount > 0 {
			progress = 60 + int(float64(healthyCount)/float64(totalCount)*25)
		}
		dep.Progress = progress
	}
	swapStatus := "active"
	if len(dep.Steps) < 5 {
		dep.Steps = buildDeploymentSteps("completed", "completed", "completed", swapStatus, "active")
	} else {
		dep.Steps[1].Status = "completed"
		dep.Steps[2].Status = "completed"
		dep.Steps[3].Status = swapStatus
		dep.Steps[4].Status = "active"
	}
}

func applyGroupPullingState(dep *DeploymentStatus) {
	if stageRank[dep.Stage] < stageRank["pulling"] {
		dep.Stage = "pulling"
		dep.Progress = 55
	}
	if len(dep.Steps) < 5 {
		dep.Steps = buildDeploymentSteps("completed", "completed", "active", "pending", "pending")
	} else {
		dep.Steps[1].Status = "completed"
		dep.Steps[2].Status = "active"
		dep.Steps[3].Status = "pending"
		dep.Steps[4].Status = "pending"
	}
}

func applyGroupLiveGraceState(dep *DeploymentStatus, groupContainers []DockerContainerJSON, gitSync GitSyncStatusResponse, refTime time.Time) {
	if dep.Stage == "" || dep.Stage == "awaiting_pull" || stageRank[dep.Stage] <= stageRank["live"] {
		dep.Stage = "live"
		dep.Progress = 100
		if len(dep.Steps) < 5 {
			dep.Steps = buildDeploymentSteps("completed", "completed", "completed", "completed", "completed")
		} else {
			for i := range dep.Steps {
				dep.Steps[i].Status = "completed"
			}
		}
		attachContainerChips(dep, groupContainers, gitSync, refTime)
	}
}

func applyContainerGroupState(
	dep *DeploymentStatus,
	groupSHA string,
	groupContainers []DockerContainerJSON,
	gitSync GitSyncStatusResponse,
	refTime time.Time,
) {
	latestCreatedAt, minUptimeSec, hasStarting, hasDegraded, healthyCount := summarizeContainerGroup(groupContainers, refTime)
	if dep.StartedAt.IsZero() {
		dep.StartedAt = latestCreatedAt
	}

	isHangarActive := isHangarActiveForGroup(gitSync.Reconciliation, groupSHA)
	isHangarSwapping := isHangarActive && gitSync.Reconciliation.State == "swapping"
	isHangarPulling := isHangarActive && gitSync.Reconciliation.State == "pulling"

	if hasDegraded {
		applyGroupDegradedState(dep)
		attachContainerChips(dep, groupContainers, gitSync, refTime)
	} else if hasStarting || minUptimeSec < 120 || isHangarSwapping {
		applyGroupSwappingState(dep, healthyCount, len(groupContainers))
		attachContainerChips(dep, groupContainers, gitSync, refTime)
	} else if isHangarPulling {
		applyGroupPullingState(dep)
		attachContainerChips(dep, groupContainers, gitSync, refTime)
	} else if minUptimeSec < 600 {
		applyGroupLiveGraceState(dep, groupContainers, gitSync, refTime)
	}
}

func compareDeployments(a, b DeploymentStatus) bool {
	rankA := stageRank[a.Stage]
	rankB := stageRank[b.Stage]
	if rankA != rankB {
		return rankA > rankB
	}
	if a.Stage != b.Stage {
		if a.Stage == "failed" && b.Stage == "degraded" {
			return true
		}
		if a.Stage == "degraded" && b.Stage == "failed" {
			return false
		}
	}
	if !a.StartedAt.Equal(b.StartedAt) {
		return a.StartedAt.After(b.StartedAt)
	}
	return a.Commit < b.Commit
}

// MergeClusterDeploymentsWithHangar aggregates concurrent GitHub Actions CI runs,
// active Hangar host reconciliation, and local container groups into independent pipelines.
func MergeClusterDeploymentsWithHangar(
	runs []GitHubRun,
	jobs map[int64][]GitHubJob,
	gitSync GitSyncStatusResponse,
	aerialContainers []DockerContainerJSON,
	currentCommit string,
	refTime time.Time,
) []DeploymentStatus {
	if refTime.IsZero() {
		refTime = time.Now().UTC()
	}

	reg := &pipelineRegistry{}

	// 1. Ingest GitHub Actions CI runs
	for _, run := range runs {
		ingestGitHubRun(reg, run, jobs, gitSync, aerialContainers, currentCommit, refTime)
	}

	// 2. Ingest Hangar Reconciliation
	ingestHangarReconciliation(reg, runs, gitSync, aerialContainers, currentCommit, refTime)

	// 3. Ingest Local Containers grouped by commit
	groupsByCommit := groupContainersByCommit(aerialContainers, gitSync, currentCommit)
	for groupSHA, groupContainers := range groupsByCommit {
		entry := reg.find(groupSHA)
		if entry == nil {
			// Strictly top-down invariant: Unparented containers NEVER synthesize deployment cards.
			continue
		}
		applyContainerGroupState(entry.dep, groupSHA, groupContainers, gitSync, refTime)
	}

	var result []DeploymentStatus
	for _, e := range reg.entries {
		if e.dep.Stage == "" || e.dep.Stage == "idle" {
			continue
		}
		result = append(result, *e.dep)
	}

	sort.Slice(result, func(i, j int) bool {
		return compareDeployments(result[i], result[j])
	})

	if len(result) > 5 {
		result = result[:5]
	}
	return result
}

type NomadJobSummaryGroup struct {
	Queued   int `json:"Queued"`
	Complete int `json:"Complete"`
	Failed   int `json:"Failed"`
	Running  int `json:"Running"`
	Starting int `json:"Starting"`
}

type NomadJobItem struct {
	ID         string `json:"ID"`
	Name       string `json:"Name"`
	Status     string `json:"Status"`
	Type       string `json:"Type"`
	JobSummary struct {
		Summary map[string]NomadJobSummaryGroup `json:"Summary"`
	} `json:"JobSummary"`
	SubmitTime int64 `json:"SubmitTime"`
}

type NomadDeploymentStatus struct {
	Healthy   bool   `json:"Healthy"`
	Timestamp string `json:"Timestamp"`
}

type NomadTaskState struct {
	State     string     `json:"State"`
	Failed    bool       `json:"Failed"`
	Restarts  int        `json:"Restarts"`
	StartedAt *time.Time `json:"StartedAt"`
}

type NomadAllocItem struct {
	ID               string                    `json:"ID"`
	JobID            string                    `json:"JobID"`
	Name             string                    `json:"Name"`
	ClientStatus     string                    `json:"ClientStatus"`
	DesiredStatus    string                    `json:"DesiredStatus"`
	DeploymentStatus *NomadDeploymentStatus    `json:"DeploymentStatus"`
	CreateTime       int64                     `json:"CreateTime"`
	ModifyTime       int64                     `json:"ModifyTime"`
	TaskStates       map[string]NomadTaskState `json:"TaskStates"`
}

func indexRunningNomadAllocs(allocs []NomadAllocItem) map[string]NomadAllocItem {
	runningAllocs := make(map[string]NomadAllocItem)
	for _, alloc := range allocs {
		if alloc.ClientStatus != "running" {
			continue
		}
		existing, exists := runningAllocs[alloc.JobID]
		if !exists || alloc.CreateTime > existing.CreateTime {
			runningAllocs[alloc.JobID] = alloc
		}
	}
	return runningAllocs
}

func evaluateRunningAllocStatus(alloc NomadAllocItem, now time.Time) (string, int64) {
	var uptimeSec int64
	if alloc.CreateTime > 0 {
		uptimeSec = int64(now.Sub(time.Unix(0, alloc.CreateTime).UTC()).Seconds())
		if uptimeSec < 0 {
			uptimeSec = 0
		}
	}

	if alloc.DeploymentStatus != nil {
		if alloc.DeploymentStatus.Healthy {
			return "healthy", uptimeSec
		}
		for _, task := range alloc.TaskStates {
			if task.State == "running" || task.State == "pending" {
				return "starting", uptimeSec
			}
		}
		return "unhealthy", uptimeSec
	}

	for _, task := range alloc.TaskStates {
		if task.Failed || task.State != "running" {
			return "unhealthy", uptimeSec
		}
	}
	return "healthy", uptimeSec
}

func evaluateUnallocatedJobStatus(job NomadJobItem) (string, int64) {
	startingCount := 0
	for _, grp := range job.JobSummary.Summary {
		startingCount += grp.Starting
	}
	if job.Status == "pending" || startingCount > 0 {
		return "starting", 0
	}
	return "unhealthy", 0
}

func resolveNomadJobServiceStatus(job NomadJobItem, runningAllocs map[string]NomadAllocItem, now time.Time) (string, int64) {
	jobID := job.ID
	if jobID == "" {
		jobID = job.Name
	}
	alloc, hasAlloc := runningAllocs[jobID]
	if !hasAlloc && job.Name != "" {
		alloc, hasAlloc = runningAllocs[job.Name]
	}
	if hasAlloc {
		return evaluateRunningAllocStatus(alloc, now)
	}
	return evaluateUnallocatedJobStatus(job)
}

// ParseNomadClusterState parses Nomad jobs and allocations into unified ServiceStatus items.
func ParseNomadClusterState(jobsData, allocsData []byte, now time.Time) ([]ServiceStatus, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}

	var jobs []NomadJobItem
	if err := json.Unmarshal(jobsData, &jobs); err != nil {
		return nil, fmt.Errorf("failed to parse nomad jobs: %w", err)
	}

	var allocs []NomadAllocItem
	if err := json.Unmarshal(allocsData, &allocs); err != nil {
		return nil, fmt.Errorf("failed to parse nomad allocations: %w", err)
	}

	runningAllocs := indexRunningNomadAllocs(allocs)

	services := make([]ServiceStatus, 0, len(jobs))
	for _, job := range jobs {
		if job.Type != "" && job.Type != "service" {
			continue
		}

		jobID := job.ID
		if jobID == "" {
			jobID = job.Name
		}
		if jobID == "" {
			continue
		}

		status, uptimeSec := resolveNomadJobServiceStatus(job, runningAllocs, now)

		services = append(services, ServiceStatus{
			Name:          jobID,
			Status:        status,
			UptimeSeconds: uptimeSec,
			LastCheckTime: now,
		})
	}

	sort.Slice(services, func(i, j int) bool {
		return services[i].Name < services[j].Name
	})

	return services, nil
}
