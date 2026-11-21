package main

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
)

type DockerContainerHealth = struct {
	Status string
}

// DockerContainerJSON is the container view of a Kubernetes service that the rollout
// and container-group logic reads; synthesizeContainersFromServices builds it.
type DockerContainerJSON struct {
	Created int64
	State   string
	Labels  map[string]string
	Health  *DockerContainerHealth
}

type pipelineEntry struct {
	dep *DeploymentStatus
	// canonical is the normalized SHA of the run currently applied to the card.
	canonical string
	// key is the pipeline identity (pipelineKey): "pr:<repo>:<branch>" or a SHA.
	key string
	// latestRunAt is the CreatedAt of the newest run applied to the card.
	latestRunAt time.Time
	// rolloutDecided marks cards whose stage came from Kubernetes image tags, so
	// commit-grouped container heuristics must not override it.
	rolloutDecided bool
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
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') && (ch < 'A' || ch > 'F') {
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

// findByKey returns the entry owning a pipeline key. PR keys match exactly;
// SHA keys match SHA-keyed entries only, with the same prefix rules as find.
func (r *pipelineRegistry) findByKey(key string) *pipelineEntry {
	if isPRPipelineKey(key) {
		for _, e := range r.entries {
			if e.key == key {
				return e
			}
		}
		return nil
	}
	if key == "" {
		return nil
	}
	for _, e := range r.entries {
		if isPRPipelineKey(e.key) {
			continue
		}
		if e.canonical == key || (isHexSHA(e.canonical) && isHexSHA(key) && shasMatchPrefix(e.canonical, key)) {
			return e
		}
	}
	return nil
}

func (r *pipelineRegistry) getOrCreateByKey(key, sha, fallbackID string) *pipelineEntry {
	if e := r.findByKey(key); e != nil {
		return e
	}
	return r.create(key, normalizeSHA(sha), fallbackID)
}

func (r *pipelineRegistry) create(key, norm, fallbackID string) *pipelineEntry {
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
		key:       key,
	}
	r.entries = append(r.entries, entry)
	return entry
}

func attachContainerChips(dep *DeploymentStatus, groupContainers []DockerContainerJSON, refTime time.Time) {
	for _, chip := range dep.MatrixJobs {
		if chip.Type == chipTypeContainer {
			return
		}
	}
	dep.MatrixJobs = append(dep.MatrixJobs, BuildContainerChips(groupContainers, refTime)...)
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

const (
	labelCommitSHA         = "aerial.commit_sha"
	labelImage             = "aerial.image"
	buildPushGHCRJobPrefix = "Build & Push Images to GHCR ("
)

// builtImages maps successful docker-publish.yml matrix jobs
// ("Build & Push Images to GHCR (<service>)") to their image names ("aerial-<service>").
func builtImages(jobs []GitHubJob) []string {
	var out []string
	for _, j := range jobs {
		if j.Conclusion != conclusionSuccess {
			continue
		}
		rest, ok := strings.CutPrefix(j.Name, buildPushGHCRJobPrefix)
		if !ok {
			continue
		}
		inner, _, _ := strings.Cut(rest, ")")
		svc, _, _ := strings.Cut(inner, ",")
		svc = strings.TrimSpace(svc)
		if svc == "" {
			continue
		}
		img := "aerial-" + svc
		if !slices.Contains(out, img) {
			out = append(out, img)
		}
	}
	sort.Strings(out)
	return out
}

// hasImageTaggedContainers reports whether containers were synthesized from
// Kubernetes Deployments with image metadata.
func hasImageTaggedContainers(containers []DockerContainerJSON) bool {
	for _, c := range containers {
		if c.Labels[labelImage] != "" {
			return true
		}
	}
	return false
}

func isAerialRepo(repo string) bool {
	return repo == "" || strings.HasSuffix(repo, "/aerial")
}

// newerMainRunSHAs returns head SHAs of push-to-main runs in the same repository
// created after run.
func newerMainRunSHAs(run GitHubRun, runs []GitHubRun) []string {
	var out []string
	for _, r := range runs {
		if isPRRun(r) || r.Repository != run.Repository || !r.CreatedAt.After(run.CreatedAt) {
			continue
		}
		if sha := normalizeSHA(r.HeadSHA); sha != "" {
			out = append(out, sha)
		}
	}
	return out
}

// rolloutContainers returns containers running one of the built images whose
// commit is known from the image tag.
func rolloutContainers(built []string, containers []DockerContainerJSON) []DockerContainerJSON {
	var out []DockerContainerJSON
	for _, c := range containers {
		if c.Labels[labelCommitSHA] == "" || !slices.Contains(built, c.Labels[labelImage]) {
			continue
		}
		out = append(out, c)
	}
	return out
}

// evaluateImageRollout derives a card stage and progress from Kubernetes Deployment
// image tags: live once every container running a built image is on runSHA (or a
// newer main SHA) and healthy; swapping while the rollout is underway; degraded when
// any is unhealthy; awaiting_pull while none has started moving.
func evaluateImageRollout(runSHA string, newerSHAs []string, matching []DockerContainerJSON) (string, int) {
	if len(matching) == 0 {
		return stageLive, 100
	}
	runSHA = normalizeSHA(runSHA)
	var starting, degraded bool
	rolled, rolledHealthy := 0, 0
	for _, c := range matching {
		healthy := false
		switch {
		case c.Health != nil && c.Health.Status == healthStarting:
			starting = true
		case c.State != containerStateRunning || (c.Health != nil && c.Health.Status == healthUnhealthy):
			degraded = true
		default:
			healthy = true
		}
		commit := normalizeSHA(c.Labels[labelCommitSHA])
		onTarget := shasMatchPrefix(commit, runSHA)
		for _, n := range newerSHAs {
			onTarget = onTarget || shasMatchPrefix(commit, n)
		}
		if onTarget {
			rolled++
			if healthy {
				rolledHealthy++
			}
		}
	}
	swapProgress := 60 + int(float64(rolledHealthy)/float64(len(matching))*25)
	switch {
	case degraded:
		return statusDegraded, 85
	case starting:
		return stageSwapping, swapProgress
	case rolled == len(matching):
		return stageLive, 100
	case rolled > 0:
		return stageSwapping, swapProgress
	default:
		return stageAwaitingPull, 55
	}
}

// isImageRolloutDecided reports whether a run's card stage is decided by
// Kubernetes image tags: a successful aerial main run whose jobs are known.
func isImageRolloutDecided(run GitHubRun, runJobs []GitHubJob, containers []DockerContainerJSON) bool {
	return run.Status == statusCompleted && run.Conclusion == conclusionSuccess && !isPRRun(run) &&
		isAerialRepo(run.Repository) && len(runJobs) > 0 && hasImageTaggedContainers(containers)
}

func isPRRun(run GitHubRun) bool {
	return (run.HeadBranch != "" && run.HeadBranch != "main" && run.HeadBranch != "master") || run.Event == "pull_request"
}

const prPipelineKeyPrefix = "pr:"

func isPRPipelineKey(key string) bool {
	return strings.HasPrefix(key, prPipelineKeyPrefix)
}

// pipelineKey returns the card identity for a run: one card per pull request
// (branch runs and the push to main that merged them), else the head SHA.
func pipelineKey(run GitHubRun) string {
	switch {
	case isPRRun(run) && run.HeadBranch != "":
		return prPipelineKeyPrefix + run.Repository + ":" + run.HeadBranch
	case !isPRRun(run) && run.MergedFromBranch != "":
		return prPipelineKeyPrefix + run.Repository + ":" + run.MergedFromBranch
	default:
		return normalizeSHA(run.HeadSHA)
	}
}

// prPipelineCardID returns a stable card ID for a PR key so the UI key does not
// change as new runs supersede old ones.
func prPipelineCardID(repo, branch string) string {
	raw := strings.ToLower("pr-" + strings.ReplaceAll(repo, "/", "-") + "-" + branch)
	var b strings.Builder
	b.Grow(len(raw))
	for _, ch := range raw {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-' {
			b.WriteRune(ch)
		} else {
			b.WriteByte('-')
		}
	}
	return b.String()
}

// runCardBranch returns the PR branch backing a PR-keyed run.
func runCardBranch(run GitHubRun) string {
	if isPRRun(run) {
		return run.HeadBranch
	}
	return run.MergedFromBranch
}

// dropSupersededCancelledMainRuns removes cancelled push-to-main runs when a
// newer push-to-main run exists for the same repository (main runs queue, and
// newer pushes cancel the queued ones).
func dropSupersededCancelledMainRuns(runs []GitHubRun) []GitHubRun {
	newestMain := make(map[string]time.Time)
	for _, r := range runs {
		if isPRRun(r) {
			continue
		}
		if r.CreatedAt.After(newestMain[r.Repository]) {
			newestMain[r.Repository] = r.CreatedAt
		}
	}
	kept := make([]GitHubRun, 0, len(runs))
	for _, r := range runs {
		if !isPRRun(r) && r.Conclusion == "cancelled" && newestMain[r.Repository].After(r.CreatedAt) {
			continue
		}
		kept = append(kept, r)
	}
	return kept
}

func evaluateSuccessfulCIRun(
	run GitHubRun,
	runJobs []GitHubJob,
	runs []GitHubRun,
	aerialContainers []DockerContainerJSON,
) (stage, ciStatus string, progress int, skip bool) {
	if isPRRun(run) {
		return "", "", 0, true
	}
	hasContainerBuilds := hasActiveContainerBuildJob(runJobs)
	if len(runJobs) > 0 && !hasContainerBuilds {
		return stageLive, statusCompleted, 100, false
	}
	if isImageRolloutDecided(run, runJobs, aerialContainers) {
		matching := rolloutContainers(builtImages(runJobs), aerialContainers)
		stage, progress := evaluateImageRollout(run.HeadSHA, newerMainRunSHAs(run, runs), matching)
		return stage, statusCompleted, progress, false
	}
	// The card waits in awaiting_pull until the Kubernetes rollout is observed.
	return stageAwaitingPull, statusCompleted, 55, false
}

func evaluateCompletedCIRun(
	run GitHubRun,
	runJobs []GitHubJob,
	runs []GitHubRun,
	aerialContainers []DockerContainerJSON,
	refTime time.Time,
) (stage, ciStatus string, progress int, skip bool) {
	if refTime.Sub(run.UpdatedAt) >= 30*time.Minute && refTime.Sub(run.CreatedAt) >= 30*time.Minute {
		return "", statusPending, 0, false
	}
	switch run.Conclusion {
	case conclusionFailure, "cancelled":
		return statusFailed, statusFailed, 40, false
	case conclusionSuccess:
		return evaluateSuccessfulCIRun(run, runJobs, runs, aerialContainers)
	default:
		return "", statusPending, 0, false
	}
}

func evaluateCIRunStage(
	run GitHubRun,
	runJobs []GitHubJob,
	ciChips []MatrixJobChip,
	runs []GitHubRun,
	aerialContainers []DockerContainerJSON,
	refTime time.Time,
) (stage, ciStatus string, progress int) {
	switch run.Status {
	case statusQueued:
		return statusQueued, statusPending, 15
	case statusInProgress:
		progress := 25
		if len(ciChips) > 0 {
			doneCount := 0
			for _, c := range ciChips {
				if c.Status == statusCompleted {
					doneCount++
				}
			}
			progress = 20 + int(float64(doneCount)/float64(len(ciChips))*30)
		}
		return stageBuilding, statusActive, progress
	case statusCompleted:
		stage, ciStatus, progress, skip := evaluateCompletedCIRun(
			run, runJobs, runs, aerialContainers, refTime,
		)
		if skip {
			return "", "", 0
		}
		return stage, ciStatus, progress
	default:
		return "", statusPending, 0
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
	cardID string,
) {
	dep.ID = cardID
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

	syncStatus := statusPending
	switch stage {
	case stageAwaitingPull:
		syncStatus = statusActive
	case stageLive, stageSwapping, statusDegraded:
		syncStatus = statusCompleted
	}

	step4Status := statusPending
	step5Status := statusPending
	switch stage {
	case stageLive:
		step4Status = statusCompleted
		step5Status = statusCompleted
	case stageSwapping:
		step4Status = statusActive
		step5Status = statusActive
	case statusDegraded:
		step4Status = statusCompleted
		step5Status = statusFailed
	}

	dep.Stage = stage
	dep.Progress = progress
	dep.Steps = buildDeploymentSteps("completed", ciStatus, syncStatus, step4Status, step5Status)
	dep.MatrixJobs = ciChips
}

// resetDeploymentForRun clears run-derived card state before a newer run for
// the same pipeline key takes the card over.
func resetDeploymentForRun(entry *pipelineEntry, normSHA string, runCreatedAt time.Time) {
	dep := entry.dep
	dep.Stage = ""
	dep.Progress = 0
	dep.Steps = nil
	dep.MatrixJobs = nil
	dep.CommitMsg = ""
	dep.CommitTime = nil
	dep.HTMLURL = ""
	dep.StartedAt = time.Time{}
	dep.Commit = shortenSHA(normSHA)
	entry.canonical = normSHA
	entry.latestRunAt = runCreatedAt
	entry.rolloutDecided = false
}

func ingestGitHubRun(
	reg *pipelineRegistry,
	run GitHubRun,
	runs []GitHubRun,
	jobs map[int64][]GitHubJob,
	aerialContainers []DockerContainerJSON,
	refTime time.Time,
) {
	normSHA := normalizeSHA(run.HeadSHA)
	shortSHA := shortenSHA(normSHA)
	if shortSHA == "" {
		shortSHA = fmt.Sprintf("gh-run-%d", run.ID)
	}
	key := pipelineKey(run)
	prKeyed := isPRPipelineKey(key)
	cardID := fmt.Sprintf("gh-run-%d", run.ID)
	if prKeyed {
		cardID = prPipelineCardID(run.Repository, runCardBranch(run))
	}

	existing := reg.findByKey(key)
	if existing != nil && run.CreatedAt.Before(existing.latestRunAt) {
		// An older run for this pipeline was superseded by a newer one.
		return
	}

	commitMsg, commitTime := extractRunCommitMetadata(run)

	var runJobs []GitHubJob
	if jobs != nil {
		runJobs = jobs[run.ID]
	}
	ciChips := ParseMatrixJobChips(runJobs, refTime)

	stage, ciStatus, progress := evaluateCIRunStage(
		run, runJobs, ciChips, runs, aerialContainers, refTime,
	)
	if stage == "" && !prKeyed {
		return
	}

	entry := existing
	if entry == nil {
		entry = reg.getOrCreateByKey(key, normSHA, cardID)
	}
	dep := entry.dep
	applyRepositoryService(dep, run.Repository)

	isNewer := entry.latestRunAt.IsZero() || run.CreatedAt.After(entry.latestRunAt)
	if isNewer {
		resetDeploymentForRun(entry, normSHA, run.CreatedAt)
		dep.ID = cardID
	}
	if stage == "" {
		// Newest PR run needs no card (successful branch run, stale, pending):
		// keep the entry hidden so older runs cannot resurrect it.
		return
	}
	if isNewer || shouldUpdateDeploymentFromRun(dep, stage, run.CreatedAt) {
		applyCIRunToDeployment(dep, run, shortSHA, commitMsg, commitTime, stage, ciStatus, progress, ciChips, cardID)
		entry.canonical = normSHA
		entry.rolloutDecided = isImageRolloutDecided(run, runJobs, aerialContainers)
		if entry.rolloutDecided && stage != stageLive {
			matching := rolloutContainers(builtImages(runJobs), aerialContainers)
			dep.MatrixJobs = append(dep.MatrixJobs, BuildContainerChips(matching, refTime)...)
		}
	}
}

func resolveContainerCommit(c DockerContainerJSON, currentCommit string) string {
	commit := c.Labels[labelCommitSHA]
	if c.Labels[labelImage] != "" {
		// Kubernetes image tags are authoritative: no tag commit means unknown, not current.
		return commit
	}
	if commit == "" && containerServiceName(c) != "" {
		commit = currentCommit
	}
	return commit
}

func groupContainersByCommit(aerialContainers []DockerContainerJSON, currentCommit string) map[string][]DockerContainerJSON {
	groupsByCommit := make(map[string][]DockerContainerJSON)
	for _, c := range aerialContainers {
		commit := resolveContainerCommit(c, currentCommit)
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
		uptimeSec := max(int64(refTime.Sub(createdAt).Seconds()), 0)
		if uptimeSec < minUptimeSec {
			minUptimeSec = uptimeSec
		}
		switch {
		case c.Health != nil && c.Health.Status == healthStarting:
			hasStarting = true
		case c.State != containerStateRunning || (c.Health != nil && c.Health.Status == healthUnhealthy):
			hasDegraded = true
		default:
			healthyCount++
		}
	}
	return latestCreatedAt, minUptimeSec, hasStarting, hasDegraded, healthyCount
}

func applyGroupDegradedState(dep *DeploymentStatus) {
	dep.Stage = statusDegraded
	dep.Progress = 85
	if len(dep.Steps) < 5 {
		dep.Steps = buildDeploymentSteps("completed", "completed", "completed", "completed", "failed")
	} else {
		dep.Steps[3].Status = statusCompleted
		dep.Steps[4].Status = statusFailed
	}
}

func applyGroupSwappingState(dep *DeploymentStatus, healthyCount, totalCount int) {
	if stageRank[dep.Stage] < stageRank[stageSwapping] {
		dep.Stage = stageSwapping
		progress := 60
		if totalCount > 0 {
			progress = 60 + int(float64(healthyCount)/float64(totalCount)*25)
		}
		dep.Progress = progress
	}
	swapStatus := statusActive
	if len(dep.Steps) < 5 {
		dep.Steps = buildDeploymentSteps("completed", "completed", "completed", swapStatus, "active")
	} else {
		dep.Steps[1].Status = statusCompleted
		dep.Steps[2].Status = statusCompleted
		dep.Steps[3].Status = swapStatus
		dep.Steps[4].Status = statusActive
	}
}

func applyGroupLiveGraceState(dep *DeploymentStatus, groupContainers []DockerContainerJSON, refTime time.Time) {
	if dep.Stage == "" || dep.Stage == stageAwaitingPull || stageRank[dep.Stage] <= stageRank[stageLive] {
		dep.Stage = stageLive
		dep.Progress = 100
		if len(dep.Steps) < 5 {
			dep.Steps = buildDeploymentSteps("completed", "completed", "completed", "completed", "completed")
		} else {
			for i := range dep.Steps {
				dep.Steps[i].Status = statusCompleted
			}
		}
		attachContainerChips(dep, groupContainers, refTime)
	}
}

func applyContainerGroupState(
	dep *DeploymentStatus,
	groupContainers []DockerContainerJSON,
	refTime time.Time,
) {
	latestCreatedAt, minUptimeSec, hasStarting, hasDegraded, healthyCount := summarizeContainerGroup(groupContainers, refTime)
	imageTagged := hasImageTaggedContainers(groupContainers)
	if dep.StartedAt.IsZero() {
		dep.StartedAt = latestCreatedAt
	}

	switch {
	case hasDegraded:
		applyGroupDegradedState(dep)
		attachContainerChips(dep, groupContainers, refTime)
	case hasStarting || (!imageTagged && minUptimeSec < 120):
		applyGroupSwappingState(dep, healthyCount, len(groupContainers))
		attachContainerChips(dep, groupContainers, refTime)
	case imageTagged || minUptimeSec < 600:
		// Kubernetes readiness is authoritative: a fully healthy image-tagged group
		// is live regardless of uptime.
		applyGroupLiveGraceState(dep, groupContainers, refTime)
	}
}

func compareDeployments(a, b DeploymentStatus) bool {
	rankA := stageRank[a.Stage]
	rankB := stageRank[b.Stage]
	if rankA != rankB {
		return rankA > rankB
	}
	if a.Stage != b.Stage {
		if a.Stage == statusFailed && b.Stage == statusDegraded {
			return true
		}
		if a.Stage == statusDegraded && b.Stage == statusFailed {
			return false
		}
	}
	if !a.StartedAt.Equal(b.StartedAt) {
		return a.StartedAt.After(b.StartedAt)
	}
	return a.Commit < b.Commit
}

// MergeClusterDeployments aggregates concurrent GitHub Actions CI runs and
// Kubernetes service containers grouped by commit into independent pipelines.
func MergeClusterDeployments(
	runs []GitHubRun,
	jobs map[int64][]GitHubJob,
	aerialContainers []DockerContainerJSON,
	currentCommit string,
	refTime time.Time,
) []DeploymentStatus {
	if refTime.IsZero() {
		refTime = time.Now().UTC()
	}

	reg := &pipelineRegistry{}

	// 1. Ingest GitHub Actions CI runs
	for _, run := range dropSupersededCancelledMainRuns(runs) {
		ingestGitHubRun(reg, run, runs, jobs, aerialContainers, refTime)
	}

	// 2. Ingest service containers grouped by commit
	groupsByCommit := groupContainersByCommit(aerialContainers, currentCommit)
	for groupSHA, groupContainers := range groupsByCommit {
		entry := reg.find(groupSHA)
		if entry == nil || entry.dep.Stage == "" || entry.rolloutDecided {
			// Strictly top-down invariant: Unparented containers NEVER synthesize deployment cards,
			// hidden PR cards are never resurrected, and image-tag rollout verdicts are authoritative.
			continue
		}
		applyContainerGroupState(entry.dep, groupContainers, refTime)
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
