package main

import (
	"encoding/json"
	"fmt"
	"mime"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

var sensitiveKeys = []string{
	"GEMINI_HARNESS_API_KEY",
	"AERIAL_GEMINI_API_KEY",
	"GEMINI_API_KEY",
	"DISCORD_TOKEN",
	"DISCORD_BOT_TOKEN",
	"GITHUB_PAT",
	"GITHUB_PERSONAL_ACCESS_TOKEN",
	"HA_TOKEN",
	"SECRET",
	"PASSWORD",
	"TOKEN",
	"KEY",
}

var mimeFallbacks = map[string]string{
	".html":  "text/html; charset=utf-8",
	".css":   "text/css; charset=utf-8",
	".js":    "application/javascript; charset=utf-8",
	".json":  "application/json; charset=utf-8",
	".svg":   "image/svg+xml",
	".ico":   "image/x-icon",
	".png":   "image/png",
	".woff2": "font/woff2",
	".woff":  "font/woff",
}

// parseUpstreamURL normalizes baseURL, validates control characters and empty conditions,
// and parses the combined URL with pathSuffix.
func parseUpstreamURL(baseURL, emptyErr, pathSuffix string) (*url.URL, error) {
	cleanBase := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if cleanBase == "" {
		return nil, fmt.Errorf("%s", emptyErr)
	}
	if strings.ContainsAny(cleanBase, "\x7f\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\r\n") {
		return nil, fmt.Errorf("invalid control character in base URL")
	}
	return url.Parse(cleanBase + pathSuffix)
}

// shortenSHA returns the first 7 characters of a commit SHA if longer.
func shortenSHA(s string) string {
	if len(s) > 7 {
		return s[:7]
	}
	return s
}

// containerChipStatusAndDuration computes the chip status, conclusion, and uptime duration for a container.
func containerChipStatusAndDuration(c DockerContainerJSON, now time.Time) (status, conclusion, duration string) {
	status = "completed"
	conclusion = "success"
	if c.Health != nil && c.Health.Status == "starting" {
		status = "active"
		conclusion = ""
	} else if c.State != "running" || (c.Health != nil && c.Health.Status == "unhealthy") {
		status = "failed"
		conclusion = "failure"
	}

	duration = "0s"
	if c.Created > 0 {
		createdAt := time.Unix(c.Created, 0).UTC()
		durSec := int(now.Sub(createdAt).Seconds())
		if durSec > 0 {
			duration = FormatUptimeString(durSec)
		}
	}
	return status, conclusion, duration
}

// buildDeploymentSteps constructs the standard 5-step Permet HUD deployment pipeline.
func buildDeploymentSteps(s1, s2, s3, s4, s5 string) []DeploymentStep {
	return []DeploymentStep{
		{Name: "Commit Trigger", Icon: "📦", Status: s1},
		{Name: "CI Build & GHCR", Icon: "⚙️", Status: s2},
		{Name: "Hangar Sync", Icon: "⬇️", Status: s3},
		{Name: "Container Swap", Icon: "🔄", Status: s4},
		{Name: "Health Check", Icon: "🩺", Status: s5},
	}
}

// BuildFactsUpstreamURL validates the base URL and constructs the target upstream
// URL with whitelisted, normalized, and clamped query parameters.
func BuildFactsUpstreamURL(brainBaseURL string, inQuery url.Values) (targetURL string, limit, offset int, err error) {
	target, err := parseUpstreamURL(brainBaseURL, fmt.Sprintf("invalid upstream URL configuration: %q", brainBaseURL), "/facts")
	if err != nil {
		return "", 0, 0, err
	}

	outQuery := target.Query()
	if inQuery != nil {
		if limitStr := inQuery.Get("limit"); limitStr != "" {
			if n, err := strconv.Atoi(limitStr); err == nil && n > 0 {
				limit = n
				outQuery.Set("limit", strconv.Itoa(limit))
			}
		}

		if offsetStr := inQuery.Get("offset"); offsetStr != "" {
			if n, err := strconv.Atoi(offsetStr); err == nil && n >= 0 {
				offset = n
				if offset > 0 {
					outQuery.Set("offset", strconv.Itoa(offset))
				}
			}
		}

		if search := strings.TrimSpace(inQuery.Get("q")); search != "" {
			runes := []rune(search)
			if len(runes) > 64 {
				search = strings.TrimSpace(string(runes[:64]))
			}
			outQuery.Set("q", search)
		}
	}

	target.RawQuery = outQuery.Encode()
	return target.String(), limit, offset, nil
}

// BuildSchedulesUpstreamURL validates the base URL and constructs the upstream /schedules URL.
func BuildSchedulesUpstreamURL(haroBaseURL string) (string, error) {
	target, err := parseUpstreamURL(haroBaseURL, fmt.Sprintf("invalid upstream URL configuration: %q", haroBaseURL), "/schedules")
	if err != nil {
		return "", err
	}
	return target.String(), nil
}

// BuildScheduleRunsUpstreamURL validates the base URL and constructs the upstream
// /schedules/runs URL with sanitized limit, offset, schedule_id, and status parameters.
func BuildScheduleRunsUpstreamURL(haroBaseURL string, inQuery url.Values) (targetURL string, limit, offset int, err error) {
	target, err := parseUpstreamURL(haroBaseURL, fmt.Sprintf("invalid upstream URL configuration: %q", haroBaseURL), "/schedules/runs")
	if err != nil {
		return "", 0, 0, err
	}

	outQuery := target.Query()
	limit = 50
	offset = 0

	if inQuery != nil {
		if lStr := inQuery.Get("limit"); lStr != "" {
			if n, err := strconv.Atoi(lStr); err == nil && n > 0 && n <= 100 {
				limit = n
			}
		}

		if oStr := inQuery.Get("offset"); oStr != "" {
			if n, err := strconv.Atoi(oStr); err == nil && n >= 0 {
				offset = n
			}
		}

		if schedID := strings.TrimSpace(inQuery.Get("schedule_id")); schedID != "" {
			outQuery.Set("schedule_id", schedID)
		}
		if status := strings.TrimSpace(inQuery.Get("status")); status != "" {
			outQuery.Set("status", status)
		}
	}

	outQuery.Set("limit", strconv.Itoa(limit))
	outQuery.Set("offset", strconv.Itoa(offset))
	target.RawQuery = outQuery.Encode()

	return target.String(), limit, offset, nil
}

// BuildVanadisBuildsUpstreamURL validates the base URL and constructs the upstream /builds URL
// with clamped limit (1..100, default 50), and optional status and queue_state query parameters.
func BuildVanadisBuildsUpstreamURL(vanadisBaseURL string, inQuery url.Values) (targetURL string, limit int, err error) {
	target, err := parseUpstreamURL(vanadisBaseURL, fmt.Sprintf("invalid upstream URL configuration: %q", vanadisBaseURL), "/builds")
	if err != nil {
		return "", 0, err
	}

	outQuery := target.Query()
	limit = 50

	if inQuery != nil {
		if lStr := strings.TrimSpace(inQuery.Get("limit")); lStr != "" {
			if n, err := strconv.Atoi(lStr); err == nil {
				if n < 1 {
					limit = 1
				} else if n > 100 {
					limit = 100
				} else {
					limit = n
				}
			}
		}

		if status := strings.TrimSpace(inQuery.Get("status")); status != "" {
			outQuery.Set("status", status)
		}
		if queueState := strings.TrimSpace(inQuery.Get("queue_state")); queueState != "" {
			outQuery.Set("queue_state", queueState)
		}
	}

	outQuery.Set("limit", strconv.Itoa(limit))
	target.RawQuery = outQuery.Encode()

	return target.String(), limit, nil
}

// BuildVanadisBuildDetailUpstreamURL validates the base URL, extracts and validates the build ID
// from requestPath, and constructs the upstream /builds/<id> URL.
func BuildVanadisBuildDetailUpstreamURL(vanadisBaseURL string, requestPath string) (targetURL string, buildID string, err error) {
	baseParsed, err := parseUpstreamURL(vanadisBaseURL, "vanadis base URL is empty", "")
	if err != nil {
		return "", "", err
	}

	var rawID string
	if strings.HasPrefix(requestPath, "/dashboard/api/builds/") {
		rawID = strings.TrimPrefix(requestPath, "/dashboard/api/builds/")
	} else if strings.HasPrefix(requestPath, "/api/builds/") {
		rawID = strings.TrimPrefix(requestPath, "/api/builds/")
	} else {
		return "", "", fmt.Errorf("invalid request path %q: missing expected prefix", requestPath)
	}

	buildID = strings.TrimSpace(rawID)
	if buildID == "" {
		return "", "", fmt.Errorf("missing or empty build ID")
	}
	if strings.Contains(buildID, "/") || strings.Contains(buildID, "..") {
		return "", "", fmt.Errorf("invalid build ID %q: path traversal not allowed", buildID)
	}
	if strings.ContainsAny(buildID, "\x7f\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\r\n") {
		return "", "", fmt.Errorf("invalid build ID %q: control characters not allowed", buildID)
	}

	target, err := url.Parse(baseParsed.String() + "/builds/" + url.PathEscape(buildID))
	if err != nil {
		return "", "", err
	}

	return target.String(), buildID, nil
}

// CalculateClusterStatus computes cluster health from a slice of ServiceStatus items.
// If any service is unhealthy, the cluster is considered degraded.
func CalculateClusterStatus(services []ServiceStatus) string {
	for _, s := range services {
		if s.Status == "unhealthy" {
			return "degraded"
		}
	}
	return "healthy"
}

// FormatUptimeString converts seconds into a human-readable duration format (%ds, %dm, %dh).
func FormatUptimeString(sec int) string {
	if sec <= 0 {
		return "0s"
	}
	if sec < 60 {
		return fmt.Sprintf("%ds", sec)
	}
	if sec < 3600 {
		return fmt.Sprintf("%dm", sec/60)
	}
	return fmt.Sprintf("%dh", sec/3600)
}

// ExtractAerialServiceName extracts the canonical Aerial service name and returns whether
// the container belongs to the Aerial stack (either Docker Compose or Nomad managed).
func ExtractAerialServiceName(c DockerContainerJSON) (string, bool) {
	if c.Labels != nil {
		// 1. Docker Compose labels: project must be aerial if specified
		if proj, ok := c.Labels["com.docker.compose.project"]; ok {
			if proj == "aerial" {
				if svc, ok := c.Labels["com.docker.compose.service"]; ok && svc != "" {
					return svc, true
				}
			}
			// Foreign Compose project (e.g. homeassistant) -> reject
			return "", false
		}

		// 2. Explicit aerial.service label (present on all first-party Aerial images)
		if svc, ok := c.Labels["aerial.service"]; ok && svc != "" {
			return svc, true
		}

		// 3. Nomad allocation container (task_name-<alloc_id>)
		if allocID, ok := c.Labels["com.hashicorp.nomad.alloc_id"]; ok && allocID != "" {
			if len(c.Names) > 0 {
				name := strings.TrimPrefix(c.Names[0], "/")
				// Nomad names containers <task>-<alloc_id>
				if strings.HasSuffix(name, "-"+allocID) {
					task := strings.TrimSuffix(name, "-"+allocID)
					if task != "" {
						return task, true
					}
				}
			}
		}

		// 4. Fallback for test fixtures where com.docker.compose.service was set without project
		if svc, ok := c.Labels["com.docker.compose.service"]; ok && svc != "" {
			return svc, true
		}
	}

	// 5. Legacy/fallback container name prefix
	if len(c.Names) > 0 {
		name := strings.TrimPrefix(c.Names[0], "/")
		if strings.HasPrefix(name, "aerial-") {
			svc := strings.TrimPrefix(name, "aerial-")
			if svc != "" {
				return svc, true
			}
		}
	}

	return "", false
}

// IsCoreAerialContainer determines whether a container is part of the core Aerial stack.
func IsCoreAerialContainer(c DockerContainerJSON) bool {
	svcName, _ := ExtractAerialServiceName(c)
	svcName = strings.ToLower(svcName)

	switch svcName {
	case "agentsview", "watchtower", "autoheal", "ollama":
		return false
	}

	img := strings.ToLower(c.Image)
	if strings.Contains(img, "watchtower") ||
		strings.Contains(img, "autoheal") ||
		strings.Contains(img, "ollama") ||
		strings.Contains(img, "agentsview") {
		return false
	}

	if c.Labels != nil {
		if src, ok := c.Labels["org.opencontainers.image.source"]; ok && src != "" {
			if !strings.Contains(strings.ToLower(src), "azylman/aerial") {
				return false
			}
		}
	}

	return true
}

// ExtractServiceNameFromJobName resolves a microservice name from a GitHub Actions job name.
func ExtractServiceNameFromJobName(jobName string) string {
	lower := strings.ToLower(jobName)
	services := []string{
		"brain",
		"dashboard",
		"proxy",
		"haro",
		"scheduler-mcp",
		"coven",
		"memory-mcp",
		"discord-mcp",
		"docker-mcp",
		"github-mcp",
		"nomad-mcp",
		"infisical-mcp",
		"webhooks-router",
		"ollama",
		"agentsview",
		"hangar",
		"docs",
	}
	for _, s := range services {
		if strings.Contains(lower, s) {
			return s
		}
	}
	if strings.Contains(lower, "unit test") || strings.Contains(lower, "test") {
		return "unit-tests"
	}
	if strings.Contains(lower, "lint") {
		return "lint"
	}
	if strings.Contains(lower, "config") {
		return "config"
	}
	if start := strings.Index(lower, "("); start != -1 {
		if end := strings.Index(lower[start+1:], ")"); end != -1 {
			target := strings.TrimSpace(lower[start+1 : start+1+end])
			if target != "" && !strings.Contains(target, "linux/") && !strings.Contains(target, "darwin/") && !strings.Contains(target, "windows/") {
				return target
			}
		}
	}
	return ""
}

// ParseMatrixJobChips converts GitHub Actions job runs into Permet HUD chip models.
// A reference time `now` is injected for deterministic duration calculations on in-progress jobs.
func ParseMatrixJobChips(jobs []GitHubJob, now time.Time) []MatrixJobChip {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	chips := make([]MatrixJobChip, 0)
	seen := make(map[string]bool)

	for _, j := range jobs {
		svc := ExtractServiceNameFromJobName(j.Name)
		if svc == "" || seen[svc] {
			continue
		}
		seen[svc] = true

		chipStatus := "pending"
		if j.Status == "in_progress" {
			chipStatus = "active"
		} else if j.Status == "completed" {
			if j.Conclusion == "success" {
				chipStatus = "completed"
			} else if j.Conclusion == "failure" {
				chipStatus = "failed"
			} else {
				chipStatus = "pending"
			}
		}

		var durStr string
		if !j.StartedAt.IsZero() {
			end := j.CompletedAt
			if end.IsZero() {
				end = now
			}
			sec := int(end.Sub(j.StartedAt).Seconds())
			if sec > 0 {
				durStr = fmt.Sprintf("%ds", sec)
			}
		}

		chips = append(chips, MatrixJobChip{
			Name:       svc,
			Status:     chipStatus,
			Conclusion: j.Conclusion,
			Duration:   durStr,
			Type:       "ci",
		})
	}
	return chips
}

// BuildContainerChips constructs Permet HUD service chips from Docker container metadata.
// A reference time `now` is injected for deterministic uptime calculations.
func BuildContainerChips(rawContainers []DockerContainerJSON, now time.Time) []MatrixJobChip {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	chips := make([]MatrixJobChip, 0)
	seen := make(map[string]bool)

	for _, c := range rawContainers {
		svcName, isAerial := ExtractAerialServiceName(c)
		if !isAerial || svcName == "" || seen[svcName] {
			continue
		}
		seen[svcName] = true

		status, conclusion, durStr := containerChipStatusAndDuration(c, now)

		chips = append(chips, MatrixJobChip{
			Name:       svcName,
			Status:     status,
			Conclusion: conclusion,
			Duration:   durStr,
			Type:       "container",
		})
	}
	return chips
}

// BuildTargetContainerChips constructs Permet HUD service chips scoped to targeted services.
// If targets is empty, it falls back to BuildContainerChips.
// It iterates over targets as canonical keys to ensure missing or pending containers are visibly represented.
func BuildTargetContainerChips(rawContainers []DockerContainerJSON, targets []string, startedAt time.Time, now time.Time) []MatrixJobChip {
	if len(targets) == 0 {
		return BuildContainerChips(rawContainers, now)
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	containerByService := make(map[string]DockerContainerJSON)
	for _, c := range rawContainers {
		svcName, isAerial := ExtractAerialServiceName(c)
		if isAerial && svcName != "" {
			if existing, ok := containerByService[svcName]; !ok || c.Created > existing.Created {
				containerByService[svcName] = c
			}
		}
	}

	chips := make([]MatrixJobChip, 0, len(targets))
	seen := make(map[string]bool)

	for _, target := range targets {
		svc := strings.TrimSpace(target)
		if svc == "" || seen[svc] {
			continue
		}
		seen[svc] = true

		c, found := containerByService[svc]
		if !found {
			chips = append(chips, MatrixJobChip{
				Name:       svc,
				Status:     "active",
				Conclusion: "",
				Duration:   "pending",
				Type:       "container",
			})
			continue
		}

		createdAt := time.Unix(c.Created, 0).UTC()
		isRecentlyRecreated := true
		if !startedAt.IsZero() && createdAt.Before(startedAt.Add(-30*time.Second)) {
			isRecentlyRecreated = false
		}

		if !isRecentlyRecreated {
			chips = append(chips, MatrixJobChip{
				Name:       svc,
				Status:     "active",
				Conclusion: "",
				Duration:   "pending",
				Type:       "container",
			})
			continue
		}

		status, conclusion, durStr := containerChipStatusAndDuration(c, now)

		chips = append(chips, MatrixJobChip{
			Name:       svc,
			Status:     status,
			Conclusion: conclusion,
			Duration:   durStr,
			Type:       "container",
		})
	}

	return chips
}

// GetContainerCommit extracts and truncates the 7-character commit SHA from container labels.
func GetContainerCommit(containers []DockerContainerJSON) string {
	for _, c := range containers {
		if !IsCoreAerialContainer(c) {
			continue
		}
		if sha := ExtractSingleContainerCommit(c); sha != "" {
			return shortenSHA(sha)
		}
	}
	return ""
}

// MatchesETag implements RFC 7232 §2.3.2 / RFC 9110 §13.1.2 weak comparison.
func MatchesETag(ifNoneMatch, targetETag, targetHash string) bool {
	if ifNoneMatch == "" {
		return false
	}
	if strings.TrimSpace(ifNoneMatch) == "*" {
		return true
	}
	cleanTargetETag := strings.Trim(strings.TrimPrefix(targetETag, "W/"), "\"")
	parts := strings.Split(ifNoneMatch, ",")
	for _, part := range parts {
		token := strings.TrimSpace(part)
		token = strings.TrimPrefix(token, "W/")
		token = strings.Trim(token, "\"")
		if token == cleanTargetETag || token == targetHash {
			return true
		}
	}
	return false
}

// GetMimeType resolves file extensions to MIME types with hardened fallbacks.
func GetMimeType(filename string) string {
	ext := strings.ToLower(filepath.Ext(filename))
	if ct := mime.TypeByExtension(ext); ct != "" {
		return ct
	}
	if ct, ok := mimeFallbacks[ext]; ok {
		return ct
	}
	return "application/octet-stream"
}

var stageRank = map[string]int{
	"swapping":      6,
	"pulling":       5,
	"building":      4,
	"awaiting_pull": 3,
	"queued":        2,
	"failed":        1,
	"degraded":      1,
	"live":          0,
}

// CalculateDeployStatus evaluates active deployment stages with deterministic precedence:
// swapping > pulling > building > awaiting_pull > queued > failed > degraded > idle.
// Returns "idle" if no deployments exist or all deployments are completed/live.
func CalculateDeployStatus(deployments []DeploymentStatus) string {
	bestRank := 0
	bestStage := "idle"

	for _, d := range deployments {
		if rank, ok := stageRank[d.Stage]; ok {
			if rank > bestRank || (rank == bestRank && d.Stage == "failed" && bestStage == "degraded") {
				bestRank = rank
				bestStage = d.Stage
			}
		}
	}

	return bestStage
}

// IsDeployOngoing returns true if any deployment is actively in-progress
// ("queued", "building", "awaiting_pull", "pulling", "swapping").
// Accepts string, DeploymentStatus, []DeploymentStatus, etc.
func IsDeployOngoing(target any) bool {
	switch v := target.(type) {
	case string:
		switch v {
		case "queued", "building", "awaiting_pull", "pulling", "swapping":
			return true
		}
		return false
	case DeploymentStatus:
		return IsDeployOngoing(v.Stage)
	case *DeploymentStatus:
		if v != nil {
			return IsDeployOngoing(v.Stage)
		}
		return false
	case []DeploymentStatus:
		for _, d := range v {
			if IsDeployOngoing(d.Stage) {
				return true
			}
		}
		return false
	case []*DeploymentStatus:
		for _, d := range v {
			if d != nil && IsDeployOngoing(d.Stage) {
				return true
			}
		}
		return false
	}
	return false
}

// ExtractSingleContainerCommit extracts the raw commit SHA from container labels.
func ExtractSingleContainerCommit(c DockerContainerJSON) string {
	if c.Labels == nil {
		return ""
	}
	if rev, ok := c.Labels["org.opencontainers.image.revision"]; ok && rev != "" {
		return rev
	}
	if rev, ok := c.Labels["aerial.commit_sha"]; ok && rev != "" {
		return rev
	}
	if rev, ok := c.Labels["vcs-ref"]; ok && rev != "" {
		return rev
	}
	return ""
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

	isHexSHA := func(s string) bool {
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

	normalizeSHA := func(s string) string {
		trimmed := strings.TrimSpace(s)
		if isHexSHA(trimmed) {
			return strings.ToLower(trimmed)
		}
		return strings.ToLower(trimmed)
	}

	type pipelineEntry struct {
		dep       *DeploymentStatus
		canonical string
	}
	var entries []*pipelineEntry

	findEntry := func(sha string) *pipelineEntry {
		if sha == "" {
			return nil
		}
		norm := normalizeSHA(sha)
		for _, e := range entries {
			if e.canonical == norm {
				return e
			}
			if len(e.canonical) >= 7 && len(norm) >= 7 && isHexSHA(e.canonical) && isHexSHA(norm) {
				if strings.HasPrefix(e.canonical, norm) || strings.HasPrefix(norm, e.canonical) {
					return e
				}
			}
		}
		return nil
	}

	getOrCreateEntry := func(sha string, fallbackID string) *pipelineEntry {
		if e := findEntry(sha); e != nil {
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
		entries = append(entries, entry)
		return entry
	}

	attachContainerChips := func(dep *DeploymentStatus, groupContainers []DockerContainerJSON) {
		hasContainerChips := false
		for _, chip := range dep.MatrixJobs {
			if chip.Type == "container" {
				hasContainerChips = true
				break
			}
		}
		if !hasContainerChips {
			if gitSync.Reconciliation != nil && len(gitSync.Reconciliation.TargetServices) > 0 &&
				(gitSync.Reconciliation.CommitSHA == "" || strings.HasPrefix(dep.Commit, shortenSHA(gitSync.Reconciliation.CommitSHA)) || strings.HasPrefix(shortenSHA(gitSync.Reconciliation.CommitSHA), dep.Commit)) {
				containerChips := BuildTargetContainerChips(groupContainers, gitSync.Reconciliation.TargetServices, gitSync.Reconciliation.StartedAt, refTime)
				dep.MatrixJobs = append(dep.MatrixJobs, containerChips...)
			} else {
				containerChips := BuildContainerChips(groupContainers, refTime)
				dep.MatrixJobs = append(dep.MatrixJobs, containerChips...)
			}
		}
	}

	// 1. Ingest GitHub Actions CI runs
	for _, run := range runs {
		normSHA := normalizeSHA(run.HeadSHA)
		shortSHA := shortenSHA(normSHA)
		if shortSHA == "" {
			shortSHA = fmt.Sprintf("gh-run-%d", run.ID)
		}

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

		var runJobs []GitHubJob
		if jobs != nil {
			runJobs = jobs[run.ID]
		}
		ciChips := ParseMatrixJobChips(runJobs, refTime)

		stage := ""
		progress := 0
		ciStatus := "pending"

		switch run.Status {
		case "queued":
			stage = "queued"
			progress = 15
			ciStatus = "pending"
		case "in_progress":
			stage = "building"
			progress = 25
			ciStatus = "active"
			if len(ciChips) > 0 {
				doneCount := 0
				for _, c := range ciChips {
					if c.Status == "completed" {
						doneCount++
					}
				}
				progress = 20 + int(float64(doneCount)/float64(len(ciChips))*30)
			}
		case "completed":
			switch run.Conclusion {
			case "failure", "cancelled":
				if refTime.Sub(run.UpdatedAt) < 30*time.Minute || refTime.Sub(run.CreatedAt) < 30*time.Minute {
					stage = "failed"
					progress = 40
					ciStatus = "failed"
				}
			case "success":
				if refTime.Sub(run.UpdatedAt) < 30*time.Minute || refTime.Sub(run.CreatedAt) < 30*time.Minute {
					hasContainerBuilds := false
					for _, j := range runJobs {
						if strings.Contains(j.Name, "Build & Push") && strings.Contains(j.Name, "Images") && j.Conclusion != "skipped" {
							hasContainerBuilds = true
							break
						}
					}
					ciElapsed := refTime.Sub(run.UpdatedAt)
					if ciElapsed < 0 {
						ciElapsed = refTime.Sub(run.CreatedAt)
					}

					hasRecentContainerSwap := false
					if currentCommit != "" && (currentCommit == shortSHA || strings.HasPrefix(run.HeadSHA, currentCommit)) {
						hasRecentContainerSwap = true
					}
					containerCommit := GetContainerCommit(aerialContainers)
					if containerCommit != "" && (containerCommit == shortSHA || strings.HasPrefix(run.HeadSHA, containerCommit)) {
						hasRecentContainerSwap = true
					}
					for _, c := range aerialContainers {
						createdAt := time.Unix(c.Created, 0).UTC()
						if createdAt.After(run.CreatedAt.Add(-30*time.Second)) || (c.Health != nil && c.Health.Status == "starting") {
							hasRecentContainerSwap = true
							break
						}
					}

					isPRRun := (run.HeadBranch != "" && run.HeadBranch != "main" && run.HeadBranch != "master") || run.Event == "pull_request"
					if isPRRun {
						continue
					}
					isNonContainerRepo := run.Repository != "" && !strings.HasSuffix(run.Repository, "/aerial") && !hasContainerBuilds
					if isNonContainerRepo {
						hasRecentContainerSwap = true
					}

					isHangarActive := gitSync.Reconciliation != nil && gitSync.Reconciliation.Active

					if len(runJobs) > 0 && !hasContainerBuilds {
						stage = "live"
						progress = 100
						ciStatus = "completed"
					} else if hasContainerBuilds && !hasRecentContainerSwap && !isHangarActive && ciElapsed > 120*time.Second {
						stage = "failed"
						progress = 55
						ciStatus = "completed"
						commitMsg = "Hangar reconciliation timed out (>120s)"
					} else {
						stage = "awaiting_pull"
						progress = 55
						ciStatus = "completed"
					}
				}
			}
		}

		if stage == "" {
			continue
		}

		entry := getOrCreateEntry(normSHA, fmt.Sprintf("gh-run-%d", run.ID))
		dep := entry.dep
		if run.Repository != "" {
			dep.Repository = run.Repository
			parts := strings.Split(run.Repository, "/")
			if len(parts) == 2 && parts[1] != "aerial" {
				dep.Service = parts[1]
			}
		}

		shouldUpdate := false
		if dep.Stage == "" {
			shouldUpdate = true
		} else if stageRank[stage] > stageRank[dep.Stage] {
			shouldUpdate = true
		} else if stageRank[stage] == stageRank[dep.Stage] && run.CreatedAt.After(dep.StartedAt) {
			shouldUpdate = true
		}

		if shouldUpdate {
			dep.ID = fmt.Sprintf("gh-run-%d", run.ID)
			dep.Commit = shortSHA
			if run.Repository != "" {
				dep.Repository = run.Repository
				parts := strings.Split(run.Repository, "/")
				if len(parts) == 2 && parts[1] != "aerial" {
					dep.Service = parts[1]
				}
			}
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
	}

	// 2. Ingest Hangar Reconciliation
	if gitSync.Reconciliation != nil && (gitSync.Reconciliation.Active || gitSync.Reconciliation.State == "failed" || gitSync.Reconciliation.State == "pulling" || gitSync.Reconciliation.State == "swapping" || gitSync.Reconciliation.State == "healthy") {
		recon := gitSync.Reconciliation
		reconSHA := recon.CommitSHA
		if reconSHA == "" {
			if len(runs) > 0 && runs[0].HeadSHA != "" {
				reconSHA = runs[0].HeadSHA
			} else {
				reconSHA = currentCommit
			}
		}
		normReconSHA := normalizeSHA(reconSHA)
		shortReconSHA := shortenSHA(normReconSHA)
		if shortReconSHA == "" {
			shortReconSHA = "hangar"
		}

		entry := getOrCreateEntry(normReconSHA, fmt.Sprintf("dep-aerial-stack-%s", shortReconSHA))
		dep := entry.dep

		if dep.StartedAt.IsZero() && !recon.StartedAt.IsZero() {
			dep.StartedAt = recon.StartedAt
		}

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
				failMsg = fmt.Sprintf("Hangar reconciliation failed: %s", recon.Error)
			}
			dep.CommitMsg = failMsg
			if len(dep.Steps) < 5 {
				dep.Steps = buildDeploymentSteps("completed", "completed", "failed", "pending", "pending")
			} else {
				dep.Steps[2].Status = "failed"
			}
		}

		if len(recon.TargetServices) > 0 {
			containerChips := BuildTargetContainerChips(aerialContainers, recon.TargetServices, recon.StartedAt, refTime)
			dep.MatrixJobs = append(dep.MatrixJobs, containerChips...)
		}
	}

	// 3. Ingest Local Containers grouped by commit
	groupsByCommit := make(map[string][]DockerContainerJSON)
	for _, c := range aerialContainers {
		commit := ExtractSingleContainerCommit(c)
		svc, isStackContainer := ExtractAerialServiceName(c)
		if commit == "" && gitSync.Reconciliation != nil && len(gitSync.Reconciliation.TargetServices) > 0 && svc != "" {
			for _, ts := range gitSync.Reconciliation.TargetServices {
				if ts == svc {
					if gitSync.Reconciliation.CommitSHA != "" {
						commit = gitSync.Reconciliation.CommitSHA
					}
					break
				}
			}
		}
		if commit == "" && isStackContainer {
			commit = currentCommit
		}
		if commit == "" {
			continue
		}
		norm := normalizeSHA(commit)
		matchedKey := ""
		for gk := range groupsByCommit {
			if len(gk) >= 7 && len(norm) >= 7 && (strings.HasPrefix(gk, norm) || strings.HasPrefix(norm, gk)) {
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

	for groupSHA, groupContainers := range groupsByCommit {
		entry := findEntry(groupSHA)
		if entry == nil {
			// Strictly top-down invariant: Unparented containers NEVER synthesize deployment cards.
			continue
		}
		dep := entry.dep

		var latestCreatedAt time.Time
		minUptimeSec := int64(999999999)
		hasStarting := false
		hasDegraded := false
		healthyCount := 0

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

		if dep.StartedAt.IsZero() {
			dep.StartedAt = latestCreatedAt
		}

		isHangarActive := gitSync.Reconciliation != nil && gitSync.Reconciliation.Active &&
			(gitSync.Reconciliation.CommitSHA == "" || strings.HasPrefix(groupSHA, normalizeSHA(gitSync.Reconciliation.CommitSHA)) || strings.HasPrefix(normalizeSHA(gitSync.Reconciliation.CommitSHA), groupSHA))
		isHangarSwapping := isHangarActive && gitSync.Reconciliation.State == "swapping"
		isHangarPulling := isHangarActive && gitSync.Reconciliation.State == "pulling"

		if hasDegraded {
			dep.Stage = "degraded"
			dep.Progress = 85
			if len(dep.Steps) < 5 {
				dep.Steps = buildDeploymentSteps("completed", "completed", "completed", "completed", "failed")
			} else {
				dep.Steps[3].Status = "completed"
				dep.Steps[4].Status = "failed"
			}
			attachContainerChips(dep, groupContainers)
		} else if hasStarting || minUptimeSec < 120 || isHangarSwapping {
			if stageRank[dep.Stage] < stageRank["swapping"] {
				dep.Stage = "swapping"
				progress := 60
				if len(groupContainers) > 0 {
					progress = 60 + int(float64(healthyCount)/float64(len(groupContainers))*25)
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
			attachContainerChips(dep, groupContainers)
		} else if isHangarPulling {
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
			attachContainerChips(dep, groupContainers)
		} else if minUptimeSec < 600 {
			// Live grace window
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
				attachContainerChips(dep, groupContainers)
			}
		}
	}

	var result []DeploymentStatus
	for _, e := range entries {
		if e.dep.Stage == "" || e.dep.Stage == "idle" {
			continue
		}
		result = append(result, *e.dep)
	}

	sort.Slice(result, func(i, j int) bool {
		rankI := stageRank[result[i].Stage]
		rankJ := stageRank[result[j].Stage]
		if rankI != rankJ {
			return rankI > rankJ
		}
		if result[i].Stage != result[j].Stage {
			if result[i].Stage == "failed" && result[j].Stage == "degraded" {
				return true
			}
			if result[i].Stage == "degraded" && result[j].Stage == "failed" {
				return false
			}
		}
		if !result[i].StartedAt.Equal(result[j].StartedAt) {
			return result[i].StartedAt.After(result[j].StartedAt)
		}
		return result[i].Commit < result[j].Commit
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

	services := make([]ServiceStatus, 0)
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

		alloc, hasAlloc := runningAllocs[jobID]
		if !hasAlloc && job.Name != "" {
			alloc, hasAlloc = runningAllocs[job.Name]
		}

		var uptimeSec int64
		var status string

		if hasAlloc {
			if alloc.CreateTime > 0 {
				uptimeSec = int64(now.Sub(time.Unix(0, alloc.CreateTime).UTC()).Seconds())
				if uptimeSec < 0 {
					uptimeSec = 0
				}
			}

			if alloc.DeploymentStatus != nil {
				if alloc.DeploymentStatus.Healthy {
					status = "healthy"
				} else {
					hasRunningOrPending := false
					for _, task := range alloc.TaskStates {
						if task.State == "running" || task.State == "pending" {
							hasRunningOrPending = true
							break
						}
					}
					if hasRunningOrPending {
						status = "starting"
					} else {
						status = "unhealthy"
					}
				}
			} else {
				hasFailedOrNotRunning := false
				for _, task := range alloc.TaskStates {
					if task.Failed || task.State != "running" {
						hasFailedOrNotRunning = true
						break
					}
				}
				if hasFailedOrNotRunning {
					status = "unhealthy"
				} else {
					status = "healthy"
				}
			}
		} else {
			uptimeSec = 0
			startingCount := 0
			for _, grp := range job.JobSummary.Summary {
				startingCount += grp.Starting
			}
			if job.Status == "pending" || startingCount > 0 {
				status = "starting"
			} else {
				status = "unhealthy"
			}
		}

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

// normalizeGitCommit sanitizes and truncates a git commit string to short 7-character form.
func normalizeGitCommit(commit string) string {
	commit = strings.TrimSpace(commit)
	if len(commit) > 7 {
		return commit[:7]
	}
	return commit
}

// resolveDashboardGitHubRepos resolves the primary and list of GitHub repositories from lookup and YAML config.
func resolveDashboardGitHubRepos(lookup func(string) string, yamlRepos []string, yamlRepo string) (string, []string) {
	var ghRepos []string
	rawGHRepos := strings.TrimSpace(lookup("GITHUB_REPOS"))
	if rawGHRepos != "" {
		seen := make(map[string]bool)
		for _, part := range strings.Split(rawGHRepos, ",") {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" && !seen[trimmed] {
				seen[trimmed] = true
				ghRepos = append(ghRepos, trimmed)
			}
		}
	} else if len(yamlRepos) > 0 {
		ghRepos = append(ghRepos, yamlRepos...)
	}

	ghRepo := strings.TrimSpace(lookup("GITHUB_REPO"))
	if ghRepo == "" && yamlRepo != "" {
		ghRepo = strings.TrimSpace(yamlRepo)
	}
	if len(ghRepos) > 0 {
		if ghRepo == "" {
			ghRepo = ghRepos[0]
		}
	} else if ghRepo != "" {
		ghRepos = []string{ghRepo}
	} else {
		ghRepo = "azylman/aerial"
		ghRepos = []string{"azylman/aerial"}
	}
	return ghRepo, ghRepos
}

// resolveDashboardVanadisURL resolves the Vanadis service base URL from user config, YAML, or defaults.
func resolveDashboardVanadisURL(configPath, yamlVanadisURL string) string {
	vanadisURL := loadDashboardVanadisURL(configPath)
	if vanadisURL == "" && yamlVanadisURL != "" {
		vanadisURL = strings.TrimSpace(yamlVanadisURL)
	}
	if vanadisURL == "" {
		vanadisURL = "http://vanadis.aerial"
	}
	return vanadisURL
}

// resolveDashboardHaroURL resolves the Haro service base URL from env lookup, YAML config, or defaults.
func resolveDashboardHaroURL(lookup func(string) string, configPath, yamlHaroURL string) string {
	haroURL := strings.TrimSpace(lookup("HARO_URL"))
	if haroURL == "" {
		if schedURL := strings.TrimSpace(lookup("SCHEDULER_MCP_URL")); schedURL != "" {
			schedURL = strings.TrimRight(schedURL, "/")
			schedURL = strings.TrimSuffix(schedURL, "/mcp")
			schedURL = strings.TrimSuffix(schedURL, "/sse")
			schedURL = strings.TrimRight(schedURL, "/")
			haroURL = schedURL
		}
	}
	if haroURL == "" {
		haroURL = loadDashboardHaroURL(configPath)
	}
	if haroURL == "" && yamlHaroURL != "" {
		haroURL = strings.TrimSpace(yamlHaroURL)
	}
	if haroURL == "" {
		haroURL = "http://haro.aerial"
	}
	return haroURL
}
