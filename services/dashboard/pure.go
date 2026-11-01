package main

import (
	"fmt"
	"mime"
	"net/url"
	"path/filepath"
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

func parsePositiveInt(val string) (int, bool) {
	if val == "" {
		return 0, false
	}
	n, err := strconv.Atoi(val)
	return n, err == nil && n > 0
}

func parseNonNegativeInt(val string) (int, bool) {
	if val == "" {
		return 0, false
	}
	n, err := strconv.Atoi(val)
	return n, err == nil && n >= 0
}

func clampInt(val, minVal, maxVal int) int {
	if val < minVal {
		return minVal
	}
	if val > maxVal {
		return maxVal
	}
	return val
}

func applyFactsQueryParams(outQuery url.Values, inQuery url.Values) (int, int) {
	if inQuery == nil {
		return 0, 0
	}
	limit := 0
	if n, ok := parsePositiveInt(inQuery.Get("limit")); ok {
		limit = n
		outQuery.Set("limit", strconv.Itoa(limit))
	}

	offset := 0
	if n, ok := parseNonNegativeInt(inQuery.Get("offset")); ok {
		offset = n
		if offset > 0 {
			outQuery.Set("offset", strconv.Itoa(offset))
		}
	}

	if search := strings.TrimSpace(inQuery.Get("q")); search != "" {
		runes := []rune(search)
		if len(runes) > 64 {
			search = strings.TrimSpace(string(runes[:64]))
		}
		outQuery.Set("q", search)
	}
	return limit, offset
}

// BuildFactsUpstreamURL validates the base URL and constructs the target upstream
// URL with whitelisted, normalized, and clamped query parameters.
func BuildFactsUpstreamURL(brainBaseURL string, inQuery url.Values) (targetURL string, limit, offset int, err error) {
	target, err := parseUpstreamURL(brainBaseURL, fmt.Sprintf("invalid upstream URL configuration: %q", brainBaseURL), "/facts")
	if err != nil {
		return "", 0, 0, err
	}

	outQuery := target.Query()
	limit, offset = applyFactsQueryParams(outQuery, inQuery)

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
		if n, ok := parsePositiveInt(inQuery.Get("limit")); ok && n <= 100 {
			limit = n
		}

		if n, ok := parseNonNegativeInt(inQuery.Get("offset")); ok {
			offset = n
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
		if n, err := strconv.Atoi(strings.TrimSpace(inQuery.Get("limit"))); err == nil {
			limit = clampInt(n, 1, 100)
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
	if after, ok := strings.CutPrefix(requestPath, "/dashboard/api/builds/"); ok {
		rawID = after
	} else if after, ok := strings.CutPrefix(requestPath, "/api/builds/"); ok {
		rawID = after
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

func extractServiceNameFromCompose(labels map[string]string) (string, bool, bool) {
	proj, hasProj := labels["com.docker.compose.project"]
	if hasProj {
		if proj == "aerial" {
			if svc := labels["com.docker.compose.service"]; svc != "" {
				return svc, true, true
			}
		}
		// Foreign Compose project (e.g. homeassistant) -> reject
		return "", false, true
	}
	return "", false, false
}

func extractServiceNameFromNomad(labels map[string]string, names []string) (string, bool) {
	allocID := labels["com.hashicorp.nomad.alloc_id"]
	if allocID == "" || len(names) == 0 {
		return "", false
	}
	name := strings.TrimPrefix(names[0], "/")
	suffix := "-" + allocID
	if before, ok := strings.CutSuffix(name, suffix); ok {
		task := before
		if task != "" {
			return task, true
		}
	}
	return "", false
}

func extractServiceNameFromLabels(labels map[string]string, names []string) (string, bool) {
	if svc, ok, handled := extractServiceNameFromCompose(labels); handled {
		return svc, ok
	}
	if svc := labels["aerial.service"]; svc != "" {
		return svc, true
	}
	if svc, ok := extractServiceNameFromNomad(labels, names); ok {
		return svc, true
	}
	if svc := labels["com.docker.compose.service"]; svc != "" {
		return svc, true
	}
	return "", false
}

func extractServiceNameFromNames(names []string) (string, bool) {
	if len(names) == 0 {
		return "", false
	}
	name := strings.TrimPrefix(names[0], "/")
	if after, ok := strings.CutPrefix(name, "aerial-"); ok {
		svc := after
		if svc != "" {
			return svc, true
		}
	}
	return "", false
}

// ExtractAerialServiceName extracts the canonical Aerial service name and returns whether
// the container belongs to the Aerial stack (either Docker Compose or Nomad managed).
func ExtractAerialServiceName(c DockerContainerJSON) (string, bool) {
	if c.Labels != nil {
		if svc, ok := extractServiceNameFromLabels(c.Labels, c.Names); ok {
			return svc, true
		}
		if _, hasProj := c.Labels["com.docker.compose.project"]; hasProj {
			return "", false
		}
	}

	return extractServiceNameFromNames(c.Names)
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

// containsWordToken returns true if token matches any discrete word token in s
// when splitting on non-alphanumeric/non-hyphen characters.
func containsWordToken(s, token string) bool {
	tokens := strings.FieldsFunc(s, func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-')
	})
	for _, tok := range tokens {
		if strings.EqualFold(tok, token) {
			return true
		}
	}
	return false
}

// ExtractServiceNameFromJobName resolves a microservice name from a GitHub Actions job name.
func ExtractServiceNameFromJobName(jobName string) string {
	lower := strings.ToLower(jobName)
	services := []string{
		"brain",
		"dashboard",
		"proxy",
		"haro",
		"coven",
		"vanadis",
		"github-runner",
		"discord-mcp",
		"kernel-mcp",
		"github-mcp",
		"nomad-mcp",
		"infisical-mcp",
		"synapse",
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
	if containsWordToken(lower, "eri") {
		return "eri"
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
	parts := strings.SplitSeq(ifNoneMatch, ",")
	for part := range parts {
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

// normalizeGitCommit sanitizes and truncates a git commit string to short 7-character form.
func normalizeGitCommit(commit string) string {
	commit = strings.TrimSpace(commit)
	if len(commit) > 7 {
		return commit[:7]
	}
	return commit
}
