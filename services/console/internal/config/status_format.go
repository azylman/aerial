package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	chipTypeContainer     = "container"
	healthStarting        = "starting"
	healthHealthy         = "healthy"
	healthUnhealthy       = "unhealthy"
	statusDegraded        = "degraded"
	statusError           = "error"
	statusCompleted       = "completed"
	statusFailed          = "failed"
	statusPending         = "pending"
	statusActive          = "active"
	statusQueued          = "queued"
	statusInProgress      = "in_progress"
	conclusionFailure     = "failure"
	conclusionSuccess     = "success"
	stageAwaitingPull     = "awaiting_pull"
	stagePulling          = "pulling"
	stageSwapping         = "swapping"
	stageBuilding         = "building"
	stageLive             = "live"
	containerStateRunning = "running"
	serviceOllama         = "ollama"
	serviceAgentsview     = "agentsview"
	defaultAgentTenant    = "aerial"
	agentFilterAll        = "all"
)

var SensitiveKeys = []string{
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
	".html":        "text/html; charset=utf-8",
	".css":         "text/css; charset=utf-8",
	".js":          "application/javascript; charset=utf-8",
	".json":        "application/json; charset=utf-8",
	".webmanifest": "application/manifest+json; charset=utf-8",
	".svg":         "image/svg+xml",
	".ico":         "image/x-icon",
	".png":         "image/png",
	".woff2":       "font/woff2",
	".woff":        "font/woff",
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
	status = statusCompleted
	conclusion = conclusionSuccess
	if c.Health != nil && c.Health.Status == healthStarting {
		status = statusActive
		conclusion = ""
	} else if c.State != containerStateRunning || (c.Health != nil && c.Health.Status == healthUnhealthy) {
		status = statusFailed
		conclusion = conclusionFailure
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

// buildDeploymentSteps constructs the standard 5-step Permet HUD deployment pipeline. The
// commit trigger step is always completed by the time a deployment card exists.
func buildDeploymentSteps(s2, s3, s4, s5 string) []DeploymentStep {
	return []DeploymentStep{
		{Name: "Commit Trigger", Icon: "📦", Status: "completed"},
		{Name: "CI Build & GHCR", Icon: "⚙️", Status: s2},
		{Name: "Flux Sync", Icon: "⬇️", Status: s3},
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

	if agent := strings.TrimSpace(inQuery.Get("agent")); agent != "" && !strings.EqualFold(agent, agentFilterAll) {
		outQuery.Set("agent", NormalizeAgent(agent))
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
func BuildSchedulesUpstreamURL(haroBaseURL string, inQuery ...url.Values) (string, error) {
	target, err := parseUpstreamURL(haroBaseURL, fmt.Sprintf("invalid upstream URL configuration: %q", haroBaseURL), "/schedules")
	if err != nil {
		return "", err
	}
	if len(inQuery) > 0 && inQuery[0] != nil {
		outQuery := target.Query()
		if agent := strings.TrimSpace(inQuery[0].Get("agent")); agent != "" && !strings.EqualFold(agent, agentFilterAll) {
			outQuery.Set("agent", NormalizeAgent(agent))
			target.RawQuery = outQuery.Encode()
		}
	}
	return target.String(), nil
}

func applyScheduleRunsQueryParams(outQuery, inQuery url.Values) (int, int) {
	limit := 50
	offset := 0
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
	if agent := strings.TrimSpace(inQuery.Get("agent")); agent != "" && !strings.EqualFold(agent, agentFilterAll) {
		outQuery.Set("agent", NormalizeAgent(agent))
	}
	return limit, offset
}

// BuildScheduleRunsUpstreamURL validates the base URL and constructs the upstream
// /schedules/runs URL with sanitized limit, offset, schedule_id, status, and agent parameters.
func BuildScheduleRunsUpstreamURL(haroBaseURL string, inQuery url.Values) (targetURL string, limit, offset int, err error) {
	target, err := parseUpstreamURL(haroBaseURL, fmt.Sprintf("invalid upstream URL configuration: %q", haroBaseURL), "/schedules/runs")
	if err != nil {
		return "", 0, 0, err
	}

	outQuery := target.Query()
	limit = 50
	offset = 0

	if inQuery != nil {
		limit, offset = applyScheduleRunsQueryParams(outQuery, inQuery)
	}

	outQuery.Set("limit", strconv.Itoa(limit))
	outQuery.Set("offset", strconv.Itoa(offset))
	target.RawQuery = outQuery.Encode()

	return target.String(), limit, offset, nil
}

// BuildVanadisBuildsUpstreamURL validates the base URL and constructs the upstream /builds URL
// with clamped limit (1..100, default 50), and optional status, queue_state, and agent query parameters.
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
		if agent := strings.TrimSpace(inQuery.Get("agent")); agent != "" && !strings.EqualFold(agent, agentFilterAll) {
			outQuery.Set("agent", NormalizeAgent(agent))
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
	if after, ok := strings.CutPrefix(requestPath, "/console/api/builds/"); ok {
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

// ErrInvalidDocketRequest marks client-side request validation failures (bad task ID or
// search parameters) so docket handlers can answer 400 instead of a configuration error.
var (
	ErrInvalidDocketRequest = errors.New("invalid docket request")
	errInvalidDocketRequest = ErrInvalidDocketRequest
)

// maxDocketSearchQueryLen bounds the free-text search query forwarded to docket-mcp.
const maxDocketSearchQueryLen = 200

// copyNonEmptyParams copies the trimmed, non-empty values of keys from in to out.
func copyNonEmptyParams(out, in url.Values, keys ...string) {
	for _, key := range keys {
		if v := strings.TrimSpace(in.Get(key)); v != "" {
			out.Set(key, v)
		}
	}
}

// BuildDocketTasksUpstreamURL constructs the docket-mcp /api/tasks URL, forwarding only status.
func BuildDocketTasksUpstreamURL(docketBaseURL string, inQuery url.Values) (string, error) {
	target, err := parseUpstreamURL(docketBaseURL, "docket base URL is empty", "/api/tasks")
	if err != nil {
		return "", err
	}
	outQuery := url.Values{}
	copyNonEmptyParams(outQuery, inQuery, "status")
	target.RawQuery = outQuery.Encode()
	return target.String(), nil
}

// BuildDocketSearchUpstreamURL constructs the docket-mcp /api/search URL, forwarding q, status and limit.
// An empty query or one longer than maxDocketSearchQueryLen characters wraps errInvalidDocketRequest.
func BuildDocketSearchUpstreamURL(docketBaseURL string, inQuery url.Values) (string, error) {
	target, err := parseUpstreamURL(docketBaseURL, "docket base URL is empty", "/api/search")
	if err != nil {
		return "", err
	}
	q := strings.TrimSpace(inQuery.Get("q"))
	if q == "" {
		return "", fmt.Errorf("%w: search query q is required", errInvalidDocketRequest)
	}
	if utf8.RuneCountInString(q) > maxDocketSearchQueryLen {
		return "", fmt.Errorf("%w: search query q exceeds %d characters", errInvalidDocketRequest, maxDocketSearchQueryLen)
	}
	outQuery := url.Values{}
	outQuery.Set("q", q)
	copyNonEmptyParams(outQuery, inQuery, "status", "limit")
	target.RawQuery = outQuery.Encode()
	return target.String(), nil
}

// BuildDocketTaskDetailUpstreamURL extracts a positive decimal task ID from requestPath
// (/api/docket/tasks/<id>, optionally under /console) and constructs the docket-mcp /api/tasks/<id> URL.
// Malformed or missing IDs wrap errInvalidDocketRequest.
func BuildDocketTaskDetailUpstreamURL(docketBaseURL, requestPath string) (targetURL string, id string, err error) {
	baseParsed, err := parseUpstreamURL(docketBaseURL, "docket base URL is empty", "")
	if err != nil {
		return "", "", err
	}

	rawID, ok := strings.CutPrefix(strings.TrimPrefix(requestPath, "/console"), "/api/docket/tasks/")
	if !ok {
		return "", "", fmt.Errorf("%w: unexpected request path %q", errInvalidDocketRequest, requestPath)
	}
	if rawID == "" || strings.Trim(rawID, "0123456789") != "" {
		return "", "", fmt.Errorf("%w: task ID %q must be a positive integer", errInvalidDocketRequest, rawID)
	}
	n, convErr := strconv.ParseInt(rawID, 10, 64)
	if convErr != nil || n <= 0 {
		return "", "", fmt.Errorf("%w: task ID %q must be a positive integer", errInvalidDocketRequest, rawID)
	}

	id = strconv.FormatInt(n, 10)
	return baseParsed.String() + "/api/tasks/" + id, id, nil
}

// CalculateClusterStatus computes cluster health from a slice of ServiceStatus items.
// If any service is unhealthy, the cluster is considered degraded.
func CalculateClusterStatus(services []ServiceStatus) string {
	for _, s := range services {
		if s.Status == healthUnhealthy {
			return statusDegraded
		}
	}
	return healthHealthy
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

// containerServiceName returns the Aerial service a container was synthesized for.
func containerServiceName(c DockerContainerJSON) string {
	return c.Labels["aerial.service"]
}

// containsWordToken returns true if token matches any discrete word token in s
// when splitting on non-alphanumeric/non-hyphen characters.
func containsWordToken(s, token string) bool {
	tokens := strings.FieldsFunc(s, func(r rune) bool {
		return (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-'
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
		"console",
		"proxy",
		"haro",
		"coven",
		"vanadis",
		"github-runner",
		"discord-mcp",
		"kernel-mcp",
		"github-mcp",
		"infisical-mcp",
		"synapse",
		serviceOllama,
		serviceAgentsview,
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

		chipStatus := statusPending
		switch j.Status {
		case statusInProgress:
			chipStatus = statusActive
		case statusCompleted:
			switch j.Conclusion {
			case conclusionSuccess:
				chipStatus = statusCompleted
			case conclusionFailure:
				chipStatus = statusFailed
			default:
				chipStatus = statusPending
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
		svcName := containerServiceName(c)
		if svcName == "" || seen[svcName] {
			continue
		}
		seen[svcName] = true

		status, conclusion, durStr := containerChipStatusAndDuration(c, now)

		chips = append(chips, MatrixJobChip{
			Name:       svcName,
			Status:     status,
			Conclusion: conclusion,
			Duration:   durStr,
			Type:       chipTypeContainer,
		})
	}
	return chips
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
	stageSwapping:     6,
	stagePulling:      5,
	stageBuilding:     4,
	stageAwaitingPull: 3,
	statusQueued:      2,
	statusFailed:      1,
	statusDegraded:    1,
	stageLive:         0,
}

// CalculateDeployStatus evaluates active deployment stages with deterministic precedence:
// swapping > pulling > building > awaiting_pull > queued > failed > degraded > idle.
// Returns "idle" if no deployments exist or all deployments are completed/live.
func CalculateDeployStatus(deployments []DeploymentStatus) string {
	bestRank := 0
	bestStage := "idle"

	for _, d := range deployments {
		if rank, ok := stageRank[d.Stage]; ok {
			if rank > bestRank || (rank == bestRank && d.Stage == statusFailed && bestStage == statusDegraded) {
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
		case statusQueued, stageBuilding, stageAwaitingPull, stagePulling, stageSwapping:
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

// NormalizeAgent cleans an agent identifier to a lowercase DNS slug ([a-z0-9-]).
func NormalizeAgent(agent string) string {
	clean := strings.ToLower(strings.TrimSpace(agent))
	var sb strings.Builder
	for _, r := range clean {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

// MatchAgent reports whether an item's agent matches filterAgent.
// An empty filter or "all" matches any item agent.
// An empty item agent is treated as defaultAgentTenant.
func MatchAgent(itemAgent, filterAgent string) bool {
	f := NormalizeAgent(filterAgent)
	if f == "" || f == agentFilterAll {
		return true
	}
	item := NormalizeAgent(itemAgent)
	if item == "" {
		item = defaultAgentTenant
	}
	return item == f
}

// SortConfiguredAgents normalizes and sorts agent slugs, always placing defaultAgentTenant first
// and sorting any additional agents alphabetically without duplicates.
func SortConfiguredAgents(agents []string) []string {
	seen := make(map[string]bool)
	var others []string

	for _, raw := range agents {
		a := NormalizeAgent(raw)
		if a == "" || a == agentFilterAll || seen[a] {
			continue
		}
		seen[a] = true
		if a != defaultAgentTenant {
			others = append(others, a)
		}
	}
	sort.Strings(others)
	result := make([]string, 0, len(others)+1)
	result = append(result, defaultAgentTenant)
	result = append(result, others...)
	return result
}

func filterByAgent[T any](items []T, filterAgent string, agentOf func(T) string) []T {
	f := NormalizeAgent(filterAgent)
	if f == "" || f == agentFilterAll {
		return items
	}
	out := make([]T, 0, len(items))
	for _, item := range items {
		if MatchAgent(agentOf(item), f) {
			out = append(out, item)
		}
	}
	return out
}

// FilterFactsByAgent filters facts to those matching filterAgent.
func FilterFactsByAgent(facts []FactItem, filterAgent string) []FactItem {
	return filterByAgent(facts, filterAgent, func(item FactItem) string { return item.Agent })
}

// FilterCronsByAgent filters cron schedules to those matching filterAgent.
func FilterCronsByAgent(crons []CronSchedule, filterAgent string) []CronSchedule {
	return filterByAgent(crons, filterAgent, func(item CronSchedule) string { return item.Agent })
}

// FilterOneShotsByAgent filters one-shot schedules to those matching filterAgent.
func FilterOneShotsByAgent(oneshots []OneShotSchedule, filterAgent string) []OneShotSchedule {
	return filterByAgent(oneshots, filterAgent, func(item OneShotSchedule) string { return item.Agent })
}

// FilterScheduleRunsByAgent filters schedule runs to those matching filterAgent.
func FilterScheduleRunsByAgent(runs []ScheduleRun, filterAgent string) []ScheduleRun {
	return filterByAgent(runs, filterAgent, func(item ScheduleRun) string { return item.Agent })
}

// FilterBuildsByAgent filters Vanadis builds to those matching filterAgent.
func FilterBuildsByAgent(builds []VanadisBuildDTO, filterAgent string) []VanadisBuildDTO {
	return filterByAgent(builds, filterAgent, func(item VanadisBuildDTO) string { return item.Agent })
}

// FilterActiveTasksByAgent filters active tasks to those matching filterAgent.
func FilterActiveTasksByAgent(tasks []ActiveTaskStatus, filterAgent string) []ActiveTaskStatus {
	return filterByAgent(tasks, filterAgent, func(item ActiveTaskStatus) string { return item.Agent })
}

// AgentBrainTarget is one agent stack's eri base URL.
type AgentBrainTarget struct {
	Agent string
	URL   string
}

// AgentBrainURL derives an agent's eri base URL from the configured default eri URL by
// naming convention: the default agent uses the configured URL as-is; any other agent
// replaces the host's first DNS label "eri" with "eri-<agent>", keeping scheme, port and
// path. It returns "" when the URL cannot be derived (unparseable, or first label not "eri").
func AgentBrainURL(defaultURL, defaultAgent, agent string) string {
	base := strings.TrimSpace(defaultURL)
	a := NormalizeAgent(agent)
	if base == "" || a == "" {
		return ""
	}
	if a == NormalizeAgent(defaultAgent) {
		return base
	}
	u, err := url.Parse(base)
	if err != nil || u.Host == "" {
		return ""
	}
	host := u.Hostname()
	label, rest, _ := strings.Cut(host, ".")
	if !strings.EqualFold(label, "eri") {
		return ""
	}
	newHost := "eri-" + a
	if rest != "" {
		newHost += "." + rest
	}
	if port := u.Port(); port != "" {
		newHost += ":" + port
	}
	u.Host = newHost
	return u.String()
}

// SiblingBrainTargets returns eri targets for every discovered agent other than the default,
// deduplicated by normalized agent and skipping agents whose URL cannot be derived.
func SiblingBrainTargets(defaultURL, defaultAgent string, agents []string) []AgentBrainTarget {
	def := NormalizeAgent(defaultAgent)
	seen := map[string]bool{}
	out := []AgentBrainTarget{}
	for _, raw := range agents {
		a := NormalizeAgent(raw)
		if a == "" || a == def || seen[a] {
			continue
		}
		seen[a] = true
		if u := AgentBrainURL(defaultURL, def, a); u != "" {
			out = append(out, AgentBrainTarget{Agent: a, URL: u})
		}
	}
	return out
}

// DefaultAgentFromTasks returns the agent the configured (default) eri reports in its task
// rows, falling back to defaultAgentTenant when none of its rows carry an agent.
func DefaultAgentFromTasks(tasks []ActiveTaskStatus) string {
	for _, t := range tasks {
		if a := NormalizeAgent(t.Agent); a != "" {
			return a
		}
	}
	return defaultAgentTenant
}

// StampTaskAgent sets Agent on rows that have none, returning a new slice.
func StampTaskAgent(tasks []ActiveTaskStatus, agent string) []ActiveTaskStatus {
	out := make([]ActiveTaskStatus, len(tasks))
	for i, t := range tasks {
		if strings.TrimSpace(t.Agent) == "" {
			t.Agent = agent
		}
		out[i] = t
	}
	return out
}

type ServiceStatus struct {
	Name          string    `json:"name"`
	Status        string    `json:"status"`
	UptimeSeconds int64     `json:"uptime_seconds"`
	LastCheckTime time.Time `json:"last_check_time"`
	// Commit is the full commit SHA from the Deployment's image tag (k8s mode only).
	Commit string `json:"commit,omitempty"`
	// Image is the Deployment's image reference (k8s mode only).
	Image string `json:"image,omitempty"`
}

type ActiveTaskStatus struct {
	ID          string    `json:"id"`
	ThreadID    string    `json:"thread_id"`
	SessionID   string    `json:"session_id,omitempty"`
	AuthorName  string    `json:"author_name"`
	Prompt      string    `json:"prompt"`
	Summary     string    `json:"summary"`
	Status      string    `json:"status"`
	RetryCount  int       `json:"retry_count"`
	TriggerType string    `json:"trigger_type"`
	CreatedAt   time.Time `json:"created_at"`
	StartedAt   time.Time `json:"started_at"`
	Agent       string    `json:"agent,omitempty"`
}

type BrainTasksAPIResponse struct {
	Status string `json:"status"`
	Total  int    `json:"total"`
	Tasks  []struct {
		ID          string    `json:"id"`
		ThreadID    string    `json:"thread_id"`
		SessionID   string    `json:"session_id"`
		AuthorName  string    `json:"author_name"`
		Prompt      string    `json:"prompt"`
		Summary     string    `json:"summary"`
		Status      string    `json:"status"`
		RetryCount  int       `json:"retry_count"`
		TriggerType string    `json:"trigger_type"`
		CreatedAt   time.Time `json:"created_at"`
		UpdatedAt   time.Time `json:"updated_at"`
		Agent       string    `json:"agent,omitempty"`
	} `json:"tasks"`
}

type HeadCommit struct {
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

type GitHubRun struct {
	ID      int64  `json:"id"`
	Name    string `json:"name"`
	RepoObj *struct {
		FullName string `json:"full_name"`
	} `json:"repository,omitempty"`
	Repository string      `json:"-"`
	HeadSHA    string      `json:"head_sha"`
	HeadBranch string      `json:"head_branch"`
	Event      string      `json:"event,omitempty"`
	HeadCommit *HeadCommit `json:"head_commit,omitempty"`
	Status     string      `json:"status"`     // "queued", "in_progress", "completed"
	Conclusion string      `json:"conclusion"` // "success", "failure", "cancelled"
	CreatedAt  time.Time   `json:"created_at"`
	UpdatedAt  time.Time   `json:"updated_at"`
	HTMLURL    string      `json:"html_url"`
	// MergedFromBranch is the head branch of the pull request a push-to-main
	// run merged, resolved by the poller so the run shares its PR's card.
	MergedFromBranch string `json:"-"`
}

type GitHubJob struct {
	ID          int64     `json:"id"`
	RunID       int64     `json:"run_id"`
	Name        string    `json:"name"`
	Status      string    `json:"status"`
	Conclusion  string    `json:"conclusion"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	HTMLURL     string    `json:"html_url"`
}

type GitHubRunsResponse struct {
	TotalCount   int         `json:"total_count"`
	WorkflowRuns []GitHubRun `json:"workflow_runs"`
}

type GitHubJobsResponse struct {
	TotalCount int         `json:"total_count"`
	Jobs       []GitHubJob `json:"jobs"`
}

type MergedPullRequest struct {
	MergedAt *time.Time `json:"merged_at"`
	Head     struct {
		Ref string `json:"ref"`
	} `json:"head"`
}

// ParseMergedPullHeadRef returns the head branch of the first merged pull
// request in a commits/{sha}/pulls response, or "" when none was merged.
func ParseMergedPullHeadRef(body []byte) (string, error) {
	var pulls []MergedPullRequest
	if err := json.Unmarshal(body, &pulls); err != nil {
		return "", fmt.Errorf("decode commit pulls: %w", err)
	}
	for _, pr := range pulls {
		if pr.MergedAt != nil && pr.Head.Ref != "" {
			return pr.Head.Ref, nil
		}
	}
	return "", nil
}

// FactItem represents a semantic memory item received from brain
type FactItem struct {
	ID               int64     `json:"id"`
	FactText         string    `json:"fact_text"`
	ReinforceCount   int       `json:"reinforce_count"`
	LastReinforcedAt time.Time `json:"last_reinforced_at,omitzero"`
	CreatedAt        time.Time `json:"created_at"`
	Agent            string    `json:"agent,omitempty"`
}

// UnmarshalJSON handles decoding facts reading source_agent as agent if agent is missing.
func (f *FactItem) UnmarshalJSON(data []byte) error {
	type Alias FactItem
	var aux struct {
		Alias

		SourceAgent string `json:"source_agent,omitempty"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	*f = FactItem(aux.Alias)
	if f.Agent == "" && aux.SourceAgent != "" {
		f.Agent = aux.SourceAgent
	}
	return nil
}

type FactsAPIResponse struct {
	Facts  []FactItem `json:"facts"`
	Total  int        `json:"total"`
	Limit  int        `json:"limit"`
	Offset int        `json:"offset"`
	Status string     `json:"status,omitempty"`
	Error  string     `json:"error,omitempty"`
}

// UnmarshalJSON decodes facts and applies FactItem custom unmarshaling to raw items.
func (r *FactsAPIResponse) UnmarshalJSON(data []byte) error {
	type Alias FactsAPIResponse
	var aux struct {
		*Alias

		Facts []json.RawMessage `json:"facts"`
	}
	aux.Alias = (*Alias)(r)
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	*r = FactsAPIResponse(*aux.Alias)
	if len(aux.Facts) > 0 {
		r.Facts = make([]FactItem, len(aux.Facts))
		for i, raw := range aux.Facts {
			if err := r.Facts[i].UnmarshalJSON(raw); err != nil {
				return err
			}
		}
	}
	return nil
}

// ScheduleSummaryMetrics represents aggregated schedule execution metrics
type ScheduleSummaryMetrics struct {
	TotalActive    int     `json:"total_active"`
	CronCount      int     `json:"cron_count"`
	HeartbeatCount int     `json:"heartbeat_count,omitempty"`
	OneShotCount   int     `json:"one_shot_count"`
	TotalRuns24h   int     `json:"total_runs_24h"`
	NextRunAt      *string `json:"next_run_at,omitempty"`
	SuccessRate24h float64 `json:"success_rate_24h"`
}

// CronSchedule represents a recurring cron schedule
type CronSchedule struct {
	ID              string    `json:"id"`
	TargetID        string    `json:"target_id,omitempty"`
	ChannelID       string    `json:"channel_id"`
	TitlePrefix     string    `json:"title_prefix"`
	CronExpr        string    `json:"cron_expr"`
	CronDescription string    `json:"cron_description"`
	Prompt          string    `json:"prompt"`
	Timezone        string    `json:"timezone"`
	NextRunAt       time.Time `json:"next_run_at"`
	Enabled         bool      `json:"enabled"`
	CreatedAt       time.Time `json:"created_at"`
	Agent           string    `json:"agent,omitempty"`
}

// OneShotSchedule represents a one-time scheduled reminder
type OneShotSchedule struct {
	ID        string    `json:"id"`
	ThreadID  string    `json:"thread_id"`
	Prompt    string    `json:"prompt"`
	RunAt     time.Time `json:"run_at"`
	CreatedAt time.Time `json:"created_at"`
	Agent     string    `json:"agent,omitempty"`
}

// SchedulesAPIResponse represents the response payload for schedules and summary metrics
type SchedulesAPIResponse struct {
	Status     string                 `json:"status"`
	SystemTime time.Time              `json:"system_time,omitempty"`
	Summary    ScheduleSummaryMetrics `json:"summary"`
	Crons      []CronSchedule         `json:"crons"`
	Heartbeats []CronSchedule         `json:"heartbeats,omitempty"`
	OneShots   []OneShotSchedule      `json:"one_shots"`
	Error      string                 `json:"error,omitempty"`
}

// ScheduleRun represents a logged schedule execution run
type ScheduleRun struct {
	ID           string     `json:"id"`
	ScheduleID   string     `json:"schedule_id"`
	ScheduleType string     `json:"schedule_type"`
	MessageID    string     `json:"message_id,omitempty"`
	TargetID     string     `json:"target_id,omitempty"`
	ThreadID     string     `json:"thread_id,omitempty"`
	Title        string     `json:"title,omitempty"`
	Prompt       string     `json:"prompt"`
	Status       string     `json:"status"`
	StartedAt    time.Time  `json:"started_at"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
	DurationMs   int64      `json:"duration_ms"`
	Error        string     `json:"error,omitempty"`
	Agent        string     `json:"agent,omitempty"`
}

// ScheduleRunsAPIResponse represents paginated execution run logs
type ScheduleRunsAPIResponse struct {
	Status string        `json:"status"`
	Total  int           `json:"total"`
	Limit  int           `json:"limit"`
	Offset int           `json:"offset"`
	Runs   []ScheduleRun `json:"runs"`
	Error  string        `json:"error,omitempty"`
}

// VanadisChunkDTO represents an individual task chunk within a Vanadis build.
type VanadisChunkDTO struct {
	ID                 string    `json:"id"`
	BuildID            string    `json:"build_id"`
	ChunkIndex         int       `json:"chunk_index"`
	Repo               string    `json:"repo,omitempty"`
	Title              string    `json:"title"`
	Instructions       string    `json:"instructions,omitempty"`
	TargetFiles        []string  `json:"target_files,omitempty"`
	TargetSymbols      []string  `json:"target_symbols,omitempty"`
	Effort             string    `json:"effort,omitempty"`
	AcceptanceCriteria []string  `json:"acceptance_criteria,omitempty"`
	DependsOn          []string  `json:"depends_on"`
	Status             string    `json:"status"`
	PRNumber           int       `json:"pr_number,omitempty"`
	StartedAt          time.Time `json:"started_at,omitempty"`
	CompletedAt        time.Time `json:"completed_at,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
	Agent              string    `json:"agent,omitempty"`
}

// VanadisBuildDTO represents a Vanadis build run or build entry.
type VanadisBuildDTO struct {
	ID              string            `json:"id"`
	ThreadID        string            `json:"thread_id"`
	Tier            int               `json:"tier"`
	Title           string            `json:"title"`
	Objective       string            `json:"objective"`
	SharedContext   string            `json:"shared_context,omitempty"`
	Status          string            `json:"status"`
	StatusMessageID string            `json:"status_message_id,omitempty"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`
	QueueState      string            `json:"queue_state,omitempty"`
	TotalChunks     int               `json:"total_chunks,omitempty"`
	CompletedChunks int               `json:"completed_chunks,omitempty"`
	RunningChunks   int               `json:"running_chunks,omitempty"`
	PendingChunks   int               `json:"pending_chunks,omitempty"`
	FailedChunks    int               `json:"failed_chunks,omitempty"`
	Chunks          []VanadisChunkDTO `json:"chunks,omitempty"`
	Agent           string            `json:"agent,omitempty"`
}

// BuildsAPIResponse represents the response payload for listing Vanadis builds.
type BuildsAPIResponse struct {
	Status         string            `json:"status"`
	Total          int               `json:"total"`
	RunningCount   int               `json:"running_count"`
	QueuedCount    int               `json:"queued_count"`
	CompletedCount int               `json:"completed_count"`
	Builds         []VanadisBuildDTO `json:"builds"`
	Error          string            `json:"error,omitempty"`
}

// BuildDetailAPIResponse represents the response payload for inspecting a single Vanadis build and its chunks.
type BuildDetailAPIResponse struct {
	Status string            `json:"status"`
	Build  *VanadisBuildDTO  `json:"build,omitempty"`
	Chunks []VanadisChunkDTO `json:"chunks"`
	Entry  *VanadisBuildDTO  `json:"entry,omitempty"`
	Error  string            `json:"error,omitempty"`
}

// DocketTaskSummaryDTO mirrors docket-mcp's TaskSummary.
type DocketTaskSummaryDTO struct {
	ID        int64   `json:"id"`
	Title     string  `json:"title"`
	Status    string  `json:"status"`
	ParentID  *int64  `json:"parent_id,omitempty"`
	BlockedBy []int64 `json:"blocked_by,omitempty"`
	Blocks    []int64 `json:"blocks,omitempty"`
}

// DocketScoredTaskDTO mirrors docket-mcp's ScoredTaskSummary.
type DocketScoredTaskDTO struct {
	ID       int64   `json:"id"`
	Title    string  `json:"title"`
	Status   string  `json:"status"`
	ParentID *int64  `json:"parent_id,omitempty"`
	Score    float64 `json:"score"`
}

// DocketTaskCommentDTO mirrors docket-mcp's TaskComment.
type DocketTaskCommentDTO struct {
	ID        int64     `json:"id"`
	Author    string    `json:"author,omitempty"`
	Comment   string    `json:"comment"`
	CreatedAt time.Time `json:"created_at"`
}

// DocketTaskDTO mirrors docket-mcp's HydratedTask.
type DocketTaskDTO struct {
	ID          int64                  `json:"id"`
	Title       string                 `json:"title"`
	Status      string                 `json:"status"`
	Description string                 `json:"description,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	ClosedAt    *time.Time             `json:"closed_at,omitempty"`
	Parent      *DocketTaskSummaryDTO  `json:"parent,omitempty"`
	Subtasks    []DocketTaskSummaryDTO `json:"subtasks,omitempty"`
	Blocks      []DocketTaskSummaryDTO `json:"blocks,omitempty"`
	BlockedBy   []DocketTaskSummaryDTO `json:"blocked_by,omitempty"`
	Related     []DocketTaskSummaryDTO `json:"related,omitempty"`
	Comments    []DocketTaskCommentDTO `json:"comments,omitempty"`
}

// DocketTasksAPIResponse is the console envelope for GET /api/docket/tasks.
type DocketTasksAPIResponse struct {
	Status string                 `json:"status"`
	Tasks  []DocketTaskSummaryDTO `json:"tasks"`
	Error  string                 `json:"error,omitempty"`
}

// UnmarshalJSON decodes docket-mcp's bare task-summary array into the envelope's Tasks.
func (d *DocketTasksAPIResponse) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &d.Tasks)
}

// DocketSearchAPIResponse is the console envelope for GET /api/docket/search.
type DocketSearchAPIResponse struct {
	Status  string                `json:"status"`
	Results []DocketScoredTaskDTO `json:"results"`
	Error   string                `json:"error,omitempty"`
}

// UnmarshalJSON decodes docket-mcp's bare scored-hit array into the envelope's Results.
func (d *DocketSearchAPIResponse) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &d.Results)
}

// DocketTaskDetailAPIResponse is the console envelope for GET /api/docket/tasks/{id}.
type DocketTaskDetailAPIResponse struct {
	Status string         `json:"status"`
	Task   *DocketTaskDTO `json:"task,omitempty"`
	Error  string         `json:"error,omitempty"`
}
