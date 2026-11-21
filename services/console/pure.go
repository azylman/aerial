package main

import (
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

// buildDeploymentSteps constructs the standard 5-step Permet HUD deployment pipeline.
func buildDeploymentSteps(s1, s2, s3, s4, s5 string) []DeploymentStep {
	return []DeploymentStep{
		{Name: "Commit Trigger", Icon: "📦", Status: s1},
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

// errInvalidDocketRequest marks client-side request validation failures (bad task ID or
// search parameters) so docket handlers can answer 400 instead of a configuration error.
var errInvalidDocketRequest = errors.New("invalid docket request")

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

// FilterFactsByAgent filters facts to those matching filterAgent.
func FilterFactsByAgent(facts []FactItem, filterAgent string) []FactItem {
	f := NormalizeAgent(filterAgent)
	if f == "" || f == agentFilterAll {
		return facts
	}
	out := make([]FactItem, 0, len(facts))
	for _, item := range facts {
		if MatchAgent(item.Agent, f) {
			out = append(out, item)
		}
	}
	return out
}

// FilterCronsByAgent filters cron schedules to those matching filterAgent.
func FilterCronsByAgent(crons []CronSchedule, filterAgent string) []CronSchedule {
	f := NormalizeAgent(filterAgent)
	if f == "" || f == agentFilterAll {
		return crons
	}
	out := make([]CronSchedule, 0, len(crons))
	for _, item := range crons {
		if MatchAgent(item.Agent, f) {
			out = append(out, item)
		}
	}
	return out
}

// FilterOneShotsByAgent filters one-shot schedules to those matching filterAgent.
func FilterOneShotsByAgent(oneshots []OneShotSchedule, filterAgent string) []OneShotSchedule {
	f := NormalizeAgent(filterAgent)
	if f == "" || f == agentFilterAll {
		return oneshots
	}
	out := make([]OneShotSchedule, 0, len(oneshots))
	for _, item := range oneshots {
		if MatchAgent(item.Agent, f) {
			out = append(out, item)
		}
	}
	return out
}

// FilterScheduleRunsByAgent filters schedule runs to those matching filterAgent.
func FilterScheduleRunsByAgent(runs []ScheduleRun, filterAgent string) []ScheduleRun {
	f := NormalizeAgent(filterAgent)
	if f == "" || f == agentFilterAll {
		return runs
	}
	out := make([]ScheduleRun, 0, len(runs))
	for _, item := range runs {
		if MatchAgent(item.Agent, f) {
			out = append(out, item)
		}
	}
	return out
}

// FilterBuildsByAgent filters Vanadis builds to those matching filterAgent.
func FilterBuildsByAgent(builds []VanadisBuildDTO, filterAgent string) []VanadisBuildDTO {
	f := NormalizeAgent(filterAgent)
	if f == "" || f == agentFilterAll {
		return builds
	}
	out := make([]VanadisBuildDTO, 0, len(builds))
	for _, item := range builds {
		if MatchAgent(item.Agent, f) {
			out = append(out, item)
		}
	}
	return out
}

// FilterActiveTasksByAgent filters active tasks to those matching filterAgent.
func FilterActiveTasksByAgent(tasks []ActiveTaskStatus, filterAgent string) []ActiveTaskStatus {
	f := NormalizeAgent(filterAgent)
	if f == "" || f == agentFilterAll {
		return tasks
	}
	out := make([]ActiveTaskStatus, 0, len(tasks))
	for _, item := range tasks {
		if MatchAgent(item.Agent, f) {
			out = append(out, item)
		}
	}
	return out
}
