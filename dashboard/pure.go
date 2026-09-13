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

// BuildFactsUpstreamURL validates the base URL and constructs the target upstream
// URL with whitelisted, normalized, and clamped query parameters.
func BuildFactsUpstreamURL(brainBaseURL string, inQuery url.Values) (targetURL string, limit, offset int, err error) {
	cleanBase := strings.TrimRight(strings.TrimSpace(brainBaseURL), "/")
	if cleanBase == "" || strings.ContainsAny(cleanBase, "\x7f\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\r\n") {
		return "", 0, 0, fmt.Errorf("invalid upstream URL configuration: %q", brainBaseURL)
	}

	target, err := url.Parse(cleanBase + "/facts")
	if err != nil {
		return "", 0, 0, err
	}

	outQuery := target.Query()
	if inQuery != nil {
		if lStr := inQuery.Get("limit"); lStr != "" {
			if n, err := strconv.Atoi(lStr); err == nil && n > 0 {
				limit = n
				outQuery.Set("limit", strconv.Itoa(limit))
			}
		}

		if oStr := inQuery.Get("offset"); oStr != "" {
			if n, err := strconv.Atoi(oStr); err == nil && n >= 0 {
				offset = n
				if offset > 0 {
					outQuery.Set("offset", strconv.Itoa(offset))
				}
			}
		}

		if cat := strings.TrimSpace(inQuery.Get("category")); cat != "" {
			outQuery.Set("category", cat)
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
func BuildSchedulesUpstreamURL(brainBaseURL string) (string, error) {
	cleanBase := strings.TrimRight(strings.TrimSpace(brainBaseURL), "/")
	if cleanBase == "" || strings.ContainsAny(cleanBase, "\x7f\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\r\n") {
		return "", fmt.Errorf("invalid upstream URL configuration: %q", brainBaseURL)
	}

	target, err := url.Parse(cleanBase + "/schedules")
	if err != nil {
		return "", err
	}
	return target.String(), nil
}

// BuildScheduleRunsUpstreamURL validates the base URL and constructs the upstream
// /schedules/runs URL with sanitized limit, offset, schedule_id, and status parameters.
func BuildScheduleRunsUpstreamURL(brainBaseURL string, inQuery url.Values) (targetURL string, limit, offset int, err error) {
	cleanBase := strings.TrimRight(strings.TrimSpace(brainBaseURL), "/")
	if cleanBase == "" || strings.ContainsAny(cleanBase, "\x7f\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\r\n") {
		return "", 0, 0, fmt.Errorf("invalid upstream URL configuration: %q", brainBaseURL)
	}

	target, err := url.Parse(cleanBase + "/schedules/runs")
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

// IsCoreAerialContainer determines whether a container is part of the core Aerial stack.
func IsCoreAerialContainer(c DockerContainerJSON) bool {
	svcName := ""
	if c.Labels != nil {
		svcName = c.Labels["com.docker.compose.service"]
	}
	if svcName == "" && len(c.Names) > 0 {
		name := strings.TrimPrefix(c.Names[0], "/")
		svcName = strings.TrimPrefix(name, "aerial-")
	}
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
		"scheduler-mcp",
		"discord-mcp",
		"docker-mcp",
		"github-mcp",
		"ollama",
		"agentsview",
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
		isAerial := false
		var svcName string

		if proj, ok := c.Labels["com.docker.compose.project"]; ok && proj == "aerial" {
			isAerial = true
			svcName = c.Labels["com.docker.compose.service"]
		} else if len(c.Names) > 0 {
			name := strings.TrimPrefix(c.Names[0], "/")
			if strings.HasPrefix(name, "aerial-") {
				isAerial = true
				svcName = strings.TrimPrefix(name, "aerial-")
			}
		}

		if !isAerial || svcName == "" || seen[svcName] {
			continue
		}
		seen[svcName] = true

		status := "completed"
		conclusion := "success"
		if c.Health != nil && c.Health.Status == "starting" {
			status = "active"
			conclusion = ""
		} else if c.State != "running" || (c.Health != nil && c.Health.Status == "unhealthy") {
			status = "failed"
			conclusion = "failure"
		}

		durStr := "0s"
		if c.Created > 0 {
			createdAt := time.Unix(c.Created, 0).UTC()
			durSec := int(now.Sub(createdAt).Seconds())
			if durSec > 0 {
				durStr = FormatUptimeString(durSec)
			}
		}

		chips = append(chips, MatrixJobChip{
			Name:       svcName,
			Status:     status,
			Conclusion: conclusion,
			Duration:   durStr,
		})
	}
	return chips
}

// GetContainerCommit extracts and truncates the 7-character commit SHA from container labels.
func GetContainerCommit(containers []DockerContainerJSON) string {
	for _, c := range containers {
		if !IsCoreAerialContainer(c) || c.Labels == nil {
			continue
		}
		if rev, ok := c.Labels["org.opencontainers.image.revision"]; ok && rev != "" {
			if len(rev) > 7 {
				return rev[:7]
			}
			return rev
		}
		if rev, ok := c.Labels["aerial.commit_sha"]; ok && rev != "" {
			if len(rev) > 7 {
				return rev[:7]
			}
			return rev
		}
		if rev, ok := c.Labels["vcs-ref"]; ok && rev != "" {
			if len(rev) > 7 {
				return rev[:7]
			}
			return rev
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

// SanitizeEnvVars redacts sensitive credentials from environment variable strings.
func SanitizeEnvVars(envVars []string) []string {
	cleaned := make([]string, 0, len(envVars))
	for _, env := range envVars {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.ToUpper(parts[0])
		isSensitive := false
		for _, sensitive := range sensitiveKeys {
			if strings.Contains(key, sensitive) {
				isSensitive = true
				break
			}
		}
		if isSensitive {
			cleaned = append(cleaned, fmt.Sprintf("%s=[REDACTED]", parts[0]))
		} else {
			cleaned = append(cleaned, env)
		}
	}
	return cleaned
}

// Backward-compatible unexported forwarders ensuring all existing test callers in main_test.go compile and pass unchanged.
func formatUptimeString(sec int) string {
	return FormatUptimeString(sec)
}

func isCoreAerialContainer(c DockerContainerJSON) bool {
	return IsCoreAerialContainer(c)
}

func getContainerCommit(containers []DockerContainerJSON) string {
	return GetContainerCommit(containers)
}

func extractServiceNameFromJobName(jobName string) string {
	return ExtractServiceNameFromJobName(jobName)
}

func parseMatrixJobChips(jobs []GitHubJob) []MatrixJobChip {
	return ParseMatrixJobChips(jobs, time.Now().UTC())
}

func buildContainerChips(rawContainers []DockerContainerJSON) []MatrixJobChip {
	return BuildContainerChips(rawContainers, time.Now().UTC())
}

func getMimeType(filename string) string {
	return GetMimeType(filename)
}
