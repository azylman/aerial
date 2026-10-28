package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/sanitizer"
)

// IsScheduleStale reports whether scheduledAt is more overdue than maxOverdue relative to now.
func IsScheduleStale(scheduledAt, now time.Time, maxOverdue time.Duration) bool {
	return maxOverdue > 0 && now.Sub(scheduledAt) > maxOverdue
}

// ShouldPruneRuns reports whether runs should be pruned given the last prune time, interval, and now.
func ShouldPruneRuns(now, lastPrune time.Time, interval time.Duration) bool {
	return interval > 0 && (lastPrune.IsZero() || now.Sub(lastPrune) >= interval)
}

// CalculateNextRun calculates the next UTC execution time for a cron expression in the specified timezone.
func CalculateNextRun(cronExpr, timezone string, from time.Time) (time.Time, error) {
	return CalculateNextCronRun(cronExpr, timezone, from)
}

// BuildHeartbeatPrompt wraps a raw heartbeat prompt with runtime instructions for the LLM.
func BuildHeartbeatPrompt(hb HeartbeatSchedule) string {
	targetID := strings.TrimSpace(hb.TargetID)
	title := strings.TrimSpace(hb.TitlePrefix)
	if title == "" {
		title = "Heartbeat Alert"
	}
	return fmt.Sprintf(`<\HEARTBEAT_INSTRUCTIONS>
Your direct text response in this turn is suppressed (silent: true) and will NOT be delivered to Discord.
Target Discord Channel ID for Escalations: %s

1. Evaluate the monitoring check below using your read/query tools. Track state across turns in this persistent heartbeat session (prior turns are already present in your conversation context; if none appear, this is Turn 1—do NOT scan /data or filesystem transcripts on disk).
2. High-SNR Silence Bias & Edge-Triggered Gating (Default to Quiet):
   - Keep the signal-to-noise ratio high: if findings are nominal, borderline, transient, or ambiguous, err strictly on the side of silence—state a brief internal log note and end your turn without calling /prompt.
   - Compare against your prior turns in this heartbeat session and NEVER re-escalate an unchanged ongoing condition. Only escalate on a meaningful state transition (a net-new actionable issue, a significant severity spike, or an explicit recovery when prior alerts warrant closure).
3. Escalation State (User Notification Required): If and only if a clear, high-signal state transition requires user attention, send an HTTP POST request to http://eri.aerial/prompt using channel_id %q. You MUST replace <INSERT_ACTUAL_FINDINGS_AND_METRICS_HERE> with your concrete diagnostic findings, numbers, and context (NEVER send the literal placeholder). Keep the heredoc delimiter single-quoted (<<'EOF') so bash never expands markdown backticks, quotes, or $variables as shell commands:
   jq -Rs --arg channel_id %q --arg title %q '{channel_id: $channel_id, title: $title, prompt: (rtrimstr("\n") + "\n\nSummarize the situation for the user and propose concrete follow-up actions that you (Aerial) can execute upon their approval. Do NOT re-invoke /prompt.")}' <<'EOF' | curl -sS -f -X POST http://eri.aerial/prompt -H "Content-Type: application/json" -d @-
<INSERT_ACTUAL_FINDINGS_AND_METRICS_HERE>
EOF
   - Delivery & Retry Policy: Retry the POST if curl fails (non-zero exit code, network error, or non-2xx HTTP status), but NEVER send a second POST once Eri returns a successful HTTP 2xx response (e.g. "status":"accepted").
<\/HEARTBEAT_INSTRUCTIONS>

%s`, targetID, targetID, targetID, title, strings.TrimSpace(hb.Prompt))
}

// NormalizeEffort validates effort tiers, defaulting empty to "low".
func NormalizeEffort(effort string) (string, error) {
	switch eff := strings.ToLower(strings.TrimSpace(effort)); eff {
	case "":
		return "low", nil
	case "low", "medium", "high":
		return eff, nil
	default:
		return "", fmt.Errorf("invalid effort tier %q: expected 'low', 'medium', or 'high'", effort)
	}
}

// IsNumericSnowflake reports whether id is a non-empty string consisting only of ASCII digits [0-9].
func IsNumericSnowflake(id string) bool {
	id = strings.TrimSpace(id)
	if id == "" {
		return false
	}
	for i := 0; i < len(id); i++ {
		if id[i] < '0' || id[i] > '9' {
			return false
		}
	}
	return true
}

var (
	reVanadisBuild    = regexp.MustCompile(`^build-[a-z0-9-]+$`)
	reVanadisChunk    = regexp.MustCompile(`^.+?-chunk-[0-9]+$`)
	reAllDigits       = regexp.MustCompile(`^[0-9]+$`)
	reHexSHA          = regexp.MustCompile(`^[0-9a-fA-F]{40}$`)
	reNomadJob        = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)
	managedEventRepos = map[string]bool{
		"azylman/aerial":          true,
		"azylman/aerial-config":   true,
		"azylman/aerial-sidecars": true,
		"azylman/mirrormere":      true,
	}
)

func validateVanadisBuild(target, eventType, normEventType string) (string, error) {
	if normEventType != "completed" && normEventType != "failed" {
		return "", fmt.Errorf("invalid event_type %q for vanadis_build: expected 'completed' or 'failed'", eventType)
	}
	if !reVanadisBuild.MatchString(target) || strings.Contains(target, "-chunk-") {
		return "", fmt.Errorf("invalid target %q for vanadis_build: must match '^build-[a-z0-9-]+$' and not contain '-chunk-'", target)
	}
	return target, nil
}

func validateVanadisChunk(target, eventType, normEventType string) (string, error) {
	if normEventType != "merged" && normEventType != "failed" {
		return "", fmt.Errorf("invalid event_type %q for vanadis_chunk: expected 'merged' or 'failed'", eventType)
	}
	if !reVanadisChunk.MatchString(target) {
		return "", fmt.Errorf("invalid target %q for vanadis_chunk: must match '^.+?-chunk-[0-9]+$'", target)
	}
	return target, nil
}

func validateHangar(target, eventType, normEventType string) (string, error) {
	if normEventType != "sync_success" && normEventType != "sync_failed" {
		return "", fmt.Errorf("invalid event_type %q for hangar: expected 'sync_success' or 'sync_failed'", eventType)
	}
	repo := target
	if !strings.Contains(repo, "/") {
		repo = "azylman/" + repo
	}
	if !managedEventRepos[repo] {
		return "", fmt.Errorf("invalid target %q for hangar: repo %q not in managed allowlist", target, repo)
	}
	return repo, nil
}

func validateGithub(target, eventType, normEventType string) (string, error) {
	if normEventType != "pr_merged" && normEventType != "ci_passed" && normEventType != "ci_failed" {
		return "", fmt.Errorf("invalid event_type %q for github: expected 'pr_merged', 'ci_passed', or 'ci_failed'", eventType)
	}
	repoPart, branchPart, hasBranch := strings.Cut(target, ":")
	if !strings.Contains(repoPart, "/") {
		repoPart = "azylman/" + repoPart
	}
	if !managedEventRepos[repoPart] {
		return "", fmt.Errorf("invalid target %q for github: repo %q not in managed allowlist", target, repoPart)
	}
	if !hasBranch {
		return repoPart, nil
	}

	branchPart = strings.TrimPrefix(branchPart, "refs/heads/")
	branchPart = strings.TrimSpace(branchPart)
	if branchPart == "" {
		return "", fmt.Errorf("invalid target %q for github: branch cannot be empty", target)
	}
	if strings.HasPrefix(branchPart, "#") || reAllDigits.MatchString(branchPart) {
		return "", fmt.Errorf("invalid target %q for github: PR numbers are not allowed as branch target", target)
	}
	if reHexSHA.MatchString(branchPart) {
		return "", fmt.Errorf("invalid target %q for github: 40-character commit SHAs are not allowed as branch target", target)
	}
	return repoPart + ":" + branchPart, nil
}

func validateNomad(target, eventType, normEventType string) (string, error) {
	if normEventType != "deploy_success" && normEventType != "deploy_failed" {
		return "", fmt.Errorf("invalid event_type %q for nomad: expected 'deploy_success' or 'deploy_failed'", eventType)
	}
	target = strings.ToLower(target)
	if !reNomadJob.MatchString(target) {
		return "", fmt.Errorf("invalid target %q for nomad: must match '^[a-z0-9][a-z0-9-]*$'", target)
	}
	return target, nil
}

// FormatEventTriggerPrompt combines an optional context header with an event trigger prompt.
func FormatEventTriggerPrompt(contextHeader, prompt string) string {
	header := strings.TrimSpace(contextHeader)
	body := strings.TrimSpace(prompt)
	if header == "" {
		return body
	}
	if body == "" {
		return header
	}
	return header + "\n\n" + body
}

// ValidateEventWatchSpec validates and normalizes event trigger parameters.
func ValidateEventWatchSpec(source, target, eventType string, extraSources ...string) (normSource, normTarget, normEventType string, err error) {
	normSource = strings.ToLower(strings.TrimSpace(source))
	normEventType = strings.ToLower(strings.TrimSpace(eventType))
	normTarget = strings.TrimSpace(target)

	if normTarget == "" {
		return "", "", "", fmt.Errorf("target cannot be empty")
	}

	switch normSource {
	case "vanadis_build":
		normTarget, err = validateVanadisBuild(normTarget, eventType, normEventType)
	case "vanadis_chunk":
		normTarget, err = validateVanadisChunk(normTarget, eventType, normEventType)
	case "hangar":
		normTarget, err = validateHangar(normTarget, eventType, normEventType)
	case "github":
		normTarget, err = validateGithub(normTarget, eventType, normEventType)
	case "nomad":
		normTarget, err = validateNomad(normTarget, eventType, normEventType)
	default:
		allowedExtra := false
		for _, extra := range extraSources {
			if normSource != "" && normSource == strings.ToLower(strings.TrimSpace(extra)) {
				allowedExtra = true
				break
			}
		}
		if !allowedExtra {
			return "", "", "", fmt.Errorf("invalid source %q: expected 'vanadis_build', 'vanadis_chunk', 'hangar', 'github', or 'nomad'", source)
		}
		if normEventType == "" {
			return "", "", "", fmt.Errorf("event_type cannot be empty for source %q", normSource)
		}
	}

	if err != nil {
		return "", "", "", err
	}
	return normSource, normTarget, normEventType, nil
}

var cronMonthNames = map[int]string{
	1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
	7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

var cronDayNames = map[int]string{
	0: "Sunday", 1: "Monday", 2: "Tuesday", 3: "Wednesday", 4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday",
}

var cronDayShortNames = map[int]string{
	0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun",
}

// Ordinal formats an integer into its English ordinal string (e.g., 1st, 2nd, 3rd, 4th, 11th, 111th).
func Ordinal(n int) string {
	abs := n
	if abs < 0 {
		abs = -abs
	}
	rem100 := abs % 100
	if rem100 >= 11 && rem100 <= 13 {
		return fmt.Sprintf("%dth", n)
	}
	switch abs % 10 {
	case 1:
		return fmt.Sprintf("%dst", n)
	case 2:
		return fmt.Sprintf("%dnd", n)
	case 3:
		return fmt.Sprintf("%drd", n)
	default:
		return fmt.Sprintf("%dth", n)
	}
}

func parseCronMacro(expr string) (string, bool) {
	switch strings.ToLower(expr) {
	case "@yearly", "@annually":
		return "Every year on Jan 1st at 00:00", true
	case "@monthly":
		return "1st of every month at 00:00", true
	case "@weekly":
		return "Every week on Sunday at 00:00", true
	case "@daily", "@midnight":
		return "Every day at 00:00", true
	case "@hourly":
		return "Every hour", true
	default:
		return "", false
	}
}

func parseCronStepMinutesOrHours(minStr, hourStr, domStr, monStr, dowStr string) (string, bool) {
	if minStr == "*" && hourStr == "*" && domStr == "*" && monStr == "*" && dowStr == "*" {
		return "Every minute", true
	}
	if strings.HasPrefix(minStr, "*/") && hourStr == "*" && domStr == "*" && monStr == "*" && dowStr == "*" {
		return fmt.Sprintf("Every %s minutes", strings.TrimPrefix(minStr, "*/")), true
	}
	if minStr == "0" && strings.HasPrefix(hourStr, "*/") && domStr == "*" && monStr == "*" && dowStr == "*" {
		return fmt.Sprintf("Every %s hours", strings.TrimPrefix(hourStr, "*/")), true
	}
	return "", false
}

func formatCronDayOfWeek(dowStr, timeStr string) string {
	switch strings.ToUpper(dowStr) {
	case "1-5", "MON-FRI":
		return fmt.Sprintf("Weekdays (Mon–Fri) at %s", timeStr)
	case "0,6", "6,0", "SAT,SUN", "SUN,SAT":
		return fmt.Sprintf("Weekends (Sat–Sun) at %s", timeStr)
	}
	if dowNum, err := strconv.Atoi(dowStr); err == nil && dowNum >= 0 && dowNum <= 7 {
		return fmt.Sprintf("Every %s at %s", cronDayNames[dowNum], timeStr)
	}
	var names []string
	for _, p := range strings.Split(dowStr, ",") {
		pTrim := strings.TrimSpace(p)
		if dNum, err := strconv.Atoi(pTrim); err == nil && dNum >= 0 && dNum <= 7 {
			names = append(names, cronDayShortNames[dNum])
		} else {
			names = append(names, pTrim)
		}
	}
	if len(names) > 0 {
		return fmt.Sprintf("%s at %s", strings.Join(names, ", "), timeStr)
	}
	return ""
}

func formatCronDayAndMonth(domStr, monStr, timeStr string) string {
	if monStr == "*" {
		if domNum, err := strconv.Atoi(domStr); err == nil && domNum >= 1 && domNum <= 31 {
			return fmt.Sprintf("%s of every month at %s", Ordinal(domNum), timeStr)
		}
	} else {
		domNum, errDom := strconv.Atoi(domStr)
		monNum, errMon := strconv.Atoi(monStr)
		if errDom == nil && errMon == nil && monNum >= 1 && monNum <= 12 {
			return fmt.Sprintf("Every year on %s %s at %s", cronMonthNames[monNum], Ordinal(domNum), timeStr)
		}
	}
	return ""
}

// FormatCronDescription converts a standard 5-field cron expression or descriptor into human-readable English.
func FormatCronDescription(cronExpr string) string {
	expr := strings.TrimSpace(cronExpr)
	if expr == "" {
		return ""
	}
	if desc, ok := parseCronMacro(expr); ok {
		return desc
	}

	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return expr
	}

	minStr, hourStr, domStr, monStr, dowStr := fields[0], fields[1], fields[2], fields[3], fields[4]
	if desc, ok := parseCronStepMinutesOrHours(minStr, hourStr, domStr, monStr, dowStr); ok {
		return desc
	}

	m, minErr := strconv.Atoi(minStr)
	h, hourErr := strconv.Atoi(hourStr)
	if minErr != nil || hourErr != nil {
		return expr
	}

	timeStr := fmt.Sprintf("%02d:%02d", h, m)
	if domStr == "*" && monStr == "*" && dowStr == "*" {
		return fmt.Sprintf("Every day at %s", timeStr)
	}
	if domStr == "*" && monStr == "*" && dowStr != "*" {
		if desc := formatCronDayOfWeek(dowStr, timeStr); desc != "" {
			return desc
		}
	}
	if domStr != "*" && dowStr == "*" {
		if desc := formatCronDayAndMonth(domStr, monStr, timeStr); desc != "" {
			return desc
		}
	}
	return fmt.Sprintf("At %s (cron: %s)", timeStr, expr)
}

// SanitizeScheduleText redacts GitHub PATs, Bearer tokens, and secret key assignments.
func SanitizeScheduleText(s string) string {
	return sanitizer.SanitizeString(s)
}

type CronScheduleWithDesc struct {
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
	Effort          string    `json:"effort,omitempty"`
}

type HeartbeatScheduleWithDesc struct {
	ID              string    `json:"id"`
	TargetID        string    `json:"target_id,omitempty"`
	ChannelID       string    `json:"channel_id"`
	TitlePrefix     string    `json:"title_prefix"`
	CronExpr        string    `json:"cron_expr"`
	CronDescription string    `json:"cron_description"`
	Prompt          string    `json:"prompt"`
	Timezone        string    `json:"timezone"`
	Effort          string    `json:"effort,omitempty"`
	NextRunAt       time.Time `json:"next_run_at"`
	Enabled         bool      `json:"enabled"`
	CreatedAt       time.Time `json:"created_at"`
}

type OneShotScheduleJSON struct {
	ID        string    `json:"id"`
	ThreadID  string    `json:"thread_id"`
	Prompt    string    `json:"prompt"`
	RunAt     time.Time `json:"run_at"`
	CreatedAt time.Time `json:"created_at"`
	Effort    string    `json:"effort,omitempty"`
}

type SchedulesResponse struct {
	Status     string                      `json:"status"`
	SystemTime time.Time                   `json:"system_time"`
	Summary    ScheduleSummaryMetrics      `json:"summary"`
	Crons      []CronScheduleWithDesc      `json:"crons"`
	Heartbeats []HeartbeatScheduleWithDesc `json:"heartbeats"`
	OneShots   []OneShotScheduleJSON       `json:"one_shots"`
}

type ScheduleRunsResponse struct {
	Status string            `json:"status"`
	Total  int               `json:"total"`
	Limit  int               `json:"limit"`
	Offset int               `json:"offset"`
	Runs   []ScheduleRunJSON `json:"runs"`
}

type CreateOneShotRequest struct {
	ID       string    `json:"id"`
	ThreadID string    `json:"thread_id"`
	Prompt   string    `json:"prompt"`
	RunAt    time.Time `json:"run_at"`
	Effort   string    `json:"effort,omitempty"`
}

// FormatCronSchedulesWithDesc converts raw CronSchedule records into serialized CronScheduleWithDesc items.
func FormatCronSchedulesWithDesc(raw []CronSchedule) []CronScheduleWithDesc {
	crons := make([]CronScheduleWithDesc, 0, len(raw))
	for _, c := range raw {
		crons = append(crons, CronScheduleWithDesc{
			ID:              c.ID,
			TargetID:        c.TargetID,
			ChannelID:       c.TargetID,
			TitlePrefix:     SanitizeScheduleText(c.TitlePrefix),
			CronExpr:        c.CronExpr,
			CronDescription: FormatCronDescription(c.CronExpr),
			Prompt:          SanitizeScheduleText(c.Prompt),
			Timezone:        c.Timezone,
			NextRunAt:       c.NextRunAt,
			Enabled:         c.Enabled,
			CreatedAt:       c.CreatedAt,
			Effort:          c.Effort,
		})
	}
	return crons
}

// FormatHeartbeatSchedulesWithDesc converts raw HeartbeatSchedule records into serialized HeartbeatScheduleWithDesc items.
func FormatHeartbeatSchedulesWithDesc(raw []HeartbeatSchedule) []HeartbeatScheduleWithDesc {
	heartbeats := make([]HeartbeatScheduleWithDesc, 0, len(raw))
	for _, h := range raw {
		heartbeats = append(heartbeats, HeartbeatScheduleWithDesc{
			ID:              h.ID,
			TargetID:        h.TargetID,
			ChannelID:       h.TargetID,
			TitlePrefix:     SanitizeScheduleText(h.TitlePrefix),
			CronExpr:        h.CronExpr,
			CronDescription: FormatCronDescription(h.CronExpr),
			Prompt:          SanitizeScheduleText(h.Prompt),
			Timezone:        h.Timezone,
			Effort:          h.Effort,
			NextRunAt:       h.NextRunAt,
			Enabled:         h.Enabled,
			CreatedAt:       h.CreatedAt,
		})
	}
	return heartbeats
}

// FormatOneShotSchedulesJSON converts raw OneShotSchedule records into serialized OneShotScheduleJSON items.
func FormatOneShotSchedulesJSON(raw []OneShotSchedule) []OneShotScheduleJSON {
	oneShots := make([]OneShotScheduleJSON, 0, len(raw))
	for _, s := range raw {
		oneShots = append(oneShots, OneShotScheduleJSON{
			ID:        s.ID,
			ThreadID:  s.ThreadID,
			Prompt:    SanitizeScheduleText(s.Prompt),
			RunAt:     s.RunAt,
			CreatedAt: s.CreatedAt,
			Effort:    s.Effort,
		})
	}
	return oneShots
}
