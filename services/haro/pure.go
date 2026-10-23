package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"
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

1. Evaluate the monitoring check below using your read/query tools. Track state across turns in this persistent heartbeat session.
2. High-SNR Silence Bias & Edge-Triggered Gating (Default to Quiet):
   - Keep the signal-to-noise ratio high: if findings are nominal, borderline, transient, or ambiguous, err strictly on the side of silence—state a brief internal log note and end your turn without calling /prompt.
   - Compare against your prior turns in this heartbeat session and NEVER re-escalate an unchanged ongoing condition. Only escalate on a meaningful state transition (a net-new actionable issue, a significant severity spike, or an explicit recovery when prior alerts warrant closure).
3. Escalation State (User Notification Required): If and only if a clear, high-signal state transition requires user attention, send an HTTP POST request to http://eri.aerial/prompt using channel_id %q. You MUST replace <INSERT_ACTUAL_FINDINGS_AND_METRICS_HERE> with your concrete diagnostic findings, numbers, and context (NEVER send the literal placeholder):
   jq -n --arg channel_id %q --arg title %q --arg findings "<INSERT_ACTUAL_FINDINGS_AND_METRICS_HERE>" '{channel_id: $channel_id, title: $title, prompt: ($findings + "\n\nSummarize the situation for the user and propose concrete follow-up actions that you (Aerial) can execute upon their approval. Do NOT re-invoke /prompt.")}' | curl -sS -X POST http://eri.aerial/prompt -H "Content-Type: application/json" -d @-
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

func boolPtr(b bool) *bool {
	return &b
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

// ValidateEventWatchSpec validates and normalizes event trigger parameters.
func ValidateEventWatchSpec(source, target, eventType string) (normSource, normTarget, normEventType string, err error) {
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
		return "", "", "", fmt.Errorf("invalid source %q: expected 'vanadis_build', 'vanadis_chunk', 'hangar', 'github', or 'nomad'", source)
	}

	if err != nil {
		return "", "", "", err
	}
	return normSource, normTarget, normEventType, nil
}
