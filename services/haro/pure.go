package main

import (
	"fmt"
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
