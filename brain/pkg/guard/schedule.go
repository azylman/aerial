package guard

import (
	"regexp"
	"strconv"
	"strings"
)

// ScheduleDenyReason explains the CLI schedule tool prohibition and points to persistent scheduler MCP tools.
const ScheduleDenyReason = "The built-in ephemeral CLI 'schedule' tool is strictly prohibited by system invariants. " +
	"Calling 'schedule' produces background task chatter that triggers agy's print-mode drain, " +
	"killing active execution. ALWAYS use persistent scheduler MCP tools instead: " +
	"scheduler_schedule_once, scheduler_schedule_recurring, scheduler_list_schedules, scheduler_cancel_schedule."

// SleepDenyReason explains the foreground sleep prohibition in run_command.
const SleepDenyReason = "Arbitrary sleep/polling commands in run_command are prohibited by system invariants. " +
	"Foreground sleeps burn active turn time and trigger execution timeouts. Please use " +
	"event-driven signaling or persistent scheduler MCP tools instead of sleep loops."

// sleepCommandPattern matches executable sleep invocations like `sleep 10`, `/bin/sleep 5`, `foo && sleep 20`.
var sleepCommandPattern = regexp.MustCompile(`(?:^|[;&|\n]\s*)(?:/bin/|/usr/bin/)?sleep\s+([1-9]\d*)(?:s|m)?(?:\s|$|[;&|])`)

// whileSleepPattern matches while loops executing sleep commands.
var whileSleepPattern = regexp.MustCompile(`while\s+.*?(?:do|;)\s*(?:/bin/|/usr/bin/)?sleep\s+`)

// CheckSchedule evaluates tool calls against scheduling and sleep invariants.
func CheckSchedule(toolName string, args ToolArgs) *Decision {
	normalizedName := strings.ToLower(toolName)
	if idx := strings.LastIndex(normalizedName, ":"); idx != -1 {
		normalizedName = normalizedName[idx+1:]
	}

	if normalizedName == "schedule" {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   ScheduleDenyReason,
		}
	}

	if normalizedName == "run_command" {
		if isSleepCommand(args.CommandLine) {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   SleepDenyReason,
			}
		}
	}

	return &Decision{Decision: DecisionAllow}
}

func isSleepCommand(cmd string) bool {
	trimmed := strings.TrimSpace(cmd)
	if trimmed == "" {
		return false
	}

	// 1. Check for sleep command with duration >= 5 seconds
	if matches := sleepCommandPattern.FindAllStringSubmatch(cmd, -1); len(matches) > 0 {
		for _, m := range matches {
			if len(m) > 1 {
				seconds, err := strconv.Atoi(m[1])
				if err == nil && seconds >= 5 {
					return true
				}
			}
		}
	}

	// 2. Check for while loops executing sleep
	if whileSleepPattern.MatchString(cmd) {
		return true
	}

	return false
}
