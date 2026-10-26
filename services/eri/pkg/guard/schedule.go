package guard

import (
	"path/filepath"
	"regexp"
	"strings"
)

// MaxSyncWaitMs is the maximum synchronous wait duration (10s) supported by Antigravity's run_command tool.
const MaxSyncWaitMs = 10000

// ScheduleDenyReason explains the CLI schedule tool prohibition and points to persistent scheduler MCP tools.
const ScheduleDenyReason = "The built-in ephemeral CLI 'schedule' tool is strictly prohibited by system invariants " +
	"because it is ephemeral in-memory state lost across restarts. ALWAYS use persistent haro scheduler MCP tools instead: " +
	"schedule_once, schedule_recurring, list_schedules, cancel_schedule, schedule_heartbeat, watch_event."

// SleepDenyReason explains the foreground sleep prohibition in run_command.
const SleepDenyReason = "Arbitrary sleep/polling commands in run_command are prohibited by system invariants. " +
	"When a background command or subagent is running and no parallel work remains, stop calling tools and emit empty text—" +
	"agy natively suspends the turn in place and wakes you reactively via <SYSTEM_MESSAGE> when the task completes."

// TaskPollDenyReason explains the background task polling prohibition.
const TaskPollDenyReason = "Manual background task polling ('manage_task status'/'list' or reading '/.system_generated/tasks/' logs) " +
	"is strictly prohibited by the Zero-Polling Policy. When a background command is running and no parallel work remains, " +
	"stop calling tools and emit empty text—agy natively suspends the turn in place and wakes you reactively via <SYSTEM_MESSAGE> " +
	"as soon as the background task completes."

// SubagentPollDenyReason explains the subagent status polling prohibition.
const SubagentPollDenyReason = "Manual subagent status polling ('manage_subagents list') is strictly prohibited by the Zero-Polling Policy. " +
	"When an in-flight subagent is running and no parallel work remains, stop calling tools and emit empty text—" +
	"agy natively suspends the turn in place and wakes you reactively via <SYSTEM_MESSAGE> when the subagent completes."

// sleepCommandPattern matches executable sleep invocations with duration >= 1 second like `sleep 1`, `sleep 3`, `/bin/sleep 5`, `foo && sleep 20`.
var sleepCommandPattern = regexp.MustCompile(`(?:^|[;&|\n]\s*)(?:/bin/|/usr/bin/)?sleep\s+([1-9]\d*)(?:s|m)?(?:\s|$|[;&|])`)

// whileSleepPattern matches while loops executing sleep commands.
var whileSleepPattern = regexp.MustCompile(`while\s+.*?(?:do|;)\s*(?:/bin/|/usr/bin/)?sleep\s+`)

// pythonSingleQuotePattern matches single-quoted `python -c '...'` or `python3 -c '...'` invocations.
var pythonSingleQuotePattern = regexp.MustCompile(`(?:^|[;&|\n]\s*)(?:/usr/bin/|/usr/local/bin/|/bin/)?python[0-9.]*\s+-c\s+'`)

// CheckSchedule evaluates tool calls against scheduling, sleep, polling, and synchronous wait invariants.
func CheckSchedule(toolName string, args ToolArgs) *Decision {
	switch normalizeToolName(toolName) {
	case "schedule":
		return &Decision{Decision: DecisionDeny, Reason: ScheduleDenyReason}
	case "manage_task":
		if act := args.NormalizedAction(); act == "status" || act == "list" {
			return &Decision{Decision: DecisionDeny, Reason: TaskPollDenyReason}
		}
	case "manage_subagents":
		if args.NormalizedAction() == "list" {
			return &Decision{Decision: DecisionDeny, Reason: SubagentPollDenyReason}
		}
	case "view_file", "read_file":
		if isTaskLogPath(args.FilePath()) {
			return &Decision{Decision: DecisionDeny, Reason: TaskPollDenyReason}
		}
	case "run_command":
		return checkScheduleRunCommand(args)
	}
	return &Decision{Decision: DecisionAllow}
}

func checkScheduleRunCommand(args ToolArgs) *Decision {
	if isSleepCommand(args.CommandLine) {
		return &Decision{Decision: DecisionDeny, Reason: SleepDenyReason}
	}
	if isTaskLogPath(args.CommandLine) {
		return &Decision{Decision: DecisionDeny, Reason: TaskPollDenyReason}
	}

	var overwrite map[string]any
	if rewritten := rewritePythonInlineQuotes(args.CommandLine); rewritten != args.CommandLine {
		overwrite = map[string]any{"CommandLine": rewritten}
	}
	if !args.IsDaemonBool() && args.WaitMs() != MaxSyncWaitMs {
		if overwrite == nil {
			overwrite = make(map[string]any)
		}
		overwrite["WaitMsBeforeAsync"] = MaxSyncWaitMs
	}
	return &Decision{Decision: DecisionAllow, Overwrite: overwrite}
}

func rewritePythonInlineQuotes(cmd string) string {
	if !strings.Contains(cmd, "python") || !strings.Contains(cmd, "-c") || !strings.Contains(cmd, `\"`) {
		return cmd
	}
	locs := pythonSingleQuotePattern.FindAllStringIndex(cmd, -1)
	if len(locs) == 0 {
		return cmd
	}

	rewritten := cmd
	for i := len(locs) - 1; i >= 0; i-- {
		start := locs[i][1]
		end := findClosingSingleQuote(rewritten, start)
		if end > start {
			fixedBody := unescapeBracedQuotes(rewritten[start:end])
			rewritten = rewritten[:start] + fixedBody + rewritten[end:]
		}
	}
	return rewritten
}

func findClosingSingleQuote(s string, start int) int {
	for i := start; i < len(s); i++ {
		if s[i] == '\'' {
			if strings.HasPrefix(s[i:], `'\''`) {
				i += 3
				continue
			}
			return i
		}
	}
	return -1
}

func unescapeBracedQuotes(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	depth := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '{':
			depth++
			b.WriteByte('{')
		case '}':
			if depth > 0 {
				depth--
			}
			b.WriteByte('}')
		case '\\':
			if depth > 0 && i+1 < len(s) && s[i+1] == '"' {
				b.WriteByte('"')
				i++
				continue
			}
			b.WriteByte('\\')
		default:
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

func isTaskLogPath(s string) bool {
	return strings.Contains(filepath.ToSlash(s), "/.system_generated/tasks/")
}

func isSleepCommand(cmd string) bool {
	trimmed := strings.TrimSpace(cmd)
	if trimmed == "" {
		return false
	}
	return sleepCommandPattern.MatchString(cmd) || whileSleepPattern.MatchString(cmd)
}
