package runner

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// WatchdogAction represents the action recommended by watchdog evaluation.
type WatchdogAction int

const (
	WatchdogActionNone WatchdogAction = iota
	WatchdogActionKillInactivity
	WatchdogActionKillMaxDuration
)

// AgyArgsInput holds the configuration parameters for building CLI arguments.
type AgyArgsInput struct {
	OutputFormat string
	Model        string
	MaxDuration  time.Duration
	SessionID    string
	Prompt       string
	WorkerMode   bool
}

// BuildAgyArgs constructs command-line arguments for executing the agy binary.
func BuildAgyArgs(input AgyArgsInput) []string {
	outputFmt := strings.TrimSpace(input.OutputFormat)
	if outputFmt == "" {
		outputFmt = "stream-json"
	}

	args := []string{"--dangerously-skip-permissions"}
	if input.WorkerMode {
		args = append(args, "--input-format", "stream-json")
	}
	args = append(args, "--output-format", outputFmt)

	if model := strings.TrimSpace(input.Model); model != "" {
		args = append(args, "--model", model)
	}

	if input.MaxDuration > 0 {
		if input.MaxDuration >= time.Minute {
			args = append(args, "--print-timeout", fmt.Sprintf("%dm", int(input.MaxDuration.Minutes())))
		} else {
			args = append(args, "--print-timeout", fmt.Sprintf("%ds", int(input.MaxDuration.Seconds())))
		}
	}

	if !input.WorkerMode {
		if sessionID := strings.TrimSpace(input.SessionID); sessionID != "" {
			args = append(args, "--conversation", sessionID)
		}
		if input.Prompt != "" {
			args = append(args, "-p", input.Prompt)
		}
	}

	return args
}

// AgyEnvInput holds environment configuration parameters.
type AgyEnvInput struct {
	BaseEnv  []string
	HomeDir  string
	APIKey   string
	TargetID string
	ExtraEnv []string
}

// BuildAgyEnv constructs the process environment slice for agy execution.
func BuildAgyEnv(input AgyEnvInput) []string {
	var cmdEnv []string
	if len(input.BaseEnv) > 0 {
		cmdEnv = append(cmdEnv, input.BaseEnv...)
	}

	cmdEnv = append(cmdEnv,
		"GIT_TERMINAL_PROMPT=0",
		"AGY_LOG_LEVEL=debug",
		"ANTIGRAVITY_LOG_LEVEL=debug",
	)

	if home := strings.TrimSpace(input.HomeDir); home != "" {
		cmdEnv = append(cmdEnv,
			"HOME="+home,
			"USERPROFILE="+home,
		)
	}

	if apiKey := strings.TrimSpace(input.APIKey); apiKey != "" {
		cmdEnv = append(cmdEnv,
			"GEMINI_API_KEY="+apiKey,
			"ANTIGRAVITY_API_KEY="+apiKey,
			"GOOGLE_GENAI_API_KEY="+apiKey,
		)
	}

	if target := strings.TrimSpace(input.TargetID); target != "" {
		cmdEnv = append(cmdEnv, "AERIAL_TARGET_ID="+target)
	}

	if len(input.ExtraEnv) > 0 {
		cmdEnv = append(cmdEnv, input.ExtraEnv...)
	}

	return cmdEnv
}

// WatchdogStatusInput holds timing state for watchdog threshold evaluation.
type WatchdogStatusInput struct {
	Now                  time.Time
	StartTime            time.Time
	LastActivityTime     time.Time
	LatestDiskActivity   time.Time
	LastSeenDiskActivity time.Time
	InactivityTimeout    time.Duration
	MaxDuration          time.Duration
}

// WatchdogDecision represents the decision produced by EvaluateWatchdogStatus.
type WatchdogDecision struct {
	Action              WatchdogAction
	Reason              string
	NewLastSeenDisk     time.Time
	UpdatedActivityTime time.Time
}

// EvaluateWatchdogStatus evaluates elapsed durations against inactivity and max duration thresholds.
func EvaluateWatchdogStatus(input WatchdogStatusInput) WatchdogDecision {
	now := input.Now
	if now.IsZero() {
		now = time.Now()
	}

	inactivityTimeout := input.InactivityTimeout
	if inactivityTimeout <= 0 {
		inactivityTimeout = 5 * time.Minute
	}

	maxDuration := input.MaxDuration
	if maxDuration <= 0 {
		maxDuration = 60 * time.Minute
	}

	decision := WatchdogDecision{
		Action:              WatchdogActionNone,
		NewLastSeenDisk:     input.LastSeenDiskActivity,
		UpdatedActivityTime: input.LastActivityTime,
	}

	effectiveActivity := input.LastActivityTime
	if input.LatestDiskActivity.After(input.LastSeenDiskActivity) {
		decision.NewLastSeenDisk = input.LatestDiskActivity
		decision.UpdatedActivityTime = now
		effectiveActivity = now
	}

	if now.Sub(effectiveActivity) > inactivityTimeout {
		decision.Action = WatchdogActionKillInactivity
		decision.Reason = fmt.Sprintf("inactivity timeout exceeded (%v without output or transcript update)", inactivityTimeout)
		return decision
	}

	if !input.StartTime.IsZero() && now.Sub(input.StartTime) > maxDuration {
		decision.Action = WatchdogActionKillMaxDuration
		decision.Reason = fmt.Sprintf("max duration exceeded (%v total duration cap)", maxDuration)
		return decision
	}

	return decision
}

// ShouldRotateWorker evaluates whether a persistent worker should be rotated.
func ShouldRotateWorker(turnsUsed, turnBudget int, rssBytes, maxRSSBytes uint64, isDead bool) (bool, string) {
	if isDead {
		return true, "worker is dead"
	}
	if maxRSSBytes > 0 && rssBytes > maxRSSBytes {
		return true, fmt.Sprintf("rss limit exceeded (%d > %d bytes)", rssBytes, maxRSSBytes)
	}
	if turnBudget > 0 && turnsUsed >= turnBudget {
		return true, fmt.Sprintf("turn budget reached (%d >= %d)", turnsUsed, turnBudget)
	}
	return false, ""
}

// ParseInitEvent extracts conversation ID from an NDJSON line containing {"event":"init"}.
func ParseInitEvent(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return "", false
	}

	var ev struct {
		Event          string `json:"event"`
		ConversationID string `json:"conversation_id"`
	}
	if err := json.Unmarshal([]byte(trimmed), &ev); err == nil {
		if ev.Event == "init" && IsValidUUID(ev.ConversationID) {
			return ev.ConversationID, true
		}
	}

	if match := reNDJSONInitSession.FindStringSubmatch(trimmed); len(match) > 1 {
		candidate := strings.TrimSpace(match[1])
		if IsValidUUID(candidate) {
			return candidate, true
		}
	}

	return "", false
}

// IsResultEvent checks if an output line represents a stream-json result event.
func IsResultEvent(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return false
	}

	var ev struct {
		Event string `json:"event"`
	}
	if err := json.Unmarshal([]byte(trimmed), &ev); err == nil {
		return ev.Event == "result"
	}

	normalized := strings.ReplaceAll(trimmed, " ", "")
	return strings.HasPrefix(normalized, `{"event":"result"`)
}

// WriteWorkerTurn serializes and writes a prompt turn to the given io.Writer.
func WriteWorkerTurn(w io.Writer, prompt string) error {
	if w == nil {
		return fmt.Errorf("worker writer is nil: %w", ErrWorkerDead)
	}

	req := streamUserMessage{
		Event: "user",
		Message: streamUserMessageBody{
			Content: prompt,
		},
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal prompt request: %w", err)
	}
	payload = append(payload, '\n')

	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("failed to write prompt to worker stdin: %w", err)
	}
	return nil
}
