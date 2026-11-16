package runner

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

type DaemonState string

const (
	StateStarting  DaemonState = "STARTING"
	StateReady     DaemonState = "READY"
	StateExecuting DaemonState = "EXECUTING"
	StateClosed    DaemonState = "CLOSED"
)

type DaemonConfig struct {
	Engine                  string
	ClaudeBin               string
	CodexBin                string
	ResumeSession           bool
	SessionID               string
	ThreadID                string
	InitialTurnCount        int
	Model                   string
	Effort                  string
	AgyBin                  string
	Cwd                     string
	Env                     []string
	GeminiHomeDir           string
	TurnEnvFile             string
	Timeout                 time.Duration
	TranscriptRescuer       func(convID string, since time.Time) string
	MemoryRetriever         MemoryRetriever
	AmbientContextRetriever AmbientContextRetriever
}

// IsClaude returns true if the configured Engine is "claude" (case-insensitive).
func (c DaemonConfig) IsClaude() bool {
	return strings.EqualFold(strings.TrimSpace(c.Engine), "claude")
}

// IsCodex returns true if the configured Engine is "codex" (case-insensitive).
func (c DaemonConfig) IsCodex() bool {
	return strings.EqualFold(strings.TrimSpace(c.Engine), "codex")
}

func parseModelAndEffort(model, explicitEffort string, allowXHigh bool) (string, string) {
	trimmedModel := strings.TrimSpace(model)
	effort := strings.ToLower(strings.TrimSpace(explicitEffort))
	lowerModel := strings.ToLower(trimmedModel)

	suffixes := []string{"-high", "-medium", "-low"}
	if allowXHigh {
		suffixes = []string{"-xhigh", "-high", "-medium", "-low"}
	}

	for _, suffix := range suffixes {
		if strings.HasSuffix(lowerModel, suffix) && len(trimmedModel) > len(suffix) {
			base := strings.TrimSpace(trimmedModel[:len(trimmedModel)-len(suffix)])
			if base != "" {
				return base, strings.TrimPrefix(suffix, "-")
			}
		}
	}
	return trimmedModel, effort
}

func parseClaudeModelAndEffort(model, explicitEffort string) (string, string) {
	return parseModelAndEffort(model, explicitEffort, false)
}

// TurnError is a turn failure that still consumed tokens: Usage covers the model calls the
// engine completed before the turn failed.
type TurnError struct {
	Err   error
	Usage AgyUsage
}

func (e *TurnError) Error() string { return e.Err.Error() }
func (e *TurnError) Unwrap() error { return e.Err }

// UsageFromError returns the token usage carried by a TurnError in err's chain.
func UsageFromError(err error) (AgyUsage, bool) {
	if te, ok := errors.AsType[*TurnError](err); ok {
		return te.Usage, true
	}
	return AgyUsage{}, false
}

type TurnResult struct {
	ConversationID string
	Response       string
	Stderr         string
	Usage          AgyUsage
	IsYieldTrap    bool
	Duration       time.Duration
}

type streamInputPayload struct {
	Event   string             `json:"event"`
	Message streamInputMessage `json:"message"`
}

type streamInputMessage struct {
	Content string `json:"content"`
}

func buildClaudeDaemonArgs(cfg DaemonConfig) []string {
	args := []string{"-p", "--verbose", "--dangerously-skip-permissions", "--input-format", formatStreamJSON, "--output-format", formatStreamJSON, "--include-partial-messages"}
	if cfg.ResumeSession && cfg.SessionID != "" {
		args = append(args, "--resume", cfg.SessionID)
	} else if cfg.SessionID != "" {
		args = append(args, "--session-id", cfg.SessionID)
	}
	baseModel, effort := parseClaudeModelAndEffort(cfg.Model, cfg.Effort)
	if baseModel != "" {
		args = append(args, "--model", baseModel)
	}
	if effort != "" {
		args = append(args, "--effort", effort)
	}
	if cfg.GeminiHomeDir != "" {
		args = append(args, "--mcp-config", filepath.Join(cfg.GeminiHomeDir, ".claude", "mcp_config.json"))
	}
	return args
}

// BuildDaemonArgs constructs the CLI arguments for launching an agy, claude, or codex streaming daemon process.
func BuildDaemonArgs(cfg DaemonConfig) []string {
	if cfg.IsCodex() {
		return []string{"app-server", "--listen", "stdio://"}
	}
	if cfg.IsClaude() {
		return buildClaudeDaemonArgs(cfg)
	}

	args := []string{"--dangerously-skip-permissions", "--input-format", formatStreamJSON, "--output-format", formatStreamJSON}
	if cfg.SessionID != "" {
		args = append(args, "--conversation", cfg.SessionID)
	}
	if cfg.Model != "" {
		args = append(args, "--model", cfg.Model)
	}
	t := cfg.Timeout
	if t <= 0 {
		t = 60 * time.Minute
	}
	if int(t.Seconds())%60 == 0 {
		args = append(args, "--print-timeout", fmt.Sprintf("%dm", int(t.Minutes())))
	} else {
		args = append(args, "--print-timeout", fmt.Sprintf("%ds", int(t.Seconds())))
	}
	return args
}
