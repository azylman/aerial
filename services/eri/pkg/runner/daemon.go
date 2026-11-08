package runner

import (
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
	ResumeSession           bool
	SessionID               string
	ThreadID                string
	InitialTurnCount        int
	Model                   string
	AgyBin                  string
	Cwd                     string
	Env                     []string
	GeminiHomeDir           string
	Timeout                 time.Duration
	TranscriptRescuer       func(convID string, since time.Time) string
	MemoryRetriever         MemoryRetriever
	AmbientContextRetriever AmbientContextRetriever
}

// IsClaude returns true if the configured Engine is "claude" (case-insensitive).
func (c DaemonConfig) IsClaude() bool {
	return strings.EqualFold(strings.TrimSpace(c.Engine), "claude")
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

// BuildDaemonArgs constructs the CLI arguments for launching an agy or claude streaming daemon process.
func BuildDaemonArgs(cfg DaemonConfig) []string {
	if cfg.IsClaude() {
		args := []string{"-p", "--verbose", "--dangerously-skip-permissions", "--input-format", "stream-json", "--output-format", "stream-json", "--include-partial-messages"}
		if cfg.ResumeSession && cfg.SessionID != "" {
			args = append(args, "--resume", cfg.SessionID)
		} else if cfg.SessionID != "" {
			args = append(args, "--session-id", cfg.SessionID)
		}
		if cfg.Model != "" {
			args = append(args, "--model", cfg.Model)
		}
		if cfg.GeminiHomeDir != "" {
			args = append(args, "--mcp-config", filepath.Join(cfg.GeminiHomeDir, ".claude", "mcp_config.json"))
		}
		return args
	}

	args := []string{"--dangerously-skip-permissions", "--input-format", "stream-json", "--output-format", "stream-json"}
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
