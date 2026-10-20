package runner

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type DaemonState string

const (
	StateStarting     DaemonState = "STARTING"
	StateReady        DaemonState = "READY"
	StateExecuting    DaemonState = "EXECUTING"
	StateYieldWaiting DaemonState = "YIELD_WAITING"
	StateClosed       DaemonState = "CLOSED"
)

type DaemonConfig struct {
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

type TurnResult struct {
	ConversationID string
	Response       string
	Stderr         string
	Usage          AgyUsage
	ActiveTasks    []TaskMetadata
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

// BuildDaemonArgs constructs the CLI arguments for launching an agy streaming daemon process.
func BuildDaemonArgs(cfg DaemonConfig) []string {
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

// extractSubagentID extracts a subagent conversation ID from invoke_subagent tool output.
func extractSubagentID(toolOut string) string {
	trimmed := strings.TrimSpace(toolOut)
	if trimmed == "" {
		return ""
	}
	type itemJSON struct {
		ID    string `json:"conversationId"`
		AltID string `json:"conversation_id"`
	}
	type payload struct {
		itemJSON
		Subagents []itemJSON `json:"subagents"`
	}
	getID := func(i itemJSON) string {
		if s := strings.TrimSpace(i.ID); s != "" {
			return s
		}
		return strings.TrimSpace(i.AltID)
	}
	decodeAt := func(s string) string {
		var p payload
		if err := json.NewDecoder(strings.NewReader(s)).Decode(&p); err == nil {
			if id := getID(p.itemJSON); id != "" {
				return id
			}
			for _, sub := range p.Subagents {
				if id := getID(sub); id != "" {
					return id
				}
			}
		}
		var list []itemJSON
		if err := json.NewDecoder(strings.NewReader(s)).Decode(&list); err == nil {
			for _, sub := range list {
				if id := getID(sub); id != "" {
					return id
				}
			}
		}
		return ""
	}
	if id := decodeAt(trimmed); id != "" {
		return id
	}
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] == '{' || trimmed[i] == '[' {
			if id := decodeAt(trimmed[i:]); id != "" {
				return id
			}
		}
	}
	return ""
}
