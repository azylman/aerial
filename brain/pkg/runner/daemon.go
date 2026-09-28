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
	SessionID     string
	ThreadID      string
	Model         string
	AgyBin        string
	Cwd           string
	Env           []string
	GeminiHomeDir string
	Timeout       time.Duration
}

type TurnResult struct {
	ConversationID string
	Response       string
	Stderr         string
	Usage          AgyUsage
	ActiveTasks    []TaskMetadata
	IsYieldTrap    bool
	ExitCode       int
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
	args := []string{
		"--dangerously-skip-permissions",
		"--input-format", "stream-json",
		"--output-format", "stream-json",
	}
	if cfg.SessionID != "" {
		args = append(args, "--conversation", cfg.SessionID)
	}
	if cfg.Model != "" {
		args = append(args, "--model", cfg.Model)
	}

	printTimeout := cfg.Timeout
	if printTimeout <= 0 {
		printTimeout = 60 * time.Minute
	}
	if int(printTimeout.Seconds())%60 == 0 {
		args = append(args, "--print-timeout", fmt.Sprintf("%dm", int(printTimeout.Minutes())))
	} else {
		args = append(args, "--print-timeout", fmt.Sprintf("%ds", int(printTimeout.Seconds())))
	}

	return args
}

// extractSubagentID extracts a subagent conversation ID from invoke_subagent tool output.
// It parses JSON objects and arrays via json.Unmarshal / json.Decoder, supporting both
// camelCase ("conversationId") and snake_case ("conversation_id") keys.
func extractSubagentID(toolOut string) string {
	trimmed := strings.TrimSpace(toolOut)
	if trimmed == "" {
		return ""
	}

	type subagentItem struct {
		ConversationID string `json:"conversationId"`
		AltConvID      string `json:"conversation_id"`
	}
	type subagentPayload struct {
		ConversationID string         `json:"conversationId"`
		AltConvID      string         `json:"conversation_id"`
		Subagents      []subagentItem `json:"subagents"`
	}

	extractFromItem := func(item subagentItem) string {
		id := strings.TrimSpace(item.ConversationID)
		if id == "" {
			id = strings.TrimSpace(item.AltConvID)
		}
		if id != "" {
			return strings.Clone(id)
		}
		return ""
	}

	extractFromPayload := func(payload subagentPayload) string {
		id := strings.TrimSpace(payload.ConversationID)
		if id == "" {
			id = strings.TrimSpace(payload.AltConvID)
		}
		if id != "" {
			return strings.Clone(id)
		}
		for _, item := range payload.Subagents {
			if subID := extractFromItem(item); subID != "" {
				return subID
			}
		}
		return ""
	}

	// 1. Direct array unmarshal
	var list []subagentItem
	if err := json.Unmarshal([]byte(trimmed), &list); err == nil {
		for _, item := range list {
			if id := extractFromItem(item); id != "" {
				return id
			}
		}
	}

	// 2. Direct object unmarshal
	var payload subagentPayload
	if err := json.Unmarshal([]byte(trimmed), &payload); err == nil {
		if id := extractFromPayload(payload); id != "" {
			return id
		}
	}

	// 3. Embedded JSON fallback for lines with prefixes
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] == '[' {
			var embeddedList []subagentItem
			dec := json.NewDecoder(strings.NewReader(trimmed[i:]))
			if err := dec.Decode(&embeddedList); err == nil {
				for _, item := range embeddedList {
					if id := extractFromItem(item); id != "" {
						return id
					}
				}
			}
		} else if trimmed[i] == '{' {
			var embeddedPayload subagentPayload
			dec := json.NewDecoder(strings.NewReader(trimmed[i:]))
			if err := dec.Decode(&embeddedPayload); err == nil {
				if id := extractFromPayload(embeddedPayload); id != "" {
					return id
				}
			}
		}
	}

	return ""
}
