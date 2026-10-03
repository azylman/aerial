package guard

import (
	"encoding/json"
)

// DecisionType represents the hook authorization verdict.
type DecisionType string

const (
	DecisionAllow DecisionType = "allow"
	DecisionDeny  DecisionType = "deny"
)

// Decision is the JSON payload returned to Antigravity.
type Decision struct {
	Decision DecisionType `json:"decision"`
	Reason   string       `json:"reason,omitempty"`
}

// Payload is the root JSON structure received from Antigravity via stdin.
type Payload struct {
	ToolCall       ToolCall `json:"toolCall"`
	StepIdx        int      `json:"stepIdx"`
	ConversationID string   `json:"conversationId"`
}

// ToolCall represents the intercepted tool invocation.
type ToolCall struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
}

// ToolArgs represents commonly inspected tool parameters.
type ToolArgs struct {
	TargetFile   string `json:"TargetFile"`
	AbsolutePath string `json:"AbsolutePath"`
	Path         string `json:"path"`
	File         string `json:"file"`
	CommandLine  string `json:"CommandLine"`
	Cwd          string `json:"Cwd"`
}

// FilePath returns the resolved file path from TargetFile, AbsolutePath, Path, or File.
func (a ToolArgs) FilePath() string {
	if a.TargetFile != "" {
		return a.TargetFile
	}
	if a.AbsolutePath != "" {
		return a.AbsolutePath
	}
	if a.Path != "" {
		return a.Path
	}
	return a.File
}

// ParseArgs extracts ToolArgs from raw JSON message (handling both object and JSON-string representations).
func ParseArgs(raw json.RawMessage) ToolArgs {
	var args ToolArgs
	if len(raw) == 0 {
		return args
	}

	// 1. Try direct object unmarshal
	if err := json.Unmarshal(raw, &args); err == nil && (args.FilePath() != "" || args.CommandLine != "" || args.Cwd != "") {
		return args
	}

	// 2. Try string containing JSON
	var s string
	if err := json.Unmarshal(raw, &s); err == nil && s != "" {
		if err := json.Unmarshal([]byte(s), &args); err == nil {
			return args
		}
	}

	return args
}
