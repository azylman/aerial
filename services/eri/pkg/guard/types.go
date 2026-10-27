package guard

import (
	"encoding/json"
	"strings"
)

// normalizeToolName returns the lowercased tool base name without server prefix.
func normalizeToolName(toolName string) string {
	n := strings.ToLower(strings.TrimSpace(toolName))
	if idx := strings.LastIndex(n, ":"); idx != -1 {
		n = n[idx+1:]
	}
	return n
}

// DecisionType represents the hook authorization verdict.
type DecisionType string

const (
	DecisionAllow DecisionType = "allow"
	DecisionDeny  DecisionType = "deny"
)

// Decision is the JSON payload returned to Antigravity.
type Decision struct {
	Decision  DecisionType   `json:"decision"`
	Reason    string         `json:"reason,omitempty"`
	Overwrite map[string]any `json:"overwrite,omitempty"`
}

// InvocationResponse is the JSON payload returned for PreInvocation and PostToolUse hooks.
type InvocationResponse struct{}

// Payload is the root JSON structure received from Antigravity via stdin.
type Payload struct {
	ToolCall       ToolCall `json:"toolCall"`
	StepIdx        int      `json:"stepIdx"`
	InvocationNum  int      `json:"invocationNum,omitempty"`
	Error          string   `json:"error,omitempty"`
	ConversationID string   `json:"conversationId"`
}

// ToolCall represents the intercepted tool invocation.
type ToolCall struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args"`
}

// ToolArgs represents commonly inspected tool parameters.
type ToolArgs struct {
	TargetFile        string          `json:"TargetFile"`
	AbsolutePath      string          `json:"AbsolutePath"`
	Path              string          `json:"path"`
	File              string          `json:"file"`
	StartLine         int             `json:"StartLine"`
	EndLine           int             `json:"EndLine"`
	CommandLine       string          `json:"CommandLine"`
	Cwd               string          `json:"Cwd"`
	ServerName        string          `json:"ServerName"`
	ToolName          string          `json:"ToolName"`
	Arguments         json.RawMessage `json:"Arguments"`
	Action            string          `json:"Action"`
	WaitMsBeforeAsync json.RawMessage `json:"WaitMsBeforeAsync"`
	IsDaemon          json.RawMessage `json:"IsDaemon"`
}

// NormalizedAction returns the lowercased, unquoted Action parameter.
func (a ToolArgs) NormalizedAction() string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(a.Action), `"`))
}

// IsDaemonBool reports whether IsDaemon is set to true (handling bool or string JSON encodings).
func (a ToolArgs) IsDaemonBool() bool {
	return strings.EqualFold(strings.Trim(strings.TrimSpace(string(a.IsDaemon)), `"`), "true")
}

// WaitMs returns the parsed integer value of WaitMsBeforeAsync, or 0 if unset/unparseable.
func (a ToolArgs) WaitMs() int {
	raw := strings.Trim(strings.TrimSpace(string(a.WaitMsBeforeAsync)), `"`)
	if raw == "" {
		return 0
	}
	var n int
	if err := json.Unmarshal([]byte(raw), &n); err == nil {
		return n
	}
	return 0
}

func (a ToolArgs) hasAnyField() bool {
	return a.FilePath() != "" || a.CommandLine != "" || a.Cwd != "" ||
		a.ToolName != "" || a.ServerName != "" || a.Action != "" ||
		len(a.WaitMsBeforeAsync) > 0 || len(a.IsDaemon) > 0
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
	if err := json.Unmarshal(raw, &args); err == nil && args.hasAnyField() {
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
