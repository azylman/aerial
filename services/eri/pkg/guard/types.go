package guard

import (
	"encoding/json"
	"strings"
)

// normalizeToolName returns the lowercased tool base name without server prefix.
func normalizeToolName(toolName string) string {
	n := strings.ToLower(strings.TrimSpace(toolName))
	// Strip Claude MCP tool prefix: mcp__<server>__<tool> -> <tool>
	if strings.HasPrefix(n, "mcp__") {
		parts := strings.Split(n, "__")
		if len(parts) >= 3 {
			n = parts[len(parts)-1]
		}
	}
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

// InvocationResponse is the JSON payload returned for PreInvocation and PostToolUse hooks.
type InvocationResponse struct{}

// Decision is the JSON payload returned to Antigravity.
type Decision struct {
	Decision  DecisionType   `json:"decision"`
	Reason    string         `json:"reason,omitempty"`
	Overwrite map[string]any `json:"overwrite,omitempty"`
}

// ClaudeHookResponse represents the JSON output format for Claude Code hooks.
type ClaudeHookResponse struct {
	HookSpecificOutput ClaudeHookSpecificOutput `json:"hookSpecificOutput"`
}

type ClaudeHookSpecificOutput struct {
	HookEventName            string         `json:"hookEventName"`
	PermissionDecision       string         `json:"permissionDecision"`
	PermissionDecisionReason string         `json:"permissionDecisionReason,omitempty"`
	UpdatedInput             map[string]any `json:"updatedInput,omitempty"`
}

// Payload is the root JSON structure received from Antigravity or Claude Code via stdin.
type Payload struct {
	ToolCall       ToolCall        `json:"toolCall"`
	StepIdx        int             `json:"stepIdx"`
	InvocationNum  int             `json:"invocationNum,omitempty"`
	Error          string          `json:"error,omitempty"`
	ConversationID string          `json:"conversationId"`
	SessionID      string          `json:"session_id,omitempty"`
	HookEventName  string          `json:"hook_event_name,omitempty"`
	ToolName       string          `json:"tool_name,omitempty"`
	ToolInput      json.RawMessage `json:"tool_input,omitempty"`
	Cwd            string          `json:"cwd,omitempty"`
}

// IsClaude reports whether this payload originated from Claude Code hooks.
func (p *Payload) IsClaude() bool {
	if p == nil {
		return false
	}
	return p.SessionID != "" || p.HookEventName != "" || p.ToolName != "" || len(p.ToolInput) > 0
}

// Normalize normalizes Claude Code fields into canonical Antigravity Payload fields.
func (p *Payload) Normalize() {
	if p == nil {
		return
	}
	if p.ConversationID == "" && p.SessionID != "" {
		p.ConversationID = p.SessionID
	}
	if p.ToolCall.Name == "" && p.ToolName != "" {
		p.ToolCall.Name = p.ToolName
	}
	if len(p.ToolCall.Args) == 0 && len(p.ToolInput) > 0 {
		p.ToolCall.Args = p.ToolInput
	}
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
	FilePathSnake     string          `json:"file_path,omitempty"`
	Command           string          `json:"command,omitempty"`
	Pattern           string          `json:"pattern,omitempty"`
	Offset            int             `json:"offset,omitempty"`
	Limit             int             `json:"limit,omitempty"`
	StartLine         int             `json:"StartLine"`
	EndLine           int             `json:"EndLine"`
	CommandLine       string          `json:"CommandLine"`
	Cwd               string          `json:"Cwd"`
	ServerName        string          `json:"ServerName"`
	ToolName          string          `json:"ToolName"`
	Arguments         json.RawMessage `json:"Arguments"`
	ServerNameLower   string          `json:"server_name,omitempty"`
	ToolNameLower     string          `json:"tool_name,omitempty"`
	ArgumentsLower    json.RawMessage `json:"arguments,omitempty"`
	Action            string          `json:"Action"`
	WaitMsBeforeAsync json.RawMessage `json:"WaitMsBeforeAsync"`
	IsDaemon          json.RawMessage `json:"IsDaemon"`
}

// NormalizedAction returns the lowercased, unquoted Action parameter.
func (a *ToolArgs) NormalizedAction() string {
	if a == nil {
		return ""
	}
	return strings.ToLower(strings.Trim(strings.TrimSpace(a.Action), `"`))
}

// IsDaemonBool reports whether IsDaemon is set to true (handling bool or string JSON encodings).
func (a *ToolArgs) IsDaemonBool() bool {
	if a == nil {
		return false
	}
	return strings.EqualFold(strings.Trim(strings.TrimSpace(string(a.IsDaemon)), `"`), "true")
}

// WaitMs returns the parsed integer value of WaitMsBeforeAsync, or 0 if unset/unparseable.
func (a *ToolArgs) WaitMs() int {
	if a == nil {
		return 0
	}
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

func (a *ToolArgs) hasAnyField() bool {
	if a == nil {
		return false
	}
	return a.FilePath() != "" || a.CommandLine != "" || a.Cwd != "" ||
		a.ToolName != "" || a.ServerName != "" || a.ToolNameLower != "" || a.ServerNameLower != "" ||
		a.Action != "" || len(a.Arguments) > 0 || len(a.ArgumentsLower) > 0 ||
		len(a.WaitMsBeforeAsync) > 0 || len(a.IsDaemon) > 0
}

// FilePath returns the resolved file path from TargetFile, AbsolutePath, Path, FilePathSnake, or File.
func (a *ToolArgs) FilePath() string {
	if a == nil {
		return ""
	}
	if a.TargetFile != "" {
		return a.TargetFile
	}
	if a.AbsolutePath != "" {
		return a.AbsolutePath
	}
	if a.Path != "" {
		return a.Path
	}
	if a.FilePathSnake != "" {
		return a.FilePathSnake
	}
	return a.File
}

// Normalize normalizes Claude fields (Command -> CommandLine, Offset/Limit -> StartLine/EndLine).
func (a *ToolArgs) Normalize() {
	if a == nil {
		return
	}
	if a.CommandLine == "" && a.Command != "" {
		a.CommandLine = a.Command
	}
	if a.StartLine <= 0 && a.EndLine <= 0 && (a.Offset > 0 || a.Limit > 0) {
		a.StartLine = max(1, a.Offset)
		if a.Limit > 0 {
			a.EndLine = a.StartLine + a.Limit - 1
		}
	}
}

// ParseArgs extracts ToolArgs from raw JSON message (handling both object and JSON-string representations).
func ParseArgs(raw json.RawMessage) ToolArgs {
	var args ToolArgs
	if len(raw) == 0 {
		return args
	}

	// 1. Try direct object unmarshal
	if err := json.Unmarshal(raw, &args); err == nil && args.hasAnyField() {
		args.Normalize()
		return args
	}

	// 2. Try string containing JSON
	var s string
	if err := json.Unmarshal(raw, &s); err == nil && s != "" {
		if err := json.Unmarshal([]byte(s), &args); err == nil {
			args.Normalize()
			return args
		}
	}

	args.Normalize()
	return args
}

// AdoptClaudeMCPCall fills the canonical agy call_mcp_tool fields (ServerName, ToolName, Arguments)
// from a Claude mcp__<server>__<tool> invocation, whose tool_input is the raw MCP argument object.
// Fields already set by the payload are left untouched.
func (a *ToolArgs) AdoptClaudeMCPCall(toolName string, raw json.RawMessage) {
	if a == nil || !isClaudeMCPToolName(toolName) {
		return
	}
	parts := strings.Split(strings.TrimSpace(toolName), "__")
	if a.ServerName == "" {
		a.ServerName = strings.Join(parts[1:len(parts)-1], "__")
	}
	if a.ToolName == "" {
		a.ToolName = parts[len(parts)-1]
	}
	if len(a.Arguments) == 0 {
		a.Arguments = raw
	}
}

// TraceAnnouncement tells eri's MCP trace proxy which daemon is about to call an MCP tool so
// the proxied request can be parented under that daemon's turn. It carries names only, never
// tool arguments.
type TraceAnnouncement struct {
	DaemonID       string `json:"daemon_id"`
	ConversationID string `json:"conversation_id"`
	Server         string `json:"server"`
	Tool           string `json:"tool"`
}

// NewTraceAnnouncement builds the announcement for an agy call_mcp_tool PreToolUse payload;
// ok is false when the payload does not name both an MCP server and tool.
func NewTraceAnnouncement(payload Payload, daemonID string) (TraceAnnouncement, bool) {
	args := ParseArgs(payload.ToolCall.Args)
	args.AdoptClaudeMCPCall(payload.ToolCall.Name, payload.ToolCall.Args)
	server, tool := args.ServerName, args.ToolName
	if server == "" {
		server = args.ServerNameLower
	}
	if tool == "" {
		tool = args.ToolNameLower
	}
	if server == "" || tool == "" {
		return TraceAnnouncement{}, false
	}
	return TraceAnnouncement{DaemonID: daemonID, ConversationID: payload.ConversationID, Server: server, Tool: tool}, true
}
