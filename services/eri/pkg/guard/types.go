package guard

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strconv"
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
	switch n {
	case "exec_command", "shell", "shell_command", "local_shell":
		return toolBash
	case "apply_patch":
		return toolEdit
	default:
		return n
	}
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

// StringOrSlice unmarshals from either a JSON string or a JSON array of strings (such as an argv slice).
// When an array is provided, if it is a shell wrapper like ["bash", "-c", script] or ["bash", "-lc", script],
// it extracts the script payload; otherwise it joins elements with spaces.
type StringOrSlice string

func (s *StringOrSlice) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*s = ""
		return nil
	}
	if trimmed[0] == '"' {
		var str string
		if err := json.Unmarshal(trimmed, &str); err != nil {
			return err
		}
		*s = StringOrSlice(str)
		return nil
	}
	if trimmed[0] == '[' {
		var slice []string
		if err := json.Unmarshal(trimmed, &slice); err != nil {
			return err
		}
		*s = StringOrSlice(joinShellArgv(slice))
		return nil
	}
	*s = StringOrSlice(string(trimmed))
	return nil
}

func joinShellArgv(argv []string) string {
	if len(argv) == 0 {
		return ""
	}
	if len(argv) >= 3 {
		bin := strings.ToLower(filepath.Base(argv[0]))
		if bin == binBash || bin == "sh" || bin == "zsh" {
			for i := 1; i < len(argv)-1; i++ {
				if argv[i] == "-c" || argv[i] == "-lc" || argv[i] == "-cl" {
					return argv[len(argv)-1]
				}
			}
		}
	}
	var b strings.Builder
	for i, arg := range argv {
		if i > 0 {
			b.WriteByte(' ')
		}
		if strings.ContainsAny(arg, " \t\n\"'") {
			b.WriteString(strconv.Quote(arg))
		} else {
			b.WriteString(arg)
		}
	}
	return b.String()
}

// ToolArgs represents commonly inspected tool parameters.
type ToolArgs struct {
	TargetFile        string          `json:"TargetFile"`
	AbsolutePath      string          `json:"AbsolutePath"`
	Path              string          `json:"path"`
	File              string          `json:"file"`
	FilePathSnake     string          `json:"file_path,omitempty"`
	Patch             string          `json:"patch,omitempty"`
	Input             string          `json:"input,omitempty"`
	Command           StringOrSlice   `json:"command,omitempty"`
	Cmd               StringOrSlice   `json:"cmd,omitempty"`
	Workdir           string          `json:"workdir,omitempty"`
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
	return a.FilePath() != "" || a.CommandLine != "" || a.Command != "" || a.Cmd != "" ||
		a.Patch != "" || a.Input != "" ||
		a.Cwd != "" || a.Workdir != "" ||
		a.ToolName != "" || a.ServerName != "" || a.ToolNameLower != "" || a.ServerNameLower != "" ||
		a.Action != "" || len(a.Arguments) > 0 || len(a.ArgumentsLower) > 0 ||
		len(a.WaitMsBeforeAsync) > 0 || len(a.IsDaemon) > 0
}

func (a *ToolArgs) patchText() string {
	if a == nil {
		return ""
	}
	if a.Patch != "" {
		return a.Patch
	}
	if a.Input != "" {
		return a.Input
	}
	if a.Command != "" {
		return string(a.Command)
	}
	if a.Cmd != "" {
		return string(a.Cmd)
	}
	if a.CommandLine != "" {
		return a.CommandLine
	}
	return ""
}

func extractPatchPaths(text string) []string {
	if text == "" {
		return nil
	}
	var paths []string
	seen := make(map[string]struct{})
	add := func(p string) {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, `"'`)
		if p == "" || p == "/dev/null" {
			return
		}
		if _, ok := seen[p]; !ok {
			seen[p] = struct{}{}
			paths = append(paths, p)
		}
	}

	isCodexPatch := strings.Contains(text, "*** Begin Patch")
	inHunk := false

	for line := range strings.SplitSeq(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "@@") {
			inHunk = true
			continue
		}
		if strings.HasPrefix(trimmed, "diff --git") {
			inHunk = false
		}
		switch {
		case strings.HasPrefix(trimmed, "*** Update File:"):
			add(strings.TrimPrefix(trimmed, "*** Update File:"))
		case strings.HasPrefix(trimmed, "*** Add File:"):
			add(strings.TrimPrefix(trimmed, "*** Add File:"))
		case strings.HasPrefix(trimmed, "*** Delete File:"):
			add(strings.TrimPrefix(trimmed, "*** Delete File:"))
		case strings.HasPrefix(trimmed, "*** Move to:"):
			add(strings.TrimPrefix(trimmed, "*** Move to:"))
		case strings.HasPrefix(trimmed, "*** Move from:"):
			add(strings.TrimPrefix(trimmed, "*** Move from:"))
		case !isCodexPatch && !inHunk && strings.HasPrefix(trimmed, "+++ "):
			target := strings.TrimPrefix(trimmed, "+++ ")
			target = strings.TrimPrefix(target, "b/")
			add(target)
		case !isCodexPatch && !inHunk && strings.HasPrefix(trimmed, "--- "):
			target := strings.TrimPrefix(trimmed, "--- ")
			target = strings.TrimPrefix(target, "a/")
			add(target)
		}
	}
	return paths
}

func (a *ToolArgs) explicitFilePath() string {
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

// TargetPaths returns all candidate file paths targeted by this tool call.
// It includes the explicit file path (if any) and any paths extracted from patch text.
func (a *ToolArgs) TargetPaths() []string {
	if a == nil {
		return nil
	}
	seen := make(map[string]struct{})
	var paths []string
	add := func(p string) {
		p = strings.TrimSpace(p)
		if p == "" {
			return
		}
		if _, ok := seen[p]; !ok {
			seen[p] = struct{}{}
			paths = append(paths, p)
		}
	}
	if exp := a.explicitFilePath(); exp != "" {
		add(exp)
	}
	for _, p := range extractPatchPaths(a.patchText()) {
		add(p)
	}
	return paths
}

// FilePath returns the resolved file path from TargetFile, AbsolutePath, Path, FilePathSnake, File,
// or patch content headers. When multiple patch paths exist, protected or live-state paths are
// prioritized to ensure safety guards detect violations.
func (a *ToolArgs) FilePath() string {
	if a == nil {
		return ""
	}
	if exp := a.explicitFilePath(); exp != "" {
		return exp
	}
	paths := extractPatchPaths(a.patchText())
	if len(paths) == 0 {
		return ""
	}
	for _, p := range paths {
		resolved := resolveArg(p, a.Cwd)
		if IsProtectedPath(resolved) || IsLiveStatePath(resolved) {
			return p
		}
	}
	return paths[0]
}

// Normalize normalizes Claude and Codex fields (Command/Cmd -> CommandLine, Workdir -> Cwd, Offset/Limit -> StartLine/EndLine).
func (a *ToolArgs) Normalize() {
	if a == nil {
		return
	}
	if a.CommandLine == "" {
		if a.Command != "" {
			a.CommandLine = string(a.Command)
		} else if a.Cmd != "" {
			a.CommandLine = string(a.Cmd)
		}
	}
	if a.Cwd == "" && a.Workdir != "" {
		a.Cwd = a.Workdir
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
