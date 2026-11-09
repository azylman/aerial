package transcript

import "strings"

// Normalized tool kinds shared across engines. MCP tools normalize to ToolKindMCPPrefix + server.
const (
	ToolKindShell     = "shell"
	ToolKindRead      = "read"
	ToolKindEdit      = "edit"
	ToolKindWrite     = "write"
	ToolKindSearch    = "search"
	ToolKindWeb       = "web"
	ToolKindSubagent  = "subagent"
	ToolKindSkill     = "skill"
	ToolKindTask      = "task"
	ToolKindOther     = "other"
	ToolKindMCPPrefix = "mcp:"
)

// Engine values recorded per session.
const (
	EngineAgy    = "agy"
	EngineClaude = "claude"
)

// claudeToolKinds maps Claude Code tool names to normalized kinds. Claude Code and agy tool
// names do not collide, so a name's table also identifies the engine that emitted it.
var claudeToolKinds = map[string]string{
	"Bash":         ToolKindShell,
	"BashOutput":   ToolKindShell,
	"KillShell":    ToolKindShell,
	"Read":         ToolKindRead,
	"NotebookRead": ToolKindRead,
	"Edit":         ToolKindEdit,
	"MultiEdit":    ToolKindEdit,
	"NotebookEdit": ToolKindEdit,
	"Write":        ToolKindWrite,
	"Grep":         ToolKindSearch,
	"Glob":         ToolKindSearch,
	"WebFetch":     ToolKindWeb,
	"WebSearch":    ToolKindWeb,
	"Agent":        ToolKindSubagent,
	"Task":         ToolKindSubagent,
	"Skill":        ToolKindSkill,
	"TodoWrite":    ToolKindTask,
}

// agyToolKinds maps agy tool names to normalized kinds.
var agyToolKinds = map[string]string{
	"run_command":                ToolKindShell,
	"send_command_input":         ToolKindShell,
	"read_terminal":              ToolKindShell,
	"view_file":                  ToolKindRead,
	"view_file_outline":          ToolKindRead,
	"view_code_item":             ToolKindRead,
	"list_dir":                   ToolKindRead,
	"edit_file":                  ToolKindEdit,
	"replace_file_content":       ToolKindEdit,
	"multi_replace_file_content": ToolKindEdit,
	"write_to_file":              ToolKindWrite,
	"grep_search":                ToolKindSearch,
	"find_by_name":               ToolKindSearch,
	"codebase_search":            ToolKindSearch,
	"read_url_content":           ToolKindWeb,
	"search_web":                 ToolKindWeb,
	"invoke_subagent":            ToolKindSubagent,
	"manage_subagents":           ToolKindSubagent,
}

// NormalizeToolKind maps an engine-native tool name to its engine-agnostic kind.
// mcpServers lists configured MCP server names, used to split agy mcp_<server>_<tool>
// names whose server segment itself contains underscores. An empty name maps to "".
func NormalizeToolKind(toolName string, mcpServers []string) string {
	if toolName == "" {
		return ""
	}
	if kind, ok := claudeToolKinds[toolName]; ok {
		return kind
	}
	if kind, ok := agyToolKinds[toolName]; ok {
		return kind
	}
	if strings.HasPrefix(toolName, "browser_") {
		return ToolKindWeb
	}
	if rest, ok := strings.CutPrefix(toolName, "mcp__"); ok {
		if server, _, found := strings.Cut(rest, "__"); found && server != "" {
			return ToolKindMCPPrefix + server
		}
		return ToolKindOther
	}
	if rest, ok := strings.CutPrefix(toolName, "mcp_"); ok {
		if server := agyMCPServer(rest, mcpServers); server != "" {
			return ToolKindMCPPrefix + server
		}
	}
	return ToolKindOther
}

// agyMCPServer resolves the server segment of an agy MCP tool name remainder (<server>_<tool>):
// the longest configured server name followed by an underscore, else the segment before the
// first underscore.
func agyMCPServer(rest string, mcpServers []string) string {
	best := ""
	for _, s := range mcpServers {
		if len(s) > len(best) && strings.HasPrefix(rest, s+"_") {
			best = s
		}
	}
	if best != "" {
		return best
	}
	server, _, found := strings.Cut(rest, "_")
	if !found {
		return ""
	}
	return server
}

// InferEngine guesses which engine produced a session from its stored tool names, for sessions
// whose raw log is gone. Claude Code tools and mcp__ names count as claude; agy tools and mcp_
// (single underscore) names count as agy. It returns "" unless exactly one engine matched.
func InferEngine(toolNames []string) string {
	var claude, agy bool
	for _, name := range toolNames {
		_, isClaude := claudeToolKinds[name]
		_, isAgy := agyToolKinds[name]
		switch {
		case isClaude || strings.HasPrefix(name, "mcp__"):
			claude = true
		case isAgy || strings.HasPrefix(name, "mcp_"):
			agy = true
		}
	}
	switch {
	case claude && !agy:
		return EngineClaude
	case agy && !claude:
		return EngineAgy
	}
	return ""
}
