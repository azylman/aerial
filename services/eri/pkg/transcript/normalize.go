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

// toolKindByName maps engine-native tool names to normalized kinds. Claude Code and agy
// tool names do not collide, so a single lookup table serves both engines.
var toolKindByName = map[string]string{
	// Claude Code
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
	// agy
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
	if kind, ok := toolKindByName[toolName]; ok {
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
