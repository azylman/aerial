package transcript

import "testing"

func TestNormalizeToolKind(t *testing.T) {
	t.Parallel()

	servers := []string{"home", "home_assistant", "github"}
	tests := []struct {
		name       string
		toolName   string
		mcpServers []string
		want       string
	}{
		{"empty", "", servers, ""},
		{"unknown", "frobnicate", servers, "other"},
		{"claude Bash", "Bash", nil, "shell"},
		{"claude BashOutput", "BashOutput", nil, "shell"},
		{"claude KillShell", "KillShell", nil, "shell"},
		{"claude Read", "Read", nil, "read"},
		{"claude NotebookRead", "NotebookRead", nil, "read"},
		{"claude Edit", "Edit", nil, "edit"},
		{"claude MultiEdit", "MultiEdit", nil, "edit"},
		{"claude NotebookEdit", "NotebookEdit", nil, "edit"},
		{"claude Write", "Write", nil, "write"},
		{"claude Grep", "Grep", nil, "search"},
		{"claude Glob", "Glob", nil, "search"},
		{"claude WebFetch", "WebFetch", nil, "web"},
		{"claude WebSearch", "WebSearch", nil, "web"},
		{"claude Agent", "Agent", nil, "subagent"},
		{"claude Task", "Task", nil, "subagent"},
		{"claude Skill", "Skill", nil, "skill"},
		{"claude TodoWrite", "TodoWrite", nil, "task"},
		{"claude mcp", "mcp__github__get_me", servers, "mcp:github"},
		{"claude mcp server with underscore", "mcp__home_assistant__call", nil, "mcp:home_assistant"},
		{"claude mcp malformed", "mcp__github", nil, "other"},
		{"agy run_command", "run_command", nil, "shell"},
		{"agy send_command_input", "send_command_input", nil, "shell"},
		{"agy read_terminal", "read_terminal", nil, "shell"},
		{"agy view_file", "view_file", nil, "read"},
		{"agy view_file_outline", "view_file_outline", nil, "read"},
		{"agy view_code_item", "view_code_item", nil, "read"},
		{"agy list_dir", "list_dir", nil, "read"},
		{"agy edit_file", "edit_file", nil, "edit"},
		{"agy replace_file_content", "replace_file_content", nil, "edit"},
		{"agy multi_replace_file_content", "multi_replace_file_content", nil, "edit"},
		{"agy write_to_file", "write_to_file", nil, "write"},
		{"agy grep_search", "grep_search", nil, "search"},
		{"agy find_by_name", "find_by_name", nil, "search"},
		{"agy codebase_search", "codebase_search", nil, "search"},
		{"agy read_url_content", "read_url_content", nil, "web"},
		{"agy search_web", "search_web", nil, "web"},
		{"agy browser prefix", "browser_click", nil, "web"},
		{"agy invoke_subagent", "invoke_subagent", nil, "subagent"},
		{"agy manage_subagents", "manage_subagents", nil, "subagent"},
		{"agy mcp longest configured prefix", "mcp_home_assistant_call_service", servers, "mcp:home_assistant"},
		{"agy mcp shorter configured prefix", "mcp_home_lights_on", servers, "mcp:home"},
		{"agy mcp unconfigured falls back to first segment", "mcp_docket_create", servers, "mcp:docket"},
		{"agy mcp no servers configured", "mcp_home_assistant_call", nil, "mcp:home"},
		{"agy mcp without tool segment", "mcp_docket", servers, "other"},
		{"codex exec_command", "exec_command", nil, "shell"},
		{"codex shell", "shell", nil, "shell"},
		{"codex shell_command", "shell_command", nil, "shell"},
		{"codex local_shell", "local_shell", nil, "shell"},
		{"codex write_stdin", "write_stdin", nil, "shell"},
		{"codex apply_patch", "apply_patch", nil, "edit"},
		{"codex web_search", "web_search", nil, "web"},
		{"codex view_image", "view_image", nil, "read"},
		{"codex spawn_agent", "spawn_agent", nil, "subagent"},
		{"codex send_input", "send_input", nil, "subagent"},
		{"codex wait_agent", "wait_agent", nil, "subagent"},
		{"codex close_agent", "close_agent", nil, "subagent"},
		{"codex resume_agent", "resume_agent", nil, "subagent"},
		{"codex update_plan", "update_plan", nil, "task"},
		{"codex request_user_input", "request_user_input", nil, "other"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := NormalizeToolKind(tc.toolName, tc.mcpServers); got != tc.want {
				t.Fatalf("NormalizeToolKind(%q) = %q, want %q", tc.toolName, got, tc.want)
			}
		})
	}
}

func TestInferEngine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		toolNames []string
		want      string
	}{
		{"claude tools only", []string{"Bash", "Read", "TodoWrite"}, "claude"},
		{"agy tools only", []string{"run_command", "view_file"}, "agy"},
		{"codex tools only", []string{"local_shell", "apply_patch", "web_search"}, "codex"},
		{"claude mcp double underscore", []string{"mcp__github__create_issue", "unknown_tool"}, "claude"},
		{"agy mcp single underscore", []string{"mcp_docket_create"}, "agy"},
		{"mixed engines", []string{"Bash", "run_command"}, ""},
		{"mixed codex and claude", []string{"local_shell", "Bash"}, ""},
		{"mixed codex and agy", []string{"apply_patch", "run_command"}, ""},
		{"mixed mcp styles", []string{"mcp__github__get_me", "mcp_docket_create"}, ""},
		{"empty", nil, ""},
		{"unknown names only", []string{"browser_click", "mystery"}, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := InferEngine(tc.toolNames); got != tc.want {
				t.Fatalf("InferEngine(%q) = %q, want %q", tc.toolNames, got, tc.want)
			}
		})
	}
}
