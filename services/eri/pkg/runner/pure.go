package runner

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"
)

// AgyEnvInput holds environment configuration parameters.
type AgyEnvInput struct {
	BaseEnv  []string
	HomeDir  string
	APIKey   string
	TargetID string
	Timezone string
	ExtraEnv []string
}

func filterEnvKeys(env []string, excluded ...string) []string {
	excludeMap := make(map[string]struct{}, len(excluded))
	for _, k := range excluded {
		excludeMap[strings.ToUpper(strings.TrimSpace(k))] = struct{}{}
	}
	filtered := make([]string, 0, len(env))
	for _, e := range env {
		if k, _, ok := strings.Cut(e, "="); ok {
			if _, skip := excludeMap[strings.ToUpper(strings.TrimSpace(k))]; skip {
				continue
			}
		}
		filtered = append(filtered, e)
	}
	return filtered
}

func FilterAgyBaseEnv(env []string) []string {
	return filterEnvKeys(env, "GEMINI_API_KEY", "ANTIGRAVITY_API_KEY", "GOOGLE_GENAI_API_KEY")
}

func filterHomeEnv(env []string) []string {
	return filterEnvKeys(env, "HOME", "USERPROFILE", "GEMINI_CLI_HOME")
}

func filterVanadisChunkEnv(env []string) []string {
	return filterEnvKeys(env, "AERIAL_VANADIS_CHUNK_ID", "VANADIS_CHUNK_ID")
}

// BuildAgyEnv constructs the process environment slice for agy execution.
func BuildAgyEnv(input AgyEnvInput) []string {
	var cmdEnv []string
	if len(input.BaseEnv) > 0 {
		base := filterVanadisChunkEnv(input.BaseEnv)
		if strings.TrimSpace(input.APIKey) == "" {
			base = FilterAgyBaseEnv(base)
		}
		if strings.TrimSpace(input.HomeDir) != "" {
			base = filterHomeEnv(base)
		}
		cmdEnv = append(cmdEnv, base...)
	}

	cmdEnv = append(cmdEnv, "GIT_TERMINAL_PROMPT=0", "AGY_LOG_LEVEL=debug", "ANTIGRAVITY_LOG_LEVEL=debug")
	if h := strings.TrimSpace(input.HomeDir); h != "" {
		cmdEnv = append(cmdEnv, "HOME="+h, "USERPROFILE="+h, "GEMINI_CLI_HOME="+h)
	}
	if k := strings.TrimSpace(input.APIKey); k != "" {
		cmdEnv = append(cmdEnv, "GEMINI_API_KEY="+k, "ANTIGRAVITY_API_KEY="+k, "GOOGLE_GENAI_API_KEY="+k)
	}
	if t := strings.TrimSpace(input.TargetID); t != "" {
		cmdEnv = append(cmdEnv, "AERIAL_TARGET_ID="+t)
	}
	if tz := strings.TrimSpace(input.Timezone); tz != "" {
		cmdEnv = append(cmdEnv, "TZ="+tz)
	}
	return append(cmdEnv, input.ExtraEnv...)
}

// ParseInitEvent extracts conversation ID from an NDJSON line containing {"event":"init"}.
// Note: agy --output-format stream-json emits strict NDJSON lines on stdout (diagnostics go to stderr),
// so ParseInitEvent intentionally uses direct json.Unmarshal without manual '{' prefix-scanning.
func ParseInitEvent(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return "", false
	}
	var probe sessionProbe
	if json.Unmarshal([]byte(trimmed), &probe) == nil && (probe.Event == "init" || probe.Type == "init") && probe.extractUUID() != "" {
		return probe.extractUUID(), true
	}
	return "", false
}

// CleanUnescapedString strips surrounding quotes and whitespace from an unmarshaled string.
func CleanUnescapedString(s string) string {
	s = strings.TrimSpace(s)
	for len(s) >= 2 && (s[0] == '"' || s[0] == '\\') {
		trimmed := strings.Trim(s, "\\\" \t\r\n")
		if trimmed == s {
			break
		}
		s = trimmed
	}
	return s
}

// ExtractMCPToolInfo extracts the canonical tool name and MCP server namespace.
func ExtractMCPToolInfo(toolName string, params map[string]any) (string, string) {
	if toolName = strings.TrimSpace(toolName); toolName == "" {
		return "unknown", "native"
	}
	if toolName == "call_mcp_tool" {
		if params != nil {
			var sName, tName string
			if s, ok := params["ServerName"].(string); ok {
				sName = CleanUnescapedString(s)
			}
			if t, ok := params["ToolName"].(string); ok {
				tName = CleanUnescapedString(t)
			}
			if tName != "" && sName != "" {
				return tName, sName
			} else if tName != "" {
				return tName, "unknown"
			} else if sName != "" {
				return "call_mcp_tool", sName
			}
		}
		return "call_mcp_tool", "unknown"
	}
	if strings.HasPrefix(toolName, "mcp_") {
		parts := strings.SplitN(toolName[4:], "_", 2)
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			return parts[1], parts[0]
		}
		if len(parts) == 1 && parts[0] != "" {
			return parts[0], "unknown"
		}
	}
	return toolName, "native"
}

// ExtractSkillFromTarget extracts a skill name from a file path or command string referencing SKILL.md.
func ExtractSkillFromTarget(target string) (string, bool) {
	trimmed := strings.TrimSpace(target)
	idx := strings.Index(trimmed, "SKILL.md")
	if idx == -1 {
		return "", false
	}
	prefix := strings.TrimRight(trimmed[:idx], "/\\ \t\"'`")
	if prefix == "" {
		return "", false
	}
	skill := prefix
	if lastSlash := strings.LastIndexAny(prefix, "/\\"); lastSlash != -1 {
		skill = prefix[lastSlash+1:]
	}
	if skill = strings.Trim(skill, "\"'` "); skill != "" && skill != "skills" && skill != "custom-skills" {
		return skill, true
	}
	return "", false
}

func appendSection(p, tag, content string) string {
	if c := strings.TrimSpace(content); c != "" && !strings.Contains(p, "<"+tag+">") {
		return "<" + tag + ">\n" + c + "\n</" + tag + ">\n\n" + p
	}
	return p
}

// compileLeasePrompt compiles cold-start guidelines, session info, and coordination context into the turn prompt.
func compileLeasePrompt(turn *TurnContext, isCold bool, prevSessID string) string {
	if turn == nil {
		return ""
	}
	p := turn.Prompt
	if isCold {
		scope := strings.TrimSpace(firstNonEmpty(turn.ScopeInstructions, turn.ChannelInstructions))
		if scope != "" && !strings.Contains(p, "<SCOPE_INSTRUCTIONS>") && !strings.Contains(p, "<CHANNEL_INSTRUCTIONS>") {
			p = "<SCOPE_INSTRUCTIONS>\nScope-specific guidelines for this conversation:\n\n" + scope + "\n</SCOPE_INSTRUCTIONS>\n\n" + p
		}
		if prevSessID != "" {
			p = appendSection(p, "PREVIOUS_SESSION", "Previous Session ID: "+prevSessID)
		}
		p = appendSection(p, "THREAD_SUMMARY", turn.ThreadSummary)
	}
	return appendSection(p, "COORDINATION_CONTEXT", turn.CoordinationContext)
}

// injectRetrievedContext retrieves ambient context and memory within specified timeouts and prepends them to prompt.
func injectRetrievedContext(ctx context.Context, prompt, memQuery string, amb AmbientContextRetriever, ambTimeout time.Duration, mem MemoryRetriever, memTimeout time.Duration, logPrefix string) string {
	if ctx == nil {
		ctx = context.Background()
	}
	payloadPrompt := prompt
	fetch := func(tag, name string, to time.Duration, run func(context.Context) (string, error)) {
		if run == nil || strings.Contains(prompt, tag) {
			return
		}
		c, cancel := context.WithTimeout(ctx, to)
		defer cancel()
		if block, err := run(c); err != nil {
			log.Printf("%s Warning: %s failed: %v", logPrefix, name, err)
		} else if b := strings.TrimSpace(block); b != "" {
			payloadPrompt = b + "\n\n" + payloadPrompt
		}
	}
	if amb != nil {
		fetch("<ambient_context>", "AmbientContextRetriever", ambTimeout, amb)
	}
	if mem != nil {
		q := strings.TrimSpace(memQuery)
		if q == "" {
			q = prompt
		}
		fetch("<retrieved_memory>", "MemoryRetriever", memTimeout, func(c context.Context) (string, error) { return mem(c, q) })
	}
	return payloadPrompt
}
