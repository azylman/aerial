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
	return filterEnvKeys(env, "AERIAL_VANADIS_CHUNK_ID", "VANADIS_CHUNK_ID", "AERIAL_TARGET_ID")
}

// NormalizeEnvTargetID normalizes a daemon target key for the AERIAL_TARGET_ID
// environment variable. It strips "discord-" (and optional pool prefixes like
// "stateful_medium:discord-") for Discord targets, and returns "" for headless
// or pooled worker keys ("heartbeat-*", "dispatch-*", "worker-*", "vanadis-*", or colon-scoped
// pool keys like "discord:worker-0" and "ephemeral:worker-0").
func NormalizeEnvTargetID(id string) string {
	clean := strings.TrimSpace(id)
	if clean == "" {
		return ""
	}
	if idx := strings.LastIndex(clean, ":"); idx >= 0 {
		suffix := strings.TrimSpace(clean[idx+1:])
		if !strings.HasPrefix(suffix, "discord-") {
			return ""
		}
		clean = suffix
	}
	if strings.HasPrefix(clean, "heartbeat-") || strings.HasPrefix(clean, "dispatch-") || strings.HasPrefix(clean, "worker-") || strings.HasPrefix(clean, "vanadis-") {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(clean, "discord-"))
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
	if t := NormalizeEnvTargetID(input.TargetID); t != "" {
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
		s = strings.Trim(s, "\\\" \t\r\n")
	}
	return s
}

func extractCallMCPToolInfo(params map[string]any) (string, string) {
	if params == nil {
		return "call_mcp_tool", "unknown"
	}
	var sName, tName string
	if s, ok := params["ServerName"].(string); ok {
		sName = CleanUnescapedString(s)
	}
	if t, ok := params["ToolName"].(string); ok {
		tName = CleanUnescapedString(t)
	}
	if tName != "" && sName != "" {
		return tName, sName
	}
	if tName != "" {
		return tName, "unknown"
	}
	if sName != "" {
		return "call_mcp_tool", sName
	}
	return "call_mcp_tool", "unknown"
}

// ExtractMCPToolInfo extracts the canonical tool name and MCP server namespace.
func ExtractMCPToolInfo(toolName string, params map[string]any) (string, string) {
	if toolName = strings.TrimSpace(toolName); toolName == "" {
		return "unknown", "native"
	}
	if toolName == "call_mcp_tool" {
		return extractCallMCPToolInfo(params)
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

func isSkillNameChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_'
}

func isReservedSkillDir(name string) bool {
	switch name {
	case "skills", "custom-skills", "common", "discord", "voice", "ephemeral", "builtin":
		return true
	default:
		return false
	}
}

func isValidSkillName(name string) bool {
	if len(name) == 0 || len(name) > 64 || isReservedSkillDir(name) || name[0] == '-' || name[0] == '_' || name[len(name)-1] == '-' || name[len(name)-1] == '_' {
		return false
	}
	for i := range len(name) {
		if !isSkillNameChar(name[i]) {
			return false
		}
	}
	return true
}

// ExtractSkillFromTarget extracts a skill name from a file path or command string referencing <skill>/SKILL.md.
func ExtractSkillFromTarget(target string) (string, bool) {
	const marker = "SKILL.md"
	s := strings.ReplaceAll(strings.TrimSpace(target), `\"`, `"`)
	for {
		idx := strings.Index(s, marker)
		if idx == -1 {
			return "", false
		}
		afterIdx := idx + len(marker)
		validAfter := afterIdx == len(s) || (!isSkillNameChar(s[afterIdx]) && s[afterIdx] != '.' && s[afterIdx] != '/' && s[afterIdx] != '\\')
		if idx > 0 && (s[idx-1] == '/' || s[idx-1] == '\\') && validAfter {
			prefix := s[:idx-1]
			segStart := strings.LastIndexAny(prefix, "/\\ \t\n\r\"'`=:")
			skill := strings.TrimSuffix(prefix[segStart+1:], ".tmp")
			if isValidSkillName(skill) {
				return skill, true
			}
		}
		s = s[afterIdx:]
	}
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
