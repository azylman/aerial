package guard

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/azylman/aerial/pkg/sanitizer"
)

// categoryInternalClusterDomain flags internal cluster hostnames in commit scans.
const categoryInternalClusterDomain = "InternalClusterDomain"

// Violation represents a secret or PII detection in a commit diff.
type Violation struct {
	Category string `json:"category"`
	Pattern  string `json:"pattern"`
	Sample   string `json:"sample"`
	File     string `json:"file"`
	LineNum  int    `json:"lineNum"`
}

// PatternRule binds a pattern category name to a compiled regular expression.
type PatternRule struct {
	Category string
	Regex    *regexp.Regexp
}

// BuiltinPatterns defines the standard generic security patterns.
// Strictly contains generic patterns without any hardcoded personal PII.
var BuiltinPatterns = []PatternRule{
	{Category: "RFC1918_192_168", Regex: regexp.MustCompile(`\b192\.168\.\d{1,3}\.\d{1,3}\b`)},
	{Category: categoryInternalClusterDomain, Regex: regexp.MustCompile(`(?i)\b[a-zA-Z0-9_-]+\.(?:aerial|lan)\b`)},
	{Category: "GitHubPAT", Regex: regexp.MustCompile(`\b(ghp|github_pat|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{20,80}\b`)},
	{Category: "PrivateKey", Regex: regexp.MustCompile(`-{5}BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-{5}`)},
	{Category: "DiscordWebhook", Regex: regexp.MustCompile(`https://discord(?:app)?\.com/api/webhooks/\d+/[A-Za-z0-9_-]+`)},
	{Category: "SlackWebhook", Regex: regexp.MustCompile(`https://hooks\.slack\.com/services/T[0-9A-Z]+/B[0-9A-Z]+/[0-9A-Za-z]+`)},
	{Category: "GenericAPIKey", Regex: regexp.MustCompile(`\b(sk-[a-zA-Z0-9]{20,80})\b`)},
	{Category: "JWTToken", Regex: regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b`)},
}

// LoadCustomPatterns reads dynamic patterns from ~/.gemini/config/commit-guard.json,
// ~/.gemini/config/guard.json, or the AERIAL_COMMIT_GUARD_PATTERNS variable resolved through getenv.
// An empty homeDir falls back to getenv("HOME").
func LoadCustomPatterns(homeDir string, getenv func(string) string) []PatternRule {
	var rules []PatternRule

	if homeDir == "" {
		homeDir = getenv("HOME")
	}

	// 1. Check file candidates in homeDir
	if homeDir != "" {
		candidates := []string{
			filepath.Join(homeDir, ".gemini", "config", "commit-guard.json"),
			filepath.Join(homeDir, ".gemini", "config", "guard.json"),
		}
		for _, cand := range candidates {
			if data, err := os.ReadFile(cand); err == nil && len(data) > 0 {
				rules = append(rules, parsePatternPayload(data)...)
				break
			}
		}
	}

	// 2. Check environment variable
	if envVal := getenv("AERIAL_COMMIT_GUARD_PATTERNS"); strings.TrimSpace(envVal) != "" {
		rules = append(rules, parsePatternPayload([]byte(envVal))...)
	}

	return rules
}

func compilePatternEntry(cat, pat string) (PatternRule, bool) {
	if pat == "" {
		return PatternRule{}, false
	}
	re, err := regexp.Compile(pat)
	if err != nil {
		return PatternRule{}, false
	}
	if cat == "" {
		cat = "CustomRule"
	}
	return PatternRule{Category: cat, Regex: re}, true
}

type rawPatternEntry struct {
	Category string `json:"category"`
	Pattern  string `json:"pattern"`
}

func parseJSONPatternEntries(data []byte) []PatternRule {
	var entries []rawPatternEntry
	if err := json.Unmarshal(data, &entries); err != nil || len(entries) == 0 {
		var wrapper struct {
			Patterns []rawPatternEntry `json:"patterns"`
		}
		if err := json.Unmarshal(data, &wrapper); err == nil {
			entries = wrapper.Patterns
		}
	}
	if len(entries) == 0 {
		return nil
	}
	rules := make([]PatternRule, 0, len(entries))
	for _, e := range entries {
		if rule, ok := compilePatternEntry(e.Category, e.Pattern); ok {
			rules = append(rules, rule)
		}
	}
	return rules
}

func parseJSONPatternMap(data []byte) []PatternRule {
	var kv map[string]string
	if err := json.Unmarshal(data, &kv); err != nil || len(kv) == 0 {
		return nil
	}
	rules := make([]PatternRule, 0, len(kv))
	for k, v := range kv {
		if rule, ok := compilePatternEntry(k, v); ok {
			rules = append(rules, rule)
		}
	}
	return rules
}

func parseDelimitedPatternLines(trimmed string) []PatternRule {
	lines := strings.Split(trimmed, "\n")
	if len(lines) == 1 && strings.Contains(trimmed, ",") && !strings.HasPrefix(trimmed, "{") && !strings.HasPrefix(trimmed, "[") {
		lines = strings.Split(trimmed, ",")
	}
	var rules []PatternRule
	for i, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" && !strings.HasPrefix(l, "#") {
			defaultCat := fmt.Sprintf("CustomRule_%d", i+1)
			if rule, ok := compilePatternEntry(defaultCat, l); ok {
				rules = append(rules, rule)
			}
		}
	}
	return rules
}

func parsePatternPayload(data []byte) []PatternRule {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil
	}
	if rules := parseJSONPatternEntries(data); rules != nil {
		return rules
	}
	if rules := parseJSONPatternMap(data); rules != nil {
		return rules
	}
	return parseDelimitedPatternLines(trimmed)
}

// GetActivePatterns returns BuiltinPatterns augmented with any custom user patterns found via getenv.
func GetActivePatterns(getenv func(string) string) []PatternRule {
	custom := LoadCustomPatterns("", getenv)
	if len(custom) == 0 {
		return BuiltinPatterns
	}
	result := make([]PatternRule, 0, len(BuiltinPatterns)+len(custom))
	result = append(result, BuiltinPatterns...)
	result = append(result, custom...)
	return result
}

// ScanTextForSecrets scans text line-by-line against registered Infisical secrets and pattern rules.
// RFC1918_192_168 and InternalClusterDomain patterns are skipped so local cluster commands/URLs remain permitted.
func ScanTextForSecrets(text, sourceLabel string, patterns []PatternRule) []Violation {
	if text == "" {
		return nil
	}

	var violations []Violation
	lines := strings.Split(text, "\n")
	for lineNum, line := range lines {
		currentLineNum := lineNum + 1

		matches := sanitizer.MatchSensitiveTokens(line)
		for _, sample := range matches {
			violations = append(violations, Violation{
				Category: "InfisicalSecret",
				Pattern:  "registered_secret",
				File:     sourceLabel,
				LineNum:  currentLineNum,
				Sample:   sample,
			})
		}

		for _, p := range patterns {
			if p.Category == "RFC1918_192_168" || p.Category == categoryInternalClusterDomain {
				continue
			}
			if sample := p.Regex.FindString(line); sample != "" {
				violations = append(violations, Violation{
					Category: p.Category,
					Pattern:  p.Regex.String(),
					File:     sourceLabel,
					LineNum:  currentLineNum,
					Sample:   sample,
				})
			}
		}
	}

	return violations
}

// scanLocal scans text in-process against registered secrets and the active
// pattern rules. Pushed content is scanned by the eri-restate GitHub proxy.
func scanLocal(text, sourceLabel string, getenv func(string) string) []Violation {
	return ScanTextForSecrets(text, sourceLabel, GetActivePatterns(getenv))
}

// unwrapShellScriptCommand unwraps bash/sh/zsh -c <script> commands.
func unwrapShellScriptCommand(tokens []string) (string, bool) {
	if len(tokens) < 3 {
		return "", false
	}
	bin := strings.ToLower(filepath.Base(tokens[0]))
	if bin != binBash && bin != "sh" && bin != "zsh" {
		return "", false
	}
	for i, t := range tokens {
		if t == "-c" && i+1 < len(tokens) {
			return tokens[i+1], true
		}
	}
	return "", false
}

// FormatCommitDenyReason constructs a human-readable explanation of detected violations.
func FormatCommitDenyReason(violations []Violation) string {
	var sb strings.Builder
	sb.WriteString("Commit blocked by commit-safety-guard: sensitive pattern violations detected:\n")
	for _, v := range violations {
		fmt.Fprintf(&sb, "- %s:%d [%s] matches '%s' (sample: %s)\n",
			v.File, v.LineNum, v.Category, v.Pattern, v.Sample)
	}
	sb.WriteString("Please remove secrets, tokens, or PII before committing. ")
	sb.WriteString("If this is a configuration change intended for aerial-config, author changes in aerial-config where this guard is disabled.")
	return sb.String()
}

// CheckOutboundSecrets inspects run_command (args.CommandLine) and call_mcp_tool / mcp_*
// (marshaling args.Arguments/Extra + args.CommandLine) against registered Infisical secrets and pattern rules.
// getenv resolves the custom pattern sources (HOME, AERIAL_COMMIT_GUARD_PATTERNS).
func CheckOutboundSecrets(toolName string, args ToolArgs, getenv func(string) string) *Decision {
	rawLower := strings.ToLower(strings.TrimSpace(toolName))
	cleanName := normalizeToolName(toolName)
	if isInfisicalMCPCall(toolName, args) {
		return &Decision{Decision: DecisionAllow}
	}
	var textToScan string

	switch {
	case cleanName == toolRunCommand || cleanName == toolBash:
		textToScan = args.CommandLine
	case cleanName == toolCallMCPTool || strings.HasPrefix(cleanName, "mcp_") || strings.HasPrefix(rawLower, "mcp_") || strings.HasPrefix(rawLower, "mcp:"):
		parts := []string{
			args.CommandLine,
			string(args.Arguments),
			string(args.ArgumentsLower),
			args.ToolName,
			args.ToolNameLower,
			args.ServerName,
			args.ServerNameLower,
		}
		textToScan = strings.TrimSpace(strings.Join(parts, "\n"))
	default:
		return &Decision{Decision: DecisionAllow}
	}

	if textToScan == "" {
		return &Decision{Decision: DecisionAllow}
	}

	violations := scanLocal(textToScan, cleanName, getenv)
	if len(violations) > 0 {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   FormatCommitDenyReason(violations),
		}
	}

	return &Decision{Decision: DecisionAllow}
}

// infisicalMCPServer is the server name the Infisical MCP is registered under on both stacks.
const infisicalMCPServer = "infisical"

// isInfisicalMCPCall reports whether a tool call targets the Infisical MCP server, whose whole
// purpose is reading and writing secrets (every call carries the registered project ID), so the
// outbound secret scan would deny all of them. The server segment must match exactly.
func isInfisicalMCPCall(toolName string, args ToolArgs) bool {
	if isClaudeMCPToolName(toolName) {
		parts := strings.Split(strings.TrimSpace(toolName), "__")
		return strings.EqualFold(strings.Join(parts[1:len(parts)-1], "__"), infisicalMCPServer)
	}
	if normalizeToolName(toolName) != toolCallMCPTool {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(args.ServerName), infisicalMCPServer) ||
		strings.EqualFold(strings.TrimSpace(args.ServerNameLower), infisicalMCPServer)
}
