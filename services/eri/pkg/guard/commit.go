package guard

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
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
// Samples are masked so a matched token is never echoed back into the transcript.
func FormatCommitDenyReason(violations []Violation) string {
	var sb strings.Builder
	sb.WriteString("Commit blocked by commit-safety-guard: sensitive pattern violations detected:\n")
	for _, v := range violations {
		fmt.Fprintf(&sb, "- %s:%d [%s] matches '%s' (sample: %s)\n",
			v.File, v.LineNum, v.Category, v.Pattern, maskSampleOnce(v.Sample))
	}
	sb.WriteString("Please remove secrets, tokens, or PII before committing. ")
	sb.WriteString("If this is a configuration change intended for aerial-config, author changes in aerial-config where this guard is disabled.")
	return sb.String()
}

// maskSampleOnce masks sample unless it already is a masked sample (first2***last2 or [REDACTED]).
func maskSampleOnce(sample string) string {
	if sample == "[REDACTED]" || strings.Contains(sample, "***") {
		return sample
	}
	return sanitizer.MaskTokenSample(sample)
}

// toolExec is Codex's code-mode custom tool; MCP calls are made from the JavaScript it runs.
const toolExec = "exec"

// codexMCPRefPattern matches mcp__<server>__<tool> identifiers referenced from Codex exec code
// (tools.mcp__a__b(...), tools["mcp__a__b"], destructuring). Every reference counts, not only
// call sites, so aliasing a tool before calling it is still scanned.
var codexMCPRefPattern = regexp.MustCompile(`\bmcp__[A-Za-z0-9_$-]+`)

// resolveMCPCall derives the MCP destination (Server is empty when the shape cannot be parsed) from every engine's PreToolUse shape: Claude and
// muse mcp__<server>__<tool>, Codex namespace="mcp__<server>" + name=<tool>, muse/Codex
// mcp:<server>:<tool> and agy call_mcp_tool (canonical fields, then lowercase fallbacks).
// ok is false when the call is not an MCP call at all.
func resolveMCPCall(toolName string, raw json.RawMessage, args ToolArgs) (EgressScanRequest, bool) {
	trimmed := strings.TrimSpace(toolName)
	lower := strings.ToLower(trimmed)
	switch {
	case isClaudeMCPToolName(trimmed):
		parts := strings.Split(trimmed, "__")
		return EgressScanRequest{Server: strings.Join(parts[1:len(parts)-1], "__"), Tool: parts[len(parts)-1], Arguments: raw}, true
	case strings.HasPrefix(strings.ToLower(strings.TrimSpace(args.Namespace)), "mcp__"):
		return EgressScanRequest{Server: strings.TrimSpace(args.Namespace)[len("mcp__"):], Tool: trimmed, Arguments: raw}, true
	case strings.HasPrefix(lower, "mcp:"):
		parts := strings.Split(trimmed, ":")
		if len(parts) < 3 {
			return EgressScanRequest{Tool: trimmed, Arguments: raw}, true
		}
		return EgressScanRequest{Server: strings.Join(parts[1:len(parts)-1], ":"), Tool: parts[len(parts)-1], Arguments: raw}, true
	case normalizeToolName(trimmed) == toolCallMCPTool:
		call := EgressScanRequest{
			Server:    cmp.Or(strings.TrimSpace(args.ServerName), strings.TrimSpace(args.ServerNameLower)),
			Tool:      cmp.Or(strings.TrimSpace(args.ToolName), strings.TrimSpace(args.ToolNameLower)),
			Arguments: args.Arguments,
		}
		if len(call.Arguments) == 0 {
			call.Arguments = args.ArgumentsLower
		}
		return call, true
	case strings.HasPrefix(lower, "mcp_"):
		return EgressScanRequest{Arguments: raw}, true
	default:
		return EgressScanRequest{}, false
	}
}

// codexExecMCPCalls returns the distinct non-Infisical MCP destinations referenced by a Codex
// exec input, each carrying the full exec input encoded as a JSON string, sorted by server/tool.
func codexExecMCPCalls(raw json.RawMessage, args ToolArgs) []EgressScanRequest {
	text := args.Input
	if text == "" {
		if err := json.Unmarshal(raw, &text); err != nil {
			text = string(raw)
		}
	}
	encoded, err := json.Marshal(text)
	if err != nil {
		return nil
	}
	seen := make(map[string]bool)
	var calls []EgressScanRequest
	for _, ref := range codexMCPRefPattern.FindAllString(text, -1) {
		parts := strings.Split(ref[len("mcp__"):], "__")
		if len(parts) < 2 {
			continue
		}
		server, tool := strings.Join(parts[:len(parts)-1], "__"), parts[len(parts)-1]
		key := strings.ToLower(server + "/" + tool)
		if server == "" || tool == "" || isInfisicalMCPCall(server) || seen[key] {
			continue
		}
		seen[key] = true
		calls = append(calls, EgressScanRequest{Server: server, Tool: tool, Arguments: encoded})
	}
	slices.SortFunc(calls, func(a, b EgressScanRequest) int {
		return cmp.Or(cmp.Compare(a.Server, b.Server), cmp.Compare(a.Tool, b.Tool))
	})
	return calls
}

// CheckOutboundSecrets guards outbound tool calls. Shell commands (run_command / Bash) are
// regex-scanned locally. MCP calls (every engine shape, including Codex exec code referencing
// mcp__ tools) are sent to the eri-restate egress scan endpoint, which scans only blocklisted
// destinations against registered secret values and the pattern rules. Infisical MCP calls
// pass unscanned. A missing or failing scanner, or an MCP call whose server cannot be derived,
// denies (fail closed). raw is the payload's tool input; getenv resolves the custom pattern
// sources for the local shell scan.
func CheckOutboundSecrets(toolName string, raw json.RawMessage, args ToolArgs, getenv func(string) string, scanner EgressScanner) *Decision {
	cleanName := normalizeToolName(toolName)
	switch {
	case cleanName == toolRunCommand || cleanName == toolBash:
		return checkShellSecrets(args.CommandLine, cleanName, getenv)
	case cleanName == toolExec && args.Namespace == "":
		return checkEgress(codexExecMCPCalls(raw, args), scanner)
	}
	call, ok := resolveMCPCall(toolName, raw, args)
	if !ok {
		return &Decision{Decision: DecisionAllow}
	}
	if call.Server == "" || call.Tool == "" {
		return &Decision{
			Decision: DecisionDeny,
			Reason: fmt.Sprintf("Outbound secret guard: could not derive the MCP server and tool from tool name %q, "+
				"so the MCP call was blocked (unrecognized MCP call shape; teach guard.resolveMCPCall this engine's format).", toolName),
		}
	}
	if isInfisicalMCPCall(call.Server) {
		return &Decision{Decision: DecisionAllow}
	}
	return checkEgress([]EgressScanRequest{call}, scanner)
}

// checkShellSecrets regex-scans a shell command line in-process.
func checkShellSecrets(commandLine, label string, getenv func(string) string) *Decision {
	if strings.TrimSpace(commandLine) == "" {
		return &Decision{Decision: DecisionAllow}
	}
	if violations := scanLocal(commandLine, label, getenv); len(violations) > 0 {
		return &Decision{Decision: DecisionDeny, Reason: FormatCommitDenyReason(violations)}
	}
	return &Decision{Decision: DecisionAllow}
}

// checkEgress asks scanner about each MCP destination and denies on the first blocked result or
// scan error. A nil scanner counts as unavailable.
func checkEgress(calls []EgressScanRequest, scanner EgressScanner) *Decision {
	for _, call := range calls {
		if scanner == nil {
			return egressUnavailableDecision(call, errors.New("no egress scanner configured"))
		}
		result, err := scanner.Scan(context.Background(), call)
		if err != nil {
			return egressUnavailableDecision(call, err)
		}
		if result.Blocked {
			return &Decision{Decision: DecisionDeny, Reason: formatEgressDenyReason(call, result.Violations)}
		}
	}
	return &Decision{Decision: DecisionAllow}
}

func egressUnavailableDecision(call EgressScanRequest, err error) *Decision {
	return &Decision{
		Decision: DecisionDeny,
		Reason: fmt.Sprintf("Outbound secret guard: the egress scan is unavailable (%v), so the MCP call to %s/%s was blocked. "+
			"Retry once eri-restate's egress scan endpoint is healthy.", err, call.Server, call.Tool),
	}
}

// formatEgressDenyReason lists category and masked sample of each violation, never raw values.
func formatEgressDenyReason(call EgressScanRequest, violations []EgressViolation) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Outbound secret guard: MCP call to %s/%s blocked because its arguments carry secrets:\n", call.Server, call.Tool)
	for _, v := range violations {
		fmt.Fprintf(&sb, "- [%s] sample: %s\n", v.Category, maskSampleOnce(v.Sample))
	}
	sb.WriteString("This destination never legitimately receives secrets; remove the secret from the arguments.")
	return sb.String()
}

// infisicalMCPServer is the server name the Infisical MCP is registered under on both stacks.
const infisicalMCPServer = "infisical"

// isInfisicalMCPCall reports whether server is the Infisical MCP server, whose whole purpose is
// reading and writing secrets (every call carries the registered project ID), so the outbound
// secret scan would deny all of them. The server must match exactly (case-insensitive).
func isInfisicalMCPCall(server string) bool {
	return strings.EqualFold(strings.TrimSpace(server), infisicalMCPServer)
}
