package runner

import (
	"encoding/json"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	reStrictUUID                 = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	reUUIDInText                 = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	reUpdateStream               = regexp.MustCompile(`Starting conversation update stream for ([^\s\r\n]+)`)
	reGeneralSession             = regexp.MustCompile(`(?i)(?:conversation|session)(?:_id)?[:\s=]+([a-zA-Z0-9\-]+)`)
	reQuotaResetIn               = regexp.MustCompile(`(?i)(?:resets in|reset after)\s+([-0-9a-zA-Z\s]+?)(?:\.|\n|\r|$)`)
	reWhitespace                 = regexp.MustCompile(`\s+`)
	safeCmdRegex                 = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)
	reTerminatingBackgroundTasks = regexp.MustCompile(`(?i)terminating\s+([1-9]\d*)\s+background\s+task\(s\)`)

	quotaUnitReplacer = strings.NewReplacer(
		"minutes", "m", "minute", "m", "mins", "m", "min", "m",
		"hours", "h", "hour", "h", "hrs", "h", "hr", "h",
		"seconds", "s", "second", "s", "secs", "s", "sec", "s",
	)
)

func IsValidUUID(s string) bool { return reStrictUUID.MatchString(s) }

func extractUUIDByRegex(text string) string {
	for _, re := range []*regexp.Regexp{reUpdateStream, reGeneralSession} {
		if match := re.FindStringSubmatch(text); len(match) > 1 && IsValidUUID(strings.TrimSpace(match[1])) {
			return strings.TrimSpace(match[1])
		}
	}
	if match := reUUIDInText.FindString(text); match != "" && IsValidUUID(strings.TrimSpace(match)) {
		return strings.TrimSpace(match)
	}
	return ""
}

var (
	corruptionKeywords    = []string{"session corrupt", "corrupted session", "invalid session", "session not found", "failed to load conversation", "corrupted transcript", "failed to parse session", "stream was interrupted", "the stream was interrupted"}
	contextWindowKeywords = []string{"context window", "context length", "maximum context length", "token limit exceeded", "prompt is too long", "request too large"}
)

func hasKeyword(s string, keywords []string) bool {
	for _, kw := range keywords {
		if strings.Contains(s, kw) {
			return true
		}
	}
	return false
}

func hasContextWindowKeyword(lower string) bool { return hasKeyword(lower, contextWindowKeywords) }
func hasCorruptionKeyword(lower string) bool    { return hasKeyword(lower, corruptionKeywords) }

type sessionProbe struct {
	Event          string          `json:"event,omitempty"`
	Type           string          `json:"type,omitempty"`
	ConversationID string          `json:"conversation_id,omitempty"`
	SessionID      string          `json:"session_id,omitempty"`
	AltConvID      string          `json:"conversationId,omitempty"`
	AltSessID      string          `json:"sessionId,omitempty"`
	Data           json.RawMessage `json:"data,omitempty"`
	Result         json.RawMessage `json:"result,omitempty"`
}

func (p *sessionProbe) extractUUID() string {
	for _, candidate := range []string{p.ConversationID, p.SessionID, p.AltConvID, p.AltSessID} {
		if candidate = strings.TrimSpace(candidate); IsValidUUID(candidate) {
			return candidate
		}
	}
	for _, raw := range [][]byte{p.Data, p.Result} {
		if len(raw) > 0 {
			var sub sessionProbe
			if err := json.Unmarshal(raw, &sub); err == nil {
				if id := sub.extractUUID(); id != "" {
					return id
				}
			}
		}
	}
	return ""
}

type AgyResponse struct {
	ConversationID  string   `json:"conversation_id"`
	Status          string   `json:"status"`
	Response        string   `json:"response"`
	DurationSeconds float64  `json:"duration_seconds"`
	NumTurns        int      `json:"num_turns"`
	Usage           AgyUsage `json:"usage"`
	Error           string   `json:"error,omitempty"`
}

type AgyUsage struct {
	InputTokens     int `json:"input_tokens"`
	OutputTokens    int `json:"output_tokens"`
	ThinkingTokens  int `json:"thinking_tokens"`
	CacheReadTokens int `json:"cache_read_tokens"`
	TotalTokens     int `json:"total_tokens"`
}

type TokenUsage = AgyUsage

type StepToolInfo struct {
	Name       string             `json:"name,omitempty"`
	Parameters StepToolParameters `json:"parameters,omitempty"`
}

type StepToolParameters struct {
	CommandLine      string `json:"CommandLine,omitempty"`
	CommandLineSnake string `json:"command_line,omitempty"`
	Command          string `json:"command,omitempty"`
	Cmd              string `json:"cmd,omitempty"`
}

type StepUpdateEvent struct {
	Event          string        `json:"event,omitempty"`
	Type           string        `json:"type,omitempty"`
	StepType       string        `json:"step_type,omitempty"`
	Name           string        `json:"name,omitempty"`
	ToolName       string        `json:"tool_name,omitempty"`
	State          string        `json:"state,omitempty"`
	Status         string        `json:"status,omitempty"`
	ConversationID string        `json:"conversation_id,omitempty"`
	StepIndex      int           `json:"step_index,omitempty"`
	ToolInfo       *StepToolInfo `json:"tool_info,omitempty"`
	TextDelta      string        `json:"text_delta,omitempty"`
	Delta          string        `json:"delta,omitempty"`
	Content        string        `json:"content,omitempty"`
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func (e *StepUpdateEvent) ResolvedType() string {
	if e == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(firstNonEmpty(e.StepType, e.Type)))
}

func (e *StepUpdateEvent) ResolvedToolName() string {
	if e == nil {
		return ""
	}
	return strings.TrimSpace(firstNonEmpty(e.ToolName, e.Name))
}

func (e *StepUpdateEvent) ResolvedCommandLine() string {
	if e == nil || e.ToolInfo == nil {
		return ""
	}
	p := e.ToolInfo.Parameters
	return firstNonEmpty(p.CommandLine, p.CommandLineSnake, p.Command, p.Cmd)
}

func (e *StepUpdateEvent) ResolvedCommandName() string {
	return ExtractCommandName(e.ResolvedCommandLine())
}

func isShellOperatorToken(tok string) bool {
	return tok == "&&" || tok == "||" || tok == ";" || tok == "|" || tok == "&"
}

func isCommandWrapperToken(tok string) bool {
	return tok == "sudo" || tok == "nohup" || tok == "time" || tok == "exec"
}

func isEnvAssignmentToken(tok string) bool {
	eq := strings.IndexByte(tok, '=')
	return eq > 0 && isPOSIXIdentifier(tok[:eq])
}

func consumeEnvCommandFlags(tokens []string, i int) (nextIdx int, isBareEnv bool) {
	for i++; i < len(tokens) && strings.HasPrefix(tokens[i], "-"); i++ {
		if tokens[i] != "-i" && tokens[i] != "--ignore-environment" {
			return i, true
		}
	}
	return i, false
}

func isValidCommandCandidate(candidate string) bool {
	return candidate != "." && candidate != ".." && len(candidate) > 0 && len(candidate) <= 24 && safeCmdRegex.MatchString(candidate)
}

func ExtractCommandName(cmdLine string) string {
	tokens, ok := tokenizeCommandLine(strings.TrimSpace(cmdLine))
	if !ok || len(tokens) == 0 {
		return ""
	}

	i := 0
	for i < len(tokens) {
		tok := tokens[i]
		if isShellOperatorToken(tok) {
			return ""
		}
		if isEnvAssignmentToken(tok) || isCommandWrapperToken(tok) {
			i++
			continue
		}
		if tok == "env" {
			var isBareEnv bool
			i, isBareEnv = consumeEnvCommandFlags(tokens, i)
			if isBareEnv {
				return "env"
			}
			continue
		}
		break
	}

	if i >= len(tokens) {
		return ""
	}
	candidate := path.Base(strings.ReplaceAll(tokens[i], "\\", "/"))
	if !isValidCommandCandidate(candidate) {
		return ""
	}
	return candidate
}

func consumeEscapedRune(runes []rune, i int, inDouble bool, cur *strings.Builder) (nextIdx int, ok bool) {
	if i+1 >= len(runes) {
		return i, false
	}
	next := runes[i+1]
	if inDouble {
		if next == '$' || next == '`' || next == '"' || next == '\\' || next == '\n' {
			cur.WriteRune(next)
			return i + 1, true
		}
		cur.WriteRune('\\')
		return i, true
	}
	if strings.ContainsRune(" \t\n\r\"'\\", next) {
		cur.WriteRune(next)
		return i + 1, true
	}
	cur.WriteRune('\\')
	return i, true
}

func isTokenWhitespace(r rune) bool {
	return r == ' ' || r == '\t' || r == '\n' || r == '\r'
}

func appendNonEmptyToken(tokens []string, cur *strings.Builder) []string {
	if cur.Len() > 0 {
		tokens = append(tokens, cur.String())
		cur.Reset()
	}
	return tokens
}

func tokenizeCommandLine(s string) ([]string, bool) {
	var tokens []string
	var cur strings.Builder
	inSingle, inDouble := false, false
	runes := []rune(s)
	n := len(runes)

	for i := 0; i < n; i++ {
		r := runes[i]
		if inSingle {
			if r == '\'' {
				inSingle = false
			} else {
				cur.WriteRune(r)
			}
			continue
		}
		if r == '\\' {
			var ok bool
			i, ok = consumeEscapedRune(runes, i, inDouble, &cur)
			if !ok {
				return nil, false
			}
			continue
		}
		if r == '\'' && !inDouble {
			inSingle = true
			continue
		}
		if r == '"' {
			inDouble = !inDouble
			continue
		}
		if !inDouble && isTokenWhitespace(r) {
			tokens = appendNonEmptyToken(tokens, &cur)
			continue
		}
		cur.WriteRune(r)
	}
	if inSingle || inDouble {
		return nil, false
	}
	tokens = appendNonEmptyToken(tokens, &cur)
	return tokens, true
}

func isPOSIXIdentifier(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i, r := range s {
		if r != '_' && (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (i == 0 || r < '0' || r > '9') {
			return false
		}
	}
	return true
}

type StepUpdateHandler func(ev *StepUpdateEvent)

type WatchdogOptions struct {
	HomeDir           string
	InactivityTimeout time.Duration
	MaxDuration       time.Duration
	TranscriptDirs    []string
	OutputFormat      string
	StepUpdateHandler StepUpdateHandler
	TargetID          string
	ExtraEnv          []string
}

// ExtractSessionID extracts a session UUID from agy output.
// Strict NDJSON lines are unmarshaled directly via json.Unmarshal; log-prefixed lines fall through to extractUUIDByRegex.
func ExtractSessionID(output string, _ time.Time) string {
	for _, rawLine := range strings.Split(output, "\n") {
		line := strings.TrimSpace(rawLine)
		var probe sessionProbe
		if strings.HasPrefix(line, "{") && json.Unmarshal([]byte(line), &probe) == nil && probe.extractUUID() != "" {
			return probe.extractUUID()
		}
	}
	return extractUUIDByRegex(output)
}

func isWatchdogMatch(s, key string) bool {
	lower := strings.ToLower(s)
	return strings.Contains(lower, key+" exceeded") || (strings.Contains(lower, "[watchdog]") && strings.Contains(lower, key))
}

func IsInactivityTimeout(errDetail, stderr string) bool {
	c := errDetail + "\n" + stderr
	return isWatchdogMatch(c, "inactivity timeout") || isWatchdogMatch(c, "inactivity")
}

var nonTransientKeywords = []string{
	"invalid api key", "invalid_api_key", "authentication failed", "unauthorized", "permission_denied", "permissiondenied",
	"unknown flag", "flag provided but not defined", "unknown command", "executable file not found", "command not found",
	"is not recognized as an internal or external command", "model not found", "panic:",
}

func isNonTransientError(s string) bool {
	if strings.Contains(strings.ToUpper(s), "MODEL_PLACEHOLDER_") {
		return false
	}
	lower := strings.ToLower(s)
	return hasKeyword(lower, nonTransientKeywords) || (strings.Contains(lower, "fork/exec") && (strings.Contains(lower, "no such file or directory") || strings.Contains(lower, "permission denied")))
}

func IsQuotaPause(errDetail, stderr string) bool {
	combined := strings.ToLower(errDetail + "\n" + stderr)
	if strings.Contains(combined, "individual quota reached") ||
		strings.Contains(combined, "please upgrade your subscription") ||
		strings.Contains(combined, "exhausted your capacity on this model") ||
		strings.Contains(combined, "brain bucket resets in") {
		return true
	}
	hasQuotaWord := strings.Contains(combined, "quota") ||
		strings.Contains(combined, "resource_exhausted") ||
		strings.Contains(combined, "resource has been exhausted")
	hasResetTimer := strings.Contains(combined, "resets in") || strings.Contains(combined, "reset after")
	return hasQuotaWord && (hasResetTimer || strings.Contains(combined, "upgrade your subscription"))
}

func hasCapacityPhrases(combined string) bool {
	if strings.Contains(combined, "individual quota reached") || strings.Contains(combined, "upgrade your subscription") || strings.Contains(combined, "exceeded your current quota") {
		return false
	}
	return strings.Contains(combined, "capacity on this model") || strings.Contains(combined, "exhausted your capacity") || strings.Contains(combined, "rate limit reached") || strings.Contains(combined, "rate limit exceeded")
}

func IsCapacityBlip(errDetail, stderr string) bool {
	combined := strings.ToLower(errDetail + "\n" + stderr)
	if hasCapacityPhrases(combined) {
		return true
	}
	if strings.Contains(combined, "individual quota reached") || strings.Contains(combined, "upgrade your subscription") || strings.Contains(combined, "exceeded your current quota") {
		return false
	}
	dur, exact := ExtractQuotaResetDuration(errDetail, stderr)
	return exact && dur <= 5*time.Second
}

func ExtractQuotaResetDuration(errDetail, stderr string) (time.Duration, bool) {
	combined := strings.ToLower(errDetail + "\n" + stderr)
	match := reQuotaResetIn.FindStringSubmatch(errDetail + "\n" + stderr)
	if len(match) > 1 {
		raw := reWhitespace.ReplaceAllString(quotaUnitReplacer.Replace(strings.ToLower(strings.TrimSpace(match[1]))), "")
		if d, err := time.ParseDuration(strings.Trim(raw, ".\"')[] ")); err == nil {
			if d < 0 {
				return 0, true
			}
			if d > 24*time.Hour {
				return 24 * time.Hour, true
			}
			return d, true
		}
	}
	if hasCapacityPhrases(combined) {
		return 70 * time.Second, false
	}
	return 20 * time.Minute, false
}

func IsYieldTrap(exitCode int, stdout, stderr string) (bool, int) {
	if exitCode != 0 {
		return false, 0
	}
	for _, output := range []string{stderr, stdout} {
		if match := reTerminatingBackgroundTasks.FindStringSubmatch(output); len(match) > 1 {
			count := 1
			if c, err := strconv.Atoi(match[1]); err == nil && c > 0 {
				count = c
			}
			return true, count
		}
	}
	return false, 0
}
