package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	reStrictUUID     = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	reUUIDInText     = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	reUpdateStream   = regexp.MustCompile(`Starting conversation update stream for ([^\s\r\n]+)`)
	reGeneralSession = regexp.MustCompile(`(?i)(?:conversation|session)(?:_id)?[:\s=]+([a-zA-Z0-9\-]+)`)
	reQuotaResetIn   = regexp.MustCompile(`(?i)(?:resets in|reset after)\s+([-0-9a-zA-Z\s]+?)(?:\.|\n|\r|$)`)
	reWhitespace     = regexp.MustCompile(`\s+`)
	reFatalError     = regexp.MustCompile(`(?i)(?:fatal|panic|traceback|terminated due to error|error:\s+[^\n\r]+)`)
	safeCmdRegex     = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]*$`)
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

type ActivityWriter struct {
	mu           sync.Mutex
	buf          bytes.Buffer
	lastActivity atomic.Int64
	sessionID    atomic.Pointer[string]
}

func NewActivityWriter(initialSessionID string) *ActivityWriter {
	w := &ActivityWriter{}
	w.lastActivity.Store(time.Now().UnixNano())
	if initialSessionID != "" {
		w.sessionID.Store(&initialSessionID)
	}
	return w
}

func (w *ActivityWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	w.lastActivity.Store(time.Now().UnixNano())
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.buf.Write(p)
	if w.sessionID.Load() == nil {
		window := w.buf.Bytes()
		if len(window) > 512 {
			window = window[len(window)-512:]
		}
		if first := bytes.IndexByte(window, '{'); first >= 0 {
			if last := bytes.LastIndexByte(window, '}'); last > first {
				var probe sessionProbe
				if err := json.NewDecoder(bytes.NewReader(window[first : last+1])).Decode(&probe); err == nil {
					if id := probe.extractUUID(); id != "" {
						w.sessionID.Store(&id)
					}
				}
			}
		}
		if w.sessionID.Load() == nil {
			if sess := extractUUIDByRegex(string(window)); sess != "" {
				w.sessionID.Store(&sess)
			}
		}
	}
	return n, err
}

func (w *ActivityWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func (w *ActivityWriter) ResetBuffer() {
	if w != nil {
		w.mu.Lock()
		defer w.mu.Unlock()
		w.buf.Reset()
	}
}

func (w *ActivityWriter) LastActivity() time.Time { return time.Unix(0, w.lastActivity.Load()) }

func (w *ActivityWriter) SessionID() string {
	if ptr := w.sessionID.Load(); ptr != nil {
		return *ptr
	}
	return ""
}

func (w *ActivityWriter) SetSessionID(id string) {
	if !IsValidUUID(id) {
		return
	}
	for {
		current := w.sessionID.Load()
		if (current != nil && *current != "") || w.sessionID.CompareAndSwap(current, &id) {
			return
		}
	}
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

func ParseAgyOutput(stdout string) (*AgyResponse, error) {
	trimmed := strings.TrimSpace(stdout)
	if trimmed == "" {
		return nil, fmt.Errorf("empty output")
	}

	var legacyResp AgyResponse
	if err := json.Unmarshal([]byte(trimmed), &legacyResp); err == nil && (legacyResp.Status != "" || legacyResp.Response != "" || legacyResp.ConversationID != "") {
		return &legacyResp, nil
	}

	var initConvID string
	var resultResp *AgyResponse
	for _, rawLine := range strings.Split(trimmed, "\n") {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		var probe sessionProbe
		if err := json.Unmarshal([]byte(line), &probe); err != nil {
			continue
		}
		if probe.Event == "init" {
			if id := probe.extractUUID(); id != "" {
				initConvID = id
			}
		} else if probe.Event == "result" {
			var res AgyResponse
			raw := []byte(line)
			if len(probe.Result) > 0 {
				raw = probe.Result
			}
			if json.Unmarshal(raw, &res) == nil {
				resultResp = &res
			}
			if resultResp != nil && resultResp.ConversationID == "" {
				resultResp.ConversationID = probe.extractUUID()
			}
		} else if id := probe.extractUUID(); id != "" && initConvID == "" {
			initConvID = id
		}
	}

	if resultResp == nil {
		if err := json.Unmarshal([]byte(trimmed), &legacyResp); err == nil {
			return &legacyResp, nil
		}
		return nil, fmt.Errorf("failed to parse agy output: stream-json missing result event")
	}
	if resultResp.ConversationID == "" && initConvID != "" {
		resultResp.ConversationID = initConvID
	}
	return resultResp, nil
}

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

func (e *StepUpdateEvent) ResolvedTextDelta() string {
	if e == nil {
		return ""
	}
	if d := firstNonEmpty(e.TextDelta, e.Delta); d != "" {
		return d
	}
	if e.ResolvedType() == "agent_response" {
		return e.Content
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

func (e *StepUpdateEvent) ResolvedCommandName() string { return ExtractCommandName(e.ResolvedCommandLine()) }

func ExtractCommandName(cmdLine string) string {
	tokens, ok := tokenizeCommandLine(strings.TrimSpace(cmdLine))
	if !ok || len(tokens) == 0 {
		return ""
	}

	i := 0
	for i < len(tokens) {
		tok := tokens[i]
		if tok == "&&" || tok == "||" || tok == ";" || tok == "|" || tok == "&" {
			return ""
		}
		if eq := strings.IndexByte(tok, '='); eq > 0 && isPOSIXIdentifier(tok[:eq]) {
			i++
			continue
		}
		if tok == "sudo" || tok == "nohup" || tok == "time" || tok == "exec" {
			i++
			continue
		}
		if tok == "env" {
			for i++; i < len(tokens) && strings.HasPrefix(tokens[i], "-"); i++ {
				if tokens[i] != "-i" && tokens[i] != "--ignore-environment" {
					return "env"
				}
			}
			continue
		}
		break
	}

	if i >= len(tokens) {
		return ""
	}
	candidate := path.Base(strings.ReplaceAll(tokens[i], "\\", "/"))
	if candidate == "." || candidate == ".." || len(candidate) == 0 || len(candidate) > 24 || !safeCmdRegex.MatchString(candidate) {
		return ""
	}
	return candidate
}

func tokenizeCommandLine(s string) ([]string, bool) {
	var tokens []string
	var cur strings.Builder
	inSingle, inDouble := false, false
	runes := []rune(s)
	n := len(runes)

	for i := 0; i < n; i++ {
		r := runes[i]
		if r == '\\' && !inSingle {
			if i+1 < n {
				next := runes[i+1]
				if inDouble && (next == '$' || next == '`' || next == '"' || next == '\\' || next == '\n') {
					cur.WriteRune(next)
					i++
					continue
				}
				if !inDouble && strings.ContainsRune(" \t\n\r\"'\\", next) {
					cur.WriteRune(next)
					i++
					continue
				}
				cur.WriteRune(r)
				continue
			}
			return nil, false
		}
		if r == '\'' && !inDouble {
			inSingle = !inSingle
			continue
		}
		if r == '"' && !inSingle {
			inDouble = !inDouble
			continue
		}
		if !inSingle && !inDouble && (r == ' ' || r == '\t' || r == '\n' || r == '\r') {
			if cur.Len() > 0 {
				tokens = append(tokens, cur.String())
				cur.Reset()
			}
			continue
		}
		cur.WriteRune(r)
	}
	if inSingle || inDouble {
		return nil, false
	}
	if cur.Len() > 0 {
		tokens = append(tokens, cur.String())
	}
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

func DefaultWatchdogOptions(timeoutMinutes int) WatchdogOptions {
	return WatchdogOptions{
		InactivityTimeout: 5 * time.Minute,
		MaxDuration:       time.Duration(timeoutMinutes) * time.Minute,
		OutputFormat:      "stream-json",
	}
}

type RunnerFunc func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error)

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

func isWatchdogInactivity(s string) bool { return isWatchdogMatch(s, "inactivity") }
func isWatchdogMaxDuration(s string) bool { return isWatchdogMatch(s, "max duration") }

func extractWatchdogDetail(source, fallbackKeyword string) string {
	for _, l := range strings.Split(source, "\n") {
		trimmed := strings.TrimSpace(l)
		if strings.Contains(trimmed, "[watchdog]") || strings.Contains(strings.ToLower(trimmed), fallbackKeyword) {
			if len(trimmed) > 200 {
				return trimmed[:197] + "..."
			}
			return trimmed
		}
	}
	return fallbackKeyword
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

func isMCPSessionError(s string) bool {
	lower := strings.ToLower(s)
	return strings.Contains(lower, "failed to connect (session id") || strings.Contains(lower, "calling \"initialize\"") || strings.Contains(lower, "server name") || strings.Contains(lower, "mcp server")
}

func checkCorruption(s string) bool {
	if hasContextWindowKeyword(s) {
		return true
	}
	if isMCPSessionError(s) {
		return false
	}
	return hasCorruptionKeyword(s)
}

func classifyErrorTarget(target string) (isCorrupt, isTransient bool) {
	lower := strings.ToLower(target)
	if checkCorruption(lower) || (!isMCPSessionError(lower) && strings.Contains(lower, "conversation not found")) {
		return true, false
	}
	return false, !isNonTransientError(lower)
}

func ClassifyError(exitCode int, stdout, stderr string) (isFailure bool, isTransient bool, isSessionCorruption bool, errDetail string) {
	trimmedStdout, trimmedStderr := strings.TrimSpace(stdout), strings.TrimSpace(stderr)

	if exitCode != 0 {
		if isWatchdogInactivity(trimmedStderr) {
			return true, false, false, extractWatchdogDetail(trimmedStderr, "inactivity timeout exceeded")
		}
		if isWatchdogMaxDuration(trimmedStderr) {
			return true, false, false, extractWatchdogDetail(trimmedStderr, "max duration exceeded")
		}
	}

	if exitCode == 0 {
		if trimmedStdout == "" {
			detail := extractErrorDetail(trimmedStderr, exitCode)
			if strings.HasPrefix(detail, "execution failed with exit code") || trimmedStderr == "" {
				detail = "process produced empty stdout"
			}
			return true, !containsFatalStderrError(trimmedStderr) && !isNonTransientError(trimmedStderr), false, detail
		}

		resp, parseErr := ParseAgyOutput(stdout)
		if parseErr != nil {
			errTarget := trimmedStderr
			if len(trimmedStdout) < 300 {
				errTarget += " " + trimmedStdout
			}
			lower := strings.ToLower(errTarget)
			if hasContextWindowKeyword(lower) {
				return true, false, true, "context window exceeded"
			}
			if checkCorruption(lower) {
				return true, false, true, extractErrorDetail(trimmedStderr, exitCode)
			}
			if containsFatalStderrError(trimmedStderr) || isNonTransientError(lower) {
				return true, false, false, extractErrorDetail(trimmedStderr, exitCode)
			}
			return true, true, false, fmt.Sprintf("invalid json response from runner: %v", parseErr)
		}

		if (resp.Status != "" && strings.ToUpper(resp.Status) != "SUCCESS") || resp.Error != "" {
			detail := resp.Error
			if detail == "" {
				detail = fmt.Sprintf("runner status: %s", resp.Status)
			}
			isCorrupt, isTrans := classifyErrorTarget(resp.Status + " " + resp.Error + " " + trimmedStderr)
			return true, isTrans, isCorrupt, detail
		}

		if trimmedStderr != "" && containsFatalStderrError(trimmedStderr) {
			return true, false, false, extractErrorDetail(trimmedStderr, exitCode)
		}

		if strings.TrimSpace(resp.Response) != "" {
			return false, false, false, ""
		}
		return true, false, false, "process produced empty response"
	}

	errorTarget := trimmedStderr
	if resp, err := ParseAgyOutput(stdout); err == nil && resp != nil && resp.Error != "" {
		errorTarget = resp.Status + " " + resp.Error + " " + trimmedStderr
		errDetail = resp.Error
	} else if errorTarget == "" && len(trimmedStdout) < 300 {
		errorTarget = trimmedStdout
	}

	if errDetail == "" {
		errDetail = extractErrorDetail(stderr, exitCode)
		if (errDetail == "" || strings.HasPrefix(errDetail, "execution failed with exit code")) && len(trimmedStdout) < 300 && trimmedStdout != "" {
			errDetail = trimmedStdout
		}
	}

	isSessionCorruption, isTransient = classifyErrorTarget(errorTarget)
	return true, isTransient, isSessionCorruption, errDetail
}

func containsFatalStderrError(stderr string) bool { return stderr != "" && reFatalError.MatchString(stderr) }

func extractErrorDetail(stderr string, exitCode int) string {
	fallback := fmt.Sprintf("execution failed with exit code %d", exitCode)
	if stderr == "" {
		return fallback
	}
	var lastLine string
	for _, l := range strings.Split(stderr, "\n") {
		trimmed := strings.TrimSpace(l)
		if trimmed != "" && !strings.Contains(trimmed, "Starting conversation update stream") && !strings.HasPrefix(trimmed, "DEBUG") && !strings.HasPrefix(trimmed, "INFO") {
			lastLine = trimmed
		}
	}
	if lastLine == "" {
		return fallback
	}
	if len(lastLine) > 200 {
		return lastLine[:197] + "..."
	}
	return lastLine
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
