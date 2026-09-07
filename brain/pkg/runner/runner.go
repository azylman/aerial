package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/metrics"
)

var (
	reStrictUUID        = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	reUUIDInText        = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	reUpdateStream      = regexp.MustCompile(`Starting conversation update stream for ([^\s\r\n]+)`)
	reGeneralSession    = regexp.MustCompile(`(?i)(?:conversation|session)(?:_id)?[:\s=]+([a-zA-Z0-9\-]+)`)
	reNDJSONInitSession = regexp.MustCompile(`"event"\s*:\s*"init"[^}]*"conversation_id"\s*:\s*"([^"]+)"`)
)

// IsValidUUID validates that a string strictly matches RFC 4122 UUID format.
func IsValidUUID(s string) bool {
	return reStrictUUID.MatchString(s)
}

// ActivityWriter is a thread-safe buffer that tracks write activity timestamps
// and sniffs conversation UUIDs from stderr streams.
type ActivityWriter struct {
	mu           sync.Mutex
	buf          bytes.Buffer
	lastActivity atomic.Int64 // UnixNano
	sessionID    atomic.Pointer[string]
}

// NewActivityWriter initializes an ActivityWriter with an optional initial session ID.
func NewActivityWriter(initialSessionID string) *ActivityWriter {
	w := &ActivityWriter{}
	w.lastActivity.Store(time.Now().UnixNano())
	if initialSessionID != "" {
		w.sessionID.Store(&initialSessionID)
	}
	return w
}

// Write appends bytes to the internal buffer, updates the activity timestamp,
// and extracts the conversation session ID if not already discovered.
func (w *ActivityWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	w.lastActivity.Store(time.Now().UnixNano())

	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.buf.Write(p)
	if w.sessionID.Load() == nil {
		bufBytes := w.buf.Bytes()
		window := bufBytes
		if len(window) > 512 {
			window = window[len(window)-512:]
		}
		if match := reUpdateStream.FindSubmatch(window); len(match) > 1 {
			sess := strings.TrimSpace(string(match[1]))
			if IsValidUUID(sess) {
				w.sessionID.Store(&sess)
			}
		} else if match := reGeneralSession.FindSubmatch(window); len(match) > 1 {
			sess := strings.TrimSpace(string(match[1]))
			if IsValidUUID(sess) {
				w.sessionID.Store(&sess)
			}
		} else if match := reUUIDInText.Find(window); len(match) > 0 {
			sess := strings.TrimSpace(string(match))
			if IsValidUUID(sess) {
				w.sessionID.Store(&sess)
			}
		}
	}

	return n, err
}

// String returns the accumulated buffered output under mutex protection.
func (w *ActivityWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// LastActivity returns the timestamp of the most recent write.
func (w *ActivityWriter) LastActivity() time.Time {
	return time.Unix(0, w.lastActivity.Load())
}

// SessionID returns the dynamically extracted or initial session UUID.
func (w *ActivityWriter) SessionID() string {
	ptr := w.sessionID.Load()
	if ptr == nil {
		return ""
	}
	return *ptr
}

// SetSessionID atomically latches the session UUID if not already set.
func (w *ActivityWriter) SetSessionID(id string) {
	if !IsValidUUID(id) {
		return
	}
	for {
		current := w.sessionID.Load()
		if current != nil && *current != "" {
			return
		}
		if w.sessionID.CompareAndSwap(current, &id) {
			return
		}
	}
}

// AgyResponse models the top-level structured output of agy --output-format json.
type AgyResponse struct {
	ConversationID  string   `json:"conversation_id"`
	Status          string   `json:"status"`
	Response        string   `json:"response"`
	DurationSeconds float64  `json:"duration_seconds"`
	NumTurns        int      `json:"num_turns"`
	Usage           AgyUsage `json:"usage"`
	Error           string   `json:"error,omitempty"`
}

// AgyUsage models token usage telemetry in AgyResponse.
type AgyUsage struct {
	InputTokens     int `json:"input_tokens"`
	OutputTokens    int `json:"output_tokens"`
	ThinkingTokens  int `json:"thinking_tokens"`
	CacheReadTokens int `json:"cache_read_tokens"`
	TotalTokens     int `json:"total_tokens"`
}

// ParseAgyOutput unmarshals raw stdout into an AgyResponse struct.
// It supports both legacy single-line JSON and stream-json NDJSON streams.
func ParseAgyOutput(stdout string) (*AgyResponse, error) {
	trimmed := strings.TrimSpace(stdout)
	if trimmed == "" {
		return nil, fmt.Errorf("empty output")
	}

	// Fast path: attempt direct unmarshaling as legacy single-line JSON.
	var legacyResp AgyResponse
	if err := json.Unmarshal([]byte(trimmed), &legacyResp); err == nil && (legacyResp.Status != "" || legacyResp.Response != "" || legacyResp.ConversationID != "") {
		return &legacyResp, nil
	}

	// Stream-json NDJSON path: process line by line.
	// Uses strings.Split to avoid bufio.Scanner's default 64KB token limit on large tool responses.
	var initConvID string
	var resultResp *AgyResponse

	lines := strings.Split(trimmed, "\n")
	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}

		if !strings.Contains(line, `"event"`) {
			continue
		}

		var ev struct {
			Event          string          `json:"event"`
			ConversationID string          `json:"conversation_id,omitempty"`
			Result         json.RawMessage `json:"result,omitempty"`
		}
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}

		switch ev.Event {
		case "init":
			if ev.ConversationID != "" {
				initConvID = ev.ConversationID
			}
		case "result":
			var res AgyResponse
			if len(ev.Result) > 0 {
				if err := json.Unmarshal(ev.Result, &res); err == nil {
					resultResp = &res
				}
			} else {
				if err := json.Unmarshal([]byte(line), &res); err == nil {
					resultResp = &res
				}
			}
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

// IsSilentSentinel checks whether stdout is empty or consists solely of whitespace.
// Returns true if empty string or whitespace.
// Returns false for visible conversational responses.
func IsSilentSentinel(stdout string) bool {
	return strings.TrimSpace(stdout) == ""
}

var (
	ErrInactivityTimeout = errors.New("watchdog: inactivity timeout exceeded")
	ErrMaxDuration       = errors.New("watchdog: max duration exceeded")
)

// WatchdogOptions configures execution timeouts and activity polling behavior.
type WatchdogOptions struct {
	InactivityTimeout time.Duration
	MaxDuration       time.Duration
	PollInterval      time.Duration
	TranscriptDirs    []string
	OutputFormat      string
}

// activityTap wraps an io.Writer, bumps the ActivityWriter timestamp on every write,
// parses stream-json events across chunk boundaries, and filters high-volume intermediate events.
type activityTap struct {
	mu           sync.Mutex
	w            io.Writer
	actWriter    *ActivityWriter
	filterStream bool
	remainder    []byte
}

func newActivityTap(w io.Writer, actWriter *ActivityWriter, filterStream bool) *activityTap {
	return &activityTap{
		w:            w,
		actWriter:    actWriter,
		filterStream: filterStream,
	}
}

func (t *activityTap) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if t.actWriter != nil {
		t.actWriter.lastActivity.Store(time.Now().UnixNano())
	}

	if !t.filterStream {
		if t.w != nil {
			return t.w.Write(p)
		}
		return len(p), nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	combined := append(t.remainder, p...)
	start := 0

	for i := 0; i < len(combined); i++ {
		if combined[i] == '\n' {
			line := combined[start : i+1]
			t.processLine(line)
			start = i + 1
		}
	}

	if start < len(combined) {
		t.remainder = make([]byte, len(combined)-start)
		copy(t.remainder, combined[start:])
	} else {
		t.remainder = nil
	}

	// MUST strictly return (len(p), nil) to satisfy io.Writer contract and prevent
	// os/exec's internal io.Copy from aborting the child process with io.ErrShortWrite.
	return len(p), nil
}

func (t *activityTap) processLine(line []byte) {
	trimmed := bytes.TrimSpace(line)
	if len(trimmed) == 0 {
		return
	}

	// Sniff session ID from {"event":"init","conversation_id":"..."}
	if bytes.Contains(trimmed, []byte(`"event"`)) && bytes.Contains(trimmed, []byte(`"init"`)) {
		if t.actWriter != nil {
			if match := reNDJSONInitSession.FindSubmatch(trimmed); len(match) > 1 {
				sessID := strings.TrimSpace(string(match[1]))
				if IsValidUUID(sessID) {
					t.actWriter.SetSessionID(sessID)
				}
			}
		}
		if t.w != nil {
			_, _ = t.w.Write(line)
		}
		return
	}

	// Preserve final result event in the output buffer
	if bytes.Contains(trimmed, []byte(`"event"`)) && bytes.Contains(trimmed, []byte(`"result"`)) {
		if t.w != nil {
			_, _ = t.w.Write(line)
		}
		return
	}

	// Filter out high-volume step_update events from the output buffer to prevent OOM
	if bytes.Contains(trimmed, []byte(`"event"`)) && bytes.Contains(trimmed, []byte(`"step_update"`)) {
		return
	}

	// Pass through any other output (e.g. non-event output, legacy json)
	if t.w != nil {
		_, _ = t.w.Write(line)
	}
}

// Flush writes any remaining trailing fragment when the stream closes.
func (t *activityTap) Flush() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.remainder) > 0 {
		t.processLine(t.remainder)
		t.remainder = nil
	}
}

// DefaultWatchdogOptions returns sensible defaults for watchdog liveness tracking.
func DefaultWatchdogOptions(timeoutMinutes int) WatchdogOptions {
	maxDur := 60 * time.Minute
	if timeoutMinutes > 0 {
		maxDur = time.Duration(timeoutMinutes) * time.Minute
	}
	return WatchdogOptions{
		InactivityTimeout: 5 * time.Minute,
		MaxDuration:       maxDur,
		PollInterval:      3 * time.Second,
	}
}

// RunAgyWithWatchdog executes the agy binary with dynamic stderr, stdout, and transcript liveness monitoring.
func RunAgyWithWatchdog(parentCtx context.Context, agyBin, prompt, sessionID, apiKey, model string, opts WatchdogOptions) (stdout, stderr string, exitCode int, err error) {
	if opts.InactivityTimeout <= 0 {
		opts.InactivityTimeout = 5 * time.Minute
	}
	if opts.MaxDuration <= 0 {
		opts.MaxDuration = 60 * time.Minute
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = 3 * time.Second
	}

	start := time.Now()
	defer func() {
		status := "success"
		if err != nil || exitCode != 0 {
			status = "error"
		}
		metrics.RecordRunnerExecution(status, model, time.Since(start))
	}()

	if agyBin == "" {
		agyBin = "agy"
	}

	outputFmt := opts.OutputFormat
	if outputFmt == "" {
		outputFmt = "stream-json"
	}

	args := []string{"--dangerously-skip-permissions", "--output-format", outputFmt}
	if model != "" {
		args = append(args, "--model", model)
	}
	if opts.MaxDuration > 0 {
		if opts.MaxDuration >= time.Minute {
			args = append(args, "--print-timeout", fmt.Sprintf("%dm", int(opts.MaxDuration.Minutes())))
		} else {
			args = append(args, "--print-timeout", fmt.Sprintf("%ds", int(opts.MaxDuration.Seconds())))
		}
	}
	if sessionID != "" {
		args = append(args, "--conversation", sessionID)
	}
	args = append(args, "-p", prompt)

	runCtx, runCancel := context.WithCancel(parentCtx)
	defer runCancel()

	cmd := exec.CommandContext(runCtx, agyBin, args...)
	if _, statErr := os.Stat("/share/aerial"); statErr == nil {
		cmd.Dir = "/share/aerial"
	} else if _, statErr := os.Stat("/app"); statErr == nil {
		cmd.Dir = "/app"
	} else {
		cmd.Dir = "."
	}
	cmd.Stdin = strings.NewReader("")
	env := append(cmd.Environ(),
		"GIT_TERMINAL_PROMPT=0",
		"AGY_LOG_LEVEL=debug",
		"ANTIGRAVITY_LOG_LEVEL=debug",
	)
	if apiKey != "" {
		env = append(env,
			"GEMINI_API_KEY="+apiKey,
			"ANTIGRAVITY_API_KEY="+apiKey,
			"GOOGLE_GENAI_API_KEY="+apiKey,
		)
	}
	cmd.Env = env

	var outBuf bytes.Buffer
	actWriter := NewActivityWriter(sessionID)
	tap := newActivityTap(&outBuf, actWriter, outputFmt == "stream-json")
	cmd.Stdout = tap
	cmd.Stderr = actWriter

	configureSysProcAttr(cmd)

	// Pre-flight: ensure settings.json matches authentication mode prior to executing agy
	_ = config.EnsureAgySettings(apiKey, model)

	if startErr := cmd.Start(); startErr != nil {
		return "", actWriter.String(), -1, startErr
	}

	var watchdogReason atomic.Pointer[string]
	done := make(chan struct{})
	var doneOnce sync.Once
	stopWatchdog := func() {
		doneOnce.Do(func() {
			close(done)
		})
	}
	var watchdogWg sync.WaitGroup
	defer func() {
		stopWatchdog()
		watchdogWg.Wait()
	}()

	watchdogWg.Add(1)
	go func() {
		defer watchdogWg.Done()
		ticker := time.NewTicker(opts.PollInterval)
		defer ticker.Stop()

		type fileState struct {
			modTime time.Time
			size    int64
		}
		lastFileStates := make(map[string]fileState)

		homeDir, _ := os.UserHomeDir()
		if homeDir == "" {
			homeDir = "/root"
		}

		for {
			select {
			case <-done:
				return
			case <-runCtx.Done():
				return
			case <-ticker.C:
				activeSess := actWriter.SessionID()
				if activeSess != "" {
					var candidateDirs []string
					if len(opts.TranscriptDirs) > 0 {
						for _, d := range opts.TranscriptDirs {
							if strings.Contains(d, "%s") {
								candidateDirs = append(candidateDirs, fmt.Sprintf(d, activeSess))
							} else if strings.Contains(d, "{session}") {
								candidateDirs = append(candidateDirs, strings.ReplaceAll(d, "{session}", activeSess))
							} else {
								candidateDirs = append(candidateDirs, filepath.Join(d, activeSess, ".system_generated", "logs"))
							}
						}
					} else {
						candidateDirs = []string{
							filepath.Join("/data", "brain", activeSess, ".system_generated", "logs"),
							filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain", activeSess, ".system_generated", "logs"),
							filepath.Join(homeDir, ".gemini", "antigravity", "brain", activeSess, ".system_generated", "logs"),
						}
					}
					for _, dir := range candidateDirs {
						for _, filename := range []string{"transcript.jsonl", "transcript_full.jsonl"} {
							filePath := filepath.Join(dir, filename)
							info, statErr := os.Stat(filePath)
							if statErr == nil {
								prevState, exists := lastFileStates[filePath]
								if !exists {
									lastFileStates[filePath] = fileState{
										modTime: info.ModTime(),
										size:    info.Size(),
									}
									if info.ModTime().After(start) || info.Size() > 0 {
										actWriter.lastActivity.Store(time.Now().UnixNano())
									}
								} else if info.ModTime().After(prevState.modTime) || info.Size() != prevState.size {
									lastFileStates[filePath] = fileState{
										modTime: info.ModTime(),
										size:    info.Size(),
									}
									actWriter.lastActivity.Store(time.Now().UnixNano())
								}
							}
						}
					}
				}

				// Check inactivity timeout
				if time.Since(actWriter.LastActivity()) > opts.InactivityTimeout {
					reason := fmt.Sprintf("inactivity timeout exceeded (%v without output or transcript update)", opts.InactivityTimeout)
					watchdogReason.Store(&reason)
					runCancel()
					return
				}

				// Check max duration cap
				if time.Since(start) > opts.MaxDuration {
					reason := fmt.Sprintf("max duration exceeded (%v total duration cap)", opts.MaxDuration)
					watchdogReason.Store(&reason)
					runCancel()
					return
				}
			}
		}
	}()

	runErr := cmd.Wait()
	tap.Flush()
	stopWatchdog()
	watchdogWg.Wait()
	stdout = outBuf.String()
	stderr = actWriter.String()

	if reasonPtr := watchdogReason.Load(); reasonPtr != nil {
		reason := *reasonPtr
		if stderr != "" && !strings.HasSuffix(stderr, "\n") {
			stderr += "\n"
		}
		stderr += fmt.Sprintf("[watchdog] %s\n", reason)
		exitCode = -1
		if strings.Contains(reason, "inactivity timeout exceeded") {
			err = ErrInactivityTimeout
		} else if strings.Contains(reason, "max duration exceeded") {
			err = ErrMaxDuration
		} else {
			err = fmt.Errorf("watchdog: %s", reason)
		}
		return stdout, stderr, exitCode, err
	}

	if runErr != nil {
		if exitErr, ok := runErr.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
		}
		err = runErr
		if strings.Contains(strings.ToLower(stderr), "modelprovider is set to \"gemini\"") && apiKey == "" {
			_ = config.EnsureAgySettings("", model)
		}
	} else {
		exitCode = 0
	}

	return stdout, stderr, exitCode, err
}

// RunnerFunc defines the signature for invoking the underlying agent runner (e.g. RunAgy).
type RunnerFunc func(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error)

// RunAgy executes the agy binary with the given parameters, capturing stdout and stderr.
func RunAgy(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
	return RunAgyWithWatchdog(ctx, agyBin, prompt, sessionID, apiKey, model, DefaultWatchdogOptions(timeoutMinutes))
}

// ExtractSessionID searches output (stdout or stderr) for an active session/conversation UUID.
func ExtractSessionID(output string, _ time.Time) string {
	if output != "" {
		if match := reNDJSONInitSession.FindStringSubmatch(output); len(match) > 1 {
			candidate := strings.TrimSpace(match[1])
			if IsValidUUID(candidate) {
				return candidate
			}
		}

		if match := reUpdateStream.FindStringSubmatch(output); len(match) > 1 {
			candidate := strings.TrimSpace(match[1])
			if IsValidUUID(candidate) {
				return candidate
			}
		}

		if match := reGeneralSession.FindStringSubmatch(output); len(match) > 1 {
			candidate := strings.TrimSpace(match[1])
			if IsValidUUID(candidate) {
				return candidate
			}
		}

		if match := reUUIDInText.FindString(output); match != "" {
			return match
		}
	}

	return ""
}

// IsInactivityTimeout checks if the error message or stderr indicates an inactivity watchdog timeout.
func IsInactivityTimeout(errDetail, stderr string) bool {
	combined := strings.ToLower(errDetail + "\n" + stderr)
	if strings.Contains(combined, "inactivity timeout exceeded") {
		return true
	}
	if strings.Contains(combined, "[watchdog]") && strings.Contains(combined, "inactivity") {
		return true
	}
	return false
}

func isWatchdogInactivity(s string) bool {
	lower := strings.ToLower(s)
	return strings.Contains(lower, "inactivity timeout exceeded") ||
		(strings.Contains(lower, "[watchdog]") && strings.Contains(lower, "inactivity"))
}

func isWatchdogMaxDuration(s string) bool {
	lower := strings.ToLower(s)
	return strings.Contains(lower, "max duration exceeded") ||
		(strings.Contains(lower, "[watchdog]") && strings.Contains(lower, "max duration"))
}

func extractWatchdogDetail(source string, fallbackKeyword string) string {
	lines := strings.Split(source, "\n")
	for _, l := range lines {
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

// ClassifyError categorizes execution results into failure, transient, and session corruption states.
func ClassifyError(exitCode int, stdout, stderr string) (isFailure bool, isTransient bool, isSessionCorruption bool, errDetail string) {
	trimmedStdout := strings.TrimSpace(stdout)
	trimmedStderr := strings.TrimSpace(stderr)
	combined := strings.ToLower(trimmedStderr + "\n" + trimmedStdout)

	// 1. Intercept watchdog timeout diagnoses FIRST on non-zero exit codes (watchdog kills with SIGKILL / exit code -1)
	// Watchdog diagnostic markers are strictly emitted to stderr by the runner harness and must NEVER inspect stdout.
	if exitCode != 0 {
		if isWatchdogInactivity(trimmedStderr) {
			return true, false, false, extractWatchdogDetail(trimmedStderr, "inactivity timeout exceeded")
		}
		if isWatchdogMaxDuration(trimmedStderr) {
			return true, false, false, extractWatchdogDetail(trimmedStderr, "max duration exceeded")
		}
	}

	transientKeywords := []string{
		"error 503",
		"503 service unavailable",
		"status: unavailable",
		"high demand",
		"rate limit",
		"resource_exhausted",
		"429",
		"deadline_exceeded",
		"context deadline exceeded",
		"timeout",
		"connection reset by peer",
		"temporary failure in name resolution",
		"quota exceeded",
		"exceeded your current quota",
		"insufficient_quota",
		"too many requests",
		"process produced empty stdout",
		"empty stdout",
		"modelprovider is set to \"gemini\"",
		"modelprovider",
	}

	corruptionKeywords := []string{
		"session corrupt",
		"corrupted session",
		"invalid session",
		"session not found",
		"failed to load conversation",
		"corrupted transcript",
		"failed to parse session",
	}

	contextWindowKeywords := []string{
		"context window exceeded",
		"maximum context length",
		"token limit exceeded",
		"context length exceeded",
		"prompt is too long",
	}

	isMCPSessionError := func(s string) bool {
		lower := strings.ToLower(s)
		return strings.Contains(lower, "failed to connect (session id") ||
			strings.Contains(lower, "calling \"initialize\"") ||
			strings.Contains(lower, "server name") ||
			strings.Contains(lower, "mcp server")
	}

	checkCorruption := func(s string) bool {
		lower := strings.ToLower(s)
		for _, kw := range contextWindowKeywords {
			if strings.Contains(lower, kw) {
				return true
			}
		}
		if isMCPSessionError(lower) {
			return false
		}
		for _, kw := range corruptionKeywords {
			if strings.Contains(lower, kw) {
				return true
			}
		}
		return false
	}

	if exitCode == 0 {
		if trimmedStdout == "" {
			errDetail := extractErrorDetail(trimmedStderr, exitCode)
			if errDetail == fmt.Sprintf("execution failed with exit code %d", exitCode) || trimmedStderr == "" {
				errDetail = "process produced empty stdout"
			}
			if containsFatalStderrError(trimmedStderr) {
				return true, false, false, errDetail
			}
			return true, true, false, errDetail
		}

		resp, parseErr := ParseAgyOutput(stdout)
		if parseErr != nil {
			for _, kw := range contextWindowKeywords {
				if strings.Contains(combined, kw) {
					return true, false, true, "context window exceeded"
				}
			}
			if checkCorruption(combined) {
				return true, false, true, extractErrorDetail(trimmedStderr, exitCode)
			}
			for _, kw := range transientKeywords {
				if strings.Contains(combined, kw) {
					return true, true, false, extractErrorDetail(trimmedStderr, exitCode)
				}
			}
			if containsFatalStderrError(trimmedStderr) {
				return true, false, false, extractErrorDetail(trimmedStderr, exitCode)
			}
			return true, false, false, fmt.Sprintf("invalid json response from runner: %v", parseErr)
		}

		// Parsed JSON successfully
		if resp.Status != "" && strings.ToUpper(resp.Status) != "SUCCESS" {
			isFailure = true
			errTarget := resp.Error + " " + trimmedStderr
			errTargetLower := strings.ToLower(errTarget)
			if checkCorruption(errTargetLower) {
				isSessionCorruption = true
			}
			for _, kw := range transientKeywords {
				if strings.Contains(errTargetLower, kw) {
					isTransient = true
					break
				}
			}
			errDetail = resp.Error
			if errDetail == "" {
				errDetail = fmt.Sprintf("runner status: %s", resp.Status)
			}
			return isFailure, isTransient, isSessionCorruption, errDetail
		}

		if resp.Error != "" {
			isFailure = true
			errTarget := resp.Error + " " + trimmedStderr
			errTargetLower := strings.ToLower(errTarget)
			if checkCorruption(errTargetLower) {
				isSessionCorruption = true
			}
			for _, kw := range transientKeywords {
				if strings.Contains(errTargetLower, kw) {
					isTransient = true
					break
				}
			}
			return isFailure, isTransient, isSessionCorruption, resp.Error
		}

		if trimmedStderr != "" {
			if containsFatalStderrError(trimmedStderr) {
				return true, false, false, extractErrorDetail(trimmedStderr, exitCode)
			}
		}

		// CRITICAL ADVERSARIAL GUARD: If resp.Status == "SUCCESS" (or empty) and resp.Error == ""
		// with non-empty resp.Response, return clean success. Under no circumstances inspect
		// resp.Response for corruption keywords.
		return false, false, false, ""
	}

	// Non-zero exit code
	isFailure = true

	if checkCorruption(combined) || (!isMCPSessionError(combined) && strings.Contains(combined, "conversation not found")) {
		isSessionCorruption = true
	}

	for _, kw := range transientKeywords {
		if strings.Contains(combined, kw) {
			isTransient = true
			break
		}
	}

	errDetail = extractErrorDetail(stderr, exitCode)
	if (errDetail == "" || errDetail == fmt.Sprintf("execution failed with exit code %d", exitCode)) && trimmedStdout != "" {
		if resp, err := ParseAgyOutput(stdout); err == nil && resp.Error != "" {
			errDetail = resp.Error
		} else if len(trimmedStdout) < 300 {
			errDetail = trimmedStdout
		}
	}
	if errDetail == "" {
		errDetail = fmt.Sprintf("execution failed with exit code %d", exitCode)
	}

	return isFailure, isTransient, isSessionCorruption, errDetail
}

var (
	reFatalError = regexp.MustCompile(`(?i)(?:fatal|panic|traceback|terminated due to error|error:\s+[^\n\r]+)`)
)

func containsFatalStderrError(stderr string) bool {
	if stderr == "" {
		return false
	}
	return reFatalError.MatchString(stderr)
}

func extractErrorDetail(stderr string, exitCode int) string {
	if stderr == "" {
		return fmt.Sprintf("execution failed with exit code %d", exitCode)
	}

	lines := strings.Split(stderr, "\n")
	var significantLines []string
	for _, l := range lines {
		trimmed := strings.TrimSpace(l)
		if trimmed == "" {
			continue
		}
		if strings.Contains(trimmed, "Starting conversation update stream") {
			continue
		}
		if strings.HasPrefix(trimmed, "DEBUG") || strings.HasPrefix(trimmed, "INFO") {
			continue
		}
		significantLines = append(significantLines, trimmed)
	}

	if len(significantLines) > 0 {
		lastLine := significantLines[len(significantLines)-1]
		if len(lastLine) > 200 {
			return lastLine[:197] + "..."
		}
		return lastLine
	}

	return fmt.Sprintf("execution failed with exit code %d", exitCode)
}

var (
	reQuotaResetIn = regexp.MustCompile(`(?i)resets in\s+([0-9a-zA-Z\s]+?)(?:\.|\n|\r|$)`)
	reWhitespace   = regexp.MustCompile(`\s+`)
)

// IsQuotaPause detects Google subscription and resource exhaustion quota lockouts.
func IsQuotaPause(errDetail, stderr string) bool {
	combined := strings.ToLower(errDetail + "\n" + stderr)

	// Direct subscription quota markers
	if strings.Contains(combined, "individual quota reached") ||
		strings.Contains(combined, "please upgrade your subscription") ||
		strings.Contains(combined, "brain bucket resets in") {
		return true
	}

	// Quota / resource exhausted markers paired with a reset countdown or quota keywords
	hasQuotaWord := strings.Contains(combined, "quota") ||
		strings.Contains(combined, "resource_exhausted") ||
		strings.Contains(combined, "resource has been exhausted")
	hasResetTimer := strings.Contains(combined, "resets in")
	hasUpgradePrompt := strings.Contains(combined, "upgrade your subscription")

	if hasQuotaWord && (hasResetTimer || hasUpgradePrompt) {
		return true
	}

	return false
}

// ExtractQuotaResetDuration parses compound reset countdowns (e.g. "16m58s", "16m 58s", "1h 15m", "45s", "15 minutes").
// Normalizes unit words and strips internal whitespace. Clamps between 10s and 24h.
// Falls back to (20 * time.Minute, false) if missing or unparseable.
func ExtractQuotaResetDuration(errDetail, stderr string) (time.Duration, bool) {
	combined := errDetail + "\n" + stderr
	match := reQuotaResetIn.FindStringSubmatch(combined)
	if len(match) > 1 {
		raw := strings.TrimSpace(match[1])
		raw = strings.ToLower(raw)
		// Normalize unit words to standard Go duration units
		raw = strings.ReplaceAll(raw, "minutes", "m")
		raw = strings.ReplaceAll(raw, "minute", "m")
		raw = strings.ReplaceAll(raw, "mins", "m")
		raw = strings.ReplaceAll(raw, "min", "m")
		raw = strings.ReplaceAll(raw, "hours", "h")
		raw = strings.ReplaceAll(raw, "hour", "h")
		raw = strings.ReplaceAll(raw, "hrs", "h")
		raw = strings.ReplaceAll(raw, "hr", "h")
		raw = strings.ReplaceAll(raw, "seconds", "s")
		raw = strings.ReplaceAll(raw, "second", "s")
		raw = strings.ReplaceAll(raw, "secs", "s")
		raw = strings.ReplaceAll(raw, "sec", "s")
		raw = reWhitespace.ReplaceAllString(raw, "")

		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			// Safety clamping: min 10s, max 24h
			if d < 10*time.Second {
				d = 30 * time.Second
			} else if d > 24*time.Hour {
				d = 24 * time.Hour
			}
			return d, true
		}
	}
	return 20 * time.Minute, false
}
