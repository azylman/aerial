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

	"github.com/azylman/aerial/brain/pkg/metrics"
)

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
	n, err := w.buf.Write(p)
	if w.sessionID.Load() == nil {
		if match := reUpdateStream.FindSubmatch(p); len(match) > 1 {
			sess := strings.TrimSpace(string(match[1]))
			w.sessionID.Store(&sess)
		} else if match := reGeneralSession.FindSubmatch(p); len(match) > 1 {
			sess := strings.TrimSpace(string(match[1]))
			w.sessionID.Store(&sess)
		}
	}
	w.mu.Unlock()

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
func ParseAgyOutput(stdout string) (*AgyResponse, error) {
	trimmed := strings.TrimSpace(stdout)
	if trimmed == "" {
		return nil, fmt.Errorf("empty output")
	}
	var resp AgyResponse
	if err := json.Unmarshal([]byte(trimmed), &resp); err != nil {
		return nil, fmt.Errorf("failed to parse agy json output: %w", err)
	}
	return &resp, nil
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
}

// activityTap wraps an io.Writer and bumps the ActivityWriter timestamp on every write.
type activityTap struct {
	w         io.Writer
	actWriter *ActivityWriter
}

func (t *activityTap) Write(p []byte) (int, error) {
	if len(p) > 0 {
		t.actWriter.lastActivity.Store(time.Now().UnixNano())
	}
	return t.w.Write(p)
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

	args := []string{"--dangerously-skip-permissions", "--output-format", "json"}
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
	env := append(os.Environ(),
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
	cmd.Stdout = &activityTap{w: &outBuf, actWriter: actWriter}
	cmd.Stderr = actWriter

	configureSysProcAttr(cmd)

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
	defer stopWatchdog()

	go func() {
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
	stopWatchdog()
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
	} else {
		exitCode = 0
	}

	return stdout, stderr, exitCode, err
}

// RunAgy executes the agy binary with the given parameters, capturing stdout and stderr.
func RunAgy(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
	return RunAgyWithWatchdog(ctx, agyBin, prompt, sessionID, apiKey, model, DefaultWatchdogOptions(timeoutMinutes))
}

var (
	reUpdateStream   = regexp.MustCompile(`Starting conversation update stream for ([^\s\r\n]+)`)
	reGeneralSession = regexp.MustCompile(`(?i)(?:conversation|session)(?:_id)?[:\s=]+([a-zA-Z0-9\-]+)`)
)

// ExtractSessionID searches stderr for an active session/conversation UUID.
func ExtractSessionID(stderr string, _ time.Time) string {
	if stderr != "" {
		if match := reUpdateStream.FindStringSubmatch(stderr); len(match) > 1 {
			return strings.TrimSpace(match[1])
		}

		if match := reGeneralSession.FindStringSubmatch(stderr); len(match) > 1 {
			return strings.TrimSpace(match[1])
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

	// 1. Intercept watchdog timeout diagnoses FIRST across all exit codes (non-transient, non-corruption failure)
	if isWatchdogInactivity(combined) {
		source := stderr
		if source == "" {
			source = stdout
		}
		return true, false, false, extractWatchdogDetail(source, "inactivity timeout exceeded")
	}
	if isWatchdogMaxDuration(combined) {
		source := stderr
		if source == "" {
			source = stdout
		}
		return true, false, false, extractWatchdogDetail(source, "max duration exceeded")
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
			return true, false, false, errDetail
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
			errTarget := resp.Error + " " + resp.Response + " " + trimmedStderr
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
	if errDetail == "" && trimmedStdout != "" {
		errDetail = trimmedStdout
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
