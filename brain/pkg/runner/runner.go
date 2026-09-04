package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
	"unicode"
)

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

// IsSilentSentinel checks whether stdout is empty or represents a silent sentinel like [NO_REPLY].
// It strips leading/trailing whitespace, backticks, quotes, asterisks, and punctuation.
// Returns true if empty string or if the normalized string starts with [NO_REPLY] (case-insensitive).
// Returns false for visible conversational responses.
func IsSilentSentinel(stdout string) bool {
	trimmed := strings.TrimFunc(stdout, func(r rune) bool {
		if r == '[' || r == ']' {
			return false
		}
		return unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r)
	})
	if trimmed == "" {
		return true
	}
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "[no_reply]") {
		rem := strings.TrimPrefix(lower, "[no_reply]")
		rem = strings.TrimFunc(rem, func(r rune) bool {
			return unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r)
		})
		return rem == ""
	}
	return false
}

// RunAgy executes the agy binary with the given parameters, capturing stdout and stderr.
func RunAgy(ctx context.Context, agyBin, prompt, sessionID, apiKey, model string, timeoutMinutes int) (stdout, stderr string, exitCode int, err error) {
	if agyBin == "" {
		agyBin = "agy"
	}

	args := []string{"--dangerously-skip-permissions", "--output-format", "json"}
	if model != "" {
		args = append(args, "--model", model)
	}
	if timeoutMinutes > 0 {
		args = append(args, "--print-timeout", fmt.Sprintf("%dm", timeoutMinutes))
	}
	if sessionID != "" {
		args = append(args, "--conversation", sessionID)
	}
	args = append(args, "-p", prompt)

	cmd := exec.CommandContext(ctx, agyBin, args...)
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

	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	configureSysProcAttr(cmd)

	runErr := cmd.Run()
	stdout = outBuf.String()
	stderr = errBuf.String()

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

// ClassifyError categorizes execution results into failure, transient, and session corruption states.
func ClassifyError(exitCode int, stdout, stderr string) (isFailure bool, isTransient bool, isSessionCorruption bool, errDetail string) {
	trimmedStdout := strings.TrimSpace(stdout)
	trimmedStderr := strings.TrimSpace(stderr)

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
			combined := strings.ToLower(trimmedStderr)
			for _, kw := range contextWindowKeywords {
				if strings.Contains(combined, kw) {
					return true, false, true, "context window exceeded"
				}
			}
			for _, kw := range corruptionKeywords {
				if strings.Contains(combined, kw) {
					return true, false, true, extractErrorDetail(trimmedStderr, exitCode)
				}
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
			errTarget := strings.ToLower(resp.Error + " " + resp.Response + " " + trimmedStderr)
			for _, kw := range contextWindowKeywords {
				if strings.Contains(errTarget, kw) {
					isSessionCorruption = true
					break
				}
			}
			if !isSessionCorruption {
				for _, kw := range corruptionKeywords {
					if strings.Contains(errTarget, kw) {
						isSessionCorruption = true
						break
					}
				}
			}
			for _, kw := range transientKeywords {
				if strings.Contains(errTarget, kw) {
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
			errTarget := strings.ToLower(resp.Error + " " + trimmedStderr)
			for _, kw := range contextWindowKeywords {
				if strings.Contains(errTarget, kw) {
					isSessionCorruption = true
					break
				}
			}
			if !isSessionCorruption {
				for _, kw := range corruptionKeywords {
					if strings.Contains(errTarget, kw) {
						isSessionCorruption = true
						break
					}
				}
			}
			for _, kw := range transientKeywords {
				if strings.Contains(errTarget, kw) {
					isTransient = true
					break
				}
			}
			return isFailure, isTransient, isSessionCorruption, resp.Error
		}

		if trimmedStderr != "" && containsFatalStderrError(trimmedStderr) {
			return true, false, false, extractErrorDetail(trimmedStderr, exitCode)
		}

		return false, false, false, ""
	}

	// Exit code != 0
	isFailure = true
	combined := strings.ToLower(stdout + "\n" + stderr)

	for _, kw := range contextWindowKeywords {
		if strings.Contains(combined, kw) {
			isSessionCorruption = true
			break
		}
	}

	if !isSessionCorruption {
		for _, kw := range corruptionKeywords {
			if strings.Contains(combined, kw) {
				isSessionCorruption = true
				break
			}
		}
		if strings.Contains(combined, "conversation not found") {
			isSessionCorruption = true
		}
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

func extractErrorDetail(stderr string, exitCode int) string {
	lines := strings.Split(stderr, "\n")
	for _, l := range lines {
		lTrim := strings.TrimSpace(l)
		if lTrim != "" && (strings.Contains(strings.ToLower(lTrim), "error") || strings.Contains(strings.ToLower(lTrim), "fail") || strings.Contains(strings.ToLower(lTrim), "503") || strings.Contains(strings.ToLower(lTrim), "panic") || strings.Contains(strings.ToLower(lTrim), "terminated")) {
			return lTrim
		}
	}
	trimmedStderr := strings.TrimSpace(stderr)
	if trimmedStderr != "" {
		return trimmedStderr
	}
	return fmt.Sprintf("execution failed with exit code %d", exitCode)
}

func containsFatalStderrError(stderr string) bool {
	lower := strings.ToLower(stderr)
	fatalIndicators := []string{
		"panic:",
		"runtime error",
		"fatal error",
		"agent execution terminated",
		"fatal:",
	}
	for _, ind := range fatalIndicators {
		if strings.Contains(lower, ind) {
			return true
		}
	}
	return false
}

