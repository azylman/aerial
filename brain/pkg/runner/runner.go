package runner

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/azylman/aerial/brain/pkg/session"
)

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

	args := []string{"--dangerously-skip-permissions"}
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

// ExtractSessionID searches stderr for an active session/conversation UUID, falling back to disk discovery.
func ExtractSessionID(stderr string, startTime time.Time) string {
	if stderr != "" {
		if match := reUpdateStream.FindStringSubmatch(stderr); len(match) > 1 {
			return strings.TrimSpace(match[1])
		}

		if match := reGeneralSession.FindStringSubmatch(stderr); len(match) > 1 {
			return strings.TrimSpace(match[1])
		}
	}

	return session.FindLatestSessionDir(startTime)
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
		"conversation not found",
		"failed to load conversation",
		"corrupted transcript",
		"json: cannot unmarshal",
		"unexpected end of json input",
		"failed to parse session",
	}

	combined := strings.ToLower(stdout + "\n" + stderr)
	searchTarget := combined
	if exitCode == 0 {
		searchTarget = strings.ToLower(stderr)
	}

	for _, kw := range transientKeywords {
		if strings.Contains(searchTarget, kw) {
			isTransient = true
			break
		}
	}

	for _, kw := range corruptionKeywords {
		if strings.Contains(searchTarget, kw) {
			isSessionCorruption = true
			break
		}
	}

	if exitCode == 0 {
		if isSessionCorruption {
			isFailure = true
		} else if isTransient {
			isFailure = true
		} else if trimmedStderr != "" && containsFatalStderrError(trimmedStderr) {
			isFailure = true
		} else if IsSilentSentinel(stdout) || trimmedStderr == "" {
			return false, false, false, ""
		} else {
			return false, false, false, ""
		}
	} else {
		isFailure = true
	}

	// Extract meaningful detail line from stderr or stdout
	lines := strings.Split(stderr, "\n")
	for _, l := range lines {
		lTrim := strings.TrimSpace(l)
		if lTrim != "" && (strings.Contains(strings.ToLower(lTrim), "error") || strings.Contains(strings.ToLower(lTrim), "fail") || strings.Contains(strings.ToLower(lTrim), "503") || strings.Contains(strings.ToLower(lTrim), "panic") || strings.Contains(strings.ToLower(lTrim), "terminated")) {
			errDetail = lTrim
			break
		}
	}
	if errDetail == "" {
		if trimmedStderr != "" {
			errDetail = trimmedStderr
		} else if trimmedStdout != "" && exitCode != 0 {
			errDetail = trimmedStdout
		} else if trimmedStdout == "" && exitCode == 0 {
			errDetail = "process produced empty stdout"
		} else {
			errDetail = fmt.Sprintf("execution failed with exit code %d", exitCode)
		}
	}

	return isFailure, isTransient, isSessionCorruption, errDetail
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

