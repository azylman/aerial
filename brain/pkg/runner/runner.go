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

	"github.com/azylman/aerial/brain/pkg/session"
)

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
	if apiKey != "" {
		cmd.Env = append(os.Environ(),
			"GEMINI_API_KEY="+apiKey,
			"ANTIGRAVITY_API_KEY="+apiKey,
			"GOOGLE_GENAI_API_KEY="+apiKey,
			"AGY_LOG_LEVEL=debug",
			"ANTIGRAVITY_LOG_LEVEL=debug",
		)
	}

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

	// If exitCode == 0 and stdout is non-empty, this is a clean success regardless of keywords in stdout (e.g. model discussing error codes or session corrupt in generated code).
	if exitCode == 0 && trimmedStdout != "" {
		return false, false, false, ""
	}

	combined := strings.ToLower(stdout + "\n" + stderr)

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

	failureKeywords := []string{
		"agent execution terminated",
		"error in generator",
		"error encountered while processing planner output",
		"panic:",
		"fatal error:",
	}

	for _, kw := range transientKeywords {
		if strings.Contains(combined, kw) {
			isTransient = true
			isFailure = true
			break
		}
	}

	for _, kw := range corruptionKeywords {
		if strings.Contains(combined, kw) {
			isSessionCorruption = true
			isFailure = true
			break
		}
	}

	for _, kw := range failureKeywords {
		if strings.Contains(combined, kw) {
			isFailure = true
			break
		}
	}

	if exitCode != 0 {
		isFailure = true
	}

	if isFailure {
		// Extract meaningful detail line from stderr or stdout
		lines := strings.Split(stderr, "\n")
		for _, l := range lines {
			lTrim := strings.TrimSpace(l)
			if lTrim != "" && (strings.Contains(strings.ToLower(lTrim), "error") || strings.Contains(strings.ToLower(lTrim), "fail") || strings.Contains(strings.ToLower(lTrim), "503")) {
				errDetail = lTrim
				break
			}
		}
		if errDetail == "" {
			if strings.TrimSpace(stderr) != "" {
				errDetail = strings.TrimSpace(stderr)
			} else if trimmedStdout != "" && exitCode != 0 {
				errDetail = trimmedStdout
			} else {
				errDetail = fmt.Sprintf("execution failed with exit code %d", exitCode)
			}
		}
	}

	return isFailure, isTransient, isSessionCorruption, errDetail
}
