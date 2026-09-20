package runner

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// AgyArgsInput holds the configuration parameters for building CLI arguments.
type AgyArgsInput struct {
	OutputFormat string
	Model        string
	SessionID    string
	WorkerMode   bool
}

// BuildAgyArgs constructs command-line arguments for executing the agy binary.
func BuildAgyArgs(input AgyArgsInput) []string {
	outputFmt := strings.TrimSpace(input.OutputFormat)
	if outputFmt == "" {
		outputFmt = "stream-json"
	}

	args := []string{"--dangerously-skip-permissions"}
	if input.WorkerMode {
		args = append(args, "--input-format", "stream-json")
	}
	args = append(args, "--output-format", outputFmt)

	if sessionID := strings.TrimSpace(input.SessionID); sessionID != "" {
		args = append(args, "--conversation", sessionID)
	}

	if model := strings.TrimSpace(input.Model); model != "" {
		args = append(args, "--model", model)
	}

	return args
}

// AgyEnvInput holds environment configuration parameters.
type AgyEnvInput struct {
	BaseEnv  []string
	HomeDir  string
	APIKey   string
	TargetID string
	ExtraEnv []string
}

// BuildAgyEnv constructs the process environment slice for agy execution.
func BuildAgyEnv(input AgyEnvInput) []string {
	var cmdEnv []string
	if len(input.BaseEnv) > 0 {
		cmdEnv = append(cmdEnv, input.BaseEnv...)
	}

	cmdEnv = append(cmdEnv,
		"GIT_TERMINAL_PROMPT=0",
		"AGY_LOG_LEVEL=debug",
		"ANTIGRAVITY_LOG_LEVEL=debug",
	)

	if home := strings.TrimSpace(input.HomeDir); home != "" {
		cmdEnv = append(cmdEnv,
			"HOME="+home,
			"USERPROFILE="+home,
		)
	}

	if apiKey := strings.TrimSpace(input.APIKey); apiKey != "" {
		cmdEnv = append(cmdEnv,
			"GEMINI_API_KEY="+apiKey,
			"ANTIGRAVITY_API_KEY="+apiKey,
			"GOOGLE_GENAI_API_KEY="+apiKey,
		)
	}

	if target := strings.TrimSpace(input.TargetID); target != "" {
		cmdEnv = append(cmdEnv, "AERIAL_TARGET_ID="+target)
	}

	if len(input.ExtraEnv) > 0 {
		cmdEnv = append(cmdEnv, input.ExtraEnv...)
	}

	return cmdEnv
}


// ShouldRotateWorker evaluates whether a persistent worker should be rotated.
func ShouldRotateWorker(turnsUsed, turnBudget int, rssBytes, maxRSSBytes uint64, isDead bool) (bool, string) {
	if isDead {
		return true, "worker is dead"
	}
	if maxRSSBytes > 0 && rssBytes > maxRSSBytes {
		return true, fmt.Sprintf("rss limit exceeded (%d > %d bytes)", rssBytes, maxRSSBytes)
	}
	if turnBudget > 0 && turnsUsed >= turnBudget {
		return true, fmt.Sprintf("turn budget reached (%d >= %d)", turnsUsed, turnBudget)
	}
	return false, ""
}

// ParseInitEvent extracts conversation ID from an NDJSON line containing {"event":"init"}.
func ParseInitEvent(line string) (string, bool) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return "", false
	}

	var probe sessionProbe
	if err := json.Unmarshal([]byte(trimmed), &probe); err == nil {
		if (probe.Event == "init" || probe.Type == "init") && probe.extractUUID() != "" {
			return probe.extractUUID(), true
		}
	}

	// Embedded JSON fallback for lines with logging prefixes
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] == '{' {
			var embedded sessionProbe
			dec := json.NewDecoder(strings.NewReader(trimmed[i:]))
			if err := dec.Decode(&embedded); err == nil {
				if (embedded.Event == "init" || embedded.Type == "init") && embedded.extractUUID() != "" {
					return embedded.extractUUID(), true
				}
			}
		}
	}

	return "", false
}

// IsResultEvent checks if an output line represents a stream-json result event.
func IsResultEvent(line string) bool {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return false
	}

	var ev struct {
		Event string `json:"event"`
	}
	if err := json.Unmarshal([]byte(trimmed), &ev); err == nil {
		return ev.Event == "result"
	}

	normalized := strings.ReplaceAll(trimmed, " ", "")
	return strings.HasPrefix(normalized, `{"event":"result"`)
}

// WriteWorkerTurn serializes and writes a prompt turn to the given io.Writer.
func WriteWorkerTurn(w io.Writer, prompt string) error {
	if w == nil {
		return fmt.Errorf("worker writer is nil: %w", ErrWorkerDead)
	}

	req := streamUserMessage{
		Event: "user",
		Message: streamUserMessageBody{
			Content: prompt,
		},
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("failed to marshal prompt request: %w", err)
	}
	payload = append(payload, '\n')

	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("failed to write prompt to worker stdin: %w", err)
	}
	return nil
}
