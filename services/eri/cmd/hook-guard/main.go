package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/guard"
)

// Mode represents the hook-guard operating mode.
type Mode string

const (
	ModeAll      Mode = "all"
	ModeShare    Mode = "share"
	ModeSchedule Mode = "schedule"
	ModeBatch    Mode = "batch"
	ModePost     Mode = "post-batch"
	ModeInv      Mode = "pre-invocation"
	ModeSearch   Mode = "search"
	ModeCommit   Mode = "commit"
	ModeTrace    Mode = "trace"
)

// traceAnnounceTimeout bounds the announcement POST so a stalled eri never delays a tool call.
const traceAnnounceTimeout = 300 * time.Millisecond

// Config encapsulates CLI arguments and mode settings.
type Config struct {
	Mode     Mode
	ShowHelp bool
}

// HelpMessage returns the CLI usage documentation.
func HelpMessage() string {
	return "Usage: hook-guard [share|schedule|batch|post-batch|pre-invocation|search|commit|trace|all]"
}

func detectModeFromBase(base string) (Mode, bool) {
	switch {
	case strings.Contains(base, "share"):
		return ModeShare, true
	case strings.Contains(base, "schedule"):
		return ModeSchedule, true
	case strings.Contains(base, "post-batch"), strings.Contains(base, "post-tool"):
		return ModePost, true
	case strings.Contains(base, "invocation"):
		return ModeInv, true
	case strings.Contains(base, "batch"):
		return ModeBatch, true
	case strings.Contains(base, "search"):
		return ModeSearch, true
	case strings.Contains(base, "commit"):
		return ModeCommit, true
	default:
		return ModeAll, false
	}
}

// ParseCLIConfig parses command line arguments and infers mode from executable name.
func ParseCLIConfig(args []string) Config {
	cfg := Config{
		Mode: ModeAll,
	}

	// 1. Detect mode from binary base name (supporting symlinks like share-guard, schedule-guard, batch-guard, search-guard, commit-guard)
	if len(args) > 0 {
		base := strings.ToLower(filepath.Base(args[0]))
		if m, ok := detectModeFromBase(base); ok {
			cfg.Mode = m
		}
	}

	// 2. Allow explicit CLI argument to override mode
	for i := 1; i < len(args); i++ {
		arg := strings.ToLower(strings.TrimSpace(args[i]))
		switch arg {
		case "share", "share-guard":
			cfg.Mode = ModeShare
		case "schedule", "schedule-guard":
			cfg.Mode = ModeSchedule
		case "post-batch", "post-batch-guard", "post-tool":
			cfg.Mode = ModePost
		case "pre-invocation", "invocation", "invocation-guard":
			cfg.Mode = ModeInv
		case "batch", "batch-guard":
			cfg.Mode = ModeBatch
		case "search", "search-guard":
			cfg.Mode = ModeSearch
		case "commit", "commit-guard", "commit-safety-guard":
			cfg.Mode = ModeCommit
		case "trace", "trace-announce":
			cfg.Mode = ModeTrace
		case "all":
			cfg.Mode = ModeAll
		case "-h", "--help", "help":
			cfg.ShowHelp = true
		}
	}

	return cfg
}

// RunCLI executes the hook-guard CLI workflow and returns an exit code.
func RunCLI(args []string, r io.Reader, w io.Writer) int {
	cfg := ParseCLIConfig(args)
	if cfg.ShowHelp {
		if _, err := fmt.Fprintln(w, HelpMessage()); err != nil {
			return 1
		}
		return 0
	}

	if cfg.Mode == ModeTrace {
		return runTrace(r, w, os.Getenv, &http.Client{Timeout: traceAnnounceTimeout})
	}

	if err := guard.Process(r, w, string(cfg.Mode)); err != nil {
		// Preserves fail-open invariant
		out := `{"decision":"allow"}`
		if guard.IsInvocationMode(string(cfg.Mode)) {
			out = `{}`
		}
		if _, wErr := fmt.Fprintln(w, out); wErr != nil {
			return 1
		}
		return 1
	}
	return 0
}

// runTrace announces an MCP tool call to eri's trace proxy and always allows it, in the hook
// output schema of the engine that invoked it. Every failure (no endpoint, malformed payload,
// refused connection, timeout, non-2xx) is logged to stderr and otherwise ignored: tracing
// must never block or alter a tool call, and stdout carries only the allow decision.
func runTrace(r io.Reader, w io.Writer, getenv func(string) string, client *http.Client) int {
	endpoint := strings.TrimSpace(getenv("ERI_HOOK_ENDPOINT"))
	payload, err := readHookPayload(r)
	if err == nil && endpoint != "" {
		err = announceMCPCall(payload, endpoint, strings.TrimSpace(getenv("ERI_DAEMON_ID")), client)
	}
	if err != nil && endpoint != "" {
		log.Printf("hook-guard trace: announcement skipped: %v", err)
	}
	var out any = guard.Decision{Decision: guard.DecisionAllow}
	if payload.IsClaude() {
		out = guard.FormatClaudeResponse(&guard.Decision{Decision: guard.DecisionAllow}, nil)
	}
	if err := json.NewEncoder(w).Encode(out); err != nil {
		return 1
	}
	return 0
}

// readHookPayload parses a PreToolUse payload from r.
func readHookPayload(r io.Reader) (guard.Payload, error) {
	var payload guard.Payload
	data, err := io.ReadAll(r)
	if err != nil {
		return payload, fmt.Errorf("read payload: %w", err)
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return guard.Payload{}, fmt.Errorf("parse payload: %w", err)
	}
	return payload, nil
}

// announceMCPCall posts the server and tool names of payload's MCP call to endpoint.
func announceMCPCall(payload guard.Payload, endpoint, daemonID string, client *http.Client) error {
	payload.Normalize()
	announcement, ok := guard.NewTraceAnnouncement(payload, daemonID)
	if !ok {
		return errors.New("payload names no MCP server and tool")
	}
	body, err := json.Marshal(announcement)
	if err != nil {
		return fmt.Errorf("encode announcement: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), traceAnnounceTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("post announcement: %w", err)
	}
	if resp.StatusCode >= http.StatusMultipleChoices {
		err = fmt.Errorf("announcement rejected: %s", resp.Status)
	}
	return errors.Join(err, resp.Body.Close())
}

func main() {
	os.Exit(RunCLI(os.Args, os.Stdin, os.Stdout))
}
