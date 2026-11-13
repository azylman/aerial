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

// runTrace announces an agy MCP tool call to eri's trace proxy and always allows it. Every
// failure (no endpoint, malformed payload, refused connection, timeout, non-2xx) is logged to
// stderr and otherwise ignored: tracing must never block or alter a tool call, and stdout
// carries only the allow decision.
func runTrace(r io.Reader, w io.Writer, getenv func(string) string, client *http.Client) int {
	if err := announceMCPCall(r, getenv, client); err != nil {
		log.Printf("hook-guard trace: announcement skipped: %v", err)
	}
	if err := json.NewEncoder(w).Encode(guard.Decision{Decision: guard.DecisionAllow}); err != nil {
		return 1
	}
	return 0
}

// announceMCPCall posts the server and tool names of the call on r to ERI_HOOK_ENDPOINT. It is
// a no-op when the endpoint is unset (tracing disabled).
func announceMCPCall(r io.Reader, getenv func(string) string, client *http.Client) error {
	endpoint := strings.TrimSpace(getenv("ERI_HOOK_ENDPOINT"))
	if endpoint == "" {
		return nil
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("read payload: %w", err)
	}
	var payload guard.Payload
	if err := json.Unmarshal(data, &payload); err != nil {
		return fmt.Errorf("parse payload: %w", err)
	}
	payload.Normalize()
	announcement, ok := guard.NewTraceAnnouncement(payload, strings.TrimSpace(getenv("ERI_DAEMON_ID")))
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
