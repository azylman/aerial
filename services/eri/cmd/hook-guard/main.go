package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

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
)

// Config encapsulates CLI arguments and mode settings.
type Config struct {
	Mode     Mode
	ShowHelp bool
}

// HelpMessage returns the CLI usage documentation.
func HelpMessage() string {
	return "Usage: hook-guard [share|schedule|batch|post-batch|pre-invocation|search|commit|all]"
}

// ParseCLIConfig parses command line arguments and infers mode from executable name.
func ParseCLIConfig(args []string) Config {
	cfg := Config{
		Mode: ModeAll,
	}

	// 1. Detect mode from binary base name (supporting symlinks like share-guard, schedule-guard, batch-guard, search-guard, commit-guard)
	if len(args) > 0 {
		base := strings.ToLower(filepath.Base(args[0]))
		if strings.Contains(base, "share") {
			cfg.Mode = ModeShare
		} else if strings.Contains(base, "schedule") {
			cfg.Mode = ModeSchedule
		} else if strings.Contains(base, "post-batch") || strings.Contains(base, "post-tool") {
			cfg.Mode = ModePost
		} else if strings.Contains(base, "invocation") {
			cfg.Mode = ModeInv
		} else if strings.Contains(base, "batch") {
			cfg.Mode = ModeBatch
		} else if strings.Contains(base, "search") {
			cfg.Mode = ModeSearch
		} else if strings.Contains(base, "commit") {
			cfg.Mode = ModeCommit
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

func main() {
	os.Exit(RunCLI(os.Args, os.Stdin, os.Stdout))
}
