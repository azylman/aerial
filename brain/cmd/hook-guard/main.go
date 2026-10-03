package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/azylman/aerial/brain/pkg/guard"
)

// Mode represents the hook-guard operating mode.
type Mode string

const (
	ModeAll      Mode = "all"
	ModeShare    Mode = "share"
	ModeSchedule Mode = "schedule"
)

// Config encapsulates CLI arguments and mode settings.
type Config struct {
	Mode     Mode
	ShowHelp bool
}

// HelpMessage returns the CLI usage documentation.
func HelpMessage() string {
	return "Usage: hook-guard [share|schedule|all]"
}

// ParseCLIConfig parses command line arguments and infers mode from executable name.
func ParseCLIConfig(args []string) Config {
	cfg := Config{
		Mode: ModeAll,
	}

	// 1. Detect mode from binary base name (supporting symlinks like share-guard or schedule-guard)
	if len(args) > 0 {
		base := strings.ToLower(filepath.Base(args[0]))
		if strings.Contains(base, "share") {
			cfg.Mode = ModeShare
		} else if strings.Contains(base, "schedule") {
			cfg.Mode = ModeSchedule
		}
	}

	// 2. Allow explicit CLI argument to override mode
	if len(args) > 1 {
		arg := strings.ToLower(strings.TrimSpace(args[1]))
		switch arg {
		case "share", "share-guard":
			cfg.Mode = ModeShare
		case "schedule", "schedule-guard":
			cfg.Mode = ModeSchedule
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
		_, _ = fmt.Fprintln(w, `{"decision":"allow"}`) //nolint:errcheck
		return 1
	}
	return 0
}

func main() {
	os.Exit(RunCLI(os.Args, os.Stdin, os.Stdout))
}
