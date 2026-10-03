package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/azylman/aerial/brain/pkg/guard"
)

func main() {
	mode := "all"

	// 1. Detect mode from binary name (supporting symlinks like share-guard or schedule-guard)
	base := strings.ToLower(filepath.Base(os.Args[0]))
	if strings.Contains(base, "share") {
		mode = "share"
	} else if strings.Contains(base, "schedule") {
		mode = "schedule"
	}

	// 2. Allow CLI argument to override mode
	if len(os.Args) > 1 {
		arg := strings.ToLower(strings.TrimSpace(os.Args[1]))
		switch arg {
		case "share", "share-guard":
			mode = "share"
		case "schedule", "schedule-guard":
			mode = "schedule"
		case "all":
			mode = "all"
		case "-h", "--help", "help":
			fmt.Println("Usage: hook-guard [share|schedule|all]")
			os.Exit(0)
		}
	}

	if err := guard.Process(os.Stdin, os.Stdout, mode); err != nil {
		// Preserves fail-open invariant
		fmt.Println(`{"decision":"allow"}`)
	}
}
