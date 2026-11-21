// Command redact-pattern writes the OTel Collector trace-redaction pattern.
//
// It loads secret values from a shared-secrets directory (or JSON file) plus an optional
// newline-separated extra file, builds sanitizer.ValuePattern over them, and writes the
// OTTL-escaped pattern to -out. Only the number of values is ever printed.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/azylman/aerial/pkg/sanitizer"
)

type config struct {
	secretsDir string
	extraFile  string
	out        string
}

func parseFlags(args []string, stderr io.Writer) (config, error) {
	fs := flag.NewFlagSet("redact-pattern", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var cfg config
	fs.StringVar(&cfg.secretsDir, "secrets-dir", sanitizer.DefaultSharedSecretsPath, "shared secrets directory or JSON file")
	fs.StringVar(&cfg.extraFile, "extra-file", "", "optional file of newline-separated extra values")
	fs.StringVar(&cfg.out, "out", "", "output path for the OTTL-escaped pattern")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.out == "" {
		return config{}, errors.New("-out is required")
	}
	return cfg, nil
}

// collectValues returns the sorted, non-empty secret values plus extra-file lines.
func collectValues(secretsDir, extraFile string) ([]string, error) {
	shared := sanitizer.LoadSharedSecrets(secretsDir)
	keys := make([]string, 0, len(shared))
	for k := range shared {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	values := make([]string, 0, len(keys))
	for _, k := range keys {
		if v := strings.TrimSpace(shared[k]); v != "" {
			values = append(values, v)
		}
	}
	if extraFile == "" {
		return values, nil
	}
	data, err := os.ReadFile(extraFile)
	if err != nil {
		return nil, fmt.Errorf("read extra file: %w", err)
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		if v := strings.TrimSpace(line); v != "" {
			values = append(values, v)
		}
	}
	return values, nil
}

func run(args []string, stdout, stderr io.Writer) int {
	errLog := log.New(stderr, "redact-pattern: ", 0)
	outLog := log.New(stdout, "redact-pattern: ", 0)
	cfg, err := parseFlags(args, stderr)
	if err != nil {
		errLog.Print(err)
		return 2
	}
	values, err := collectValues(cfg.secretsDir, cfg.extraFile)
	if err != nil {
		errLog.Print(err)
		return 1
	}
	body := sanitizer.OTTLStringBody(sanitizer.ValuePattern(values))
	// 0644: the file lives in a pod-scoped memory emptyDir and the collector container may run as a different uid.
	if err := os.WriteFile(cfg.out, []byte(body), 0o644); err != nil {
		errLog.Printf("write pattern: %v", err)
		return 1
	}
	outLog.Printf("wrote pattern for %d values", len(values))
	return 0
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
