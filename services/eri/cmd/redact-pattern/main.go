// Command redact-pattern writes the OTel Collector trace-redaction config.
//
// It loads secret values from a shared-secrets directory (or JSON file) plus an optional
// newline-separated extra file and builds sanitizer.ValuePattern over them. -out writes
// one OTTL-escaped pattern covering every value. -statements-out writes the complete
// transform/redact trace_statements list as YAML, with the values split into groups whose
// patterns each fit -max-pattern-bytes: OTTL parse time grows super-linearly with literal
// size, so many small literals start far faster than one huge one. Only counts are ever
// printed.
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
	"unicode/utf8"

	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/sanitizer"
)

const defaultMaxPatternBytes = 8192

type config struct {
	secretsDir      string
	extraFile       string
	out             string
	statementsOut   string
	maxPatternBytes int
}

func parseFlags(args []string, stderr io.Writer) (config, error) {
	fs := flag.NewFlagSet("redact-pattern", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var cfg config
	fs.StringVar(&cfg.secretsDir, "secrets-dir", sanitizer.DefaultSharedSecretsPath, "shared secrets directory or JSON file")
	fs.StringVar(&cfg.extraFile, "extra-file", "", "optional file of newline-separated extra values")
	fs.StringVar(&cfg.out, "out", "", "output path for the single OTTL-escaped pattern")
	fs.StringVar(&cfg.statementsOut, "statements-out", "", "output path for the split transform/redact trace_statements YAML")
	fs.IntVar(&cfg.maxPatternBytes, "max-pattern-bytes", defaultMaxPatternBytes, "max OTTL-escaped pattern bytes per statement group")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	if cfg.out == "" && cfg.statementsOut == "" {
		return config{}, errors.New("-out or -statements-out is required")
	}
	if cfg.maxPatternBytes <= 0 {
		return config{}, errors.New("-max-pattern-bytes must be positive")
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

// patternBytes is the size of the OTTL string literal body redacting values.
func patternBytes(values []string) int {
	return len(sanitizer.OTTLStringBody(sanitizer.ValuePattern(values)))
}

// groupValues splits values into groups whose OTTL-escaped ValuePattern fits maxBytes.
// Whole values are packed (ValuePattern expands a value's variants, so they stay
// together), longest first, so a secret is redacted before any shorter secret that may be
// its substring. A value whose own pattern exceeds maxBytes gets a group of its own.
// Duplicates and values sanitizer ignores (too short to register) are dropped.
func groupValues(values []string, maxBytes int) [][]string {
	nothing := sanitizer.ValuePattern(nil)
	seen := make(map[string]bool, len(values))
	kept := make([]string, 0, len(values))
	for _, v := range values {
		v = strings.TrimSpace(v)
		if seen[v] || sanitizer.ValuePattern([]string{v}) == nothing {
			continue
		}
		seen[v] = true
		kept = append(kept, v)
	}
	sort.SliceStable(kept, func(i, j int) bool {
		if len(kept[i]) != len(kept[j]) {
			return len(kept[i]) > len(kept[j])
		}
		return kept[i] < kept[j]
	})
	groups := [][]string{}
	start := 0
	for i := range kept {
		if i > start && patternBytes(kept[start:i+1]) > maxBytes {
			groups = append(groups, kept[start:i:i])
			start = i
		}
	}
	if start < len(kept) {
		groups = append(groups, kept[start:])
	}
	return groups
}

// groupPatterns returns each group's OTTL-escaped pattern in order. With no groups it
// returns the match-nothing pattern so the rendered config stays valid and inert.
func groupPatterns(groups [][]string) []string {
	if len(groups) == 0 {
		return []string{sanitizer.OTTLStringBody(sanitizer.ValuePattern(nil))}
	}
	patterns := make([]string, len(groups))
	for i, g := range groups {
		patterns[i] = sanitizer.OTTLStringBody(sanitizer.ValuePattern(g))
	}
	return patterns
}

type statementContext struct {
	Context    string   `yaml:"context"`
	Statements []string `yaml:"statements"`
}

// statementTemplates are the redaction statements per OTTL context. Paths carry explicit
// context prefixes: unprefixed paths make OTTL log rewritten statements at startup, which
// would print the pattern. %s is an OTTL-escaped pattern body.
var statementTemplates = []struct {
	context   string
	templates []string
}{
	{"resource", []string{
		`replace_all_patterns(resource.attributes, "value", "%s", "[REDACTED]")`,
	}},
	{"scope", []string{
		`replace_all_patterns(scope.attributes, "value", "%s", "[REDACTED]")`,
		`replace_pattern(scope.name, "%s", "[REDACTED]")`,
	}},
	{"span", []string{
		`replace_all_patterns(span.attributes, "value", "%s", "[REDACTED]")`,
		`replace_pattern(span.name, "%s", "[REDACTED]")`,
		`replace_pattern(span.status.message, "%s", "[REDACTED]")`,
	}},
	{"spanevent", []string{
		`replace_pattern(spanevent.name, "%s", "[REDACTED]")`,
		`replace_all_patterns(spanevent.attributes, "value", "%s", "[REDACTED]")`,
	}},
}

// renderStatements renders the transform/redact trace_statements list: per context, each
// field gets one statement per pattern, in pattern order. Invalid UTF-8 is refused: YAML
// would encode it as !!binary, which the collector cannot use as a statement.
func renderStatements(patterns []string) ([]byte, error) {
	for i, p := range patterns {
		if !utf8.ValidString(p) {
			return nil, fmt.Errorf("statement group %d is not valid UTF-8", i)
		}
	}
	out := make([]statementContext, 0, len(statementTemplates))
	for _, c := range statementTemplates {
		sc := statementContext{Context: c.context, Statements: make([]string, 0, len(c.templates)*len(patterns))}
		for _, tmpl := range c.templates {
			for _, p := range patterns {
				sc.Statements = append(sc.Statements, strings.Replace(tmpl, "%s", p, 1))
			}
		}
		out = append(out, sc)
	}
	return yaml.Marshal(out)
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
	// 0644: the files live in a pod-scoped memory emptyDir and the collector container may run as a different uid.
	if cfg.out != "" {
		body := sanitizer.OTTLStringBody(sanitizer.ValuePattern(values))
		if err := os.WriteFile(cfg.out, []byte(body), 0o644); err != nil {
			errLog.Printf("write pattern: %v", err)
			return 1
		}
		outLog.Printf("wrote pattern for %d values", len(values))
	}
	if cfg.statementsOut != "" {
		groups := groupValues(values, cfg.maxPatternBytes)
		doc, err := renderStatements(groupPatterns(groups))
		if err != nil {
			errLog.Printf("render statements: %v", err)
			return 1
		}
		if err := os.WriteFile(cfg.statementsOut, doc, 0o644); err != nil {
			errLog.Printf("write statements: %v", err)
			return 1
		}
		outLog.Printf("wrote statements for %d values in %d groups", len(values), len(groups))
	}
	return 0
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
