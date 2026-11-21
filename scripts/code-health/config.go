package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
)

// Test hygiene rule IDs that a repo config may switch off via
// disabled_test_rules. Every rule is on by default.
const (
	ruleHTTPDefaultClient = "http-default-client"
	ruleHTTPClientLiteral = "http-client-literal"
	ruleTimeSleep         = "time-sleep"
	ruleSyscallKill       = "syscall-kill"
	ruleReservedPort      = "reserved-port"
)

var knownTestRules = map[string]bool{
	ruleHTTPDefaultClient: true,
	ruleHTTPClientLiteral: true,
	ruleTimeSleep:         true,
	ruleSyscallKill:       true,
	ruleReservedPort:      true,
}

// defaultBannedTestFilenameSuffixes rejects catch-all test filenames that
// accumulate unrelated coverage-slop cases outside their domain scope. Fold
// such files into canonical domain-scoped test tables. Extend this list when
// a new catch-all pattern appears; renaming a file to dodge a listed suffix
// defeats the ratchet.
var defaultBannedTestFilenameSuffixes = []string{"_extra_test.go", "_more_test.go", "_coverage_gaps_test.go", "_additional_test.go"}

// fileConfig is the optional -config JSON file. Absent fields keep aerial's
// defaults. "generated" and "skipped" are synonyms (aerial-sidecars used
// "generated", aerial and mirrormere "skipped"); both lists are honoured.
type fileConfig struct {
	MaxFileLines               int       `json:"max_file_lines"`
	MaxDepth                   int       `json:"max_depth"`
	BannedTestFilenameSuffixes *[]string `json:"banned_test_filename_suffixes"`
	BannedClosureFieldPrefixes []string  `json:"banned_closure_field_prefixes"`
	DisabledTestRules          []string  `json:"disabled_test_rules"`
	Generated                  []string  `json:"generated"`
	Skipped                    []string  `json:"skipped"`
	CheckOrdering              bool      `json:"check_ordering"`
}

// rules is the resolved rule set every check reads.
type rules struct {
	maxFileLines               int
	maxDepth                   int
	bannedTestFilenameSuffixes []string
	bannedClosureFieldPrefixes []string
	disabledTestRules          map[string]bool
	skippedDirs                []string
	checkOrdering              bool
}

func defaultRules() rules {
	return rules{
		maxFileLines:               maxFileLines,
		maxDepth:                   maxDepth,
		bannedTestFilenameSuffixes: defaultBannedTestFilenameSuffixes,
		disabledTestRules:          map[string]bool{},
	}
}

// loadRules returns aerial's defaults overlaid with the config file at p.
// An empty p means no config file.
func loadRules(p string) (rules, error) {
	r := defaultRules()
	if p == "" {
		return r, nil
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return r, err
	}
	var fc fileConfig
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&fc); err != nil {
		return r, fmt.Errorf("parse %s: %w", p, err)
	}
	if fc.MaxFileLines < 0 || fc.MaxDepth < 0 {
		return r, fmt.Errorf("parse %s: thresholds must not be negative", p)
	}
	if fc.MaxFileLines > 0 {
		r.maxFileLines = fc.MaxFileLines
	}
	if fc.MaxDepth > 0 {
		r.maxDepth = fc.MaxDepth
	}
	if fc.BannedTestFilenameSuffixes != nil {
		r.bannedTestFilenameSuffixes = *fc.BannedTestFilenameSuffixes
	}
	r.bannedClosureFieldPrefixes = fc.BannedClosureFieldPrefixes
	for _, id := range fc.DisabledTestRules {
		if !knownTestRules[id] {
			return r, fmt.Errorf("parse %s: unknown test rule %q", p, id)
		}
		r.disabledTestRules[id] = true
	}
	for _, dir := range append(append([]string{}, fc.Generated...), fc.Skipped...) {
		clean := path.Clean(strings.Trim(strings.TrimSpace(dir), "/"))
		if clean == "." || clean == "" || strings.HasPrefix(clean, "..") {
			return r, fmt.Errorf("parse %s: invalid skipped directory %q", p, dir)
		}
		r.skippedDirs = append(r.skippedDirs, clean)
	}
	r.checkOrdering = fc.CheckOrdering
	return r, nil
}

// withoutSkipped drops targets under a configured generated/skipped dir.
func (r rules) withoutSkipped(targets []string) []string {
	if len(r.skippedDirs) == 0 {
		return targets
	}
	out := targets[:0:0]
	for _, rel := range targets {
		if !r.isSkippedPath(rel) {
			out = append(out, rel)
		}
	}
	return out
}

func (r rules) isSkippedPath(rel string) bool {
	for _, dir := range r.skippedDirs {
		if rel == dir || strings.HasPrefix(rel, dir+"/") {
			return true
		}
	}
	return false
}

func (r rules) isCatchAllTestFilename(rel string) bool {
	for _, suffix := range r.bannedTestFilenameSuffixes {
		if strings.HasSuffix(rel, suffix) {
			return true
		}
	}
	return false
}

// isBannedClosureFieldName bans the fixed closure field names plus any
// configured prefix followed by an upper-case letter (checkFoo, runBar).
func (r rules) isBannedClosureFieldName(name string) bool {
	switch name {
	case "run", "check", "extraCheck", "setup", "validate", "validator", "verify", "lookup":
		return true
	}
	for _, p := range r.bannedClosureFieldPrefixes {
		if len(name) > len(p) && strings.HasPrefix(name, p) {
			if next := name[len(p)]; next >= 'A' && next <= 'Z' {
				return true
			}
		}
	}
	return false
}

func (r rules) testRuleEnabled(id string) bool {
	return !r.disabledTestRules[id]
}
