package guard

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// Violation represents a secret or PII detection in a commit diff.
type Violation struct {
	Category string `json:"category"`
	Pattern  string `json:"pattern"`
	Sample   string `json:"sample"`
	File     string `json:"file"`
	LineNum  int    `json:"lineNum"`
}

// PatternRule binds a pattern category name to a compiled regular expression.
type PatternRule struct {
	Category string
	Regex    *regexp.Regexp
}

// BuiltinPatterns defines the standard generic security patterns.
// Strictly contains generic patterns without any hardcoded personal PII.
var BuiltinPatterns = []PatternRule{
	{Category: "RFC1918_192_168", Regex: regexp.MustCompile(`\b192\.168\.\d{1,3}\.\d{1,3}\b`)},
	{Category: "GitHubPAT", Regex: regexp.MustCompile(`\b(ghp|github_pat|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{20,80}\b`)},
	{Category: "PrivateKey", Regex: regexp.MustCompile(`-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----`)},
	{Category: "DiscordWebhook", Regex: regexp.MustCompile(`https://discord(?:app)?\.com/api/webhooks/\d+/[A-Za-z0-9_-]+`)},
	{Category: "SlackWebhook", Regex: regexp.MustCompile(`https://hooks\.slack\.com/services/T[0-9A-Z]+/B[0-9A-Z]+/[0-9A-Za-z]+`)},
	{Category: "GenericAPIKey", Regex: regexp.MustCompile(`\b(sk-[a-zA-Z0-9]{20,80})\b`)},
	{Category: "JWTToken", Regex: regexp.MustCompile(`\beyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b`)},
}

// gitCmdRunner executes git commands. Package variable to enable hermetic mocking in tests.
var gitCmdRunner = func(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.Output()
	return string(out), err
}

// LoadCustomPatterns reads dynamic patterns from ~/.gemini/config/commit-guard.json,
// ~/.gemini/config/guard.json, or the AERIAL_COMMIT_GUARD_PATTERNS environment variable.
func LoadCustomPatterns(homeDir string) []PatternRule {
	var rules []PatternRule

	if homeDir == "" {
		if uHome, err := os.UserHomeDir(); err == nil {
			homeDir = uHome
		}
	}

	// 1. Check file candidates in homeDir
	if homeDir != "" {
		candidates := []string{
			filepath.Join(homeDir, ".gemini", "config", "commit-guard.json"),
			filepath.Join(homeDir, ".gemini", "config", "guard.json"),
		}
		for _, cand := range candidates {
			if data, err := os.ReadFile(cand); err == nil && len(data) > 0 {
				rules = append(rules, parsePatternPayload(data)...)
				break
			}
		}
	}

	// 2. Check environment variable
	if envVal := os.Getenv("AERIAL_COMMIT_GUARD_PATTERNS"); strings.TrimSpace(envVal) != "" {
		rules = append(rules, parsePatternPayload([]byte(envVal))...)
	}

	return rules
}

func parsePatternPayload(data []byte) []PatternRule {
	var rules []PatternRule
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return rules
	}

	// Try format 1: [{"category": "...", "pattern": "..."}]
	type entry struct {
		Category string `json:"category"`
		Pattern  string `json:"pattern"`
	}
	var entries []entry
	if err := json.Unmarshal(data, &entries); err == nil && len(entries) > 0 {
		for _, e := range entries {
			if e.Pattern != "" {
				if re, err := regexp.Compile(e.Pattern); err == nil {
					cat := e.Category
					if cat == "" {
						cat = "CustomRule"
					}
					rules = append(rules, PatternRule{Category: cat, Regex: re})
				}
			}
		}
		return rules
	}

	// Try format 2: {"patterns": [...]}
	var wrapper struct {
		Patterns []entry `json:"patterns"`
	}
	if err := json.Unmarshal(data, &wrapper); err == nil && len(wrapper.Patterns) > 0 {
		for _, e := range wrapper.Patterns {
			if e.Pattern != "" {
				if re, err := regexp.Compile(e.Pattern); err == nil {
					cat := e.Category
					if cat == "" {
						cat = "CustomRule"
					}
					rules = append(rules, PatternRule{Category: cat, Regex: re})
				}
			}
		}
		return rules
	}

	// Try format 3: map[string]string {"Category": "Pattern"}
	var kv map[string]string
	if err := json.Unmarshal(data, &kv); err == nil && len(kv) > 0 {
		for k, v := range kv {
			if v != "" {
				if re, err := regexp.Compile(v); err == nil {
					rules = append(rules, PatternRule{Category: k, Regex: re})
				}
			}
		}
		return rules
	}

	// Try format 4: line or comma delimited raw patterns
	lines := strings.Split(trimmed, "\n")
	if len(lines) == 1 && strings.Contains(trimmed, ",") && !strings.HasPrefix(trimmed, "{") && !strings.HasPrefix(trimmed, "[") {
		lines = strings.Split(trimmed, ",")
	}
	for i, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" && !strings.HasPrefix(l, "#") {
			if re, err := regexp.Compile(l); err == nil {
				rules = append(rules, PatternRule{
					Category: fmt.Sprintf("CustomRule_%d", i+1),
					Regex:    re,
				})
			}
		}
	}

	return rules
}

// GetActivePatterns returns BuiltinPatterns augmented with any custom user patterns.
func GetActivePatterns() []PatternRule {
	custom := LoadCustomPatterns("")
	if len(custom) == 0 {
		return BuiltinPatterns
	}
	result := make([]PatternRule, 0, len(BuiltinPatterns)+len(custom))
	result = append(result, BuiltinPatterns...)
	result = append(result, custom...)
	return result
}

// isTestFile returns true if the file path indicates a test file or test fixture.
func isTestFile(path string) bool {
	normalized := filepath.ToSlash(strings.TrimSpace(path))
	if normalized == "" {
		return false
	}

	// Test extensions
	testSuffixes := []string{
		"_test.go",
		"_test.py",
		".test.ts",
		".test.js",
		".spec.ts",
		".spec.js",
	}
	for _, suffix := range testSuffixes {
		if strings.HasSuffix(normalized, suffix) {
			return true
		}
	}

	// Fixture and testdata directories
	if strings.Contains(normalized, "/fixtures/") || strings.HasPrefix(normalized, "fixtures/") ||
		strings.Contains(normalized, "/testdata/") || strings.HasPrefix(normalized, "testdata/") {
		return true
	}

	return false
}

// ScanDiff parses a unified diff line-by-line and inspects added lines for violations.
// If checkTestFiles is false, test files and fixtures are skipped so tests don't self-block.
func ScanDiff(diff string, checkTestFiles bool) []Violation {
	return ScanDiffWithPatterns(diff, checkTestFiles, GetActivePatterns())
}

// ScanDiffWithPatterns parses a unified diff and checks added lines against the supplied patterns.
func ScanDiffWithPatterns(diff string, checkTestFiles bool, patterns []PatternRule) []Violation {
	var violations []Violation
	if len(diff) == 0 || len(patterns) == 0 {
		return violations
	}

	lines := strings.Split(diff, "\n")
	currentFile := ""
	lineNum := 0
	inHunk := false
	isBinary := false

	for _, line := range lines {
		// Detect file headers: diff --git a/... b/...
		if strings.HasPrefix(line, "diff --git ") {
			inHunk = false
			isBinary = false
			lineNum = 0
			// Extract target file
			if idx := strings.Index(line, " b/"); idx != -1 {
				currentFile = strings.TrimSpace(line[idx+3:])
			}
			continue
		}

		// Detect diff --no-index or direct --- / +++
		if strings.HasPrefix(line, "--- ") {
			inHunk = false
			continue
		}

		if strings.HasPrefix(line, "+++ ") {
			inHunk = false
			if line != "+++ /dev/null" {
				target := strings.TrimPrefix(line, "+++ ")
				target = strings.TrimPrefix(target, "b/")
				currentFile = strings.TrimSpace(target)
			}
			continue
		}

		// Detect binary files
		if strings.HasPrefix(line, "Binary files ") && strings.HasSuffix(line, "differ") {
			isBinary = true
			continue
		}

		// Detect hunk headers: @@ -a,b +c,d @@ or @@ -a +c @@
		if strings.HasPrefix(line, "@@ ") {
			inHunk = true
			lineNum = parseHunkLineNum(line)
			continue
		}

		if isBinary {
			continue
		}

		// If no hunk header was encountered (e.g. raw diff), start line numbering at 1
		if !inHunk && lineNum == 0 && (strings.HasPrefix(line, "+") || strings.HasPrefix(line, " ")) {
			inHunk = true
			lineNum = 1
		}

		// Check added line
		if strings.HasPrefix(line, "+") && !strings.HasPrefix(line, "+++") {
			currentLineNum := lineNum
			lineNum++

			if !checkTestFiles && isTestFile(currentFile) {
				continue
			}

			content := line[1:]
			for _, p := range patterns {
				if match := p.Regex.FindString(content); match != "" {
					violations = append(violations, Violation{
						Category: p.Category,
						Pattern:  p.Regex.String(),
						Sample:   match,
						File:     currentFile,
						LineNum:  currentLineNum,
					})
				}
			}
			continue
		}

		// Context line: unified diffs emit ' ' prefix or an empty line for empty lines
		if inHunk && (strings.HasPrefix(line, " ") || line == "") {
			lineNum++
			continue
		}

		// Deleted line: '-' line, do not increment target file lineNum
	}

	return violations
}

// parseHunkLineNum extracts the start line of the target file from hunk header @@ -a,b +c,d @@.
func parseHunkLineNum(hunkHeader string) int {
	plusIdx := strings.Index(hunkHeader, "+")
	if plusIdx == -1 {
		return 1
	}
	part := hunkHeader[plusIdx+1:]
	endIdx := strings.IndexAny(part, ", ")
	if endIdx != -1 {
		part = part[:endIdx]
	}
	if n, err := strconv.Atoi(strings.TrimSpace(part)); err == nil && n > 0 {
		return n
	}
	return 1
}

// IsCommitCommand determines whether a command executes a git commit or aerial-pr.sh submit.
// It parses compound commands (&&, ;, ||, |) and extracts target directory and submit flag.
func IsCommitCommand(cmd string) (isCommit bool, targetDir string, isSubmit bool) {
	subCommands := splitCommands(cmd)
	for _, sub := range subCommands {
		tokens := tokenizeCommand(sub)
		if len(tokens) == 0 {
			continue
		}

		// 1. Check for aerial-pr.sh submit
		for i, t := range tokens {
			base := filepath.Base(t)
			if base == "aerial-pr.sh" || strings.HasSuffix(t, "aerial-pr.sh") {
				for j := i + 1; j < len(tokens); j++ {
					if tokens[j] == "submit" {
						isCommit = true
						isSubmit = true
						for k := j + 1; k < len(tokens); k++ {
							if !strings.HasPrefix(tokens[k], "-") {
								targetDir = tokens[k]
								break
							}
						}
						return
					}
				}
			}
		}

		// 2. Check for git commit
		bin := strings.ToLower(filepath.Base(tokens[0]))
		if bin == "git" {
			var foundDir string
			for i := 1; i < len(tokens); i++ {
				tok := tokens[i]
				if tok == "-C" && i+1 < len(tokens) {
					foundDir = tokens[i+1]
					i++
				} else if strings.HasPrefix(tok, "-C") && len(tok) > 2 {
					foundDir = strings.TrimPrefix(tok, "-C")
				} else if tok == "commit" {
					isCommit = true
					isSubmit = false
					targetDir = foundDir
					return
				}
			}
		}
	}
	return false, "", false
}

// IsAerialConfig checks if the target repository or command targets aerial-config,
// where commit guard enforcement is intentionally disabled per user instructions.
func IsAerialConfig(targetDir string, cmd string) bool {
	// 1. Direct path check
	if strings.Contains(targetDir, "aerial-config") {
		return true
	}

	// 2. Command line flag / string check
	if strings.Contains(cmd, "aerial-config") {
		return true
	}

	dir := targetDir
	if dir == "" {
		dir = "."
	}

	// 3. Inspect .git/config in targetDir if present
	gitConfigFile := filepath.Join(dir, ".git", "config")
	if data, err := os.ReadFile(gitConfigFile); err == nil {
		if strings.Contains(string(data), "aerial-config") {
			return true
		}
	}

	// Handle git worktree or submodule where .git is a file
	gitFile := filepath.Join(dir, ".git")
	if data, err := os.ReadFile(gitFile); err == nil {
		content := string(data)
		if strings.Contains(content, "aerial-config") {
			return true
		}
		if strings.HasPrefix(strings.TrimSpace(content), "gitdir:") {
			gitDir := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(content), "gitdir:"))
			if !filepath.IsAbs(gitDir) {
				gitDir = filepath.Join(dir, gitDir)
			}
			if cfgData, err := os.ReadFile(filepath.Join(gitDir, "config")); err == nil {
				if strings.Contains(string(cfgData), "aerial-config") {
					return true
				}
			}
		}
	}

	// 4. Query git remote get-url origin
	if out, err := gitCmdRunner(dir, "remote", "get-url", "origin"); err == nil {
		if strings.Contains(strings.TrimSpace(out), "aerial-config") {
			return true
		}
	}

	return false
}

// isAllCommit checks if git commit was called with -a, -am, or --all flags.
func isAllCommit(cmd string) bool {
	subCommands := splitCommands(cmd)
	for _, sub := range subCommands {
		tokens := tokenizeCommand(sub)
		if len(tokens) == 0 {
			continue
		}
		if strings.ToLower(filepath.Base(tokens[0])) == "git" {
			isCommit := false
			hasAll := false
			for _, tok := range tokens[1:] {
				if tok == "commit" {
					isCommit = true
				}
				if tok == "-a" || tok == "--all" || strings.HasPrefix(tok, "-a") || (strings.HasPrefix(tok, "-") && !strings.HasPrefix(tok, "--") && strings.Contains(tok, "a")) {
					hasAll = true
				}
			}
			if isCommit && hasAll {
				return true
			}
		}
	}
	return false
}

// FormatCommitDenyReason constructs a human-readable explanation of detected violations.
func FormatCommitDenyReason(violations []Violation) string {
	var sb strings.Builder
	sb.WriteString("Commit blocked by commit-safety-guard: sensitive pattern violations detected:\n")
	for _, v := range violations {
		sb.WriteString(fmt.Sprintf("- %s:%d [%s] matches '%s' (sample: %s)\n",
			v.File, v.LineNum, v.Category, v.Pattern, v.Sample))
	}
	sb.WriteString("Please remove secrets, tokens, or PII before committing. ")
	sb.WriteString("If this is a configuration change intended for aerial-config, author changes in aerial-config where this guard is disabled.")
	return sb.String()
}

// CheckCommit evaluates run_command tool calls against commit safety guard invariants.
// Guaranteed fail-open: returns DecisionAllow on any unexpected error or non-commit command.
func CheckCommit(toolName string, args ToolArgs) *Decision {
	normalizedName := strings.ToLower(toolName)
	if idx := strings.LastIndex(normalizedName, ":"); idx != -1 {
		normalizedName = normalizedName[idx+1:]
	}

	if normalizedName != "run_command" {
		return &Decision{Decision: DecisionAllow}
	}

	isCommit, targetDir, isSubmit := IsCommitCommand(args.CommandLine)
	if !isCommit {
		return &Decision{Decision: DecisionAllow}
	}

	dir := targetDir
	if dir == "" {
		dir = args.Cwd
	} else if !filepath.IsAbs(dir) && args.Cwd != "" {
		dir = filepath.Join(args.Cwd, dir)
	}
	if dir == "" {
		dir = "."
	}

	if IsAerialConfig(dir, args.CommandLine) {
		return &Decision{Decision: DecisionAllow}
	}

	var diffBuilder strings.Builder

	if isSubmit {
		// aerial-pr.sh submit: check diff against HEAD, fallback to empty tree hash ONLY if HEAD does not exist (err != nil)
		diffOut, err := gitCmdRunner(dir, "diff", "HEAD")
		if err != nil {
			emptyTree := "4b825dc642cb6eb9a060e54bf8d69288fbee4904"
			diffOut, err = gitCmdRunner(dir, "diff", emptyTree)
			if err != nil {
				if fallbackDiff, diffErr := gitCmdRunner(dir, "diff"); diffErr == nil {
					diffOut = fallbackDiff
				}
			}
		}
		diffBuilder.WriteString(diffOut)

		// Untracked files: git ls-files --others --exclude-standard
		if untrackedOut, err := gitCmdRunner(dir, "ls-files", "--others", "--exclude-standard"); err == nil {
			for _, f := range strings.Split(untrackedOut, "\n") {
				f = strings.TrimSpace(f)
				if f == "" {
					continue
				}
				filePath := filepath.Join(dir, f)
				if fi, statErr := os.Stat(filePath); statErr == nil && !fi.IsDir() && fi.Size() < 1024*1024 {
					if data, readErr := os.ReadFile(filePath); readErr == nil {
						diffBuilder.WriteString("\ndiff --git a/" + f + " b/" + f + "\n")
						diffBuilder.WriteString("--- /dev/null\n")
						diffBuilder.WriteString("+++ b/" + f + "\n")
						diffBuilder.WriteString("@@ -0,0 +1,1 @@\n")
						for _, line := range strings.Split(string(data), "\n") {
							diffBuilder.WriteString("+" + line + "\n")
						}
					}
				}
			}
		}
	} else {
		// git commit: inspect staged diff
		if cachedDiff, err := gitCmdRunner(dir, "diff", "--cached"); err == nil {
			diffBuilder.WriteString(cachedDiff)
		}

		if isAllCommit(args.CommandLine) {
			if unstagedDiff, err := gitCmdRunner(dir, "diff"); err == nil {
				diffBuilder.WriteString("\n")
				diffBuilder.WriteString(unstagedDiff)
			}
		}
	}

	diff := diffBuilder.String()
	violations := ScanDiff(diff, false)
	if len(violations) > 0 {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   FormatCommitDenyReason(violations),
		}
	}

	return &Decision{Decision: DecisionAllow}
}
