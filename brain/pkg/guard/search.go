package guard

import (
	"path/filepath"
	"strings"
)

var binaryExtensions = map[string]bool{
	".so":    true,
	".a":     true,
	".o":     true,
	".dylib": true,
	".exe":   true,
	".tar":   true,
	".gz":    true,
	".tgz":   true,
	".zip":   true,
	".pyc":   true,
	".bin":   true,
	".iso":   true,
	".wasm":  true,
}

var binaryDirs = []string{
	"/bin",
	"/sbin",
	"/usr/bin",
	"/usr/sbin",
	"/usr/local/bin",
	"/usr/local/sbin",
}

// splitPipeline splits a shell command line on ;, &&, ||, and | respecting single and double quotes.
func splitPipeline(cmd string) []string {
	var cmds []string
	var current strings.Builder
	inSingle := false
	inDouble := false
	escaped := false

	chars := []rune(cmd)
	for i := 0; i < len(chars); i++ {
		r := chars[i]
		if escaped {
			current.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' && !inSingle {
			escaped = true
			current.WriteRune(r)
			continue
		}
		if r == '\'' && !inDouble {
			inSingle = !inSingle
			current.WriteRune(r)
			continue
		}
		if r == '"' && !inSingle {
			inDouble = !inDouble
			current.WriteRune(r)
			continue
		}

		if !inSingle && !inDouble {
			if r == '&' && i+1 < len(chars) && chars[i+1] == '&' {
				if s := strings.TrimSpace(current.String()); s != "" {
					cmds = append(cmds, s)
				}
				current.Reset()
				i++
				continue
			}
			if r == '|' && i+1 < len(chars) && chars[i+1] == '|' {
				if s := strings.TrimSpace(current.String()); s != "" {
					cmds = append(cmds, s)
				}
				current.Reset()
				i++
				continue
			}
			if r == ';' || r == '|' {
				if s := strings.TrimSpace(current.String()); s != "" {
					cmds = append(cmds, s)
				}
				current.Reset()
				continue
			}
		}
		current.WriteRune(r)
	}

	if s := strings.TrimSpace(current.String()); s != "" {
		cmds = append(cmds, s)
	}
	return cmds
}

// tokenizeQuoteAware tokenizes a single command respecting single and double quotes.
func tokenizeQuoteAware(sub string) []string {
	var tokens []string
	var current strings.Builder
	inSingle := false
	inDouble := false
	escaped := false

	for _, r := range sub {
		if escaped {
			current.WriteRune(r)
			escaped = false
			continue
		}
		if r == '\\' && !inSingle {
			escaped = true
			continue
		}
		if r == '\'' && !inDouble {
			inSingle = !inSingle
			continue
		}
		if r == '"' && !inSingle {
			inDouble = !inDouble
			continue
		}
		if !inSingle && !inDouble && (r == ' ' || r == '\t' || r == '\n') {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			continue
		}
		current.WriteRune(r)
	}
	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}
	return tokens
}

// CheckSearch evaluates search commands in run_command against unconstrained root, missing maxdepth,
// and binary scanning invariants.
func CheckSearch(toolName string, args ToolArgs) *Decision {
	if !strings.HasSuffix(toolName, "run_command") && toolName != "grep_search" && toolName != "find_by_name" {
		return &Decision{Decision: DecisionAllow}
	}

	cmdLine := strings.TrimSpace(args.CommandLine)
	if cmdLine == "" {
		return &Decision{Decision: DecisionAllow}
	}

	commands := splitPipeline(cmdLine)
	for _, cmd := range commands {
		tokens := tokenizeQuoteAware(cmd)
		if len(tokens) == 0 {
			continue
		}

		bin := filepath.Base(tokens[0])

		// 1. Evaluate find commands
		if bin == "find" {
			if dec := inspectFindCommand(tokens, args.Cwd); dec != nil {
				return dec
			}
			continue
		}

		// 2. Evaluate grep / search / strings commands
		if isGrepOrSearchBinary(bin) {
			if dec := inspectGrepCommand(bin, tokens); dec != nil {
				return dec
			}
			continue
		}
	}

	return &Decision{Decision: DecisionAllow}
}

func isGrepOrSearchBinary(bin string) bool {
	switch bin {
	case "grep", "egrep", "fgrep", "rg", "ag", "ack", "strings":
		return true
	default:
		return false
	}
}

func inspectFindCommand(tokens []string, cwd string) *Decision {
	hasMaxdepth := false
	for i := 1; i < len(tokens); i++ {
		t := tokens[i]
		if t == "-help" || t == "--help" || t == "-version" || t == "--version" {
			return nil
		}
		if t == "-maxdepth" || t == "--maxdepth" {
			hasMaxdepth = true
		}
	}

	// Check target path tokens
	for i := 1; i < len(tokens); i++ {
		t := tokens[i]
		if strings.HasPrefix(t, "-") {
			// Skip flags and their arguments if flag takes an argument
			if t == "-name" || t == "-iname" || t == "-path" || t == "-ipath" || t == "-type" || t == "-maxdepth" || t == "-mindepth" || t == "-mtime" || t == "-ctime" || t == "-atime" || t == "-perm" || t == "-size" || t == "-user" || t == "-group" {
				i++
			}
			continue
		}

		// Target path token
		clean := filepath.Clean(t)
		if clean == "/" || clean == "/proc" || clean == "/sys" || clean == "/dev" || clean == "/root" {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   "Unconstrained root search detected. Avoid searching the root filesystem ('/') or system directories directly. Scope searches to specific package or workspace directories.",
			}
		}

		if cwd == "/" && (clean == "." || clean == "./") {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   "Unconstrained root search detected. Current working directory is root ('/'). Scope searches to specific workspace directories.",
			}
		}

		if clean == ".git" || strings.HasPrefix(clean, ".git/") || clean == "node_modules" || strings.HasPrefix(clean, "node_modules/") {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   "Direct search into large internal repository directory (.git/node_modules) detected. Exclude internal version control and dependency trees from searches.",
			}
		}
	}

	if !hasMaxdepth {
		return &Decision{
			Decision: DecisionDeny,
			Reason:   "Unconstrained find command without -maxdepth detected. Always specify '-maxdepth <N>' (e.g. 'find <path> -maxdepth 3 -name ...') to prevent deep recursive tree traversals into .git/objects, vendor, or cache volumes.",
		}
	}

	return nil
}

func inspectGrepCommand(bin string, tokens []string) *Decision {
	hasExcludeDir := false
	isRecursive := (bin == "rg" || bin == "ag" || bin == "ack")
	hasExplicitPattern := false

	var pathTokens []string

	for i := 1; i < len(tokens); i++ {
		t := tokens[i]
		if strings.HasPrefix(t, "-") {
			if strings.HasPrefix(t, "--exclude-dir") || strings.HasPrefix(t, "--exclude") {
				hasExcludeDir = true
				if (t == "--exclude-dir" || t == "--exclude") && i+1 < len(tokens) {
					i++ // skip the exclude dir argument
				}
			}
			// Only short flags (-r, -rn, -inr) or explicit --recursive enable recursive mode for grep.
			// Do NOT match long flags like --color or --ignore-case just because they contain the letter 'r'.
			if t == "-r" || t == "-R" || t == "--recursive" || (!strings.HasPrefix(t, "--") && (strings.Contains(t, "r") || strings.Contains(t, "R"))) {
				if bin == "grep" || bin == "egrep" || bin == "fgrep" {
					isRecursive = true
				}
			}
			if t == "-e" || t == "-f" {
				hasExplicitPattern = true
				i++ // skip pattern argument
			}
			continue
		}

		// First non-flag token is pattern unless -e was passed
		if !hasExplicitPattern {
			hasExplicitPattern = true
			continue
		}

		pathTokens = append(pathTokens, t)
	}

	// For commands like `strings`, all non-flag tokens are target file paths
	if bin == "strings" {
		pathTokens = nil
		for _, t := range tokens[1:] {
			if !strings.HasPrefix(t, "-") {
				pathTokens = append(pathTokens, t)
			}
		}
	}

	for _, pt := range pathTokens {
		clean := filepath.Clean(pt)

		// Rule 1: Root search
		if clean == "/" || clean == "/proc" || clean == "/sys" || clean == "/dev" {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   "Unconstrained root search detected. Avoid searching the root filesystem ('/') or system directories directly. Scope searches to specific package or workspace directories.",
			}
		}

		// Rule 3: Giant directory search
		if clean == ".git" || strings.HasPrefix(clean, ".git/") || clean == "node_modules" || strings.HasPrefix(clean, "node_modules/") {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   "Direct search into large internal repository directory (.git/node_modules) detected. Exclude internal version control and dependency trees from searches.",
			}
		}

		// Rule 4: Binary search (extension or system binary dir)
		ext := strings.ToLower(filepath.Ext(clean))
		if binaryExtensions[ext] {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   "Binary search detected. Scanning compiled system binaries or archive files with grep/strings is prohibited. Inspect source code or documentation instead.",
			}
		}

		for _, bDir := range binaryDirs {
			if clean == bDir || strings.HasPrefix(clean, bDir+"/") {
				return &Decision{
					Decision: DecisionDeny,
					Reason:   "Binary directory search detected. Scanning system binary directories is prohibited. Inspect source code or documentation instead.",
				}
			}
		}
	}

	// For recursive standard grep targeting broad paths, require --exclude-dir
	if isRecursive && (bin == "grep" || bin == "egrep" || bin == "fgrep") && !hasExcludeDir {
		for _, pt := range pathTokens {
			clean := filepath.Clean(pt)
			if clean == "." || clean == ".." || clean == "/share" || clean == "/share/aerial" || clean == "/data" {
				return &Decision{
					Decision: DecisionDeny,
					Reason:   "Recursive grep without directory exclusions detected. Add '--exclude-dir=.git' (or use ripgrep/rg, which excludes .git by default) to prevent scanning raw git packfiles and binary objects.",
				}
			}
		}
	}

	return nil
}
