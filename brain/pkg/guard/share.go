package guard

import (
	"path/filepath"
	"strings"
)

// ProtectedPrefixes defines the read-only volume mounts that must not be mutated.
var ProtectedPrefixes = []string{
	"/share/aerial",
	"/share/aerial-config",
}

// ShareDenyReason is returned when a tool call attempts to mutate read-only mounts.
const ShareDenyReason = "Target is mounted read-only (:ro). Direct edits and mutating commands in " +
	"/share/aerial and /share/aerial-config are blocked to prevent EROFS errors. " +
	"Please initialize an ephemeral scratch workspace via 'scripts/aerial-pr.sh init <repo>' " +
	"to author and verify your changes safely."

var mutatingGitSubcommands = map[string]bool{
	"add":         true,
	"commit":      true,
	"checkout":    true,
	"switch":      true,
	"merge":       true,
	"rebase":      true,
	"reset":       true,
	"pull":        true,
	"push":        true,
	"apply":       true,
	"cherry-pick": true,
	"revert":      true,
	"stash":       true,
}

var mutatingBinaries = map[string]bool{
	"rm":    true,
	"touch": true,
	"mv":    true,
	"cp":    true,
	"mkdir": true,
	"rmdir": true,
	"sed":   true,
	"tee":   true,
	"chmod": true,
	"chown": true,
}

// IsProtectedPath returns true if the path points to or resides within a protected mount.
func IsProtectedPath(p string) bool {
	p = strings.TrimSpace(p)
	if p == "" {
		return false
	}

	clean := filepath.Clean(p)
	abs, err := filepath.Abs(clean)
	if err != nil {
		abs = clean
	}

	// Resolve symlinks if path or ancestors exist
	realPath := abs
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		realPath = resolved
	} else {
		// Resolve nearest existing parent directory
		parent := filepath.Dir(abs)
		base := filepath.Base(abs)
		for parent != "/" && parent != "." {
			if resolvedParent, err := filepath.EvalSymlinks(parent); err == nil {
				realPath = filepath.Join(resolvedParent, base)
				break
			}
			base = filepath.Join(filepath.Base(parent), base)
			parent = filepath.Dir(parent)
		}
	}

	for _, prefix := range ProtectedPrefixes {
		realPrefix := prefix
		if resolved, err := filepath.EvalSymlinks(prefix); err == nil {
			realPrefix = resolved
		}
		if realPath == realPrefix || strings.HasPrefix(realPath, realPrefix+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// CheckShare evaluates tool calls against read-only /share invariants.
func CheckShare(toolName string, args ToolArgs) *Decision {
	normalizedName := strings.ToLower(toolName)
	if idx := strings.LastIndex(normalizedName, ":"); idx != -1 {
		normalizedName = normalizedName[idx+1:]
	}

	switch normalizedName {
	case "replace_file_content", "write_to_file":
		if IsProtectedPath(args.TargetFile) {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   ShareDenyReason,
			}
		}

	case "run_command":
		if inspectRunCommand(args.CommandLine, args.Cwd) {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   ShareDenyReason,
			}
		}
	}

	return &Decision{Decision: DecisionAllow}
}

func inspectRunCommand(cmd, cwd string) bool {
	cwdProtected := IsProtectedPath(cwd)

	// Check for output redirection to protected path: > /share/... or >> /share/...
	if strings.Contains(cmd, ">") {
		tokens := strings.Fields(cmd)
		for i, t := range tokens {
			if strings.HasPrefix(t, ">") {
				target := strings.TrimLeft(t, ">")
				if target == "" && i+1 < len(tokens) {
					target = tokens[i+1]
				}
				if target != "" && IsProtectedPath(target) {
					return true
				}
			}
		}
	}

	// Split compound commands by operators (;, &&, ||, |, \n)
	subCommands := splitCommands(cmd)
	for _, sub := range subCommands {
		tokens := tokenizeCommand(sub)
		if len(tokens) == 0 {
			continue
		}

		cmdBin := strings.ToLower(filepath.Base(tokens[0]))

		// Inspect git operations
		if cmdBin == "git" {
			targetsShare := false
			for i, t := range tokens {
				if t == "-C" && i+1 < len(tokens) && IsProtectedPath(tokens[i+1]) {
					targetsShare = true
					break
				} else if strings.HasPrefix(t, "-C") && len(t) > 2 && IsProtectedPath(strings.TrimPrefix(t, "-C")) {
					targetsShare = true
					break
				} else if strings.HasPrefix(t, "--git-dir=") && IsProtectedPath(strings.TrimPrefix(t, "--git-dir=")) {
					targetsShare = true
					break
				} else if strings.HasPrefix(t, "--work-tree=") && IsProtectedPath(strings.TrimPrefix(t, "--work-tree=")) {
					targetsShare = true
					break
				}
			}

			if (cwdProtected || targetsShare) && isGitMutating(tokens[1:]) {
				return true
			}
		}

		// If Cwd is protected, check mutating binaries
		if cwdProtected && mutatingBinaries[cmdBin] {
			return true
		}

		// If mutating binary called from anywhere with target arg in share
		if mutatingBinaries[cmdBin] {
			for _, arg := range tokens[1:] {
				if strings.HasPrefix(arg, "-") {
					continue
				}
				if IsProtectedPath(arg) {
					return true
				}
			}
		}
	}

	return false
}

func isGitMutating(args []string) bool {
	idx := 0
	for idx < len(args) {
		arg := args[idx]
		if arg == "-C" && idx+1 < len(args) {
			idx += 2
			continue
		} else if strings.HasPrefix(arg, "-C") || strings.HasPrefix(arg, "--git-dir=") || strings.HasPrefix(arg, "--work-tree=") {
			idx++
			continue
		} else if strings.HasPrefix(arg, "-") {
			idx++
			continue
		}

		subcmd := strings.ToLower(arg)
		if mutatingGitSubcommands[subcmd] {
			return true
		}

		// Handle `git branch -d` or `git branch -D`
		if subcmd == "branch" {
			for _, opt := range args[idx+1:] {
				if opt == "-d" || opt == "-D" || strings.HasPrefix(opt, "--delete") {
					return true
				}
			}
		}
		break
	}
	return false
}

func splitCommands(cmd string) []string {
	// Replaces operators with newline delimiter then splits
	s := cmd
	for _, op := range []string{";", "&&", "||", "|"} {
		s = strings.ReplaceAll(s, op, "\n")
	}
	lines := strings.Split(s, "\n")
	var result []string
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" {
			result = append(result, l)
		}
	}
	return result
}

func tokenizeCommand(sub string) []string {
	// Simple whitespace tokenizer that strips surrounding quotes
	var tokens []string
	for _, field := range strings.Fields(sub) {
		trimmed := strings.Trim(field, `"'`)
		if trimmed != "" {
			tokens = append(tokens, trimmed)
		}
	}
	return tokens
}
