package guard

import (
	"path/filepath"
	"regexp"
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
	"rm":       true,
	"touch":    true,
	"mv":       true,
	"cp":       true,
	"mkdir":    true,
	"rmdir":    true,
	"sed":      true,
	"tee":      true,
	"chmod":    true,
	"chown":    true,
	"install":  true,
	"ln":       true,
	"truncate": true,
}

// removalBinaries delete or relocate their operands, so a target that is an
// ancestor of protected state is as destructive as the state itself.
var removalBinaries = map[string]bool{
	"rm":    true,
	"rmdir": true,
	"mv":    true,
}

// commandWrappers run their trailing argv unchanged and are stripped before
// the mutating-binary lookup.
var commandWrappers = map[string]bool{
	"sudo":    true,
	"env":     true,
	"command": true,
}

// outputRedirect matches optionally fd-prefixed output redirects (>, >>, 2>, 2>>)
// with the target either attached or in the next token.
var outputRedirect = regexp.MustCompile(`^\d*>>?(.*)$`)

// IsProtectedPath returns true if the path points to or resides within a protected mount.
func IsProtectedPath(p string) bool {
	return isUnderPrefixes(p, ProtectedPrefixes)
}

// isUnderPrefixes reports whether p, after cleaning and symlink resolution of
// its nearest existing ancestor, equals or resides within one of prefixes.
func isUnderPrefixes(p string, prefixes []string) bool {
	realPath, ok := resolvePath(p)
	if !ok {
		return false
	}
	for _, prefix := range prefixes {
		realPrefix := resolvePrefix(prefix)
		if realPath == realPrefix || strings.HasPrefix(realPath, realPrefix+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// resolvePath cleans p to an absolute path and resolves symlinks in the path
// itself or, when it does not exist, its nearest existing parent directory.
func resolvePath(p string) (string, bool) {
	p = strings.TrimSpace(p)
	if p == "" {
		return "", false
	}

	clean := filepath.Clean(p)
	abs, err := filepath.Abs(clean)
	if err != nil {
		abs = clean
	}

	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return resolved, true
	}
	parent := filepath.Dir(abs)
	base := filepath.Base(abs)
	for parent != "/" && parent != "." {
		if resolvedParent, err := filepath.EvalSymlinks(parent); err == nil {
			return filepath.Join(resolvedParent, base), true
		}
		base = filepath.Join(filepath.Base(parent), base)
		parent = filepath.Dir(parent)
	}
	return abs, true
}

func resolvePrefix(prefix string) string {
	if resolved, err := filepath.EvalSymlinks(prefix); err == nil {
		return resolved
	}
	return prefix
}

// CheckShare evaluates tool calls against read-only /share invariants.
func CheckShare(toolName string, args ToolArgs) *Decision {
	normalizedName := normalizeToolName(toolName)

	switch normalizedName {
	case "replace_file_content", "write_to_file", "edit_file", "write_file", "edit", "multiedit", "write":
		if IsProtectedPath(args.FilePath()) {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   ShareDenyReason,
			}
		}

	case "run_command", "bash":
		if inspectRunCommand(args.CommandLine, args.Cwd, IsProtectedPath, IsProtectedPath) {
			return &Decision{
				Decision: DecisionDeny,
				Reason:   ShareDenyReason,
			}
		}
	}

	return &Decision{Decision: DecisionAllow}
}

func hasProtectedOutputRedirect(cmd, cwd string, protected func(string) bool) bool {
	if !strings.Contains(cmd, ">") {
		return false
	}
	tokens := strings.Fields(cmd)
	for i, t := range tokens {
		m := outputRedirect.FindStringSubmatch(t)
		if m == nil {
			continue
		}
		target := m[1]
		if target == "" && i+1 < len(tokens) {
			target = tokens[i+1]
		}
		// >&N duplicates a file descriptor rather than naming a file.
		if target != "" && !strings.HasPrefix(target, "&") && protected(resolveArg(target, cwd)) {
			return true
		}
	}
	return false
}

func gitCommandTargets(tokens []string, protected func(string) bool) bool {
	for i, t := range tokens {
		switch {
		case t == "-C" && i+1 < len(tokens) && protected(tokens[i+1]):
			return true
		case strings.HasPrefix(t, "-C") && len(t) > 2 && protected(strings.TrimPrefix(t, "-C")):
			return true
		case strings.HasPrefix(t, "--git-dir=") && protected(strings.TrimPrefix(t, "--git-dir=")):
			return true
		case strings.HasPrefix(t, "--work-tree=") && protected(strings.TrimPrefix(t, "--work-tree=")):
			return true
		}
	}
	return false
}

func hasProtectedPositionalArg(targets []string, cwd string, protected func(string) bool) bool {
	for _, target := range targets {
		if protected(resolveArg(target, cwd)) {
			return true
		}
	}
	return false
}

// resolveArg anchors a relative path argument at the command's working directory.
func resolveArg(arg, cwd string) string {
	if cwd == "" || filepath.IsAbs(arg) {
		return arg
	}
	return filepath.Join(cwd, arg)
}

// stripCommandWrappers drops leading env assignments and sudo/env/command
// wrappers (with their flags) so the real binary is tokens[0].
func stripCommandWrappers(tokens []string) []string {
	for len(tokens) > 0 {
		t := tokens[0]
		switch {
		case commandWrappers[strings.ToLower(filepath.Base(t))]:
			tokens = tokens[1:]
			for len(tokens) > 0 && strings.HasPrefix(tokens[0], "-") {
				tokens = tokens[1:]
			}
		case strings.Contains(t, "=") && !strings.HasPrefix(t, "-"):
			tokens = tokens[1:]
		default:
			return tokens
		}
	}
	return tokens
}

// mutationTargets returns the arguments bin writes to: the destination for
// cp/install/ln, the edited files for sed -i (none otherwise), and every
// positional argument for all other mutating binaries.
func mutationTargets(bin string, args []string) []string {
	var positional []string
	for i, arg := range args {
		switch {
		case (bin == "cp" || bin == "install" || bin == "ln") && arg == "-t" && i+1 < len(args):
			return []string{args[i+1]}
		case (bin == "cp" || bin == "install" || bin == "ln") && strings.HasPrefix(arg, "--target-directory="):
			return []string{strings.TrimPrefix(arg, "--target-directory=")}
		case !strings.HasPrefix(arg, "-"):
			positional = append(positional, arg)
		}
	}

	switch bin {
	case "cp", "install", "ln":
		if len(positional) == 0 {
			return nil
		}
		return positional[len(positional)-1:]
	case "sed":
		if !sedInPlace(args) {
			return nil
		}
	}
	return positional
}

func sedInPlace(args []string) bool {
	for _, arg := range args {
		if strings.HasPrefix(arg, "--in-place") {
			return true
		}
		if strings.HasPrefix(arg, "-") && !strings.HasPrefix(arg, "--") && strings.Contains(arg, "i") {
			return true
		}
	}
	return false
}

// inspectRunCommand reports whether cmd mutates a path matched by protected.
// Operands of removal binaries (rm, rmdir, mv) are checked with removal instead,
// which callers may widen to cover ancestors of the protected state.
func inspectRunCommand(cmd, cwd string, protected, removal func(string) bool) bool {
	if hasProtectedOutputRedirect(cmd, cwd, protected) {
		return true
	}

	cwdProtected := protected(cwd)
	subCommands := splitPipeline(cmd)
	for _, sub := range subCommands {
		tokens := stripCommandWrappers(tokenizeQuoteAware(sub))
		if len(tokens) == 0 {
			continue
		}

		// Unwrap bash -c <script> or sh -c <script>
		if script, ok := unwrapShellScriptCommand(tokens); ok {
			if inspectRunCommand(script, cwd, protected, removal) {
				return true
			}
		}

		cmdBin := strings.ToLower(filepath.Base(tokens[0]))

		// Inspect git operations
		if cmdBin == "git" {
			targetsProtected := gitCommandTargets(tokens, protected)
			if (cwdProtected || targetsProtected) && isGitMutating(tokens[1:]) {
				return true
			}
		}

		if !mutatingBinaries[cmdBin] {
			continue
		}

		// If Cwd is protected, check mutating binaries
		if cwdProtected {
			return true
		}

		// If mutating binary called from anywhere with a write target in protected state
		targetProtected := protected
		if removalBinaries[cmdBin] {
			targetProtected = removal
		}
		if hasProtectedPositionalArg(mutationTargets(cmdBin, tokens[1:]), cwd, targetProtected) {
			return true
		}
	}

	return false
}

func isGitMutating(args []string) bool {
	idx := 0
	for idx < len(args) {
		arg := args[idx]
		switch {
		case arg == "-C" && idx+1 < len(args):
			idx += 2
			continue
		case strings.HasPrefix(arg, "-C") || strings.HasPrefix(arg, "--git-dir=") || strings.HasPrefix(arg, "--work-tree="):
			idx++
			continue
		case strings.HasPrefix(arg, "-"):
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
