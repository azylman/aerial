package guard

import (
	"path/filepath"
	"strings"
)

// LiveStatePrefixes lists live runtime state that agents must never mutate in place.
var LiveStatePrefixes = []string{"/data/ssh"}

// LiveStateDenyReason is returned when a tool call attempts to mutate live SSH state.
const LiveStateDenyReason = "Target is live SSH state. /data/ssh holds the key, config, and known_hosts " +
	"every ssh invocation in this pod depends on, and is written only by the ssh-bootstrap initContainer. " +
	"Test scripts against a copy instead: d=$(mktemp -d) && cp -a /data/ssh/. \"$d\"/ and point the script at \"$d\"."

// IsLiveStatePath returns true if the path points to or resides within live SSH state.
func IsLiveStatePath(p string) bool {
	return isUnderPrefixes(p, LiveStatePrefixes)
}

// isLiveStateRemovalTarget extends IsLiveStatePath to strict ancestors of live
// state, since removing or moving /data takes /data/ssh with it.
func isLiveStateRemovalTarget(p string) bool {
	if IsLiveStatePath(p) {
		return true
	}
	realPath, ok := resolvePath(p)
	if !ok {
		return false
	}
	for _, prefix := range LiveStatePrefixes {
		realPrefix := resolvePrefix(prefix)
		if realPath == string(filepath.Separator) || strings.HasPrefix(realPrefix, realPath+string(filepath.Separator)) {
			return true
		}
	}
	return false
}

// CheckLiveState evaluates tool calls against the live SSH state invariant.
func CheckLiveState(toolName string, args ToolArgs) *Decision {
	switch normalizeToolName(toolName) {
	case "replace_file_content", "write_to_file", "edit_file", "write_file", "edit", "multiedit", "write":
		if IsLiveStatePath(args.FilePath()) {
			return &Decision{Decision: DecisionDeny, Reason: LiveStateDenyReason}
		}

	case "run_command", "bash":
		if inspectRunCommand(args.CommandLine, args.Cwd, IsLiveStatePath, isLiveStateRemovalTarget) {
			return &Decision{Decision: DecisionDeny, Reason: LiveStateDenyReason}
		}
	}

	return &Decision{Decision: DecisionAllow}
}
