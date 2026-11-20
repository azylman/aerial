package session

import (
	"os"
	"path/filepath"
	"strings"
)

// NormalizeEnvTargetID normalizes a daemon target key for the AERIAL_TARGET_ID
// environment variable. It strips "discord-" (and optional pool prefixes like
// "stateful_medium:discord-") for Discord targets, and returns "" for headless
// or pooled worker keys ("heartbeat-*", "dispatch-*", "worker-*", "vanadis-*", or colon-scoped
// pool keys like "discord:worker-0" and "ephemeral:worker-0").
func NormalizeEnvTargetID(id string) string {
	clean := strings.TrimSpace(id)
	if clean == "" {
		return ""
	}
	if idx := strings.LastIndex(clean, ":"); idx >= 0 {
		suffix := strings.TrimSpace(clean[idx+1:])
		if !strings.HasPrefix(suffix, "discord-") {
			return ""
		}
		clean = suffix
	}
	if strings.HasPrefix(clean, "heartbeat-") || strings.HasPrefix(clean, "dispatch-") || strings.HasPrefix(clean, "worker-") || strings.HasPrefix(clean, "vanadis-") {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(clean, "discord-"))
}

// DefaultK8sSATokenPath is the standard Kubernetes projected ServiceAccount token path.
const DefaultK8sSATokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"

// SyncTurnSATokenFile copies the projected ServiceAccount token from srcPath (defaulting to
// DefaultK8sSATokenPath when empty) into destPath with mode 0600 owned by uid 1000:1000 when
// running as root, returning the trimmed destPath when a non-empty token is synced or "" if
// srcPath is missing or empty.
func SyncTurnSATokenFile(srcPath, destPath string, euid int, chownFn func(string, int, int) error) (string, error) {
	cleanDest := strings.TrimSpace(destPath)
	if cleanDest == "" {
		return "", nil
	}
	cleanSrc := strings.TrimSpace(srcPath)
	if cleanSrc == "" {
		cleanSrc = DefaultK8sSATokenPath
	}
	raw, err := os.ReadFile(cleanSrc)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	token := strings.TrimSpace(string(raw))
	if token == "" {
		return "", nil
	}
	if err := os.MkdirAll(filepath.Dir(cleanDest), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(cleanDest, []byte(token+"\n"), 0o600); err != nil {
		return "", err
	}
	if err := os.Chmod(cleanDest, 0o600); err != nil {
		return "", err
	}
	if euid == 0 && chownFn != nil {
		if err := chownFn(cleanDest, 1000, 1000); err != nil {
			return "", err
		}
	}
	return cleanDest, nil
}

// FormatTurnEnvFileContent formats the per-daemon turn environment file sourced by BASH_ENV.
// It unsets BASH_ENV first so nested bash invocations do not re-source the file and clobber
// local overrides, followed by exporting or unsetting AERIAL_TARGET_ID, and guarding
// AERIAL_VANADIS_CHUNK_ID and VANADIS_CHUNK_ID behind AERIAL_NO_VANADIS_CHUNK so explicit unsets
// are respected across non-interactive subshells. When saTokenPath is provided, it also exports
// AERIAL_SA_TOKEN_PATH so unprivileged (uid=1000) CLI helpers can read the rotated SA token.
func FormatTurnEnvFileContent(targetID, vanadisChunkID string, saTokenPath ...string) string {
	var sb strings.Builder
	sb.WriteString("unset BASH_ENV\n")

	normTarget := NormalizeEnvTargetID(targetID)
	if normTarget != "" {
		sb.WriteString("export AERIAL_TARGET_ID='")
		sb.WriteString(strings.ReplaceAll(normTarget, "'", `'\''`))
		sb.WriteString("'\n")
	} else {
		sb.WriteString("unset AERIAL_TARGET_ID\n")
	}

	cleanChunk := strings.TrimSpace(vanadisChunkID)
	if cleanChunk != "" {
		escaped := strings.ReplaceAll(cleanChunk, "'", `'\''`)
		sb.WriteString("if [ \"${AERIAL_NO_VANADIS_CHUNK:-0}\" != \"1\" ]; then\n")
		sb.WriteString("export AERIAL_VANADIS_CHUNK_ID='" + escaped + "'\n")
		sb.WriteString("export VANADIS_CHUNK_ID='" + escaped + "'\n")
		sb.WriteString("else\n")
		sb.WriteString("unset AERIAL_VANADIS_CHUNK_ID\n")
		sb.WriteString("unset VANADIS_CHUNK_ID\n")
		sb.WriteString("fi\n")
	} else {
		sb.WriteString("unset AERIAL_VANADIS_CHUNK_ID\n")
		sb.WriteString("unset VANADIS_CHUNK_ID\n")
	}

	if len(saTokenPath) > 0 {
		if cleanTokenPath := strings.TrimSpace(saTokenPath[0]); cleanTokenPath != "" {
			escapedTokenPath := strings.ReplaceAll(cleanTokenPath, "'", `'\''`)
			sb.WriteString("export AERIAL_SA_TOKEN_PATH='" + escapedTokenPath + "'\n")
		}
	}

	return sb.String()
}
