package session

import (
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

// FormatTurnEnvFileContent formats the per-daemon turn environment file sourced by BASH_ENV.
// It unsets BASH_ENV first so nested bash invocations do not re-source the file and clobber
// local overrides, followed by exporting or unsetting AERIAL_TARGET_ID, and guarding
// AERIAL_VANADIS_CHUNK_ID and VANADIS_CHUNK_ID behind AERIAL_NO_VANADIS_CHUNK so explicit unsets
// are respected across non-interactive subshells.
func FormatTurnEnvFileContent(targetID, vanadisChunkID string) string {
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

	return sb.String()
}
