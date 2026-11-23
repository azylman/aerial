package queue

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/runner"
)

var standalonePRRemediationRegex = regexp.MustCompile(`PR #(\d+)\s+on\s+([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)|(?:on|for repo)\s+([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)\s+\((?:commit: [^,]+,\s*)?PR #(\d+)`)

func extractPRRemediationTarget(prompt string) (repo string, prNum int, ok bool) {
	m := standalonePRRemediationRegex.FindStringSubmatch(prompt)
	if len(m) < 5 {
		return "", 0, false
	}
	prNumStr, repo := m[1], m[2]
	if prNumStr == "" {
		repo, prNumStr = m[3], m[4]
	}
	prNum, err := strconv.Atoi(prNumStr)
	return repo, prNum, err == nil && prNum > 0
}

func extractVanadisBuildID(chunkID, sessionID, targetID string) string {
	if strings.HasPrefix(chunkID, "build-") {
		if idx := strings.LastIndex(chunkID, "-chunk-"); idx > 0 {
			return chunkID[:idx]
		}
	}
	if after, ok := strings.CutPrefix(sessionID, "vanadis-"); ok {
		if idx := strings.LastIndex(after, "-chunk-"); idx > 0 {
			return after[:idx]
		}
	}
	if t := runner.NormalizeEnvTargetID(targetID); strings.HasPrefix(t, "build-") {
		if idx := strings.LastIndex(t, "-chunk-"); idx > 0 {
			return t[:idx]
		}
		return t
	}
	return ""
}

func extractVanadisDispatchInfo(d *db.Dispatch) (chunkID, buildID string) {
	if d == nil {
		return "", ""
	}
	if chunk := strings.TrimSpace(d.VanadisChunkID); chunk != "" {
		chunkID = chunk
	}
	if chunkID == "" {
		if t := runner.NormalizeEnvTargetID(d.TargetID); strings.HasPrefix(t, "build-") || strings.HasPrefix(t, "pr-") {
			chunkID = t
		}
	}
	if chunkID == "" {
		if after, ok := strings.CutPrefix(d.SessionID, "vanadis-"); ok {
			chunkID = strings.TrimSpace(after)
		}
	}
	buildID = extractVanadisBuildID(chunkID, d.SessionID, d.TargetID)
	if chunkID == "" {
		if repo, prNum, ok := extractPRRemediationTarget(d.Prompt); ok {
			prID := fmt.Sprintf("pr-%s-%d", repo, prNum)
			return prID, prID
		}
	}
	return chunkID, buildID
}
