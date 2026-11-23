package prwire

import (
	"slices"
	"strings"
)

// Canonical check states ClassifyCheckState returns.
const (
	CheckStatePassed  = "passed"
	CheckStateFailed  = "failed"
	CheckStatePending = "pending"
)

// IsRoboRevCheck reports whether checkName is the RoboRev review gate:
// exactly "roborev" or "RoboRev Code Review", case-insensitive.
func IsRoboRevCheck(checkName string) bool {
	trimmed := strings.TrimSpace(checkName)
	return strings.EqualFold(trimmed, "roborev") || strings.EqualFold(trimmed, "RoboRev Code Review")
}

// CanonicalFailedCheckKey is the failed-check map key for checkName: "roborev"
// for any RoboRev review gate name, else the trimmed lowercase name.
func CanonicalFailedCheckKey(checkName string) string {
	if IsRoboRevCheck(checkName) {
		return "roborev"
	}
	return strings.ToLower(strings.TrimSpace(checkName))
}

// IsGateAggregatorCheck reports an overarching gate check ("CI Gate", "* gate").
func IsGateAggregatorCheck(checkName string) bool {
	lower := strings.ToLower(strings.TrimSpace(checkName))
	return lower == "ci gate" || strings.HasSuffix(lower, " gate")
}

// ClassifyCheckState maps a GitHub check status and conclusion to
// CheckStatePassed, CheckStateFailed or CheckStatePending, or "" when the
// check carries no status or completed with an unrecognized conclusion.
func ClassifyCheckState(status, conclusion string) string {
	s := strings.ToLower(strings.TrimSpace(status))
	c := strings.ToLower(strings.TrimSpace(conclusion))
	if s == "completed" {
		switch c {
		case "success", "neutral", "skipped":
			return CheckStatePassed
		case "failure", "timed_out", "cancelled", "action_required":
			return CheckStateFailed
		default:
			return ""
		}
	}
	if s == "" {
		return ""
	}
	return CheckStatePending
}

// AutoMergeBlockReason maps a PR's mergeable state to the reason auto-merge
// is blocked, or "" when nothing blocks it.
func AutoMergeBlockReason(mergeableState string, mergeable *bool) string {
	state := strings.ToLower(strings.TrimSpace(mergeableState))
	switch {
	case state == "dirty" || (mergeable != nil && !*mergeable):
		return "merge_conflict"
	case state == "blocked":
		return "checks_or_reviews_blocked"
	case state == "behind":
		return "branch_behind_base"
	case slices.Contains([]string{"", "clean", "unstable", "has_hooks"}, state):
		return ""
	default:
		return state
	}
}

// IsActionableAutoMergeBlock reports whether reason calls for an automated
// branch update or a conflict alert.
func IsActionableAutoMergeBlock(reason string) bool {
	clean := strings.ToLower(strings.TrimSpace(reason))
	return clean == "merge_conflict" || clean == "dirty" || strings.Contains(clean, "conflict") || clean == "branch_behind_base"
}
