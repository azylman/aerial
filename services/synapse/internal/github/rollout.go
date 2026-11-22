package github

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

const (
	StatusFailed       = "failed"
	StatusPassed       = "passed"
	StatusPending      = "pending"
	GHStatusCompleted  = "completed"
	ConclusionSuccess  = "success"
	ConclusionFailure  = "failure"
	ConclusionTimedOut = "timed_out"
)

var ansiEscapeRegex = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)

// IsRoboRevCheck returns true if the check run name corresponds to the RoboRev review gate.
func IsRoboRevCheck(checkName string) bool {
	trimmed := strings.TrimSpace(checkName)
	return strings.EqualFold(trimmed, "roborev") || strings.EqualFold(trimmed, "RoboRev Code Review")
}

// IsGateAggregatorCheck returns true if the check run represents an overarching gate check.
func IsGateAggregatorCheck(checkName string) bool {
	lower := strings.ToLower(strings.TrimSpace(checkName))
	return lower == "ci gate" || strings.HasSuffix(lower, " gate")
}

// ClassifyCheckState maps GitHub check-run status and conclusion into canonical state ("passed", "failed", "pending", or "").
func ClassifyCheckState(status, conclusion string) string {
	s := strings.ToLower(strings.TrimSpace(status))
	c := strings.ToLower(strings.TrimSpace(conclusion))
	if s == GHStatusCompleted {
		switch c {
		case ConclusionSuccess, "neutral", "skipped":
			return StatusPassed
		case ConclusionFailure, ConclusionTimedOut, "cancelled", "action_required":
			return StatusFailed
		default:
			return ""
		}
	}
	switch s {
	case "queued", "in_progress", "waiting", "requested", StatusPending:
		return StatusPending
	default:
		if s != "" {
			return StatusPending
		}
		return ""
	}
}

// ExtractAutoMergeBlockReason inspects PR mergeable state and returns the block reason.
func ExtractAutoMergeBlockReason(mergeableState string, mergeable *bool) string {
	cleanState := strings.ToLower(strings.TrimSpace(mergeableState))
	if cleanState == "dirty" || (mergeable != nil && !*mergeable) {
		return "merge_conflict"
	}
	if cleanState == "blocked" {
		return "checks_or_reviews_blocked"
	}
	if cleanState == "behind" {
		return "branch_behind_base"
	}
	if cleanState != "" && cleanState != "clean" && cleanState != "unstable" && cleanState != "has_hooks" {
		return cleanState
	}
	return ""
}

// IsActionableAutoMergeBlock returns true if the block reason requires automated branch rebase or conflict alerting.
func IsActionableAutoMergeBlock(reason string) bool {
	clean := strings.ToLower(strings.TrimSpace(reason))
	return clean == "merge_conflict" || clean == "dirty" || strings.Contains(clean, "conflict") || clean == "branch_behind_base"
}

// ExtractLogErrorLines extracts relevant error lines from raw command or job execution logs.
func ExtractLogErrorLines(rawLog string, maxLines int) []string {
	if maxLines <= 0 {
		maxLines = 10
	}
	lines := strings.Split(rawLog, "\n")
	var matched []string
	var nonBlank []string

	errorPatterns := []string{
		"fail:", "error:", "fatal:", "panic:", "failed with status",
		"--- fail", "failure", "exception:", "caused by:",
	}

	for _, l := range lines {
		trimmed := strings.TrimSpace(ansiEscapeRegex.ReplaceAllString(l, ""))
		if trimmed == "" {
			continue
		}
		nonBlank = append(nonBlank, trimmed)
		lower := strings.ToLower(trimmed)
		isErr := false
		for _, pat := range errorPatterns {
			if strings.Contains(lower, pat) {
				isErr = true
				break
			}
		}
		if isErr {
			matched = append(matched, trimmed)
		}
	}

	if len(matched) > 0 {
		if len(matched) > maxLines {
			return matched[len(matched)-maxLines:]
		}
		return matched
	}

	if len(nonBlank) > maxLines {
		return nonBlank[len(nonBlank)-maxLines:]
	}
	return nonBlank
}

// SanitizeDetailsForPrompt strips ANSI color codes, cleans formatting, and bounds length safely.
func SanitizeDetailsForPrompt(details string, maxRunes int) string {
	if maxRunes <= 0 {
		maxRunes = 2000
	}
	cleaned := strings.TrimSpace(ansiEscapeRegex.ReplaceAllString(details, ""))
	return strutil.TruncateRunes(cleaned, maxRunes)
}

// FormatCheckFailurePrompt formats a prompt describing a check failure for remediation.
func FormatCheckFailurePrompt(repo string, prNumber int, headSHA, checkName, details, checkURL string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "CI check %q failed on %s#%d (commit %s).\n", checkName, model.NormalizeRepo(repo), prNumber, headSHA)
	if strings.TrimSpace(checkURL) != "" {
		fmt.Fprintf(&sb, "Check URL: %s\n", strings.TrimSpace(checkURL))
	}
	if strings.TrimSpace(details) != "" {
		fmt.Fprintf(&sb, "Details:\n```\n%s\n```\n", SanitizeDetailsForPrompt(details, 2000))
	}
	return sb.String()
}

// FormatRoboRevPrompt formats a prompt describing RoboRev findings for automated remediation.
func FormatRoboRevPrompt(repo string, prNumber int, headSHA, comment string) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "RoboRev review failed on %s#%d (commit %s).\n", model.NormalizeRepo(repo), prNumber, headSHA)
	if strings.TrimSpace(comment) != "" {
		fmt.Fprintf(&sb, "Review Findings:\n```\n%s\n```\n", SanitizeDetailsForPrompt(comment, 3000))
	}
	return sb.String()
}

// FormatCollaboratorPRTarget builds a markdown link and metadata summary for a PR.
func FormatCollaboratorPRTarget(repo string, prNumber int, prTitle, branch, headSHA string) string {
	normRepo := model.NormalizeRepo(repo)
	cleanTitle := strings.TrimSpace(prTitle)
	cleanBranch := strings.TrimPrefix(strings.TrimSpace(branch), "refs/heads/")
	shortSHA := strings.TrimSpace(headSHA)
	if len(shortSHA) > 7 {
		shortSHA = shortSHA[:7]
	}

	prLink := fmt.Sprintf("[%s#%d](https://github.com/%s/pull/%d)", normRepo, prNumber, normRepo, prNumber)
	if normRepo == "" {
		prLink = fmt.Sprintf("PR #%d", prNumber)
	}

	var meta []string
	if cleanTitle != "" {
		meta = append(meta, fmt.Sprintf("%q", cleanTitle))
	}
	if cleanBranch != "" {
		meta = append(meta, "branch: "+cleanBranch)
	}
	if shortSHA != "" {
		meta = append(meta, "commit: "+shortSHA)
	}
	if len(meta) > 0 {
		return fmt.Sprintf("%s (%s)", prLink, strings.Join(meta, ", "))
	}
	return prLink
}

// BuildCollaboratorAlertMessage builds a notification message when a collaborator's PR fails CI.
func BuildCollaboratorAlertMessage(mention, repo string, prNumber int, prTitle, branch, headSHA, checkName, checkURL string) string {
	cleanMention := strings.TrimSpace(mention)
	cleanCheckName := strings.TrimSpace(checkName)
	target := FormatCollaboratorPRTarget(repo, prNumber, prTitle, branch, headSHA)

	var header strings.Builder
	if cleanMention != "" {
		header.WriteString(cleanMention)
		header.WriteString(" ")
	}
	if cleanCheckName != "" {
		fmt.Fprintf(&header, "CI check %q failed for %s.", cleanCheckName, target)
	} else {
		fmt.Fprintf(&header, "CI check failed for %s.", target)
	}

	if cleanURL := strings.TrimSpace(checkURL); cleanURL != "" {
		fmt.Fprintf(&header, "\nCheck URL: %s", cleanURL)
	}

	return header.String()
}

// BuildCollaboratorConflictMessage builds a notification message when a collaborator's PR has merge conflicts.
func BuildCollaboratorConflictMessage(mention, repo string, prNumber int, prTitle, branch, headSHA string) string {
	target := FormatCollaboratorPRTarget(repo, prNumber, prTitle, branch, headSHA)
	cleanMention := strings.TrimSpace(mention)
	if cleanMention != "" {
		return fmt.Sprintf("%s Merge conflict with `main` detected on %s — automatic branch update could not resolve conflicts. Please rebase or merge `origin/main` and push.", cleanMention, target)
	}
	return fmt.Sprintf("Merge conflict with `main` detected on %s — automatic branch update could not resolve conflicts. Please rebase or merge `origin/main` and push.", target)
}
