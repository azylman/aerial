// Package prompt builds worker/resume/conflict/deploy-failure prompts and scans
// local /share repository checkouts for AST symbol spans.
package prompt

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

const maxPromptRunes = 6000

// Action vocabulary stays local; pure repo/branch helpers delegate to the
// canonical internal/model copies.
const chunkModeAction = "action"

var declKeywordPrefixes = []string{"func ", "type ", "var ", "const ", "export ", "function ", "class "}

// FormatCIFailurePrompt formats the failure prompt when a CI check fails.
func FormatCIFailurePrompt(ev pubsub.PRLifecycleEvent, details string) string {
	var extraParts []string
	cleanBranch := strings.TrimPrefix(strings.TrimSpace(ev.Branch), "refs/heads/")
	if cleanBranch != "" {
		extraParts = append(extraParts, "Branch: "+cleanBranch)
	}
	if cleanURL := strings.TrimSpace(ev.CheckURL); cleanURL != "" {
		extraParts = append(extraParts, "Check URL: "+cleanURL)
	}
	extraLine := strings.Join(extraParts, "\n")

	shortRepo := store.FirstNonEmpty(model.CleanRepoName(ev.Repo), "aerial")
	branchArg := store.FirstNonEmpty(cleanBranch, "<branch>")
	checkoutAndSubmit := fmt.Sprintf("Check out the existing PR branch via '/share/aerial/scripts/aerial-pr.sh init %s --branch %s' and push updates via '/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> \"<commit_msg>\"' (PR_DESCRIPTION.md is not required when updating an existing PR; never run bare 'git push').", shortRepo, branchArg)

	directive := "Please investigate and fix the failure by inspecting the failing logs, diagnosing the issue, verifying with local checks, and pushing a fix. " + checkoutAndSubmit
	if isRoboRevCheckName(ev.CheckName) {
		directive = fmt.Sprintf("Please critically evaluate each RoboRev finding against codebase reality and architectural intent. Fix verified issues, or record rebuttal via '/share/aerial/scripts/aerial-pr.sh --repo %s override-roborev %d '<technical_rebuttal>''. %s", shortRepo, ev.PRNumber, checkoutAndSubmit)
	}
	return formatErrorDetailsBlock(
		fmt.Sprintf("CI check %q failed for PR #%d on %s (head commit: %s).", ev.CheckName, ev.PRNumber, ev.Repo, ev.HeadSHA),
		details, extraLine,
		directive,
	)
}

// FormatNoPRResumePrompt builds a self-contained resume prompt for a PR chunk
// whose previous turn ended without opening a PR. It re-includes the full chunk
// prompt because sessions rotate and an aborted conversation may be damaged.
func FormatNoPRResumePrompt(chunk model.VanadisChunk, chunkPrompt string) string {
	chunkID := strings.TrimSpace(chunk.ID)
	return fmt.Sprintf(`[VANADIS AUTOMATIC RESUME]
Your previous turn for chunk %s ended without opening a PR. This is the single automatic resume; if this turn also ends without a PR, the chunk and build fail.

Before doing anything else, check whether a PR already exists for this chunk's branch (branch name contains "vanadis-%s"): an attach failure can leave an open PR that Vanadis has not recorded. If one exists, re-run '/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> "<commit_msg>" --vanadis-chunk %s' from its checkout only if needed to attach it.

Otherwise, finish the work and verification below and submit via '/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> "<commit_msg>" --vanadis-chunk %s'. Call fail_chunk with a reason only if genuinely blocked. Never end the turn while a background command is still needed for the next step; run it in the foreground and wait for it.

The original chunk instructions follow.

%s`, chunkID, chunkID, chunkID, chunkID, chunkPrompt)
}

// FormatConflictPrompt formats the conflict prompt when merge conflict is detected.
func FormatConflictPrompt(ev pubsub.PRLifecycleEvent) string {
	shortRepo := store.FirstNonEmpty(model.CleanRepoName(ev.Repo), "aerial")
	cleanBranch := store.FirstNonEmpty(strings.TrimPrefix(strings.TrimSpace(ev.Branch), "refs/heads/"), "<branch>")
	return fmt.Sprintf("Merge conflict detected on PR #%d on %s (branch: %s, head: %s). Please check out the branch via '/share/aerial/scripts/aerial-pr.sh init %s --branch %s', merge origin/main to resolve conflicts, verify locally, and push to update the PR via '/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> \"<commit_msg>\"' (PR_DESCRIPTION.md is not required when updating an existing PR; never run bare 'git push').", ev.PRNumber, ev.Repo, cleanBranch, ev.HeadSHA, shortRepo, cleanBranch)
}

// FormatDeployFailureBanner formats the chunk banner when deployment or git sync fails.
func FormatDeployFailureBanner(ev pubsub.PRLifecycleEvent) string {
	var banner string
	if strings.EqualFold(strings.TrimSpace(ev.Event), "sync_failed") {
		banner = "Git sync failed"
	} else if cleanJob := strings.TrimSpace(ev.JobName); cleanJob != "" {
		banner = "Deployment failed for " + cleanJob
	} else {
		banner = "Deployment failed"
	}
	if cleanDetails := strings.TrimSpace(ev.Details); cleanDetails != "" {
		banner = fmt.Sprintf("%s: %s", banner, cleanDetails)
	}
	return banner
}

func formatDeployFailureHeader(repo, commit, jobName string, prNumber int) string {
	if cleanJob := strings.TrimSpace(jobName); cleanJob != "" {
		return fmt.Sprintf("Deployment failed for job %s on %s (PR #%d, commit: %s).", cleanJob, repo, prNumber, commit)
	}
	return fmt.Sprintf("Deployment failed for PR #%d on %s (commit: %s).", prNumber, repo, commit)
}

// FormatDeployFailurePrompt formats the failure prompt when deployment or git sync fails.
func FormatDeployFailurePrompt(ev pubsub.PRLifecycleEvent) string {
	commit := store.FirstNonEmpty(ev.MergeSHA, ev.HeadSHA)
	cleanEvent := strings.ToLower(strings.TrimSpace(ev.Event))
	var extraLine string
	cleanBranch := strings.TrimPrefix(strings.TrimSpace(ev.Branch), "refs/heads/")
	if cleanBranch != "" {
		extraLine = "Branch: " + cleanBranch
	}
	if cleanEvent == "sync_failed" {
		header := fmt.Sprintf("Git sync failed for PR #%d on %s (commit: %s).", ev.PRNumber, ev.Repo, commit)
		return formatErrorDetailsBlock(header, ev.Details, extraLine, "Please investigate and fix the git sync failure.")
	}
	header := formatDeployFailureHeader(ev.Repo, commit, ev.JobName, ev.PRNumber)
	return formatErrorDetailsBlock(header, ev.Details, extraLine, "Please investigate and fix the deployment failure.")
}

func formatErrorDetailsBlock(header, details, extraLine, footer string) string {
	lines := []string{header}
	if clean := strings.TrimSpace(details); clean != "" {
		lines = append(lines, fmt.Sprintf("Error details:\n```\n%s\n```", TruncatePromptDetails(clean)))
	}
	if cleanExtra := strings.TrimSpace(extraLine); cleanExtra != "" {
		lines = append(lines, cleanExtra)
	}
	if footer != "" {
		lines = append(lines, footer)
	}
	return strings.Join(lines, "\n")
}

func isRoboRevCheckName(name string) bool {
	lower := strings.ToLower(strings.TrimSpace(name))
	return strings.Contains(lower, "roborev")
}

// TruncatePromptRunes caps s at maxRunes runes with a truncation marker.
func TruncatePromptRunes(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "... (truncated)"
}

// TruncatePromptDetails sanitizes details for prompt embedding.
func TruncatePromptDetails(s string) string {
	return TruncatePromptRunes(strings.ReplaceAll(s, "```", "'''"), maxPromptRunes)
}

// AST Symbol Spans resolution
func repoCheckoutDir(shareRoot, repo string) (string, bool) {
	name := strings.TrimSpace(repo)
	if idx := strings.LastIndex(name, "/"); idx >= 0 {
		name = name[idx+1:]
	}
	if name == "" || name == "." || name == ".." {
		return "", false
	}
	return filepath.Join(strings.TrimSpace(shareRoot), name), true
}

func normalizeSymbolName(symbol string) string {
	sym := strings.TrimSpace(symbol)
	sym = strings.TrimSuffix(sym, "()")
	if idx := strings.LastIndex(sym, "."); idx >= 0 {
		sym = sym[idx+1:]
	}
	return strings.TrimSpace(sym)
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

func isIdentByte(b byte) bool {
	return b == '_' || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

func isBoundedIdent(line string, start, end int) bool {
	if start > 0 && isIdentByte(line[start-1]) {
		return false
	}
	if end < len(line) && isIdentByte(line[end]) {
		return false
	}
	return true
}

func containsIdent(line, ident string) bool {
	if ident == "" {
		return false
	}
	idx := 0
	for {
		pos := strings.Index(line[idx:], ident)
		if pos < 0 {
			return false
		}
		start := idx + pos
		end := start + len(ident)
		if isBoundedIdent(line, start, end) {
			return true
		}
		idx = start + 1
	}
}

func stripLineComment(line string) string {
	if before, _, found := strings.Cut(line, "//"); found {
		return before
	}
	return line
}

// FindSymbolSpan locates the 1-based start and end line for symbol in content.
func FindSymbolSpan(content, symbol string) (int, int, bool) {
	sym := normalizeSymbolName(symbol)
	if sym == "" || content == "" {
		return 0, 0, false
	}
	lines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	for idx, line := range lines {
		clean := strings.TrimSpace(stripLineComment(line))
		if hasAnyPrefix(clean, declKeywordPrefixes) && containsIdent(clean, sym) {
			end := idx + 1
			for j := idx + 1; j < len(lines); j++ {
				if strings.HasPrefix(lines[j], "}") {
					end = j + 1
					break
				}
			}
			return idx + 1, end, true
		}
	}
	for idx, line := range lines {
		clean := strings.TrimSpace(stripLineComment(line))
		if clean != "" && containsIdent(clean, sym) {
			return idx + 1, idx + 1, true
		}
	}
	return 0, 0, false
}

// ResolveSymbolSpans resolves start and end lines for target symbols in repo checkout under shareRoot.
func ResolveSymbolSpans(shareRoot, repo string, targetFiles, targetSymbols []string) ([]string, bool) {
	repoDir, ok := repoCheckoutDir(shareRoot, repo)
	if !ok {
		return nil, false
	}
	var spans []string
	allResolved := true
	for _, f := range targetFiles {
		cleanFile := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(f), "./"), "/")
		fullPath := filepath.Join(repoDir, cleanFile)
		data, err := os.ReadFile(fullPath)
		if err != nil {
			allResolved = false
			continue
		}
		content := string(data)
		var fileSpans []string
		for _, sym := range targetSymbols {
			start, end, found := FindSymbolSpan(content, sym)
			if found {
				fileSpans = append(fileSpans, fmt.Sprintf("%s (L%d-L%d)", sym, start, end))
			} else {
				allResolved = false
			}
		}
		if len(fileSpans) > 0 {
			spans = append(spans, fmt.Sprintf("- %s: %s", cleanFile, strings.Join(fileSpans, ", ")))
		}
	}
	return spans, allResolved && len(spans) > 0
}

// FormatChunkPromptWithShareRoot renders a structured prompt for executing a specific chunk.
func FormatChunkPromptWithShareRoot(build model.VanadisBuild, chunks []model.VanadisChunk, activeIdx int, shareRoot string) (string, error) {
	if len(chunks) == 0 {
		return "", errors.New("chunks slice cannot be empty")
	}
	if activeIdx < 0 || activeIdx >= len(chunks) {
		return "", fmt.Errorf("active chunk index %d out of range [0, %d)", activeIdx, len(chunks))
	}
	total := len(chunks)
	active := chunks[activeIdx]
	isActionChunk := active.Mode == chunkModeAction
	activeRepo := ""
	if !isActionChunk {
		activeRepo = store.FirstNonEmpty(model.CleanRepoName(active.Repo), "aerial")
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: %s\n", build.ID)
	if bTitle := strings.TrimSpace(build.Title); bTitle != "" {
		fmt.Fprintf(&sb, "Build Title: %s\n", bTitle)
	}
	if isActionChunk {
		fmt.Fprintf(&sb, "Dispatch ID: %s\nInclude this dispatch_id in action completion and failure calls.\n", model.ActionDispatchID(active))
	}
	if !isActionChunk {
		fmt.Fprintf(&sb, "Repository: %s\n", activeRepo)
	}
	fmt.Fprintf(&sb, "Tier: %d\n\n=== Build Objective ===\n%s\n\n", build.Tier, strings.TrimSpace(build.Objective))
	if sharedCtx := strings.TrimSpace(build.SharedContext); sharedCtx != "" {
		fmt.Fprintf(&sb, "=== Shared Build Context ===\n%s\n\n", sharedCtx)
	}

	sb.WriteString("=== Completed Chunks Ledger ===\n")
	if activeIdx == 0 {
		sb.WriteString("(none — this is the first chunk)\n\n")
	} else {
		for k, c := range chunks[:activeIdx] {
			fmt.Fprintf(&sb, "- [Chunk %d/%d] %s\n", k+1, total, c.Title)
		}
		sb.WriteString("\n")
	}

	targetFilesStr := store.FirstNonEmpty(strings.Join(active.TargetFiles, ", "), "(none specified)")
	fmt.Fprintf(&sb, "=== Active Chunk (Chunk %d of %d) ===\nChunk ID: %s\nTitle: %s\n", activeIdx+1, total, active.ID, active.Title)
	if isActionChunk {
		fmt.Fprintf(&sb, "Dispatch ID: %s\nInclude this dispatch_id in action completion and failure calls.\n", model.ActionDispatchID(active))
	}
	if !isActionChunk {
		fmt.Fprintf(&sb, "Repository: %s\nTarget Files: %s\n", activeRepo, targetFilesStr)
		if len(active.TargetSymbols) > 0 {
			fmt.Fprintf(&sb, "Target Symbols: %s\n", strings.Join(active.TargetSymbols, ", "))
		}
		spans, _ := ResolveSymbolSpans(shareRoot, activeRepo, active.TargetFiles, active.TargetSymbols)
		if len(spans) > 0 {
			fmt.Fprintf(&sb, "Target Symbol Spans:\n%s\n", strings.Join(spans, "\n"))
		}
	}
	fmt.Fprintf(&sb, "Instructions:\n%s\n\n", active.Instructions)

	sb.WriteString("=== Remaining Chunks ===\n")
	if activeIdx == total-1 {
		sb.WriteString("(none — this is the final chunk)\n\n")
	} else {
		for k, c := range chunks[activeIdx+1:] {
			fmt.Fprintf(&sb, "- [Chunk %d/%d] %s\n", activeIdx+2+k, total, c.Title)
		}
		sb.WriteString("\n")
	}

	fmt.Fprintf(&sb, "=== Mandatory Execution Contract ===\n"+
		"1. Execute chunk instructions directly in this turn.\n"+
		"2. Submit via `/share/aerial/scripts/aerial-pr.sh submit \"$SCRATCH_DIR\" \"<commit_msg>\" --vanadis-chunk %s`.\n", active.ID)

	return sb.String(), nil
}
