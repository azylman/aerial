// Package prompt builds worker/resume/conflict/deploy-failure prompts and scans
// local /share repository checkouts for AST symbol spans.
package prompt

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/strutil"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// Action vocabulary stays local; pure repo/branch helpers delegate to the
// canonical internal/model copies.
const chunkModeAction = "action"

var declKeywordPrefixes = []string{"func ", "type ", "var ", "const ", "export ", "function ", "class "}

func hasCoalescedRoboRevCheck(checkName string) bool {
	if prwire.IsRoboRevCheck(checkName) {
		return false
	}
	for part := range strings.SplitSeq(checkName, ",") {
		if prwire.IsRoboRevCheck(strings.TrimSpace(part)) {
			return true
		}
	}
	return false
}

type ciPromptMeta struct {
	shortRepo string
	branchArg string
	extraLine string
}

func parseCIPromptMeta(ev pubsub.PRLifecycleEvent) ciPromptMeta {
	cleanBranch := strings.TrimPrefix(strings.TrimSpace(ev.Branch), "refs/heads/")
	var extraParts []string
	if cleanBranch != "" {
		extraParts = append(extraParts, "Branch: "+cleanBranch)
	}
	if cleanURL := strings.TrimSpace(ev.CheckURL); cleanURL != "" {
		extraParts = append(extraParts, "Check URL: "+cleanURL)
	}
	return ciPromptMeta{
		shortRepo: strutil.FirstNonEmpty(model.CleanRepoName(ev.Repo), "aerial"),
		branchArg: strutil.FirstNonEmpty(cleanBranch, "<branch>"),
		extraLine: strings.Join(extraParts, "\n"),
	}
}

// FormatCIFailurePrompt formats the failure prompt when a CI check fails.
func FormatCIFailurePrompt(ev pubsub.PRLifecycleEvent, details, chunkID, dispatchID string) string {
	failChunk := fmt.Sprintf("call fail_chunk(chunk_id=%s, dispatch_id=%s, reason=\"BLOCKED: <reason and the decision needed>\") and then end the turn", chunkID, dispatchID)
	meta := parseCIPromptMeta(ev)
	checkoutAndSubmit := fmt.Sprintf("Check out the existing PR branch via '/share/aerial/scripts/aerial-pr.sh init %s --branch %s' and push updates via '/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> \"<commit_msg>\"' (PR_DESCRIPTION.md is not required when updating an existing PR; never run bare 'git push').", meta.shortRepo, meta.branchArg)

	var directive string
	switch {
	case prwire.IsRoboRevCheck(ev.CheckName):
		directive = "[VANADIS CI REMEDIATION — MANDATORY]\n" +
			fmt.Sprintf("Please critically evaluate each RoboRev finding against codebase reality and architectural intent. This turn must end in exactly one machine-visible outcome: (1) a verified fix pushed to the existing PR branch, (2) a technical rebuttal recorded via '/share/aerial/scripts/aerial-pr.sh --repo %s override-roborev %d '<technical_rebuttal>'', or (3) when genuinely blocked, %s. %s\n", meta.shortRepo, ev.PRNumber, failChunk, checkoutAndSubmit) +
			"Ending this turn with none of these outcomes pauses the chunk for human review automatically and is a failed remediation, not a completed one. A recommendation, waiver request, or \"no legitimate fix\" conclusion written only in your final response is invisible to the thread and counts as no outcome.\n" +
			"If the correct fix conflicts with the chunk's stated boundaries, do not silently stop: apply the smallest boundary-crossing fix that makes the gate pass, note the scope change in the commit and PR description, and let the next chunk absorb it."
	case hasCoalescedRoboRevCheck(ev.CheckName):
		directive = "[VANADIS CI REMEDIATION — MANDATORY]\n" +
			fmt.Sprintf("Both deterministic CI jobs and RoboRev code review failed. Investigate the failing logs, diagnose the root cause, verify locally, and critically evaluate each RoboRev finding against codebase reality. This turn must end in exactly one machine-visible outcome: (1) a verified fix pushed to the existing PR branch fixing all deterministic CI failures and any valid RoboRev findings in the same commit (or recording a technical rebuttal via '/share/aerial/scripts/aerial-pr.sh --repo %s override-roborev %d '<technical_rebuttal>'' before pushing when a RoboRev finding is a false positive), or (2) when genuinely blocked, %s. %s\n", meta.shortRepo, ev.PRNumber, failChunk, checkoutAndSubmit) +
			"If the details above lack the failing log, list the run's jobs via curl http://127.0.0.1:4028/internal/github-api/repos/<owner>/<repo>/actions/runs/<run_id>/jobs and fetch failed job logs from .../actions/jobs/<job_id>/logs (gh is not installed; github.com job pages need sign-in).\n" +
			"Ending this turn with none of these outcomes pauses the chunk for human review automatically and is a failed remediation, not a completed one. A recommendation, waiver request, or \"no legitimate fix\" conclusion written only in your final response is invisible to the thread and counts as no outcome.\n" +
			"If the correct fix conflicts with the chunk's stated boundaries, do not silently stop: apply the smallest boundary-crossing fix that makes the gate pass, note the scope change in the commit and PR description, and let the next chunk absorb it."
	default:
		directive = "[VANADIS CI REMEDIATION — MANDATORY]\n" +
			"Investigate the failing logs, diagnose the root cause, verify locally, and remediate this turn. This turn must end in exactly one machine-visible outcome: (1) a fix pushed to the existing PR branch, or (2) when genuinely blocked, " + failChunk + ". " + checkoutAndSubmit + "\n" +
			"If the details above lack the failing log, list the run's jobs via curl http://127.0.0.1:4028/internal/github-api/repos/<owner>/<repo>/actions/runs/<run_id>/jobs and fetch failed job logs from .../actions/jobs/<job_id>/logs (gh is not installed; github.com job pages need sign-in).\n" +
			"Ending this turn with none of these outcomes pauses the chunk for human review automatically and is a failed remediation, not a completed one. A recommendation, waiver request, or \"no legitimate fix\" conclusion written only in your final response is invisible to the thread and counts as no outcome.\n" +
			"If the correct fix conflicts with the chunk's stated boundaries, do not silently stop: apply the smallest boundary-crossing fix that makes the gate pass, note the scope change in the commit and PR description, and let the next chunk absorb it."
	}
	return formatErrorDetailsBlock(
		fmt.Sprintf("CI check %q failed for PR #%d on %s (head commit: %s).", ev.CheckName, ev.PRNumber, ev.Repo, ev.HeadSHA),
		details, meta.extraLine,
		directive,
	)
}

func buildInlineCapDirective(ev pubsub.PRLifecycleEvent, shortRepo, branchArg string, maxAttempts int) string {
	switch {
	case prwire.IsRoboRevCheck(ev.CheckName):
		return "[VANADIS INLINE THREAD CI REMEDIATION — MANDATORY]\n" +
			fmt.Sprintf("Worker CI remediation cap (%d/%d) reached. As the source-thread agent running in-line on the thread's high-effort model with full thread context, please remediate in this turn by critically evaluating each RoboRev finding against codebase reality and full thread context and either (1) checking out '/share/aerial/scripts/aerial-pr.sh init %s --branch %s' and pushing a verified fix via '/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> \"<commit_msg>\"', or (2) recording a commit-SHA-bound technical rebuttal via '/share/aerial/scripts/aerial-pr.sh --repo %s override-roborev %d '<technical_rebuttal>''. Note that either action automatically recovers the paused chunk to pr_opened and resumes the build.", maxAttempts, maxAttempts, shortRepo, branchArg, shortRepo, ev.PRNumber)
	case hasCoalescedRoboRevCheck(ev.CheckName):
		return "[VANADIS INLINE THREAD CI REMEDIATION — MANDATORY]\n" +
			fmt.Sprintf("Worker CI remediation cap (%d/%d) reached. As the source-thread agent running in-line on the thread's high-effort model with full thread context, please remediate in this turn by fixing all deterministic CI failures and critically evaluating each RoboRev finding against codebase reality: check out '/share/aerial/scripts/aerial-pr.sh init %s --branch %s', fix deterministic CI failures and any valid RoboRev findings in the code (recording '/share/aerial/scripts/aerial-pr.sh --repo %s override-roborev %d '<technical_rebuttal>'' before pushing if any finding is a false positive), and push a verified fix via '/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> \"<commit_msg>\"'. Either action automatically recovers the paused chunk to pr_opened and resumes the build.", maxAttempts, maxAttempts, shortRepo, branchArg, shortRepo, ev.PRNumber)
	default:
		return "[VANADIS INLINE THREAD CI REMEDIATION — MANDATORY]\n" +
			fmt.Sprintf("Worker CI remediation cap (%d/%d) reached. As the source-thread agent running in-line on the thread's high-effort model with full thread context, please remediate in this turn by checking out '/share/aerial/scripts/aerial-pr.sh init %s --branch %s', diagnosing the root cause, verifying locally, and pushing a verified fix via '/share/aerial/scripts/aerial-pr.sh submit <scratch_dir> \"<commit_msg>\"'. Either action automatically recovers the paused chunk to pr_opened and resumes the build.", maxAttempts, maxAttempts, shortRepo, branchArg)
	}
}

// FormatInlineCapRemediationPrompt formats the prompt sent to the build's source thread
// when worker CI remediation attempts are exhausted.
func FormatInlineCapRemediationPrompt(ev pubsub.PRLifecycleEvent, details, chunkID string, failCount, maxAttempts int) string {
	meta := parseCIPromptMeta(ev)
	header := fmt.Sprintf("CI check %q failed %d times for PR #%d on %s (chunk: %s, head commit: %s; worker remediation cap %d/%d reached).", ev.CheckName, failCount, ev.PRNumber, ev.Repo, chunkID, ev.HeadSHA, maxAttempts, maxAttempts)
	directive := buildInlineCapDirective(ev, meta.shortRepo, meta.branchArg, maxAttempts)
	return formatErrorDetailsBlock(header, details, meta.extraLine, directive)
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

// actionVerdictContract states how an action chunk finishes. Both the launch
// prompt and every automatic resume carry it so a chunk launched under an older
// prompt learns complete_chunk on its first resume.
func actionVerdictContract(chunkID, dispatchID string, maxResumes int) string {
	return fmt.Sprintf(`=== Action Chunk Verdict Contract ===
- This chunk completes ONLY when you call complete_chunk(chunk_id=%q, dispatch_id=%q, summary="<PASS evidence>"). Ending a turn without it never completes the chunk: Vanadis resumes it automatically (up to %d times) and then fails it as RESUMABLE.
- Turns stop at about 1h. Wait at most 45 min per turn on anything (deploys, rollouts, backfills). If work remains, write a checkpoint (what is done, what is next) to /tmp/vanadis-%s/checkpoint.md, summarize it in your final message and end the turn; the next automatic resume continues from that checkpoint.
- A repair PR (/share/aerial/scripts/aerial-pr.sh submit "$SCRATCH_DIR" "<commit_msg>" --vanadis-chunk %s) is a step, not the verdict: after it deploys, re-verify live and then call complete_chunk.
- For blockers outside this chunk's control, call fail_chunk(chunk_id=%q, dispatch_id=%q, reason="RESUMABLE: <blocker>"). Fix forward on test, lint or CI failures instead.
`, chunkID, dispatchID, maxResumes, chunkID, chunkID, chunkID, dispatchID)
}

// FormatActionResumePrompt builds the self-contained prompt for automatic
// resume k of an action chunk whose previous turn ended without a verdict. It
// re-includes the full chunk prompt because sessions rotate.
func FormatActionResumePrompt(chunk model.VanadisChunk, chunkPrompt string, k, maxResumes int) string {
	chunkID := strings.TrimSpace(chunk.ID)
	chunk.ID = chunkID
	dispatchID := store.ActionResumeDispatchID(chunk, k)
	return fmt.Sprintf(`[VANADIS AUTOMATIC RESUME %d/%d]
Your previous turn for action chunk %s ended without calling complete_chunk. Dispatch ID for this turn: %s (use it, not the one in the original prompt, for complete_chunk and fail_chunk).

Continue from your checkpoint (/tmp/vanadis-%s/checkpoint.md and your previous final message, if present); do not restart work that is already done and verified.

%s
The original chunk instructions follow.

%s`, k, maxResumes, chunkID, dispatchID, chunkID, actionVerdictContract(chunkID, dispatchID, maxResumes), chunkPrompt)
}

// FormatConflictPrompt formats the conflict prompt when merge conflict is detected.
func FormatConflictPrompt(ev pubsub.PRLifecycleEvent) string {
	shortRepo := strutil.FirstNonEmpty(model.CleanRepoName(ev.Repo), "aerial")
	cleanBranch := strutil.FirstNonEmpty(strings.TrimPrefix(strings.TrimSpace(ev.Branch), "refs/heads/"), "<branch>")
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
	commit := strutil.FirstNonEmpty(ev.MergeSHA, ev.HeadSHA)
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
	return prwire.FormatErrorDetailsBlock(header, details, extraLine, footer)
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
		if slices.ContainsFunc(declKeywordPrefixes, func(p string) bool { return strings.HasPrefix(clean, p) }) && containsIdent(clean, sym) {
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
		activeRepo = strutil.FirstNonEmpty(model.CleanRepoName(active.Repo), "aerial")
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

	targetFilesStr := strutil.FirstNonEmpty(strings.Join(active.TargetFiles, ", "), "(none specified)")
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

	if isActionChunk {
		sb.WriteString(actionVerdictContract(active.ID, model.ActionDispatchID(active), store.MaxActionResumes))
		fmt.Fprintf(&sb, "\n=== Mandatory Execution Contract ===\n"+
			"1. Execute chunk instructions directly in this turn.\n"+
			"2. Finish only with complete_chunk(chunk_id=%q, dispatch_id=%q, summary=\"<PASS evidence>\").\n", active.ID, model.ActionDispatchID(active))
		return sb.String(), nil
	}
	fmt.Fprintf(&sb, "=== Mandatory Execution Contract ===\n"+
		"1. Execute chunk instructions directly in this turn.\n"+
		"2. Submit via `/share/aerial/scripts/aerial-pr.sh submit \"$SCRATCH_DIR\" \"<commit_msg>\" --vanadis-chunk %s`.\n", active.ID)

	return sb.String(), nil
}
