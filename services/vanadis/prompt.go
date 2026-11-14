package main

import (
	"errors"
	"fmt"
	"strings"
)

// FormatChunkPrompt renders a structured, context-rich prompt for executing a specific Vanadis chunk.
func FormatChunkPrompt(build VanadisBuild, chunks []VanadisChunk, activeIdx int) (string, error) {
	return formatChunkPromptWithShareRoot(build, chunks, activeIdx, defaultShareRoot)
}

func formatPromptCompletedLedger(sb *strings.Builder, completed []VanadisChunk, total int) {
	sb.WriteString("=== Completed Chunks Ledger ===\n")
	if len(completed) == 0 {
		sb.WriteString("(none — this is the first chunk)\n\n")
		return
	}
	for k, c := range completed {
		if c.Mode == "action" && (c.Status == "completed" || c.Status == "merged") {
			fmt.Fprintf(sb, "- [Chunk %d/%d] %s (action, completed)\n", k+1, total, c.Title)
		} else {
			fmt.Fprintf(sb, "- [Chunk %d/%d] %s (%s, PR #%d)\n", k+1, total, c.Title, firstNonEmpty(normalizeRepo(c.Repo), normalizeRepo("aerial")), c.PRNumber)
		}
	}
	sb.WriteString("\n")
}

func formatPromptActiveChunk(sb *strings.Builder, active VanadisChunk, activeIdx, total int, activeRepo, shareRoot string) {
	if active.Mode == "action" {
		fmt.Fprintf(sb, "=== Active Chunk (Chunk %d of %d) ===\nChunk ID: %s\nTitle: %s\nChunk Mode: action\nInstructions:\n%s\n\n", activeIdx+1, total, active.ID, active.Title, active.Instructions)
		return
	}
	targetFilesStr := firstNonEmpty(strings.Join(active.TargetFiles, ", "), "(none specified)")
	fmt.Fprintf(sb, "=== Active Chunk (Chunk %d of %d) ===\nChunk ID: %s\nTitle: %s\nRepository: %s\nTarget Files: %s\n", activeIdx+1, total, active.ID, active.Title, activeRepo, targetFilesStr)
	if len(active.TargetSymbols) > 0 {
		fmt.Fprintf(sb, "Target Symbols: %s\n", strings.Join(active.TargetSymbols, ", "))
	}
	spanLines, _ := ResolveSymbolSpans(shareRoot, activeRepo, active.TargetFiles, active.TargetSymbols)
	if len(spanLines) > 0 {
		fmt.Fprintf(sb, "Target Symbol Spans:\n%s\n", strings.Join(spanLines, "\n"))
	}
	fmt.Fprintf(sb, "Instructions:\n%s\n\n", active.Instructions)
}

func formatPromptRemainingChunks(sb *strings.Builder, remaining []VanadisChunk, activeIdx, total int) {
	sb.WriteString("=== Remaining Chunks ===\n")
	if len(remaining) == 0 {
		sb.WriteString("(none — this is the final chunk)\n\n")
		return
	}
	sb.WriteString("DO NOT implement these remaining chunks in this turn—they are listed strictly for architectural boundary awareness.\n")
	for k, c := range remaining {
		if c.Mode == "action" {
			fmt.Fprintf(sb, "- [Chunk %d/%d] %s (action)\n", activeIdx+2+k, total, c.Title)
		} else {
			fmt.Fprintf(sb, "- [Chunk %d/%d] %s (%s)\n", activeIdx+2+k, total, c.Title, firstNonEmpty(normalizeRepo(c.Repo), normalizeRepo("aerial")))
		}
	}
	sb.WriteString("\n")
}

func formatPromptContract(sb *strings.Builder, active VanadisChunk, activeRepo, shareRoot string) {
	if active.Mode == "action" {
		fmt.Fprintf(sb, "=== Action Chunk Execution Contract ===\n1. Execute the operational instructions directly in this turn, adhering to the Build Objective and Shared Build Context.\n2. Iterate and diagnose until all acceptance criteria are verified.\n3. NEVER spawn or invoke subagents (`invoke_subagent`, `define_subagent`): Vanadis build chunks are already isolated leaf tasks—execute all operational steps directly in this turn.\n4. End turn normally on success: the eri harness automatically marks the action chunk completed and advances the build.\n5. Ending the turn is treated as successful completion: NEVER end it while work is still pending in the background (downloads, installs, builds, deploys, or waits)—run long operations in the foreground with a long timeout and confirm the outcome before ending the turn.\n6. If an unrecoverable blocker prevents completion, call the vanadis MCP tool fail_chunk with chunk_id %q and the failure reason, and end the turn.\n7. File any out-of-scope bugs, tech debt, or follow-up tasks discovered during execution via the docket MCP create tool (referencing the active Build ID and Chunk ID %q in the task description) rather than expanding the active chunk's scope.\n", active.ID, active.ID)
		return
	}
	fmt.Fprintf(sb, "=== Mandatory Execution Contract ===\n1. Initialize a fresh scratch workspace off `origin/main`: `/share/aerial/scripts/aerial-pr.sh init %s`\n2. Implement ONLY this chunk following TDD, optimizing all design and implementation choices for the `Build Objective` and `Shared Build Context` so the change achieves the objective in substance and integrates cleanly and idiomatically into the existing architecture without superficial workarounds, dead scaffolding, or bolted-on catch-all blocks, run package tests and `./scripts/verify.sh --staged` (if coverage is below 95%%, run `./scripts/check-coverage.sh --service <svc> --check --gaps` in a single pass rather than manual `go test -coverprofile` loops), and write `PR_DESCRIPTION.md`.\n   If any detail in chunk `Instructions` is ambiguous, underspecified, or conflicts with the `Build Objective`, the `Build Objective` takes precedence—adapt the implementation to satisfy the `Build Objective`.\n3. Inspect `Target Files` and `Target Symbols` directly rather than running broad repo-wide searches: resolve live symbol line offsets first and `view_file` ONLY those targeted `StartLine`–`EndLine` slices instead of reading entire files from line 1.\n", activeRepo)
	_, allResolved := ResolveSymbolSpans(shareRoot, activeRepo, active.TargetFiles, active.TargetSymbols)
	if !allResolved {
		if grepCmd := formatSymbolGrepCommand(active.TargetFiles, active.TargetSymbols); grepCmd != "" {
			fmt.Fprintf(sb, "   Run after init: `%s`\n", grepCmd)
		}
	}
	fmt.Fprintf(sb, "4. NEVER spawn or invoke subagents (`invoke_subagent`, `define_subagent`): Vanadis build chunks are already isolated leaf tasks—execute all file edits, tests, and verification directly in this turn.\n5. File any out-of-scope bugs, tech debt, or follow-up tasks discovered during execution via the docket MCP create tool (referencing the active Build ID and Chunk ID %q in the task description) rather than expanding the active PR's scope.\n6. Submit via `/share/aerial/scripts/aerial-pr.sh submit \"$SCRATCH_DIR\" \"<commit_msg>\" --vanadis-chunk %s`\n7. The turn may ONLY end after `aerial-pr.sh submit` has succeeded or the vanadis MCP tool fail_chunk has been called with chunk_id %q. Run long downloads, installs (e.g. Playwright/Chromium), builds, and tests in the foreground with a long timeout—never background them and end the turn \"waiting for X\". If a dependency genuinely cannot be installed, either call fail_chunk with the reason, or skip only that verification step, state it in `PR_DESCRIPTION.md`, and submit.\n8. Terminate turn immediately after submission.\n", active.ID, active.ID, active.ID)
}

func formatChunkPromptWithShareRoot(build VanadisBuild, chunks []VanadisChunk, activeIdx int, shareRoot string) (string, error) {
	if len(chunks) == 0 {
		return "", errors.New("chunks slice cannot be empty")
	}
	if activeIdx < 0 || activeIdx >= len(chunks) {
		return "", fmt.Errorf("active chunk index %d out of range [0, %d)", activeIdx, len(chunks))
	}
	total := len(chunks)
	active := chunks[activeIdx]
	isActionChunk := active.Mode == "action"
	activeRepo := ""
	if !isActionChunk {
		activeRepo = firstNonEmpty(normalizeRepo(active.Repo), normalizeRepo("aerial"))
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "[VANADIS BUILD CHUNK EXECUTION]\nBuild ID: %s\n", build.ID)
	if bTitle := strings.TrimSpace(build.Title); bTitle != "" {
		fmt.Fprintf(&sb, "Build Title: %s\n", bTitle)
	}
	if !isActionChunk {
		fmt.Fprintf(&sb, "Repository: %s\n", activeRepo)
	}
	fmt.Fprintf(&sb, "Tier: %d\n\n=== Build Objective ===\n%s\n\n", build.Tier, strings.TrimSpace(build.Objective))
	if sharedCtx := strings.TrimSpace(build.SharedContext); sharedCtx != "" {
		fmt.Fprintf(&sb, "=== Shared Build Context ===\n%s\n\n", sharedCtx)
	}
	formatPromptCompletedLedger(&sb, chunks[:activeIdx], total)
	formatPromptActiveChunk(&sb, active, activeIdx, total, activeRepo, shareRoot)
	formatPromptRemainingChunks(&sb, chunks[activeIdx+1:], activeIdx, total)
	formatPromptContract(&sb, active, activeRepo, shareRoot)
	return sb.String(), nil
}

func formatSymbolGrepCommand(targetFiles, targetSymbols []string) string {
	var syms, files []string
	for _, s := range targetSymbols {
		if s = strings.TrimSpace(s); s != "" {
			syms = append(syms, s)
		}
	}
	for _, f := range targetFiles {
		if f = strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(f), "./"), "/"); f != "" {
			files = append(files, fmt.Sprintf(`"$SCRATCH_DIR/%s"`, f))
		}
	}
	if len(syms) == 0 || len(files) == 0 {
		return ""
	}
	return fmt.Sprintf("grep -n -E %q %s", strings.Join(syms, "|"), strings.Join(files, " "))
}
