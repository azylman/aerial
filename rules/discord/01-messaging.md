# Discord Messaging, Tool Execution & Token Conservation Invariants

1. **Discord Output Delivery & Substantive Output Invariant**:
   - Deliver responses via Markdown directly in Discord at the end of the turn (the user only receives the final substantive result).
   - Completed agent turns must always produce substantive output; unrecovered empty stdout on a completed turn (when no background command or subagent is in flight) is treated as a failure/retry. When suspending mid-workflow for a running background task or subagent per Rules 4 and 8, stopping tool calls with empty text is required and suspends the turn in place rather than completing it. Under NO circumstance should `[NO_REPLY]` or dummy sentinel strings be emitted or instructed.

2. **Silent Multi-Step Execution (No Intermediate Waiting Chatter)**:
   - When executing multi-step tool calls, commands, or background tasks, NEVER emit intermediate play-by-play status chatter ("I have initiated a search...", "I will review results when the task finishes...", "waiting for X...").
   - Execute all intermediate steps completely silently and deliver strictly the final substantive answer or deliverable once work is complete.

3. **Parallel Tool Batching Invariant**:
   - When reading, inspecting, or searching files across packages, ALWAYS emit all `read_file`, `view_file`, `grep_search`, and read tool calls in parallel within a single turn rather than serializing them one file at a time. Never execute sequential single-file read or search loops when target files, directories, or symbols are known.

4. **Zero-Polling & Reactive Subagent/Task Wakeup Policy**:
   - NEVER enter manual status polling loops on background tasks (`manage_task status`), active subagents (`manage_subagents list` or reading a running subagent's `transcript.jsonl` / `logAbsoluteUri`), or repeatedly set short `schedule` timers.
   - When an in-flight subagent (`invoke_subagent`) or background task is running and no parallel work remains, simply **stop calling tools and emit empty text**—`agy` natively suspends the turn in place and wakes you reactively via `<SYSTEM_MESSAGE>` when the subagent or task completes.

5. **Subagent Offloading Threshold & Tool Budget Ceiling**:
   - When a task involves deep exploration (>10 files viewed), repetitive test/coverage cycles, or approaches 30 internal tool steps without completing, stop and delegate the work to an isolated subagent (`invoke_subagent`). Subagents execute in fresh, minimal context windows and return only their final findings.

6. **GitHub Web Links Only (No `file:///` Links)**:
   - Link files exclusively via public GitHub URLs (e.g. `https://github.com/azylman/aerial/blob/main/...`) or clean inline backticks (e.g. `GEMINI.md`). NEVER emit `file://` or `file:///` URIs or masked local file links, as local paths are inaccessible in Discord. Masked links (`[label](url)`) are strictly restricted to valid `http://` or `https://` URLs.

7. **Action-Output Synchronicity Invariant (No False Starts / Future-Tense Promises)**:
   - NEVER end a turn stating in prose that you are "starting", "running", or "kicking off" a task right now (e.g. *"Kicking off Task 1 right now!"*). In a turn-based system, final text is delivered to Discord only after your execution process has completely stopped.
   - If work remains to be done, **do not speak**—execute the tool calls, write code, run tests, or spawn subagents silently.
   - If you are stopping to deliver a message, speak strictly in the **past tense** about what has already been executed or committed, and explicitly state that you are **paused waiting for the user's review or next directive**.

8. **Background Command & Subagent Yield Invariant (Option B Prompt Rejection)**:
   - When any command backgrounds or a subagent is launched, NEVER take CLI harness Option B ("update the user with a short message that you have launched the command and will wait for it to finish"). Proceed silently with parallel work (Option A); once parallel work is finished, **stop calling tools and emit no conversational text** so `agy` suspends until the reactive `<SYSTEM_MESSAGE>` completion wakeup arrives.

9. **No Markdown Tables**:
   - NEVER format responses using Markdown tables as Discord does not support table rendering.

10. **Discord Message Length, Proportional Brevity & Zero Tool-Receipt Invariant**:
    - Discord enforces a 2,000-character limit per message. Deliver responses strictly within single-message bounds (< 1,800 characters), leading naturally with the key conclusion or answer first. Never output literal acronym tags or prefixes like 'BLUF:' or 'Bottom Line:' in user-facing responses.
    - **Proportional Brevity & Zero Tool-Receipt Echoing**: Scale response length to the turn. When executing a routine action or follow-up that was just discussed in-thread (e.g. scheduling a reminder, toggling a setting, or shipping an agreed tweak), confirm in a single short sentence. Never echo internal tool-receipt fields (UUIDs, schedule IDs, exact clock timestamps on relative timers, or restatements of prompts/plans already established in the thread) unless explicitly requested or debugging a failure.
