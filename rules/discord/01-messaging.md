# Discord Messaging, Tool Execution & Token Conservation Invariants

1. **Discord Output Delivery & Substantive Output Invariant**:
   - Deliver responses via Markdown directly in Discord at the end of the turn (the user only receives the final substantive result).
   - Active agent turns must always produce substantive output; unrecovered empty stdout or missing response is treated as a failure/retry. Under NO circumstance should `[NO_REPLY]` or dummy sentinel strings be emitted or instructed.

2. **Silent Multi-Step Execution (No Intermediate Waiting Chatter)**:
   - When executing multi-step tool calls, commands, or background tasks, NEVER emit intermediate play-by-play status chatter ("I have initiated a search...", "I will review results when the task finishes...", "waiting for X...").
   - In `agy` print mode (`-p`), emitting conversational text without tool calls signals to `agy` that the turn is complete, triggering print-mode drain and killing active background tasks or subagents. Execute all intermediate steps completely silently and deliver strictly the final substantive answer or deliverable.

3. **Parallel Tool Batching Invariant**:
   - When reading, inspecting, or searching files across packages, ALWAYS emit all `view_file`, `grep_search`, and read tool calls in parallel within a single turn rather than serializing them one file at a time. Never execute sequential single-file read or search loops when target files, directories, or symbols are known.

4. **Coarse-Grained Code Editing Invariant**:
   - Avoid iterative micro-chunk editing loops. Formulate the complete target diff for each file and apply it in a single comprehensive `replace_file_content` or `write_to_file` invocation per file instead of making multiple small 3–5 line edits. Never interleave edits with redundant read-verify loops on the same file.

5. **Zero-Polling Policy**:
   - NEVER enter manual status polling loops on background tasks (`manage_task status`) or repeatedly set short `schedule` timers. Run background tasks reactively and wait for completion notifications to prevent context inflation.

6. **Subagent Offloading Threshold & Tool Budget Ceiling**:
   - When a task involves deep exploration (>10 files viewed), repetitive test/coverage cycles, or approaches 30 internal tool steps without completing, stop and delegate the work to an isolated subagent (`invoke_subagent`). Subagents execute in fresh, minimal context windows and return only their final findings.
   - **Model Tier Selection on Offload**: When offloading work to an isolated subagent (`invoke_subagent`), explicitly declare `Model: "flash"` (or `"flash_lite"`) for research lookups, broad file inspections, test executions, and repetitive remediation. Reserve `pro` exclusively for deep architectural planning or formal multi-perspective review.

7. **GitHub Web Links Only (No `file:///` Links)**:
   - Link files exclusively via public GitHub URLs (e.g. `https://github.com/azylman/aerial/blob/main/...`) or clean inline backticks (e.g. `GEMINI.md`). NEVER emit `file://` or `file:///` URIs or masked local file links, as local paths are inaccessible in Discord. Masked links (`[label](url)`) are strictly restricted to valid `http://` or `https://` URLs.

8. **Action-Output Synchronicity Invariant (No False Starts / Future-Tense Promises)**:
   - NEVER end a turn stating in prose that you are "starting", "running", or "kicking off" a task right now (e.g. *"Kicking off Task 1 right now!"*). In a turn-based system, final text is delivered to Discord only after your execution process has completely stopped.
   - If work remains to be done, **do not speak**—execute the tool calls, write code, run tests, or spawn subagents silently.
   - If you are stopping to deliver a message, speak strictly in the **past tense** about what has already been executed or committed, and explicitly state that you are **paused waiting for the user's review or next directive**.

9. **Background Command & Tool Yield Trap (Option B Prompt Rejection)**:
   - When any command backgrounds, NEVER take CLI harness Option B ("update the user with a short message that you have launched the command and will wait for it to finish"). In headless execution (`agy -p`), emitting conversational text without tool calls signals turn completion, triggering print-mode drain and terminating background tasks. Proceed silently with parallel work (Option A) or wait synchronously via an active tool step; never emit end-of-turn waiting prose.

10. **No Markdown Tables**:
    - NEVER format responses using Markdown tables as Discord does not support table rendering.

11. **Discord Message Length & Verbosity Ceiling**:
    - Discord enforces a 2,000-character limit per message. Deliver responses strictly within single-message bounds (< 1,800 characters), leading naturally with the key conclusion or answer first. Never output literal acronym tags or prefixes like 'BLUF:' or 'Bottom Line:' in user-facing responses.
