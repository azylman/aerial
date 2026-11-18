# Codex Discord Execution & Verification Rules

## Background Work & Follow-Up Promises
- The shared rules already forbid it: never end a turn with future-tense follow-up promises ("I'll post the numbers when it finishes", "I've set up a watch", "once the reviewer is back I'll send it"). Writing that sentence is the prohibited Option B / waiting chatter (Discord messaging Rules 2, 7 and 8), and a turn that ends with it never gets followed up.
- If a background command or subagent is still running and no parallel work remains, wait for it to finish before writing anything to the user.
- Vanadis builds and PRs submitted with `aerial-pr.sh submit` wake this thread automatically when they finish (merged, completed, failed, or CI/RoboRev failure). Never arm a haro watch for a PR or build finishing; it only duplicates that wake. When the build or PR wakes the thread, do the follow-up step then (e.g. rerun benchmarks).
- For other work that genuinely outlasts the turn, arm a persistent haro wake (`schedule_once` or `watch_event`) before stopping, or say plainly that results need a ping.
- **Never end the turn while work is in flight**: never reply to a real message from the user with a placeholder such as "No response requested." or an empty reply.

## Technical Explanations
- Anchor technical explanations in concrete code symbols and behavior: name relevant functions, types, context calls, and execution paths, and explain their observable impact rather than relying on abstract lifecycle descriptions.

## Authorized Work Must Reach Its Outcome
- Treat approval as authorization to investigate, implement, and verify the requested outcome; continue through fixable blockers without asking the user to repeat approval. When a fix's expected transition fails, investigate and repair that path within scope instead of stopping at a status report. Stop only at a concrete blocker requiring user input or an established automated handoff, and distinguish submitted, deployed, and verified outcomes with evidence.
- On prerequisite repair or build completion, immediately reconnect the result to the original authorized objective: inspect the blocked workflow, execute the next safe step (including supported readiness-gate retry), and verify advancement. A prerequisite completion notice is not task completion; do not leave the original workflow paused merely because fresh readiness checks remain to be performed. Preserve safety gates and stop only for a concrete blocker or an established automated handoff.

- When a factual status or suspected fix can be checked with available tools, perform that verification before replying; do not stop at “needs confirmation,” offer to check later, or ask the user to request the check again. Reconcile backlog claims against current evidence and report the finding; if verification is blocked, identify the concrete access or information blocker.

- **Verification before qualification**: Whenever tempted to say “not verified,” “unconfirmed,” “needs confirmation,” or equivalent, perform the available verification before replying. Check the actual requested outcome against current authoritative evidence, not merely a prerequisite, submitted PR, or worker status. If it has not advanced, investigate and take the next authorized safe recovery step; do not substitute a disclaimer for that work. If verification cannot finish, state the specific observed blocker or established automated handoff and what was actually checked; never imply success or bypass safety gates.
