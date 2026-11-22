# Vanadis Multi-Chunk Build Authoring Runbook (`scripts/aerial-build.sh enqueue <plan.json>`)

Whenever a change requires **≥ 2 PRs**—whether across multiple repositories (`azylman/aerial`, `azylman/aerial-config`, `azylman/mirrormere`, `azylman/aerial-sidecars`), Two-Step Feature Rule rollouts, or sequential multi-chunk refactors in a single repository—**always default to a single declarative Vanadis build** enqueued via `/share/aerial/scripts/aerial-build.sh enqueue <plan.json>`. Never propose or execute manual turn-by-turn *"Step 1 PR now, wait for deploy, then Step 2 PR"* loops.

## 1. Architect/Planner Up-Front Inspection, Cross-Repo Sequencing & Scope Inventory
- **Removal of Build-Level `"repo"` & Cross-Repo Ordering**: Vanadis builds do NOT define a build-level `"repo"`. Repository routing is determined strictly per-chunk on `'pr'` chunks (`"azylman/mirrormere"`, `"azylman/aerial-sidecars"`, `"azylman/aerial"`, `"azylman/aerial-config"`). Order chunks strictly by deployment dependency (e.g., upstream service endpoints in Chunks 1–2 -> shared package / collector changes in Chunk 3 -> `aerial-config` job specs in Chunk 4). Vanadis and `synapse` track all repositories on a single Discord status widget, suppress intermediate deployment wakes, and advance chunks automatically through final rollout.
- **DAG Orchestration (`depends_on`) & Two-Milestone Unblocking Rules**:
  - **Mandatory Explicit `depends_on` & Parallel-First DAG Decomposition**: Implicit sequential fallback (`applySequentialDefaults`) is removed. Vanadis strictly rejects any build or appended chunk that omits `depends_on` (`400 Bad Request` / `ErrInvalidBuildInput`). Every chunk MUST explicitly declare `"depends_on": []` (for parallel root chunks) or `"depends_on": ["<upstream-id>"]` (only when a true compile, schema, or live-runtime prerequisite exists).
    Parallel-First DAG authoring optimizes throughput across workers (`maxParallelChunks = 4`). Whenever chunks do not share a hard dependency, decompose them as parallel branches. The 3 canonical parallel DAG shapes are:
    1. **Cross-Repo Root Fan-Out**: Chunks targeting separate repositories (`azylman/aerial`, `azylman/aerial-sidecars`, `azylman/mirrormere`) start concurrently with `"depends_on": []`, joining downstream at a readiness or verification gate chunk (`mode: "action"` or final config PR).
    2. **Hub-and-Spoke Service Fan-Out**: A shared upstream contract/schema chunk finishes first, immediately fanning out to parallel consumer/service PR chunks with disjoint `target_files` (`depends_on: ["shared-contract-chunk"]`).
    3. **Disjoint Same-Repo Bugfix Fan-Out**: Multiple independent fixes or feature additions within the same repository run concurrently with `"depends_on": []`, declaring strictly non-overlapping, disjoint `target_files`.
  - **Static DAG Validation Invariants**: Kahn's topological sort rejects unknown chunk references, self-dependencies, and cycles with `400 Bad Request` (`ErrInvalidBuildInput`). Any two concurrent `"mode": "pr"` chunks targeting the same `repo` (where neither is an ancestor of the other in `depends_on`) MUST declare non-empty, disjoint `target_files` so parallel PR branches never collide or produce merge conflicts.
  - **Two-Milestone Unblocking Semantics**:
    - `ReadyForCode` (`merged`): Satisfied when an upstream `"mode": "pr"` chunk merges (`aerial.pr.merged`), or when an upstream `"mode": "action"` chunk completes through its worker's explicit verdict (not from a dispatch outcome event). Unblocks downstream `"mode": "pr"` chunks immediately upon merge without waiting for image rollout or cluster deployment.
    - `ReadyForRuntime` (`deployed` / `completed`): Satisfied when an upstream `"mode": "pr"` chunk completes cluster deployment (`aerial.pr.deploy_success` or no-deploy `aerial.pr.sync_success`), or when an upstream `"mode": "action"` chunk completes through its worker's explicit verdict. Required before unblocking downstream `"mode": "action"` verification chunks and final build completion.
- **4-Phase Cutover & Migration Sequencing Rule**: Whenever a build replaces an execution path, queue consumer, storage table, or service boundary, structure chunks up front into four distinct phases:
  1. **Additive / Dark-Launch (`"mode": "pr"`)**: Deploy the new workflow, consumer, or schema alongside the existing path without switching live entrypoints, verified via hermetic unit/integration tests (`httptest.Server` + in-memory mocks).
     - **Strict Seam Isolation in Phase 1 (Zero Half-Wired Callers)**: Phase 1 dark-launch chunks must register new components (`VirtualObject`, workflow, handler, or schema) and test them directly via hermetic unit tests with **zero changes to existing caller entrypoints**. Never wire a new component into *one* caller entrypoint (e.g. `CreateBuild`) behind an env/config toggle while leaving downstream lifecycle entrypoints (`merged`, `deploy_success`, `action_completed`) on the legacy path for Phase 3—partial caller wiring creates a broken half-migrated lifecycle that triggers `roborev` lifecycle defect failures and forces scope creep. All caller entrypoints must cut over atomically in **Phase 3 — Cutover & Legacy Purge (`mode: "pr"`)**.
  2. **Pre-Cutover Readiness Gate (`"mode": "action"`)**: Verify the dark-launched component live in-cluster using strictly read-only / non-mutating probes (health endpoints, Restate `/deployments` registration, consumer readiness, and `400 Bad Request` validation rejection checks). **Strict Hermeticity Boundary**: Live `"mode": "action"` chunks MUST NEVER inject synthetic builds, fake PR numbers (e.g. `999901`), or synthetic NATS lifecycle events into production `vanadis`, `restate`, `eri`, or Discord threads—all synthetic DAG/workflow state-machine simulation belongs strictly in hermetic Go unit tests.
  3. **Cutover & Legacy Purge (`"mode": "pr"`)**: Switch the live entrypoint and remove legacy code/columns only *after* the pre-cutover readiness `action` chunk succeeds.
  4. **Post-Cutover Boundary Gate (`"mode": "action"`)**: Verify pod readiness, zero error logs, and static boundary invariants after legacy code is removed.
- **In-Flight Control-Plane Continuity Rule**: When a build modifies `vanadis`, `synapse`, `eri`, `haro`, or `nats` (the control plane executing the build itself), require the cutover chunk to preserve a fallback for already-running builds started under the prior state so the active build never strands itself mid-flight.
- **Per-Chunk Mode (`"mode": "pr"` vs. `"mode": "action"`)**:
  - Builds can interleave `'pr'` and `'action'` chunks in any order; each individual chunk has a single immutable mode:
  - `"mode": "pr"` (default when mode is omitted or empty): Code modifications authored in an ephemeral scratch workspace resulting in a git branch, Pull Request, CI checks, and deployment tracking. Requires a non-empty chunk-level `"repo"`, and non-empty `"target_files"` and `"target_symbols"`.
  - `"mode": "action"`: Live operational action chunk executing against cluster services, host processes, or operational APIs in the active environment; scoped repair PRs are supported when verification discovers a defect blocking the assigned objective. At creation, requires empty `"repo"` (`""`), empty `"target_files"` (`[]`), empty `"target_symbols"` (`[]`), and no attached PR. Attaching a repair PR records its repository and PR number without changing the chunk mode.
  - Advancement: a merged PR chunk starts the next chunk on merge only when that next chunk is a `"pr"` chunk; if the next chunk is an `"action"` chunk, or the merged PR was the build's last chunk, Vanadis waits for the PR to deploy before advancing or completing the build.
- **Verdict-Driven Action Chunk Lifecycle & Completion**:
  - An action chunk completes only when its worker calls `complete_chunk(chunk_id, dispatch_id, summary)` with a summary of the PASS evidence. A turn that ends without a verdict, or that hits eri's restart cap, is resumed automatically (up to 12 times) with a checkpoint prompt, then failed as a resumable chunk failure that wakes the thread; only its dependents wait.
  - Turns stop at `turn_timeout_minutes` (default 2h), but still write chunk instructions that time-box each wait to 45 min per turn and keep resumable evidence files so a resumed turn continues from checkpoints instead of restarting.
  - A repair PR's deploy returns the chunk to running; it never completes the chunk.
  - In Completed Chunks Ledgers, completed action chunks render as:
    `- [Chunk X/Y] <Title> (action, completed)`
- **Repairing Verification Blockers**: Keep live verification probes read-only and non-mutating. When a defect blocks the assigned objective, author a scoped repair in a scratch clone and submit through `aerial-pr.sh submit --vanadis-chunk <chunk-id>` under the normal review, CI, and deployment gates. Submission is not successful verification: rerun the failed checks against the deployed repair before passing the readiness gate or allowing cutover. Preserve unrelated scope boundaries and production safety restrictions.
- **Action Chunk Failure Handling (`fail_chunk` MCP Tool & Dispatch Failures)**:
  - On an unrecoverable blocker the worker calls `fail_chunk(chunk_id, reason)`; a failed dispatch outcome does the same. Either fails only that chunk: its transitive dependents stay blocked while every other ready chunk keeps launching, the card shows the failure banner, and the thread wakes once per chunk failure. The build turns failed only when nothing else is in flight or launchable. Repair in place with `retry_action_chunk` (allowed while siblings run). Fixable test, lint, or CI failures are fixed forward, not failed.
- **Action Chunks Run Their Own Work (No Detached Jobs)**: Write action chunk instructions so the chunk's agent runs long data work (backfills, copies, migrations) itself, as idempotent, checkpointed batches of foreground calls that each finish within the tool timeout, then verifies the result (e.g. per-day row-count parity) as its final step. Never instruct a chunk to launch a Kubernetes Job, Pod, or other detached process for this work. A detached process outlives the turn, so the harness can complete the chunk (including after a mid-turn restart) while the work is still running, and later destructive chunks then act on unfinished data. Any chunk that deletes or decommissions data must re-prove the preceding chunk's result itself before its first destructive step.
- **Planner Inspection & Prompt Sizing Budget (≤ 10 Read Steps, ~1–2k Chars/Chunk)**: Batch parallel `read_file` / `view_file` calls across the target files once up front (`O(1)` cached input tokens, targeting **≤ 10 read steps**) rather than planning blind or reading entire test suites line-by-line. Target **~1,000–2,000 characters per chunk** in `instructions` so the Planner synthesizes architectural contracts without pre-writing the entire PR diff in Markdown.
- When a build targets an entire package or subsystem, cross-check `git ls-files <pkg>` against the union of chunk `target_files` before enqueueing so no intended file silently drops out of scope.
- **Planner-Side Chunk Sizing (Authoring Heuristic, Not Executor Guardrails)**: Size chunks small at authoring time so no single chunk becomes a giant, hard-to-review PR:
  - **Canonical Go Service Layout (L0 -> L3)**: Root `package main` (`services/<svc>`, `mcp/<srv>`) is capped at 3 files (`main.go`, `main_test.go`, `goleak_test.go`) for bootstrap/wiring only; domain packages live under `internal/*` ordered strictly `L0` (`internal/config`, `internal/model` — zero sibling `internal/*` or `pgx` imports) -> `L1` (`internal/store`, outbound clients) -> `L2` (`internal/mcp`, `internal/tools`, `internal/service`, `internal/ticker`) -> `L3` (`internal/server`, `internal/router`). Horizontal grab-bag filenames (`pure.go`, `helpers.go`, `utils.go`, `common.go`, `types.go`, `frozen.go`, `*_part[0-9]*_test.go`) and `func(...)` fields inside `[]struct{...}` test tables are prohibited.
  - Give each chunk **one cohesive concern** and one outcome that can be reviewed and verified on its own (e.g. one behaviour or bugfix, one handler family, one migration step, one config/manifest change, or one relocation). Include in the same chunk whatever artifacts that outcome needs to be complete and provable — implementation with its tests, schema with the code that uses it, config with its manifests and docs, or, for a move, the relocated code with its tests. If the planner cannot cleanly name the concern, outcome, and scope it covers, decompose further before enqueueing instead of bundling nearby work.
  - As a sizing signal only, aim for roughly **≤ 8 files and ≤ ~800 lines changed per chunk**. Exceeding that signal means the planner should split the work into a named chain of smaller chunks up front. It is not a hard limit, diff gate, or acceptance check.
  - Keep the planner manifest lightweight: boundary and concern, starting symbols/files, frozen contracts, and verification command. The planner does not do full discovery or pre-solve the implementation; that work stays with the chunk worker and avoids moving execution cost into the expensive planner layer.
  - **Workers retain full execution freedom**: `target_files` and `target_symbols` are the starting scope, not an enforced allowlist. Because the planner has not done full discovery, workers may follow dependencies where they lead, pull in needed helpers, widen the file set, or adjust the approach within the chunk's concern. Never add fail-fast line/file-count gates, mechanical diff rejection, or instructions that force a worker to stop mid-work for crossing a planner-estimated size.
  - If execution discovers the chunk is genuinely larger than estimated, that is feedback for sizing the next split more accurately — not a leash on the current chunk. Let the current chunk finish its concern coherently.

## 2. Canonical Vanadis Build JSON Schema (`plan.json`)

```json
{
  "title": "Concise 5-7 Word Build Title",
  "tier": 2,
  "objective": "Overarching engineering objective and trade-off rule governing all chunks.",
  "shared_context": "Cross-chunk contracts, structural signatures, shared mock contracts, and anti-pattern guardrails.",
  "chunks": [
    {
      "id": "chunk-1-dark-launch-service",
      "mode": "pr",
      "repo": "azylman/aerial",
      "title": "Dark Launch New Workflow Service",
      "effort": "medium",
      "depends_on": [],
      "target_files": ["services/vanadis/pkg/workflow/service.go", "services/vanadis/pkg/workflow/service_test.go"],
      "target_symbols": ["WorkflowService", "TestWorkflowService_Execution"],
      "instructions": "Implement additive workflow service side-by-side with existing path without switching entrypoints. Verify via hermetic tests."
    },
    {
      "id": "chunk-2-dark-launch-schema",
      "mode": "pr",
      "repo": "azylman/aerial",
      "title": "Additive Database Schema Migration",
      "effort": "low",
      "depends_on": [],
      "target_files": ["services/eri/pkg/db/schema.sql"],
      "target_symbols": ["vanadis_builds", "vanadis_chunks"],
      "instructions": "Add additive columns with default values. Non-overlapping target_files allows parallel execution with chunk-1."
    },
    {
      "id": "chunk-3-pre-cutover-readiness-gate",
      "mode": "action",
      "repo": "",
      "title": "Verify Pre-Cutover Service Readiness",
      "effort": "low",
      "depends_on": ["chunk-1-dark-launch-service", "chunk-2-dark-launch-schema"],
      "target_files": [],
      "target_symbols": [],
      "instructions": "Use read-only live probes to verify service health, Restate deployment registrations, and 400 Bad Request rejection invariants; never inject synthetic PRs or builds into production. Repair defects blocking this objective through reviewed, chunk-attached PRs, then rerun verification against the deployed repair before passing the gate."
    },
    {
      "id": "chunk-4-cutover-and-purge",
      "mode": "pr",
      "repo": "azylman/aerial",
      "title": "Cut Over Entrypoint and Purge",
      "effort": "medium",
      "depends_on": ["chunk-3-pre-cutover-readiness-gate"],
      "target_files": ["services/vanadis/pkg/api/handlers.go", "services/synapse/conflicts.go"],
      "target_symbols": ["CreateBuildHandler", "PurgeLegacyHandler"],
      "instructions": "Switch live entrypoint to the verified workflow and purge legacy code. Maintain in-flight control-plane continuity fallback."
    },
    {
      "id": "chunk-5-post-cutover-boundary-gate",
      "mode": "action",
      "repo": "",
      "title": "Verify Post-Cutover Clean Boundaries",
      "effort": "low",
      "depends_on": ["chunk-4-cutover-and-purge"],
      "target_files": [],
      "target_symbols": [],
      "instructions": "Verify pod health, zero error logs, and static boundary invariants (e.g. rg -i legacy returns 0 matches)."
    }
  ]
}
```

## 3. Planner Owns Architecture & Contracts; Workers Own Implementation Code
- **No Worker Subagents**: Vanadis PR and action workers must NEVER spawn or invoke subagents. Never put delegation or subagent review requirements in chunk instructions or shared context; workers perform their assigned edits, operational steps, tests, and verification directly. Any required subagent review must be arranged by the parent planner outside worker execution.
- Never write vague outcome mandates (*"consolidate X, Y, and Z"* or *"refactor component A"*) that defer structural design discovery to a worker model.
- **Contracts, Not Code (Zero Full Function Dumps, Import Lists, or Line Numbers)**:
  - Specify **concrete interfaces, struct definitions/tags, function signatures, behavioral/edge-case rules, and shared helper/mock contracts strictly by symbol name**.
  - **NEVER** write full function implementations, line-by-line imperative pseudocode, manual Go `import (...)` lists, or hardcoded line-number ranges (`lines 873–1500`) in `shared_context` or `instructions`—doing so burns expensive Planner tokens duplicating work that `flash` workers and `ResolveSymbolSpans` handle natively.
- **Zero "Escape-Hatch" Clauses in Chunk Prompts**: Never include permissive fallback language like *"plus an optional callback/override/closure only where genuinely needed"*; worker models treat optional escape hatches as the default path for complex cases, so the Planner must resolve edge cases in the contract design up front.
- **Concrete Structural Contracts Over Stylistic Directives**: Never rely on vague stylistic directives (*"compact verbose structs"* or *"clean up X"*); every refactoring mandate must specify the concrete target types and helper signatures directly in `shared_context` and `instructions`.

## 4. Explicit "Must Not Change" Boundary Callouts in Chunk `instructions`
- Scope production-probe restrictions to the probes themselves. Verification chunk boundaries must permit scoped repair PRs for defects blocking their assigned objective while preserving deferred cutover and unrelated-code boundaries; do not describe the entire worker as read-only or prohibit all producer repairs.
- Every chunk's `instructions` MUST explicitly state what **must not change** by **symbol and file name**—naming adjacent callers, existing function signatures, behavioral contracts, or sibling types in the same file that must remain untouched—so worker models stay strictly within the chunk's blast radius without drifting into surrounding code.
- **Phase 1 Dark-Launch Boundary Enforcement**: Every Phase 1 dark-launch chunk's `instructions` must explicitly list all Phase 3 caller entrypoint files and symbols under a **Must Not Change** boundary. If `roborev` flags uncalled Phase 1 dark-launch components or deferred caller wiring, workers must remove any premature caller hooks or issue `/share/aerial/scripts/aerial-pr.sh override-roborev` rather than pulling Phase 3 caller files into Phase 1.

## 5. Strict Separation of Build-Level Fields (`title` vs. `objective` vs. `shared_context`)
- **`title` (5–7 Word Phrase Convention)**: Both the top-level build `title` (`CreateBuildRequest.Title`) and every chunk `title` (`CreateChunkInput.Title`) must be a concise **5–7 word phrase** describing the build or chunk for Discord status card headers and chunk rows.
- **`objective` (Governing Objective Function & Trade-Off Rule)**: State the overarching engineering goal and trade-off decision rule for the build—what "good" optimizes for across every chunk and *why* the changes are being made (e.g., latency, reliability, clean abstraction boundaries, or eliminating duplicated boilerplate via shared helpers without sacrificing readability or coverage). Never write `objective` as a redundant table-of-contents summary of chunk titles. Also state the chunk-specific objective at the top of each chunk's `instructions`.
- **`shared_context` (Cross-Chunk Contracts, Guardrails & Repeated-Pattern Scenario Examples)**:
  - Populate with architectural topology, service invariants, cross-chunk type/helper signatures, and explicit anti-pattern guardrails (banning syntactic evasion, pass-through wrapper stubs, union-flag/mode-switch structs, or `||` assertion-chaining code golf). Never duplicate per-chunk `instructions` inside `shared_context`.
  - **Repeated-Pattern Scenario Examples (Refactorings & Migrations)**: When a build involves applying the same repeated code pattern across many places or chunks (such as a table-driven test refactoring, helper/handler consolidation, or an API/schema migration), include **2–3 concrete example scenarios** in `shared_context` showing how to apply that pattern across representative cases (e.g., standard declarative case, error/mock-boundary case, and when to split vs. unify). Teach the transformation pattern *once* in `shared_context` (`O(1)`) and reference those scenario patterns by symbol name in per-chunk `instructions`—never pre-write the translated code for every target function across `N` chunks (`O(N)`).
  - **Full-Stack Signature Coverage Across Touched Layers**: When a build spans multiple layers (domain logic, API handlers, callers, or tests), `shared_context` must define the concrete struct/function signatures connecting each layer so workers do not invent mismatched cross-layer contracts.
  - **Complete Boundary Fixtures in Test Contracts**: When a unit under test interacts with multiple external boundaries (`git`, `k8s`, `http`, `db`), specify the declarative mock fields on the test-env struct so zero inline `func(...)` closures appear in table rows or runner loops.

## 6. Cognitive-Load `effort` Routing, `target_symbols` & Verification
- **Per-Chunk `effort` Routing**: Route per-chunk `effort` by cognitive load (`"low"` or `"medium"`; Vanadis caps `"high"` to `"medium"` so background chunk turns execute on interchangeable worker pools without locking the primary Discord thread queue):
  - `"effort": "low"` (`flash-low`): Mechanical, additive, or schema-driven execution where exact types, wiring, config, or boilerplate templates are fully specified by the Planner.
  - `"effort": "medium"` (`flash-medium`, default): Multi-function structural refactors, control-flow reorganization, test suite restructurings, concurrency primitives, or cross-cutting logic extraction.
- **`target_files` & `target_symbols` (JIT Production, Helper & `TestXxx` Span Resolution)**:
  - List all modified production and `*_test.go` files in `target_files`.
  - In `target_symbols`, always include: (1) **modified production symbols** (functions, methods, structs, interfaces), (2) **existing helper functions/types referenced in `instructions`** that the worker must call or inspect, and (3) **canonical `TestXxx` table function names** to extend. Vanadis pre-resolves exact `start_line`–`end_line` spans on `origin/main` across both production and test files (automatically scanning sibling `*_test.go` files in the same package when `<stem>_test.go` is absent) so workers read only targeted slices in `O(1)` without grepping test suites.
- **Zero Per-Chunk `acceptance_criteria` Checklists**: Do NOT populate per-chunk `acceptance_criteria` checklists or invent syntactic shell/regex gates (`grep`, `! grep`, `awk`, `wc -l`), which trigger Goodhart's Law. Universal verification (`go test -race`, `./scripts/verify.sh --staged`, 95% coverage floor) is enforced automatically, and workers evaluate their full `git diff` holistically against `objective` and `shared_context` prior to submission.
