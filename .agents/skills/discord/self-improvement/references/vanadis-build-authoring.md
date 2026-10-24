# Vanadis Multi-Chunk Build Authoring Runbook (`scripts/aerial-build.sh enqueue <plan.json>`)

For multi-step autonomous builds spanning multiple PRs or services, author a declarative build plan JSON and enqueue via `/share/aerial/scripts/aerial-build.sh enqueue <plan.json>`.

## 1. Architect/Planner Up-Front Inspection & Scope Inventory
- Before writing the build plan JSON, the Planner MUST batch parallel `read_file` / `view_file` calls across the target files once up front (`O(1)` cached input tokens) rather than planning blind, authoring detailed per-chunk `instructions`.
- When a build targets an entire package or subsystem, cross-check `git ls-files <pkg>` against the union of chunk `target_files` before enqueueing so no intended file silently drops out of scope.

## 2. Planner Owns Architecture & Design Synthesis; Workers Execute Concrete Contracts
- Never write vague outcome mandates (*"consolidate X, Y, and Z"* or *"refactor component A"*) that defer structural design discovery to a worker model.
- While inspecting target files, the Planner MUST verify architectural boundaries and **explicitly specify the concrete interfaces, struct definitions, function signatures, or shared helper/mock contracts in each chunk's `instructions`** so workers implement well-defined designs rather than inventing ad-hoc abstractions (such as mode-switch union structs or concatenated mega-functions).
- **Zero "Escape-Hatch" Clauses in Chunk Prompts**: Never include permissive fallback language like *"plus an optional callback/override/closure only where genuinely needed"*; worker models treat optional escape hatches as the default path for complex cases, so the Planner must resolve edge cases in the contract design up front.
- **Concrete Structural Contracts Over Stylistic Directives**: Never rely on vague stylistic directives (*"compact verbose structs"* or *"clean up X"*); every refactoring mandate must specify the concrete target types, helper signatures, and code exemplar directly in `shared_context` and `instructions`.

## 3. Explicit "Must Not Change" Boundary Callouts in Chunk `instructions`
- Every chunk's `instructions` MUST explicitly state what **must not change**—naming adjacent callers, existing function signatures, behavioral contracts, or sibling code blocks in the same file that must remain untouched—so worker models stay strictly within the chunk's blast radius without drifting into surrounding code.

## 4. Strict Separation of Build-Level Fields (`title` vs. `objective` vs. `shared_context`)
- **`title` (5–7 Word Phrase Convention)**: Both the top-level build `title` (`CreateBuildRequest.Title`) and every chunk `title` (`CreateChunkInput.Title`) must be a concise **5–7 word phrase** describing the build or chunk for Discord status card headers and chunk rows.
- **`objective` (Governing Objective Function & Trade-Off Rule)**: State the overarching engineering goal and trade-off decision rule for the build—what "good" optimizes for across every chunk and *why* the changes are being made (e.g., latency, reliability, clean abstraction boundaries, or eliminating duplicated boilerplate via shared helpers without sacrificing readability or coverage). Never write `objective` as a redundant table-of-contents summary of chunk titles. Also state the chunk-specific objective at the top of each chunk's `instructions`.
- **`shared_context` (Cross-Chunk Contracts, Exemplars & Guardrails)**:
  - Populate with architectural topology, service invariants, cross-chunk type/helper contracts, and—when introducing a recurring structural pattern—a concise **~15–20 line code exemplar** paired with explicit anti-pattern guardrails (banning syntactic evasion, pass-through wrapper stubs, union-flag/mode-switch structs, or `||` assertion-chaining code golf).
  - **Full-Stack `shared_context` Coverage Across All Touched Layers**: When a build spans multiple layers (domain logic, API handlers, callers, or tests), `shared_context` must define concrete signatures and exemplars for *every* layer being modified—any layer omitted from `shared_context` will be written ad-hoc by each chunk worker.
  - **Complete Boundary Fixtures in Exemplars**: When a unit under test interacts with multiple external boundaries (`git`, `nomad`, `http`, `db`), the test-env exemplar must declaratively configure *all* mocked boundaries so zero inline `func(...)` closures appear in table rows or runner loops.

## 5. Cognitive-Load `effort` Routing, `target_symbols` & Verification
- **Per-Chunk `effort` Routing**: Route per-chunk `effort` by cognitive load (`"low"` or `"medium"`; Vanadis caps `"high"` to `"medium"` so background chunk turns execute on interchangeable worker pools without locking the primary Discord thread queue):
  - `"effort": "low"` (`flash-low`): Mechanical, additive, or schema-driven execution where exact types, wiring, config, or boilerplate templates are fully specified by the Planner.
  - `"effort": "medium"` (`flash-medium`, default): Multi-function structural refactors, control-flow reorganization, test suite restructurings, concurrency primitives, or cross-cutting logic extraction.
- **`target_files` & `target_symbols` (JIT Symbol Line-Slice Resolution)**: Specify exact files in `target_files` and symbol names (functions, structs, interfaces, or tables) in `target_symbols`. Vanadis pre-resolves exact `start_line`–`end_line` spans on `origin/main` and injects a fallback `grep -n -E` command so workers read only targeted slices rather than entire files.
- **Zero Per-Chunk `acceptance_criteria` Checklists**: Do NOT populate per-chunk `acceptance_criteria` checklists or invent syntactic shell/regex gates (`grep`, `! grep`, `awk`, `wc -l`), which trigger Goodhart's Law. Universal verification (`go test -race`, `./scripts/verify.sh --staged`, 95% coverage floor) is enforced automatically, and workers evaluate their full `git diff` holistically against `objective` and `shared_context` prior to submission.
