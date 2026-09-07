# Thread-Only Background History Summarizer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement thread-only background history summarization (`snap.IsThread && snap.ParentID != ""`) over recent thread history (max 100 messages) during cold session initialization in `aerial-brain`.

**Architecture:** Add `FetchRecentThreadHistory` and `SummarizeThreadHistory` in `brain/pkg/queue/history.go` with singleflight deduplication, context cancellation (3.0s timeout), XML structural validation, and SQLite caching with `last_summarized_message_id` watermark invalidation. Wire strict AND condition check (`snap.IsThread && snap.ParentID != "" && snap.ParentID != snap.ID`) in `brain/pkg/queue/queue.go` Turn 1 seed prompt builder.

**Tech Stack:** Go 1.22+, `discordgo`, `golang.org/x/sync/singleflight`, Flash tier model (`ClassifierModel`).

**Spec:** `docs/superpowers/specs/2026-09-07-thread-background-summarizer-design.md`

## Global Constraints
- Strict AND condition: `snap.IsThread && snap.ParentID != "" && snap.ParentID != snap.ID`. Main channels MUST NOT run history summarization.
- Context cancellation & singleflight: 3.0s timeout with `defer cancel()`, deduplicated via `singleflight.Group`.
- Local DB First: Read `/data/aerial.db` SQLite `messages` first; bypass Discord API if local history is present.
- Cache Invalidation: Compare `last_summarized_message_id` against latest thread message; invalidate cache if new turns exist.
- Output Validation: Validate `<THREAD_SUMMARY>` XML structure before injection.
- GitHub Links Only: No `file:///` links in external output.

---

### Task 1: Thread History Fetcher, Singleflight Summarizer & XML Validation

**Files:**
- Modify: `scratch/aerial/brain/pkg/queue/history.go`
- Test: `scratch/aerial/brain/pkg/queue/history_test.go`

**Interfaces:**
- Consumes: `discordgo.Session`, `sql.DB`, `singleflight.Group`, `ClassifierModel`
- Produces: `FetchRecentThreadHistory`, `SummarizeThreadHistory`

- [ ] **Step 1: Write failing unit test for `SummarizeThreadHistory` with singleflight & XML validation**

- [ ] **Step 2: Run test to verify failure**

- [ ] **Step 3: Implement `FetchRecentThreadHistory`, singleflight deduplication, and XML structural validation in `history.go`**

- [ ] **Step 4: Run test to verify it passes**

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/queue/history.go brain/pkg/queue/history_test.go
git commit -m "feat(queue): implement thread history fetcher, singleflight Flash summarizer, and XML validator"
```

---

### Task 2: Wire Thread-Only Seed Prompt Injection & Watermarked SQLite Summary Caching

**Files:**
- Modify: `scratch/aerial/brain/pkg/queue/queue.go`
- Modify: `scratch/aerial/brain/pkg/db/sessions.go`
- Test: `scratch/aerial/brain/pkg/queue/queue_test.go`

- [ ] **Step 1: Write failing integration test for thread vs channel summarizer routing & watermarked SQLite summary caching**

- [ ] **Step 2: Run test to verify failure**

- [ ] **Step 3: Wire `isThreadColdStart` AND condition (`snap.IsThread && snap.ParentID != ""`) in `queue.go` with 3.0s context timeout fallback & `last_summarized_message_id` watermark invalidation**

- [ ] **Step 4: Run full test suite to verify all tests pass**

- [ ] **Step 5: Commit**

```bash
git add brain/pkg/queue/queue.go brain/pkg/db/sessions.go brain/pkg/queue/queue_test.go
git commit -m "feat(queue): wire thread-only background summarization into seed prompt pipeline with watermarked SQLite caching"
```
