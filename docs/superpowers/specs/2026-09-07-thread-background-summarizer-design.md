# Thread-Only Background History Summarizer Architecture

## Executive Summary
This specification defines the design for thread-only background history summarization in `aerial-brain`.

When a cold session initializes (`internal_session_id == ""`), if and ONLY if execution is occurring inside a Discord thread sub-channel (`snap.IsThread == true && snap.ParentID != ""`), `aerial-brain` dispatches a high-speed Flash tier model call to summarize the thread history, focusing on key technical decisions, open action items, and future discussion topics.

---

## 1. Motivation & Requirements
- **Thread Continuity**: Long-running threads contain rich context that raw 10-message lookback misses.
- **Channel Isolation Invariant**: Main channel cold starts MUST NOT trigger history summarization to prevent channel-wide token bloat.
- **Focus Area**: The summarization prompt must prioritize architecture decisions, unresolved questions, user preferences, and pending action items.

---

## 2. Core Condition & Execution Flow

```
                     +-----------------------------------+
                     |   Cold Session (session_id = "")  |
                     +-----------------------------------+
                                       |
                                       v
                     +-----------------------------------+
                     | IsThread && ParentID != "" && ID  |
                     +-----------------------------------+
                                   /       \
                             YES  /         \  NO
                                 v           v
   +---------------------------------+   +----------------------------------+
   | Check SQLite Cached Summary     |   | Fetch Raw 10 Messages            |
   | & Compare Watermark             |   | (DefaultHistoryFetcher)          |
   | (Invalidate if new msgs exist)  |   +----------------------------------+
   +---------------------------------+                   |
                   |                                     |
                   v                                     v
   +---------------------------------+   +----------------------------------+
   | Fetch Recent Thread History     |   | Format raw <CHANNEL_HISTORY>     |
   | (aerial.db first, max 100)      |   +----------------------------------+
   +---------------------------------+                   |
                   |                                     |
                   v                                     v
   +---------------------------------+   +----------------------------------+
   | SummarizeThreadHistory (Flash)  |   | Inject into Turn 1 Prompt        |
   | (Singleflight + 3.0s Timeout)   |   +----------------------------------+
   +---------------------------------+
                   |
                   v
   +---------------------------------+
   | Validate XML Output & Cache DB  |
   | (Store last_summarized_msg_id)  |
   +---------------------------------+
                   |
                   v
   +---------------------------------+
   | Inject <THREAD_SUMMARY>         |
   +---------------------------------+
```

### 2.1 Strict AND Condition & Parent ID Check
```go
isThreadColdStart := snap.IsThread && snap.ParentID != "" && snap.ParentID != snap.ID && currentSessionID == ""
```
This guarantees main channels always use raw 10-message lookback, while threads execute thread history summarization.

### 2.2 Database Read Priority, Singleflight & Cache Watermark Invariants
1. **Local DB First**: `FetchRecentThreadHistory` queries the local `/data/aerial.db` SQLite `messages` table first for that thread. If local DB history contains up to 100 messages, Discord REST API requests are completely bypassed.
2. **Singleflight Deduplication**: Cold-start summarization calls use `singleflight.Group` keyed by `threadID` to prevent concurrent messages in the same thread from stampeding redundant Flash summarization requests.
3. **SQLite Summary Watermark & Cache Invalidation**: Upon successful Flash summarization, the resulting `<THREAD_SUMMARY>` text is persisted in `aerial.db` along with a watermark field `last_summarized_message_id`. On subsequent cold starts:
   - `aerial-brain` checks if any new thread messages have been received with `id > last_summarized_message_id`.
   - If **no new messages** exist, the cached summary is loaded directly from SQLite.
   - If **new messages exist**, the cache is invalidated, and Flash is re-invoked over the updated history to produce a fresh summary!

### 2.3 Prompt Assembly & Stacking Limits
When both `<THREAD_SUMMARY>` and `<FACTS>` are present:
- `<THREAD_SUMMARY>` is prioritized as thread structural memory.
- `<FACTS>` items are capped at 5 highest-relevance items to prevent prompt bloat.
- Total seed prompt token length is strictly bounded below 4,000 tokens.

### 2.4 Summarization Extraction Prompt & Structural Validation
The transcript is isolated within `<raw_thread_transcript>` XML tags. The Flash model (`appCfg.Current().ClassifierModel`) is instructed with:
```
Synthesize the following Discord thread transcript into a concise memory block. Focus specifically on:
1. Technical & Architecture Decisions: Key invariants, design choices, and code changes agreed upon.
2. Open Questions & Future Action Items: Pending decisions, TODOs, and topics likely to be referenced in future turns.
3. User Directives & Constraints: Explicit instructions and preferences given by the user.
```
- **Structural Validation**: Flash output must parse as valid XML containing `<THREAD_SUMMARY>...</THREAD_SUMMARY>`. If output is malformed or invalid XML, `aerial-brain` discards it and falls back to raw history.

---

## 3. Fallback & Safety Constraints
- **Context & Timeout Ceiling**: Summarization uses `context.WithTimeout(parentCtx, 3*time.Second)` with `defer cancel()`. Outbound HTTP requests to the LLM are terminated immediately upon timeout expiry.
- **Fallback**: If summarization times out or errors out, `aerial-brain` falls back immediately to raw 10-message `<CHANNEL_HISTORY>` lookback without failing the turn.
- **Sanitization**: Output is sanitized with `SanitizeHistoryContent` to escape XML delimiter tags before injection.

---

## 4. Verification & Testing Strategy
- Unit tests in `queue_test.go` verifying that main channels skip thread summarization.
- Unit tests verifying local DB read priority, singleflight deduplication, cache watermark invalidation, and summary caching in `aerial.db`.
- Unit tests verifying XML structural validation and fallback behavior when summarizer times out.
- Integration tests checking `<THREAD_SUMMARY>` XML block injection.
