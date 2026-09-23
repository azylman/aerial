# Implementation Plan: Structured Message Lifecycle & JIT Prompt Flattening

## 1. Problem Statement & Architectural Context
Currently, when a Discord event arrives at `brain/funnel.go`, `buildDiscordPrompt` immediately flattens `discordgo.Message` into a pseudo-YAML prompt envelope string (`<USER_REQUEST>\nHere's a message...\n- content: ...\n- mention_user_ids: [123]...`) and stores it directly into `db.Message.Content`.

Because downstream queue workers and components only possess this flat string:
1. **`isTier1Wake` in `brain/pkg/queue/channel_resolution.go`**: Performs brittle string searches (`strings.Index(m.Content, "- mention_user_ids: [")`, bracket extraction, and line splitting for `replying_to:`) to detect bot mentions and replies.
2. **`extractMessageBody` in `brain/pkg/queue/channel_resolution.go`**: Performs delimiter hunting (`- content:`, `\n- timestamp:`, `\n- mentions:`) to slice out the user's utterance. If a user pastes markdown or code containing these markers, the utterance is truncated or broken.
3. **`CoalesceBurstPrompt` in `brain/pkg/queue/burst.go`**: Re-slices prompt envelopes using `extractMessageBody` to construct multi-message prompts, creating an asymmetric flow where single messages bypass coalescing while bursts undergo re-parsing.
4. **`FormatMessage` in `brain/pkg/classifier/classifier.go`**: Injects the entire prompt envelope into the ambient classifier prompt, inflating tokens by ~80% and degrading classification accuracy.
5. **`CleanTaskSummary` in `brain/pkg/db/tasks.go`**: Splits lines hunting for `- content:` and `Prompt:` to generate task previews.
6. **`ExtractQueryText` in `brain/pkg/memory/search.go`**: Uses regex `reDiscordContent` to fish for `- content:` to extract search queries for vector memory retrieval.

The architectural fix requested by Alex is: **keep the value structured as long as possible, and only flatten it into a prompt string immediately prior to dispatching to `agy`**.

---

## 2. Proposed Architecture & Schema Specification

### 2.1 Database Schema Migration & `db.Message` Refactor
Add a structured `metadata JSONB` column to the `messages` table in PostgreSQL and `TEXT` in SQLite test fixtures.

#### Go Struct Definitions (`brain/pkg/db/messages.go`):
```go
// MessageMetadata stores structured Discord and source metadata preserved throughout the queue lifecycle.
type MessageMetadata struct {
	ChannelID         string   `json:"channel_id,omitempty"`
	TargetThreadID    string   `json:"target_thread_id,omitempty"`
	GuildID           string   `json:"guild_id,omitempty"`
	AuthorGlobalName  string   `json:"author_global_name,omitempty"`
	AuthorBot         bool     `json:"author_bot,omitempty"`
	IsAdmin           bool     `json:"is_admin,omitempty"`
	Mentions          []string `json:"mentions,omitempty"`
	MentionUserIDs    []string `json:"mention_user_ids,omitempty"`
	MentionRoleIDs    []string `json:"mention_role_ids,omitempty"`
	ReplyingToAuthor  string   `json:"replying_to_author,omitempty"`
	ReplyingToContent string   `json:"replying_to_content,omitempty"`
	Attachments       []string `json:"attachments,omitempty"`
}

type Message struct {
	ID            string          `json:"id"`
	RowID         int64           `json:"row_id,omitempty"`
	ThreadID      string          `json:"thread_id"`
	GuildID       string          `json:"guild_id"`
	AuthorID      string          `json:"author_id"`
	AuthorName    string          `json:"author_name"`
	Content       string          `json:"content"` // Raw user utterance (or prompt if from HTTP/scheduler)
	Summary       string          `json:"summary"`
	Status        string          `json:"status"`
	RetryCount    int             `json:"retry_count"`
	RestartCount  int             `json:"restart_count"`
	Effort        string          `json:"effort,omitempty"`
	ErrorMessage  string          `json:"error_message,omitempty"`
	ResponseText  string          `json:"response_text,omitempty"`
	ScheduleRunID string          `json:"schedule_run_id,omitempty"`
	Metadata      MessageMetadata `json:"metadata,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}
```

#### Backward-Compatibility Accessor (`brain/pkg/db/messages.go`):
```go
// BodyText returns the clean user utterance. If Content contains a legacy prompt envelope,
// it extracts the body for backward compatibility with historical DB rows and unmigrated test fixtures.
func (m Message) BodyText() string {
	if strings.Contains(m.Content, "<USER_REQUEST>") {
		return extractMessageBody(m.Content)
	}
	return m.Content
}
```

#### PostgreSQL Migration (`brain/pkg/db/schema.go`):
```sql
ALTER TABLE messages ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;
```

#### SQLite Test Fixture (`brain/pkg/db/sqlite_test_fixture_test.go`):
```sql
metadata TEXT NOT NULL DEFAULT '{}'
```

### 2.2 Ingestion & Funnel (`brain/funnel.go`)
When a Discord message arrives:
1. Extract author metadata, admin status, mentions, role IDs, reply author/content, and attachment URLs directly into `db.MessageMetadata`.
2. Populate `db.Message`:
   - `Content`: `m.Content` (the raw utterance typed by the user, WITHOUT prompt envelope wrapping).
   - `Metadata`: the populated `MessageMetadata`.
   - `AuthorID`, `AuthorName`, `ThreadID`, `GuildID`, `CreatedAt`.
3. Insert into `store.InsertMessage` and enqueue into `WorkerPool`.
4. Bypasses premature call to `BuildDiscordPrompt`!

### 2.3 Queue & Decision Logic (`brain/pkg/queue/`)
1. **`isTier1Wake`**:
   - Primary: Directly inspect `m.Metadata.MentionUserIDs`, `m.Metadata.MentionRoleIDs`, `m.Metadata.ReplyingToAuthor`, and `m.Metadata.Mentions`.
   - Fallback: If `m.Metadata` is empty (legacy test fixtures or DB rows), fall back to checking `m.Content` for bracket syntax or un-enveloped `<@id>` mentions.
   - Eliminates `strings.Index` bracket searches and line splits in production.
2. **`PlanBurstExecution`**:
   - Uses `m.BodyText()` when checking `classifier.IsHeuristicSkip`.
3. **`classifier.BuildBurstPrompt` & `FormatMessage`**:
   - `FormatMessage(m)` uses `m.BodyText()`.
   - The classifier receives only `[@Author] (ts): <utterance>` without prompt envelope noise.
4. **`CleanTaskSummary` & `ExtractQueryText`**:
   - Receives clean `m.BodyText()` directly.

### 2.4 JIT Prompt Assembly (`brain/pkg/queue/burst.go`)
In `AssembleTurnPrompt(input TurnPromptInput)`:
Right before dispatch to `agy`:
1. If `len(burst) == 1`:
   - If `burst[0].Metadata` is present: format the canonical single-message `<USER_REQUEST>` prompt envelope JIT using `FormatSingleDiscordPrompt(burst[0])`.
   - If `burst[0].Metadata` is empty and `burst[0].Content` is already a prompt (e.g. scheduler or legacy), use `burst[0].Content`.
2. If `len(burst) > 1`:
   - Coalesce into `<USER_REQUEST>\n[Multiple messages received in channel]\n--- Message 1 (by @author at HH:MM:SS) ---\n<body1>\n...` using `m.BodyText()`.
3. Apply standard layers: Coordination Context Hook, Channel Instructions, Semantic Memory Facts, Previous Session, Channel History, Thread Summary.

---

## 3. Review Panel Composition & Scope (The Girl Gang)
In accordance with `GEMINI.md` Invariant 8, a single consolidated review subagent will audit this architecture across 4 disciplines:
1. **Architecture & Gateway Specialist**: Funnel data flow, JIT prompt assembly, and envelope lifecycle.
2. **Database & Persistence Specialist**: PostgreSQL JSONB column, SQLite compatibility, zero-downtime migration, and SQL query serialization.
3. **Queue & Concurrency Specialist**: `WorkerPool`, `PlanBurstExecution`, thread state, and latency implications.
4. **Mandatory Adversarial Devil's Advocate**: Failure modes, edge cases (empty metadata, legacy envelopes, user markdown breaking envelopes, SQL injection/escaping).

---

## 4. Test Strategy & Verification Contract
1. **Unit Tests (TDD)**:
   - `messages_test.go`: Verify `InsertMessage`, `GetRecentMessages`, `GetPendingMessages` correctly round-trip `MessageMetadata` across PostgreSQL and SQLite.
   - `channel_resolution_test.go`: Verify `isTier1Wake` with populated `Metadata` (mentions, roles, reply) and verify backward compatibility fallback with legacy envelope strings.
   - `burst_test.go`: Verify JIT prompt assembly for both single message and burst messages.
   - `classifier_test.go`: Verify `FormatMessage` emits clean utterance without envelope overhead.
2. **Coverage Floor Contract**:
   - Maintain statement coverage strictly above 95.0% across all packages (`db`, `queue`, `classifier`, `runner`, `session`).
3. **Pre-Flight Verification**:
   - `./scripts/verify.sh --staged` passing with exit code 0.
