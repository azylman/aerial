# Channel-Mode Native Transcript Lookback & Fast Ambient Relevance Scorer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement two-tier channel wake (Tier 1 direct address vs. Tier 2 fast relevance scorer) and zero-prompt-injection native Antigravity transcript lookback for Discord channel mode.

**Architecture:** Discord gateway enqueues all channel messages immediately to `queue.WorkerPool`. The dedicated channel thread worker serializes processing, appends unaddressed ambient messages to `transcript.jsonl` as clean `USER_INPUT` turns (with zero `[NO_REPLY]`), runs unaddressed messages through a fast 1.5s classifier with trailing 10 messages of context, and executes `runner.RunAgy` for active wakes while updating SQLite with classification telemetry.

**Tech Stack:** Go 1.22, DiscordGo, modernc.org/sqlite, Antigravity CLI native session JSONL, Google Gemini API / Antigravity runner.

**Spec:** `docs/superpowers/specs/2026-09-02-channel-ambient-lookback-and-relevance-scorer-design.md`

## Global Constraints
- Target Go version: 1.22
- Zero fake `PLANNER_RESPONSE` steps and zero `[NO_REPLY]` strings written to transcripts.
- Strict serialization via `queue.WorkerPool.runThreadWorker`: Go and `agy` must never concurrently write to `transcript.jsonl`.
- Decouple ambient messages from `MaxSessionTurns`: only `runner.RunAgy` turns increment `turn_count`.
- 1.5-second SLA on ambient classifier with 3-failure circuit breaker defaulting to confidence `0.0`.
- All tests must pass cleanly under `docker run --rm -v ... golang:1.22 go test -race ./...`.

---

### Task 1: ChannelPolicy Configuration & Boolean Pointer Inheritance (`brain/pkg/config`)

**Files:**
- Modify: `brain/pkg/config/config.go:40-75, 430-475`
- Test: `brain/pkg/config/config_test.go`

**Interfaces:**
- Produces:
  - `ChannelPolicy.AmbientWakeThreshold float64`
  - `ChannelPolicy.IgnoreBots *bool`
  - `(p ChannelPolicy) IsBotIgnored() bool`
  - `ResolveChannelPolicy(channelID, channelName string) ChannelPolicy` (preserves explicit `ignore_bots: false`)

- [ ] **Step 1: Write the failing tests in `config_test.go`**
Add tests verifying:
1. `AmbientWakeThreshold` parses from YAML and defaults to `0.80` when `mode: "channel"`, or `0.0` when explicitly set to 0.
2. Pointer boolean `IgnoreBots`: when `channels.default` has `ignore_bots: true`, but `#lounge` explicitly specifies `ignore_bots: false`, `ResolveChannelPolicy` returns `IsBotIgnored() == false`.

```go
func TestChannelPolicy_AmbientWakeThreshold(t *testing.T) {
	yamlContent := `
channels:
  default:
    mode: "ignore"
  lounge:
    mode: "channel"
    ambient_wake_threshold: 0.75
  silent:
    mode: "channel"
    ambient_wake_threshold: 0.0
  auto:
    mode: "channel"
`
	cfg, err := LoadConfigFromYAML([]byte(yamlContent))
	if err != nil {
		t.Fatalf("LoadConfigFromYAML failed: %v", err)
	}

	pLounge := cfg.ResolveChannelPolicy("123", "lounge")
	if pLounge.AmbientWakeThreshold != 0.75 {
		t.Errorf("expected 0.75, got %f", pLounge.AmbientWakeThreshold)
	}

	pSilent := cfg.ResolveChannelPolicy("456", "silent")
	if pSilent.AmbientWakeThreshold != 0.0 {
		t.Errorf("expected 0.0, got %f", pSilent.AmbientWakeThreshold)
	}

	pAuto := cfg.ResolveChannelPolicy("789", "auto")
	if pAuto.AmbientWakeThreshold != 0.80 {
		t.Errorf("expected default 0.80 for channel mode, got %f", pAuto.AmbientWakeThreshold)
	}
}

func TestChannelPolicy_IgnoreBotsInheritance(t *testing.T) {
	yamlContent := `
channels:
  default:
    mode: "ignore"
    ignore_bots: true
  lounge:
    mode: "channel"
    ignore_bots: false
`
	cfg, err := LoadConfigFromYAML([]byte(yamlContent))
	if err != nil {
		t.Fatalf("LoadConfigFromYAML failed: %v", err)
	}

	pLounge := cfg.ResolveChannelPolicy("123", "lounge")
	if pLounge.IsBotIgnored() {
		t.Errorf("expected lounge ignore_bots: false to override default ignore_bots: true")
	}
}
```

- [ ] **Step 2: Run tests to verify failure**
Run: `docker run --rm -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -v ./pkg/config`
Expected: Compile error / FAIL (missing `AmbientWakeThreshold` and `IsBotIgnored`).

- [ ] **Step 3: Implement `AmbientWakeThreshold` and pointer `IgnoreBots` in `config.go`**
1. Update `ChannelPolicy`:
```go
type ChannelPolicy struct {
	Mode                 string   `yaml:"mode" json:"mode"`
	TypingIndicator      string   `yaml:"typing_indicator" json:"typing_indicator"`
	IgnoreBots           *bool    `yaml:"ignore_bots,omitempty" json:"ignore_bots,omitempty"`
	MaxSessionTurns      int      `yaml:"max_session_turns" json:"max_session_turns"`
	AmbientWakeThreshold *float64 `yaml:"ambient_wake_threshold,omitempty" json:"ambient_wake_threshold,omitempty"`
}

func (p ChannelPolicy) GetAmbientWakeThreshold() float64 {
	if p.AmbientWakeThreshold != nil {
		return *p.AmbientWakeThreshold
	}
	if strings.ToLower(p.Mode) == "channel" {
		return 0.80
	}
	return 0.0
}

func (p ChannelPolicy) IsBotIgnored() bool {
	if p.IgnoreBots != nil {
		return *p.IgnoreBots
	}
	return false
}
```
2. In `ResolveChannelPolicy`:
```go
if res.IgnoreBots == nil && def.IgnoreBots != nil {
	val := *def.IgnoreBots
	res.IgnoreBots = &val
}
if res.AmbientWakeThreshold == nil && def.AmbientWakeThreshold != nil {
	val := *def.AmbientWakeThreshold
	res.AmbientWakeThreshold = &val
}
```

- [ ] **Step 4: Run tests to verify pass**
Run: `docker run --rm -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -race -v ./pkg/config`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add brain/pkg/config/
git commit -m "feat(config): add AmbientWakeThreshold and fix IgnoreBots pointer inheritance"
```

---

### Task 2: SQLite Context Query & Indexing (`brain/pkg/db`)

**Files:**
- Modify: `brain/pkg/db/db.go:70-130, 450-500`
- Test: `brain/pkg/db/db_test.go`

**Interfaces:**
- Produces:
  - `idx_messages_thread_created_at` index in `InitDB`
  - `GetRecentThreadMessages(database *sql.DB, threadID string, limit int) ([]Message, error)`

- [ ] **Step 1: Write the failing tests in `db_test.go`**
```go
func TestGetRecentThreadMessages(t *testing.T) {
	dbase, err := InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer dbase.Close()

	now := time.Now().UTC()
	for i := 1; i <= 15; i++ {
		msg := Message{
			ID:         fmt.Sprintf("msg-%d", i),
			ThreadID:   "thread-lounge",
			AuthorID:   "user-1",
			AuthorName: "Alice",
			Content:    fmt.Sprintf("Message number %d", i),
			Status:     StatusCompleted,
			CreatedAt:  now.Add(time.Duration(i) * time.Minute),
			UpdatedAt:  now.Add(time.Duration(i) * time.Minute),
		}
		if err := InsertMessage(dbase, msg); err != nil {
			t.Fatalf("InsertMessage failed: %v", err)
		}
	}

	// Fetch recent 10 messages
	recent, err := GetRecentThreadMessages(dbase, "thread-lounge", 10)
	if err != nil {
		t.Fatalf("GetRecentThreadMessages failed: %v", err)
	}
	if len(recent) != 10 {
		t.Fatalf("expected 10 messages, got %d", len(recent))
	}
	// Verify chronological order (oldest to newest within the window)
	if recent[0].ID != "msg-6" || recent[9].ID != "msg-15" {
		t.Errorf("expected messages 6 through 15, got %s through %s", recent[0].ID, recent[9].ID)
	}
}
```

- [ ] **Step 2: Run test to verify failure**
Run: `docker run --rm -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -v -run TestGetRecentThreadMessages ./pkg/db`
Expected: FAIL (missing `GetRecentThreadMessages`).

- [ ] **Step 3: Implement index and query in `db.go`**
1. In `InitDB`, add:
```sql
CREATE INDEX IF NOT EXISTS idx_messages_thread_created_at ON messages(thread_id, created_at DESC);
```
2. Implement `GetRecentThreadMessages`:
```go
func GetRecentThreadMessages(database *sql.DB, threadID string, limit int) ([]Message, error) {
	if limit <= 0 {
		limit = 10
	}
	query := `
		SELECT id, thread_id, guild_id, author_id, author_name, content, summary, status,
		       retry_count, error_message, response_text, schedule_run_id, created_at, updated_at
		FROM (
			SELECT * FROM messages
			WHERE thread_id = ?
			ORDER BY created_at DESC
			LIMIT ?
		)
		ORDER BY created_at ASC
	`
	rows, err := database.Query(query, threadID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query recent thread messages: %w", err)
	}
	defer rows.Close()

	var msgs []Message
	for rows.Next() {
		var m Message
		var errMsg, respText, schedID sql.NullString
		if err := rows.Scan(
			&m.ID, &m.ThreadID, &m.GuildID, &m.AuthorID, &m.AuthorName, &m.Content, &m.Summary,
			&m.Status, &m.RetryCount, &errMsg, &respText, &schedID, &m.CreatedAt, &m.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan recent message: %w", err)
		}
		if errMsg.Valid { m.ErrorMessage = errMsg.String }
		if respText.Valid { m.ResponseText = respText.String }
		if schedID.Valid { m.ScheduleRunID = schedID.String }
		msgs = append(msgs, m)
	}
	return msgs, nil
}
```

- [ ] **Step 4: Run tests to verify pass**
Run: `docker run --rm -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -race -v ./pkg/db`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add brain/pkg/db/
git commit -m "feat(db): add thread_created_at index and GetRecentThreadMessages query"
```

---

### Task 3: Native Antigravity Transcript Appending Engine (`brain/pkg/session`)

**Files:**
- Modify: `brain/pkg/session/session.go:240-340`
- Test: `brain/pkg/session/session_test.go`

**Interfaces:**
- Produces:
  - `AppendAmbientTurn(convID string, compactContent string, createdAt time.Time) error`
  - Ensures session bootstrapping if transcript files do not exist
  - Monotonic `step_index` increment
  - Synchronizes both `transcript.jsonl` and `transcript_full.jsonl`

- [ ] **Step 1: Write the failing tests in `session_test.go`**
```go
func TestAppendAmbientTurn(t *testing.T) {
	tempDir := t.TempDir()
	convID := "test-session-ambient"
	logsDir := filepath.Join(tempDir, convID, ".system_generated", "logs")

	// Set override or mock target dirs
	// 1. Verify bootstrapping on fresh session (creates dir, starts step 0)
	now := time.Now().UTC()
	err := AppendAmbientTurnWithBaseDir(tempDir, convID, "[Chat #lounge] @Alice: Hello world", now)
	if err != nil {
		t.Fatalf("AppendAmbientTurn failed on bootstrap: %v", err)
	}

	tPath := filepath.Join(logsDir, "transcript.jsonl")
	data, err := os.ReadFile(tPath)
	if err != nil {
		t.Fatalf("failed to read transcript.jsonl: %v", err)
	}
	if !strings.Contains(string(data), `"step_index":0`) {
		t.Errorf("expected step_index 0, got: %s", string(data))
	}
	if strings.Contains(string(data), "NO_REPLY") {
		t.Errorf("transcript must NEVER contain NO_REPLY")
	}

	// 2. Append second message, verify step_index: 1
	err = AppendAmbientTurnWithBaseDir(tempDir, convID, "[Chat #lounge] @Bob: Hey Alice", now.Add(time.Minute))
	if err != nil {
		t.Fatalf("AppendAmbientTurn failed on second append: %v", err)
	}
	data2, _ := os.ReadFile(tPath)
	lines := strings.Split(strings.TrimSpace(string(data2)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	if !strings.Contains(lines[1], `"step_index":1`) {
		t.Errorf("expected step_index 1 on line 2, got: %s", lines[1])
	}
}
```

- [ ] **Step 2: Run test to verify failure**
Run: `docker run --rm -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -v -run TestAppendAmbientTurn ./pkg/session`
Expected: FAIL (missing `AppendAmbientTurn`).

- [ ] **Step 3: Implement `AppendAmbientTurn` in `session.go`**
1. Backward seek scanner:
```go
type transcriptStepHeader struct {
	StepIndex int `json:"step_index"`
}

func getLastStepIndex(filePath string) (int, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return -1, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return -1, err
	}
	if fi.Size() == 0 {
		return -1, nil
	}

	seekOffset := int64(4096)
	if fi.Size() < seekOffset {
		seekOffset = fi.Size()
	}
	if _, err := f.Seek(-seekOffset, io.SeekEnd); err != nil {
		return -1, err
	}

	buf := make([]byte, seekOffset)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		return -1, err
	}

	lines := strings.Split(strings.TrimRight(string(buf[:n]), "\r\n"), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}
		var hdr transcriptStepHeader
		if err := json.Unmarshal([]byte(line), &hdr); err == nil {
			return hdr.StepIndex, nil
		}
	}
	return -1, nil
}
```
2. In `AppendAmbientTurn`:
- Resolve base directory (`/data/brain` or fallback).
- Create `.system_generated/logs` if not present.
- Find last `step_index`; next index is `lastIndex + 1` (or 0 if new/empty).
- Construct canonical JSON line:
  ```json
  {"step_index":nextIndex,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"...","content":"<compactContent>"}
  ```
- Atomically append to both `transcript.jsonl` and `transcript_full.jsonl` with trailing newline.

- [ ] **Step 4: Run tests to verify pass**
Run: `docker run --rm -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -race -v ./pkg/session`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add brain/pkg/session/
git commit -m "feat(session): implement AppendAmbientTurn with backward-seek and session bootstrapping"
```

---

### Task 4: Fast Ambient Relevance Classifier (`brain/pkg/classifier`)

**Files:**
- Create: `brain/pkg/classifier/classifier.go`
- Create: `brain/pkg/classifier/classifier_test.go`

**Interfaces:**
- Produces:
  - `type EvaluationResult struct { Confidence float64; Reason string }`
  - `type LLMCaller func(ctx context.Context, prompt string) (string, error)`
  - `EvaluateMessageRelevance(ctx context.Context, caller LLMCaller, current db.Message, recent []db.Message) (EvaluationResult, error)`
  - `CircuitBreaker` tracking consecutive failures and 60s cooldown

- [ ] **Step 1: Write the failing tests in `classifier_test.go`**
```go
func TestEvaluateMessageRelevance_HighConfidence(t *testing.T) {
	mockCaller := func(ctx context.Context, prompt string) (string, error) {
		return `{"confidence": 0.95, "reason": "User is asking for help with a server crash"}`, nil
	}
	res, err := EvaluateMessageRelevance(context.Background(), mockCaller, db.Message{Content: "server crashed"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Confidence != 0.95 {
		t.Errorf("expected 0.95, got %f", res.Confidence)
	}
}

func TestEvaluateMessageRelevance_CircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(3, 60*time.Second)
	// Fail 3 times
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}
	if !cb.IsOpen() {
		t.Errorf("expected circuit breaker to be open after 3 failures")
	}
}
```

- [ ] **Step 2: Run test to verify failure**
Run: `docker run --rm -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -v ./pkg/classifier`
Expected: FAIL (package does not exist).

- [ ] **Step 3: Implement `classifier.go`**
1. Circuit Breaker with mutex protecting state.
2. Prompt formulation with recent 10 messages context + current message.
3. Strict 1500ms timeout context.
4. JSON unmarshaling of `{ "confidence": float, "reason": string }` with clamp `[0.0, 1.0]`.
5. Fallback to `confidence = 0.0` on any error/timeout.

- [ ] **Step 4: Run tests to verify pass**
Run: `docker run --rm -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -race -v ./pkg/classifier`
Expected: PASS.

- [ ] **Step 5: Commit**
```bash
git add brain/pkg/classifier/
git commit -m "feat(classifier): implement fast ambient relevance scorer with circuit breaker and 1.5s SLA"
```

---

### Task 5: Gateway Routing, Burst Partitioning & Classification Telemetry (`brain/funnel.go` & `brain/pkg/queue`)

**Files:**
- Modify: `brain/funnel.go:185-195, 250-275, 595-605`
- Modify: `brain/pkg/queue/queue.go:340-480`
- Test: `brain/funnel_test.go`
- Test: `brain/pkg/queue/queue_test.go`

**Interfaces:**
- Consumes:
  - `pkg/config.ChannelPolicy.GetAmbientWakeThreshold()`
  - `pkg/db.GetRecentThreadMessages()`
  - `pkg/session.AppendAmbientTurn()`
  - `pkg/classifier.EvaluateMessageRelevance()`
- Produces:
  - 3-Phase Burst Partitioning in `processBurst`
  - Telemetry logging: `[AmbientClassifier] ...` to stdout and SQLite `messages.error_message`
  - Removal of `[NO_REPLY]` guidance from `buildDiscordPrompt`
  - Proper bot check in `RunStartupCatchUpSweep`

- [ ] **Step 1: Write failing tests in `queue_test.go`**
Test:
1. `TestProcessBurst_PureAmbient`: 2 ambient messages are appended via `AppendAmbientTurn`, `turn_count` remains 0, SQLite status `COMPLETED [AMBIENT score=...]`.
2. `TestProcessBurst_Tier1Wake`: Direct mention triggers `RunAgy` and increments `turn_count`.
3. `TestProcessBurst_Tier2Wake`: Unaddressed message with confidence >= threshold triggers `RunAgy`, increments `turn_count`, and logs telemetry.
4. `TestProcessBurst_MixedBurst`: Mixed burst `[Ambient1, Wake2]` records Ambient1 first, then runs `RunAgy` on Wake2.

- [ ] **Step 2: Run test to verify failure**
Run: `docker run --rm -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -v -run TestProcessBurst ./pkg/queue`
Expected: FAIL.

- [ ] **Step 3: Implement changes in `funnel.go` and `queue.go`**
1. In `funnel.go:buildDiscordPrompt`: remove `If this message does not require your response... output [NO_REPLY]` line.
2. In `funnel.go:RunStartupCatchUpSweep`: replace `if m.Author.Bot` with `if m.Author.Bot && policy.IsBotIgnored()`.
3. In `queue.go:processBurst`:
   - Identify Tier 1 wakes (mention, reply, regex `\b(?i:aerial)\b`).
   - If not Tier 1, invoke `classifier.EvaluateMessageRelevance`.
   - Log classification to stdout:
     `log.Printf("[AmbientClassifier] Channel %s | Msg %s | Author %s | Score: %.2f (Thresh: %.2f) | Wake: %t | Reason: %s", ...)`
   - Implement 3-phase burst partitioning:
     - Leading ambient messages appended individually via `session.AppendAmbientTurn`.
     - Active wake message runs typing indicator, increments `turn_count`, invokes `runner.RunAgy`, delivers to Discord, and marks SQLite `COMPLETED`.
     - If all messages ambient: mark SQLite `COMPLETED` with `fmt.Sprintf("[AMBIENT score=%.2f/%.2f reason=%q]", conf, thresh, reason)`.

- [ ] **Step 4: Run full test suite to verify pass**
Run: `docker run --rm -v "C:\Users\alexz\.gemini\antigravity\scratch\gundam\brain:/app" -w /app golang:1.22 go test -race -v ./...`
Expected: All packages PASS.

- [ ] **Step 5: Commit**
```bash
git add brain/funnel.go brain/funnel_test.go brain/pkg/queue/
git commit -m "feat(queue): implement 3-phase burst partitioning, Tier 2 wake, and classification telemetry"
```
