# Architecture & Implementation Plan: Pluggable Voice Engine Interface & Dual Implementations

## 1. Overview & Problem Statement
Profiling of voice turn latency identified that `aerial-brain` deliberation was bottlenecked by Cloud Code PA HTTP 429 concurrency throttles and `agy`'s mandatory reasoning token overhead (~100–150 tokens per turn).
The user requested an interface abstraction for voice processing to support two distinct implementations selectable via `config.yaml`:
1. **`agy`**: The existing process pool (`UnifiedProcessPool`) backed by the `agy` streaming daemon.
2. **`gemini_api`**: A direct Gemini API engine (`generativelanguage.googleapis.com`) with `thinking_budget: 0` for sub-second TTFT and quota isolation.

Both options must coexist, be configurable in `config.yaml` (`voice.engine`), and allow side-by-side performance, rate limit, and latency benchmarking.

---

## 2. Architecture & Design

### 2.1 Interface Definitions (`brain/pkg/runner/voice_pool.go`)
Define a decoupled, minimal contract for voice turn execution:

```go
// VoiceSession represents an active conversational session for a voice device (e.g. kiosk).
type VoiceSession interface {
    Send(prompt string, turn *TurnContext) error
    SessionID() string
}

// VoiceProcessPool abstracts the session pool management for voice execution engines.
type VoiceProcessPool interface {
    GetOrCreateSession(ctx context.Context, targetKey string) (VoiceSession, error)
    Initialize(ctx context.Context) error
    Close() error
}
```

### 2.2 Implementation 1: Existing `agy` CLI Runner (`UnifiedProcessPool`)
- `*UnifiedProcessPool` already implements `Initialize(ctx)` and `Close()`.
- Add `GetOrCreateSession(ctx context.Context, targetKey string) (VoiceSession, error)` to `*UnifiedProcessPool`, delegating to `p.GetOrCreate(ctx, targetKey)`.
- `*StreamingDaemon` already implements `Send(prompt, turn)` and `SessionID() string`, directly satisfying `VoiceSession`.
- 100% backward-compatible, zero-cost adaptation.

### 2.3 Implementation 2: Direct Gemini API Engine (`GeminiVoicePool`)
- Implements `VoiceProcessPool`.
- Manages `GeminiVoiceSession` instances per target device (e.g. `kiosk`), protected by `sync.RWMutex`.
- Each `GeminiVoiceSession` protects its conversational history slice with a `sync.Mutex`.
- **Context Window Protection**: Implements a sliding window history buffer (clamped to the last 20 messages / 10 turns) to prevent context explosion on long-running kiosk displays.
- Calls Google Generative Language API (`/v1beta/models/{model}:streamGenerateContent`):
  - Injects `thinkingConfig: { thinkingBudget: 0 }` to eliminate reasoning token latency.
  - Passes system instructions (loaded from voice rules or config).
  - Streams SSE / JSON chunks:
    - Calls `turn.Sink.OnTurnStarted()`
    - Emits incremental text deltas to `turn.Sink.OnTextDelta(chunk)` (which drives sentence detection in `voiceTurnSink`).
    - Emits tool execution status `turn.Sink.OnToolCall(name, cmd)`.
    - Completes with `turn.Sink.OnResult(&TurnResult{...})`.
  - **Resilience & Security**:
    - Validates prompt is non-empty before dispatching.
    - Zero plaintext tokens: strips API key query parameters/headers from any diagnostic error logs via sanitizer.
    - HTTP client enforces per-chunk read timeouts to prevent hanging indefinitely on stalled streams.
    - Explicitly catches HTTP 429 and translates to actionable error message for the device.
- Supports pluggable `ToolDispatcher` for MCP tools (`ha-mcp`, `scheduler-mcp`).
- Fully testable via `httptest.Server` and mock round-trippers for hermetic unit testing.

### 2.4 Configuration (`brain/pkg/config/config.go`)
Extend `VoiceConfig`:
```yaml
voice:
  engine: agy # or gemini_api (defaults to agy)
  model: gemini-2.5-flash
  prewarmed_targets:
    - kiosk
```
- Normalization: `c.VoiceEngine()` returns `"agy"` (default) or `"gemini_api"`. Unrecognized strings fall back safely to `"agy"`.
- Defensive copying and immutability preserved.

### 2.5 Queue Worker Pool Integration (`brain/pkg/queue/pool.go`)
- Update `WorkerPoolConfig.VoiceProcessPool` to type `runner.VoiceProcessPool` (was `*runner.UnifiedProcessPool`).
- In `ExecuteVoiceTurn`, use `procPool.GetOrCreateSession(ctx, deviceKey)`.

### 2.6 Application Wiring (`brain/main.go`)
- Check `cfg.VoiceEngine()`:
  - If `"gemini_api"`, instantiate `runner.NewGeminiVoicePool(...)`.
  - Else (`"agy"`), instantiate `runner.NewUnifiedProcessPool(...)`.
- Pass to `WorkerPoolConfig.VoiceProcessPool`.

---

## 3. Implementation Tasks (~15-Minute Chunks)

### Task 1: Configuration Schema & Methods
- File: `brain/pkg/config/config.go`, `brain/pkg/config/config_test.go`
- Add `Engine` to `VoiceConfig`.
- Add `VoiceEngine()` getter with normalization and default `"agy"`.
- Add table-driven tests for parsing, defaults, fallback, and cloning.

### Task 2: Core Voice Interfaces & `UnifiedProcessPool` Adapter
- File: `brain/pkg/runner/voice_pool.go`, `brain/pkg/runner/unified_pool.go`, `brain/pkg/runner/voice_pool_test.go`
- Define `VoiceSession` and `VoiceProcessPool` interfaces.
- Add `GetOrCreateSession` to `UnifiedProcessPool`.
- Verify `*StreamingDaemon` satisfies `VoiceSession` and `*UnifiedProcessPool` satisfies `VoiceProcessPool`.

### Task 3: Queue Worker Pool Integration
- File: `brain/pkg/queue/pool.go`, `brain/pkg/queue/voice_test.go`, `brain/pkg/queue/unified_pool_wiring_test.go`
- Update `WorkerPoolConfig.VoiceProcessPool` to `runner.VoiceProcessPool`.
- Update `ExecuteVoiceTurn` to call `GetOrCreateSession`.
- Ensure all existing unit tests in `pkg/queue` compile and pass.

### Task 4: Direct Gemini API Voice Pool (`GeminiVoicePool`) with MCP Tool Dispatcher
- File: `brain/pkg/runner/gemini_voice_pool.go`, `brain/pkg/runner/gemini_voice_pool_test.go`
- Implement `GeminiVoicePool` and `GeminiVoiceSession`:
  - HTTP streaming client to `generativelanguage.googleapis.com` with API key auth (`HarnessAPIKey` fallback to `APIKey`).
  - Zero thinking budget (`thinking_budget: 0`).
  - Session message history buffer with sliding window history clamping.
  - Streaming event dispatch to `TurnSink` (`OnTurnStarted`, `OnTextDelta`, `OnResult`, `OnError`).
  - **MCP Tool Calling & Dispatcher**:
    - `MCPDispatcher` interface and `DefaultMCPDispatcher` discovery (`tools/list`) and execution (`tools/call`).
    - Transparent support for both standard JSON and SSE Streamable HTTP (Home Assistant `ha-mcp`).
    - Tool schema parameter cleanup (`$schema` stripping, uppercase OpenAPI types).
    - Multi-turn tool calling loop (up to 5 iterations) feeding `functionCall` to dispatcher and `functionResponse` back to model.
    - Sink notification of tool execution via `turn.Sink.OnToolCall(name, args)`.
  - **State Persistence & Recovery**:
    - Disk transcript logging to `<DataDir>/brain/<SessionID>/.system_generated/logs/transcript.jsonl`.
    - Auto-hydration of recent history on session startup to prevent state drift.
  - **Security & Hygiene**:
    - Strict redaction of API keys from query parameters and headers in all logs and error messages.
- Table-driven unit tests using `httptest.Server` covering streaming, zero thinking config, errors, MCP discovery, function calling, tool errors, transcript logging, and hydration.

### Task 5: Application Wiring & Example Configuration
- File: `brain/main.go`, `config.example.yaml`, `brain/main_test.go`
- Factory switch in `brain/main.go` selecting between `agy` and `gemini_api` based on `cfg.VoiceEngine()`.
- Helper `extractVoiceMCPServers` extracting `scheduler`, `ha-mcp` (from `Common` and `Voice`), and custom voice servers.
- **Fail-Safe Fallback**: If `gemini_api` is selected but no API key is configured (`harness_api_key` and `api_key` are both empty), log a warning and fall back to `UnifiedProcessPool` (`agy`).
- Document `voice.engine` and partitioned `mcp_servers` (`voice:`, `common:`) in `config.example.yaml`.
- Update `main_test.go` with tests for engine configurations and voice MCP extraction.

### Task 6: Pre-flight Verification & Diff Review
- Run `./scripts/verify.sh --staged` and package test sweeps.
- Consolidated Girl Gang / Devil's Advocate diff review.
- Create `PR_DESCRIPTION.md` and push updates to PR branch.

