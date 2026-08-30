# Design Spec: Semantic Memory Context Injection & Startup Backfill

**Date**: 2026-08-30  
**Status**: In Implementation  
**Author**: Aerial AI  

---

## 1. Problem Statement

Aerial's semantic memory subsystem consists of two operational halves:
1. **Extraction**: Periodic background fact extraction from transcripts into SQLite (`facts` and `fact_embeddings`), embedded via Ollama (`all-minilm:latest`).
2. **Retrieval & Injection**: Dynamic vector similarity retrieval of top-$N$ relevant facts and injection into prompt context.

While extraction has been actively operating and storing facts in SQLite, **context retrieval (`RetrieveRelevantFacts`) was never wired into the prompt execution pipeline (`pkg/queue/queue.go`)**. Consequently, prompts sent to the headless `agy` CLI runner lack any `<retrieved_memory>` blocks in conversation transcripts. Furthermore, fact extraction only runs after 120 ticks (1 hour) of uninterrupted execution, causing delays after restarts.

---

## 2. Architectural Design

### 2.1 Query Text Extraction (`pkg/memory`)

When messages originate from the Discord Gateway funnel, `msg.Content` is structured with Discord metadata:
```markdown
<USER_REQUEST>
Here's a message someone sent you from Discord:

- id: 1543706746294509568
- channel_id: 1542423172400291873
- thread_id: 1543706746294509568
- content: What office am I working from on Thursdays?
...
</USER_REQUEST>
```

Passing raw metadata into vector embedding models pollutes the query vector with ID strings and boilerplate.
We implement `ExtractQueryText(content string) string`:
- If `content` matches `- content:\s*(.+)`, extracts the exact Discord user utterance.
- If `content` is wrapped in `<USER_REQUEST>...</USER_REQUEST>`, extracts inner text.
- If raw prompt, strips whitespace and returns up to 1000 characters.

### 2.2 Dynamic Prompt Augmentation in Worker Pool (`pkg/queue`)

In `WorkerPoolConfig`:
- Add `MemoryClient *memory.Client` (defaults to `memory.NewClient("")`).
- Add `MemoryRetriever func(ctx context.Context, database *sql.DB, client *memory.Client, queryText string, maxFacts int) ([]db.Fact, error)` (defaults to `memory.RetrieveRelevantFacts`).

In `processMessage(msg db.Message)`:
1. Extract query text: `query := memory.ExtractQueryText(msg.Content)`
2. Query facts: `facts, err := p.cfg.MemoryRetriever(runCtx, p.cfg.DB, p.cfg.MemoryClient, query, 10)`
3. If `len(facts) > 0`:
   - Generate block: `memoryBlock := memory.FormatMemoryContext(facts)`
   - Prepend to prompt: `prompt = memoryBlock + "\n\n" + msg.Content`
4. If retrieval fails or times out:
   - Gracefully log warning and fall back to unmodified `msg.Content`.

### 2.3 Immediate Startup Fact Extraction (`pkg/scheduler`)

In `scheduler.Run()`:
- In addition to the hourly ticker (`tickCount%120 == 0`), launch an initial background goroutine running `memory.ExtractActiveConversationFacts(ctx, database, ollamaClient, llmFunc, 12)` immediately on startup to process any pending un-extracted conversations.

---

## 3. Invariants & Safety

1. **Zero Personal Data Invariant**: All test fixtures, mock data, and comments must use generic identifiers (`UserA`, `Test fact`, `12345`).
2. **Resilience & LKGC**: If Ollama is offline or slow (>1s timeout), message execution must NEVER fail; it proceeds cleanly without injected context.
3. **No Redundant DB Mutation**: `messages.content` in SQLite preserves the pristine incoming message, while the augmented prompt is passed to the runner.
