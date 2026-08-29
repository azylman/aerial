# Design Spec: Semantic Memory RAG System

**Date**: 2026-08-28  
**Status**: Approved / Refined  
**Author**: Aerial AI  

---

## 1. Overview & Objectives

The goal of this feature is to equip Aerial with a persistent, vector-search powered semantic memory system (RAG). Aerial will periodically extract atomic facts (user preferences, system configurations, recurring tasks, operational state) from active conversation transcripts, convert them into vector embeddings using a local Ollama service (`bge-small-en`), store them in SQLite, and dynamically inject the most relevant facts into incoming prompt contexts.

---

## 2. Key Architectural Decisions

1. **Pre-Baked Ollama Docker Container**:
   - Ollama runs in a custom container image (`docker/ollama/Dockerfile`).
   - The vector embedding model (`bge-small-en`) is pulled at Docker build time using a health-check readiness loop.
   - Aerial's `brain` service requires zero startup model downloads or warm-up calls.

2. **Hourly Single-Flight Batch Extraction**:
   - A background cron worker runs every hour (`0 * * * *`).
   - Protected by a Go single-flight mutex lock (`sync/mutex`) to prevent overlapping execution.
   - Scopes to conversations active in the last 12 hours where `last_fact_extracted_at < updated_at`.
   - Tracks `last_fact_extracted_at DATETIME` on conversations to avoid duplicate processing.

3. **Number-of-Facts Limit & Non-Blocking Timeout**:
   - Context injection limits top $N$ facts (`MEMORY_MAX_FACTS`, default: `10`).
   - Pre-message retrieval enforces a strict **800ms context timeout**. If Ollama is slow/offline, Aerial falls back gracefully without injected context.

4. **Pure Go Vector Search & BGE Instruction Formatting**:
   - Calculates dot product over normalized $L_2$ `float32` embedding arrays in pure Go.
   - Asymmetric query embeddings prepend the BGE instruction prefix: `Represent this sentence for searching relevant passages:`.

5. **Fact Rot Protection (Superseding)**:
   - Facts include an `is_active BOOLEAN DEFAULT 1` column.
   - When extracting new facts, vector similarity $> 0.85$ against older facts of the same category triggers an update marking outdated facts as inactive.

---

## 3. Container & Integration Architecture

### 3.1 Custom Ollama Dockerfile (`docker/ollama/Dockerfile`)

```dockerfile
FROM ollama/ollama:latest

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Start Ollama in background and poll health check until ready before pulling model
RUN ollama serve & \
    until curl -s http://localhost:11434/api/tags > /dev/null; do sleep 1; done && \
    ollama pull bge-small-en

EXPOSE 11434
```

### 3.2 Docker Compose Integration (`docker-compose.yml`)

```yaml
services:
  brain:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: aerial-brain
    restart: unless-stopped
    environment:
      - OLLAMA_URL=http://ollama:11434
      - MEMORY_MAX_FACTS=10
      - MEMORY_EXTRACTION_CRON=0 * * * *
    depends_on:
      - ollama
    networks:
      - aerial-net

  ollama:
    build:
      context: ./docker/ollama
      dockerfile: Dockerfile
    container_name: aerial-ollama
    restart: unless-stopped
    volumes:
      - ollama_data:/root/.ollama
    networks:
      - aerial-net

volumes:
  ollama_data:
```

---

## 4. Database Schema (`pkg/db`)

### 4.1 Schema Migration

```sql
-- Track last fact extraction timestamp on conversations
ALTER TABLE conversations ADD COLUMN last_fact_extracted_at DATETIME;

-- Table to store atomic facts
CREATE TABLE IF NOT EXISTS facts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT NOT NULL,           -- e.g. 'user_preference', 'system_config', 'project_state'
    fact_text TEXT NOT NULL,          -- Atomic fact statement (1-2 sentences)
    importance_score REAL DEFAULT 1.0,-- Weight multiplier (0.1 - 1.0)
    is_active INTEGER DEFAULT 1,      -- 1 = Active, 0 = Superseded / Inactive
    conversation_id TEXT,             -- Originating conversation ID
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Table to store vector embeddings for facts
CREATE TABLE IF NOT EXISTS fact_embeddings (
    fact_id INTEGER NOT NULL REFERENCES facts(id) ON DELETE CASCADE,
    embedding BLOB NOT NULL,          -- Binary float32 slice array (384 dimensions)
    PRIMARY KEY (fact_id)
);

CREATE INDEX IF NOT EXISTS idx_facts_active_cat ON facts(is_active, category);
CREATE INDEX IF NOT EXISTS idx_facts_conversation ON facts(conversation_id);
```

---

## 5. Fact Extraction Pipeline (`pkg/memory`)

1. **Hourly Worker Execution**:
   - The cron scheduler triggers `ExtractActiveConversationFacts()` under a `sync/mutex` lock.
   - Query target:
     ```sql
     SELECT id, updated_at FROM conversations 
     WHERE updated_at >= datetime('now', '-12 hours') 
       AND (last_fact_extracted_at IS NULL OR last_fact_extracted_at < updated_at);
     ```

2. **LLM Fact Extraction**:
   - Sends transcript to primary LLM with structured fact extraction prompt.
   - Returns atomic facts:
     ```json
     {
       "facts": [
         {
           "category": "user_preference",
           "fact_text": "Arcane prefers Pacific Time (America/Los_Angeles)",
           "importance_score": 1.0
         }
       ]
     }
     ```

3. **Embedding & Superseding Workflow**:
   - Call Ollama POST `http://ollama:11434/api/embeddings` (`model: "bge-small-en"`).
   - Compute vector similarity against existing active facts in the same category. If similarity $> 0.85$, update existing fact `is_active = 0`.
   - Store new fact and embedding in SQLite transaction.
   - Update `conversations.last_fact_extracted_at = CURRENT_TIMESTAMP`.

---

## 6. Context Retrieval & Prompt Injection (`pkg/memory`)

1. **Query Embedding**:
   - Call Ollama POST `/api/embeddings` with prompt:  
     `Represent this sentence for searching relevant passages: <user_message>`
   - Executed within an 800ms `context.WithTimeout`.

2. **Vector Ranking**:
   - Query `facts` where `is_active = 1`.
   - Compute dot product against query vector in pure Go:
     $$\text{Score} = \sum_{i=1}^{384} (Q_i \cdot F_i) \times \text{importance\_score}$$
   - Filter Score $> 0.45$, pick Top $N$ (`MEMORY_MAX_FACTS`).

3. **Prompt Injection**:
   - Prepend matching facts under `<retrieved_memory>` block in prompt context:
     ```markdown
     <retrieved_memory>
     - [user_preference] Arcane prefers Pacific Time (America/Los_Angeles)
     - [system_config] Home Assistant runs at http://homeassistant:8123
     </retrieved_memory>
     ```

---

## 7. Next Steps in Workflow

1. Human review of updated design spec (`docs/specs/2026-08-28-semantic-memory-rag.md`).
2. Write modular package code (`pkg/memory`, schema migrations, Dockerfile).
3. Senior Code Review & Verification.
4. Pre-commit Docker build verification gate & deployment.
