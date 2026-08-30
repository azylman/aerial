# Design Spec: Semantic Memory Retrieval Calibration & Query Optimization

**Date**: 2026-08-30  
**Status**: In Implementation  
**Author**: Aerial AI  

---

## 1. Problem Statement

Following the initial rollout of prompt memory injection, semantic facts were not being retrieved or injected for incoming user queries. Root cause analysis revealed three specific issues:
1. **Query Prefix Vector Distortion**: `ollama.Client.GenerateEmbedding` prepended `BGEQueryPrefix` (`"Represent this sentence for searching relevant passages: "`) to queries. Because Aerial runs `all-minilm:latest` (`sentence-transformers/all-MiniLM-L6-v2`), which is a symmetric sentence embedding model, the prefix diluted semantic embeddings and severely reduced similarity scores to relevant stored facts.
2. **Overly Restrictive Score Threshold**: `DefaultMinScoreThreshold` was set to `0.45`. For sentence-transformer cosine similarity across short natural questions and statement facts (multiplied by fact importance <= 1.0), relevant matches typically score between 0.20 and 0.60. The 0.45 threshold caused false negatives on over 90% of relevant queries.
3. **Discord Mention Noise**: Incoming user messages contain snowflake mentions like `<@1542035925603713086>`, which contaminated the query text when passed to the embedding model.
4. **Missing Embeddings for Legacy Facts**: Certain existing facts in SQLite had `NULL` or empty embedding BLOBs.

---

## 2. Proposed Changes

### 2.1 Clean Query Embeddings (`pkg/memory/ollama.go`)
- Remove hardcoded `BGEQueryPrefix` injection for symmetric embedding models (`all-minilm`). Pass the clean text directly to `/api/embeddings`.

### 2.2 Calibrate Retrieval Score Threshold (`pkg/memory/search.go`)
- Update `DefaultMinScoreThreshold` from `0.45` to `0.20`.
- Update `ExtractQueryText(content string) string` to strip Discord user/role/channel mentions (`<@!?[0-9]+>`, `<@&[0-9]+>`, `<#[0-9]+>`) and normalize whitespace.

### 2.3 Automatic Embedding Backfill on Startup (`pkg/memory/extractor.go`)
- Add `BackfillMissingEmbeddings(ctx context.Context, database *sql.DB, client *Client) error`.
- In `scheduler.Run()`, invoke `BackfillMissingEmbeddings` alongside startup fact extraction to repair any un-embedded facts.

---

## 3. Invariants & Safety

1. **Zero Personal Data Invariant**: All test fixtures and code remain 100% generic with synthetic IDs and identifiers.
2. **Resilience**: If embedding generation fails or Ollama is unreachable, retrieval falls back gracefully to empty facts without interrupting turn execution.
3. **Deterministic Testing**: Unit tests verify exact threshold ranking and mention sanitization.
