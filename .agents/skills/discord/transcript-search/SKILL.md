---
name: transcript-search
description: >-
  Search, discover, and analyze historical agent execution transcripts, previous conversation turns, debugging sessions, past solutions, and past command or tool outputs using the transcript-search CLI. Activate whenever asked "did we solve this before?", "check past sessions for X", "what was the error in previous turns", or when investigating prior implementation details, past PRs, or tool outputs across historical conversations.
---

# Historical Transcript & Session Search Skill

This skill defines operational procedures and guidelines for querying Aerial's persistent transcript index. Aerial automatically syncs and indexes historical transcripts and tool execution steps into PostgreSQL with hybrid semantic vector embeddings and full-text search (BM25 / tsvector).

---

## 1. When to Use

Activate this skill when:
- The user asks about previous tasks, PRs, or conversations (e.g., *"Have we seen this error before?"*, *"How did we fix the vector collector last week?"*, *"What was the output of the benchmark in that other thread?"*).
- You need prior implementation patterns, past debugging root causes, or architectural decisions from historical sessions.
- You need to locate specific tool calls (e.g., shell commands, Docker operations, file edits) executed in previous turns.
- You want to inspect session summaries or index volume via `transcript-search stats`.

---

## 2. CLI Tool Reference (`transcript-search`)

Aerial installs the native Go `transcript-search` binary at `/usr/local/bin/transcript-search`.

### Basic Syntax
```bash
transcript-search search "<query>" [options]
transcript-search stats [options]
```

### Search Subcommand Flags
- `-q, --query <string>`: The query text or question to search. Can also be passed as positional arguments.
- `-m, --mode <auto|sessions|steps>`:
  - `auto` (default): Searches both high-level session summaries and individual tool execution steps.
  - `sessions`: Dense vector cosine similarity + BM25 full-text search fused with 50/50 Reciprocal Rank Fusion (RRF) over session summaries. Best for conceptual, architectural, or multi-turn conversational queries.
  - `steps`: Exact full-text search with `ts_rank_cd` over granular tool calls and agent dialogue. Best for error messages, specific commands, or code identifiers.
- `-t, --tool <name>`: Filter steps by tool name (e.g. `run_command`, `view_file`, `replace_file_content`, `mcp_github_create_pull_request`).
- `-s, --session <id>`: Restrict search results to a specific session / conversation ID.
- `-n, --limit <int>`: Maximum number of records to return (default: `10`).
- `--json`: Output structured JSON for piping into `jq`.
- `--url <url>`: Override the Aerial brain daemon HTTP endpoint (default: `http://localhost:8080` or `$AERIAL_BRAIN_URL`).
- `--direct-db`: Bypass HTTP daemon and query PostgreSQL directly via `$POSTGRES_URL`.

---

## 3. Recommended Search Workflows

### Scenario A: Architectural / High-Level Historical Solutions
When looking for how a feature was designed or how a past incident was resolved:
```bash
transcript-search search "gemini 429 quota exhaustion backoff" --mode sessions -n 5
```
*Tip:* Read the returned session summaries to identify relevant conversation IDs.

### Scenario B: Pinpointing a Specific Error or Command Output
When hunting for an exact log line, shell command, or stack trace:
```bash
transcript-search search "SASL auth failed for user aerial" --mode steps -n 5
```
Or filter specifically to shell executions:
```bash
transcript-search search "docker compose restart" --tool run_command -n 5
```

### Scenario C: Filtering Within a Known Session
When inspecting a known historical session for a specific step:
```bash
transcript-search search "verify.sh --staged" --session "d3a1a377-e199-426d-8588-fc3083131264"
```

### Scenario D: Programmatic Traversal via JSON
```bash
transcript-search search "atlas migration validate" --json | jq '.steps[] | {step: .step_index, content: .content[:120]}'
```

---

## 4. Invariants & Best Practices

- **Zero Arbitrary Transcript Scrapes**: Avoid running heavy `find` or `grep` loops against `/data/runtimes/discord/.gemini/antigravity-cli/brain/*/transcript.jsonl`. Always prefer `transcript-search` for fast, indexed queries.
- **Link Navigation**: Results include `[conv_id](conversation://<conv_id>)` links. Use these IDs directly if you need to read the full transcript file on disk (`<appDataDir>/brain/<conv_id>/.system_generated/logs/transcript.jsonl`).
- **Daemon-First Resilience**: `transcript-search` communicates with the local `aerial-brain` daemon over HTTP (`/api/transcripts/search`). If the daemon is temporarily unavailable or restarting, it automatically falls back to direct PostgreSQL queries.
