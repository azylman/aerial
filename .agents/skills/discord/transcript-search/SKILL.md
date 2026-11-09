---
name: transcript-search
description: >-
  Search, discover, and analyze historical agent execution transcripts, previous conversation turns, debugging sessions, past solutions, and past command or tool outputs using the transcript-search CLI. Activate whenever asked "did we solve this before?", "check past sessions for X", "what was the error in previous turns", or when investigating prior implementation details, past PRs, or tool outputs across historical conversations.
---

# Historical Transcript & Session Search Skill

This skill defines operational procedures and guidelines for querying Aerial's persistent transcript index. Aerial automatically syncs and indexes historical transcripts and tool execution steps into PostgreSQL with hybrid semantic vector embeddings and full-text search (BM25 / tsvector).

The index is **shared across every bot (stack)** and every engine (agy and Claude Code). Each bot only writes its own sessions, but **searches, `get`, and `stats` cover every bot's sessions by default**. Pass `--stack <name>` to narrow results to one bot.

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
transcript-search get --session <id> --step <N> [options]
transcript-search stats [options]
```

### Search Subcommand Flags
- `-q, --query <string>`: The query text or question to search. Can also be passed as positional arguments.
- `-m, --mode <auto|sessions|steps>`:
  - `auto` (default): Searches both high-level session summaries and individual tool execution steps.
  - `sessions`: Dense vector cosine similarity + BM25 full-text search fused with 70/30 weighted Reciprocal Rank Fusion (RRF) over session summaries. Best for conceptual, architectural, or multi-turn conversational queries.
  - `steps`: Exact full-text search with `ts_rank_cd` over granular tool calls and agent dialogue. Best for error messages, specific commands, or code identifiers.
- `-t, --tool <kind|name>`: Filter steps by normalized tool kind (works across engines) or by a raw engine-native tool name. See the vocabulary below.
- `--stack <name>`: Restrict results to one bot's sessions (default: all stacks).
- `-s, --session <id>`: Restrict search results to a specific session / conversation ID.
- `-n, --limit <int>`: Maximum number of records to return (default: `10`).
- `--json`: Output structured JSON for piping into `jq`.
- `--url <url>`: Override the Aerial eri daemon HTTP endpoint (default: `http://localhost:8080` or `$ERI_URL`).

### Get Subcommand Flags (Step-Window Zoom)
- `-s, --session <id>`: Session / conversation ID to inspect (required).
- `-i, --step <int>`: Target step index to zoom into (required, non-negative).
- `-C, --context <int>`: Number of surrounding context steps before and after the target step (default: `2`, range: 0–20).
- `--json`: Output raw JSON containing `{session_id, step, context, steps: [...]}`.
- `--stack <name>`: Only return steps from that stack (default: any stack; the same session ID can exist on more than one stack).
- `--url <url>`: Override the Aerial eri daemon HTTP endpoint (default: `http://localhost:8080` or `$ERI_URL`).

### Stats Subcommand
Prints total indexed sessions and steps plus a per-stack breakdown. `--stack <name>` limits the counts to one stack; `--json` emits `{total_sessions, total_steps, stacks: [{stack, sessions, steps}], status}`.

### Tool Kind Vocabulary (`--tool`)
Every step stores its raw `tool_name` plus a normalized `tool_kind`. A `--tool` value that is a known kind matches `tool_kind`; anything else matches the raw `tool_name` exactly.
- `shell`: `Bash`, `BashOutput`, `KillShell`, `run_command`, `send_command_input`, `read_terminal`
- `read`: `Read`, `NotebookRead`, `view_file`, `view_file_outline`, `view_code_item`, `list_dir`
- `edit`: `Edit`, `MultiEdit`, `NotebookEdit`, `edit_file`, `replace_file_content`, `multi_replace_file_content`
- `write`: `Write`, `write_to_file`
- `search`: `Grep`, `Glob`, `grep_search`, `find_by_name`, `codebase_search`
- `web`: `WebFetch`, `WebSearch`, `read_url_content`, `search_web`, `browser_*`
- `subagent`: `Agent`, `Task`, `invoke_subagent`, `manage_subagents`
- `skill`: `Skill`
- `task`: `TodoWrite`
- `mcp:<server>`: any MCP tool of that server (`mcp__github__create_pull_request`, `mcp_github_create_pull_request` → `mcp:github`)
- `mcp`: on its own, every MCP tool from any server
- `other`: any unrecognized tool

Examples: `--tool shell` finds both Claude `Bash` and agy `run_command` steps; `--tool mcp:github` finds GitHub MCP calls from either engine; `--tool mcp` finds every MCP call from any server; `--tool Bash` still matches only raw `Bash` steps.

### Result Labels
Every session and step result is prefixed with a `[stack/engine]` label showing which bot and engine wrote it (e.g. `[aerial/claude]`, `[aerial/agy]`); an unknown part prints as `?`. Step badges show `tool_name · tool_kind`, e.g. `- [aerial/agy] **[run_command · shell]** Step #5 in [d3a1a377...](conversation://...)`.

---

## 3. Recommended Search & Zoom Workflows

### Two-Phase Workflow: Search -> Step-Window Zoom
Never scrape or grep raw `transcript.jsonl` files on disk when investigating historical sessions. Instead, follow the two-phase indexed search and zoom protocol:

1. **Phase 1: Discover candidate steps with `transcript-search search`**:
   ```bash
   transcript-search search "atlas migration validate" --mode steps -n 5
   ```
   Identify the target `session_id` and `step_index` from the search results snippet.

2. **Phase 2: Zoom into untruncated step window with `transcript-search get`**:
   ```bash
   transcript-search get --session "d3a1a377-e199-426d-8588-fc3083131264" --step 14 -C 2
   ```
   This retrieves the centered target step (marked with `▶`) along with 2 preceding and 2 succeeding execution steps, displaying full untruncated content, tool outputs, and exact timestamps.

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
Or filter specifically to shell executions from any engine:
```bash
transcript-search search "docker compose restart" --tool shell -n 5
```
Or to file edits on one bot only:
```bash
transcript-search search "schema.sql" --tool edit --stack <stack> -n 5
```

### Scenario C: Filtering Within a Known Session
When inspecting a known historical session for a specific step:
```bash
transcript-search search "verify.sh --staged" --session "d3a1a377-e199-426d-8588-fc3083131264"
```

### Scenario D: Programmatic Traversal via JSON
```bash
transcript-search search "atlas migration validate" --json | jq '.steps[] | {stack, engine, step: .step_index, tool: .tool_kind, content: .content[:120]}'
```

---

## 4. Invariants & Best Practices

- **Zero Arbitrary Transcript Scrapes**: Avoid running heavy `find` or `grep` loops against `/data/runtimes/discord/.gemini/antigravity-cli/brain/*/transcript.jsonl`. Always prefer `transcript-search` for fast, indexed queries and step-window zoom (`transcript-search get`).
- **Link Navigation**: Results include `[conv_id](conversation://<conv_id>)` links. Use these IDs directly if you need to read the full transcript file on disk (`<appDataDir>/brain/<conv_id>/.system_generated/logs/transcript.jsonl`).
- **HTTP Only**: `transcript-search` talks only to the local eri daemon over HTTP (`/api/transcripts/search`, `/steps`, `/stats`). If eri is restarting, the command fails; retry shortly instead of querying PostgreSQL directly.
- **Cross-Bot Sessions**: A session labelled with another stack lives on that bot's disk, so its raw transcript file is not available locally; use `transcript-search get --stack <stack>` to read its steps.
