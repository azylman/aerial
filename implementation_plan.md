# Implementation Plan: Eliminate P0 Regex Parsing of JSON & Structured Payloads

## Problem Statement
The codebase contains brittle regular expressions for parsing structured JSON and event payloads:
1. `brain/pkg/runner/pure.go` and `runner.go` use `reNDJSONInitSession` (`"event"\s*:\s*"init"[^}]*"conversation_id"\s*:\s*"([^"]+)"`) to extract conversation IDs from raw log lines.
2. `brain/pkg/runner/daemon.go` uses `reSubagentStarted` (`"conversationId":\s*"([^"]+)"`) to extract subagent IDs from `invoke_subagent` tool outputs.
3. `brain/pkg/runner/daemon.go` and `brain/pkg/session/session.go` use regexes (`reBackgroundTaskStarted`, `reTaskMessageSender`, `reTaskFinishedContent`) to scrape task and sender IDs from output strings.

This pattern is brittle, vulnerable to ordering or formatting changes, prone to self-poisoning when command outputs or test logs quote task signatures, and incurs unnecessary regex engine overhead.

## Architecture & Replacements (Approved by The Girl Gang Review Panel)

### 1. Centralized Task Parsers in `brain/pkg/session/task_parsers.go`
Rather than duplicating parsers across `runner` and `session`, define pure, deterministic parsers in `session` (which `runner` already imports):
- `ParseBackgroundTaskStarted(s string) string`:
  - Case-insensitive search for `"tool is running as a background task with task id:"`.
  - Extracts the subsequent non-whitespace token, strips quotes and punctuation, returns `strings.Clone(token)`.
- `ParseTaskMessageSender(s string) string`:
  - Case-insensitive search for `"sender="`.
  - Extracts the subsequent non-whitespace token, strips quotes and punctuation, returns `strings.Clone(token)`.
- `ParseTaskFinishedContent(s string) string`:
  - Finds `"task id"` and verifies `"finished with result:"` appears subsequently.
  - Slices intermediate substring; verifies it is a SINGLE non-whitespace token (rejects any internal whitespace).
  - Trims quotes and whitespace, returns `strings.Clone(token)`.

### 2. `brain/pkg/runner/pure.go` & `runner.go` (`ParseInitEvent` and `ExtractSessionID`)
- Remove `reNDJSONInitSession = regexp.MustCompile(...)` completely from `runner.go`.
- Implement robust JSON unmarshaling into `sessionProbe`:
  - Fast path: Direct `json.Unmarshal([]byte(trimmed), &probe)` if string starts with `{` and ends with `}`.
  - Embedded JSON fallback: Locate candidate opening brace `idx := strings.Index(line, `{"event"`)` or `json.NewDecoder` stream decoding to avoid brace contamination from logger prefixes like `[2026-09-19 {thread-1}]`.
  - Validate candidate UUID using `IsValidUUID`.

### 3. `brain/pkg/runner/daemon.go`
- Implement `extractSubagentID(toolOut string) string`:
  - Handles direct JSON objects, array payloads `[...]`, and tool output with text prefixes.
  - Deserializes into struct supporting both `conversationId` and `conversation_id`.
  - Returns `strings.Clone(id)`.
- Replace regex calls with `session.ParseBackgroundTaskStarted`, `session.ParseTaskMessageSender`, and `session.ParseTaskFinishedContent`.
- Remove all 4 regexes (`reBackgroundTaskStarted`, `reSubagentStarted`, `reTaskMessageSender`, `reTaskFinishedContent`) and the `"regexp"` import from `daemon.go`.

### 4. `brain/pkg/session/session.go`
- Update `HasUnfinishedBackgroundTask` to use `ParseBackgroundTaskStarted`, `ParseTaskMessageSender`, and `ParseTaskFinishedContent`.
- Remove all 3 regexes and the `"regexp"` import from `session.go`.

### 5. Verification & TDD
- Write tests in `brain/pkg/session/task_parsers_test.go`:
  - Task started: quotes, whitespace, punctuation, newlines, case insensitivity.
  - Task sender: quotes, whitespace, priority attributes.
  - Task finished: quotes, prefix matching, non-whitespace constraint (reject internal spaces).
- Update and add tests in `brain/pkg/runner/pure_test.go` and `brain/pkg/runner/runner_test.go`:
  - `ParseInitEvent` with JSON prefixes, logger tags, invalid JSON, and valid UUIDs.
  - `extractSubagentID` with camelCase, snake_case, array payloads, and leading text.
- Verify full test suite passes with `go test -count=1 ./pkg/runner/... ./pkg/session/...`.
