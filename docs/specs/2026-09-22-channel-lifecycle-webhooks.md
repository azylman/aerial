# Channel Lifecycle Webhook Interceptors Specification

## Overview
Channel lifecycle webhook interceptors provide a general-purpose harness extension mechanism in Aerial. They allow external services, sidecars, or domain bots to programmatically override wake triage, gate execution, inject runtime context, and capture turn telemetry.

Channels define HTTP lifecycle hooks under the `hooks:` block in `config.yaml`:
```yaml
channels:
  general:
    hooks:
      on_wake:
        url: "http://my-sidecar:8000/hooks/on-wake"
        timeout_ms: 1500
        on_timeout: "classify"
      pre_turn:
        url: "http://my-sidecar:8000/hooks/pre-turn"
        timeout_ms: 2000
        on_timeout: "retry"
      post_turn:
        url: "http://my-sidecar:8000/hooks/post-turn"
        timeout_ms: 3000
        on_timeout: "proceed"
```

---

## 1. `on_wake` Hook

### Timing & Purpose
Dispatched upon inbound message arrival at the Discord Gateway event funnel before prompt assembly or ambient classification. Allows external sidecars to inspect sender identity, message content, or channel state and dictate whether the message should wake Aerial, be dropped, or fall back to standard LLM classification.

### HTTP Request
- **Method**: `POST`
- **Headers**: `Content-Type: application/json`
- **Payload (`WakeRequest`)**:
```json
{
  "message_id": "1551984807947538486",
  "channel_id": "1551972052884529154",
  "thread_id": "1551972052884529154",
  "guild_id": "1464358485817954538",
  "author_id": "169260920550195200",
  "author_username": "arcane103",
  "author_global_name": "Arcane",
  "author_bot": false,
  "is_admin": true,
  "content": "Can you verify both files?",
  "mentions": [],
  "mention_user_ids": [],
  "mention_role_ids": [],
  "attachments": [],
  "timestamp": "2026-09-22T15:53:32Z"
}
```

### HTTP Response
- **Status**: `200 OK`
- **Payload (`WakeResponse`)**:
```json
{
  "override": "wake"
}
```
- **Allowed `override` Values**:
  - `"wake"`: Forces immediate execution of a bot turn, bypassing the LLM classifier.
  - `"drop"`: Silently ignores the message without waking the execution brain (ambient transcript logging still occurs).
  - `"classify"`: Yields decision to the canonical ambient relevance classifier (`Gemini 3.8 Flash (Low)`).

### Timeout & Failure Handling
- Controlled by `on_timeout` in hook configuration.
- Defaults to `"classify"` if unspecified or if the endpoint returns a non-2xx status.

---

## 2. `pre_turn` Hook

### Timing & Purpose
Dispatched immediately before prompt execution and agent runner invocation. Allows sidecars to gate execution (e.g. rate-limiting, floor control, pending human intervention) or inject dynamic operational context directly into the prompt.

### HTTP Request
- **Method**: `POST`
- **Headers**: `Content-Type: application/json`
- **Payload (`PreTurnRequest`)**:
```json
{
  "channel_id": "1551972052884529154",
  "thread_id": "1551972052884529154",
  "burst_messages": [
    {
      "id": "1551984807947538486",
      "author": "arcane103",
      "content": "Yes let's just do 2 and 4 first",
      "timestamp": "2026-09-22T15:53:32Z"
    }
  ],
  "prompt": "Here's a message someone sent you from Discord...",
  "retry_count": 0
}
```

### HTTP Response
- **Status**: `200 OK`
- **Payload (`PreTurnResponse`)**:
```json
{
  "allow": true,
  "action": "proceed",
  "retry_after_seconds": 0,
  "injected_context": "Additional coordinator telemetry or dynamic status..."
}
```
- **Execution Semantics**:
  - `allow: false`: Suspends execution. If `action: "retry"`, the turn is re-queued with backoff based on `retry_after_seconds`.
  - `injected_context`: If non-empty, Aerial automatically frames this text inside `<COORDINATION_CONTEXT>` at the top of the turn prompt.

### Timeout & Failure Handling
- Controlled by `on_timeout` in hook configuration.
- Defaults to `"retry"` (protecting against dropped coordination state).

---

## 3. `post_turn` Hook

### Timing & Purpose
Dispatched upon turn completion after final agent response delivery to Discord. Allows telemetry pipelines to track latency, token consumption, and completion health.

### HTTP Request
- **Method**: `POST`
- **Headers**: `Content-Type: application/json`
- **Payload (`PostTurnRequest`)**:
```json
{
  "channel_id": "1551972052884529154",
  "thread_id": "1551972052884529154",
  "status": "success",
  "response_text": "PR created and submitted clean, king.",
  "error_message": "",
  "duration_ms": 4520,
  "token_usage": {
    "prompt_tokens": 12400,
    "completion_tokens": 350,
    "total_tokens": 12750
  }
}
```

### HTTP Response
- **Status**: `200 OK` or `204 No Content`

### Timeout & Failure Handling
- Controlled by `on_timeout` in hook configuration.
- Defaults to `"proceed"` (non-blocking notification).
