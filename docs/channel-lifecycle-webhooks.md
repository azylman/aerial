# Channel Lifecycle Webhooks

Channel lifecycle webhooks allow external services or sidecars to inspect, gate, or enrich execution turns across Discord channels. Webhooks are configured declaratively per-channel in `aerial-config` (`config.yaml`).

## Configuration Format

In `config.yaml`, lifecycle hooks are specified under `channels.<channel_name>.hooks`:

```yaml
channels:
  general:
    hooks:
      on_wake:
        url: "http://filter-sidecar.default.svc.cluster.local:8080/on-wake"
        timeout_ms: 1000
        on_timeout: "classify"
      pre_turn:
        url: "http://policy-sidecar.default.svc.cluster.local:8080/pre-turn"
        timeout_ms: 2000
        on_timeout: "drop"
      post_turn:
        url: "http://audit-sidecar.default.svc.cluster.local:8080/post-turn"
        timeout_ms: 3000
        on_timeout: "proceed"
```

### Common Hook Properties

- `url` (string, required): Full HTTP(S) endpoint of the external hook service.
- `timeout_ms` (integer, optional): Request timeout in milliseconds (default: 3000 ms).
- `on_timeout` (string, optional): Action taken when the webhook request times out or encounters any error (such as network disconnect, non-2xx HTTP response, or invalid JSON response).

---

## 1. `on_wake` Hook

Evaluated during inbound message processing to determine whether an incoming message wakes Aerial or gets classified.

### Request Payload (`WakeRequest`)

Sent via `POST` with `X-Aerial-Event: on_wake` header:

```json
{
  "channel_id": "123456789012345678",
  "thread_id": "123456789012345678",
  "message": {
    "id": "987654321098765432",
    "thread_id": "123456789012345678",
    "guild_id": "111222333444555666",
    "author_id": "222333444555666777",
    "author_name": "Alex",
    "content": "Hey Aerial, check this out",
    "status": "PENDING",
    "retry_count": 0,
    "created_at": "2026-11-24T22:00:00Z",
    "updated_at": "2026-11-24T22:00:00Z"
  },
  "timestamp": "2026-11-24T22:00:00Z"
}
```

- `channel_id` (string): Effective channel snowflake ID.
- `thread_id` (string): Thread snowflake ID.
- `message` (object): The full `db.Message` object representing the incoming message triggering the wake check.
- `timestamp` (string): ISO 8601 UTC timestamp.

### Response Payload (`WakeResponse`)

```json
{
  "override": "wake",
  "reason": "high priority alert detected"
}
```

- `override` (string, optional): Routing override: `"wake"`, `"drop"`, or `"classify"`.
- `reason` (string, optional): Explanatory reason string for telemetry and logging.

### Error and Timeout Behavior

- If the hook request fails (timeout, network error, non-2xx status, or JSON parse error) or if the response returns an empty `override`, Aerial uses the configured `on_timeout` action (default: `"classify"`, or `"wake"`, `"drop"`).

---

## 2. `pre_turn` Hook

Executed immediately before the turn runner starts an agent turn. Allows gating execution, rescheduling turns, or injecting coordination context into the agent prompt.

### Request Payload (`PreTurnRequest`)

Sent via `POST` with `X-Aerial-Event: pre_turn` header:

```json
{
  "channel_id": "123456789012345678",
  "thread_id": "123456789012345678",
  "burst_count": 1,
  "messages": [
    {
      "id": "987654321098765432",
      "author_id": "222333444555666777",
      "author_name": "Alex",
      "content": "Investigate latency on calibarn"
    }
  ],
  "prompt": "Investigate latency on calibarn",
  "retry_count": 0,
  "timestamp": "2026-11-24T22:00:00Z"
}
```

- `channel_id` (string): Effective channel snowflake ID.
- `thread_id` (string): Thread snowflake ID.
- `burst_count` (integer): Number of coalesced messages in the active burst.
- `messages` (array): Array of coalesced message objects.
- `prompt` (string): Coalesced prompt text prepared for turn execution.
- `retry_count` (integer): Maximum retry count across burst messages.
- `timestamp` (string): ISO 8601 UTC timestamp.

### Response Payload (`PreTurnResponse`)

```json
{
  "allow": true,
  "action": "proceed",
  "retry_after_seconds": 0,
  "injected_context": "System notice: calibarn GPU load is elevated.",
  "metadata": {
    "policy_version": "v2"
  },
  "reason": "gate passed"
}
```

- `allow` (boolean, required): Whether to allow the turn to proceed.
  - If `allow` is `true`, the turn proceeds regardless of `action`.
  - If `allow` is `false`, execution is halted:
    - If `action` is `"retry"`, the turn is rescheduled after `retry_after_seconds` (or 0 if unset).
    - For any other `action` value (such as `"drop"` or omitted), the turn is aborted with reason `pre_turn_rejected: <reason>`.
- `action` (string, optional): `"retry"` or `"drop"` (only evaluated when `allow` is `false`).
- `retry_after_seconds` (integer, optional): When `allow` is `false` and `action` is `"retry"`, delay in seconds before rescheduling.
- `injected_context` (string, optional): Context string wrapped in `<COORDINATION_CONTEXT>` and prepended to the agent prompt.
- `metadata` (object, optional): Arbitrary key-value metadata attached to the turn and forwarded to `post_turn`.
- `reason` (string, optional): Human-readable rejection or retry reason.

### Error and Timeout Behavior

- If the hook request fails (timeout, network error, non-2xx status, or JSON parse error), Aerial falls back to the configured `on_timeout` action:
  - `"drop"` (default): Aborts the turn with `pre_turn_timeout_drop`.
  - `"retry"`: Reschedules the turn with a 5-second delay.
  - `"proceed"`: Allows the turn to proceed normally.

---

## 3. `post_turn` Hook

Executed asynchronously after an agent turn completes. Useful for auditing, telemetry, indexing, or downstream notification.

### Request Payload (`PostTurnRequest`)

Sent via `POST` with `X-Aerial-Event: post_turn` header:

```json
{
  "channel_id": "123456789012345678",
  "thread_id": "123456789012345678",
  "status": "success",
  "response_text": "Investigated GPU metrics on calibarn; load is normal.",
  "error": "",
  "duration_ms": 3450,
  "token_usage": {
    "input_tokens": 12500,
    "output_tokens": 420,
    "thinking_tokens": 85,
    "cache_read_tokens": 8200,
    "total_tokens": 13005
  },
  "metadata": {
    "policy_version": "v2"
  },
  "timestamp": "2026-11-24T22:00:05Z"
}
```

- `channel_id` (string): Effective channel snowflake ID.
- `thread_id` (string): Thread snowflake ID.
- `status` (string): Turn execution status (`"success"`, `"failed"`, or `"timeout"`).
- `response_text` (string, optional): Generated response text delivered to the channel.
- `error` (string, optional): Error description if the turn failed.
- `duration_ms` (int64): Turn duration in milliseconds.
- `token_usage` (object): Token consumption statistics for the turn.
- `metadata` (object, optional): Hook metadata forwarded from `pre_turn`.
- `timestamp` (string): ISO 8601 UTC timestamp.

### Response Payload (`PostTurnResponse`)

```json
{
  "acknowledged": true,
  "metadata": {}
}
```

- `acknowledged` (boolean): Confirmation that the telemetry was received.
- `metadata` (object, optional): Optional state metadata.

### Error and Timeout Behavior

- The `post_turn` hook is fire-and-forget telemetry: any error (timeout, non-2xx status, network error) is logged as a warning and recorded in metrics. It does not alter the completed turn outcome, and `on_timeout` is ignored.
