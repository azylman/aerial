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
- `timeout_ms` (integer, optional): Request timeout in milliseconds (default: 2000 ms).
- `on_timeout` (string, optional): Action to execute when the webhook request times out or encounters a network error.

---

## 1. `on_wake` Hook

Evaluated during inbound message processing to determine whether an incoming message wakes Aerial or gets classified.

### Request Payload

```json
{
  "channel_id": "123456789012345678",
  "channel_name": "general",
  "message_id": "987654321098765432",
  "author_id": "111222333444555666",
  "author_name": "Alex",
  "content": "Hey Aerial, check this out"
}
```

### Response Payload

```json
{
  "override": "wake"
}
```

- `override` (string, optional): One of `"wake"`, `"drop"`, or `"classify"`.
- **Default `on_timeout`**: `"classify"`.

---

## 2. `pre_turn` Hook

Executed immediately before the turn runner starts an agent turn. Allows gating, rate-limiting, or injecting context into the agent turn.

### Request Payload

```json
{
  "channel_id": "123456789012345678",
  "channel_name": "general",
  "thread_id": "123456789012345678",
  "author_id": "111222333444555666",
  "author_name": "Alex",
  "prompt": "Investigate latency on calibarn"
}
```

### Response Payload

```json
{
  "allow": true,
  "action": "allow",
  "retry_after_seconds": 0,
  "injected_context": "System notice: calibarn GPU load is currently high."
}
```

- `allow` (boolean): `true` to proceed with the turn, `false` to suppress.
- `action` (string, optional): Action to take: `"allow"`, `"drop"`, or `"retry"`.
- `retry_after_seconds` (integer, optional): When `action` is `"retry"`, seconds to wait before retrying the turn.
- `injected_context` (string, optional): Text appended as context to the agent prompt.
- **Default `on_timeout`**: `"drop"`.

---

## 3. `post_turn` Hook

Executed asynchronously after an agent turn completes. Useful for auditing, telemetry, indexing, or downstream notification.

### Request Payload

```json
{
  "channel_id": "123456789012345678",
  "channel_name": "general",
  "thread_id": "123456789012345678",
  "turn_id": "turn-abc-123",
  "response": "Investigated GPU metrics on calibarn; load is normal.",
  "duration_ms": 3450,
  "error": ""
}
```

### Response Payload

```json
{
  "status": "ok"
}
```

- **Default `on_timeout`**: `"proceed"`.
