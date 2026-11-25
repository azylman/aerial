# Discord Channels, Funnel & Lifecycle Webhooks

1. **In-Channel Interaction, Wake Modes & Channel Lifecycle Webhooks**:
   - **Wake Modes**: `mention` (explicit @Aerial or direct replies only), `classifier` / `ambient` (Tier-1 mentions/replies; Tier-2 LLM classifier scoring; bare keywords never wake), or `all` / `always` (responds to every message). Typing indicators pulse on all active response turns.
   - **Channel Lifecycle Webhooks**: Configurable HTTP hooks (`on_wake`, `pre_turn`, `post_turn`) defined under `hooks:` in `config.yaml` enable external sidecars to inspect, gate, or enrich turns:
     - `on_wake`: Dispatched on message arrival at the Discord Gateway funnel (`POST` `WakeRequest`: `message_id`, `channel_id`, `thread_id`, `guild_id`, `author_id`, `author_username`, `content`, `is_admin`, etc.). Returns `WakeResponse` (`override: "wake" | "drop" | "classify"`). Defaults to `on_timeout: "classify"`.
     - `pre_turn`: Dispatched before agent prompt assembly (`POST` `PreTurnRequest`: `channel_id`, `thread_id`, `burst_messages`, `prompt`, `retry_count`). Returns `PreTurnResponse` (`allow: bool`, `action: "proceed" | "retry"`, `retry_after_seconds: int`, `injected_context: string` injected under `<COORDINATION_CONTEXT>`). Defaults to `on_timeout: "retry"`.
     - `post_turn`: Dispatched asynchronously after agent response delivery (`POST` `PostTurnRequest`: `channel_id`, `thread_id`, `status`, `response_text`, `duration_ms`, `token_usage`). Returns `200 OK` / `204 No Content`. Defaults to `on_timeout: "proceed"`.

2. **Discord Funnel Hardening**:
   - Thread deduplication automatically recovers existing thread IDs on Discord error 160004.
   - Message staleness check TTL is set to 30 minutes to prevent premature expiration during deployment or backlog bursts.
