# Discord Channels, Funnel & Lifecycle Webhooks

1. **In-Channel Interaction, Wake Modes & Channel Lifecycle Webhooks**:
   - **Wake Modes**: `mention` (explicit @Aerial or direct replies only), `classifier` / `ambient` (Tier-1 mentions/replies; Tier-2 LLM classifier scoring; bare keywords never wake), or `all` / `always` (responds to every message). Typing indicators pulse on all active response turns.
   - **Channel Lifecycle Webhooks**: Configurable HTTP hooks (`on_wake`, `pre_turn`, `post_turn`) enable external sidecars to inspect, gate, or enrich turns. Schemas and timeout fallback policies are documented in `docs/specs/2026-09-22-channel-lifecycle-webhooks.md`.

2. **Discord Funnel Hardening**:
   - Thread deduplication automatically recovers existing thread IDs on Discord error 160004.
   - Message staleness check TTL is set to 30 minutes to prevent premature expiration during deployment or backlog bursts.
