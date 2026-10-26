# Prospera: Autonomous Voice Call & Phone Errand Subsystem (`services/prospera`)

- **Date**: 2026-10-25
- **Status**: Reviewed by The Girl Gang (Awaiting Owner Plan Approval)
- **Repository Scope**: `azylman/aerial` (`services/prospera`, `nomad/jobs/prospera.nomad`, `nomad/jobs/edge-proxy.nomad`, `services/eri/handlers.go`, `services/eri/pkg/queue/pool.go`, `services/eri/pkg/db/schema.sql`) & `azylman/aerial-config` (`config.yaml`, Cloudflare WAF rule)

---

## 1. Intent & Why

Aerial needs a first-class autonomous telephony subsystem—named **`prospera`** (`services/prospera`, `prospera.aerial:4019`) after Prospera Mercury, President of Shin Sei Development Corporation—to place and receive real phone calls over the public switched telephone network (PSTN) via Twilio. `prospera` executes **outbound phone errands** on the owner's behalf (calling restaurants for wait times or reservations, scheduling medical/dental appointments, requesting availability and quotes from contractors, and navigating business IVR phone trees) and screens inbound calls without the owner having to sit on hold or play phone tag.

Unlike thin stateless 1:1 API adapters under `mcp/*-mcp`, `prospera` is a stateful cognitive subsystem under `services/prospera` alongside `eri`, `haro`, `coven`, and `vanadis`: it owns PostgreSQL tables, terminates public Twilio webhooks and bidirectional WebSocket Media Streams, runs a real-time 8kHz $\mu$-law DSP pipeline, drives an independent Gemini Live sub-agent session with its own tool loop and approval firewall, and exposes a Streamable HTTP MCP control plane (`/mcp`) to `eri`.

The design synthesizes the strongest architectural patterns from two reference implementations:
- **`@openclaw/voice-call` (`extensions/voice-call`)**: Compartmentalized per-call `CallBrief` with an explicit approval firewall, live transcript batching, mid-call steering (`steer_call` in `guidance` and `say` modes), mid-call context consultation, Twilio Answering Machine Detection (AMD) greeting hold, recent-outbound callback window linking, and prompt-injection-safe post-call report summarization.
- **Hermes Agent (`official/productivity/telephony`)**: Self-service Twilio local number search/purchase/webhook wiring, pre-connect `dtmfSequence` (`ww1w2`) support for known IVR trees, and SMS send/receive on the same number to capture text confirmations and booking links.

---

## 2. Goals, Non-Goals & Anti-Goals

### Goals
1. **Autonomous Outbound Errand Calls**: Initiate outbound voice calls via a structured `CallBrief` (`task`, `context`, `identity`, `approvals`, `successCriteria`, `voicemailMessage`, `maxDurationSeconds`) over a low-latency full-duplex bridge between **Twilio Media Streams** (`8kHz` G.711 $\mu$-law) and **Gemini Live** (`gemini-3.8-live` primary, `gemini-3.1-flash-live-preview` fallback) via `bidiGenerateContent`.
2. **Mid-Call Calendar, Memory & Keypad Tools**: Equip the live voice session with strictly 4 bounded runtime tools:
   - `send_dtmf`: Synthesize in-band dual-tone (`697–1633 Hz`) G.711 $\mu$-law DTMF audio frames (`0-9`, `*`, `#`, `w`) directly into the outbound Media Stream pacer queue to navigate interactive phone trees without dropping the active `<Connect><Stream>` WebSocket.
   - `consult_calendar`: Query `google-mcp` (`get_events` / `query_freebusy`) to check real-time schedule availability when a business proposes alternative times.
   - `consult_memory`: Query `coven` (`POST /context` or `memory_search`) to look up owner preferences or household facts mid-call.
   - `end_call(reason, outcome_summary)`: Cleanly terminate the call only after a live human has confirmed the outcome/inability to help or after voicemail playback completes.
3. **Live Discord Transcripts & Mid-Call Steering in `#prospera`**:
   - Route live transcripts, voicemails, inbound receptionist updates, SMS confirmations, and post-call reports to the dedicated `#prospera` channel (`mode: "channel"`, `wake_mode: "all"`, `ignore_bots: true` in `config.yaml`), or to an explicitly specified `discord_channel_id` / `discord_thread_id`.
   - Batch final transcript turns every `5s` (`live.minIntervalMs: 5000`) and deliver them via `eri` (`POST /discord/message`) with a persisted atomic cursor so restarts never duplicate lines.
   - Expose `steer_call(call_id, message, mode)` (`mode: "guidance" | "say"`) so the owner or main agent can update call instructions silently or instruct the voice model to speak a line verbatim mid-call.
4. **Twilio Answering Machine Detection (AMD) & Safe Voicemail Leaving**:
   - Hold the outbound opening greeting until human vs. machine classification settles, releasing immediately on human cadence (`>=250ms` speech followed by `>=600ms` silence, `/amd` reporting `human`/`unknown`, or `2500ms` elapsed without continuous machine speech) so real humans never experience dead air.
   - If AMD detects a machine (`machine_start` / `machine_end_*`), cancel any live turn, flush outbound audio (`clear`), wait for the beep/greeting end, speak **only** the brief's `voicemailMessage` with tool-calling disabled, wait for playback mark completion, and hang up.
5. **Three-Tier Inbound Call Routing**:
   - **Tier 1 — Owner Allowlist (`allowFrom`)**: Answered with a live conversational Gemini Live session.
   - **Tier 2 — 7-Day Callback Window (`callbacks.windowMinutes: 10080`)**: Inbound calls from any E.164 number `prospera` dialed within the past 7 days are automatically admitted into **Receptionist / Callback Mode**, linked to the original outbound call (`callback_of_call_id`), loaded with the original errand brief as background reference, and governed by a strict message-taking instruction (*"Take a message or update for the owner; do not commit to new terms beyond the original approvals"*).
   - **Tier 3 — Unknown Callers (Native Twilio `<Record>` Voicemail)**: Calls from unknown numbers outside the allowlist and 7-day callback window do **not** spin up a Gemini Live session. Instead, the webhook returns static TwiML (`<Play>` or `<Say>` greeting + `<Record maxLength="60" playBeep="true" recordingStatusCallback=".../voicemail" />`) and sets `report_status = 'awaiting_recording'`. When the recording completes, `prospera` downloads the recording (with strict SSRF and redirect auth-stripping guards), transcribes it locally via `orin-voice.aerial` (Whisper STT) with Gemini Flash fallback, and delivers both the transcript and playable audio attachment to `#prospera`.
6. **SMS Confirmation Catcher & Outbound Texting**:
   - Handle inbound SMS/MMS webhooks (`/api/webhooks/twilio/sms`) and expose `send_sms` / `list_sms` MCP tools so text confirmations, waitlist alerts, or booking links sent by businesses to `prospera`'s phone number are forwarded straight to `#prospera`.
7. **Untrusted Post-Call Summarization & Receipt Delivery**:
   - When an outbound or callback call reaches a terminal state, summarize the call with `tools: []` and `<untrusted_transcript>` tags (treating the callee's speech as untrusted data) and deliver a structured report (`Outcome: achieved | not achieved | needs follow-up`, key agreed facts, duration, end reason, and full transcript) to `#prospera` (or the originating channel/thread).

### Non-Goals
- **Multi-Carrier Abstraction**: Supporting Telnyx or Plivo is a non-goal. Targeting Twilio exclusively avoids leaky provider abstractions.
- **Third-Party Voice Wrapper SaaS**: Using Bland.ai or Vapi is a non-goal; `prospera` owns the Twilio Media Stream to Gemini Live bridge directly.
- **Universal 2FA Bypass**: Guaranteeing SMS 2FA delivery from third-party institutions that reject VoIP carrier ranges is a non-goal.

### Anti-Goals (Failure Conditions Even If Technically Functional)
- **Unauthorized Commitments**: The voice agent agreeing to pay a fee, place a credit card deposit, or accept binding terms outside `brief.approvals` because the callee pressured or prompt-injected it.
- **Talking Over Hold Music or Voicemail Greetings**: Triggering responses to background hold music/automated announcements, prematurely calling `end_call` on recorded hold messages, or blurting out the errand task onto an answering machine before the beep.
- **Unbounded AI Spend on Robocalls**: Opening Gemini Live bidirectional sessions for unrecognised spam callers.
- **Hardcoded Personal Data in `azylman/aerial`**: Committing real phone numbers, names, or credentials into the generic engine repository (Violation of the Zero Personal Data & Zero Plaintext Token invariants).

---

## 3. Decisions Settled with Owner vs. Builder Decisions

### Settled with Owner (Locked Requirements)
1. **Subsystem Name & Placement**: Named **`prospera`** and placed under `services/prospera` (`prospera.aerial:4019`) as a first-class Gundam subsystem rather than a thin `mcp/*-mcp` wrapper.
2. **Dedicated Discord Channel (`#prospera`)**: Configured `#prospera` in `mode: "channel"` (`wake_mode: "all"`, `ignore_bots: true`) in `aerial-config/config.yaml` as the dedicated home for voice call transcripts, voicemails, SMS confirmations, and call steering.
3. **Cloudflare WAF Perimeter Hardening**: Since Twilio uses dynamic AWS NAT pools rather than static webhook CIDRs, enforce an edge WAF gate on `/api/webhooks/twilio/*` requiring AWS ASN (`ip.src.asnum in {16509 14618}`) and non-empty `X-Twilio-Signature` header (`len(http.request.headers["x-twilio-signature"]) > 0`), while keeping `/api/webhooks/github` locked to `$github_webhook_ips`.
4. **Pre-Call & Mid-Call Context with Approval Firewall**: Read access to Google Calendar (`google-mcp`) and Semantic Memory (`coven`) both before and during the call, paired with a strict approval firewall (`brief.approvals`).
5. **7-Day Callback Window & Native Twilio `<Record>` Voicemail**: Stretch the outbound callback admission window to **7 days (`10,080` minutes)** and route unknown callers to native TwiML `<Record>` voicemail with local Whisper transcription and playable audio clip delivery to `#prospera`.

### Builder Decisions (Our Call — Sensible Defaults)
1. **Declarative Database Schema (`services/eri/pkg/db/schema.sql`)**: Single-service table ownership in PostgreSQL (`voice_calls` and `voice_messages` owned exclusively by `services/prospera`) rolled out via Ariga Atlas (`./scripts/atlas-diff.sh add_prospera_voice_tables`). All phone numbers are normalized to strict E.164 (`+15105550199`) and enforced via SQL `CHECK` constraints.
2. **Public Ingress, Proxy Headers & Cryptographic Auth (`edge-proxy` + `prospera`)**:
   - Expose `/api/webhooks/twilio/*` on `webhooks.zylman.com` via `edge-proxy.nomad`, forwarding `Host $host`, `X-Forwarded-Host $host`, `X-Forwarded-Proto $forwarded_proto`, and WebSocket upgrades (`Upgrade`, `Connection`, `proxy_buffering off`, `proxy_read_timeout 960s`) to `http://prospera.aerial`.
   - Enforce **RFC 2104 `X-Twilio-Signature` HMAC-SHA1 validation** (computed against `PUBLIC_WEBHOOK_BASE_URL + r.URL.RequestURI()`) on all webhook POSTs and **single-use 30s cryptographic stream tokens** on `/api/webhooks/twilio/stream/{token}` WebSocket upgrades.
3. **Channel-Name Resolution & Base64 Audio Attachment Delivery via `eri`**:
   - Default `prospera` notifications to `DEFAULT_DISCORD_CHANNEL="prospera"` (`#prospera`), with `eri`'s `/discord/message` and `/prompt` handlers resolving channel names (`"prospera"` or `"#prospera"`) via `ResolveChannelByNameOrID` in addition to numeric snowflakes.
   - Extend `DirectMessageRequest` in `services/eri/handlers.go` (`POST /discord/message`) with an optional `attachments` slice (`[{ filename, content_type, content_base64 }]`) wired to `SendMessageWithAttachments` so `prospera` can attach voicemail `.mp3`/`.wav` clips directly without holding a Discord bot token.
4. **Audio Pipeline, In-Band DTMF & Resilience**:
   - Pure-Go `8kHz` G.711 $\mu$-law (`audio/x-mulaw`) <-> `16kHz` (input) / `24kHz` (output) linear PCM transcoding with a 20ms frame pacer, in-band dual-tone $\mu$-law DTMF synthesis, and Twilio `<Connect><Stream>` mark acknowledgements.
   - Transparent Gemini Live WebSocket reconnect (re-hydrating system prompt + transcript so far) if an upstream session rotates during calls up to `900s`, plus a `2000ms` Twilio WebSocket disconnect grace window.
   - Concurrency & self-call guards (`max_concurrent_calls = 2`, duplicate active `to_number` rejection, `to_number == from_number` rejection) and startup orphan reconciliation (`ended_at IS NULL` reconciled against Twilio REST API on service boot).
5. **Local Number Provisioning**: Auto-provision or bind an Oakland `(510)` local voice+SMS number using the credentials stored at `nomad/jobs/voice-call` (`TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`), configuring `VoiceUrl`, `StatusCallback`, and `SmsUrl` via the Twilio REST API.

---

## 4. Architecture & Data Contracts

### 4.1 Ingress & Service Topology

```
Twilio PSTN / Carrier (AWS AS16509 / AS14618)
  │
  ├── HTTPS POST /api/webhooks/twilio/{voice,status,amd,voicemail,sms}
  │     (Cloudflare WAF: AWS ASN + X-Twilio-Signature header present;
  │      prospera: RFC 2104 HMAC-SHA1 verification over PUBLIC_WEBHOOK_BASE_URL)
  └── WSS /api/webhooks/twilio/stream/{one_time_token}
        (Cloudflare WAF: AWS ASN + X-Twilio-Signature header present;
         prospera: 30s single-use token issued during verified /voice POST)
        │
        ▼
  Cloudflare Tunnel (webhooks.*) ──► edge-proxy (4021) ──► prospera.aerial (4019)
                                                               │
        ┌──────────────────────────────────────────────────────┼──────────────────────────────────────┐
        ▼                                                      ▼                                      ▼
  Gemini Live API                                    Internal Aerial Mesh                    PostgreSQL (`aerial`)
  (`gemini-3.8-live` /                               ├── `eri.aerial` (`/discord/message`,   ├── `voice_calls`
   `gemini-3.1-flash-live-preview`)                  │    `/prompt` -> `#prospera`)          └── `voice_messages`
                                                     ├── `google-mcp.aerial` (`/mcp`)
                                                     ├── `coven.aerial` (`/context`, `/mcp`)
                                                     └── `orin-voice.aerial` (Whisper STT)
```

### 4.2 Database Schema (`services/eri/pkg/db/schema.sql`)

Owned exclusively by `services/prospera`:

```sql
CREATE TABLE IF NOT EXISTS voice_calls (
    call_id TEXT PRIMARY KEY,
    provider_call_sid TEXT UNIQUE,
    direction TEXT NOT NULL CHECK (direction IN ('outbound', 'inbound')),
    state TEXT NOT NULL,
    end_reason TEXT,
    from_number TEXT NOT NULL CHECK (from_number ~ '^\+[1-9][0-9]{1,14}$'),
    to_number TEXT NOT NULL CHECK (to_number ~ '^\+[1-9][0-9]{1,14}$'),
    callback_of_call_id TEXT REFERENCES voice_calls(call_id) ON DELETE SET NULL,
    discord_channel_id TEXT,
    discord_thread_id TEXT,
    brief JSONB NOT NULL DEFAULT '{}'::jsonb,
    owner_instructions JSONB NOT NULL DEFAULT '[]'::jsonb,
    transcript JSONB NOT NULL DEFAULT '[]'::jsonb,
    live_transcript_cursor INTEGER NOT NULL DEFAULT 0,
    answered_by TEXT,
    voicemail_recording_url TEXT,
    report_status TEXT NOT NULL DEFAULT 'pending' CHECK (report_status IN ('pending', 'awaiting_recording', 'processing', 'delivered', 'failed')),
    report_summary TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    answered_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_voice_calls_to_started
    ON voice_calls (to_number, started_at DESC)
    WHERE direction = 'outbound';

CREATE INDEX IF NOT EXISTS idx_voice_calls_state
    ON voice_calls (state)
    WHERE ended_at IS NULL;

CREATE TABLE IF NOT EXISTS voice_messages (
    message_sid TEXT PRIMARY KEY,
    direction TEXT NOT NULL CHECK (direction IN ('outbound', 'inbound')),
    from_number TEXT NOT NULL CHECK (from_number ~ '^\+[1-9][0-9]{1,14}$'),
    to_number TEXT NOT NULL CHECK (to_number ~ '^\+[1-9][0-9]{1,14}$'),
    body TEXT NOT NULL,
    media_urls JSONB NOT NULL DEFAULT '[]'::jsonb,
    related_call_id TEXT REFERENCES voice_calls(call_id) ON DELETE SET NULL,
    discord_delivered BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_voice_messages_created
    ON voice_messages (created_at DESC);
```

### 4.3 MCP Tool Surface (`prospera` at `http://prospera.aerial/mcp`)

Exposed to `eri` via Streamable HTTP MCP (`/mcp`):

1. **`initiate_call`**:
   - **Inputs**:
     - `to` (E.164 phone number, required; rejected if equal to `from_number` or if an active call to `to` is already in progress)
     - `message` (opening spoken greeting, required)
     - `mode` (`"conversation"` default | `"notify"`)
     - `dtmf_sequence` (optional pre-connect digits, e.g. `"ww1w2"`, passed via Twilio `SendDigits`)
     - `discord_channel_id` / `discord_thread_id` (defaults to `"prospera"` so live transcripts and final reports land in `#prospera`)
     - `brief` (`CallBrief` object, max 8,000 JSON chars):
       - `task` (max 2,000 chars): What to achieve on the call.
       - `context` (max 4,000 chars): Relevant background facts, dates, or preferences.
       - `language` (optional BCP-47 tag, default `"en-US"`).
       - `identity` (`{ introduction: string, disclose: "volunteer" | "when-asked" }`, defaults to configured assistant introduction with `"volunteer"`).
       - `disclosures` (optional `string[]`, max 20 items): Explicit personal details cleared for disclosure.
       - `approvals` (max 2,000 chars): Explicit bounds on what the agent may agree to (defaults to zero spending and no commitments outside `task`).
       - `voicemail_message` (max 1,000 chars): Exact or safe message to leave if voicemail answers.
       - `success_criteria` (max 1,000 chars): Concrete details to confirm before hanging up (e.g. booking reference, arrival window, quote amount).
       - `max_duration_seconds` (optional int, default `300`, capped at `900`).
   - **Behavior**: Persists the `voice_calls` row, dials via Twilio `POST /Calls.json` with `AsyncAmd=true`, `MachineDetection=DetectMessageEnd`, and returns `{ call_id, provider_call_sid, status: "initiated" }` immediately.

2. **`steer_call`**:
   - **Inputs**: `call_id` (required), `message` (max 500 chars, required), `mode` (`"guidance"` default | `"say"`).
   - **Behavior**: Atomically appends to `owner_instructions` in DB and injects a mid-call system/client content turn into the active Gemini Live session (`guidance` updates ongoing rules; `say` instructs the model to speak the exact sentence immediately).

3. **`end_call`**:
   - **Inputs**: `call_id` (required).
   - **Behavior**: Gracefully closes the Gemini Live bridge, hangs up the Twilio call (`Status=completed`), and triggers final report generation.

4. **`get_call_status` & `list_calls`**:
   - Inspect live state, current transcript, `answered_by`, or recent call history.

5. **`send_sms` & `list_sms`**:
   - Send outbound SMS/MMS via Twilio or query recent inbound/outbound messages.

6. **`ensure_phone_number`**:
   - Lists owned Twilio numbers, or searches and purchases an available local number (default area code `510`) and configures its `VoiceUrl`, `StatusCallback`, and `SmsUrl` to point at our public webhook endpoints.
