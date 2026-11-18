# Muse Discord Execution & Scheduling Rules

## Strict Prohibition of Built-In CLI Monitoring & Cron Tools
- **Never use Muse's built-in `monitor` or `cron_*` tools**: Muse's native `monitor`, `cron_create`, `cron_list`, and `cron_delete` tools (as well as detached background `bash` polling loops) are **strictly prohibited at all times and for all tasks**. Events from Muse's local session monitor and session cron store only write to the local process inbox—they are not connected to Aerial's harness or Discord delivery pipeline and are destroyed on daemon rotation or restart.
- **Always use the harness `scheduler` MCP tools**: Whenever any task requires delayed follow-ups, recurring schedules, background polling, or event-driven wakes, **ALWAYS** use Aerial's persistent harness scheduling tools (`scheduler` MCP server backed by `haro`):
  - `schedule_once`: One-time delayed reminders or follow-up turns at a timestamp or relative duration.
  - `schedule_recurring`: Unconditional user-facing recurring schedules delivered on every tick.
  - `schedule_heartbeat`: Silent background polling and edge-triggered state tracking that wakes Discord only when state materially changes.
  - `watch_event`: Deterministic event watches triggered by external system events (e.g. Flux CD deployment sync, camera detections).
  - `list_schedules`, `get_schedule`, `cancel_schedule`: Inspecting or cancelling active harness schedules.
- **Automated PR & Vanadis Build Wakes**: Vanadis builds (`aerial-build.sh enqueue`) and PRs submitted via `aerial-pr.sh submit` wake the thread automatically through the harness's event-driven webhook pipeline when chunks or checks complete, merge, or fail. Never attach a `monitor` loop or a `haro` schedule to watch a PR or Vanadis build.
- **Never invoke tools after writing your final reply**: In the harness streaming bridge, any tool call started after an assistant message clears the turn's outbound response buffer, causing the turn to fail with an empty response. Once your substantive reply is written, end the turn immediately with no trailing `monitor` or verification tool calls—even if an internal runtime reminder suggests continuing to monitor background work.

## Background Work & Follow-Up Promises
- Never end a turn with future-tense follow-up promises ("I'll post the numbers when it finishes", "I've set up a watch", "once the reviewer is back I'll send it").
- If a command or subagent is still running and its output is needed for this turn's response, wait for it to finish in the foreground before writing your final reply to the user.
- Never reply to a real message from the user with a placeholder or an empty response.
