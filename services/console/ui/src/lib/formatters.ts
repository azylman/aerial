// Pure display formatters ported from static/app.js. No function here reads the clock: callers
// pass `now` (epoch ms, already skew-corrected) so views can share one ticker and tests stay pure.
// Emoji chrome from the vanilla UI is replaced by an `IconName` the view renders as inline SVG.

import type { IconName } from '../components/Icon';

/** Timestamps before 2020-01-01 (Go zero time, epoch zero) are treated as absent. */
const MIN_VALID_TIMESTAMP_MS = 1577836800000;

// Intl formatters are expensive to construct; build them once per module, not per call.
const shortDate = new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric' });
const hourMinute = new Intl.DateTimeFormat(undefined, { hour: '2-digit', minute: '2-digit' });
const fullTime = new Intl.DateTimeFormat(undefined, { hour: 'numeric', minute: '2-digit', second: '2-digit' });

function isMissingSeconds(seconds: number | null | undefined): seconds is null | undefined {
  return seconds == null || Number.isNaN(seconds) || seconds < 0;
}

const pad2 = (n: number): string => String(n).padStart(2, '0');

/** Compact uptime: `1d 1h 1m`, `1h 1m`, `2m 5s`, `45s`. */
export function formatUptime(seconds: number | null | undefined): string {
  if (isMissingSeconds(seconds)) return '0s';
  const days = Math.floor(seconds / 86400);
  const hrs = Math.floor((seconds % 86400) / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);
  if (days > 0) return `${days}d ${hrs}h ${mins}m`;
  if (hrs > 0) return `${hrs}h ${mins}m`;
  if (mins > 0) return `${mins}m ${secs}s`;
  return `${secs}s`;
}

/** Stopwatch text for running tasks: `01:05s` under an hour, `01:01:05` above. */
export function formatElapsedTicker(seconds: number | null | undefined): string {
  if (isMissingSeconds(seconds)) return '00:00s';
  const hrs = Math.floor(seconds / 3600);
  const mins = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);
  if (hrs > 0) return `${pad2(hrs)}:${pad2(mins)}:${pad2(secs)}`;
  return `${pad2(mins)}:${pad2(secs)}s`;
}

/** Git-sync lag badge text: `Δ 45s`, `Δ 2m 5s`, `Δ 1h`. */
export function formatGitSyncDelta(seconds: number | null | undefined): string {
  if (isMissingSeconds(seconds)) return 'Δ 0s';
  const s = Math.floor(seconds);
  if (s < 60) return `Δ ${s}s`;
  const mins = Math.floor(s / 60);
  const secs = s % 60;
  if (mins < 60) return secs > 0 ? `Δ ${mins}m ${secs}s` : `Δ ${mins}m`;
  const hrs = Math.floor(mins / 60);
  const remMins = mins % 60;
  return remMins > 0 ? `Δ ${hrs}h ${remMins}m` : `Δ ${hrs}h`;
}

export interface TriggerBadge {
  readonly icon: IconName;
  readonly text: string;
  /** Modifier class suffix shared with the vanilla stylesheet (`trigger-badge <css>`). */
  readonly css: 'cron' | 'reminder' | 'http' | 'discord';
}

const TRIGGER_CRON: TriggerBadge = Object.freeze({ icon: 'alarm', text: 'CRON', css: 'cron' });
const TRIGGER_REMINDER: TriggerBadge = Object.freeze({ icon: 'timer', text: 'REMINDER', css: 'reminder' });
const TRIGGER_API: TriggerBadge = Object.freeze({ icon: 'bolt', text: 'API', css: 'http' });
const TRIGGER_DISCORD: TriggerBadge = Object.freeze({ icon: 'chat', text: 'DISCORD', css: 'discord' });

/** Maps a task trigger_type to its badge; unknown or empty triggers read as Discord. */
export function getTriggerBadge(triggerType: string | null | undefined): TriggerBadge {
  switch ((triggerType ?? '').toLowerCase()) {
    case 'cron':
      return TRIGGER_CRON;
    case 'reminder':
      return TRIGGER_REMINDER;
    case 'http':
    case 'api':
      return TRIGGER_API;
    default:
      return TRIGGER_DISCORD;
  }
}

/** Deep link into agentsview for an eri session; bare ids get the antigravity-cli: prefix. */
export function formatAgentsviewSessionUrl(sessionId: string | null | undefined): string {
  const cleanId = (sessionId ?? '').trim().replace(/^\/+|\/+$/g, '');
  if (!cleanId) return '/agentsview/';
  const fullId = cleanId.includes(':') ? cleanId : `antigravity-cli:${cleanId}`;
  return `/agentsview/sessions/${encodeURIComponent(fullId)}`;
}

/** Epoch ms for an ISO timestamp, or null when missing, unparseable or before 2020. */
export function parseValidTimestampMs(dateStr: string | null | undefined): number | null {
  if (!dateStr) return null;
  const ms = Date.parse(dateStr);
  if (Number.isNaN(ms) || ms < MIN_VALID_TIMESTAMP_MS) return null;
  return ms;
}

/** Commit time as `Sep 4, 04:00 AM` in the viewer's locale; empty when absent. */
export function formatCommitTimestamp(dateStr: string | null | undefined): string {
  const ms = parseValidTimestampMs(dateStr);
  if (ms === null) return '';
  return `${shortDate.format(ms)}, ${hourMinute.format(ms)}`;
}

/** Coarse relative age (`10s AGO` … `2d AGO`); `RECENT` when the timestamp is absent. */
export function formatTimeAgo(dateStr: string | null | undefined, now: number): string {
  const ms = parseValidTimestampMs(dateStr);
  if (ms === null) return 'RECENT';
  const sec = Math.max(0, Math.floor((now - ms) / 1000));
  if (sec < 60) return `${sec}s AGO`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m AGO`;
  const hrs = Math.floor(min / 60);
  if (hrs < 24) return `${hrs}h AGO`;
  return `${Math.floor(hrs / 24)}d AGO`;
}

/** Run duration: `450ms`, `4.5s`, `1m 5s`. */
export function formatDuration(durationMs: number | null | undefined): string {
  if (durationMs == null || Number.isNaN(durationMs) || durationMs <= 0) return '0ms';
  if (durationMs < 1000) return `${durationMs}ms`;
  if (durationMs < 60000) return `${(durationMs / 1000).toFixed(1)}s`;
  const mins = Math.floor(durationMs / 60000);
  const remSec = Math.floor((durationMs % 60000) / 1000);
  return `${mins}m ${remSec}s`;
}

/** Absolute timestamp as `Oct 17 8:00:05 AM` in the viewer's locale; `--` when unparseable. */
export function formatTimestamp(dateStr: string | null | undefined): string {
  if (!dateStr) return '--';
  const ms = Date.parse(dateStr);
  if (Number.isNaN(ms)) return '--';
  return `${shortDate.format(ms)} ${fullTime.format(ms)}`;
}

export type CountdownState = 'none' | 'normal' | 'urgent' | 'triggering' | 'overdue';

export interface Countdown {
  readonly text: string;
  readonly state: CountdownState;
  /** Stylesheet class for the state (`countdown-urgent`, …); empty when there is no target. */
  readonly cssClass: string;
  readonly icon: IconName | null;
}

const NO_COUNTDOWN: Countdown = Object.freeze({ text: '--:--:--', state: 'none', cssClass: '', icon: null });

const countdown = (text: string, state: Exclude<CountdownState, 'none'>): Countdown => ({
  text,
  state,
  cssClass: `countdown-${state}`,
  icon: state === 'triggering' ? 'bolt' : 'timer',
});

/**
 * Time until a schedule fires relative to `now`. Within 15s after the target the schedule is
 * shown as triggering; later than that it is overdue until the next poll catches up.
 */
export function formatCountdown(targetDateStr: string | null | undefined, now: number): Countdown {
  if (!targetDateStr) return NO_COUNTDOWN;
  const targetMs = Date.parse(targetDateStr);
  if (Number.isNaN(targetMs)) return NO_COUNTDOWN;

  const diffSec = Math.floor((targetMs - now) / 1000);
  if (diffSec > 86400) {
    return countdown(`in ${Math.floor(diffSec / 86400)}d ${Math.floor((diffSec % 86400) / 3600)}h`, 'normal');
  }
  if (diffSec >= 3600) {
    return countdown(`in ${Math.floor(diffSec / 3600)}h ${Math.floor((diffSec % 3600) / 60)}m`, 'normal');
  }
  if (diffSec > 60) return countdown(`in ${Math.floor(diffSec / 60)}m ${diffSec % 60}s`, 'normal');
  if (diffSec >= 1) return countdown(`in ${diffSec}s`, 'urgent');
  if (diffSec >= -15) return countdown('TRIGGERING...', 'triggering');
  return countdown('OVERDUE (SYNCING...)', 'overdue');
}
