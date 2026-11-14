// Vanadis build queue classification, chunk stats, status badges and timing. Inputs are structural subsets of the /api/builds DTOs so callers can pass either
// list entries (precomputed *_chunks counters) or detail payloads (full chunk arrays).

import type { VanadisBuildDTO, VanadisChunkDTO } from '../api/types';
import type { IconName } from '../components/Icon';
import { formatUptime, parseValidTimestampMs } from './formatters';

export type BuildQueueState = 'running' | 'queued' | 'completed';

type ChunkLike = Partial<Pick<VanadisChunkDTO, 'status' | 'started_at' | 'completed_at' | 'updated_at'>>;

export type BuildLike = Partial<
  Pick<
    VanadisBuildDTO,
    | 'status'
    | 'queue_state'
    | 'created_at'
    | 'updated_at'
    | 'total_chunks'
    | 'completed_chunks'
    | 'running_chunks'
    | 'pending_chunks'
    | 'failed_chunks'
  >
> & { readonly chunks?: readonly ChunkLike[] | undefined };

const norm = (s: string | null | undefined): string => (s ?? '').trim().toLowerCase();

/**
 * Buckets a build (or a bare status string) into the queue view's three filters. A running
 * build whose chunks are all still pending has not started work yet and reads as queued.
 */
export function classifyBuildQueueState(build: BuildLike | string | null | undefined): BuildQueueState {
  if (!build) return 'queued';
  const isObj = typeof build === 'object';
  const status = isObj ? norm(build.status || build.queue_state) : norm(build);
  switch (status) {
    case 'completed':
    case 'merged':
    case 'failed':
    case 'cancelled':
    case 'canceled':
      return 'completed';
    case 'queued':
    case 'pending':
      return 'queued';
    case 'running': {
      const chunks = isObj ? (build.chunks ?? []) : [];
      return chunks.length > 0 && chunks.every((c) => norm(c.status) === 'pending') ? 'queued' : 'running';
    }
    default: {
      const qs = isObj ? norm(build.queue_state) : '';
      return qs === 'running' || qs === 'completed' ? qs : 'queued';
    }
  }
}

export interface BuildChunkStats {
  readonly total: number;
  readonly completed: number;
  readonly running: number;
  readonly pending: number;
  readonly failed: number;
  /** Completed share of total, rounded to a whole percent. */
  readonly percent: number;
}

const ZERO_STATS: BuildChunkStats = Object.freeze({ total: 0, completed: 0, running: 0, pending: 0, failed: 0, percent: 0 });

const withPercent = (s: Omit<BuildChunkStats, 'percent'>): BuildChunkStats => ({
  ...s,
  percent: s.total > 0 ? Math.round((s.completed / s.total) * 100) : 0,
});

/** Chunk counters from the chunk array when present, else from the list entry's counters. */
export function computeBuildChunkStats(build: BuildLike | null | undefined): BuildChunkStats {
  if (!build) return ZERO_STATS;
  const chunks = build.chunks ?? [];
  if (chunks.length === 0) {
    return withPercent({
      total: build.total_chunks || 0,
      completed: build.completed_chunks || 0,
      running: build.running_chunks || 0,
      pending: build.pending_chunks || 0,
      failed: build.failed_chunks || 0,
    });
  }
  let completed = 0;
  let running = 0;
  let failed = 0;
  for (const c of chunks) {
    const st = norm(c.status);
    if (st === 'completed' || st === 'merged') completed++;
    else if (st === 'running' || st === 'in_progress') running++;
    else if (st === 'failed') failed++;
  }
  return withPercent({ total: chunks.length, completed, running, failed, pending: chunks.length - completed - running - failed });
}

export type BuildStatusTone = 'queued' | 'running' | 'completed' | 'failed' | 'cancelled';

export interface BuildStatusBadge {
  readonly label: string;
  /** Matches the `status-<tone>` modifier classes in the console stylesheet. */
  readonly tone: BuildStatusTone;
  readonly icon: IconName;
}

const BADGES: Readonly<Record<BuildStatusTone, BuildStatusBadge>> = Object.freeze({
  queued: Object.freeze({ label: 'QUEUED', tone: 'queued', icon: 'hourglass' }),
  running: Object.freeze({ label: 'RUNNING', tone: 'running', icon: 'bolt' }),
  completed: Object.freeze({ label: 'COMPLETED', tone: 'completed', icon: 'check' }),
  failed: Object.freeze({ label: 'FAILED', tone: 'failed', icon: 'x' }),
  cancelled: Object.freeze({ label: 'CANCELLED', tone: 'cancelled', icon: 'ban' }),
});

/** Status pill for a build: terminal failure/cancel states win over the queue bucket. */
export function getBuildStatusBadgeInfo(build: BuildLike | null | undefined): BuildStatusBadge {
  if (!build) return BADGES.queued;
  const raw = norm(build.status);
  if (raw === 'failed') return BADGES.failed;
  if (raw === 'cancelled' || raw === 'canceled') return BADGES.cancelled;
  return BADGES[classifyBuildQueueState(build)];
}

/** Compact local 12-hour time (`8:05am`), optionally prefixed with `M/D`. */
export function formatBuildTimestamp(ms: number, includeDate: boolean): string {
  const d = new Date(ms);
  const rawHours = d.getHours();
  const time = `${rawHours % 12 || 12}:${String(d.getMinutes()).padStart(2, '0')}${rawHours >= 12 ? 'pm' : 'am'}`;
  return includeDate ? `${d.getMonth() + 1}/${d.getDate()} ${time}` : time;
}

/** True when both instants fall on the same local calendar day; false if either is absent. */
export function isSameCalendarDay(msA: number | null, msB: number | null): boolean {
  if (!msA || !msB) return false;
  const a = new Date(msA);
  const b = new Date(msB);
  return a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth() && a.getDate() === b.getDate();
}

export interface BuildTimingInfo {
  readonly startText: string;
  /** End time; the date is omitted when it falls on the start day. Empty until completed. */
  readonly endText: string;
  readonly durationText: string;
}

const NO_TIMING: BuildTimingInfo = Object.freeze({ startText: '--', endText: '', durationText: '--' });

function minTimestamp(values: Iterable<string | undefined>): number | null {
  let min: number | null = null;
  for (const v of values) {
    const ms = parseValidTimestampMs(v);
    if (ms !== null && (min === null || ms < min)) min = ms;
  }
  return min;
}

function maxTimestamp(values: Iterable<string | undefined>): number | null {
  let max: number | null = null;
  for (const v of values) {
    const ms = parseValidTimestampMs(v);
    if (ms !== null && (max === null || ms > max)) max = ms;
  }
  return max;
}

/**
 * Start/end/duration for a build card. Start is the earliest chunk start (else build creation);
 * a completed build ends at its latest chunk completion, else its latest update. Running builds
 * tick their duration against `now`.
 */
export function getBuildTimingInfo(build: BuildLike | null | undefined, now: number): BuildTimingInfo {
  if (!build) return NO_TIMING;
  const qState = classifyBuildQueueState(build);
  const chunks = build.chunks ?? [];

  const startMs = minTimestamp(chunks.map((c) => c.started_at)) ?? parseValidTimestampMs(build.created_at);

  let endMs: number | null = null;
  if (qState === 'completed') {
    endMs =
      maxTimestamp(chunks.map((c) => c.completed_at)) ??
      maxTimestamp([build.updated_at, ...chunks.map((c) => c.updated_at)]);
  }

  if (startMs === null) {
    return { startText: '--', endText: endMs === null ? '' : formatBuildTimestamp(endMs, true), durationText: '--' };
  }

  const endText = endMs === null ? '' : formatBuildTimestamp(endMs, !isSameCalendarDay(startMs, endMs));
  let durationText = '--';
  if (endMs !== null && endMs >= startMs) {
    durationText = formatUptime(Math.floor((endMs - startMs) / 1000));
  } else if (qState === 'running' && now >= startMs) {
    durationText = formatUptime(Math.floor((now - startMs) / 1000));
  }
  return { startText: formatBuildTimestamp(startMs, true), endText, durationText };
}
