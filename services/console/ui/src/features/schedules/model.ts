import type { CronSchedule, OneShotSchedule, ScheduleRun, ScheduleSummaryMetrics } from '../../api/types';
import type { IconName } from '../../components/Icon';

// Pure view models for the schedules view, ported from updateScheduleMetrics, renderSchedulePills,
// renderScheduleCards, renderScheduleRuns and startSchedulesTicker in static/app.js. Components
// pass reconciled snapshots in; unchanged inputs come back as the same references so memoized
// cards skip rendering.

export type ScheduleFilter = 'ALL' | 'CRON' | 'ONE_SHOT' | 'RUNS';

export interface FilterPill {
  readonly id: ScheduleFilter;
  readonly label: string;
}

/** The category pills, in #schedules-filter-pills order. */
export const SCHEDULE_FILTERS: readonly FilterPill[] = Object.freeze([
  { id: 'ALL', label: 'ALL' },
  { id: 'CRON', label: 'CRON ROUTINES' },
  { id: 'ONE_SHOT', label: 'ONE-SHOT' },
  { id: 'RUNS', label: 'RUN AUDIT LOG' },
]);

const EMPTY_CRONS: readonly CronSchedule[] = Object.freeze([]);
const EMPTY_ONE_SHOTS: readonly OneShotSchedule[] = Object.freeze([]);

/** The search term as every matcher and highlighter sees it. */
export const normalizeQuery = (raw: string): string => raw.toLowerCase().trim();

const anyIncludes = (query: string, fields: readonly (string | undefined)[]): boolean =>
  fields.some((field) => (field ?? '').toLowerCase().includes(query));

/** The value the cron card shows as TARGET (and the field search matches as the channel). */
export const cronTarget = (cron: CronSchedule): string => cron.channel_id || cron.target_id || '';

/** Crons visible under `filter` whose title, expression, description, prompt, channel, timezone or id contain `query`. */
export function filterCrons(
  crons: readonly CronSchedule[],
  filter: ScheduleFilter,
  query: string,
): readonly CronSchedule[] {
  if (filter === 'ONE_SHOT') return EMPTY_CRONS;
  if (!query) return crons;
  return crons.filter((c) =>
    anyIncludes(query, [c.title_prefix, c.cron_expr, c.cron_description, c.prompt, cronTarget(c), c.timezone, c.id]),
  );
}

/** One-shots visible under `filter` whose prompt, thread or id contain `query`. */
export function filterOneShots(
  oneShots: readonly OneShotSchedule[],
  filter: ScheduleFilter,
  query: string,
): readonly OneShotSchedule[] {
  if (filter === 'CRON') return EMPTY_ONE_SHOTS;
  if (!query) return oneShots;
  return oneShots.filter((s) => anyIncludes(query, [s.prompt, s.thread_id, s.id]));
}

/** Runs whose prompt, title, schedule, thread, target, status or error contain `query`. */
export function filterRuns(runs: readonly ScheduleRun[], query: string): readonly ScheduleRun[] {
  if (!query) return runs;
  return runs.filter((r) =>
    anyIncludes(query, [r.prompt, r.title, r.schedule_id, r.thread_id, r.target_id, r.status, r.error]),
  );
}

/** The schedules grid is hidden on the run audit log pill. */
export const showsGrid = (filter: ScheduleFilter): boolean => filter !== 'RUNS';

/** The run feed is hidden on the cron and one-shot pills. */
export const showsRuns = (filter: ScheduleFilter): boolean => filter === 'ALL' || filter === 'RUNS';

/** Discord thread id worth linking to agentsview (all digits), trimmed; null otherwise. */
export function linkableThread(threadId: string | undefined): string | null {
  const trimmed = (threadId ?? '').trim();
  return /^\d+$/.test(trimmed) ? trimmed : null;
}

export interface RunStatusModel {
  /** Lowercased status, used for the `run-<status>` card class. */
  readonly key: string;
  readonly label: string;
  readonly icon: IconName;
  readonly chipClass: string;
}

/** Status chip of a run card. */
export function runStatus(status: string | undefined): RunStatusModel {
  const key = (status || 'unknown').toLowerCase();
  const label = key.toUpperCase();
  switch (key) {
    case 'running':
      return { key, label, icon: 'sync', chipClass: 'chip-running' };
    case 'enqueued':
      return { key, label, icon: 'hourglass', chipClass: 'chip-enqueued' };
    case 'failed':
      return { key, label, icon: 'alert', chipClass: 'chip-failed' };
    case 'completed':
    case 'success':
      return { key, label, icon: 'bolt', chipClass: 'chip-completed' };
    default:
      return { key, label, icon: 'check', chipClass: 'chip-completed' };
  }
}

export interface SuccessRateModel {
  readonly text: string;
  readonly className: string;
}

/** The 24H SUCCESS RATE card: green from 90%, amber from 75%, red below. */
export function successRate(summary: ScheduleSummaryMetrics | null): SuccessRateModel {
  // The handler always sends a number; a partial payload falls back like updateScheduleMetrics.
  const reported: unknown = summary?.success_rate_24h;
  const rate = typeof reported === 'number' ? reported : 100;
  const text = `${rate.toFixed(1)}%`;
  if (rate >= 90) return { text, className: 'value text-success' };
  if (rate >= 75) return { text, className: 'value schedules-rate-amber' };
  return { text, className: 'value text-danger' };
}

/** Sub-label under the success rate. */
export const runsLoggedLabel = (summary: ScheduleSummaryMetrics | null): string =>
  summary === null ? '0 RUNS LOGGED' : `${summary.total_runs_24h || 0} RUNS LOGGED (24H)`;

/** The soonest enabled cron run or one-shot, as its timestamp string; null when none parse. */
export function earliestTrigger(crons: readonly CronSchedule[], oneShots: readonly OneShotSchedule[]): string | null {
  const candidates: string[] = [];
  for (const c of crons) if (c.enabled && c.next_run_at) candidates.push(c.next_run_at);
  for (const s of oneShots) if (s.run_at) candidates.push(s.run_at);
  let best: string | null = null;
  let bestMs = Infinity;
  for (const at of candidates) {
    const ms = Date.parse(at);
    if (ms < bestMs) {
      best = at;
      bestMs = ms;
    }
  }
  return best;
}

/** The results bar under the pills. */
export function resultsLabel(hasData: boolean, matching: number, total: number): string {
  return hasData ? `SHOWING ${matching} / ${total} ACTIVE ROUTINES` : 'SHOWING 0 / 0 ITEMS';
}
