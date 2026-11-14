import type {
  BuildDetailAPIResponse,
  BuildsAPIResponse,
  ClusterResponse,
  FactsAPIResponse,
  GitSyncStatusResponse,
  QuickLaunchLink,
  SchedulesAPIResponse,
  ServiceStatus,
  VanadisBuildDTO,
} from '../api/types';
import { formatGitSyncDelta, parseValidTimestampMs } from '../lib/formatters';

// Pure derivations for the app shell. Components select the raw (reconciled) snapshot and pass it
// here, so an unchanged poll yields the same primitive and React bails out of the render.

/** Tab badge counts shown next to each tab label. */
export const tasksBadgeCount = (cluster: ClusterResponse | null): number => cluster?.active_tasks.length ?? 0;

export const schedulesBadgeCount = (schedules: SchedulesAPIResponse | null): number =>
  schedules === null ? 0 : schedules.summary.total_active;

export const memoryBadgeCount = (facts: FactsAPIResponse | null): number => facts?.total ?? 0;

export const buildsBadgeCount = (builds: BuildsAPIResponse | null): number =>
  builds === null ? 0 : builds.running_count + builds.queued_count;

/**
 * The build a detail payload describes (`build`, or the legacy `entry` key), only when it is the
 * build with `id`: the detail slice holds the last fetched build, which may be a different one.
 */
export function buildFromDetail(detail: BuildDetailAPIResponse | null, id: string): VanadisBuildDTO | undefined {
  const build = detail?.build ?? detail?.entry;
  return build?.id === id ? build : undefined;
}

/**
 * Build drawer header label (openBuildDetailDrawer): the build id until its detail arrives, then
 * the build's title (falling back to its id).
 */
export function buildDrawerLabel(detail: BuildDetailAPIResponse | null, id: string): string {
  const build = buildFromDetail(detail, id);
  return build === undefined ? id : build.title || build.id;
}

export type PermetTone = 'idle' | 'optimal' | 'stable' | 'warning';

export interface PermetScore {
  readonly percent: number;
  readonly label: string;
  readonly tone: PermetTone;
}

/** Static header value before the first status poll. */
export const PERMET_IDLE: PermetScore = Object.freeze({ percent: 88, label: 'LVL 5 // ACTIVE', tone: 'idle' });

/** Permet gauge: share of services reporting `healthy`. */
export function permetScore(services: readonly ServiceStatus[] | null): PermetScore {
  if (services === null) return PERMET_IDLE;
  const healthy = services.filter((s) => s.status === 'healthy').length;
  const ratio = services.length > 0 ? healthy / services.length : 0;
  const percent = Math.round(ratio * 100);
  if (ratio === 1) return { percent, label: 'LVL 5 // OPTIMAL', tone: 'optimal' };
  if (ratio > 0.7) return { percent, label: 'LVL 3 // STABLE', tone: 'stable' };
  return { percent, label: 'LVL 1 // WARNING', tone: 'warning' };
}

export type GitSyncTone = 'synced' | 'lagging' | 'error';

export interface GitSyncBadgeModel {
  readonly tone: GitSyncTone;
  readonly text: string;
}

/**
 * Git-sync badge model. A lagging repo keeps counting between polls: the lag reported at
 * `system_time` plus the seconds elapsed since then on the shared live clock. When `now` equals
 * the response's `system_time` the text shows the reported lag unchanged.
 */
export function gitSyncBadge(gitSync: GitSyncStatusResponse | null, systemTime: string | null, now: number): GitSyncBadgeModel {
  if (gitSync === null) return { tone: 'synced', text: 'GIT SYNC: IN SYNC (Δ 0s)' };
  const status = (gitSync.status || 'synced').toLowerCase();
  const reported = Number(gitSync.max_lag_seconds) || 0;
  if (status === 'error') return { tone: 'error', text: 'GIT SYNC: ERROR' };
  if (status === 'lagging' || reported > 0) {
    const sampledAt = parseValidTimestampMs(systemTime);
    const drift = sampledAt === null ? 0 : Math.max(0, Math.floor((now - sampledAt) / 1000));
    return { tone: 'lagging', text: `GIT SYNC: BEHIND (${formatGitSyncDelta(reported + drift)})` };
  }
  return { tone: 'synced', text: `GIT SYNC: IN SYNC (${formatGitSyncDelta(reported)})` };
}

const EMPTY_LINKS: readonly QuickLaunchLink[] = Object.freeze([]);

/** Links the dock can render (entries without a name or url are dropped). */
export function dockLinks(links: readonly QuickLaunchLink[] | null): readonly QuickLaunchLink[] {
  if (links === null || links.length === 0) return EMPTY_LINKS;
  return links.every((l) => l.name !== '' && l.url !== '') ? links : links.filter((l) => l.name !== '' && l.url !== '');
}

/** Whether a dock chip is active for the current location pathname. */
export function isDockLinkActive(url: string, pathname: string): boolean {
  return url === pathname || (url !== '/' && pathname.startsWith(url));
}
