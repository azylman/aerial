import type {
  BuildDetailAPIResponse,
  BuildsAPIResponse,
  ClusterResponse,
  FactsAPIResponse,
  QuickLaunchLink,
  SchedulesAPIResponse,
  ServiceStatus,
  VanadisBuildDTO,
} from '../api/types';

// Pure derivations for the app shell. Components select the raw (reconciled) snapshot and pass it
// here, so an unchanged poll yields the same primitive and React bails out of the render.

/** Filter items by agent attribution. When selectedAgent === 'all', all items match. Otherwise, item.agent (defaulting to 'aerial') must match. */
export function matchAgent<T extends { readonly agent?: string }>(item: T, selectedAgent: string): boolean {
  if (!selectedAgent || selectedAgent === 'all') return true;
  return (item.agent || 'aerial') === selectedAgent;
}

export function filterByAgent<T extends { readonly agent?: string }>(
  items: readonly T[] | null | undefined,
  selectedAgent: string,
): readonly T[] {
  if (!items || items.length === 0) return [];
  if (!selectedAgent || selectedAgent === 'all') return items;
  return items.filter((item) => (item.agent || 'aerial') === selectedAgent);
}

/** Tab badge counts shown next to each tab label. */
export const tasksBadgeCount = (cluster: ClusterResponse | null, agent: string = 'all'): number => {
  if (!cluster) return 0;
  if (!agent || agent === 'all') return cluster.active_tasks.length;
  return cluster.active_tasks.filter((t) => (t.agent || 'aerial') === agent).length;
};

export const schedulesBadgeCount = (schedules: SchedulesAPIResponse | null, agent: string = 'all'): number => {
  if (schedules === null) return 0;
  if (!agent || agent === 'all') return schedules.summary.total_active;
  const crons = schedules.crons.filter((c) => c.enabled && (c.agent || 'aerial') === agent).length;
  const oneShots = schedules.one_shots.filter((o) => (o.agent || 'aerial') === agent).length;
  return crons + oneShots;
};

export const memoryBadgeCount = (facts: FactsAPIResponse | null, agent: string = 'all'): number => {
  if (facts === null) return 0;
  if (!agent || agent === 'all') return facts.total;
  if (!facts.facts || facts.facts.length === 0) return 0;
  return facts.facts.filter((f) => (f.agent || 'aerial') === agent).length;
};

export const buildsBadgeCount = (builds: BuildsAPIResponse | null, agent: string = 'all'): number => {
  if (builds === null) return 0;
  if (!agent || agent === 'all') return builds.running_count + builds.queued_count;
  if (!builds.builds || builds.builds.length === 0) return 0;
  return builds.builds.filter((b) => {
    if ((b.agent || 'aerial') !== agent) return false;
    const s = b.status?.toLowerCase();
    return s === 'running' || s === 'queued' || s === 'pending';
  }).length;
};

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
export const PERMET_IDLE: PermetScore = Object.freeze({ percent: 100, label: 'Ready', tone: 'idle' });

/** Permet gauge: share of services reporting `healthy`. */
export function permetScore(services: readonly ServiceStatus[] | null): PermetScore {
  if (services === null) return PERMET_IDLE;
  const healthy = services.filter((s) => s.status === 'healthy').length;
  const ratio = services.length > 0 ? healthy / services.length : 0;
  const percent = Math.round(ratio * 100);
  const label = `${healthy} / ${services.length} Healthy`;
  if (ratio === 1) return { percent, label, tone: 'optimal' };
  if (ratio > 0.7) return { percent, label, tone: 'stable' };
  return { percent, label, tone: 'warning' };
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
