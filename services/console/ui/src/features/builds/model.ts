// Pure derivations for the build queue and its detail drawer, ported from renderBuildsPills,
// updateBuildsMetrics, renderBuildQueueCards, openBuildDetailDrawer and renderBuildDetailContent
// in static/app.js. The vanilla UI re-classified and re-lowercased every build on each keystroke
// and each poll; here a build is indexed once per object (polls are reconciled, so an unchanged
// build keeps its reference and its index entry) and a filter is one linear pass.

import type { BuildDetailAPIResponse, BuildsAPIResponse, VanadisBuildDTO, VanadisChunkDTO } from '../../api/types';
import { type BuildChunkStats, type BuildQueueState, classifyBuildQueueState, computeBuildChunkStats } from '../../lib/builds';
import { buildFromDetail } from '../../store/selectors';

export type BuildFilter = 'ALL' | 'RUNNING' | 'QUEUED' | 'COMPLETED';

export const BUILD_FILTERS: readonly BuildFilter[] = ['ALL', 'RUNNING', 'QUEUED', 'COMPLETED'];

/** Repo a chunk without one belongs to (renderBuildQueueCards search, chunk PR links). */
export const DEFAULT_CHUNK_REPO = 'azylman/aerial';

/** One build with the values every filter pass and the metrics need. */
export interface IndexedBuild {
  readonly build: VanadisBuildDTO;
  readonly state: BuildQueueState;
  readonly stats: BuildChunkStats;
  /** Lowercased title, objective, chunk repos and id, NUL-joined so one `includes` checks all. */
  readonly haystack: string;
}

const indexCache = new WeakMap<VanadisBuildDTO, IndexedBuild>();

function indexBuild(build: VanadisBuildDTO): IndexedBuild {
  const cached = indexCache.get(build);
  if (cached !== undefined) return cached;
  const repos = (build.chunks ?? []).map((c) => c.repo || DEFAULT_CHUNK_REPO).join(' ');
  const entry: IndexedBuild = {
    build,
    state: classifyBuildQueueState(build),
    stats: computeBuildChunkStats(build),
    haystack: [build.title || '', build.objective || '', repos, build.id || ''].join('\u0000').toLowerCase(),
  };
  indexCache.set(build, entry);
  return entry;
}

/** Index entries in payload order; unchanged builds reuse their entry. */
export function indexBuilds(builds: readonly VanadisBuildDTO[]): readonly IndexedBuild[] {
  return builds.map(indexBuild);
}

/** renderBuildQueueCards' query normalization. */
export const normalizeQuery = (search: string): string => search.trim().toLowerCase();

const FILTER_STATE: Readonly<Record<Exclude<BuildFilter, 'ALL'>, BuildQueueState>> = {
  RUNNING: 'running',
  QUEUED: 'queued',
  COMPLETED: 'completed',
};

/** Builds in the pill's queue bucket matching the normalized query; ALL + no query is `index`. */
export function filterBuilds(index: readonly IndexedBuild[], filter: BuildFilter, query: string): readonly IndexedBuild[] {
  if (filter === 'ALL' && query === '') return index;
  const state = filter === 'ALL' ? null : FILTER_STATE[filter];
  return index.filter((b) => (state === null || b.state === state) && (query === '' || b.haystack.includes(query)));
}

/**
 * Pill counts (renderBuildsPills). The server's bucket counters win; ALL is the reported total
 * when it exceeds the page of builds returned.
 */
export function pillCounts(data: BuildsAPIResponse | null, index: readonly IndexedBuild[]): Readonly<Record<BuildFilter, number>> {
  if (data === null) return { ALL: 0, RUNNING: 0, QUEUED: 0, COMPLETED: 0 };
  return {
    ALL: data.total > index.length ? data.total : index.length,
    RUNNING: data.running_count,
    QUEUED: data.queued_count,
    COMPLETED: data.completed_count,
  };
}

/** CHUNK PROGRESS card (updateBuildsMetrics): completed over total chunks across every build. */
export function chunkProgress(index: readonly IndexedBuild[]): string {
  let completed = 0;
  let total = 0;
  for (const b of index) {
    completed += b.stats.completed;
    total += b.stats.total;
  }
  return `${completed}/${total}`;
}

export const resultsLabel = (shown: number, total: number): string => `SHOWING ${shown} / ${total} BUILDS`;

/** fetchBuilds shows the reconnect banner on a failed poll or a degraded payload. */
export const showsReconnect = (data: BuildsAPIResponse | null, error: string | null): boolean =>
  error !== null || data?.status === 'degraded' || Boolean(data?.error);

// ---- detail drawer ----

export interface BuildDetailModel {
  readonly build: VanadisBuildDTO;
  readonly chunks: readonly VanadisChunkDTO[];
}

const NO_CHUNKS: readonly VanadisChunkDTO[] = Object.freeze([]);

/**
 * What the drawer renders for `id`: the fetched detail once it has arrived, else the queue entry
 * (openBuildDetailDrawer's cache seed), else null (loading or failed).
 */
export function resolveBuildDetail(
  id: string,
  detail: BuildDetailAPIResponse | null,
  entry: VanadisBuildDTO | undefined,
): BuildDetailModel | null {
  const fetched = buildFromDetail(detail, id);
  if (fetched !== undefined && detail !== null) {
    return { build: fetched, chunks: detail.chunks ?? fetched.chunks ?? NO_CHUNKS };
  }
  if (entry !== undefined) return { build: entry, chunks: entry.chunks ?? NO_CHUNKS };
  return null;
}

/** The queue entry for `id` in the current list payload. */
export const findBuild = (data: BuildsAPIResponse | null, id: string): VanadisBuildDTO | undefined =>
  data?.builds.find((b) => b.id === id);

/** `#N` label: the chunk's own index when present, else its 1-based position. */
export const chunkNumber = (chunk: Partial<Pick<VanadisChunkDTO, 'chunk_index'>>, position: number): number =>
  typeof chunk.chunk_index === 'number' ? chunk.chunk_index : position + 1;

export interface ChunkStatusBadge {
  readonly label: string;
  readonly className: string;
}

/** Chunk status pill text and `chunk-status-<status>` modifier; a missing status reads pending. */
export function chunkStatusBadge(status: string | undefined): ChunkStatusBadge {
  const raw = status || 'pending';
  return { label: raw.toUpperCase(), className: `chunk-status-badge chunk-status-${raw.toLowerCase()}` };
}

/** GitHub URL for the chunk's PR, or null before one is opened. */
export function chunkPrUrl(chunk: Partial<Pick<VanadisChunkDTO, 'pr_number' | 'repo'>>): string | null {
  if (!chunk.pr_number) return null;
  return `https://github.com/${chunk.repo || DEFAULT_CHUNK_REPO}/pull/${chunk.pr_number}`;
}
