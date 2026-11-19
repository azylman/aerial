// Pure derivations for the build queue and its detail drawer: pills, metrics, cards and chunk
// detail. A build is indexed once per object (polls are reconciled, so an unchanged
// build keeps its reference and its index entry) and a filter is one linear pass.

import { Graph, layout as dagreLayout } from '@dagrejs/dagre';
import type { BuildDetailAPIResponse, BuildsAPIResponse, VanadisBuildDTO, VanadisChunkDTO } from '../../api/types';
import { type BuildChunkStats, type BuildQueueState, classifyBuildQueueState, computeBuildChunkStats } from '../../lib/builds';
import { buildFromDetail } from '../../store/selectors';

export type BuildFilter = 'ALL' | 'RUNNING' | 'QUEUED' | 'COMPLETED';

export const BUILD_FILTERS: readonly BuildFilter[] = ['ALL', 'RUNNING', 'QUEUED', 'COMPLETED'];

/** Repo a chunk without one belongs to (queue search, chunk PR links). */
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

/** Queue search query normalization. */
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
 * Pill counts. The server's bucket counters win; ALL is the reported total
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

// ---- topological DAG layout (@dagrejs/dagre Sugiyama layout) ----

export const DAG_NODE_WIDTH = 220;
export const DAG_NODE_HEIGHT = 58;
export const DAG_NODE_SEP = 14;
export const DAG_RANK_SEP = 30;
export const DAG_MARGIN = 10;

export type DagEdgeTone = 'completed' | 'active' | 'failed' | 'pending';

export interface DagPoint {
  readonly x: number;
  readonly y: number;
}

export interface ChunkDagNode {
  readonly chunk: VanadisChunkDTO;
  readonly number: number;
  readonly status: ChunkStatusBadge;
  readonly rank: number;
  readonly dependsOn: readonly string[];
  readonly x: number;
  readonly y: number;
  readonly width: number;
  readonly height: number;
  readonly hasOutEdge: boolean;
}

export interface ChunkDagEdge {
  readonly fromId: string;
  readonly toId: string;
  readonly fromRank: number;
  readonly fromIndex: number;
  readonly fromCount: number;
  readonly toRank: number;
  readonly toIndex: number;
  readonly toCount: number;
  readonly points: readonly DagPoint[];
  readonly path: string;
  readonly tone: DagEdgeTone;
}

export interface ChunkDagLayout {
  readonly width: number;
  readonly height: number;
  readonly nodes: readonly ChunkDagNode[];
  readonly ranks: readonly (readonly ChunkDagNode[])[];
  readonly edges: readonly ChunkDagEdge[];
}

function classifyEdgeTone(fromStatus: string | undefined, toStatus: string | undefined): DagEdgeTone {
  const from = (fromStatus || 'pending').toLowerCase();
  const to = (toStatus || 'pending').toLowerCase();
  if (
    from === 'failed' ||
    from === 'ci_failed' ||
    from === 'deploy_failed' ||
    from === 'conflict' ||
    to === 'failed' ||
    to === 'ci_failed' ||
    to === 'deploy_failed' ||
    to === 'conflict'
  ) {
    return 'failed';
  }
  if (
    to === 'running' ||
    to === 'in_progress' ||
    to === 'coding' ||
    to === 'pr_opened' ||
    to === 'deploying'
  ) {
    return 'active';
  }
  if ((from === 'completed' || from === 'merged') && (to === 'completed' || to === 'merged')) {
    return 'completed';
  }
  return 'pending';
}

function buildCubicEdgePath(points: readonly DagPoint[]): string {
  const start = points[0]!;
  const end = points[points.length - 1]!;
  if (points.length <= 2) {
    const midY = Math.round(((start.y + end.y) / 2) * 10) / 10;
    return `M ${start.x} ${start.y} C ${start.x} ${midY}, ${end.x} ${midY}, ${end.x} ${end.y}`;
  }
  let d = `M ${start.x} ${start.y}`;
  for (let i = 0; i < points.length - 1; i++) {
    const p0 = points[i]!;
    const p1 = points[i + 1]!;
    const midY = Math.round(((p0.y + p1.y) / 2) * 10) / 10;
    d += ` C ${p0.x} ${midY}, ${p1.x} ${midY}, ${p1.x} ${p1.y}`;
  }
  return d;
}

/**
 * Builds a Sugiyama topological DAG layout for Vanadis build chunks using @dagrejs/dagre.
 * Ranks are 0-based depth levels (0 for roots, 1 + max(parentRank) for dependents), and
 * every node and edge receives pixel-accurate coordinates and SVG cubic-bezier routing paths.
 */
export function buildChunkDag(chunks: readonly VanadisChunkDTO[]): ChunkDagLayout {
  if (chunks.length === 0) {
    return { width: 0, height: 0, nodes: [], ranks: [], edges: [] };
  }

  const hasExplicitDependsOn = chunks.some((c) => Array.isArray(c.depends_on));
  const chunkIds = new Set(chunks.map((c) => c.id));

  const parentMap = new Map<string, readonly string[]>();
  const outDegree = new Set<string>();
  for (let i = 0; i < chunks.length; i++) {
    const c = chunks[i]!;
    const deps = hasExplicitDependsOn
      ? Array.isArray(c.depends_on)
        ? c.depends_on.filter((depId) => depId !== c.id && chunkIds.has(depId))
        : []
      : i > 0 && chunks[i - 1]
        ? [chunks[i - 1]!.id]
        : [];
    parentMap.set(c.id, deps);
    for (const pid of deps) outDegree.add(pid);
  }

  const memo = new Map<string, number>();
  const visiting = new Set<string>();

  function computeRank(id: string): number {
    if (memo.has(id)) return memo.get(id)!;
    if (visiting.has(id)) return 0;
    visiting.add(id);
    const parents = parentMap.get(id) ?? [];
    let maxParent = -1;
    for (const pid of parents) {
      if (visiting.has(pid)) continue;
      const pr = computeRank(pid);
      if (pr > maxParent) maxParent = pr;
    }
    visiting.delete(id);
    const rank = maxParent >= 0 ? 1 + maxParent : 0;
    memo.set(id, rank);
    return rank;
  }

  for (const c of chunks) {
    computeRank(c.id);
  }

  const g = new Graph({ multigraph: false, compound: false });
  g.setGraph({
    rankdir: 'TB',
    nodesep: DAG_NODE_SEP,
    ranksep: DAG_RANK_SEP,
    marginx: DAG_MARGIN,
    marginy: DAG_MARGIN,
  });
  g.setDefaultEdgeLabel(() => ({}));

  // Insert nodes and edges so dagre's DFS initial ordering places earlier chunks on the left.
  for (const c of chunks) {
    g.setNode(c.id, { width: DAG_NODE_WIDTH, height: DAG_NODE_HEIGHT });
  }
  for (let i = chunks.length - 1; i >= 0; i--) {
    const c = chunks[i]!;
    const toRank = memo.get(c.id) ?? 0;
    const deps = parentMap.get(c.id) ?? [];
    for (let j = deps.length - 1; j >= 0; j--) {
      const pid = deps[j]!;
      const fromRank = memo.get(pid) ?? 0;
      if (fromRank < toRank) {
        g.setEdge(pid, c.id);
      }
    }
  }
  dagreLayout(g);

  const nodes: ChunkDagNode[] = chunks.map((chunk, position) => {
    const layoutNode = g.node(chunk.id);
    return {
      chunk,
      number: chunkNumber(chunk, position),
      status: chunkStatusBadge(chunk.status),
      rank: memo.get(chunk.id) ?? 0,
      dependsOn: parentMap.get(chunk.id) ?? [],
      x: Math.round(layoutNode.x),
      y: Math.round(layoutNode.y),
      width: DAG_NODE_WIDTH,
      height: DAG_NODE_HEIGHT,
      hasOutEdge: outDegree.has(chunk.id),
    };
  });

  let maxRank = 0;
  for (const node of nodes) {
    if (node.rank > maxRank) maxRank = node.rank;
  }

  const rankBuckets: ChunkDagNode[][] = Array.from({ length: maxRank + 1 }, () => []);
  for (const node of nodes) {
    rankBuckets[node.rank]!.push(node);
  }
  for (const bucket of rankBuckets) {
    bucket.sort((a, b) => a.x - b.x);
  }

  const nodeMap = new Map<string, ChunkDagNode>();
  for (const node of nodes) {
    nodeMap.set(node.chunk.id, node);
  }

  const edges: ChunkDagEdge[] = [];
  for (const node of nodes) {
    for (const parentId of node.dependsOn) {
      const parentNode = nodeMap.get(parentId);
      if (!parentNode) continue;
      const fromRank = parentNode.rank;
      const fromRankNodes = rankBuckets[fromRank] ?? [];
      const fromIndex = fromRankNodes.findIndex((n) => n.chunk.id === parentId);
      const toRank = node.rank;
      const toRankNodes = rankBuckets[toRank] ?? [];
      const toIndex = toRankNodes.findIndex((n) => n.chunk.id === node.chunk.id);

      const startPort: DagPoint = { x: parentNode.x, y: parentNode.y + Math.round(parentNode.height / 2) };
      const endPort: DagPoint = { x: node.x, y: node.y - Math.round(node.height / 2) };
      const dagreEdge = g.edge(parentId, node.chunk.id);
      const interior =
        dagreEdge && Array.isArray(dagreEdge.points) && dagreEdge.points.length > 2 && Math.abs(toRank - fromRank) > 1
          ? (dagreEdge.points as readonly DagPoint[])
              .slice(1, -1)
              .map((p: DagPoint) => ({ x: Math.round(p.x), y: Math.round(p.y) }))
          : [];
      const points: readonly DagPoint[] = [startPort, ...interior, endPort];

      edges.push({
        fromId: parentId,
        toId: node.chunk.id,
        fromRank,
        fromIndex,
        fromCount: fromRankNodes.length,
        toRank,
        toIndex,
        toCount: toRankNodes.length,
        points,
        path: buildCubicEdgePath(points),
        tone: classifyEdgeTone(parentNode.chunk.status, node.chunk.status),
      });
    }
  }

  const graphMeta = g.graph();
  const width = Math.max(Math.round(graphMeta.width ?? 0), DAG_NODE_WIDTH + DAG_MARGIN * 2);
  const height = Math.max(Math.round(graphMeta.height ?? 0), DAG_NODE_HEIGHT + DAG_MARGIN * 2);

  return { width, height, nodes, ranks: rankBuckets, edges };
}
