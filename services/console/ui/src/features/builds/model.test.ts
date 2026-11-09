import { describe, expect, it } from 'vitest';
import type { BuildDetailAPIResponse, BuildsAPIResponse, VanadisBuildDTO, VanadisChunkDTO } from '../../api/types';
import {
  chunkNumber,
  chunkProgress,
  chunkPrUrl,
  chunkStatusBadge,
  filterBuilds,
  findBuild,
  indexBuilds,
  normalizeQuery,
  pillCounts,
  resolveBuildDetail,
  resultsLabel,
  showsReconnect,
} from './model';

const chunk = (id: string, status: string, repo?: string): VanadisChunkDTO => ({
  id,
  build_id: 'b',
  chunk_index: 0,
  title: id,
  status,
  started_at: '2026-11-08T11:00:00Z',
  completed_at: '0001-01-01T00:00:00Z',
  created_at: '2026-11-08T11:00:00Z',
  updated_at: '2026-11-08T11:00:00Z',
  ...(repo === undefined ? {} : { repo }),
});

const build = (id: string, status: string, title: string, chunks: readonly VanadisChunkDTO[] = []): VanadisBuildDTO => ({
  id,
  thread_id: 't',
  tier: 1,
  title,
  objective: `Ship ${title.toLowerCase()}`,
  status,
  created_at: '2026-11-08T10:00:00Z',
  updated_at: '2026-11-08T10:30:00Z',
  chunks,
});

const BUILDS: readonly VanadisBuildDTO[] = [
  build('b-run', 'running', 'Kiosk refresh', [chunk('c1', 'merged', 'acme/kiosk'), chunk('c2', 'in_progress')]),
  build('b-wait', 'running', 'Memory decay', [chunk('c3', 'pending'), chunk('c4', 'pending')]),
  build('b-q', 'queued', 'Synapse hooks', [chunk('c5', 'pending', 'acme/sidecars')]),
  build('b-done', 'completed', 'Kiosk widgets', [chunk('c6', 'completed'), chunk('c7', 'merged')]),
  build('b-fail', 'failed', 'Docs pass', [chunk('c8', 'failed'), chunk('c9', 'merged')]),
];
const INDEX = indexBuilds(BUILDS);
const ids = (list: ReturnType<typeof filterBuilds>): string[] => list.map((b) => b.build.id);

describe('filterBuilds', () => {
  it.each([
    { name: 'all, no query keeps the index', filter: 'ALL', query: '', want: ['b-run', 'b-wait', 'b-q', 'b-done', 'b-fail'] },
    { name: 'running', filter: 'RUNNING', query: '', want: ['b-run'] },
    { name: 'queued includes running builds with only pending chunks', filter: 'QUEUED', query: '', want: ['b-wait', 'b-q'] },
    { name: 'completed includes failed', filter: 'COMPLETED', query: '', want: ['b-done', 'b-fail'] },
    { name: 'title', filter: 'ALL', query: 'kiosk', want: ['b-run', 'b-done'] },
    { name: 'objective', filter: 'ALL', query: 'ship docs', want: ['b-fail'] },
    { name: 'chunk repo', filter: 'ALL', query: 'sidecars', want: ['b-q'] },
    { name: 'default repo for chunks without one', filter: 'ALL', query: 'azylman/aerial', want: ['b-run', 'b-wait', 'b-done', 'b-fail'] },
    { name: 'id', filter: 'ALL', query: 'b-wait', want: ['b-wait'] },
    { name: 'pill and query combine', filter: 'COMPLETED', query: 'kiosk', want: ['b-done'] },
    { name: 'no match', filter: 'RUNNING', query: 'docs', want: [] },
  ] as const)('$name', ({ filter, query, want }) => {
    expect(ids(filterBuilds(INDEX, filter, query))).toEqual(want);
  });

  it('returns the index itself when nothing filters and reuses entries for unchanged builds', () => {
    expect(filterBuilds(INDEX, 'ALL', '')).toBe(INDEX);
    expect(indexBuilds(BUILDS)[0]).toBe(INDEX[0]);
  });
});

const payload = (total: number, builds: readonly VanadisBuildDTO[] = BUILDS, extra: Partial<BuildsAPIResponse> = {}): BuildsAPIResponse => ({
  status: 'ok',
  total,
  running_count: 1,
  queued_count: 2,
  completed_count: 2,
  builds,
  ...extra,
});

describe('pillCounts', () => {
  it.each([
    { name: 'before the first payload', data: null, want: { ALL: 0, RUNNING: 0, QUEUED: 0, COMPLETED: 0 } },
    { name: 'server counters, list size for ALL', data: payload(3), want: { ALL: 5, RUNNING: 1, QUEUED: 2, COMPLETED: 2 } },
    { name: 'reported total when larger than the page', data: payload(40), want: { ALL: 40, RUNNING: 1, QUEUED: 2, COMPLETED: 2 } },
  ])('$name', ({ data, want }) => {
    expect(pillCounts(data, INDEX)).toEqual(want);
  });
});

describe('summary derivations', () => {
  it.each([
    { name: 'chunk progress over every build', got: chunkProgress(INDEX), want: '4/9' },
    { name: 'empty progress', got: chunkProgress([]), want: '0/0' },
    { name: 'results label', got: resultsLabel(2, 5), want: 'SHOWING 2 / 5 BUILDS' },
    { name: 'query normalization', got: normalizeQuery('  KiOsk '), want: 'kiosk' },
  ])('$name', ({ got, want }) => {
    expect(got).toBe(want);
  });

  it.each([
    { name: 'healthy', data: payload(5), error: null, want: false },
    { name: 'before any payload', data: null, error: null, want: false },
    { name: 'failed poll', data: null, error: 'HTTP 502', want: true },
    { name: 'degraded payload', data: payload(5, BUILDS, { status: 'degraded' }), error: null, want: true },
    { name: 'payload error', data: payload(5, BUILDS, { error: 'vanadis down' }), error: null, want: true },
  ])('reconnect banner: $name', ({ data, error, want }) => {
    expect(showsReconnect(data, error)).toBe(want);
  });
});

const detail = (b: VanadisBuildDTO | undefined, chunks: readonly VanadisChunkDTO[], key: 'build' | 'entry' = 'build'): BuildDetailAPIResponse => ({
  status: 'ok',
  chunks,
  ...(b === undefined ? {} : { [key]: b }),
});
const DETAIL_BUILD = build('b-run', 'running', 'Kiosk refresh (detail)');
const DETAIL_CHUNKS = [chunk('d1', 'merged')];

describe('resolveBuildDetail', () => {
  it.each([
    { name: 'fetched detail wins', detail: detail(DETAIL_BUILD, DETAIL_CHUNKS), entry: BUILDS[0], wantTitle: 'Kiosk refresh (detail)', wantChunks: ['d1'] },
    { name: 'legacy entry key', detail: detail(DETAIL_BUILD, DETAIL_CHUNKS, 'entry'), entry: undefined, wantTitle: 'Kiosk refresh (detail)', wantChunks: ['d1'] },
    { name: 'another build detail falls back to the queue entry', detail: detail(build('b-q', 'queued', 'Other'), DETAIL_CHUNKS), entry: BUILDS[0], wantTitle: 'Kiosk refresh', wantChunks: ['c1', 'c2'] },
    { name: 'queue entry before the detail', detail: null, entry: BUILDS[0], wantTitle: 'Kiosk refresh', wantChunks: ['c1', 'c2'] },
    { name: 'nothing yet', detail: null, entry: undefined, wantTitle: null, wantChunks: [] },
    { name: 'detail without a build', detail: detail(undefined, DETAIL_CHUNKS), entry: undefined, wantTitle: null, wantChunks: [] },
  ])('$name', ({ detail: d, entry, wantTitle, wantChunks }) => {
    const got = resolveBuildDetail('b-run', d, entry);
    expect(got?.build.title ?? null).toBe(wantTitle);
    expect((got?.chunks ?? []).map((c) => c.id)).toEqual(wantChunks);
  });

  it('defaults missing chunk arrays to empty', () => {
    const bare = { ...build('b-x', 'queued', 'Bare') };
    delete (bare as { chunks?: unknown }).chunks;
    expect(resolveBuildDetail('b-x', null, bare)?.chunks).toEqual([]);
  });
});

describe('chunk helpers', () => {
  it.each([
    { name: 'own index', got: chunkNumber({ chunk_index: 0 }, 4), want: 0 },
    { name: 'position fallback', got: chunkNumber({}, 4), want: 5 },
  ])('chunkNumber: $name', ({ got, want }) => {
    expect(got).toBe(want);
  });

  it.each([
    { status: 'merged', want: { label: 'MERGED', className: 'chunk-status-badge chunk-status-merged' } },
    { status: 'In_Progress', want: { label: 'IN_PROGRESS', className: 'chunk-status-badge chunk-status-in_progress' } },
    { status: undefined, want: { label: 'PENDING', className: 'chunk-status-badge chunk-status-pending' } },
  ])('chunkStatusBadge($status)', ({ status, want }) => {
    expect(chunkStatusBadge(status)).toEqual(want);
  });

  it.each([
    { name: 'no PR', chunk: {}, want: null },
    { name: 'own repo', chunk: { pr_number: 12, repo: 'acme/kiosk' }, want: 'https://github.com/acme/kiosk/pull/12' },
    { name: 'default repo', chunk: { pr_number: 7 }, want: 'https://github.com/azylman/aerial/pull/7' },
  ])('chunkPrUrl: $name', ({ chunk: c, want }) => {
    expect(chunkPrUrl(c)).toBe(want);
  });

  it.each([
    { name: 'present', data: payload(5), id: 'b-q', want: 'b-q' },
    { name: 'missing', data: payload(5), id: 'nope', want: undefined },
    { name: 'no payload', data: null, id: 'b-q', want: undefined },
  ])('findBuild: $name', ({ data, id, want }) => {
    expect(findBuild(data, id)?.id).toBe(want);
  });
});
