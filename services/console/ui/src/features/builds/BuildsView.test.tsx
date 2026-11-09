import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { Profiler, type ProfilerOnRenderCallback } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { BuildDetailAPIResponse, BuildsAPIResponse, VanadisBuildDTO, VanadisChunkDTO } from '../../api/types';
import { useKeyboardShortcuts } from '../../hooks/useKeyboardShortcuts';
import { useConsoleStore } from '../../store/store';
import { BuildDetailPanel } from './BuildDetail';
import BuildsView from './BuildsView';

/** An instant at local wall-clock time on 2026-11-08, so card times read the same in any TZ. */
const local = (h: number, m: number): string => new Date(2026, 10, 8, h, m).toISOString();
const NOW = Date.parse(local(12, 0));
const ZERO = '0001-01-01T00:00:00Z';

const chunk = (id: string, index: number, status: string, extra: Partial<VanadisChunkDTO> = {}): VanadisChunkDTO => ({
  id,
  build_id: 'b',
  chunk_index: index,
  title: `Chunk ${id}`,
  status,
  started_at: ZERO,
  completed_at: ZERO,
  created_at: local(10, 0),
  updated_at: local(10, 0),
  ...extra,
});

const build = (id: string, status: string, title: string, chunks: readonly VanadisChunkDTO[], extra: Partial<VanadisBuildDTO> = {}): VanadisBuildDTO => ({
  id,
  thread_id: 't',
  tier: 2,
  title,
  objective: `Ship ${title.toLowerCase()}`,
  status,
  created_at: local(10, 0),
  updated_at: local(10, 30),
  chunks,
  ...extra,
});

// b-run started 11:10 (running 50m); b-done ran 9:10-9:28 the same day; b-q has not started.
const RUN = build('b-run', 'running', 'Kiosk refresh', [
  chunk('r1', 0, 'merged', { started_at: local(11, 10), completed_at: local(11, 20), pr_number: 41, repo: 'acme/kiosk' }),
  chunk('r2', 1, 'in_progress', { started_at: local(11, 20) }),
]);
const QUEUED = build('b-q', 'queued', 'Synapse hooks', [chunk('q1', 0, 'pending')]);
const DONE = build('b-done', 'completed', 'Docs pass', [
  chunk('d1', 0, 'merged', { started_at: local(9, 10), completed_at: local(9, 28) }),
]);
const FAILED = build('b-fail', 'failed', 'Kiosk widgets', [chunk('f1', 0, 'failed', { started_at: local(8, 0), completed_at: local(8, 5) })]);
const BUILDS = [RUN, QUEUED, DONE, FAILED];

const payload = (builds: readonly VanadisBuildDTO[] = BUILDS, extra: Partial<BuildsAPIResponse> = {}): BuildsAPIResponse => ({
  status: 'ok',
  total: builds.length,
  running_count: 1,
  queued_count: 1,
  completed_count: 2,
  builds: structuredClone(builds),
  ...extra,
});

const fetchMock = vi.fn<(url: string) => Promise<Response>>();
const pending = (): Promise<Response> => new Promise(() => undefined);
const json = (body: unknown, status = 200): Promise<Response> => Promise.resolve(new Response(JSON.stringify(body), { status }));

beforeEach(() => {
  useConsoleStore.setState(useConsoleStore.getInitialState(), true);
  fetchMock.mockReset();
  fetchMock.mockImplementation(pending);
  vi.stubGlobal('fetch', fetchMock);
  vi.useFakeTimers({ now: NOW, toFake: ['Date', 'setInterval', 'clearInterval'] });
});
afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

const seed = (data: BuildsAPIResponse): void => act(() => useConsoleStore.getState().setBuilds(data));
const titles = (): string[] => [...document.querySelectorAll('.build-title')].map((el) => el.textContent ?? '');
const text = (selector: string): string => (document.querySelector(selector)?.textContent ?? '').replace(/\s+/g, ' ').trim();
const card = (id: string): HTMLElement => document.querySelector(`[data-build-id="${id}"]`) as HTMLElement;
const pill = (label: string): HTMLElement =>
  within(screen.getByRole('toolbar', { name: 'Build Queue Filter Pills' })).getByRole('button', { name: new RegExp(`^${label} `) });
const search = (): HTMLInputElement => screen.getByRole('textbox', { name: 'Search builds' });

function Shortcuts() {
  useKeyboardShortcuts('builds');
  return null;
}

describe('BuildsView lifecycle', () => {
  it.each([
    { name: 'loading', data: null, error: null, wantSkeletons: 4, wantCards: 0, wantBanner: false, wantResults: 'SHOWING 0 / 0 BUILDS', wantProgress: '0/0' },
    { name: 'ready', data: payload(), error: null, wantSkeletons: 0, wantCards: 4, wantBanner: false, wantResults: 'SHOWING 4 / 4 BUILDS', wantProgress: '2/5' },
    { name: 'empty queue', data: payload([]), error: null, wantSkeletons: 0, wantCards: 0, wantBanner: false, wantResults: 'SHOWING 0 / 0 BUILDS', wantProgress: '0/0' },
    { name: 'failed first poll keeps the skeleton', data: null, error: 'HTTP 502', wantSkeletons: 4, wantCards: 0, wantBanner: true, wantResults: 'SHOWING 0 / 0 BUILDS', wantProgress: '0/0' },
    { name: 'stale list under a failed poll', data: payload(), error: 'HTTP 502', wantSkeletons: 0, wantCards: 4, wantBanner: true, wantResults: 'SHOWING 4 / 4 BUILDS', wantProgress: '2/5' },
    { name: 'degraded payload', data: payload(BUILDS, { status: 'degraded' }), error: null, wantSkeletons: 0, wantCards: 4, wantBanner: true, wantResults: 'SHOWING 4 / 4 BUILDS', wantProgress: '2/5' },
  ])('$name', ({ data, error, wantSkeletons, wantCards, wantBanner, wantResults, wantProgress }) => {
    act(() => {
      if (data !== null) useConsoleStore.getState().setBuilds(data);
      if (error !== null) useConsoleStore.getState().failBuilds(error);
    });
    render(<BuildsView />);
    expect(document.querySelectorAll('.fact-card-skeleton')).toHaveLength(wantSkeletons);
    expect(document.querySelectorAll('.build-card')).toHaveLength(wantCards);
    expect(screen.queryByText('RECONNECTING TO VANADIS // STALE-WHILE-REVALIDATE ACTIVE') !== null).toBe(wantBanner);
    expect(text('#builds-results-count')).toBe(wantResults);
    expect(text('#builds-chunks-progress')).toBe(wantProgress);
    expect(document.querySelectorAll('.builds-empty-card')).toHaveLength(data !== null && wantCards === 0 ? 1 : 0);
  });

  it('shows metrics, pill counts and each card with its badge, stats and timing', () => {
    seed(payload());
    render(<BuildsView />);
    expect([text('#builds-running-count'), text('#builds-queued-count'), text('#builds-completed-count')]).toEqual(['1', '1', '2']);
    expect([...document.querySelectorAll('.cat-pill')].map((p) => p.textContent)).toEqual(['ALL 4', 'RUNNING 1', 'QUEUED 1', 'COMPLETED 2']);
    expect(titles()).toEqual(['Kiosk refresh', 'Synapse hooks', 'Docs pass', 'Kiosk widgets']);
    expect(text('[data-build-id="b-run"]')).toBe('RUNNING 50m 0sKiosk refreshSTART: 11/8 11:10am1 / 2 CHUNKS50%');
    expect(text('[data-build-id="b-q"]')).toBe('QUEUED --Synapse hooksSTART: 11/8 10:00am0 / 1 CHUNKS0%');
    expect(text('[data-build-id="b-done"]')).toBe('COMPLETED 18m 0sDocs passSTART: 11/8 9:10amEND: 9:28am1 / 1 CHUNKS100%');
    expect(card('b-fail').querySelector('.build-status-badge')?.className).toBe('build-status-badge status-failed');
    expect((card('b-run').querySelector('.build-progress-bar-fill') as HTMLElement).style.width).toBe('50%');
  });
});

describe('BuildsView filters', () => {
  it.each([
    { name: 'running pill', pill: 'RUNNING', query: '', want: ['Kiosk refresh'], results: 'SHOWING 1 / 4 BUILDS' },
    { name: 'queued pill', pill: 'QUEUED', query: '', want: ['Synapse hooks'], results: 'SHOWING 1 / 4 BUILDS' },
    { name: 'completed pill includes failed', pill: 'COMPLETED', query: '', want: ['Docs pass', 'Kiosk widgets'], results: 'SHOWING 2 / 4 BUILDS' },
    { name: 'search by title', pill: 'ALL', query: ' KIOSK ', want: ['Kiosk refresh', 'Kiosk widgets'], results: 'SHOWING 2 / 4 BUILDS' },
    { name: 'search by chunk repo', pill: 'ALL', query: 'acme/kiosk', want: ['Kiosk refresh'], results: 'SHOWING 1 / 4 BUILDS' },
    { name: 'pill and search combine', pill: 'COMPLETED', query: 'kiosk', want: ['Kiosk widgets'], results: 'SHOWING 1 / 4 BUILDS' },
    { name: 'no matches', pill: 'RUNNING', query: 'docs', want: [], results: 'SHOWING 0 / 4 BUILDS' },
  ])('$name', ({ pill: label, query, want, results }) => {
    seed(payload());
    render(<BuildsView />);
    fireEvent.click(pill(label));
    fireEvent.change(search(), { target: { value: query } });
    expect(titles()).toEqual(want);
    expect(text('#builds-results-count')).toBe(results);
    expect(pill(label).getAttribute('aria-pressed')).toBe('true');
    expect(screen.queryByText('NO BUILDS MATCHING SELECTION') !== null).toBe(want.length === 0);
  });

  it.each([
    { name: 'clear button', how: 'button' },
    { name: 'Escape in the search box', how: 'escape' },
  ])('clears the search with the $name', ({ how }) => {
    seed(payload());
    render(
      <>
        <Shortcuts />
        <BuildsView />
      </>,
    );
    fireEvent.change(search(), { target: { value: 'docs' } });
    expect(titles()).toEqual(['Docs pass']);
    if (how === 'button') {
      fireEvent.click(screen.getByRole('button', { name: 'Clear search' }));
    } else {
      search().focus();
      fireEvent.keyDown(window, { key: 'Escape' });
    }
    expect(search().value).toBe('');
    expect(titles()).toHaveLength(4);
  });
});

describe('BuildsView data', () => {
  it('polls the queue every 5s while mounted and stops on unmount', async () => {
    fetchMock.mockImplementation(() => json(payload()));
    const { unmount } = render(<BuildsView />);
    await act(async () => {
      await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    });
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/builds');
    expect(titles()).toHaveLength(4);
    await act(async () => vi.advanceTimersByTime(5000));
    expect(fetchMock).toHaveBeenCalledTimes(2);
    unmount();
    await act(async () => vi.advanceTimersByTime(20_000));
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it('SYNC BUILDS refetches now and records failures', async () => {
    // The mount poll settles first: a click while it is in flight joins it instead.
    seed(payload([QUEUED]));
    fetchMock.mockImplementation(() => json(payload([QUEUED, DONE])));
    render(<BuildsView />);
    await act(() => vi.waitFor(() => expect(titles()).toEqual(['Synapse hooks', 'Docs pass'])));
    fetchMock.mockImplementation(() => json({ error: 'vanadis down' }, 502));
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: /SYNC BUILDS/ }));
      await vi.waitFor(() => expect(useConsoleStore.getState().builds.list.error).toBe('vanadis down'));
    });
    expect(screen.getByText('RECONNECTING TO VANADIS // STALE-WHILE-REVALIDATE ACTIVE')).toBeTruthy();
  });
});

describe('BuildsView rendering cost', () => {
  it('an identical payload re-renders nothing; a changed build re-renders only its card', () => {
    seed(payload([QUEUED, DONE]));
    const renders: string[] = [];
    const onRender: ProfilerOnRenderCallback = (id) => renders.push(id);
    render(
      <Profiler id="builds" onRender={onRender}>
        <BuildsView />
      </Profiler>,
    );
    const doneCard = card('b-done');
    renders.length = 0;
    seed(payload([QUEUED, DONE]));
    expect(renders).toEqual([]);
    seed(payload([{ ...QUEUED, title: 'Synapse hooks v2' }, DONE]));
    expect(titles()).toEqual(['Synapse hooks v2', 'Docs pass']);
    expect(card('b-done')).toBe(doneCard);
  });

  it('only a running card ticks on the shared clock', async () => {
    seed(payload([RUN, DONE]));
    render(<BuildsView />);
    const doneText = text('[data-build-id="b-done"]');
    vi.setSystemTime(NOW + 61_000);
    await act(async () => vi.advanceTimersByTime(1000));
    expect(text('[data-build-id="b-run"] .build-duration-badge')).toBe('51m 2s');
    expect(text('[data-build-id="b-done"]')).toBe(doneText);
  });
});

describe('BuildCard opens the drawer', () => {
  it.each([
    { name: 'click', key: null, want: 'b-q' },
    { name: 'Enter', key: 'Enter', want: 'b-q' },
    { name: 'Space', key: ' ', want: 'b-q' },
    { name: 'other keys do nothing', key: 'a', want: null },
  ])('$name', ({ key, want }) => {
    seed(payload());
    render(<BuildsView />);
    if (key === null) fireEvent.click(card('b-q'));
    else fireEvent.keyDown(card('b-q'), { key });
    const drawer = useConsoleStore.getState().drawer;
    expect(drawer.kind === 'build' ? drawer.id : null).toBe(want);
  });

  it('a card without an id does not open a drawer', () => {
    seed(payload([build('', 'queued', 'Anonymous', [])]));
    render(<BuildsView />);
    fireEvent.click(screen.getByText('Anonymous'));
    expect(useConsoleStore.getState().drawer.kind).toBe('closed');
  });
});

// ---- detail drawer body ----

const DETAIL_BUILD = build('b-run', 'running', 'Kiosk refresh (live)', [], {
  shared_context: 'Layout: **ui** only.',
});
delete (DETAIL_BUILD as { chunks?: unknown }).chunks;
const DETAIL_CHUNKS: readonly VanadisChunkDTO[] = [
  chunk('r1', 0, 'merged', {
    pr_number: 41,
    repo: 'acme/kiosk',
    effort: 'low',
    instructions: '## Objective\n\n- keep the **budget**\n- add `tests`\n\n```ts\nconst ok = true;\n```',
    target_files: ['src/a.ts'],
    target_symbols: ['symbolA'],
    acceptance_criteria: ['Tests **pass**'],
  }),
  chunk('r2', 1, 'in_progress', { title: '' }),
];
const detailPayload = (b: VanadisBuildDTO = DETAIL_BUILD, chunks = DETAIL_CHUNKS): BuildDetailAPIResponse => ({
  status: 'ok',
  build: b,
  chunks: structuredClone(chunks),
});

const renderDetail = (id: string) => render(<BuildDetailPanel target={{ id }} />);

describe('BuildDetailPanel', () => {
  it('renders the queue entry at once, then the fetched detail', async () => {
    seed(payload());
    fetchMock.mockImplementation(() => json(detailPayload()));
    renderDetail('b-run');
    expect(screen.getByText('Kiosk refresh')).toBeTruthy();
    await act(async () => {
      await vi.waitFor(() => expect(screen.getByText('Kiosk refresh (live)')).toBeTruthy());
    });
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/builds/b-run');
    expect(text('.build-drawer-title-row')).toBe('RUNNINGTIER 2b-run');
    expect(text('.build-objective-text')).toBe('Ship kiosk refresh (live)');
    expect(document.querySelector('.build-context-box strong')?.textContent).toBe('ui');
    expect(text('.build-drawer-progress-bar .build-chunk-progress-label')).toBe('1 / 2 CHUNKS COMPLETED');
    expect(text('.chunks-section-title')).toBe('ORDERED BUILD CHUNKS (2)');
    expect([...document.querySelectorAll('.chunk-title')].map((t) => t.textContent)).toEqual(['Chunk r1', 'Chunk 1']);
    expect([...document.querySelectorAll('.chunk-status-badge')].map((b) => b.className)).toEqual([
      'chunk-status-badge chunk-status-merged',
      'chunk-status-badge chunk-status-in_progress',
    ]);
    expect(screen.getByRole('link', { name: /#41/ }).getAttribute('href')).toBe('https://github.com/acme/kiosk/pull/41');
    expect(screen.getByRole('heading', { name: 'Objective', level: 2 })).toBeTruthy();
    expect([...document.querySelectorAll('.chunk-instructions-box li')].map((li) => li.textContent)).toEqual(['keep the budget', 'add tests']);
    expect(document.querySelector('.chunk-instructions-box pre code')?.textContent).toBe('const ok = true;');
    expect([text('.file-tag'), text('.symbol-tag'), text('.chunk-criteria-list li')]).toEqual(['src/a.ts', 'symbolA', 'Tests pass']);
    expect(text('.chunk-effort-badge')).toBe('low');
  });

  it('polls while open, keeps an expanded chunk open across polls and stops on close', async () => {
    seed(payload());
    fetchMock.mockImplementation(() => json(detailPayload()));
    const { unmount } = renderDetail('b-run');
    await act(async () => {
      await vi.waitFor(() => expect(screen.getByText('Kiosk refresh (live)')).toBeTruthy());
    });
    const details = document.querySelector('details') as HTMLDetailsElement;
    details.open = true;
    await act(async () => vi.advanceTimersByTime(5000));
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(document.querySelector('details')).toBe(details);
    expect(details.open).toBe(true);
    unmount();
    await act(async () => vi.advanceTimersByTime(20_000));
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it.each([
    { name: 'loading', fetchResult: 'pending', want: 'LOADING BUILD TELEMETRY // b-x...' },
    { name: 'failed', fetchResult: 'error', want: 'FAILED TO LOAD BUILD DETAILS' },
  ])('without a queue entry: $name', async ({ fetchResult, want }) => {
    if (fetchResult === 'error') fetchMock.mockImplementation(() => json({ error: 'no such build' }, 404));
    renderDetail('b-x');
    await act(async () => {
      await vi.waitFor(() => expect(screen.getByText(want)).toBeTruthy());
    });
    expect(screen.queryByText('no such build') !== null).toBe(fetchResult === 'error');
  });

  it('renders the no-chunks state and falls back to defaults for a sparse build', async () => {
    const sparse = { ...DETAIL_BUILD, id: 'b-s', title: '', objective: '', tier: 0 };
    delete (sparse as { shared_context?: unknown }).shared_context;
    fetchMock.mockImplementation(() => json(detailPayload(sparse, [])));
    renderDetail('b-s');
    await act(async () => {
      await vi.waitFor(() => expect(screen.getByText('NO CHUNKS REGISTERED')).toBeTruthy());
    });
    expect(text('.build-drawer-title')).toBe('b-s');
    expect(text('.build-tier-badge')).toBe('TIER 0');
    expect(text('.build-objective-text')).toBe('No objective provided');
    expect(document.querySelector('.build-context-box')).toBeNull();
  });

  it('switching builds restarts the poll for the new id', async () => {
    seed(payload());
    const { rerender } = renderDetail('b-run');
    rerender(<BuildDetailPanel target={{ id: 'b-q' }} />);
    expect(fetchMock.mock.calls.map((c) => c[0])).toEqual(['/api/builds/b-run', '/api/builds/b-q']);
    expect(screen.getByText('Synapse hooks')).toBeTruthy();
  });
});
