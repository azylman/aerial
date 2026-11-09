import { act, cleanup, render, screen } from '@testing-library/react';
import { Profiler, type ProfilerOnRenderCallback } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import buildsRaw from '../e2e/fixtures/builds.json?raw';
import factsRaw from '../e2e/fixtures/facts.json?raw';
import runsRaw from '../e2e/fixtures/schedule-runs.json?raw';
import schedulesRaw from '../e2e/fixtures/schedules.json?raw';
import statusRaw from '../e2e/fixtures/status.json?raw';
import type {
  BuildsAPIResponse,
  ClusterResponse,
  FactsAPIResponse,
  ScheduleRunsAPIResponse,
  SchedulesAPIResponse,
} from './api/types';
import { App, statusPolledOn, VIEW_LOADERS } from './App';
import { initRouteFromLocation } from './hooks/useHashRoute';
import { useConsoleStore } from './store/store';

const BODIES: Readonly<Record<string, unknown>> = {
  '/console/api/status': {
    system_time: '2026-11-08T12:00:00Z',
    active_tasks: [{ id: 't1' }],
    services: [{ name: 'eri', status: 'healthy' }],
    quick_launch_links: [],
    git_sync: { status: 'synced', max_lag_seconds: 0, last_sync_time: '2026-11-08T12:00:00Z' },
  },
  '/console/api/schedules': { status: 'ok', summary: { total_active: 3 }, crons: [], one_shots: [] },
  '/console/api/facts': { facts: [], total: 9, limit: 0, offset: 0 },
  '/console/api/builds': { status: 'ok', running_count: 1, queued_count: 1, completed_count: 0, builds: [] },
};

let requests: string[] = [];

function stubFetch(): void {
  requests = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((url: string) => {
      requests.push(url);
      return Promise.resolve(new Response(JSON.stringify(BODIES[url] ?? {}), { status: url in BODIES ? 200 : 404 }));
    }),
  );
}

const count = (url: string): number => requests.filter((r) => r === url).length;

async function flush(): Promise<void> {
  await act(async () => {
    await vi.dynamicImportSettled();
  });
}

describe('statusPolledOn', () => {
  it.each([
    { tab: 'telemetry', want: true },
    { tab: 'tasks', want: true },
    { tab: 'schedules', want: false },
    { tab: 'memory', want: false },
    { tab: 'builds', want: false },
  ] as const)('$tab -> $want', ({ tab, want }) => {
    expect(statusPolledOn(tab)).toBe(want);
  });
});

describe('App', () => {
  beforeEach(() => {
    useConsoleStore.setState(useConsoleStore.getInitialState(), true);
    stubFetch();
    vi.useFakeTimers({ toFake: ['setInterval', 'clearInterval', 'setTimeout', 'clearTimeout', 'Date'] });
  });
  afterEach(() => {
    cleanup();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it.each([
    { name: 'default route', hash: '', wantPanel: 'telemetry-view', wantHash: '#telemetry', wantStatus: 1 },
    { name: 'tasks deep link', hash: '#tasks', wantPanel: 'tasks-view', wantHash: '#tasks', wantStatus: 1 },
    { name: 'memory deep link skips status', hash: '#memory', wantPanel: 'memory-view', wantHash: '#memory', wantStatus: 0 },
  ])('$name mounts only the active view and loads badge data once', async ({ hash, wantPanel, wantHash, wantStatus }) => {
    window.history.replaceState(null, '', `/console/next/${hash}`);
    initRouteFromLocation();
    render(<App />);
    await flush();

    const panels = screen.getAllByRole('tabpanel');
    expect(panels.map((p) => p.id)).toEqual([wantPanel]);
    expect(window.location.hash).toBe(wantHash);
    expect(count('/console/api/status')).toBe(wantStatus);
    expect([count('/console/api/schedules'), count('/console/api/facts'), count('/console/api/builds')]).toEqual([1, 1, 1]);
    expect(screen.getByRole('tab', { name: /MEMORY ARCHIVE/ }).textContent).toContain('9');
    expect(screen.getByRole('tab', { name: /BUILD QUEUE/ }).textContent).toContain('2');
  });

  it('records upstream failures per resource and keeps the shell up', async () => {
    vi.stubGlobal('fetch', vi.fn(() => Promise.resolve(new Response('{"error":"upstream down"}', { status: 503 }))));
    window.history.replaceState(null, '', '/console/next/');
    initRouteFromLocation();
    render(<App />);
    await flush();
    const s = useConsoleStore.getState();
    expect([s.status.cluster.error, s.schedules.list.error, s.memory.facts.error, s.builds.list.error]).toEqual([
      'upstream down',
      'upstream down',
      'upstream down',
      'upstream down',
    ]);
    expect(screen.getByText('LVL 5 // ACTIVE')).toBeTruthy();
  });

  it('polls status every 5s on telemetry and stops when leaving for schedules', async () => {
    window.history.replaceState(null, '', '/console/next/');
    initRouteFromLocation();
    render(<App />);
    await flush();
    await act(async () => vi.advanceTimersByTime(5000));
    expect(count('/console/api/status')).toBe(2);
    expect(screen.getByRole('tab', { name: /ACTIVE TASKS/ }).textContent).toContain('1');

    await act(async () => screen.getByRole('tab', { name: /SCHEDULES/ }).click());
    await flush();
    expect(window.location.hash).toBe('#schedules');
    expect(screen.getAllByRole('tabpanel').map((p) => p.id)).toEqual(['schedules-view']);

    await act(async () => vi.advanceTimersByTime(20_000));
    expect(count('/console/api/status')).toBe(2);
  });

  it.each([
    { name: 'prefetches every view chunk when idle', idle: true },
    { name: 'falls back to a timeout without requestIdleCallback', idle: false },
  ])('$name', async ({ idle }) => {
    const idleCallbacks: IdleRequestCallback[] = [];
    vi.stubGlobal('requestIdleCallback', idle ? (cb: IdleRequestCallback) => idleCallbacks.push(cb) : undefined);
    vi.stubGlobal('cancelIdleCallback', () => undefined);
    window.history.replaceState(null, '', '/console/next/#builds');
    initRouteFromLocation();
    const { unmount } = render(<App />);
    await flush();
    if (idle) {
      expect(idleCallbacks).toHaveLength(1);
      act(() => idleCallbacks[0]?.({ didTimeout: false, timeRemaining: () => 50 }));
    } else {
      act(() => vi.advanceTimersByTime(1));
    }
    await flush();
    // Every view is now evaluated: switching tabs renders synchronously with no fallback.
    await act(async () => screen.getByRole('tab', { name: /MEMORY ARCHIVE/ }).click());
    expect(screen.getAllByRole('tabpanel').map((p) => p.id)).toEqual(['memory-view']);
    unmount();
  });
});

// ---- fully seeded walk and cross-feature isolation (fixtures shared with the parity suite) ----

const STATUS = JSON.parse(statusRaw) as ClusterResponse;
const SCHEDULES = JSON.parse(schedulesRaw) as SchedulesAPIResponse;
const RUNS = JSON.parse(runsRaw) as ScheduleRunsAPIResponse;
const FACTS = JSON.parse(factsRaw) as FactsAPIResponse;
const BUILDS = JSON.parse(buildsRaw) as BuildsAPIResponse;

const FIXTURE_BODIES: Readonly<Record<string, string>> = {
  '/console/api/status': statusRaw,
  '/console/api/schedules': schedulesRaw,
  '/console/api/schedules/runs?limit=50&offset=0': runsRaw,
  '/console/api/facts': factsRaw,
  '/console/api/builds': buildsRaw,
};

function seedAll(): void {
  const s = useConsoleStore.getState();
  s.setCluster(structuredClone(STATUS));
  s.setSchedules(structuredClone(SCHEDULES));
  s.setScheduleRuns(structuredClone(RUNS));
  s.setFacts(structuredClone(FACTS));
  s.setBuilds(structuredClone(BUILDS));
}

describe('App with a fully seeded store', () => {
  beforeEach(() => {
    useConsoleStore.setState(useConsoleStore.getInitialState(), true);
    vi.stubGlobal(
      'fetch',
      vi.fn((url: string) => Promise.resolve(new Response(FIXTURE_BODIES[url] ?? '{}', { status: url in FIXTURE_BODIES ? 200 : 404 }))),
    );
    vi.useFakeTimers({ now: Date.parse('2026-11-08T12:00:00Z'), toFake: ['setInterval', 'clearInterval', 'Date'] });
    seedAll();
  });
  afterEach(() => {
    cleanup();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('walks all five tabs by hash, mounting exactly one view at a time', async () => {
    window.history.replaceState(null, '', '/console/next/#telemetry');
    initRouteFromLocation();
    render(<App />);
    const walk = [
      { hash: '#telemetry', panel: 'telemetry-view', heading: 'LIVE AGENT EXECUTION QUEUE' },
      { hash: '#tasks', panel: 'tasks-view', heading: 'LIVE DISPATCH STREAM' },
      { hash: '#schedules', panel: 'schedules-view', heading: 'ACTIVE SCHEDULED ROUTINES' },
      { hash: '#memory', panel: 'memory-view', heading: 'TOTAL MEMORY NODES' },
      { hash: '#builds', panel: 'builds-view', heading: 'RUNNING BUILDS' },
    ];
    for (const step of walk) {
      await act(async () => {
        window.location.hash = step.hash;
        window.dispatchEvent(new HashChangeEvent('hashchange'));
      });
      await flush();
      expect(screen.getAllByRole('tabpanel').map((p) => p.id)).toEqual([step.panel]);
      expect(screen.getByRole('tab', { selected: true }).id).toBe(`tab-${step.panel.replace('-view', '')}-btn`);
      expect(screen.getByText(step.heading)).toBeTruthy();
    }
    // The build drawer body ships in the builds chunk and opens from a card.
    await act(async () => screen.getByText('Console React rewrite #1').click());
    await flush();
    expect(screen.getByRole('dialog', { name: 'BUILD // BUILD-1794100000-C046DE69' })).toBeTruthy();
    expect(screen.getByText('ORDERED BUILD CHUNKS (3)')).toBeTruthy();
  });
});

const TASK_0 = STATUS.active_tasks[0];
/** Each payload differs from the seeded fixture in data its own feature renders. */
const ISOLATION: readonly {
  name: string;
  action: 'setCluster' | 'setSchedules' | 'setFacts' | 'setBuilds';
  data: ClusterResponse | SchedulesAPIResponse | FactsAPIResponse | BuildsAPIResponse;
  owners: readonly string[];
}[] = [
  {
    name: 'status',
    action: 'setCluster',
    data: { ...STATUS, active_tasks: STATUS.active_tasks.map((t) => (t === TASK_0 ? { ...t, summary: 'changed summary' } : t)) },
    owners: ['telemetry', 'tasks'],
  },
  {
    name: 'schedules',
    action: 'setSchedules',
    data: { ...SCHEDULES, crons: SCHEDULES.crons.map((c, i) => (i === 0 ? { ...c, title: 'Renamed cron' } : c)) },
    owners: ['schedules'],
  },
  {
    name: 'memory',
    action: 'setFacts',
    data: { ...FACTS, facts: FACTS.facts.map((f, i) => (i === 0 ? { ...f, fact_text: 'Rewritten fact' } : f)) },
    owners: ['memory'],
  },
  {
    name: 'builds',
    action: 'setBuilds',
    data: { ...BUILDS, builds: BUILDS.builds.map((b, i) => (i === 0 ? { ...b, title: 'Renamed build' } : b)) },
    owners: ['builds'],
  },
];
const FEATURES = ['telemetry', 'tasks', 'schedules', 'memory', 'builds'] as const;

describe('cross-feature isolation', () => {
  beforeEach(() => {
    useConsoleStore.setState(useConsoleStore.getInitialState(), true);
    vi.stubGlobal('fetch', vi.fn(() => new Promise<Response>(() => undefined)));
    vi.useFakeTimers({ now: Date.parse('2026-11-08T12:00:00Z'), toFake: ['setInterval', 'clearInterval', 'Date'] });
    seedAll();
  });
  afterEach(() => {
    cleanup();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it.each(ISOLATION)('updating $name re-renders only its own feature', async ({ action, data, owners }) => {
    const views = await Promise.all(FEATURES.map((f) => VIEW_LOADERS[f]()));
    const renders: string[] = [];
    const onRender: ProfilerOnRenderCallback = (id) => renders.push(id);
    render(
      <>
        {views.map((m, i) => (
          <Profiler key={FEATURES[i]} id={FEATURES[i] ?? ''} onRender={onRender}>
            <m.default />
          </Profiler>
        ))}
      </>,
    );
    renders.length = 0;
    act(() => {
      const s = useConsoleStore.getState();
      // The table's action names the setter; `data` matches that setter's payload type.
      (s[action] as (d: typeof data) => void)(structuredClone(data));
    });
    expect([...new Set(renders)].sort()).toEqual([...owners].sort());
  });
});
