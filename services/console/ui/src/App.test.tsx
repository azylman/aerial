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
import { App } from './App';
import { startBoot, VIEW_LOADERS } from './boot';
import { initRouteFromLocation } from './hooks/useHashRoute';
import { type TabId, useConsoleStore } from './store/store';

// Each feature view is wrapped (not replaced) so a test can count how often its tree is built:
// the state initializer runs once per mount and never on a re-render or an Activity reveal.
const viewMounts = vi.hoisted(() => new Map<string, number>());
async function countingView(tab: TabId, original: () => Promise<{ default: () => unknown }>) {
  const mod = await original();
  const { createElement, useState } = await import('react');
  const View = mod.default as () => null;
  function CountedView() {
    useState(() => viewMounts.set(tab, (viewMounts.get(tab) ?? 0) + 1));
    return createElement(View);
  }
  return { ...mod, default: CountedView };
}
vi.mock('./features/telemetry/TelemetryView', (orig) => countingView('telemetry', orig));
vi.mock('./features/tasks/TasksView', (orig) => countingView('tasks', orig));
vi.mock('./features/schedules/SchedulesView', (orig) => countingView('schedules', orig));
vi.mock('./features/memory/MemoryView', (orig) => countingView('memory', orig));
vi.mock('./features/builds/BuildsView', (orig) => countingView('builds', orig));

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

/** What main.tsx does before rendering: seed the route, then start boot's chunk and fetches. */
function boot(path: string): void {
  window.history.replaceState(null, '', path);
  void startBoot(initRouteFromLocation());
}

// Each test starts an hour after the last, so boot's shared status poll never treats a previous
// test's response as fresh.
let testClock = Date.UTC(2026, 10, 9);

describe('App', () => {
  beforeEach(() => {
    useConsoleStore.setState(useConsoleStore.getInitialState(), true);
    stubFetch();
    testClock += 3_600_000;
    vi.useFakeTimers({ now: testClock, toFake: ['setInterval', 'clearInterval', 'setTimeout', 'clearTimeout', 'Date'] });
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
    boot(`/console/next/${hash}`);
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
    boot('/console/next/');
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

  it('reuses a boot status request that settled before App mounted', async () => {
    boot('/console/next/');
    await flush();
    expect(count('/console/api/status')).toBe(1);
    render(<App />);
    await flush();
    expect(count('/console/api/status')).toBe(1);
    await act(async () => vi.advanceTimersByTime(5000));
    expect(count('/console/api/status')).toBe(2);
  });

  it('polls status every 5s on telemetry and stops when leaving for schedules', async () => {
    boot('/console/next/');
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

describe('first commit', () => {
  beforeEach(() => {
    vi.resetModules();
    stubFetch();
  });
  afterEach(() => {
    cleanup();
    vi.resetModules();
    vi.unstubAllGlobals();
  });

  // Fresh modules per row, so no earlier test has already resolved the view's React.lazy wrapper.
  it.each(['telemetry', 'tasks', 'schedules', 'memory', 'builds'] as const)(
    '%s renders on the first commit once its module has resolved',
    async (tab) => {
      const [fresh, freshBoot, route] = await Promise.all([import('./App'), import('./boot'), import('./hooks/useHashRoute')]);
      // What main.tsx does: start boot, wait for the active view's module, then render.
      window.history.replaceState(null, '', `/console/next/#${tab}`);
      void freshBoot.startBoot(route.initRouteFromLocation());
      await freshBoot.VIEW_LOADERS[tab]();
      render(<fresh.App />);
      // No flush: a suspended first commit would show the null Suspense fallback (no panel) here.
      expect(screen.getAllByRole('tabpanel').map((p) => p.id)).toEqual([`${tab}-view`]);
    },
  );
});

type Shown = readonly [selectedTab: string | undefined, visiblePanel: string | undefined];

/** The selected tab button and the visible panel, read from the DOM as each commit lands. */
function shown(): Shown {
  return [
    document.querySelector('[role="tab"][aria-selected="true"]')?.id.replace(/^tab-|-btn$/g, ''),
    // Activity hides a panel (display: none) in the commit that hides it; the panel's own props
    // (its `active` class) update later at offscreen priority, so the class would lag a commit.
    [...document.querySelectorAll<HTMLElement>('[role="tabpanel"]')]
      .find((p) => p.style.display !== 'none')
      ?.id.replace(/-view$/, ''),
  ];
}

describe('tab switch commits', () => {
  beforeEach(() => {
    vi.resetModules();
    stubFetch();
  });
  afterEach(() => {
    cleanup();
    vi.resetModules();
    vi.unstubAllGlobals();
  });

  // Fresh modules per row, so only the `loaded` views have resolved when the clicks land.
  it.each<{ name: string; loaded: readonly TabId[]; clicks: readonly TabId[]; want: readonly Shown[] }>([
    { name: 'a loaded tab swaps in the same commit', loaded: ['memory'], clicks: ['memory'], want: [['memory', 'memory']] },
    {
      name: 'an unloaded tab keeps the old view until its module resolves',
      loaded: [],
      clicks: ['memory'],
      want: [
        ['memory', 'telemetry'],
        ['memory', 'memory'],
      ],
    },
    {
      name: 'a loaded tab clicked while another loads is not overtaken by it',
      loaded: ['builds'],
      clicks: ['memory', 'builds'],
      want: [
        ['memory', 'telemetry'],
        ['builds', 'builds'],
      ],
    },
    {
      name: 'two unloaded tabs clicked in a row end on the last',
      loaded: [],
      clicks: ['memory', 'builds'],
      want: [
        ['memory', 'telemetry'],
        ['builds', 'telemetry'],
        ['builds', 'builds'],
      ],
    },
  ])('$name', async ({ loaded, clicks, want }) => {
    const [fresh, freshBoot, route] = await Promise.all([import('./App'), import('./boot'), import('./hooks/useHashRoute')]);
    window.history.replaceState(null, '', '/console/next/#telemetry');
    route.initRouteFromLocation();
    await Promise.all(['telemetry' as const, ...loaded].map((tab) => freshBoot.VIEW_LOADERS[tab]()));
    const commits: Shown[] = [];
    const onRender: ProfilerOnRenderCallback = () => {
      const now = shown();
      const last = commits.at(-1);
      if (last === undefined || last[0] !== now[0] || last[1] !== now[1]) commits.push(now);
    };
    render(
      <Profiler id="app" onRender={onRender}>
        <fresh.App />
      </Profiler>,
    );
    commits.length = 0;
    for (const tab of clicks) {
      // A synchronous act commits the click without letting a pending view import resolve.
      act(() => {
        window.location.hash = `#${tab}`;
        window.dispatchEvent(new HashChangeEvent('hashchange'));
      });
    }
    await flush();
    expect(commits).toEqual(want);
    expect(screen.getAllByRole('tabpanel').map((p) => p.id)).toEqual([`${clicks.at(-1)}-view`]);
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

  // Runs before the walk below opens a build, so the drawer body's React.lazy wrapper has never
  // been rendered (and so has not resolved) in this file yet.
  it('opens the build drawer without suspending once the builds chunk has loaded', async () => {
    window.history.replaceState(null, '', '/console/next/#builds');
    initRouteFromLocation();
    await VIEW_LOADERS.builds();
    render(<App />);
    // One synchronous act, no flush: a suspended body would leave the null Suspense fallback here.
    act(() => screen.getByText('Console React rewrite #1').click());
    expect(screen.getByRole('dialog', { name: 'BUILD // BUILD-1794100000-C046DE69' })).toBeTruthy();
    expect(screen.getByText('ORDERED BUILD CHUNKS (3)')).toBeTruthy();
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

async function goTo(hash: string): Promise<void> {
  await act(async () => {
    window.location.hash = hash;
    window.dispatchEvent(new HashChangeEvent('hashchange'));
  });
  await flush();
}

const SCHEDULE_POLLS = ['/console/api/schedules', '/console/api/schedules/runs?limit=50&offset=0'];

describe('visited views stay mounted', () => {
  let fetched: string[] = [];
  beforeEach(() => {
    useConsoleStore.setState(useConsoleStore.getInitialState(), true);
    viewMounts.clear();
    fetched = [];
    vi.stubGlobal(
      'fetch',
      vi.fn((url: string) => {
        fetched.push(url);
        return Promise.resolve(new Response(FIXTURE_BODIES[url] ?? '{}', { status: url in FIXTURE_BODIES ? 200 : 404 }));
      }),
    );
    vi.useFakeTimers({ now: Date.parse('2026-11-08T12:00:00Z'), toFake: ['setInterval', 'clearInterval', 'Date'] });
    seedAll();
  });
  afterEach(() => {
    cleanup();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it.each([
    {
      name: 'schedules, memory, schedules',
      walk: ['#schedules', '#memory', '#schedules'],
      wantMounts: { schedules: 1, memory: 1 },
      wantPanels: ['schedules-view', 'memory-view'],
      wantVisible: 'schedules-view',
      wantIds: ['#schedules-grid', '#schedules-search-input', '#memory-search-input'],
    },
    {
      name: 'telemetry, builds, telemetry, builds',
      walk: ['#telemetry', '#builds', '#telemetry', '#builds'],
      wantMounts: { telemetry: 1, builds: 1 },
      wantPanels: ['telemetry-view', 'builds-view'],
      wantVisible: 'builds-view',
      wantIds: ['#builds-search-input'],
    },
    {
      name: 'every tab and back to the first',
      walk: ['#tasks', '#telemetry', '#schedules', '#memory', '#builds', '#tasks'],
      wantMounts: { tasks: 1, telemetry: 1, schedules: 1, memory: 1, builds: 1 },
      wantPanels: ['tasks-view', 'telemetry-view', 'schedules-view', 'memory-view', 'builds-view'],
      wantVisible: 'tasks-view',
      wantIds: ['#schedules-grid', '#memory-search-input', '#builds-search-input'],
    },
  ])('$name builds each view once and shows only the active panel', async ({ walk, wantMounts, wantPanels, wantVisible, wantIds }) => {
    window.history.replaceState(null, '', `/console/next/${walk[0]}`);
    initRouteFromLocation();
    render(<App />);
    for (const hash of walk) await goTo(hash);

    expect(Object.fromEntries(viewMounts)).toEqual(wantMounts);
    const panels = [...document.querySelectorAll<HTMLElement>('[role="tabpanel"]')];
    expect(panels.map((p) => p.id)).toEqual(wantPanels);
    expect(panels.filter((p) => p.classList.contains('active')).map((p) => p.id)).toEqual([wantVisible]);
    expect(panels.filter((p) => p.style.display !== 'none').map((p) => p.id)).toEqual([wantVisible]);
    // Hidden panels are out of the accessibility tree; only the active one is a reachable tabpanel.
    expect(screen.getAllByRole('tabpanel').map((p) => p.id)).toEqual([wantVisible]);
    expect(panels.map((p) => p.getAttribute('aria-labelledby'))).toEqual(
      wantPanels.map((id) => `tab-${id.replace('-view', '')}-btn`),
    );
    expect(wantIds.map((id) => document.querySelectorAll(id).length)).toEqual(wantIds.map(() => 1));
  });

  it('stops a hidden schedules view polling and resumes it when shown again', async () => {
    window.history.replaceState(null, '', '/console/next/#schedules');
    initRouteFromLocation();
    render(<App />);
    await flush();
    const polls = (): number[] => SCHEDULE_POLLS.map((url) => fetched.filter((f) => f === url).length);
    expect(polls()).toEqual([1, 1]);
    await act(async () => vi.advanceTimersByTime(10_000));
    expect(polls()).toEqual([2, 2]);

    await goTo('#memory');
    await act(async () => vi.advanceTimersByTime(30_000));
    expect(polls()).toEqual([2, 2]);

    // Revealing runs the effects again: an immediate poll, then the 10s cadence.
    await goTo('#schedules');
    expect(polls()).toEqual([3, 3]);
    await act(async () => vi.advanceTimersByTime(10_000));
    expect(polls()).toEqual([4, 4]);
    expect(viewMounts.get('schedules')).toBe(1);
  });

  it.each([
    { name: 'memory search after leaving schedules', walk: ['#schedules', '#memory'], want: 'memory-search-input' },
    { name: 'schedules search after a round trip', walk: ['#schedules', '#memory', '#schedules'], want: 'schedules-search-input' },
  ])('"/" focuses the $name', async ({ walk, want }) => {
    window.history.replaceState(null, '', `/console/next/${walk[0]}`);
    initRouteFromLocation();
    render(<App />);
    for (const hash of walk) await goTo(hash);
    act(() => {
      window.dispatchEvent(new KeyboardEvent('keydown', { key: '/' }));
    });
    expect(document.activeElement?.id).toBe(want);
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
