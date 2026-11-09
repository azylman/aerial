import { act, cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { App, statusPolledOn } from './App';
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
