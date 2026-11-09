import { type ComponentType } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { startBoot, statusPolledOn, VIEW_LOADERS } from './boot';
import { TAB_IDS } from './hooks/useHashRoute';
import { type TabId, useConsoleStore } from './store/store';

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

interface BootRow {
  tab: TabId;
  wantRequests: string[];
}

describe('startBoot', () => {
  let requests: string[] = [];
  let loaded: TabId[] = [];

  beforeEach(() => {
    useConsoleStore.setState(useConsoleStore.getInitialState(), true);
    window.history.replaceState(null, '', '/console/next/');
    requests = [];
    loaded = [];
    vi.stubGlobal(
      'fetch',
      vi.fn((url: string) => {
        requests.push(url);
        return Promise.resolve(new Response('{"error":"down"}', { status: 503 }));
      }),
    );
    for (const id of TAB_IDS) {
      vi.spyOn(VIEW_LOADERS, id).mockImplementation(() => {
        loaded.push(id);
        return Promise.resolve({ default: () => null });
      });
    }
  });
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it.each<BootRow>([
    {
      tab: 'telemetry',
      wantRequests: ['/console/api/status', '/console/api/schedules', '/console/api/facts', '/console/api/builds'],
    },
    {
      tab: 'tasks',
      wantRequests: ['/console/api/status', '/console/api/schedules', '/console/api/facts', '/console/api/builds'],
    },
    {
      tab: 'schedules',
      wantRequests: [
        '/console/api/schedules',
        '/console/api/facts',
        '/console/api/builds',
        '/console/api/schedules/runs?limit=50&offset=0',
      ],
    },
    { tab: 'memory', wantRequests: ['/console/api/schedules', '/console/api/facts', '/console/api/builds'] },
    { tab: 'builds', wantRequests: ['/console/api/schedules', '/console/api/facts', '/console/api/builds'] },
  ])('$tab starts its view chunk and first fetches before any render', async ({ tab, wantRequests }) => {
    const settled = startBoot(tab);
    // Every request and the chunk download are issued synchronously, before React renders.
    expect(requests).toEqual(wantRequests);
    expect(loaded).toEqual([tab]);
    await settled;
    expect(useConsoleStore.getState().schedules.list.error).toBe('down');
  });
});

const LOADED_STUB: ComponentType = () => null;

interface LoadedViewRow {
  name: string;
  load: 'none' | 'resolve' | 'reject';
  want: ComponentType | undefined;
}

describe('loadedView', () => {
  afterEach(() => {
    vi.doUnmock('./features/memory/MemoryView');
    vi.resetModules();
  });

  it.each<LoadedViewRow>([
    { name: 'undefined before the view loads', load: 'none', want: undefined },
    { name: 'the view component once its loader resolves', load: 'resolve', want: LOADED_STUB },
    { name: 'undefined after a rejected import', load: 'reject', want: undefined },
  ])('$name', async ({ load, want }) => {
    vi.resetModules();
    vi.doMock('./features/memory/MemoryView', () => {
      if (load === 'reject') throw new Error('chunk failed');
      return { default: LOADED_STUB };
    });
    const boot = await import('./boot');
    if (load === 'resolve') await boot.VIEW_LOADERS.memory();
    if (load === 'reject') await expect(boot.VIEW_LOADERS.memory()).rejects.toThrow();
    expect(boot.loadedView('memory')).toBe(want);
  });
});
