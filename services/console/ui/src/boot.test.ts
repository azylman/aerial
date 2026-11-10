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
    { tab: 'docket', want: false },
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
    { tab: 'docket', wantRequests: ['/console/api/schedules', '/console/api/facts', '/console/api/builds'] },
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

interface LoadedBuildDetailPanelRow {
  name: string;
  load: 'none' | 'loadBuildsView' | 'VIEW_LOADERS.builds';
  wantLoaded: boolean;
}

describe('loadedBuildDetailPanel', () => {
  afterEach(() => {
    vi.resetModules();
  });

  it.each<LoadedBuildDetailPanelRow>([
    { name: 'undefined before the builds chunk loads', load: 'none', wantLoaded: false },
    { name: 'the drawer body once loadBuildsView resolves', load: 'loadBuildsView', wantLoaded: true },
    { name: 'the drawer body once the builds view loader resolves', load: 'VIEW_LOADERS.builds', wantLoaded: true },
  ])('$name', async ({ load, wantLoaded }) => {
    vi.resetModules();
    const [boot, buildsView] = await Promise.all([import('./boot'), import('./features/builds/BuildsView')]);
    if (load === 'loadBuildsView') await boot.loadBuildsView();
    if (load === 'VIEW_LOADERS.builds') await boot.VIEW_LOADERS.builds();
    expect(boot.loadedBuildDetailPanel()).toBe(wantLoaded ? buildsView.BuildDetailPanel : undefined);
  });
});

interface LoadedTaskDetailPanelRow {
  name: string;
  load: 'none' | 'loadDocketView' | 'VIEW_LOADERS.docket';
  wantLoaded: boolean;
}

describe('loadedTaskDetailPanel', () => {
  afterEach(() => {
    vi.resetModules();
  });

  it.each<LoadedTaskDetailPanelRow>([
    { name: 'undefined before the docket chunk loads', load: 'none', wantLoaded: false },
    { name: 'the drawer body once loadDocketView resolves', load: 'loadDocketView', wantLoaded: true },
    { name: 'the drawer body once the docket view loader resolves', load: 'VIEW_LOADERS.docket', wantLoaded: true },
  ])('$name', async ({ load, wantLoaded }) => {
    vi.resetModules();
    const [boot, docketView] = await Promise.all([import('./boot'), import('./features/docket/DocketView')]);
    if (load === 'loadDocketView') await boot.loadDocketView();
    if (load === 'VIEW_LOADERS.docket') await boot.VIEW_LOADERS.docket();
    expect(boot.loadedTaskDetailPanel()).toBe(wantLoaded ? docketView.TaskDetailPanel : undefined);
  });
});
