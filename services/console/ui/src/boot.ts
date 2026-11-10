import { type ComponentType } from 'react';
import { fetchStatus } from './api/api';
import type { DrawerPanels } from './components/Drawer';
import { syncBuilds } from './features/builds/data';
import { syncFacts } from './features/memory/data';
import { syncScheduleRuns, syncSchedules } from './features/schedules/data';
import { type PollFetcher, syncResource } from './hooks/usePollingSync';
import { type TabId, useConsoleStore } from './store/store';

// Everything the console fetches before React's first commit. main.tsx calls startBoot right
// after reading the route, so the active view's chunk and its data download in parallel (main.tsx
// then waits for the chunk alone before rendering) instead of in a chunk -> commit -> effect ->
// fetch waterfall. The shell and the
// views poll through these same fetcher instances, and syncResource joins a call made while a
// request is in flight, so a mount-time poll shares boot's request rather than repeating it.

type ViewModule = { readonly default: ComponentType };

type BuildsViewModule = typeof import('./features/builds/BuildsView');

const loaded: { buildDetailPanel?: DrawerPanels['build'] } = {};

/** The builds chunk's loader; records its drawer body once the import resolves (see loadedView). */
export const loadBuildsView = (): Promise<BuildsViewModule> =>
  import('./features/builds/BuildsView').then((mod) => {
    loaded.buildDetailPanel = mod.BuildDetailPanel;
    return mod;
  });

/** The build drawer body once the builds chunk has resolved, else undefined; like loadedView. */
export const loadedBuildDetailPanel = (): DrawerPanels['build'] | undefined => loaded.buildDetailPanel;

const loadedViews: Partial<Record<TabId, ComponentType>> = {};

/** Wraps a view's loader so its default export is recorded once the import resolves. */
function recordingLoader(tab: TabId, load: () => Promise<ViewModule>): () => Promise<ViewModule> {
  return () =>
    load().then((mod) => {
      loadedViews[tab] = mod.default;
      return mod;
    });
}

// Each view is its own chunk. The loaders are shared by boot, React.lazy and the idle prefetch,
// so a prefetched view's module is already evaluated when its tab is first opened, and whichever
// of them resolves a view first makes it available synchronously through loadedView.
export const VIEW_LOADERS: Readonly<Record<TabId, () => Promise<ViewModule>>> = {
  telemetry: recordingLoader('telemetry', () => import('./features/telemetry/TelemetryView')),
  tasks: recordingLoader('tasks', () => import('./features/tasks/TasksView')),
  schedules: recordingLoader('schedules', () => import('./features/schedules/SchedulesView')),
  memory: recordingLoader('memory', () => import('./features/memory/MemoryView')),
  builds: recordingLoader('builds', loadBuildsView),
};

/**
 * A view's component once its VIEW_LOADERS import has resolved, else undefined. React.lazy only
 * learns of a module after its own promise settles in a later microtask, so it suspends on its
 * first render even when the chunk is already here; rendering this instead never suspends.
 */
export const loadedView = (tab: TabId): ComponentType | undefined => loadedViews[tab];

/**
 * Boot's status request settles before App mounts, so a 2s freshness window lets the shell's first
 * poll reuse it instead of fetching again; it is below App's 5s poll interval, so every interval
 * tick still fetches.
 */
export const pollStatus = syncResource(
  fetchStatus,
  (data) => useConsoleStore.getState().setCluster(data),
  (error) => useConsoleStore.getState().failCluster(error),
  2000,
);

/** Tabs whose content reads /api/status (the header gauge and dock read it too). */
export const statusPolledOn = (tab: TabId): boolean => tab === 'telemetry' || tab === 'tasks';

/** Schedules, facts and builds load once at boot so every tab badge has a count. */
const SHELL_LOADS: readonly PollFetcher[] = [syncSchedules, syncFacts, syncBuilds];

/**
 * Each view's own first polls beyond the shell loads: SchedulesView also polls the run feed, and
 * BuildsView's queue poll is the shell's syncBuilds load.
 */
const VIEW_POLLS: Readonly<Record<TabId, readonly PollFetcher[]>> = {
  telemetry: [],
  tasks: [],
  schedules: [syncScheduleRuns],
  memory: [],
  builds: [],
};

/**
 * Starts the active view's chunk download and every fetch its first screen needs. Boot never
 * aborts, so later callers (the shell, the view's pollers) can join and leave its requests freely.
 * The returned promise settles once all of it has, and never rejects.
 */
export function startBoot(tab: TabId): Promise<void> {
  const signal = new AbortController().signal;
  const fetchers = [...(statusPolledOn(tab) ? [pollStatus] : []), ...SHELL_LOADS, ...VIEW_POLLS[tab]];
  const work: Promise<unknown>[] = [VIEW_LOADERS[tab](), ...fetchers.map((fetcher) => fetcher(signal))];
  return Promise.allSettled(work).then(() => undefined);
}
