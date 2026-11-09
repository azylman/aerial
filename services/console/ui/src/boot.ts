import { type ComponentType } from 'react';
import { fetchStatus } from './api/api';
import { syncBuilds } from './features/builds/data';
import { syncFacts } from './features/memory/data';
import { syncScheduleRuns, syncSchedules } from './features/schedules/data';
import { type PollFetcher, syncResource } from './hooks/usePollingSync';
import { type TabId, useConsoleStore } from './store/store';

// Everything the console fetches before React's first commit. main.tsx calls startBoot right
// after reading the route, so the active view's chunk and its data download in parallel with the
// first render instead of in a chunk -> commit -> effect -> fetch waterfall. The shell and the
// views poll through these same fetcher instances, and syncResource joins a call made while a
// request is in flight, so a mount-time poll shares boot's request rather than repeating it.

type ViewModule = { readonly default: ComponentType };

export const loadBuildsView = () => import('./features/builds/BuildsView');

// Each view is its own chunk. The loaders are shared by boot, React.lazy and the idle prefetch,
// so a prefetched view's module is already evaluated when its tab is first opened.
export const VIEW_LOADERS: Readonly<Record<TabId, () => Promise<ViewModule>>> = {
  telemetry: () => import('./features/telemetry/TelemetryView'),
  tasks: () => import('./features/tasks/TasksView'),
  schedules: () => import('./features/schedules/SchedulesView'),
  memory: () => import('./features/memory/MemoryView'),
  builds: loadBuildsView,
};

export const pollStatus = syncResource(
  fetchStatus,
  (data) => useConsoleStore.getState().setCluster(data),
  (error) => useConsoleStore.getState().failCluster(error),
);

/** Tabs whose content reads /api/status (the header gauge and dock read it too). */
export const statusPolledOn = (tab: TabId): boolean => tab === 'telemetry' || tab === 'tasks';

/** Like the vanilla UI, schedules, facts and builds load once at boot so every tab badge has a count. */
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
