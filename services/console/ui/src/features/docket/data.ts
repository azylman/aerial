import { ApiError, fetchDocketTask, fetchDocketTasks, searchDocket } from '../../api/api';
import type { DocketStatusFilter, DocketTaskLookup } from '../../api/types';
import { type PollFetcher, syncResource } from '../../hooks/usePollingSync';
import { useConsoleStore } from '../../store/store';

// The Docket tab's fetches. Nothing loads at boot (the tab has no badge): the list polls while the
// view is shown, a search runs once per debounced query, and the drawer polls its task.

/** Poller for one status pill's task list. */
export const syncDocketTasks = (status: DocketStatusFilter): PollFetcher =>
  syncResource(
    (signal) => fetchDocketTasks(status, signal),
    (data) => useConsoleStore.getState().setDocketTasks(data),
    (error) => useConsoleStore.getState().failDocketTasks(error),
  );

/** One semantic search request. */
export const syncDocketSearch = (q: string, status: DocketStatusFilter): PollFetcher =>
  syncResource(
    (signal) => searchDocket(q, status, signal),
    (data) => useConsoleStore.getState().setDocketSearch(data),
    (error) => useConsoleStore.getState().failDocketSearch(error),
  );

/** Fetches one task; a 404 is a result (the task is gone), not a failure. */
async function lookupTask(id: number, signal: AbortSignal): Promise<DocketTaskLookup> {
  try {
    const res = await fetchDocketTask(id, signal);
    return { id, task: res.task ?? null };
  } catch (err) {
    if (err instanceof ApiError && err.status === 404) return { id, task: null };
    throw err;
  }
}

/** Poller for one task's detail payload. */
export const syncDocketTask = (id: number): PollFetcher =>
  syncResource(
    (signal) => lookupTask(id, signal),
    (data) => useConsoleStore.getState().setDocketTask(data),
    (error) => useConsoleStore.getState().failDocketTask(error),
  );
