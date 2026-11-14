import { fetchBuildDetail, fetchBuilds } from '../../api/api';
import { type PollFetcher, syncResource } from '../../hooks/usePollingSync';
import { useConsoleStore } from '../../store/store';

// The build queue's fetches: the queue list and one build's detail. The shell loads the queue
// once at boot for the tab badge; the view polls it while open and re-runs it for SYNC BUILDS. The detail drawer polls its build while it is open.

export const syncBuilds = syncResource(
  fetchBuilds,
  (data) => useConsoleStore.getState().setBuilds(data),
  (error) => useConsoleStore.getState().failBuilds(error),
);

/** Poller for one build's detail payload. */
export const syncBuildDetail = (id: string): PollFetcher =>
  syncResource(
    (signal) => fetchBuildDetail(id, signal),
    (data) => useConsoleStore.getState().setBuildDetail(data),
    (error) => useConsoleStore.getState().failBuildDetail(error),
  );
