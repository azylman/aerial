import { fetchFacts } from '../../api/api';
import { syncResource } from '../../hooks/usePollingSync';
import { useConsoleStore } from '../../store/store';

// The archive's fetch (fetchFacts in static/app.js). The shell loads facts once at boot for the
// tab badge; the view re-runs this for SYNC ARCHIVE, RETRY and when opened after a failed load.
export const syncFacts = syncResource(
  fetchFacts,
  (data) => useConsoleStore.getState().setFacts(data),
  (error) => useConsoleStore.getState().failFacts(error),
);
