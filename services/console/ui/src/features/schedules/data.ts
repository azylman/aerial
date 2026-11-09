import { fetchScheduleRuns, fetchSchedules } from '../../api/api';
import { syncResource } from '../../hooks/usePollingSync';
import { useConsoleStore } from '../../store/store';
import { recordServerTime } from './clock';

// The schedules view's two polls (fetchSchedules / fetchScheduleRuns in static/app.js). The
// schedules poll also samples the server clock for drift-corrected countdowns.

export const syncSchedules = syncResource(
  fetchSchedules,
  (data) => {
    recordServerTime(data.system_time, Date.now());
    useConsoleStore.getState().setSchedules(data);
  },
  (error) => useConsoleStore.getState().failSchedules(error),
);

export const syncScheduleRuns = syncResource(
  fetchScheduleRuns,
  (data) => useConsoleStore.getState().setScheduleRuns(data),
  (error) => useConsoleStore.getState().failScheduleRuns(error),
);
