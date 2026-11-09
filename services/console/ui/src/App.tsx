import { type ComponentType, lazy, Suspense, useDeferredValue, useEffect } from 'react';
import { fetchBuilds, fetchFacts, fetchSchedules, fetchStatus } from './api/api';
import { Drawer, type DrawerPanels } from './components/Drawer';
import { Header } from './components/Header';
import { ServiceDiagnostics } from './features/telemetry/ServiceDiagnostics';
import { tabButtonId, tabPanelId } from './components/TabBar';
import { useHashRoute } from './hooks/useHashRoute';
import { useKeyboardShortcuts } from './hooks/useKeyboardShortcuts';
import { usePollingSync, syncResource } from './hooks/usePollingSync';
import { type TabId, useConsoleStore } from './store/store';
import './styles/base.css';

type ViewModule = { readonly default: ComponentType };

const loadBuilds = () => import('./features/builds/BuildsView');

// Each view is its own chunk. The loaders are shared by React.lazy and the idle prefetch below,
// so a prefetched view's module is already evaluated when its tab is first opened.
export const VIEW_LOADERS: Readonly<Record<TabId, () => Promise<ViewModule>>> = {
  telemetry: () => import('./features/telemetry/TelemetryView'),
  tasks: () => import('./features/tasks/TasksView'),
  schedules: () => import('./features/schedules/SchedulesView'),
  memory: () => import('./features/memory/MemoryView'),
  builds: loadBuilds,
};

const VIEWS: Readonly<Record<TabId, ComponentType>> = {
  telemetry: lazy(VIEW_LOADERS.telemetry),
  tasks: lazy(VIEW_LOADERS.tasks),
  schedules: lazy(VIEW_LOADERS.schedules),
  memory: lazy(VIEW_LOADERS.memory),
  builds: lazy(VIEW_LOADERS.builds),
};

// The service body is tiny and opens from the telemetry view, so it ships in the main chunk and
// never suspends the drawer. The build body (and its markdown renderer) ships with the build
// queue view, which is where builds are opened from and which the idle prefetch already warms.
const BuildDrawerBody = lazy(() => loadBuilds().then((m) => ({ default: m.BuildDetailPanel })));
const DRAWER_PANELS: DrawerPanels = { service: ServiceDiagnostics, build: BuildDrawerBody };

const STATUS_POLL_MS = 5000;

const pollStatus = syncResource(
  fetchStatus,
  (data) => useConsoleStore.getState().setCluster(data),
  (error) => useConsoleStore.getState().failCluster(error),
);
const loadSchedules = syncResource(
  fetchSchedules,
  (data) => useConsoleStore.getState().setSchedules(data),
  (error) => useConsoleStore.getState().failSchedules(error),
);
const loadFacts = syncResource(
  fetchFacts,
  (data) => useConsoleStore.getState().setFacts(data),
  (error) => useConsoleStore.getState().failFacts(error),
);
const loadBuildQueue = syncResource(
  fetchBuilds,
  (data) => useConsoleStore.getState().setBuilds(data),
  (error) => useConsoleStore.getState().failBuilds(error),
);

/** Tabs whose content reads /api/status (the header gauge and dock read it too). */
export const statusPolledOn = (tab: TabId): boolean => tab === 'telemetry' || tab === 'tasks';

/** After first paint and once the main thread is idle, warm every view chunk. */
function prefetchViews(): void {
  for (const load of Object.values(VIEW_LOADERS)) void load();
}

function useIdlePrefetch(): void {
  useEffect(() => {
    if (typeof window.requestIdleCallback === 'function') {
      const id = window.requestIdleCallback(prefetchViews);
      return () => window.cancelIdleCallback(id);
    }
    const id = window.setTimeout(prefetchViews, 1);
    return () => window.clearTimeout(id);
  }, []);
}

/**
 * Shell data. Like the vanilla UI, /api/status polls only while telemetry or tasks is open, and
 * schedules, facts and builds load once at boot so every tab badge has a count.
 */
function useShellData(activeTab: TabId): void {
  usePollingSync(pollStatus, STATUS_POLL_MS, statusPolledOn(activeTab));
  useEffect(() => {
    const controller = new AbortController();
    void loadSchedules(controller.signal);
    void loadFacts(controller.signal);
    void loadBuildQueue(controller.signal);
    return () => controller.abort();
  }, []);
}

function ViewPanel({ tab }: { readonly tab: TabId }) {
  const View = VIEWS[tab];
  return (
    <div id={tabPanelId(tab)} className="view-panel active" role="tabpanel" aria-labelledby={tabButtonId(tab)}>
      <Suspense fallback={null}>
        <View />
      </Suspense>
    </div>
  );
}

export function App() {
  const activeTab = useHashRoute();
  // The tab button flips immediately; the view swap renders as a deferred (transition) update,
  // so switching to a not-yet-loaded chunk keeps the old view until the new one is ready rather
  // than flashing an empty Suspense fallback.
  const viewTab = useDeferredValue(activeTab);
  useKeyboardShortcuts(activeTab);
  useShellData(activeTab);
  useIdlePrefetch();

  return (
    <>
      <div className="scanline" />
      <div className="hud-container">
        <Header />
        <ViewPanel key={viewTab} tab={viewTab} />
      </div>
      <Drawer panels={DRAWER_PANELS} />
    </>
  );
}
