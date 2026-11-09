import { Activity, type ComponentType, lazy, Suspense, useDeferredValue, useEffect, useState } from 'react';
import { loadBuildsView, pollStatus, statusPolledOn, VIEW_LOADERS } from './boot';
import { Drawer, type DrawerPanels } from './components/Drawer';
import { Header } from './components/Header';
import { ServiceDiagnostics } from './features/telemetry/ServiceDiagnostics';
import { tabButtonId, tabPanelId } from './components/TabBar';
import { useHashRoute } from './hooks/useHashRoute';
import { useKeyboardShortcuts } from './hooks/useKeyboardShortcuts';
import { usePollingSync } from './hooks/usePollingSync';
import type { TabId } from './store/store';
import './styles/base.css';

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
const BuildDrawerBody = lazy(() => loadBuildsView().then((m) => ({ default: m.BuildDetailPanel })));
const DRAWER_PANELS: DrawerPanels = { service: ServiceDiagnostics, build: BuildDrawerBody };

const STATUS_POLL_MS = 5000;

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
 * Shell polling. Like the vanilla UI, /api/status polls only while telemetry or tasks is open; its
 * first poll joins the request startBoot already has in flight. The one-shot badge loads
 * (schedules, facts, builds) are started by startBoot before the first render.
 */
function useShellData(activeTab: TabId): void {
  usePollingSync(pollStatus, STATUS_POLL_MS, statusPolledOn(activeTab));
}

function ViewPanel({ tab, visible }: { readonly tab: TabId; readonly visible: boolean }) {
  const View = VIEWS[tab];
  return (
    <Activity mode={visible ? 'visible' : 'hidden'}>
      <div
        id={tabPanelId(tab)}
        className={visible ? 'view-panel active' : 'view-panel'}
        role="tabpanel"
        aria-labelledby={tabButtonId(tab)}
      >
        <View />
      </div>
    </Activity>
  );
}

/**
 * Tabs in first-visit order. A tab joins once it is the deferred view tab, so its tree is built
 * once and kept: switching back reveals it instead of rebuilding it.
 */
function useVisitedTabs(viewTab: TabId): readonly TabId[] {
  const [visited, setVisited] = useState<readonly TabId[]>([viewTab]);
  if (!visited.includes(viewTab)) {
    // Adjusting state during render (not in an effect) keeps the reveal in the same commit.
    const next = [...visited, viewTab];
    setVisited(next);
    return next;
  }
  return visited;
}

export function App() {
  const activeTab = useHashRoute();
  // The tab button flips immediately; the view swap renders as a deferred (transition) update.
  // All panels share one already-revealed Suspense boundary, so first opening a not-yet-loaded
  // chunk keeps the old view on screen until the new one is ready instead of flashing a fallback.
  const viewTab = useDeferredValue(activeTab);
  const visited = useVisitedTabs(viewTab);
  useKeyboardShortcuts(activeTab);
  useShellData(activeTab);
  useIdlePrefetch();

  // Hidden Activity trees keep their DOM and state but unmount their effects, which stops a
  // hidden view's polling and its search shortcut registration until it is shown again.
  return (
    <>
      <div className="scanline" />
      <div className="hud-container">
        <Header />
        <Suspense fallback={null}>
          {visited.map((tab) => (
            <ViewPanel key={tab} tab={tab} visible={tab === viewTab} />
          ))}
        </Suspense>
      </div>
      <Drawer panels={DRAWER_PANELS} />
    </>
  );
}
