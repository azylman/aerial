import { Activity, type ComponentType, lazy, Suspense, useEffect, useState } from 'react';
import {
  loadBuildsView,
  loadDocketView,
  loadedBuildDetailPanel,
  loadedTaskDetailPanel,
  loadedView,
  pollStatus,
  statusPolledOn,
  VIEW_LOADERS,
} from './boot';
import { Drawer, type DrawerPanels } from './components/Drawer';
import { Header } from './components/Header';
import { ServiceDiagnostics } from './features/telemetry/ServiceDiagnostics';
import { tabButtonId, tabPanelId } from './components/TabBar';
import { useHashRoute } from './hooks/useHashRoute';
import { useKeyboardShortcuts } from './hooks/useKeyboardShortcuts';
import { usePollingSync } from './hooks/usePollingSync';
import type { TabId } from './store/store';
import './styles/base.css';

// The fallback path for views whose module has not resolved yet (see ViewPanel).
const VIEWS: Readonly<Record<TabId, ComponentType>> = {
  telemetry: lazy(VIEW_LOADERS.telemetry),
  tasks: lazy(VIEW_LOADERS.tasks),
  schedules: lazy(VIEW_LOADERS.schedules),
  memory: lazy(VIEW_LOADERS.memory),
  builds: lazy(VIEW_LOADERS.builds),
  docket: lazy(VIEW_LOADERS.docket),
};

// The service body is tiny and opens from the telemetry view, so it ships in the main chunk and
// never suspends the drawer. The build body (and its markdown renderer) ships with the build
// queue view, which is where builds are opened from and which the idle prefetch already warms.
// Like ViewPanel, an already-loaded body renders directly so the first open never suspends.
const LazyBuildDrawerBody = lazy(() => loadBuildsView().then((m) => ({ default: m.BuildDetailPanel })));
const BuildDrawerBody: DrawerPanels['build'] = (props) => {
  const [Panel] = useState<DrawerPanels['build']>(() => loadedBuildDetailPanel() ?? LazyBuildDrawerBody);
  return <Panel {...props} />;
};
// The task body ships with the docket view the same way.
const LazyTaskDrawerBody = lazy(() => loadDocketView().then((m) => ({ default: m.TaskDetailPanel })));
const TaskDrawerBody: DrawerPanels['task'] = (props) => {
  const [Panel] = useState<DrawerPanels['task']>(() => loadedTaskDetailPanel() ?? LazyTaskDrawerBody);
  return <Panel {...props} />;
};
const DRAWER_PANELS: DrawerPanels = { service: ServiceDiagnostics, build: BuildDrawerBody, task: TaskDrawerBody };

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
  // An already-loaded view renders directly so it never suspends; otherwise React.lazy loads it
  // under the shared Suspense boundary. The choice is fixed per panel: switching a mounted lazy
  // view to its resolved component would change the element type and remount (and reset) it.
  const [View] = useState<ComponentType>(() => loadedView(tab) ?? VIEWS[tab]);
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
 * Tabs in first-visit order. A tab joins once it is the shown view tab, so its tree is built
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

/**
 * The tab whose view is shown. A tab whose module has loaded is adopted during render, so the tab
 * bar and the view swap land in one commit. A not-yet-loaded tab keeps the old view on screen
 * while its chunk downloads and is adopted once it resolves (or fails, so React.lazy surfaces the
 * error), and only if it is still the requested tab: rapid clicks never land on a stale one.
 */
function useViewTab(activeTab: TabId): TabId {
  const [viewTab, setViewTab] = useState(activeTab);
  const adoptNow = activeTab !== viewTab && loadedView(activeTab) !== undefined;
  if (adoptNow) {
    // Adjusting state during render (not in an effect) keeps the swap in the tab bar's commit.
    setViewTab(activeTab);
  }
  useEffect(() => {
    if (loadedView(activeTab) !== undefined) return;
    let requested = true;
    const adopt = () => {
      if (requested) setViewTab(activeTab);
    };
    VIEW_LOADERS[activeTab]().then(adopt, adopt);
    return () => {
      requested = false;
    };
  }, [activeTab]);
  return adoptNow ? activeTab : viewTab;
}

export function App() {
  const activeTab = useHashRoute();
  // The tab button flips immediately and, once the view's module is loaded, the view swaps in the
  // same commit. All panels share one already-revealed Suspense boundary; useViewTab only adopts a
  // tab once its module is here, so first opening a chunk never flashes the fallback.
  const viewTab = useViewTab(activeTab);
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
