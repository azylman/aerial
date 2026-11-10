import { memo } from 'react';
import { type ConsoleState, type TabId, useConsoleStore } from '../store/store';
import { buildsBadgeCount, memoryBadgeCount, schedulesBadgeCount, tasksBadgeCount } from '../store/selectors';
import { Icon, type IconName } from './Icon';

interface TabMeta {
  readonly id: TabId;
  readonly label: string;
  readonly icon: IconName;
  /** Badge count selector; `null` for tabs without a badge (telemetry, docket). */
  readonly badge: ((state: ConsoleState) => number) | null;
}

// Same order, labels and badges as the `.hud-view-tabs` nav in static/index.html.
export const TABS: readonly TabMeta[] = [
  { id: 'telemetry', label: 'TELEMETRY', icon: 'bolt', badge: null },
  { id: 'tasks', label: 'ACTIVE TASKS', icon: 'target', badge: (s) => tasksBadgeCount(s.status.cluster.data) },
  { id: 'schedules', label: 'SCHEDULES', icon: 'alarm', badge: (s) => schedulesBadgeCount(s.schedules.list.data) },
  { id: 'memory', label: 'MEMORY ARCHIVE', icon: 'brain', badge: (s) => memoryBadgeCount(s.memory.facts.data) },
  { id: 'builds', label: 'BUILD QUEUE', icon: 'wrench', badge: (s) => buildsBadgeCount(s.builds.list.data) },
  { id: 'docket', label: 'DOCKET', icon: 'checklist', badge: null },
];

export const tabButtonId = (tab: TabId): string => `tab-${tab}-btn`;
export const tabPanelId = (tab: TabId): string => `${tab}-view`;

const noBadge = (): null => null;

/**
 * One button per tab, each subscribed to just its own `active` flag and badge count (both
 * primitives), so a tab switch re-renders two buttons and a poll re-renders at most one.
 */
const TabButton = memo(function TabButton({ tab }: { readonly tab: TabMeta }) {
  const active = useConsoleStore((s) => s.ui.activeTab === tab.id);
  const count = useConsoleStore(tab.badge ?? noBadge);
  const setActiveTab = useConsoleStore((s) => s.setActiveTab);
  return (
    <button
      type="button"
      className={active ? 'view-tab-btn active' : 'view-tab-btn'}
      id={tabButtonId(tab.id)}
      role="tab"
      aria-selected={active}
      aria-controls={tabPanelId(tab.id)}
      onClick={() => setActiveTab(tab.id)}
    >
      <span className="tab-icon">
        <Icon name={tab.icon} />
      </span>
      <span>{tab.label}</span>
      {count === null ? null : <span className="tab-badge">{count}</span>}
    </button>
  );
});

export const TabBar = memo(function TabBar() {
  return (
    <nav className="hud-view-tabs" role="tablist" aria-label="Command Center Views">
      {TABS.map((tab) => (
        <TabButton key={tab.id} tab={tab} />
      ))}
    </nav>
  );
});
