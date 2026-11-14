import { useEffect } from 'react';
import { type TabId, useConsoleStore } from '../store/store';

// Hash routing: every tab owns `#<id>`, unknown or empty
// hashes land on telemetry, and the hash is rewritten to the canonical form on load.

export const TAB_IDS = ['telemetry', 'tasks', 'schedules', 'memory', 'builds', 'docket'] as const satisfies readonly TabId[];

export const DEFAULT_TAB: TabId = 'telemetry';

export function tabFromHash(hash: string): TabId {
  return TAB_IDS.find((id) => hashForTab(id) === hash) ?? DEFAULT_TAB;
}

export function hashForTab(tab: TabId): string {
  return `#${tab}`;
}

function writeHash(tab: TabId): void {
  const hash = hashForTab(tab);
  if (window.location.hash !== hash) window.location.hash = hash;
}

function followHash(): TabId {
  const tab = tabFromHash(window.location.hash);
  useConsoleStore.getState().setActiveTab(tab);
  return tab;
}

/**
 * Seeds the ui slice from the URL before the first render, so a deep link to `#memory` mounts
 * (and lazy-loads) only the memory view instead of flashing telemetry first. Returns that tab.
 */
export function initRouteFromLocation(): TabId {
  return followHash();
}

/**
 * Two-way binding between `location.hash` and the ui slice's active tab. Navigation goes through
 * the store (`setActiveTab`); store changes are mirrored into the hash (adding a history entry)
 * and back/forward or manual hash edits flow back via `hashchange`. The hash
 * is canonicalised on mount, so `/console/next/` becomes `/console/next/#telemetry`.
 */
export function useHashRoute(): TabId {
  const activeTab = useConsoleStore((s) => s.ui.activeTab);

  useEffect(() => {
    followHash();
    writeHash(useConsoleStore.getState().ui.activeTab);
    window.addEventListener('hashchange', followHash);
    const unsubscribe = useConsoleStore.subscribe((state, prev) => {
      if (state.ui.activeTab !== prev.ui.activeTab) writeHash(state.ui.activeTab);
    });
    return () => {
      window.removeEventListener('hashchange', followHash);
      unsubscribe();
    };
  }, []);

  return activeTab;
}
