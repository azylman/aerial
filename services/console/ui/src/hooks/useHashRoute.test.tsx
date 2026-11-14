import { act, renderHook } from '@testing-library/react';
import { beforeEach, describe, expect, it } from 'vitest';
import { type TabId, useConsoleStore } from '../store/store';
import { hashForTab, initRouteFromLocation, tabFromHash, useHashRoute } from './useHashRoute';

// Expected hash names and default tab.
describe('tabFromHash', () => {
  it.each([
    { hash: '#telemetry', want: 'telemetry' },
    { hash: '#tasks', want: 'tasks' },
    { hash: '#schedules', want: 'schedules' },
    { hash: '#memory', want: 'memory' },
    { hash: '#builds', want: 'builds' },
    { hash: '', want: 'telemetry' },
    { hash: '#', want: 'telemetry' },
    { hash: '#MEMORY', want: 'telemetry' },
    { hash: '#memory/extra', want: 'telemetry' },
  ])('$hash -> $want', ({ hash, want }) => {
    expect(tabFromHash(hash)).toBe(want);
  });
});

describe('hashForTab', () => {
  it.each([
    { tab: 'telemetry', want: '#telemetry' },
    { tab: 'builds', want: '#builds' },
  ] as const)('$tab -> $want', ({ tab, want }) => {
    expect(hashForTab(tab)).toBe(want);
  });
});

interface HookRow {
  name: string;
  initialHash: string;
  /** Applied after mount: a store navigation or a manual hash edit. */
  navigate: { readonly tab: TabId } | { readonly hash: string } | null;
  wantTab: TabId;
  wantHash: string;
}

const hookRows: HookRow[] = [
  { name: 'empty hash is canonicalised to telemetry', initialHash: '', navigate: null, wantTab: 'telemetry', wantHash: '#telemetry' },
  { name: 'deep link selects its tab', initialHash: '#memory', navigate: null, wantTab: 'memory', wantHash: '#memory' },
  { name: 'unknown hash falls back to telemetry', initialHash: '#nope', navigate: null, wantTab: 'telemetry', wantHash: '#telemetry' },
  { name: 'store navigation writes the hash', initialHash: '#telemetry', navigate: { tab: 'builds' }, wantTab: 'builds', wantHash: '#builds' },
  { name: 'hashchange follows back/forward', initialHash: '#telemetry', navigate: { hash: '#schedules' }, wantTab: 'schedules', wantHash: '#schedules' },
  { name: 'hashchange to junk lands on telemetry', initialHash: '#tasks', navigate: { hash: '#junk' }, wantTab: 'telemetry', wantHash: '#telemetry' },
];

describe('useHashRoute', () => {
  beforeEach(() => {
    useConsoleStore.setState(useConsoleStore.getInitialState(), true);
  });

  it.each(hookRows)('$name', ({ initialHash, navigate, wantTab, wantHash }) => {
    window.history.replaceState(null, '', `/console/next/${initialHash}`);
    initRouteFromLocation();
    const { result, unmount } = renderHook(() => useHashRoute());

    act(() => {
      if (navigate === null) return;
      if ('tab' in navigate) {
        useConsoleStore.getState().setActiveTab(navigate.tab);
      } else {
        window.history.replaceState(null, '', `/console/next/${navigate.hash}`);
        window.dispatchEvent(new HashChangeEvent('hashchange'));
      }
    });

    expect(result.current).toBe(wantTab);
    expect(useConsoleStore.getState().ui.activeTab).toBe(wantTab);
    expect(window.location.hash).toBe(wantHash);

    unmount();
    // After unmount neither direction is wired up any more.
    act(() => useConsoleStore.getState().setActiveTab(wantTab === 'tasks' ? 'memory' : 'tasks'));
    expect(window.location.hash).toBe(wantHash);
  });
});
