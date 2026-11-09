import { act, cleanup, render } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ClusterResponse } from '../api/types';
import { useConsoleStore } from '../store/store';
import { GitSyncBadge } from './GitSyncBadge';

const NOW = Date.parse('2026-11-08T12:00:00Z');
const cluster = (status: string, lag: number): ClusterResponse =>
  ({
    system_time: '2026-11-08T12:00:00Z',
    git_sync: { status, max_lag_seconds: lag, last_sync_time: '2026-11-08T11:59:00Z' },
  }) as unknown as ClusterResponse;

interface Row {
  name: string;
  data: ClusterResponse | null;
  elapsedMs: number;
  wantClass: string;
  wantText: string;
  wantTimers: number;
}

const rows: Row[] = [
  { name: 'before the first poll', data: null, elapsedMs: 0, wantClass: 'gitsync-pill', wantText: 'GIT SYNC: IN SYNC (Δ 0s)', wantTimers: 0 },
  { name: 'synced never ticks', data: cluster('synced', 0), elapsedMs: 5000, wantClass: 'gitsync-pill', wantText: 'GIT SYNC: IN SYNC (Δ 0s)', wantTimers: 0 },
  { name: 'error never ticks', data: cluster('error', 30), elapsedMs: 5000, wantClass: 'gitsync-pill error', wantText: 'GIT SYNC: ERROR', wantTimers: 0 },
  { name: 'blank status with lag is lagging', data: cluster('', 5), elapsedMs: 0, wantClass: 'gitsync-pill lagging', wantText: 'GIT SYNC: BEHIND (Δ 5s)', wantTimers: 1 },
  { name: 'lagging counts up on the live clock', data: cluster('lagging', 75), elapsedMs: 5000, wantClass: 'gitsync-pill lagging', wantText: 'GIT SYNC: BEHIND (Δ 1m 20s)', wantTimers: 1 },
];

describe('GitSyncBadge', () => {
  beforeEach(() => {
    useConsoleStore.setState(useConsoleStore.getInitialState(), true);
    vi.useFakeTimers({ now: NOW });
  });
  afterEach(() => {
    cleanup();
    vi.useRealTimers();
  });

  it.each(rows)('$name', ({ data, elapsedMs, wantClass, wantText, wantTimers }) => {
    if (data !== null) useConsoleStore.getState().setCluster(data);
    const { container } = render(<GitSyncBadge />);
    act(() => vi.advanceTimersByTime(elapsedMs));
    const pill = container.firstElementChild;
    expect(pill?.className).toBe(wantClass);
    expect(pill?.textContent).toBe(wantText);
    expect(vi.getTimerCount()).toBe(wantTimers);
  });
});
