import { describe, expect, it } from 'vitest';
import type {
  BuildsAPIResponse,
  ClusterResponse,
  FactsAPIResponse,
  GitSyncStatusResponse,
  QuickLaunchLink,
  SchedulesAPIResponse,
  ServiceStatus,
} from '../api/types';
import {
  buildsBadgeCount,
  dockLinks,
  filterByAgent,
  gitSyncBadge,
  isDockLinkActive,
  matchAgent,
  memoryBadgeCount,
  PERMET_IDLE,
  permetScore,
  schedulesBadgeCount,
  tasksBadgeCount,
} from './selectors';

const cluster = (tasks: number): ClusterResponse =>
  ({ active_tasks: Array.from({ length: tasks }, (_, i) => ({ id: `t${i}` })) }) as unknown as ClusterResponse;
const schedules = (totalActive: number): SchedulesAPIResponse =>
  ({ summary: { total_active: totalActive }, crons: [], one_shots: [] }) as unknown as SchedulesAPIResponse;
const facts = (total: number): FactsAPIResponse => ({ facts: [], total, limit: 0, offset: 0 });
const builds = (running: number, queued: number): BuildsAPIResponse =>
  ({ running_count: running, queued_count: queued, completed_count: 9, builds: [] }) as unknown as BuildsAPIResponse;

describe('tab badge counts', () => {
  it.each([
    { name: 'tasks before first poll', got: () => tasksBadgeCount(null), want: 0 },
    { name: 'tasks counts active tasks', got: () => tasksBadgeCount(cluster(5)), want: 5 },
    {
      name: 'tasks counts by agent',
      got: () =>
        tasksBadgeCount(
          {
            active_tasks: [
              { id: 't1', agent: 'aerial' },
              { id: 't2', agent: 'agent-alpha' },
              { id: 't3' },
            ],
          } as unknown as ClusterResponse,
          'aerial',
        ),
      want: 2,
    },
    { name: 'schedules before first poll', got: () => schedulesBadgeCount(null), want: 0 },
    { name: 'schedules uses summary.total_active', got: () => schedulesBadgeCount(schedules(40)), want: 40 },
    {
      name: 'schedules counts by agent',
      got: () =>
        schedulesBadgeCount(
          {
            summary: { total_active: 3 },
            crons: [
              { id: 'c1', enabled: true, agent: 'aerial' },
              { id: 'c2', enabled: true, agent: 'agent-beta' },
            ],
            one_shots: [{ id: 'o1', agent: 'aerial' }],
          } as unknown as SchedulesAPIResponse,
          'aerial',
        ),
      want: 2,
    },
    { name: 'memory before first poll', got: () => memoryBadgeCount(null), want: 0 },
    { name: 'memory uses the server total', got: () => memoryBadgeCount(facts(150)), want: 150 },
    {
      name: 'memory counts by agent',
      got: () =>
        memoryBadgeCount(
          {
            total: 10,
            facts: [
              { id: 1, agent: 'agent-alpha' },
              { id: 2, agent: 'aerial' },
              { id: 3 },
            ],
          } as unknown as FactsAPIResponse,
          'agent-alpha',
        ),
      want: 1,
    },
    { name: 'builds before first poll', got: () => buildsBadgeCount(null), want: 0 },
    { name: 'builds counts running + queued', got: () => buildsBadgeCount(builds(3, 4)), want: 7 },
    {
      name: 'builds counts by agent',
      got: () =>
        buildsBadgeCount(
          {
            running_count: 2,
            queued_count: 1,
            builds: [
              { id: 'b1', status: 'running', agent: 'aerial' },
              { id: 'b2', status: 'queued', agent: 'agent-beta' },
              { id: 'b3', status: 'completed', agent: 'aerial' },
            ],
          } as unknown as BuildsAPIResponse,
          'aerial',
        ),
      want: 1,
    },
  ])('$name', ({ got, want }) => {
    expect(got()).toBe(want);
  });
});

describe('filterByAgent & matchAgent', () => {
  interface TestItem {
    readonly id: number;
    readonly agent?: string;
  }
  const itemAerial: TestItem = { id: 1, agent: 'aerial' };
  const itemAlpha: TestItem = { id: 2, agent: 'agent-alpha' };
  const itemDefault: TestItem = { id: 3 };
  const items: readonly TestItem[] = [itemAerial, itemAlpha, itemDefault];

  it('matchAgent defaults to aerial when agent is absent', () => {
    expect(matchAgent(itemAerial, 'aerial')).toBe(true);
    expect(matchAgent(itemAlpha, 'aerial')).toBe(false);
    expect(matchAgent(itemDefault, 'aerial')).toBe(true);
    expect(matchAgent(itemAlpha, 'all')).toBe(true);
  });

  it('filterByAgent filters by selected agent and returns all when "all"', () => {
    expect(filterByAgent(items, 'all')).toBe(items);
    expect(filterByAgent(items, 'aerial')).toEqual([itemAerial, itemDefault]);
    expect(filterByAgent(items, 'agent-alpha')).toEqual([itemAlpha]);
    expect(filterByAgent(null, 'aerial')).toEqual([]);
  });
});

const svc = (status: string): ServiceStatus => ({ name: status, status, uptime_seconds: 0, last_check_time: '' });

describe('permetScore', () => {
  it.each([
    { name: 'before first poll keeps the static value', services: null, want: PERMET_IDLE },
    { name: 'no services is a warning at 0%', services: [], want: { percent: 0, label: '0 / 0 Healthy', tone: 'warning' } },
    { name: 'all healthy is optimal', services: [svc('healthy'), svc('healthy')], want: { percent: 100, label: '2 / 2 Healthy', tone: 'optimal' } },
    {
      name: 'above 70% is stable',
      services: [svc('healthy'), svc('healthy'), svc('healthy'), svc('degraded')],
      want: { percent: 75, label: '3 / 4 Healthy', tone: 'stable' },
    },
    {
      name: 'exactly 70% is a warning',
      services: [...Array.from({ length: 7 }, () => svc('healthy')), svc('down'), svc('down'), svc('degraded')],
      want: { percent: 70, label: '7 / 10 Healthy', tone: 'warning' },
    },
    { name: 'rounds the bar width', services: [svc('healthy'), svc('down'), svc('down')], want: { percent: 33, label: '1 / 3 Healthy', tone: 'warning' } },
  ])('$name', ({ services, want }) => {
    expect(permetScore(services)).toEqual(want);
  });
});

const gs = (status: string, lag: number): GitSyncStatusResponse => ({
  status,
  max_lag_seconds: lag,
  last_sync_time: '2026-11-08T12:00:00Z',
});
const T0 = '2026-11-08T12:00:00Z';
const ms = (iso: string): number => Date.parse(iso);

describe('gitSyncBadge', () => {
  it.each([
    { name: 'no data yet', gitSync: null, systemTime: null, now: 0, want: { tone: 'synced', text: 'GIT SYNC: IN SYNC (Δ 0s)' } },
    { name: 'synced', gitSync: gs('synced', 0), systemTime: T0, now: ms('2026-11-08T12:00:30Z'), want: { tone: 'synced', text: 'GIT SYNC: IN SYNC (Δ 0s)' } },
    { name: 'empty status defaults to synced', gitSync: gs('', 0), systemTime: T0, now: ms(T0), want: { tone: 'synced', text: 'GIT SYNC: IN SYNC (Δ 0s)' } },
    { name: 'error', gitSync: gs('ERROR', 12), systemTime: T0, now: ms(T0), want: { tone: 'error', text: 'GIT SYNC: ERROR' } },
    { name: 'lagging at sample time', gitSync: gs('lagging', 75), systemTime: T0, now: ms(T0), want: { tone: 'lagging', text: 'GIT SYNC: BEHIND (Δ 1m 15s)' } },
    { name: 'lagging keeps counting', gitSync: gs('lagging', 75), systemTime: T0, now: ms('2026-11-08T12:00:45Z'), want: { tone: 'lagging', text: 'GIT SYNC: BEHIND (Δ 2m)' } },
    { name: 'positive lag reads as behind', gitSync: gs('synced', 5), systemTime: T0, now: ms(T0), want: { tone: 'lagging', text: 'GIT SYNC: BEHIND (Δ 5s)' } },
    { name: 'clock behind the server never subtracts', gitSync: gs('lagging', 5), systemTime: T0, now: ms('2026-11-08T11:59:00Z'), want: { tone: 'lagging', text: 'GIT SYNC: BEHIND (Δ 5s)' } },
    { name: 'unparseable system_time freezes the lag', gitSync: gs('lagging', 3700), systemTime: 'garbage', now: ms(T0), want: { tone: 'lagging', text: 'GIT SYNC: BEHIND (Δ 1h 1m)' } },
  ])('$name', ({ gitSync, systemTime, now, want }) => {
    expect(gitSyncBadge(gitSync, systemTime, now)).toEqual(want);
  });
});

const link = (name: string, url: string): QuickLaunchLink => ({ name, url });
const VALID = [link('Grafana', '/grafana/'), link('Docs', '/docs/')];

describe('dockLinks', () => {
  it.each([
    { name: 'null links', links: null, want: [], wantSame: false },
    { name: 'empty links', links: [], want: [], wantSame: false },
    { name: 'all valid keeps the reference', links: VALID, want: VALID, wantSame: true },
    { name: 'drops nameless and url-less entries', links: [...VALID, link('', '/x/'), link('Nope', '')], want: VALID, wantSame: false },
  ])('$name', ({ links, want, wantSame }) => {
    const got = dockLinks(links);
    expect(got).toEqual(want);
    expect(got === links).toBe(wantSame);
  });
});

describe('isDockLinkActive', () => {
  it.each([
    { url: '/console/', pathname: '/console/', want: true },
    { url: '/console/', pathname: '/console/next/', want: true },
    { url: '/grafana/', pathname: '/console/next/', want: false },
    { url: '/', pathname: '/console/', want: false },
    { url: '/', pathname: '/', want: true },
  ])('$url on $pathname -> $want', ({ url, pathname, want }) => {
    expect(isDockLinkActive(url, pathname)).toBe(want);
  });
});
