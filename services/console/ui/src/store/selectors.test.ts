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
  gitSyncBadge,
  isDockLinkActive,
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
    { name: 'schedules before first poll', got: () => schedulesBadgeCount(null), want: 0 },
    { name: 'schedules uses summary.total_active', got: () => schedulesBadgeCount(schedules(40)), want: 40 },
    { name: 'memory before first poll', got: () => memoryBadgeCount(null), want: 0 },
    { name: 'memory uses the server total', got: () => memoryBadgeCount(facts(150)), want: 150 },
    { name: 'builds before first poll', got: () => buildsBadgeCount(null), want: 0 },
    { name: 'builds counts running + queued', got: () => buildsBadgeCount(builds(3, 4)), want: 7 },
  ])('$name', ({ got, want }) => {
    expect(got()).toBe(want);
  });
});

const svc = (status: string): ServiceStatus => ({ name: status, status, uptime_seconds: 0, last_check_time: '' });

describe('permetScore', () => {
  it.each([
    { name: 'before first poll keeps the static value', services: null, want: PERMET_IDLE },
    { name: 'no services is a warning at 0%', services: [], want: { percent: 0, label: 'LVL 1 // WARNING', tone: 'warning' } },
    { name: 'all healthy is optimal', services: [svc('healthy'), svc('healthy')], want: { percent: 100, label: 'LVL 5 // OPTIMAL', tone: 'optimal' } },
    {
      name: 'above 70% is stable',
      services: [svc('healthy'), svc('healthy'), svc('healthy'), svc('degraded')],
      want: { percent: 75, label: 'LVL 3 // STABLE', tone: 'stable' },
    },
    {
      name: 'exactly 70% is a warning',
      services: [...Array.from({ length: 7 }, () => svc('healthy')), svc('down'), svc('down'), svc('degraded')],
      want: { percent: 70, label: 'LVL 1 // WARNING', tone: 'warning' },
    },
    { name: 'rounds the bar width', services: [svc('healthy'), svc('down'), svc('down')], want: { percent: 33, label: 'LVL 1 // WARNING', tone: 'warning' } },
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
