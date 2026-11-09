import { describe, expect, it } from 'vitest';
import type { CronSchedule, OneShotSchedule, ScheduleRun, ScheduleSummaryMetrics } from '../../api/types';
import {
  earliestTrigger,
  filterCrons,
  filterOneShots,
  filterRuns,
  linkableThread,
  normalizeQuery,
  resultsLabel,
  runsLoggedLabel,
  runStatus,
  showsGrid,
  showsRuns,
  successRate,
} from './model';

const cron = (id: string, over: Partial<CronSchedule> = {}): CronSchedule => ({
  id,
  channel_id: '300',
  title_prefix: `Routine ${id}`,
  cron_expr: '0 9 * * *',
  cron_description: 'Every day at 9:00 AM',
  prompt: 'Post the digest',
  timezone: 'UTC',
  next_run_at: '2026-11-08T13:00:00Z',
  enabled: true,
  created_at: '2026-11-01T00:00:00Z',
  ...over,
});

const once = (id: string, over: Partial<OneShotSchedule> = {}): OneShotSchedule => ({
  id,
  thread_id: '400',
  prompt: 'Water the basil',
  run_at: '2026-11-08T12:30:00Z',
  created_at: '2026-11-08T00:00:00Z',
  ...over,
});

const run = (id: string, over: Partial<ScheduleRun> = {}): ScheduleRun => ({
  id,
  schedule_id: 'cron-x',
  schedule_type: 'cron',
  prompt: 'Run it',
  status: 'completed',
  started_at: '2026-11-08T11:00:00Z',
  duration_ms: 1200,
  ...over,
});

const CRONS: readonly CronSchedule[] = [
  cron('c-title', { title_prefix: 'Morning STANDUP' }),
  cron('c-expr', { cron_expr: '*/15 * * * *' }),
  cron('c-desc', { cron_description: 'Weekdays at dawn' }),
  cron('c-prompt', { prompt: 'Check the Garden sensors' }),
  cron('c-chan', { channel_id: '777001' }),
  cron('c-target', { channel_id: '', target_id: '888002' }),
  cron('c-tz', { timezone: 'Europe/Paris' }),
];

const ONE_SHOTS: readonly OneShotSchedule[] = [
  once('o-prompt', { prompt: 'Take the LAUNDRY out' }),
  once('o-thread', { thread_id: '999003' }),
  once('o-plain'),
];

const RUNS: readonly ScheduleRun[] = [
  run('r-prompt', { prompt: 'Summarise the garden' }),
  run('r-title', { title: 'Nightly backup' }),
  run('r-sched', { schedule_id: 'sched-xyz' }),
  run('r-thread', { thread_id: '555004' }),
  run('r-target', { target_id: '666005' }),
  run('r-status', { status: 'FAILED' }),
  run('r-error', { error: 'timeout talking to brain' }),
];

const ids = (list: readonly { readonly id: string }[]): string[] => list.map((x) => x.id);

describe('normalizeQuery', () => {
  it.each([
    { raw: '', want: '' },
    { raw: '  Garden ', want: 'garden' },
    { raw: 'CRON', want: 'cron' },
  ])('$raw -> $want', ({ raw, want }) => {
    expect(normalizeQuery(raw)).toBe(want);
  });
});

describe('filterCrons', () => {
  it.each([
    { name: 'no query keeps all', filter: 'ALL', query: '', want: ids(CRONS) },
    { name: 'one-shot pill hides crons', filter: 'ONE_SHOT', query: '', want: [] },
    { name: 'runs pill keeps crons', filter: 'RUNS', query: '', want: ids(CRONS) },
    { name: 'title', filter: 'CRON', query: 'standup', want: ['c-title'] },
    { name: 'expression', filter: 'ALL', query: '*/15', want: ['c-expr'] },
    { name: 'description', filter: 'ALL', query: 'dawn', want: ['c-desc'] },
    { name: 'prompt', filter: 'ALL', query: 'garden', want: ['c-prompt'] },
    { name: 'channel', filter: 'ALL', query: '777001', want: ['c-chan'] },
    { name: 'target when channel empty', filter: 'ALL', query: '888002', want: ['c-target'] },
    { name: 'timezone', filter: 'ALL', query: 'paris', want: ['c-tz'] },
    { name: 'id', filter: 'ALL', query: 'c-ex', want: ['c-expr'] },
    { name: 'no match', filter: 'ALL', query: 'zzz', want: [] },
  ] as const)('$name', ({ filter, query, want }) => {
    expect(ids(filterCrons(CRONS, filter, query))).toEqual(want);
  });

  it('returns the input list by reference when nothing filters', () => {
    expect(filterCrons(CRONS, 'ALL', '')).toBe(CRONS);
  });
});

// The per-item haystacks are cached on the reconciled objects: a poll that replaces an object is
// matched on its new fields in both directions (a new match appears, a stale match is dropped),
// and a query can never match across the boundary between two fields.
describe('cached haystacks', () => {
  const garden = cron('c-1', { prompt: 'Check the garden' });
  const renamed = { ...garden, prompt: 'Check the kiosk' };
  const shot = once('o-1', { prompt: 'Water the basil' });
  const shotMoved = { ...shot, thread_id: '123456' };
  const failed = run('r-1', { status: 'failed' });
  const failedFixed = { ...failed, status: 'completed' };
  it.each([
    { name: 'cron object replaced by a poll', lists: [[garden], [renamed]], query: 'kiosk', want: [[], ['c-1']] },
    { name: 'cron match lost after a poll', lists: [[garden], [renamed]], query: 'garden', want: [['c-1'], []] },
    {
      name: 'cron query spanning two fields',
      lists: [[cron('c-2', { title_prefix: 'ab', cron_expr: 'cd' })]],
      query: 'bc',
      want: [[]],
    },
  ])('$name', ({ lists, query, want }) => {
    expect(lists.map((list) => ids(filterCrons(list, 'ALL', query)))).toEqual(want);
  });
  it.each([
    { name: 'one-shot object replaced by a poll', lists: [[shot], [shotMoved]], query: '123456', want: [[], ['o-1']] },
  ])('$name', ({ lists, query, want }) => {
    expect(lists.map((list) => ids(filterOneShots(list, 'ALL', query)))).toEqual(want);
  });
  it.each([
    { name: 'run object replaced by a poll', lists: [[failed], [failedFixed]], query: 'failed', want: [['r-1'], []] },
  ])('$name', ({ lists, query, want }) => {
    expect(lists.map((list) => ids(filterRuns(list, query)))).toEqual(want);
  });
});

describe('filterOneShots', () => {
  it.each([
    { name: 'no query keeps all', filter: 'ALL', query: '', want: ['o-prompt', 'o-thread', 'o-plain'] },
    { name: 'cron pill hides one-shots', filter: 'CRON', query: '', want: [] },
    { name: 'prompt', filter: 'ONE_SHOT', query: 'laundry', want: ['o-prompt'] },
    { name: 'thread', filter: 'ALL', query: '999003', want: ['o-thread'] },
    { name: 'id', filter: 'RUNS', query: 'o-pl', want: ['o-plain'] },
  ] as const)('$name', ({ filter, query, want }) => {
    expect(ids(filterOneShots(ONE_SHOTS, filter, query))).toEqual(want);
  });
});

describe('filterRuns', () => {
  it.each([
    { name: 'no query', query: '', want: ids(RUNS) },
    { name: 'prompt', query: 'garden', want: ['r-prompt'] },
    { name: 'title', query: 'nightly', want: ['r-title'] },
    { name: 'schedule id', query: 'sched-xyz', want: ['r-sched'] },
    { name: 'thread', query: '555004', want: ['r-thread'] },
    { name: 'target', query: '666005', want: ['r-target'] },
    { name: 'status', query: 'failed', want: ['r-status'] },
    { name: 'error', query: 'brain', want: ['r-error'] },
  ])('$name', ({ query, want }) => {
    expect(ids(filterRuns(RUNS, query))).toEqual(want);
  });
});

describe('section visibility', () => {
  it.each([
    { filter: 'ALL', grid: true, runs: true },
    { filter: 'CRON', grid: true, runs: false },
    { filter: 'ONE_SHOT', grid: true, runs: false },
    { filter: 'RUNS', grid: false, runs: true },
  ] as const)('$filter', ({ filter, grid, runs }) => {
    expect([showsGrid(filter), showsRuns(filter)]).toEqual([grid, runs]);
  });
});

describe('linkableThread', () => {
  it.each([
    { threadId: undefined, want: null },
    { threadId: '', want: null },
    { threadId: ' 400000000000000000 ', want: '400000000000000000' },
    { threadId: 'thread-abc', want: null },
  ])('$threadId', ({ threadId, want }) => {
    expect(linkableThread(threadId)).toBe(want);
  });
});

describe('runStatus', () => {
  it.each([
    { status: 'running', want: { key: 'running', label: 'RUNNING', icon: 'sync', chipClass: 'chip-running' } },
    { status: 'enqueued', want: { key: 'enqueued', label: 'ENQUEUED', icon: 'hourglass', chipClass: 'chip-enqueued' } },
    { status: 'FAILED', want: { key: 'failed', label: 'FAILED', icon: 'alert', chipClass: 'chip-failed' } },
    { status: 'completed', want: { key: 'completed', label: 'COMPLETED', icon: 'bolt', chipClass: 'chip-completed' } },
    { status: 'success', want: { key: 'success', label: 'SUCCESS', icon: 'bolt', chipClass: 'chip-completed' } },
    { status: 'skipped', want: { key: 'skipped', label: 'SKIPPED', icon: 'check', chipClass: 'chip-completed' } },
    { status: '', want: { key: 'unknown', label: 'UNKNOWN', icon: 'check', chipClass: 'chip-completed' } },
  ])('$status', ({ status, want }) => {
    expect(runStatus(status)).toEqual(want);
  });
});

const summary = (success_rate_24h: number, total_runs_24h: number): ScheduleSummaryMetrics => ({
  total_active: 3,
  cron_count: 2,
  one_shot_count: 1,
  total_runs_24h,
  success_rate_24h,
});

describe('successRate', () => {
  it.each([
    { name: 'before data', summary: null, want: { text: '100.0%', className: 'value text-success' } },
    {
      name: 'partial summary',
      summary: { total_active: 3 } as ScheduleSummaryMetrics,
      want: { text: '100.0%', className: 'value text-success' },
    },
    { name: 'healthy', summary: summary(94, 50), want: { text: '94.0%', className: 'value text-success' } },
    { name: 'boundary 90', summary: summary(90, 50), want: { text: '90.0%', className: 'value text-success' } },
    { name: 'amber', summary: summary(80.25, 50), want: { text: '80.3%', className: 'value schedules-rate-amber' } },
    { name: 'boundary 75', summary: summary(75, 50), want: { text: '75.0%', className: 'value schedules-rate-amber' } },
    { name: 'failing', summary: summary(40, 50), want: { text: '40.0%', className: 'value text-danger' } },
  ])('$name', ({ summary: s, want }) => {
    expect(successRate(s)).toEqual(want);
  });
});

describe('runsLoggedLabel', () => {
  it.each([
    { name: 'before data', summary: null, want: '0 RUNS LOGGED' },
    { name: 'no runs', summary: summary(100, 0), want: '0 RUNS LOGGED (24H)' },
    { name: 'runs', summary: summary(94, 50), want: '50 RUNS LOGGED (24H)' },
  ])('$name', ({ summary: s, want }) => {
    expect(runsLoggedLabel(s)).toBe(want);
  });
});

describe('earliestTrigger', () => {
  it.each([
    { name: 'nothing scheduled', crons: [], oneShots: [], want: null },
    {
      name: 'one-shot sooner than crons',
      crons: [cron('a', { next_run_at: '2026-11-08T13:00:00Z' })],
      oneShots: [once('b', { run_at: '2026-11-08T12:05:00Z' })],
      want: '2026-11-08T12:05:00Z',
    },
    {
      name: 'disabled cron ignored',
      crons: [
        cron('a', { next_run_at: '2026-11-08T12:01:00Z', enabled: false }),
        cron('b', { next_run_at: '2026-11-08T14:00:00Z' }),
      ],
      oneShots: [once('c', { run_at: '' })],
      want: '2026-11-08T14:00:00Z',
    },
    { name: 'unparseable only', crons: [cron('a', { next_run_at: 'soon' })], oneShots: [], want: null },
  ])('$name', ({ crons, oneShots, want }) => {
    expect(earliestTrigger(crons, oneShots)).toBe(want);
  });
});

describe('resultsLabel', () => {
  it.each([
    { name: 'before data', hasData: false, matching: 0, total: 0, want: 'SHOWING 0 / 0 ITEMS' },
    { name: 'filtered', hasData: true, matching: 3, total: 40, want: 'SHOWING 3 / 40 ACTIVE ROUTINES' },
  ])('$name', ({ hasData, matching, total, want }) => {
    expect(resultsLabel(hasData, matching, total)).toBe(want);
  });
});
