import { describe, expect, it } from 'vitest';
import {
  classifyBuildQueueState,
  computeBuildChunkStats,
  formatBuildTimestamp,
  getBuildStatusBadgeInfo,
  getBuildTimingInfo,
  isSameCalendarDay,
  type BuildChunkStats,
  type BuildLike,
  type BuildQueueState,
  type BuildStatusBadge,
  type BuildTimingInfo,
} from './builds';

// Local-time fixtures (month is 1-based here) keep expectations TZ-independent.
const localMs = (mo: number, d: number, h: number, mi: number, s = 0): number => new Date(2026, mo - 1, d, h, mi, s).getTime();
const localIso = (mo: number, d: number, h: number, mi: number, s = 0): string => new Date(localMs(mo, d, h, mi, s)).toISOString();
const ZERO_TIME = '0001-01-01T00:00:00Z';

describe('classifyBuildQueueState', () => {
  it.each<{ name: string; build: BuildLike | string | null | undefined; want: BuildQueueState }>([
    { name: 'completed', build: { status: 'completed' }, want: 'completed' },
    { name: 'merged', build: { status: 'merged' }, want: 'completed' },
    { name: 'failed', build: { status: 'failed' }, want: 'completed' },
    { name: 'cancelled', build: { status: 'cancelled' }, want: 'completed' },
    { name: 'canceled', build: { status: 'canceled' }, want: 'completed' },
    { name: 'bare COMPLETED string', build: ' COMPLETED ', want: 'completed' },
    { name: 'queued', build: { status: 'queued' }, want: 'queued' },
    { name: 'pending', build: { status: 'pending' }, want: 'queued' },
    { name: 'bare QUEUED string', build: 'QUEUED', want: 'queued' },
    { name: 'bare running string', build: 'running', want: 'running' },
    { name: 'bare unknown string', build: 'weird', want: 'queued' },
    {
      name: 'running with all chunks pending',
      build: { status: 'running', chunks: [{ status: 'pending' }, { status: ' PENDING ' }] },
      want: 'queued',
    },
    {
      name: 'running with active chunks',
      build: { status: 'running', chunks: [{ status: 'completed' }, { status: 'running' }, { status: 'pending' }] },
      want: 'running',
    },
    { name: 'running with no chunks', build: { status: 'running', chunks: [] }, want: 'running' },
    { name: 'running without chunk field', build: { status: 'running' }, want: 'running' },
    { name: 'queue_state used when status empty', build: { status: '', queue_state: 'completed' }, want: 'completed' },
    { name: 'unknown status falls back to queue_state running', build: { status: 'unknown', queue_state: 'running' }, want: 'running' },
    { name: 'unknown status falls back to queue_state completed', build: { status: 'unknown', queue_state: 'Completed' }, want: 'completed' },
    { name: 'unknown status with bogus queue_state', build: { status: 'unknown', queue_state: 'limbo' }, want: 'queued' },
    { name: 'unknown status alone', build: { status: 'unknown' }, want: 'queued' },
    { name: 'empty object', build: {}, want: 'queued' },
    { name: 'null', build: null, want: 'queued' },
    { name: 'undefined', build: undefined, want: 'queued' },
  ])('$name -> $want', ({ build, want }) => {
    expect(classifyBuildQueueState(build)).toBe(want);
  });
});

describe('computeBuildChunkStats', () => {
  it.each<{ name: string; build: BuildLike | null | undefined; want: BuildChunkStats }>([
    { name: 'null', build: null, want: { total: 0, completed: 0, running: 0, pending: 0, failed: 0, percent: 0 } },
    { name: 'undefined', build: undefined, want: { total: 0, completed: 0, running: 0, pending: 0, failed: 0, percent: 0 } },
    {
      name: 'chunk array',
      build: {
        total_chunks: 99,
        chunks: [
          { status: 'completed' },
          { status: 'merged' },
          { status: 'running' },
          { status: 'IN_PROGRESS' },
          { status: 'failed' },
          { status: 'pending' },
        ],
      },
      want: { total: 6, completed: 2, running: 2, pending: 1, failed: 1, percent: 33 },
    },
    {
      name: 'unknown and missing chunk statuses count as pending',
      build: { chunks: [{ status: 'completed' }, {}, { status: 'mystery' }] },
      want: { total: 3, completed: 1, running: 0, pending: 2, failed: 0, percent: 33 },
    },
    {
      name: 'precomputed counters',
      build: { total_chunks: 4, completed_chunks: 2, running_chunks: 1, pending_chunks: 1, failed_chunks: 0 },
      want: { total: 4, completed: 2, running: 1, pending: 1, failed: 0, percent: 50 },
    },
    {
      name: 'empty chunk array uses counters',
      build: { chunks: [], total_chunks: 3, completed_chunks: 2, failed_chunks: 1 },
      want: { total: 3, completed: 2, running: 0, pending: 0, failed: 1, percent: 67 },
    },
    { name: 'no counters', build: {}, want: { total: 0, completed: 0, running: 0, pending: 0, failed: 0, percent: 0 } },
  ])('$name', ({ build, want }) => {
    expect(computeBuildChunkStats(build)).toEqual(want);
  });
});

describe('getBuildStatusBadgeInfo', () => {
  it.each<{ name: string; build: BuildLike | null; want: BuildStatusBadge }>([
    { name: 'null', build: null, want: { label: 'QUEUED', tone: 'queued', icon: 'hourglass' } },
    { name: 'failed', build: { status: 'FAILED' }, want: { label: 'FAILED', tone: 'failed', icon: 'x' } },
    { name: 'cancelled', build: { status: 'cancelled' }, want: { label: 'CANCELLED', tone: 'cancelled', icon: 'ban' } },
    { name: 'canceled', build: { status: 'canceled' }, want: { label: 'CANCELLED', tone: 'cancelled', icon: 'ban' } },
    { name: 'running', build: { status: 'running' }, want: { label: 'RUNNING', tone: 'running', icon: 'bolt' } },
    { name: 'merged', build: { status: 'merged' }, want: { label: 'COMPLETED', tone: 'completed', icon: 'check' } },
    { name: 'pending', build: { status: 'pending' }, want: { label: 'QUEUED', tone: 'queued', icon: 'hourglass' } },
    {
      name: 'running but all chunks pending',
      build: { status: 'running', chunks: [{ status: 'pending' }] },
      want: { label: 'QUEUED', tone: 'queued', icon: 'hourglass' },
    },
  ])('$name -> $want.label', ({ build, want }) => {
    expect(getBuildStatusBadgeInfo(build)).toEqual(want);
  });
});

describe('formatBuildTimestamp', () => {
  it.each([
    { ms: localMs(10, 17, 8, 0), includeDate: true, want: '10/17 8:00am' },
    { ms: localMs(10, 17, 14, 15), includeDate: false, want: '2:15pm' },
    { ms: localMs(10, 18, 0, 5), includeDate: true, want: '10/18 12:05am' },
    { ms: localMs(1, 2, 12, 30), includeDate: false, want: '12:30pm' },
    { ms: localMs(12, 31, 23, 59), includeDate: true, want: '12/31 11:59pm' },
  ])('$want', ({ ms, includeDate, want }) => {
    expect(formatBuildTimestamp(ms, includeDate)).toBe(want);
  });
});

describe('isSameCalendarDay', () => {
  it.each([
    { name: 'same day', a: localMs(10, 17, 0, 0), b: localMs(10, 17, 23, 59), want: true },
    { name: 'next day', a: localMs(10, 17, 23, 59), b: localMs(10, 18, 0, 0), want: false },
    { name: 'same date next month', a: localMs(10, 17, 8, 0), b: localMs(11, 17, 8, 0), want: false },
    { name: 'same date next year', a: localMs(10, 17, 8, 0), b: new Date(2027, 9, 17, 8, 0).getTime(), want: false },
    { name: 'missing a', a: null, b: localMs(10, 17, 8, 0), want: false },
    { name: 'missing b', a: localMs(10, 17, 8, 0), b: null, want: false },
  ])('$name -> $want', ({ a, b, want }) => {
    expect(isSameCalendarDay(a, b)).toBe(want);
  });
});

describe('getBuildTimingInfo', () => {
  const NOW = localMs(10, 17, 8, 5, 12);
  it.each<{ name: string; build: BuildLike | null; now?: number; want: BuildTimingInfo }>([
    { name: 'null build', build: null, want: { startText: '--', endText: '', durationText: '--' } },
    {
      name: 'same-day completed prefers chunk completion over later updates',
      build: {
        status: 'completed',
        created_at: localIso(10, 17, 8, 0, 5),
        updated_at: localIso(10, 17, 18, 45),
        chunks: [
          { status: 'merged', started_at: localIso(10, 17, 8, 0, 5), completed_at: localIso(10, 17, 14, 15, 35), updated_at: localIso(10, 17, 18, 45) },
        ],
      },
      want: { startText: '10/17 8:00am', endText: '2:15pm', durationText: '6h 15m' },
    },
    {
      name: 'cross-day completed without chunk completion uses latest chunk update',
      build: {
        status: 'completed',
        created_at: localIso(10, 17, 12, 30),
        updated_at: localIso(10, 17, 13, 0),
        chunks: [
          { status: 'merged', started_at: localIso(10, 17, 12, 30), completed_at: ZERO_TIME, updated_at: localIso(10, 18, 0, 5, 35) },
        ],
      },
      want: { startText: '10/17 12:30pm', endText: '10/18 12:05am', durationText: '11h 35m' },
    },
    {
      name: 'completed without chunks ends at build update',
      build: { status: 'failed', created_at: localIso(10, 17, 9, 0), updated_at: localIso(10, 17, 9, 0, 45) },
      want: { startText: '10/17 9:00am', endText: '9:00am', durationText: '45s' },
    },
    {
      name: 'completed with end before start keeps duration unknown',
      build: { status: 'completed', created_at: localIso(10, 17, 9, 0), chunks: [{ status: 'merged', started_at: localIso(10, 17, 9, 0), completed_at: localIso(10, 17, 8, 0) }] },
      want: { startText: '10/17 9:00am', endText: '8:00am', durationText: '--' },
    },
    {
      name: 'completed with no timestamps at all',
      build: { status: 'completed', created_at: ZERO_TIME, updated_at: ZERO_TIME },
      want: { startText: '--', endText: '', durationText: '--' },
    },
    {
      name: 'completed with end but no start shows dated end',
      build: { status: 'completed', created_at: ZERO_TIME, updated_at: localIso(10, 17, 9, 0) },
      want: { startText: '--', endText: '10/17 9:00am', durationText: '--' },
    },
    {
      name: 'running ticks against now from earliest chunk start',
      build: {
        status: 'running',
        created_at: localIso(10, 17, 8, 0, 5),
        chunks: [
          { status: 'completed', started_at: localIso(10, 17, 8, 2) },
          { status: 'running', started_at: localIso(10, 17, 8, 1) },
          { status: 'pending', started_at: ZERO_TIME },
        ],
      },
      now: NOW,
      want: { startText: '10/17 8:01am', endText: '', durationText: '4m 12s' },
    },
    {
      name: 'running with start after now (clock skew) keeps duration unknown',
      build: { status: 'running', created_at: localIso(10, 17, 9, 0) },
      now: NOW,
      want: { startText: '10/17 9:00am', endText: '', durationText: '--' },
    },
    {
      name: 'queued uses creation time and no duration',
      build: { status: 'queued', created_at: localIso(10, 17, 8, 0, 5), chunks: [{ status: 'pending', started_at: ZERO_TIME }] },
      now: NOW,
      want: { startText: '10/17 8:00am', endText: '', durationText: '--' },
    },
  ])('$name', ({ build, now, want }) => {
    expect(getBuildTimingInfo(build, now ?? NOW)).toEqual(want);
  });
});
