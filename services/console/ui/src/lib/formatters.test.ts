import { describe, expect, it } from 'vitest';
import {
  formatAgentsviewSessionUrl,
  formatCommitTimestamp,
  formatCountdown,
  formatDuration,
  formatElapsedTicker,
  formatGitSyncDelta,
  formatTimeAgo,
  formatTimestamp,
  formatUptime,
  getTriggerBadge,
  parseValidTimestampMs,
  type Countdown,
} from './formatters';

// Wall-clock fixtures are built from local-time components so expectations hold in any TZ.
const localIso = (mo: number, d: number, h: number, mi: number, s: number): string =>
  new Date(2026, mo - 1, d, h, mi, s).toISOString();

const NOW = Date.parse('2026-10-17T12:00:00Z');
const ago = (sec: number): string => new Date(NOW - sec * 1000).toISOString();
const ahead = (sec: number): string => new Date(NOW + sec * 1000).toISOString();

describe('formatUptime', () => {
  it.each([
    { input: 45, want: '45s' },
    { input: 125, want: '2m 5s' },
    { input: 3665, want: '1h 1m' },
    { input: 90060, want: '1d 1h 1m' },
    { input: 0, want: '0s' },
    { input: -10, want: '0s' },
    { input: null, want: '0s' },
    { input: undefined, want: '0s' },
    { input: Number.NaN, want: '0s' },
  ])('$input -> $want', ({ input, want }) => {
    expect(formatUptime(input)).toBe(want);
  });
});

describe('formatElapsedTicker', () => {
  it.each([
    { input: 65, want: '01:05s' },
    { input: 0, want: '00:00s' },
    { input: 3665, want: '01:01:05' },
    { input: 7200, want: '02:00:00' },
    { input: null, want: '00:00s' },
    { input: -5, want: '00:00s' },
    { input: Number.NaN, want: '00:00s' },
  ])('$input -> $want', ({ input, want }) => {
    expect(formatElapsedTicker(input)).toBe(want);
  });
});

describe('formatGitSyncDelta', () => {
  it.each([
    { input: 0, want: 'Δ 0s' },
    { input: 45, want: 'Δ 45s' },
    { input: 45.9, want: 'Δ 45s' },
    { input: 60, want: 'Δ 1m' },
    { input: 125, want: 'Δ 2m 5s' },
    { input: 3600, want: 'Δ 1h' },
    { input: 3665, want: 'Δ 1h 1m' },
    { input: null, want: 'Δ 0s' },
    { input: undefined, want: 'Δ 0s' },
    { input: Number.NaN, want: 'Δ 0s' },
    { input: -10, want: 'Δ 0s' },
  ])('$input -> $want', ({ input, want }) => {
    expect(formatGitSyncDelta(input)).toBe(want);
  });
});

describe('getTriggerBadge', () => {
  it.each([
    { input: 'cron', want: { icon: 'alarm', text: 'CRON', css: 'cron' } },
    { input: 'CRON', want: { icon: 'alarm', text: 'CRON', css: 'cron' } },
    { input: 'reminder', want: { icon: 'timer', text: 'REMINDER', css: 'reminder' } },
    { input: 'http', want: { icon: 'bolt', text: 'API', css: 'http' } },
    { input: 'api', want: { icon: 'bolt', text: 'API', css: 'http' } },
    { input: 'discord', want: { icon: 'chat', text: 'DISCORD', css: 'discord' } },
    { input: 'custom_unknown', want: { icon: 'chat', text: 'DISCORD', css: 'discord' } },
    { input: null, want: { icon: 'chat', text: 'DISCORD', css: 'discord' } },
  ])('$input -> $want.text', ({ input, want }) => {
    expect(getTriggerBadge(input)).toEqual(want);
  });
});

describe('formatAgentsviewSessionUrl', () => {
  it.each([
    {
      input: '7a874a91-3afb-4eb0-bb62-8d96b9a3ae0b',
      want: '/agentsview/sessions/antigravity-cli%3A7a874a91-3afb-4eb0-bb62-8d96b9a3ae0b',
    },
    {
      input: 'antigravity-cli:7a874a91-3afb-4eb0-bb62-8d96b9a3ae0b',
      want: '/agentsview/sessions/antigravity-cli%3A7a874a91-3afb-4eb0-bb62-8d96b9a3ae0b',
    },
    { input: '/sessions/abc/', want: '/agentsview/sessions/antigravity-cli%3Asessions%2Fabc' },
    { input: null, want: '/agentsview/' },
    { input: undefined, want: '/agentsview/' },
    { input: '', want: '/agentsview/' },
    { input: '   ', want: '/agentsview/' },
    { input: '///', want: '/agentsview/' },
  ])('$input -> $want', ({ input, want }) => {
    expect(formatAgentsviewSessionUrl(input)).toBe(want);
  });
});

describe('parseValidTimestampMs', () => {
  it.each([
    { input: '2026-08-30T21:45:00Z', want: 1788126300000 },
    { input: '2020-01-01T00:00:00Z', want: 1577836800000 },
    { input: '0001-01-01T00:00:00Z', want: null },
    { input: '1970-01-01T00:00:00Z', want: null },
    { input: '2019-12-31T23:59:59Z', want: null },
    { input: null, want: null },
    { input: undefined, want: null },
    { input: '', want: null },
    { input: 'invalid-date-string', want: null },
  ])('$input -> $want', ({ input, want }) => {
    expect(parseValidTimestampMs(input)).toBe(want);
  });
});

describe('formatCommitTimestamp', () => {
  it.each([
    { input: localIso(9, 4, 4, 0, 22), want: 'Sep 4, 04:00 AM' },
    { input: localIso(10, 17, 14, 15, 35), want: 'Oct 17, 02:15 PM' },
    { input: '0001-01-01T00:00:00Z', want: '' },
    { input: '1970-01-01T00:00:00Z', want: '' },
    { input: null, want: '' },
    { input: undefined, want: '' },
    { input: '', want: '' },
    { input: 'invalid', want: '' },
  ])('$input -> $want', ({ input, want }) => {
    expect(formatCommitTimestamp(input)).toBe(want);
  });
});

describe('formatTimeAgo', () => {
  it.each([
    { input: ago(10), want: '10s AGO' },
    { input: ago(120), want: '2m AGO' },
    { input: ago(7200), want: '2h AGO' },
    { input: ago(172800), want: '2d AGO' },
    { input: ahead(30), want: '0s AGO' },
    { input: '0001-01-01T00:00:00Z', want: 'RECENT' },
    { input: '1970-01-01T00:00:00Z', want: 'RECENT' },
    { input: null, want: 'RECENT' },
    { input: undefined, want: 'RECENT' },
    { input: '', want: 'RECENT' },
    { input: 'invalid-date', want: 'RECENT' },
  ])('$input -> $want', ({ input, want }) => {
    expect(formatTimeAgo(input, NOW)).toBe(want);
  });
});

describe('formatDuration', () => {
  it.each([
    { input: 450, want: '450ms' },
    { input: 4500, want: '4.5s' },
    { input: 65000, want: '1m 5s' },
    { input: 0, want: '0ms' },
    { input: -1, want: '0ms' },
    { input: null, want: '0ms' },
    { input: Number.NaN, want: '0ms' },
  ])('$input -> $want', ({ input, want }) => {
    expect(formatDuration(input)).toBe(want);
  });
});

describe('formatTimestamp', () => {
  it.each([
    { input: localIso(10, 17, 8, 0, 5), want: 'Oct 17 8:00:05 AM' },
    { input: localIso(1, 2, 23, 59, 59), want: 'Jan 2 11:59:59 PM' },
    { input: null, want: '--' },
    { input: '', want: '--' },
    { input: 'garbage', want: '--' },
  ])('$input -> $want', ({ input, want }) => {
    expect(formatTimestamp(input)).toBe(want);
  });
});

describe('formatCountdown', () => {
  const normal = (text: string): Countdown => ({ text, state: 'normal', cssClass: 'countdown-normal', icon: 'timer' });
  it.each<{ input: string | null; want: Countdown }>([
    { input: ahead(2 * 86400 + 3 * 3600 + 59), want: normal('in 2d 3h') },
    { input: ahead(86400), want: normal('in 24h 0m') },
    { input: ahead(3600), want: normal('in 1h 0m') },
    { input: ahead(3599), want: normal('in 59m 59s') },
    { input: ahead(61), want: normal('in 1m 1s') },
    { input: ahead(60), want: { text: 'in 60s', state: 'urgent', cssClass: 'countdown-urgent', icon: 'timer' } },
    { input: ahead(1), want: { text: 'in 1s', state: 'urgent', cssClass: 'countdown-urgent', icon: 'timer' } },
    { input: ahead(0), want: { text: 'TRIGGERING...', state: 'triggering', cssClass: 'countdown-triggering', icon: 'bolt' } },
    { input: ago(15), want: { text: 'TRIGGERING...', state: 'triggering', cssClass: 'countdown-triggering', icon: 'bolt' } },
    {
      input: ago(16),
      want: { text: 'OVERDUE (SYNCING...)', state: 'overdue', cssClass: 'countdown-overdue', icon: 'timer' },
    },
    { input: null, want: { text: '--:--:--', state: 'none', cssClass: '', icon: null } },
    { input: 'not-a-date', want: { text: '--:--:--', state: 'none', cssClass: '', icon: null } },
  ])('$input -> $want.text', ({ input, want }) => {
    expect(formatCountdown(input, NOW)).toEqual(want);
  });
});
