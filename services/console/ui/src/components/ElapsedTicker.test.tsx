import { describe, expect, it } from 'vitest';
import { elapsedSeconds } from './ElapsedTicker';

const NOW = Date.parse('2026-11-08T12:00:00Z');

describe('elapsedSeconds', () => {
  it.each([
    { startedAt: '2026-11-08T11:59:00Z', want: 60 },
    { startedAt: '2026-11-08T11:59:59.400Z', want: 0 },
    { startedAt: '2026-11-08T12:00:05Z', want: 0 },
    { startedAt: '0001-01-01T00:00:00Z', want: 0 },
    { startedAt: 'garbage', want: 0 },
  ])('$startedAt -> $want', ({ startedAt, want }) => {
    expect(elapsedSeconds(startedAt, NOW)).toBe(want);
  });
});
