import { describe, expect, it } from 'vitest';
import type { FactItem } from '../api/types';
import { compareFactsImportanceDesc, parseFactReinforceCount, sortFactsByImportance } from './facts';

describe('parseFactReinforceCount', () => {
  it.each([
    { input: 1, want: 1 },
    { input: 5, want: 5 },
    { input: '3', want: 3 },
    { input: 2.9, want: 2 },
    { input: 0, want: 1 },
    { input: -5, want: 1 },
    { input: '0', want: 1 },
    { input: null, want: 1 },
    { input: undefined, want: 1 },
    { input: '', want: 1 },
    { input: 'invalid-number', want: 1 },
    { input: Number.NaN, want: 1 },
    { input: Number.POSITIVE_INFINITY, want: 1 },
  ])('$input -> $want', ({ input, want }) => {
    expect(parseFactReinforceCount(input)).toBe(want);
  });
});

const fact = (id: number, reinforce_count: number, created_at: string, last_reinforced_at?: string): FactItem => ({
  id,
  fact_text: `fact ${id}`,
  reinforce_count,
  created_at,
  ...(last_reinforced_at === undefined ? {} : { last_reinforced_at }),
});

// Each row lists facts in input order and the expected id order after sorting.
const orderRows: { name: string; facts: FactItem[]; wantIds: number[] }[] = [
  {
    name: 'descending reinforce_count',
    facts: [
      fact(1, 1, '2026-09-05T12:00:00Z'),
      fact(2, 10, '2026-09-05T12:00:00Z'),
      fact(3, 3, '2026-09-05T12:00:00Z'),
      fact(4, 7, '2026-09-05T12:00:00Z'),
      fact(5, 2, '2026-09-05T12:00:00Z'),
    ],
    wantIds: [2, 4, 3, 5, 1],
  },
  {
    name: 'recency tie-break uses last_reinforced_at, falling back to created_at',
    facts: [
      fact(1, 3, '2026-09-04T12:00:00Z'),
      fact(2, 3, '2026-09-01T12:00:00Z', '2026-09-06T12:00:00Z'),
      fact(3, 3, '2026-09-05T12:00:00Z'),
      fact(4, 3, '2026-09-07T12:00:00Z', '0001-01-01T00:00:00Z'),
    ],
    wantIds: [4, 2, 3, 1],
  },
  {
    name: 'id tie-break descending',
    facts: [fact(42, 2, '2026-09-05T12:00:00Z'), fact(105, 2, '2026-09-05T12:00:00Z')],
    wantIds: [105, 42],
  },
  {
    name: 'invalid timestamps sort as oldest',
    facts: [fact(1, 1, '0001-01-01T00:00:00Z'), fact(2, 1, '2026-09-05T12:00:00Z'), fact(3, 1, 'bogus')],
    wantIds: [2, 3, 1],
  },
  {
    name: 'zero counts clamp to 1 and tie on id',
    facts: [fact(1, 1, '2026-09-05T10:00:00Z'), fact(2, 5, '2026-09-05T10:00:00Z'), fact(3, 0, '2026-09-05T10:00:00Z')],
    wantIds: [2, 3, 1],
  },
];

describe('compareFactsImportanceDesc', () => {
  it.each(orderRows)('$name', ({ facts, wantIds }) => {
    expect(facts.slice().sort(compareFactsImportanceDesc).map((f) => f.id)).toEqual(wantIds);
  });
});

describe('sortFactsByImportance', () => {
  it.each(orderRows)('$name', ({ facts, wantIds }) => {
    const input = Object.freeze(facts.slice());
    const sorted = sortFactsByImportance(input);
    expect(sorted.map((f) => f.id)).toEqual(wantIds);
    expect(sorted).not.toBe(input);
    expect(sorted.every((f, i) => f === facts.find((x) => x.id === wantIds[i]))).toBe(true);
  });
});
