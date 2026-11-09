import { describe, expect, it } from 'vitest';
import type { FactItem, FactsAPIResponse } from '../../api/types';
import {
  categoryCounts,
  type FactCategory,
  factAgeTimestamp,
  filterFacts,
  indexFacts,
  memoryMetrics,
  memoryPhase,
  normalizeQuery,
  resultsLabel,
} from './model';

const fact = (id: number, text: string, count: number, last?: string): FactItem => ({
  id,
  fact_text: text,
  reinforce_count: count,
  created_at: '2026-10-01T00:00:00Z',
  ...(last === undefined ? {} : { last_reinforced_at: last }),
});

// Importance order: 12 (5x), 7 (3x, newer), 9 (3x, older), 30 (1x, higher id), 4 (0 -> 1x).
const FACTS: readonly FactItem[] = [
  fact(4, 'Avoids cumin in recipes', 0),
  fact(9, 'Prefers oat milk in lattes', 3, '2026-11-01T00:00:00Z'),
  fact(12, 'Builds ship through Vanadis', 5, '2026-11-02T00:00:00Z'),
  fact(30, 'Oat milk is the default', 1),
  fact(7, 'Uses dark mode everywhere', 3, '2026-11-05T00:00:00Z'),
];
const INDEX = indexFacts(FACTS);
const response = (facts: readonly FactItem[], total: number): FactsAPIResponse => ({ facts, total, limit: 500, offset: 0 });

describe('indexFacts', () => {
  it('orders by importance and records clamped counts', () => {
    expect(INDEX.map((f) => [f.fact.id, f.count])).toEqual([
      [12, 5],
      [7, 3],
      [9, 3],
      [30, 1],
      [4, 1],
    ]);
  });
});

describe('filterFacts', () => {
  it.each<{ name: string; category: FactCategory; query: string; want: number[] }>([
    { name: 'all, no query', category: 'ALL', query: '', want: [12, 7, 9, 30, 4] },
    { name: 'reinforced', category: 'REINFORCED', query: '', want: [12, 7, 9] },
    { name: 'base includes clamped zero', category: 'BASE', query: '', want: [30, 4] },
    { name: 'text query across categories', category: 'ALL', query: 'oat milk', want: [9, 30] },
    { name: 'text query within a pill', category: 'BASE', query: 'oat milk', want: [30] },
    { name: 'id query', category: 'ALL', query: '12', want: [12] },
    { name: 'no match', category: 'REINFORCED', query: 'cumin', want: [] },
  ])('$name', ({ category, query, want }) => {
    expect(filterFacts(INDEX, category, query).map((f) => f.fact.id)).toEqual(want);
  });

  it('returns the index itself when unfiltered', () => {
    expect(filterFacts(INDEX, 'ALL', '')).toBe(INDEX);
  });
});

describe('normalizeQuery', () => {
  it.each([
    { input: '  Oat Milk ', want: 'oat milk' },
    { input: '', want: '' },
  ])('$input', ({ input, want }) => {
    expect(normalizeQuery(input)).toBe(want);
  });
});

describe('categoryCounts', () => {
  it.each([
    { name: 'mixed', facts: FACTS, want: { ALL: 5, REINFORCED: 3, BASE: 2 } },
    { name: 'empty', facts: [], want: { ALL: 0, REINFORCED: 0, BASE: 0 } },
  ])('$name', ({ facts, want }) => {
    expect(categoryCounts(indexFacts(facts))).toEqual(want);
  });
});

describe('memoryMetrics', () => {
  it.each([
    { name: 'before the first payload', data: null, facts: [], want: { total: '--', reinforced: '-- REINFORCED', average: '--' } },
    { name: 'reported total wins', data: response(FACTS, 900), facts: FACTS, want: { total: '900', reinforced: '3 REINFORCED', average: '2.60x' } },
    { name: 'empty archive', data: response([], 0), facts: [], want: { total: '0', reinforced: '0 REINFORCED', average: '0.00x' } },
  ])('$name', ({ data, facts, want }) => {
    expect(memoryMetrics(data, indexFacts(facts))).toEqual(want);
  });
});

describe('factAgeTimestamp', () => {
  it.each([
    { name: 'last reinforcement', f: fact(1, 'a', 2, '2026-11-01T00:00:00Z'), want: '2026-11-01T00:00:00Z' },
    { name: 'invalid reinforcement falls back', f: fact(1, 'a', 2, 'not-a-date'), want: '2026-10-01T00:00:00Z' },
    { name: 'never reinforced', f: fact(1, 'a', 1), want: '2026-10-01T00:00:00Z' },
  ])('$name', ({ f, want }) => {
    expect(factAgeTimestamp(f)).toBe(want);
  });
});

describe('memoryPhase', () => {
  it.each([
    { name: 'loading', loaded: false, error: null, shown: 0, want: 'loading' },
    { name: 'error before data', loaded: false, error: 'down', shown: 0, want: 'error' },
    { name: 'error over cached data', loaded: true, error: 'down', shown: 3, want: 'error' },
    { name: 'empty', loaded: true, error: null, shown: 0, want: 'empty' },
    { name: 'ready', loaded: true, error: null, shown: 3, want: 'ready' },
  ])('$name', ({ loaded, error, shown, want }) => {
    expect(memoryPhase(loaded, error, shown)).toBe(want);
  });
});

describe('resultsLabel', () => {
  it('formats the count', () => {
    expect(resultsLabel(15, 150)).toBe('SHOWING 15 / 150 NODES');
  });
});
