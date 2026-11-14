// Memory archive fact ordering: most-reinforced first, then most recently reinforced (or
// created), then highest id.

import type { FactItem } from '../api/types';
import { parseValidTimestampMs } from './formatters';

type FactSortFields = Pick<FactItem, 'id' | 'reinforce_count' | 'last_reinforced_at' | 'created_at'>;

/** Reinforcement count clamped to a whole number >= 1; anything unparseable counts once. */
export function parseFactReinforceCount(val: number | string | null | undefined): number {
  if (val == null || val === '') return 1;
  const num = Number(val);
  if (!Number.isFinite(num) || num < 1) return 1;
  return Math.floor(num);
}

/** Recency key: last reinforcement when valid, else creation time, else 0. */
function factRecencyMs(f: FactSortFields): number {
  return parseValidTimestampMs(f.last_reinforced_at) ?? parseValidTimestampMs(f.created_at) ?? 0;
}

/** Array.prototype.sort comparator ordering facts by importance, descending. */
export function compareFactsImportanceDesc(a: FactSortFields, b: FactSortFields): number {
  return (
    parseFactReinforceCount(b.reinforce_count) - parseFactReinforceCount(a.reinforce_count) ||
    factRecencyMs(b) - factRecencyMs(a) ||
    b.id - a.id
  );
}

/**
 * Returns a new array in compareFactsImportanceDesc order. Sort keys are computed once per fact
 * (not once per comparison) so large archives avoid O(n log n) timestamp parses.
 */
export function sortFactsByImportance<T extends FactSortFields>(facts: readonly T[]): T[] {
  return facts
    .map((fact) => ({ fact, count: parseFactReinforceCount(fact.reinforce_count), recency: factRecencyMs(fact) }))
    .sort((a, b) => b.count - a.count || b.recency - a.recency || b.fact.id - a.fact.id)
    .map((k) => k.fact);
}
