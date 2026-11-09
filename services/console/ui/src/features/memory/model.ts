// Pure derivations for the memory archive, ported from updateMemoryMetrics, renderCategoryPills
// and applyFilters in static/app.js. The vanilla UI re-parsed every fact's reinforce count and
// lowercased every fact text on each keystroke; here each fact is indexed once per payload
// (count, search haystack, importance order) and a filter is a single linear pass over that index.

import type { FactItem, FactsAPIResponse } from '../../api/types';
import { parseFactReinforceCount, sortFactsByImportance } from '../../lib/facts';
import { parseValidTimestampMs } from '../../lib/formatters';

export type FactCategory = 'ALL' | 'REINFORCED' | 'BASE';

export const FACT_CATEGORIES: readonly FactCategory[] = ['ALL', 'REINFORCED', 'BASE'];

/** One fact with the values every filter pass needs, computed once per payload. */
export interface IndexedFact {
  readonly fact: FactItem;
  readonly count: number;
  /** Lowercased fact text and id, joined so one `includes` answers "text or id matches". */
  readonly haystack: string;
}

/**
 * Facts in compareFactsImportanceDesc order with their filter keys. Filtering preserves order, so
 * the archive sorts once per payload instead of once per filter change like applyFilters did.
 */
export function indexFacts(facts: readonly FactItem[]): readonly IndexedFact[] {
  return sortFactsByImportance(facts).map((fact) => ({
    fact,
    count: parseFactReinforceCount(fact.reinforce_count),
    haystack: `${fact.fact_text.toLowerCase()}\u0000${String(fact.id || '').toLowerCase()}`,
  }));
}

const inCategory = (count: number, category: FactCategory): boolean =>
  category === 'ALL' || (category === 'REINFORCED' ? count >= 2 : count === 1);

/** Pill counts (renderCategoryPills): a fact is REINFORCED at 2+ reinforcements, else BASE. */
export function categoryCounts(index: readonly IndexedFact[]): Readonly<Record<FactCategory, number>> {
  const reinforced = index.reduce((n, f) => (f.count >= 2 ? n + 1 : n), 0);
  return { ALL: index.length, REINFORCED: reinforced, BASE: index.length - reinforced };
}

/** applyFilters' query normalization: case-insensitive, surrounding whitespace ignored. */
export const normalizeQuery = (search: string): string => search.trim().toLowerCase();

/**
 * Facts matching the category pill and the (normalized) query, in importance order. The
 * unfiltered case returns `index` itself so the list keeps a stable reference.
 */
export function filterFacts(
  index: readonly IndexedFact[],
  category: FactCategory,
  query: string,
): readonly IndexedFact[] {
  if (category === 'ALL' && query === '') return index;
  return index.filter((f) => inCategory(f.count, category) && (query === '' || f.haystack.includes(query)));
}

export interface MemoryMetrics {
  readonly total: string;
  readonly reinforced: string;
  readonly average: string;
}

/** Static #memory-view values before the first payload. */
export const METRICS_IDLE: MemoryMetrics = Object.freeze({ total: '--', reinforced: '-- REINFORCED', average: '--' });

/** updateMemoryMetrics: reported total (falling back to the list size), reinforced count, mean. */
export function memoryMetrics(data: FactsAPIResponse | null, index: readonly IndexedFact[]): MemoryMetrics {
  if (data === null) return METRICS_IDLE;
  const sum = index.reduce((n, f) => n + f.count, 0);
  return {
    total: String(typeof data.total === 'number' ? data.total : index.length),
    reinforced: `${categoryCounts(index).REINFORCED} REINFORCED`,
    average: index.length > 0 ? `${(sum / index.length).toFixed(2)}x` : '0.00x',
  };
}

export const resultsLabel = (shown: number, total: number): string => `SHOWING ${shown} / ${total} NODES`;

/** The timestamp a card ages from: last reinforcement when valid, else creation. */
export const factAgeTimestamp = (fact: FactItem): string | undefined =>
  parseValidTimestampMs(fact.last_reinforced_at) === null ? fact.created_at : fact.last_reinforced_at;

/** What the archive body shows: skeleton, the severed-link box, the empty box or the list. */
export type MemoryPhase = 'loading' | 'error' | 'empty' | 'ready';

/** Like renderMemoryError, a failed sync replaces the grid even when an older payload is cached. */
export function memoryPhase(loaded: boolean, error: string | null, shown: number): MemoryPhase {
  if (error !== null) return 'error';
  if (!loaded) return 'loading';
  return shown === 0 ? 'empty' : 'ready';
}
