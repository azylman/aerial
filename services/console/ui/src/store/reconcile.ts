// Structural sharing for poll results: every API snapshot passes through reconcile(prev, next)
// before it lands in the store, so unchanged subtrees keep their previous references and
// selector-driven components skip re-rendering when a poll returns the same data.

type Key = string | number;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Identity key for list elements: `id` first, then `name`; undefined when the element has neither. */
function identityOf(item: unknown): Key | undefined {
  if (!isRecord(item)) return undefined;
  const { id, name } = item;
  if (typeof id === 'string' || typeof id === 'number') return id;
  if (typeof name === 'string') return name;
  return undefined;
}

function reconcileArray(prev: readonly unknown[], next: readonly unknown[]): readonly unknown[] {
  // Index previous elements by identity so reordered, inserted or removed rows still reuse
  // the references of the rows that did not change.
  const byKey = new Map<Key, unknown>();
  for (const item of prev) {
    const key = identityOf(item);
    if (key !== undefined && !byKey.has(key)) byKey.set(key, item);
  }

  let same = prev.length === next.length;
  const out = new Array<unknown>(next.length);
  for (let i = 0; i < next.length; i++) {
    const item = next[i];
    const key = identityOf(item);
    const match = key === undefined ? prev[i] : byKey.get(key);
    const merged = reconcileValue(match, item);
    out[i] = merged;
    if (merged !== prev[i]) same = false;
  }
  return same ? prev : out;
}

function reconcileRecord(prev: Record<string, unknown>, next: Record<string, unknown>): Record<string, unknown> {
  const nextKeys = Object.keys(next);
  let same = nextKeys.length === Object.keys(prev).length;
  const out: Record<string, unknown> = {};
  for (const key of nextKeys) {
    const had = Object.hasOwn(prev, key);
    const merged = had ? reconcileValue(prev[key], next[key]) : next[key];
    out[key] = merged;
    if (!had || merged !== prev[key]) same = false;
  }
  return same ? prev : out;
}

function reconcileValue(prev: unknown, next: unknown): unknown {
  if (Object.is(prev, next)) return prev;
  if (Array.isArray(prev) && Array.isArray(next)) return reconcileArray(prev, next);
  if (isRecord(prev) && isRecord(next)) return reconcileRecord(prev, next);
  return next;
}

/**
 * Returns `prev` by reference when `next` is structurally equal to it. Otherwise returns a value
 * equal to `next` whose unchanged subtrees (object properties, and array elements matched by
 * `id`/`name` or by index) are the references from `prev`.
 */
export function reconcile<T>(prev: T, next: T): T {
  return reconcileValue(prev, next) as T;
}
