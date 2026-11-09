import { useEffect, useState } from 'react';
import { afterNextPaint } from '../lib/afterPaint';

/** How many items a list renders: all of them once filled, otherwise at most `limit`. */
export function firstPassCount(total: number, limit: number, filled: boolean): number {
  return filled ? total : Math.min(total, limit);
}

/**
 * Caps a long list's first commit at about one screen of items, then renders the rest in one
 * plain (non-transition) render right after that frame paints. Once filled it stays filled for
 * the component's life, so later polls, filters and searches render everything. A list that
 * mounts empty (data still loading) gets its limited pass on the first render that has items.
 */
export function useFirstPassLimit(total: number, limit: number): number {
  const [filled, setFilled] = useState(false);
  const hasItems = total > 0;
  useEffect(() => {
    if (filled || !hasItems) return undefined;
    return afterNextPaint(() => setFilled(true));
  }, [filled, hasItems]);
  return firstPassCount(total, limit, filled);
}
