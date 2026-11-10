import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { afterNextPaint } from '../lib/afterPaint';
import { noRelease, useMotionGate } from '../lib/motionGate';

/** How many items a list renders: all of them once filled, otherwise at most `limit`. */
export function firstPassCount(total: number, limit: number, filled: boolean): number {
  return filled ? total : Math.min(total, limit);
}

/**
 * Caps a long list's first commit at about one screen of items, then renders the rest in one
 * plain (non-transition) render right after that frame paints. Once filled it stays filled for
 * the component's life, so later polls, filters and searches render everything. A list that
 * mounts empty (data still loading) gets its limited pass on the first render that has items.
 *
 * While the list has items and its fill-in has not painted yet, it holds the motion gate, so the
 * looping animations start on a later frame than the fill-in commit instead of the same one.
 */
export function useFirstPassLimit(total: number, limit: number): number {
  const [filled, setFilled] = useState(false);
  const hasItems = total > 0;
  useEffect(() => {
    if (filled || !hasItems) return undefined;
    return afterNextPaint(() => setFilled(true));
  }, [filled, hasItems]);

  const gate = useMotionGate();
  const release = useRef<(() => void) | null>(null);
  const settled = useRef(false);
  // Taken during the commit, before the first paint: main.tsx opens the gate from a paint callback
  // that a passive effect could lose the race to.
  useLayoutEffect(() => {
    if (!hasItems || settled.current) return undefined;
    const releaseHold = gate.hold();
    if (releaseHold === noRelease) return undefined;
    release.current = releaseHold;
    return () => {
      release.current = null;
      releaseHold();
    };
  }, [gate, hasItems]);
  // Scheduled during the fill-in commit, so it runs once the frame with every item has painted.
  useLayoutEffect(() => {
    const releaseHold = release.current;
    if (!filled || releaseHold === null) return undefined;
    return afterNextPaint(() => {
      settled.current = true;
      releaseHold();
    });
  }, [filled, gate, hasItems]);
  return firstPassCount(total, limit, filled);
}
