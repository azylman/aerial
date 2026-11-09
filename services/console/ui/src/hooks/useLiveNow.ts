import { useSyncExternalStore } from 'react';

// One shared 1s clock for every live ticker in the console. The interval exists only while at
// least one component is subscribed AND the document is visible, so idle and hidden tabs pay
// nothing. All subscribers see the same `now`, so a tick costs one React batch, not N timers.

const TICK_MS = 1000;

const listeners = new Set<() => void>();
let now = Date.now();
let timer: ReturnType<typeof setInterval> | null = null;

function publish(): void {
  now = Date.now();
  for (const listener of listeners) listener();
}

function syncTimer(): void {
  const shouldRun = listeners.size > 0 && !document.hidden;
  if (shouldRun && timer === null) {
    timer = setInterval(publish, TICK_MS);
  } else if (!shouldRun && timer !== null) {
    clearInterval(timer);
    timer = null;
  }
}

function onVisibilityChange(): void {
  syncTimer();
  // Coming back to a visible tab: show the current time immediately instead of a stale value.
  if (!document.hidden) publish();
}

function subscribe(listener: () => void): () => void {
  if (listeners.size === 0) {
    now = Date.now();
    document.addEventListener('visibilitychange', onVisibilityChange);
  }
  listeners.add(listener);
  syncTimer();
  return () => {
    listeners.delete(listener);
    if (listeners.size === 0) document.removeEventListener('visibilitychange', onVisibilityChange);
    syncTimer();
  };
}

const getSnapshot = (): number => now;

/** Current epoch milliseconds, re-rendering the caller once per second while visible. */
export function useLiveNow(): number {
  return useSyncExternalStore(subscribe, getSnapshot);
}
