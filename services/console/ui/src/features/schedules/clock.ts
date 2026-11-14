import { useSyncExternalStore } from 'react';
import { useLiveNow } from '../../hooks/useLiveNow';
import { parseValidTimestampMs } from '../../lib/formatters';

// Drift correction for schedule countdowns:
// each schedules poll records how far the server clock is ahead of the browser's, and every
// countdown counts against server time so a skewed laptop clock still fires "in 5s" on time.

const listeners = new Set<() => void>();
let skewMs = 0;

/** Records the server clock from a schedules response received at `receivedAt` (epoch ms). */
export function recordServerTime(systemTime: string | undefined, receivedAt: number): void {
  const serverMs = parseValidTimestampMs(systemTime);
  if (serverMs === null) return;
  const next = serverMs - receivedAt;
  if (next === skewMs) return;
  skewMs = next;
  for (const listener of listeners) listener();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

const getSkew = (): number => skewMs;

/** Server-corrected epoch milliseconds, re-rendering the caller on the shared 1s clock. */
export function useServerNow(): number {
  return useLiveNow() + useSyncExternalStore(subscribe, getSkew);
}
