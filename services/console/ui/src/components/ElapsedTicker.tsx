import { memo } from 'react';
import { useLiveNow } from '../hooks/useLiveNow';
import { formatElapsedTicker, parseValidTimestampMs } from '../lib/formatters';
import { Glyph } from './Glyph';

/** Whole seconds from `startedAt` to `now`, clamped at zero; zero when the start is unparseable. */
export function elapsedSeconds(startedAt: string, now: number): number {
  const startedMs = parseValidTimestampMs(startedAt);
  return startedMs === null ? 0 : Math.max(0, Math.floor((now - startedMs) / 1000));
}

/**
 * Stopwatch leaf for `.timer-text` elapsed times. It is the only
 * component that subscribes to the shared 1s clock, so a tick re-renders this span and nothing
 * above it: task rows and deploy cards stay untouched.
 */
export const ElapsedTicker = memo(function ElapsedTicker({ startedAt }: { readonly startedAt: string }) {
  const now = useLiveNow();
  return (
    <span className="timer-text">
      <Glyph name="timer" /> {formatElapsedTicker(elapsedSeconds(startedAt, now))}
    </span>
  );
});

/** The pulsing timer pill shared by running tasks and in-flight deploys. */
export const TimerBadge = memo(function TimerBadge({ startedAt }: { readonly startedAt: string }) {
  return (
    <div className="deploy-timer-badge">
      <span className="pulse-indicator" />
      <ElapsedTicker startedAt={startedAt} />
    </div>
  );
});
