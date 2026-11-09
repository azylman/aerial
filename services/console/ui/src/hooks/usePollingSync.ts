import { useEffect, useRef } from 'react';
import { useDocumentVisible } from './useDocumentVisible';

/** One poll: must resolve (never reject) and should stop work when `signal` aborts. */
export type PollFetcher = (signal: AbortSignal) => Promise<void>;

/**
 * Runs `fetcher` immediately and then every `intervalMs` while `enabled` and the document is
 * visible. Disabling (tab switch, hidden page, unmount) clears the interval and aborts the
 * in-flight request, so inactive views cost nothing. A tick that lands while the previous request
 * is still in flight is skipped rather than stacked.
 */
export function usePollingSync(fetcher: PollFetcher, intervalMs: number, enabled: boolean): void {
  const visible = useDocumentVisible();
  const active = enabled && visible;
  const latest = useRef(fetcher);

  useEffect(() => {
    latest.current = fetcher;
  }, [fetcher]);

  useEffect(() => {
    if (!active) return undefined;
    let inFlight: AbortController | null = null;

    const run = (): void => {
      if (inFlight !== null) return;
      const controller = new AbortController();
      inFlight = controller;
      void latest.current(controller.signal).finally(() => {
        if (inFlight === controller) inFlight = null;
      });
    };

    run();
    const id = setInterval(run, intervalMs);
    return () => {
      clearInterval(id);
      inFlight?.abort();
      inFlight = null;
    };
  }, [active, intervalMs]);
}

/**
 * Adapts a typed API fetcher and its store setters into a PollFetcher: success settles the
 * resource, failure records the message, and aborts are ignored (the poller cancelled them).
 */
export function syncResource<T>(
  fetch: (signal: AbortSignal) => Promise<T>,
  onData: (data: T) => void,
  onError: (message: string) => void,
): PollFetcher {
  return async (signal) => {
    try {
      const data = await fetch(signal);
      if (!signal.aborted) onData(data);
    } catch (err) {
      if (signal.aborted) return;
      onError(err instanceof Error ? err.message : String(err));
    }
  };
}
