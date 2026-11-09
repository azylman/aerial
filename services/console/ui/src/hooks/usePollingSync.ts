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

/** The request every concurrent caller of one syncResource fetcher shares. */
interface SharedRequest {
  readonly controller: AbortController;
  waiting: number;
  done: Promise<void> | null;
}

/**
 * Adapts a typed API fetcher and its store setters into a PollFetcher: success settles the
 * resource, failure records the message, and aborts are ignored (the poller cancelled them).
 *
 * Calls made while a request is in flight join it instead of issuing another, so the boot
 * prefetch, the shell and a freshly mounted view all share one network request. Each caller's
 * signal only stops that caller waiting; the request itself is aborted once every joined caller
 * has aborted. Nothing is cached: a call after the request settled fetches again.
 */
export function syncResource<T>(
  fetch: (signal: AbortSignal) => Promise<T>,
  onData: (data: T) => void,
  onError: (message: string) => void,
): PollFetcher {
  let shared: SharedRequest | null = null;

  const run = async (request: SharedRequest): Promise<void> => {
    const { signal } = request.controller;
    try {
      const data = await fetch(signal);
      if (!signal.aborted) onData(data);
    } catch (err) {
      if (!signal.aborted) onError(err instanceof Error ? err.message : String(err));
    } finally {
      if (shared === request) shared = null;
    }
  };

  return (signal) =>
    new Promise<void>((resolve) => {
      if (signal.aborted) {
        resolve();
        return;
      }
      shared ??= { controller: new AbortController(), waiting: 0, done: null };
      const request = shared;
      request.waiting += 1;
      const leave = (): void => {
        request.waiting -= 1;
        if (request.waiting === 0) {
          if (shared === request) shared = null;
          request.controller.abort();
        }
        resolve();
      };
      // Join before the fetch starts, so an abort raised while it starts is still observed.
      signal.addEventListener('abort', leave, { once: true });
      request.done ??= run(request);
      void request.done.then(() => {
        signal.removeEventListener('abort', leave);
        resolve();
      });
    });
}
