import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { syncResource, usePollingSync } from './usePollingSync';

let hidden = false;

function setHidden(next: boolean): void {
  hidden = next;
  document.dispatchEvent(new Event('visibilitychange'));
}

/** A fetcher whose requests stay in flight until the test settles them. */
function deferredFetcher() {
  const signals: AbortSignal[] = [];
  const settles: Array<() => void> = [];
  const fetcher = (signal: AbortSignal): Promise<void> => {
    signals.push(signal);
    return new Promise<void>((resolve) => settles.push(resolve));
  };
  return { fetcher, signals, settles };
}

interface Props {
  enabled: boolean;
}

describe('usePollingSync', () => {
  beforeEach(() => {
    hidden = false;
    vi.spyOn(document, 'hidden', 'get').mockImplementation(() => hidden);
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('fetches immediately, then on every interval once the previous request settled', async () => {
    const { fetcher, signals, settles } = deferredFetcher();
    const { unmount } = renderHook(() => usePollingSync(fetcher, 5000, true));
    expect(signals).toHaveLength(1);

    // Tick while the first request is still in flight: skipped, not stacked.
    act(() => vi.advanceTimersByTime(5000));
    expect(signals).toHaveLength(1);

    await act(async () => settles[0]?.());
    act(() => vi.advanceTimersByTime(5000));
    expect(signals).toHaveLength(2);
    unmount();
  });

  it('never fetches while disabled', () => {
    const { fetcher, signals } = deferredFetcher();
    renderHook(() => usePollingSync(fetcher, 5000, false));
    act(() => vi.advanceTimersByTime(20_000));
    expect(signals).toHaveLength(0);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('disabling (inactive tab) clears the timer and aborts the request in flight', () => {
    const { fetcher, signals } = deferredFetcher();
    const { rerender } = renderHook(({ enabled }: Props) => usePollingSync(fetcher, 5000, enabled), {
      initialProps: { enabled: true },
    });
    rerender({ enabled: false });
    expect(signals[0]?.aborted).toBe(true);
    expect(vi.getTimerCount()).toBe(0);

    rerender({ enabled: true });
    expect(signals).toHaveLength(2);
    expect(signals[1]?.aborted).toBe(false);
  });

  it('pauses while the document is hidden and refetches when visible', () => {
    const { fetcher, signals } = deferredFetcher();
    renderHook(() => usePollingSync(fetcher, 5000, true));
    act(() => setHidden(true));
    expect(signals[0]?.aborted).toBe(true);
    expect(vi.getTimerCount()).toBe(0);

    act(() => setHidden(false));
    expect(signals).toHaveLength(2);
  });

  it('unmount aborts the request in flight', () => {
    const { fetcher, signals } = deferredFetcher();
    const { unmount } = renderHook(() => usePollingSync(fetcher, 5000, true));
    unmount();
    expect(signals[0]?.aborted).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });

  it('uses the latest fetcher without restarting the interval', async () => {
    const first = deferredFetcher();
    const second = deferredFetcher();
    const { rerender } = renderHook(({ f }) => usePollingSync(f, 1000, true), {
      initialProps: { f: first.fetcher },
    });
    rerender({ f: second.fetcher });
    await act(async () => first.settles[0]?.());
    act(() => vi.advanceTimersByTime(1000));
    expect(first.signals).toHaveLength(1);
    expect(second.signals).toHaveLength(1);
  });
});

interface SyncRow {
  name: string;
  outcome: 'resolve' | 'reject' | 'rejectString';
  abortBeforeSettle: boolean;
  wantData: string[];
  wantErrors: string[];
}

describe('syncResource', () => {
  it.each<SyncRow>([
    { name: 'success settles data', outcome: 'resolve', abortBeforeSettle: false, wantData: ['payload'], wantErrors: [] },
    { name: 'Error failure records its message', outcome: 'reject', abortBeforeSettle: false, wantData: [], wantErrors: ['HTTP 503'] },
    { name: 'non-Error failure is stringified', outcome: 'rejectString', abortBeforeSettle: false, wantData: [], wantErrors: ['offline'] },
    { name: 'aborted success is dropped', outcome: 'resolve', abortBeforeSettle: true, wantData: [], wantErrors: [] },
    { name: 'aborted failure is dropped', outcome: 'reject', abortBeforeSettle: true, wantData: [], wantErrors: [] },
  ])('$name', async ({ outcome, abortBeforeSettle, wantData, wantErrors }) => {
    const data: string[] = [];
    const errors: string[] = [];
    const controller = new AbortController();
    const fetch = (): Promise<string> => {
      if (abortBeforeSettle) controller.abort();
      if (outcome === 'resolve') return Promise.resolve('payload');
      return Promise.reject(outcome === 'reject' ? new Error('HTTP 503') : 'offline');
    };
    await syncResource(fetch, (d) => data.push(d), (e) => errors.push(e))(controller.signal);
    expect(data).toEqual(wantData);
    expect(errors).toEqual(wantErrors);
  });
});

/** A typed fetch whose requests stay in flight until the test settles them. */
function deferredFetch() {
  const signals: AbortSignal[] = [];
  const settles: Array<{ resolve: (v: string) => void; reject: (e: Error) => void }> = [];
  const fetch = (signal: AbortSignal): Promise<string> => {
    signals.push(signal);
    return new Promise<string>((resolve, reject) => settles.push({ resolve, reject }));
  };
  return { fetch, signals, settles };
}

interface JoinRow {
  name: string;
  /** Concurrent callers, each with its own AbortController. */
  callers: number;
  /** Callers aborted before they call. */
  preAborted: number[];
  /** Callers aborted while the request is in flight. */
  abort: number[];
  /** How the first request settles; null leaves it in flight. */
  settle: 'resolve' | 'reject' | null;
  /** Make one more call once the callers above are done. */
  callAgain: boolean;
  wantFetches: number;
  wantFirstAborted: boolean;
  wantPendingBeforeSettle: number;
  wantData: string[];
  wantErrors: string[];
}

interface FreshRow {
  name: string;
  freshMs: number;
  first: 'resolve' | 'reject';
  waitMs: number;
  wantFetches: number;
}

describe('syncResource freshness', () => {
  beforeEach(() => vi.useFakeTimers({ toFake: ['Date'] }));
  afterEach(() => vi.useRealTimers());

  it.each<FreshRow>([
    { name: 'a call inside freshMs after success reuses it', freshMs: 2000, first: 'resolve', waitMs: 1999, wantFetches: 1 },
    { name: 'a call after freshMs fetches', freshMs: 2000, first: 'resolve', waitMs: 2000, wantFetches: 2 },
    { name: 'a call inside freshMs after failure fetches', freshMs: 2000, first: 'reject', waitMs: 10, wantFetches: 2 },
    { name: 'freshMs 0 always fetches', freshMs: 0, first: 'resolve', waitMs: 0, wantFetches: 2 },
  ])('$name', async ({ freshMs, first, waitMs, wantFetches }) => {
    const urls: string[] = [];
    const fetch = (): Promise<string> => {
      urls.push('/api/x');
      return urls.length === 1 && first === 'reject' ? Promise.reject(new Error('HTTP 503')) : Promise.resolve('payload');
    };
    const poll = syncResource(fetch, () => undefined, () => undefined, freshMs);
    await poll(new AbortController().signal);
    vi.advanceTimersByTime(waitMs);
    await poll(new AbortController().signal);
    expect(urls).toHaveLength(wantFetches);
  });
});

describe('syncResource request sharing', () => {
  it.each<JoinRow>([
    { name: 'concurrent callers join one request', callers: 3, preAborted: [], abort: [], settle: 'resolve', callAgain: false, wantFetches: 1, wantFirstAborted: false, wantPendingBeforeSettle: 3, wantData: ['payload'], wantErrors: [] },
    { name: 'a joined failure is recorded once', callers: 2, preAborted: [], abort: [], settle: 'reject', callAgain: false, wantFetches: 1, wantFirstAborted: false, wantPendingBeforeSettle: 2, wantData: [], wantErrors: ['HTTP 503'] },
    { name: 'one caller aborting stops only its wait', callers: 2, preAborted: [], abort: [0], settle: 'resolve', callAgain: false, wantFetches: 1, wantFirstAborted: false, wantPendingBeforeSettle: 1, wantData: ['payload'], wantErrors: [] },
    { name: 'every caller aborting aborts the request', callers: 2, preAborted: [], abort: [1, 0], settle: 'resolve', callAgain: false, wantFetches: 1, wantFirstAborted: true, wantPendingBeforeSettle: 0, wantData: [], wantErrors: [] },
    { name: 'a call after settle fetches again', callers: 1, preAborted: [], abort: [], settle: 'resolve', callAgain: true, wantFetches: 2, wantFirstAborted: false, wantPendingBeforeSettle: 1, wantData: ['payload'], wantErrors: [] },
    { name: 'a call after every caller aborted starts afresh', callers: 1, preAborted: [], abort: [0], settle: null, callAgain: true, wantFetches: 2, wantFirstAborted: true, wantPendingBeforeSettle: 0, wantData: [], wantErrors: [] },
    { name: 'an already-aborted caller never fetches', callers: 1, preAborted: [0], abort: [], settle: null, callAgain: false, wantFetches: 0, wantFirstAborted: false, wantPendingBeforeSettle: 0, wantData: [], wantErrors: [] },
  ])('$name', async ({ callers, preAborted, abort, settle, callAgain, wantFetches, wantFirstAborted, wantPendingBeforeSettle, wantData, wantErrors }) => {
    const { fetch, signals, settles } = deferredFetch();
    const data: string[] = [];
    const errors: string[] = [];
    const poll = syncResource(fetch, (d) => data.push(d), (e) => errors.push(e));

    const controllers = Array.from({ length: callers }, () => new AbortController());
    for (const i of preAborted) controllers[i]?.abort();
    let settledCount = 0;
    const calls = controllers.map((c) => poll(c.signal).then(() => void (settledCount += 1)));
    for (const i of abort) controllers[i]?.abort();
    // Callers that aborted (or never joined) stop waiting without the request settling.
    await Promise.all(calls.filter((_, i) => abort.includes(i) || preAborted.includes(i)));
    expect(callers - settledCount).toBe(wantPendingBeforeSettle);

    if (settle === 'resolve') settles[0]?.resolve('payload');
    if (settle === 'reject') settles[0]?.reject(new Error('HTTP 503'));
    if (settle !== null) await Promise.all(calls);
    if (callAgain) void poll(new AbortController().signal);

    expect(signals).toHaveLength(wantFetches);
    expect(signals[0]?.aborted ?? false).toBe(wantFirstAborted);
    expect(data).toEqual(wantData);
    expect(errors).toEqual(wantErrors);
  });
});
