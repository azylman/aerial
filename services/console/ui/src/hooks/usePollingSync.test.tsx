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
