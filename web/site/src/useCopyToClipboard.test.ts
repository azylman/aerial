import { act, cleanup, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useCopyToClipboard } from './useCopyToClipboard';

describe('useCopyToClipboard hook', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    cleanup();
  });

  it('copies text successfully, tracks copied item, and resets after delay', async () => {
    const writeTextMock = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, {
      clipboard: {
        writeText: writeTextMock,
      },
    });

    const { result } = renderHook(() => useCopyToClipboard<number>(2000));

    expect(result.current.copiedId).toBeNull();
    expect(result.current.copyFailedId).toBeNull();
    expect(result.current.isCopied(1)).toBe(false);

    let ok = false;
    await act(async () => {
      ok = await result.current.copy('test command', 1);
    });

    expect(ok).toBe(true);
    expect(writeTextMock).toHaveBeenCalledWith('test command');
    expect(result.current.copiedId).toBe(1);
    expect(result.current.isCopied(1)).toBe(true);
    expect(result.current.isCopied(2)).toBe(false);

    // Advance 1000ms: still copied
    act(() => {
      vi.advanceTimersByTime(1000);
    });
    expect(result.current.isCopied(1)).toBe(true);

    // Re-copy restarts timeout
    await act(async () => {
      await result.current.copy('test command', 1);
    });

    act(() => {
      vi.advanceTimersByTime(1000);
    });
    expect(result.current.isCopied(1)).toBe(true);

    // Advance remaining 1000ms: resets
    act(() => {
      vi.advanceTimersByTime(1000);
    });
    expect(result.current.copiedId).toBeNull();
    expect(result.current.isCopied(1)).toBe(false);
  });

  it('handles clipboard failure and resets after delay', async () => {
    const writeTextMock = vi.fn().mockRejectedValue(new Error('Permission denied'));
    Object.assign(navigator, {
      clipboard: {
        writeText: writeTextMock,
      },
    });

    const { result } = renderHook(() => useCopyToClipboard<string>());

    let ok = true;
    await act(async () => {
      ok = await result.current.copy('fail command', 'item-a');
    });

    expect(ok).toBe(false);
    expect(result.current.copiedId).toBeNull();
    expect(result.current.copyFailedId).toBe('item-a');
    expect(result.current.isFailed('item-a')).toBe(true);
    expect(result.current.isFailed('item-b')).toBe(false);

    act(() => {
      vi.advanceTimersByTime(2500);
    });
    expect(result.current.copyFailedId).toBeNull();
    expect(result.current.isFailed('item-a')).toBe(false);
  });

  it('handles missing navigator.clipboard gracefully', async () => {
    Object.defineProperty(navigator, 'clipboard', {
      value: undefined,
      configurable: true,
      writable: true,
    });

    const { result } = renderHook(() => useCopyToClipboard());

    let ok = true;
    await act(async () => {
      ok = await result.current.copy('text');
    });

    expect(ok).toBe(false);
    expect(result.current.isFailed()).toBe(true);
  });

  it('cleans up pending timer on unmount', async () => {
    const writeTextMock = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, {
      clipboard: {
        writeText: writeTextMock,
      },
    });

    const { result, unmount } = renderHook(() => useCopyToClipboard());

    await act(async () => {
      await result.current.copy('command');
    });

    expect(vi.getTimerCount()).toBe(1);
    unmount();
    expect(vi.getTimerCount()).toBe(0);
  });
});
