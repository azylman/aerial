import { act, render } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { firstPassCount, useFirstPassLimit } from './useFirstPassLimit';

describe('firstPassCount', () => {
  it.each([
    { name: 'below the limit', total: 3, limit: 5, filled: false, want: 3 },
    { name: 'at the limit', total: 5, limit: 5, filled: false, want: 5 },
    { name: 'above the limit', total: 8, limit: 5, filled: false, want: 5 },
    { name: 'filled above the limit', total: 8, limit: 5, filled: true, want: 8 },
    { name: 'filled below the limit', total: 3, limit: 5, filled: true, want: 3 },
    { name: 'empty', total: 0, limit: 5, filled: false, want: 0 },
  ])('$name', ({ total, limit, filled, want }) => {
    expect(firstPassCount(total, limit, filled)).toBe(want);
  });
});

function Probe({ total }: { readonly total: number }) {
  return <span data-testid="count">{useFirstPassLimit(total, 5)}</span>;
}

// Each step fires the next pending timer: the animation frame, then the setTimeout queued from it.
function step(): void {
  act(() => {
    vi.advanceTimersToNextTimer();
  });
}

describe('useFirstPassLimit', () => {
  beforeEach(() => {
    vi.useFakeTimers({
      toFake: ['requestAnimationFrame', 'cancelAnimationFrame', 'setTimeout', 'clearTimeout'],
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('renders the limit first, then everything after the next paint, and stays filled', () => {
    const { getByTestId, rerender, unmount } = render(<Probe total={8} />);
    expect(getByTestId('count').textContent).toBe('5');
    step();
    expect(getByTestId('count').textContent).toBe('5');
    step();
    expect(getByTestId('count').textContent).toBe('8');
    expect(vi.getTimerCount()).toBe(0);
    rerender(<Probe total={12} />);
    expect(getByTestId('count').textContent).toBe('12');
    expect(vi.getTimerCount()).toBe(0);
    unmount();
  });

  it('waits for items before scheduling the fill when it mounts empty', () => {
    const { getByTestId, rerender, unmount } = render(<Probe total={0} />);
    expect(getByTestId('count').textContent).toBe('0');
    expect(vi.getTimerCount()).toBe(0);
    rerender(<Probe total={9} />);
    expect(getByTestId('count').textContent).toBe('5');
    step();
    step();
    expect(getByTestId('count').textContent).toBe('9');
    unmount();
  });

  it('cancels the pending fill on unmount', () => {
    const { unmount } = render(<Probe total={8} />);
    step();
    expect(vi.getTimerCount()).toBe(1);
    unmount();
    expect(vi.getTimerCount()).toBe(0);
  });
});
