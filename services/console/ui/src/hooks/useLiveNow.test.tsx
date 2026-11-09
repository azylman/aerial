import { act, cleanup, render } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { useDocumentVisible } from './useDocumentVisible';
import { useLiveNow } from './useLiveNow';

let hidden = false;

function setHidden(next: boolean): void {
  hidden = next;
  document.dispatchEvent(new Event('visibilitychange'));
}

function Clock({ label }: { readonly label: string }) {
  const now = useLiveNow();
  return <span data-testid={label}>{now}</span>;
}

function Visible() {
  return <span data-testid="visible">{String(useDocumentVisible())}</span>;
}

const START = Date.parse('2026-11-08T12:00:00Z');

describe('useLiveNow', () => {
  beforeEach(() => {
    hidden = false;
    vi.spyOn(document, 'hidden', 'get').mockImplementation(() => hidden);
    vi.useFakeTimers({ now: START });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('ticks every subscriber once per second from one shared timer', () => {
    const { getByTestId, unmount } = render(
      <>
        <Clock label="a" />
        <Clock label="b" />
      </>,
    );
    expect(vi.getTimerCount()).toBe(1);
    act(() => vi.advanceTimersByTime(3000));
    expect(getByTestId('a').textContent).toBe(String(START + 3000));
    expect(getByTestId('b').textContent).toBe(String(START + 3000));
    unmount();
  });

  it('stops the ticker when the last subscriber unmounts', () => {
    const first = render(<Clock label="a" />);
    const second = render(<Clock label="b" />);
    first.unmount();
    expect(vi.getTimerCount()).toBe(1);
    second.unmount();
    expect(vi.getTimerCount()).toBe(0);
  });

  it('pauses while hidden and catches up immediately when visible again', () => {
    const { getByTestId, unmount } = render(<Clock label="a" />);
    act(() => setHidden(true));
    expect(vi.getTimerCount()).toBe(0);

    act(() => vi.advanceTimersByTime(10_000));
    expect(getByTestId('a').textContent).toBe(String(START));

    act(() => setHidden(false));
    expect(vi.getTimerCount()).toBe(1);
    expect(getByTestId('a').textContent).toBe(String(START + 10_000));
    unmount();
  });

  it('does not start while mounted into a hidden document', () => {
    hidden = true;
    const { unmount } = render(<Clock label="a" />);
    expect(vi.getTimerCount()).toBe(0);
    unmount();
  });
});

describe('useDocumentVisible', () => {
  afterEach(cleanup);

  beforeEach(() => {
    hidden = false;
    vi.spyOn(document, 'hidden', 'get').mockImplementation(() => hidden);
  });

  it.each([
    { name: 'visible document', toggles: [] as boolean[], want: 'true' },
    { name: 'hidden after visibilitychange', toggles: [true], want: 'false' },
    { name: 'visible again', toggles: [true, false], want: 'true' },
  ])('$name', ({ toggles, want }) => {
    const { getByTestId } = render(<Visible />);
    for (const next of toggles) act(() => setHidden(next));
    expect(getByTestId('visible').textContent).toBe(want);
  });
});
