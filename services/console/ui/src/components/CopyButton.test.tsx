import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { CopyButton } from './CopyButton';

const writeText = vi.fn<(text: string) => Promise<void>>();

beforeEach(() => {
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
  writeText.mockReset();
  Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText } });
});
afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

const button = (): HTMLElement => screen.getByRole('button', { name: 'Copy prompt text' });

async function click(): Promise<void> {
  await act(async () => {
    fireEvent.click(button());
    await Promise.resolve();
  });
}

describe('CopyButton', () => {
  it.each([
    {
      name: 'copies and shows the copied state',
      text: 'post the digest',
      write: 'ok',
      wantWrites: ['post the digest'],
      wantText: 'COPIED',
      wantClass: 'prompt-copy-btn copied',
    },
    {
      name: 'empty text is a no-op',
      text: '',
      write: 'ok',
      wantWrites: [],
      wantText: 'COPY',
      wantClass: 'prompt-copy-btn',
    },
    {
      name: 'clipboard failure keeps the idle state',
      text: 'x',
      write: 'reject',
      wantWrites: ['x'],
      wantText: 'COPY',
      wantClass: 'prompt-copy-btn',
    },
  ] as const)('$name', async ({ text, write, wantWrites, wantText, wantClass }) => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    writeText.mockImplementation(() => (write === 'ok' ? Promise.resolve() : Promise.reject(new Error('denied'))));
    render(<CopyButton text={text} ariaLabel="Copy prompt text" className="prompt-copy-btn" />);
    await click();
    expect(writeText.mock.calls.map(([t]) => t)).toEqual(wantWrites);
    expect([button().textContent?.trim(), button().className]).toEqual([wantText, wantClass]);
    expect(warn).toHaveBeenCalledTimes(write === 'reject' ? 1 : 0);
  });

  it('reverts after the copied delay, restarting it on a second copy', async () => {
    writeText.mockResolvedValue(undefined);
    render(<CopyButton text="t" ariaLabel="Copy prompt text" className="prompt-copy-btn" />);
    await click();
    act(() => vi.advanceTimersByTime(1000));
    await click();
    act(() => vi.advanceTimersByTime(1000));
    expect(button().textContent?.trim()).toBe('COPIED');
    act(() => vi.advanceTimersByTime(800));
    expect([button().textContent?.trim(), button().className]).toEqual(['COPY', 'prompt-copy-btn']);
  });

  it('clears a pending revert on unmount', async () => {
    writeText.mockResolvedValue(undefined);
    const { unmount } = render(<CopyButton text="t" ariaLabel="Copy prompt text" className="prompt-copy-btn" />);
    await click();
    unmount();
    expect(vi.getTimerCount()).toBe(0);
  });
});
