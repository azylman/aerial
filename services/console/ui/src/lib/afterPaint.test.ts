import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { afterNextPaint } from './afterPaint';

// Each step fires only the next pending timer: step 1 is the animation frame, step 2 the
// setTimeout queued from it. cancelBefore is the step index at which cancel() is called,
// and wantPending is how many timers are still queued at the end.
describe('afterNextPaint', () => {
  beforeEach(() => {
    vi.useFakeTimers({
      toFake: ['requestAnimationFrame', 'cancelAnimationFrame', 'setTimeout', 'clearTimeout'],
    });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it.each([
    { name: 'nothing fired yet', steps: 0, cancelBefore: null, wantCalls: 0, wantPending: 1 },
    { name: 'frame fired, timeout still pending', steps: 1, cancelBefore: null, wantCalls: 0, wantPending: 1 },
    { name: 'frame fired then timeout ran', steps: 2, cancelBefore: null, wantCalls: 1, wantPending: 0 },
    { name: 'runs once only', steps: 5, cancelBefore: null, wantCalls: 1, wantPending: 0 },
    { name: 'cancelled before the frame', steps: 5, cancelBefore: 0, wantCalls: 0, wantPending: 0 },
    { name: 'cancelled between frame and timeout', steps: 5, cancelBefore: 1, wantCalls: 0, wantPending: 0 },
    { name: 'cancel after it ran is a no-op', steps: 5, cancelBefore: 2, wantCalls: 1, wantPending: 0 },
  ])('$name', ({ steps, cancelBefore, wantCalls, wantPending }) => {
    const callback = vi.fn();
    const cancel = afterNextPaint(callback);
    for (let step = 0; step < steps; step++) {
      if (step === cancelBefore) cancel();
      vi.advanceTimersToNextTimer();
    }
    expect(callback).toHaveBeenCalledTimes(wantCalls);
    expect(vi.getTimerCount()).toBe(wantPending);
  });
});
