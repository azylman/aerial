import { describe, expect, it } from 'vitest';
import { createMotionGate, type MotionGate } from './motionGate';

// Each op is 'open', 'hold:<name>' or 'release:<name>'; `started` is the expected state after it.
type Op = `open` | `hold:${string}` | `release:${string}`;

describe('createMotionGate', () => {
  it.each<{ name: string; steps: ReadonlyArray<readonly [Op, boolean]> }>([
    { name: 'open with no holds starts immediately', steps: [['open', true]] },
    {
      name: 'holds before open delay the start until the last release',
      steps: [
        ['hold:a', false],
        ['hold:b', false],
        ['open', false],
        ['release:a', false],
        ['release:b', true],
      ],
    },
    {
      name: 'releasing every hold before open waits for open',
      steps: [
        ['hold:a', false],
        ['release:a', false],
        ['open', true],
      ],
    },
    {
      name: 'a double release counts once',
      steps: [
        ['hold:a', false],
        ['hold:b', false],
        ['open', false],
        ['release:a', false],
        ['release:a', false],
        ['release:b', true],
      ],
    },
    {
      name: 'open called twice starts once and stays started',
      steps: [
        ['open', true],
        ['open', true],
      ],
    },
    {
      name: 'a hold after start is inert',
      steps: [
        ['open', true],
        ['hold:a', true],
        ['release:a', true],
        ['open', true],
      ],
    },
  ])('$name', ({ steps }) => {
    let started = false;
    const gate: MotionGate = createMotionGate(() => {
      if (started) throw new Error('start ran twice');
      started = true;
    });
    const releases = new Map<string, () => void>();
    for (const [op, want] of steps) {
      const [kind, key = ''] = op.split(':');
      if (kind === 'open') gate.open();
      else if (kind === 'hold') releases.set(key, gate.hold());
      else releases.get(key)?.();
      expect([op, started]).toEqual([op, want]);
    }
  });
});
