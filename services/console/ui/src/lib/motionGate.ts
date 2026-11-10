import { createContext, useContext } from 'react';

/**
 * Decides when the looping CSS animations may start. `start` runs exactly once, inside whichever
 * call first leaves the gate opened with no unreleased holds. Lists that render a limited first
 * pass hold it until their fill-in has painted, so the animations' style recalc never lands on
 * the same frame as the fill-in commit.
 */
export interface MotionGate {
  /** Delays the start until the returned release runs; inert (returns noRelease) once started. */
  hold(): () => void;
  /** Allows the start once every hold is released. */
  open(): void;
}

/** The release a hold returns when there is nothing to release: the gate is inert or started. */
export const noRelease = (): void => undefined;

export function createMotionGate(start: () => void): MotionGate {
  let opened = false;
  let started = false;
  let holds = 0;
  const startIfReady = (): void => {
    if (started || !opened || holds > 0) return;
    started = true;
    start();
  };
  return {
    hold() {
      if (started) return noRelease;
      holds += 1;
      let released = false;
      return () => {
        if (released) return;
        released = true;
        holds -= 1;
        startIfReady();
      };
    },
    open() {
      opened = true;
      startIfReady();
    },
  };
}

const inertGate: MotionGate = { hold: () => noRelease, open: noRelease };

/** Without a provider, holds do nothing: components and tests behave as if there is no gate. */
export const MotionGateContext = createContext<MotionGate>(inertGate);

export function useMotionGate(): MotionGate {
  return useContext(MotionGateContext);
}
