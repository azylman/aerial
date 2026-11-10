import { startTransition, StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './App';
import { startBoot, VIEW_LOADERS } from './boot';
import { initRouteFromLocation } from './hooks/useHashRoute';
import { afterNextPaint } from './lib/afterPaint';
import { createMotionGate, MotionGateContext } from './lib/motionGate';

// The active view's chunk and data start downloading now, all in parallel.
const initialTab = initRouteFromLocation();
void startBoot(initialTab);

const root = document.getElementById('root');
if (root === null) throw new Error('console: #root mount point missing from index.html');

// Every infinite CSS animation is scoped under html.motion-ready: any loop running while the first
// screen renders cost about 30-40ms at 4x CPU throttle. Adding the class two paints in used to land
// on the same frame as the first-pass lists' fill-in commit, so Chrome recalculated style for every
// newly animated element and every new card together: 46ms on schedules at 4x throttle. The gate
// opens two paints in, then also waits for every first-pass list to fill in and paint.
const motionGate = createMotionGate(() => document.documentElement.classList.add('motion-ready'));

const tree = (
  <StrictMode>
    <MotionGateContext value={motionGate}>
      <App />
    </MotionGateContext>
  </StrictMode>
);

const startMotionAfterFirstPaint = (): void => {
  afterNextPaint(() => afterNextPaint(motionGate.open));
};

// Normal path: the view's module is recorded, so the first commit never suspends. Render it at
// default priority in one uninterrupted pass; inside a transition React time-slices it into 5ms
// chunks with yields between them, which cost about 280ms of screen-ready time at 4x CPU throttle.
const renderNow = (): void => {
  createRoot(root).render(tree);
  startMotionAfterFirstPaint();
};

// Failed import: the panel falls back to React.lazy, which suspends and surfaces the error. Only
// here is the transition needed, so the suspended first paint never commits the empty fallback.
const renderInTransition = (): void => {
  startTransition(() => {
    createRoot(root).render(tree);
  });
  startMotionAfterFirstPaint();
};

// Render once the active view's module is here (boot already started this same download), so the
// first commit renders it directly via loadedView instead of suspending in React.lazy.
VIEW_LOADERS[initialTab]().then(renderNow, renderInTransition);
