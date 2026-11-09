import { startTransition, StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './App';
import { startBoot, VIEW_LOADERS } from './boot';
import { initRouteFromLocation } from './hooks/useHashRoute';

// The active view's chunk and data start downloading now, all in parallel.
const initialTab = initRouteFromLocation();
void startBoot(initialTab);

const root = document.getElementById('root');
if (root === null) throw new Error('console: #root mount point missing from index.html');

const tree = (
  <StrictMode>
    <App />
  </StrictMode>
);

// Normal path: the view's module is recorded, so the first commit never suspends. Render it at
// default priority in one uninterrupted pass; inside a transition React time-slices it into 5ms
// chunks with yields between them, which cost about 280ms of screen-ready time at 4x CPU throttle.
const renderNow = (): void => {
  createRoot(root).render(tree);
};

// Failed import: the panel falls back to React.lazy, which suspends and surfaces the error. Only
// here is the transition needed, so the suspended first paint never commits the empty fallback.
const renderInTransition = (): void => {
  startTransition(() => {
    createRoot(root).render(tree);
  });
};

// Render once the active view's module is here (boot already started this same download), so the
// first commit renders it directly via loadedView instead of suspending in React.lazy.
VIEW_LOADERS[initialTab]().then(renderNow, renderInTransition);
