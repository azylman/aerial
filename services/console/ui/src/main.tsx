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

const renderApp = (): void => {
  // The transition keeps a first paint that still has to suspend (only when the view's chunk
  // failed to load, so its lazy fallback surfaces the error) from committing an empty fallback.
  startTransition(() => {
    createRoot(root).render(
      <StrictMode>
        <App />
      </StrictMode>,
    );
  });
};

// Render once the active view's module is here (boot already started this same download), so the
// first commit renders it directly via loadedView instead of suspending in React.lazy and having
// React throttle the reveal. A failed import still renders and leaves the error to the lazy path.
VIEW_LOADERS[initialTab]().then(renderApp, renderApp);
