import { startTransition, StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './App';
import { initRouteFromLocation } from './hooks/useHashRoute';

initRouteFromLocation();

const root = document.getElementById('root');
if (root === null) throw new Error('console: #root mount point missing from index.html');
// Rendering inside a transition lets the first paint wait for the active view's lazy chunk
// instead of committing an empty Suspense fallback whose reveal React would then throttle.
startTransition(() => {
  createRoot(root).render(
    <StrictMode>
      <App />
    </StrictMode>,
  );
});
