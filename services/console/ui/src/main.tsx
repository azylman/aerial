import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';

function App() {
  return (
    <main className="hud-container">
      <h1>AERIAL // CONSOLE</h1>
    </main>
  );
}

const root = document.getElementById('root');
if (root === null) throw new Error('console: #root mount point missing from index.html');
createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
