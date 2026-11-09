import { useSyncExternalStore } from 'react';

function subscribe(onChange: () => void): () => void {
  document.addEventListener('visibilitychange', onChange);
  return () => document.removeEventListener('visibilitychange', onChange);
}

const getSnapshot = (): boolean => !document.hidden;

/** True while the page is visible. Pollers and tickers pause when the tab is backgrounded. */
export function useDocumentVisible(): boolean {
  return useSyncExternalStore(subscribe, getSnapshot);
}
