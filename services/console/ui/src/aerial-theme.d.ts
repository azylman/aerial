// Globals installed by the shared theme script (web/theme/aerial-theme.js), imported for its side
// effect through the '@aerial-theme' Vite alias. It is a classic script, so there are no named
// exports: everything goes through window.AerialTheme.

type AerialThemeMode = 'dark' | 'light';

interface AerialThemeApi {
  /** Shared cookie first, then the given app key or 'aerial-theme', then prefers-color-scheme, then dark. */
  get(appStorageKey?: string): AerialThemeMode;
  /** Writes the shared cookie and 'aerial-theme', then notifies; a no-op when unchanged. */
  set(theme: AerialThemeMode): void;
  /** Calls fn when another tab or app changes the theme; returns the unsubscribe function. */
  subscribe(fn: (theme: AerialThemeMode) => void): () => void;
}

interface Window {
  readonly AerialTheme: AerialThemeApi;
}

declare module '@aerial-theme/aerial-theme.js';
