// Shared dual-theme state (web/theme/aerial-theme.js) installs window.AerialTheme as a side effect.
import '@aerial-theme/aerial-theme.js';
import { useCallback, useSyncExternalStore } from 'react';

export type ThemeMode = 'dark' | 'light';

/** Site's own mirror of the shared `theme` cookie, which is canonical across apps. */
export const THEME_STORAGE_KEY = 'aerial-site-theme';

export const THEME_COLOR: Readonly<Record<ThemeMode, string>> = {
  dark: '#181825',
  light: '#f8fafc',
} as const;

/** Shared cookie first, then the site mirror, then prefers-color-scheme, then dark. */
export function readStoredTheme(): ThemeMode {
  return window.AerialTheme.get(THEME_STORAGE_KEY);
}

/** Paints the DOM and writes the site mirror (THEME_STORAGE_KEY), whether or not the shared value changed. */
function paintAndMirrorTheme(theme: ThemeMode): void {
  document.documentElement.dataset.theme = theme;
  document.documentElement.style.colorScheme = theme;

  const meta = document.querySelector<HTMLMetaElement>('meta[name="theme-color"]');
  if (meta) {
    meta.content = THEME_COLOR[theme];
  }

  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, theme);
  } catch {
    // Storage write may fail in quota-exceeded or private mode
  }
}

/** Paints the theme and writes the site mirror, then makes it the shared choice (the shared write is a no-op when unchanged). */
export function applyTheme(theme: ThemeMode): void {
  paintAndMirrorTheme(theme);
  window.AerialTheme.set(theme);
}

export function nextTheme(theme: ThemeMode): ThemeMode {
  return theme === 'dark' ? 'light' : 'dark';
}

/** Repaints and calls listener whenever another tab or app changes the shared theme. */
export function subscribeTheme(listener: (theme: ThemeMode) => void): () => void {
  return window.AerialTheme.subscribe((theme) => {
    paintAndMirrorTheme(theme);
    listener(theme);
  });
}

/**
 * React hook for consuming and updating the shared Aerial theme.
 * Synchronizes with document attributes, theme cookie, and cross-tab storage events.
 */
export function useAerialTheme(): {
  theme: ThemeMode;
  setTheme: (theme: ThemeMode) => void;
  toggleTheme: () => void;
} {
  const theme = useSyncExternalStore(
    subscribeTheme,
    readStoredTheme,
  );

  const setTheme = useCallback((next: ThemeMode) => {
    applyTheme(next);
  }, []);

  const toggleTheme = useCallback(() => {
    applyTheme(nextTheme(theme));
  }, [theme]);

  return { theme, setTheme, toggleTheme };
}
