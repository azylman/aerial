// Shared dual-theme state (web/theme/aerial-theme.js) installs window.AerialTheme as a side effect.
import '@aerial-theme/aerial-theme.js';

export type ThemeMode = 'dark' | 'light';

/** Console's own mirror of the shared `theme` cookie, which is canonical across apps. */
export const THEME_STORAGE_KEY = 'aerial-console-theme';

export const THEME_COLOR: Readonly<Record<ThemeMode, string>> = {
  dark: '#181825',
  light: '#f8fafc',
} as const;

/** Shared cookie first, then the Console mirror, then prefers-color-scheme, then dark. */
export function readStoredTheme(): ThemeMode {
  return window.AerialTheme.get(THEME_STORAGE_KEY);
}

/** Paints the DOM and writes the Console mirror (THEME_STORAGE_KEY), whether or not the shared value changed. */
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

/** Paints the theme and writes the Console mirror, then makes it the shared choice (the shared write is a no-op when unchanged). */
export function applyTheme(theme: ThemeMode): void {
  paintAndMirrorTheme(theme);
  window.AerialTheme.set(theme);
}

export function initTheme(): ThemeMode {
  const theme = readStoredTheme();
  applyTheme(theme);
  return theme;
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
