export type ThemeMode = 'dark' | 'light';

export const THEME_STORAGE_KEY = 'aerial-console-theme';

export const THEME_COLOR: Readonly<Record<ThemeMode, string>> = {
  dark: '#181825',
  light: '#f8fafc',
} as const;

export function readStoredTheme(): ThemeMode {
  try {
    const val = window.localStorage.getItem(THEME_STORAGE_KEY);
    return val === 'light' ? 'light' : 'dark';
  } catch {
    // Storage access may be restricted in sandboxed or third-party contexts
    return 'dark';
  }
}

export function applyTheme(theme: ThemeMode): void {
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

export function initTheme(): ThemeMode {
  const theme = readStoredTheme();
  applyTheme(theme);
  return theme;
}

export function nextTheme(theme: ThemeMode): ThemeMode {
  return theme === 'dark' ? 'light' : 'dark';
}
