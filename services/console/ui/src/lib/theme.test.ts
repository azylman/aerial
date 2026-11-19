import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  applyTheme,
  initTheme,
  readStoredTheme,
  THEME_STORAGE_KEY,
  type ThemeMode,
} from './theme';

interface ReadStoredRow {
  readonly name: string;
  readonly stored: string | null;
  readonly shouldThrow?: boolean;
  readonly want: ThemeMode;
}

const readStoredRows: readonly ReadStoredRow[] = [
  { name: 'returns light when stored value is light', stored: 'light', want: 'light' },
  { name: 'returns dark when stored value is dark', stored: 'dark', want: 'dark' },
  { name: 'returns dark when stored value is null', stored: null, want: 'dark' },
  { name: 'returns dark when stored value is arbitrary string', stored: 'polar-contrast', want: 'dark' },
  { name: 'returns dark fallback when localStorage getItem throws', stored: null, shouldThrow: true, want: 'dark' },
];

interface ApplyThemeRow {
  readonly name: string;
  readonly theme: ThemeMode;
  readonly shouldThrowStorage?: boolean;
  readonly omitMetaTag?: boolean;
  readonly wantMetaColor?: string;
}

const applyThemeRows: readonly ApplyThemeRow[] = [
  { name: 'applies dark theme to DOM and localStorage', theme: 'dark', wantMetaColor: '#181825' },
  { name: 'applies light theme to DOM and localStorage', theme: 'light', wantMetaColor: '#f8fafc' },
  {
    name: 'applies theme to DOM even when localStorage setItem throws',
    theme: 'light',
    shouldThrowStorage: true,
    wantMetaColor: '#f8fafc',
  },
  {
    name: 'applies theme to DOM when meta tag is absent',
    theme: 'light',
    omitMetaTag: true,
  },
];

interface InitThemeRow {
  readonly name: string;
  readonly stored: string | null;
  readonly want: ThemeMode;
}

const initThemeRows: readonly InitThemeRow[] = [
  { name: 'initializes dark by default', stored: null, want: 'dark' },
  { name: 'initializes light when stored as light', stored: 'light', want: 'light' },
];

describe('theme', () => {
  let metaTag: HTMLMetaElement | null = null;

  beforeEach(() => {
    window.localStorage.clear();
    delete document.documentElement.dataset.theme;
    document.documentElement.style.colorScheme = '';

    const existing = document.querySelector<HTMLMetaElement>('meta[name="theme-color"]');
    if (existing) {
      existing.remove();
    }
    metaTag = document.createElement('meta');
    metaTag.setAttribute('name', 'theme-color');
    metaTag.setAttribute('content', '#181825');
    document.head.appendChild(metaTag);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    if (metaTag?.parentNode) {
      metaTag.parentNode.removeChild(metaTag);
    }
    metaTag = null;
  });

  describe('readStoredTheme', () => {
    it.each(readStoredRows)('$name', ({ stored, shouldThrow, want }) => {
      if (shouldThrow) {
        vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
          throw new DOMException('SecurityError', 'SecurityError');
        });
      } else if (stored !== null) {
        window.localStorage.setItem(THEME_STORAGE_KEY, stored);
      }

      expect(readStoredTheme()).toBe(want);
    });
  });

  describe('applyTheme', () => {
    it.each(applyThemeRows)('$name', ({ theme, shouldThrowStorage, omitMetaTag, wantMetaColor }) => {
      if (omitMetaTag && metaTag?.parentNode) {
        metaTag.parentNode.removeChild(metaTag);
      }
      if (shouldThrowStorage) {
        vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
          throw new DOMException('QuotaExceededError', 'QuotaExceededError');
        });
      }

      applyTheme(theme);

      expect(document.documentElement.dataset.theme).toBe(theme);
      expect(document.documentElement.style.colorScheme).toBe(theme);
      if (wantMetaColor !== undefined) {
        expect(document.querySelector('meta[name="theme-color"]')?.getAttribute('content')).toBe(wantMetaColor);
      }
      if (!shouldThrowStorage) {
        expect(window.localStorage.getItem(THEME_STORAGE_KEY)).toBe(theme);
      }
    });
  });

  describe('initTheme', () => {
    it.each(initThemeRows)('$name', ({ stored, want }) => {
      if (stored !== null) {
        window.localStorage.setItem(THEME_STORAGE_KEY, stored);
      }

      const active = initTheme();
      expect(active).toBe(want);
      expect(document.documentElement.dataset.theme).toBe(want);
      expect(document.documentElement.style.colorScheme).toBe(want);
    });
  });
});
