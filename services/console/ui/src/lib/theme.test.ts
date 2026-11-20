import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  applyTheme,
  initTheme,
  readStoredTheme,
  subscribeTheme,
  THEME_STORAGE_KEY,
  type ThemeMode,
} from './theme';

const SHARED_STORAGE_KEY = 'aerial-theme';

function clearThemeCookies(): void {
  for (let i = 0; i < 3; i++) document.cookie = 'theme=; path=/; max-age=0';
}

interface ReadStoredRow {
  readonly name: string;
  readonly stored: string | null;
  readonly cookie?: string;
  readonly sharedStored?: string;
  readonly prefersDark?: boolean;
  readonly shouldThrow?: boolean;
  readonly want: ThemeMode;
}

const readStoredRows: readonly ReadStoredRow[] = [
  { name: 'returns light when stored value is light', stored: 'light', want: 'light' },
  { name: 'returns dark when stored value is dark', stored: 'dark', want: 'dark' },
  { name: 'returns dark when stored value is null', stored: null, want: 'dark' },
  { name: 'returns dark when stored value is arbitrary string', stored: 'polar-contrast', want: 'dark' },
  { name: 'returns dark fallback when localStorage getItem throws', stored: null, shouldThrow: true, want: 'dark' },
  { name: 'shared cookie wins over the stored mirror', stored: 'dark', cookie: 'theme=light', want: 'light' },
  { name: 'cookie still read when localStorage getItem throws', stored: null, cookie: 'theme=light', shouldThrow: true, want: 'light' },
  { name: 'falls back to the shared storage key without a mirror', stored: null, sharedStored: 'light', want: 'light' },
  { name: 'mirror beats the shared storage key', stored: 'dark', sharedStored: 'light', want: 'dark' },
  { name: 'follows a light OS preference when nothing is stored', stored: null, prefersDark: false, want: 'light' },
];

interface ApplyThemeRow {
  readonly name: string;
  readonly theme: ThemeMode;
  readonly shouldThrowStorage?: boolean;
  readonly omitMetaTag?: boolean;
  readonly wantMetaColor?: string;
  readonly wantCookie: string | null;
}

const applyThemeRows: readonly ApplyThemeRow[] = [
  { name: 'applies dark theme to DOM, localStorage and the shared cookie', theme: 'dark', wantMetaColor: '#181825', wantCookie: 'dark' },
  { name: 'applies light theme to DOM, localStorage and the shared cookie', theme: 'light', wantMetaColor: '#f8fafc', wantCookie: 'light' },
  {
    name: 'applies theme to DOM and cookie even when localStorage setItem throws',
    theme: 'light',
    shouldThrowStorage: true,
    wantMetaColor: '#f8fafc',
    wantCookie: 'light',
  },
  {
    name: 'applies theme to DOM when meta tag is absent',
    theme: 'light',
    omitMetaTag: true,
    wantCookie: 'light',
  },
];

interface InitThemeRow {
  readonly name: string;
  readonly stored: string | null;
  readonly cookie?: string;
  readonly want: ThemeMode;
}

const initThemeRows: readonly InitThemeRow[] = [
  { name: 'initializes dark by default', stored: null, want: 'dark' },
  { name: 'initializes light when stored as light', stored: 'light', want: 'light' },
  { name: 'initializes from the shared cookie over the mirror', stored: 'dark', cookie: 'theme=light', want: 'light' },
];

interface SubscribeRow {
  readonly name: string;
  /** Shared cookie written by "another app" before the event fires. */
  readonly cookieAfter: string;
  /** Dispatched on document with bubbles, so window listeners see it too, as in a browser. */
  readonly event: Event;
  readonly wantCalls: readonly ThemeMode[];
  readonly wantDomTheme: ThemeMode;
}

const storageEvent = (key: string): Event => new StorageEvent('storage', { key, bubbles: true });
const plainEvent = (type: string): Event => new Event(type, { bubbles: true });

const subscribeRows: readonly SubscribeRow[] = [
  { name: 'storage event on the shared key repaints light', cookieAfter: 'theme=light', event: storageEvent(SHARED_STORAGE_KEY), wantCalls: ['light'], wantDomTheme: 'light' },
  { name: 'focus re-reads a cookie flipped on another subdomain', cookieAfter: 'theme=light', event: plainEvent('focus'), wantCalls: ['light'], wantDomTheme: 'light' },
];

describe('theme', () => {
  let metaTag: HTMLMetaElement | null = null;

  beforeEach(() => {
    window.localStorage.clear();
    clearThemeCookies();
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
    vi.unstubAllGlobals();
    clearThemeCookies();
    if (metaTag?.parentNode) {
      metaTag.parentNode.removeChild(metaTag);
    }
    metaTag = null;
  });

  describe('readStoredTheme', () => {
    it.each(readStoredRows)('$name', ({ stored, cookie, sharedStored, prefersDark, shouldThrow, want }) => {
      if (cookie !== undefined) document.cookie = `${cookie}; path=/`;
      if (sharedStored !== undefined) window.localStorage.setItem(SHARED_STORAGE_KEY, sharedStored);
      if (prefersDark !== undefined) vi.stubGlobal('matchMedia', () => ({ matches: prefersDark }));
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
    it.each(applyThemeRows)('$name', ({ theme, shouldThrowStorage, omitMetaTag, wantMetaColor, wantCookie }) => {
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
      expect(/(?:^|;\s*)theme=(dark|light)/.exec(document.cookie)?.[1] ?? null).toBe(wantCookie);
    });
  });

  describe('initTheme', () => {
    it.each(initThemeRows)('$name', ({ stored, cookie, want }) => {
      if (cookie !== undefined) document.cookie = `${cookie}; path=/`;
      if (stored !== null) {
        window.localStorage.setItem(THEME_STORAGE_KEY, stored);
      }

      const active = initTheme();
      expect(active).toBe(want);
      expect(document.documentElement.dataset.theme).toBe(want);
      expect(document.documentElement.style.colorScheme).toBe(want);
    });
  });

  describe('subscribeTheme', () => {
    it.each(subscribeRows)('$name', ({ cookieAfter, event, wantCalls, wantDomTheme }) => {
      document.cookie = 'theme=dark; path=/';
      document.documentElement.dataset.theme = 'dark';
      const calls: ThemeMode[] = [];
      const unsubscribe = subscribeTheme((theme) => calls.push(theme));

      document.cookie = `${cookieAfter}; path=/`;
      document.dispatchEvent(event);
      unsubscribe();
      // Detached: later events reach nobody.
      document.cookie = 'theme=dark; path=/';
      window.dispatchEvent(new Event('focus'));

      expect(calls).toEqual(wantCalls);
      expect(document.documentElement.dataset.theme).toBe(wantDomTheme);
    });
  });
});
