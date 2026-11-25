import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { App, parseRouteHash, useAerialTheme } from './App';
import {
  applyTheme,
  getServerSnapshot,
  initTheme,
  nextTheme,
  readStoredTheme,
  subscribeTheme,
  THEME_COLOR,
  THEME_STORAGE_KEY,
} from './theme';

describe('parseRouteHash', () => {
  it('parses empty and default hashes as landing view', () => {
    expect(parseRouteHash('')).toEqual({ view: 'landing' });
    expect(parseRouteHash('#')).toEqual({ view: 'landing' });
    expect(parseRouteHash('#/')).toEqual({ view: 'landing' });
    expect(parseRouteHash('   ')).toEqual({ view: 'landing' });
    expect(parseRouteHash('#/unknown-route')).toEqual({ view: 'landing' });
  });

  it('parses docs hashes correctly', () => {
    expect(parseRouteHash('#/docs')).toEqual({ view: 'docs', chapterId: 'overview' });
    expect(parseRouteHash('#/docs/')).toEqual({ view: 'docs', chapterId: 'overview' });
    expect(parseRouteHash('docs')).toEqual({ view: 'docs', chapterId: 'overview' });
    expect(parseRouteHash('#/docs/software-factory')).toEqual({
      view: 'docs',
      chapterId: 'software-factory',
    });
    expect(parseRouteHash('#/docs/getting-started')).toEqual({
      view: 'docs',
      chapterId: 'getting-started',
    });
  });
});

function clearThemeCookies(): void {
  for (let i = 0; i < 3; i++) document.cookie = 'theme=; path=/; max-age=0';
}

describe('theme helpers and useAerialTheme', () => {
  let metaTag: HTMLMetaElement | null = null;

  beforeEach(() => {
    window.localStorage.clear();
    clearThemeCookies();
    applyTheme('dark');

    metaTag = document.querySelector('meta[name="theme-color"]');
    if (!metaTag) {
      metaTag = document.createElement('meta');
      metaTag.name = 'theme-color';
      metaTag.content = '#181825';
      document.head.appendChild(metaTag);
    }
  });

  afterEach(() => {
    cleanup();
    clearThemeCookies();
    if (metaTag && metaTag.parentNode) {
      metaTag.parentNode.removeChild(metaTag);
    }
  });

  it('calculates nextTheme correctly', () => {
    expect(nextTheme('dark')).toBe('light');
    expect(nextTheme('light')).toBe('dark');
    expect(getServerSnapshot()).toBe('dark');
  });

  it('reads stored theme and initializes theme', () => {
    clearThemeCookies();
    localStorage.setItem(THEME_STORAGE_KEY, 'light');
    expect(initTheme()).toBe('light');
    expect(readStoredTheme()).toBe('light');
  });

  it('applies theme and handles localStorage quota error gracefully', () => {
    const setSpy = vi.spyOn(Storage.prototype, 'setItem').mockImplementationOnce(() => {
      throw new Error('QuotaExceededError');
    });

    applyTheme('light');

    expect(document.documentElement.dataset.theme).toBe('light');
    expect(document.documentElement.style.colorScheme).toBe('light');
    expect(metaTag?.content).toBe(THEME_COLOR.light);

    setSpy.mockRestore();
  });

  it('applies theme when meta tag is absent from document', () => {
    if (metaTag && metaTag.parentNode) {
      metaTag.parentNode.removeChild(metaTag);
      metaTag = null;
    }

    applyTheme('light');
    expect(document.documentElement.dataset.theme).toBe('light');
  });

  it('notifies subscribers and unsubscribes cleanly', () => {
    const listener = vi.fn();
    const unsub = subscribeTheme(listener);

    applyTheme('light');
    expect(listener).toHaveBeenCalledWith('light');

    unsub();
    listener.mockClear();
    applyTheme('dark');
    expect(listener).not.toHaveBeenCalled();
  });

  it('provides useAerialTheme hook with working toggle and setTheme', () => {
    function ThemeTestComponent() {
      const { theme, toggleTheme, setTheme } = useAerialTheme();
      return (
        <div>
          <span data-testid="current-theme">{theme}</span>
          <button type="button" onClick={toggleTheme} data-testid="toggle-btn">
            Toggle
          </button>
          <button type="button" onClick={() => setTheme('light')} data-testid="set-light-btn">
            Set Light
          </button>
        </div>
      );
    }

    render(<ThemeTestComponent />);
    expect(screen.getByTestId('current-theme').textContent).toBe('dark');

    fireEvent.click(screen.getByTestId('toggle-btn'));
    expect(screen.getByTestId('current-theme').textContent).toBe('light');

    fireEvent.click(screen.getByTestId('set-light-btn'));
    expect(screen.getByTestId('current-theme').textContent).toBe('light');

    fireEvent.click(screen.getByTestId('toggle-btn'));
    expect(screen.getByTestId('current-theme').textContent).toBe('dark');
  });
});

describe('App component', () => {
  beforeEach(() => {
    window.location.hash = '';
    applyTheme('dark');
  });

  afterEach(() => {
    cleanup();
    window.location.hash = '';
  });

  it('renders landing view by default with header, hero, and features', () => {
    render(<App />);

    expect(screen.getByTestId('landing-view')).toBeTruthy();
    expect(screen.getByRole('banner')).toBeTruthy();
    expect(screen.getByText('AERIAL')).toBeTruthy();
    expect(screen.getByRole('link', { name: 'Aerial Home' })).toBeTruthy();
    expect(screen.getByRole('heading', { level: 1 }).textContent).toContain('Autonomous AI Personal Assistant');
    expect(screen.getByText('Autonomous Software Factory')).toBeTruthy();
    expect(screen.getByText('Self-Hosted Platform')).toBeTruthy();
    expect(screen.getByText('Multimodal Intelligence')).toBeTruthy();
    expect(screen.getByRole('contentinfo')).toBeTruthy();
  });

  it('navigates to handbook view and switches chapters via hashchange', () => {
    render(<App />);

    act(() => {
      window.location.hash = '#/docs/overview';
      window.dispatchEvent(new HashChangeEvent('hashchange'));
    });

    expect(screen.getByTestId('handbook-view')).toBeTruthy();
    expect(screen.getByText('Deep-Dive Handbook')).toBeTruthy();
    expect(screen.getByText('CHAPTER 1')).toBeTruthy();
    expect(screen.getByRole('heading', { level: 1 }).textContent).toBe('Overview & Why Aerial');

    // First chapter has no previous chapter button, but has next chapter button
    expect(screen.queryByLabelText('Previous chapter')).toBeNull();
    const nextBtn = screen.getByLabelText('Next chapter');
    expect(nextBtn).toBeTruthy();
    expect(nextBtn.getAttribute('href')).toBe('#/docs/getting-started');

    // Switch to Chapter 4 (software-factory)
    act(() => {
      window.location.hash = '#/docs/software-factory';
      window.dispatchEvent(new HashChangeEvent('hashchange'));
    });

    expect(screen.getByText('CHAPTER 4')).toBeTruthy();
    expect(screen.getByRole('heading', { level: 1 }).textContent).toBe('The Autonomous Software Factory');
    expect(screen.getByLabelText('Previous chapter')).toBeTruthy();
    expect(screen.getByLabelText('Next chapter')).toBeTruthy();

    // Switch to last chapter (customization-day2)
    act(() => {
      window.location.hash = '#/docs/customization-day2';
      window.dispatchEvent(new HashChangeEvent('hashchange'));
    });

    expect(screen.getByText('CHAPTER 7')).toBeTruthy();
    expect(screen.getByLabelText('Previous chapter')).toBeTruthy();
    expect(screen.queryByLabelText('Next chapter')).toBeNull();
  });

  it('falls back to Chapter 1 if unknown chapterId is requested in docs view', () => {
    render(<App />);

    act(() => {
      window.location.hash = '#/docs/nonexistent-chapter';
      window.dispatchEvent(new HashChangeEvent('hashchange'));
    });

    expect(screen.getByTestId('handbook-view')).toBeTruthy();
    expect(screen.getByText('CHAPTER 1')).toBeTruthy();
    expect(screen.getByRole('heading', { level: 1 }).textContent).toBe('Overview & Why Aerial');
  });

  it('handles chapter jump dropdown selection with both valid and empty values', () => {
    render(<App />);

    const select = screen.getByLabelText('Chapter quick-jump');

    // Changing with empty string does not change hash
    fireEvent.change(select, { target: { value: '' } });
    expect(window.location.hash).toBe('');

    // Changing with valid chapter changes hash
    fireEvent.change(select, { target: { value: 'application-platform' } });
    expect(window.location.hash).toBe('#/docs/application-platform');
  });

  it('toggles theme when clicking the theme button in navbar', () => {
    render(<App />);

    const themeBtn = screen.getByLabelText(/Switch to (light|dark) theme/i);
    expect(themeBtn.textContent).toBe('☀️');

    fireEvent.click(themeBtn);
    expect(screen.getByLabelText(/Switch to dark theme/i).textContent).toBe('🌙');

    fireEvent.click(screen.getByLabelText(/Switch to dark theme/i));
    expect(screen.getByLabelText(/Switch to light theme/i).textContent).toBe('☀️');
  });
});
