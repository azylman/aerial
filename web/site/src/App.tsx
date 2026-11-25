import { type ChangeEvent, useEffect, useState } from 'react';
import { useAerialTheme } from './theme';
import { LandingView } from './components/LandingView';
import { HandbookView } from './components/HandbookView';
import { HANDBOOK_CHAPTERS, findChapterForSection } from './data/chapters';

export { useAerialTheme };

export type RouteView = 'landing' | 'docs';

export interface RouteState {
  view: RouteView;
  chapterId?: string;
  sectionId?: string;
}

export interface ChapterInfo {
  id: string;
  number: number;
  title: string;
  summary: string;
}

export const CHAPTERS: readonly ChapterInfo[] = HANDBOOK_CHAPTERS;

/**
 * Parses location hash into RouteState.
 * Supports:
 *   - '#/' or '' -> landing view
 *   - '#/docs' or '#/docs/' -> docs view (chapter 'overview')
 *   - '#/docs/:chapterId' -> docs view with specific chapter
 */
export function parseRouteHash(hash: string): RouteState {
  const clean = hash.replace(/^#\/?/, '').trim();
  if (!clean) {
    return { view: 'landing' };
  }

  // Handle section anchor links (e.g. #section-<id>)
  if (clean.startsWith('section-')) {
    const sectionId = clean.replace(/^section-/, '');
    const chapterId = findChapterForSection(sectionId) || 'overview';
    return {
      view: 'docs',
      chapterId,
      sectionId,
    };
  }

  let pathPart = clean;
  let sectionId: string | undefined;
  if (pathPart.includes('#')) {
    const [path, hashAnchor] = pathPart.split('#', 2);
    pathPart = path || '';
    if (hashAnchor?.startsWith('section-')) {
      sectionId = hashAnchor.replace(/^section-/, '');
    }
  }

  const parts = pathPart.split('/').filter(Boolean);
  if (parts[0] === 'docs') {
    const chapterId = parts[1] || 'overview';
    if (!sectionId && parts[2]) {
      sectionId = parts[2].replace(/^section-/, '');
    }
    return {
      view: 'docs',
      chapterId,
      sectionId,
    };
  }
  return { view: 'landing' };
}

export function useHashRoute(): RouteState {
  const [route, setRoute] = useState<RouteState>(() => parseRouteHash(window.location.hash));

  useEffect(() => {
    const handleHashChange = () => {
      setRoute(parseRouteHash(window.location.hash));
    };

    window.addEventListener('hashchange', handleHashChange);
    return () => {
      window.removeEventListener('hashchange', handleHashChange);
    };
  }, []);

  return route;
}

export function App() {
  const route = useHashRoute();
  const { theme, toggleTheme } = useAerialTheme();

  const activeChapterId = route.chapterId || 'overview';
  const currentChapter = CHAPTERS.find((ch) => ch.id === activeChapterId) || CHAPTERS[0]!;

  useEffect(() => {
    if (route.sectionId) {
      const target = document.getElementById(`section-${route.sectionId}`);
      if (target) {
        if (typeof target.scrollIntoView === 'function') {
          target.scrollIntoView({ behavior: 'smooth' });
        }
        return;
      }
    }
    window.scrollTo(0, 0);
  }, [route.view, route.chapterId, route.sectionId]);

  const handleChapterJump = (event: ChangeEvent<HTMLSelectElement>) => {
    window.location.hash = `#/docs/${event.target.value}`;
  };

  return (
    <div className="site-shell" data-testid="site-shell">
      {/* Top Navigation Bar */}
      <header className="site-navbar" role="banner">
        <div className="navbar-container">
          <a href="#/" className="navbar-brand" aria-label="Aerial Home">
            <span className="brand-badge" aria-hidden="true">
              XVX
            </span>
            <span className="brand-name">AERIAL</span>
            <span className="brand-pill">v2.0</span>
          </a>

          <nav className="navbar-nav" aria-label="Main Navigation">
            <a
              href="#/"
              className={`nav-tab ${route.view === 'landing' ? 'active' : ''}`}
              aria-current={route.view === 'landing' ? 'page' : undefined}
            >
              Overview
            </a>
            <a
              href={`#/docs/${currentChapter.id}`}
              className={`nav-tab ${route.view === 'docs' ? 'active' : ''}`}
              aria-current={route.view === 'docs' ? 'page' : undefined}
            >
              Handbook
            </a>

            <div className="chapter-jump-container">
              <select
                className="chapter-select"
                aria-label="Chapter quick-jump"
                value={route.view === 'docs' ? currentChapter.id : ''}
                onChange={handleChapterJump}
              >
                <option value="" disabled>
                  Jump to chapter...
                </option>
                {CHAPTERS.map((ch) => (
                  <option key={ch.id} value={ch.id}>
                    Ch. {ch.number}: {ch.title}
                  </option>
                ))}
              </select>
              <span className="chapter-select-arrow" aria-hidden="true">
                ▾
              </span>
            </div>
          </nav>

          <div className="navbar-actions">
            <a
              href="https://github.com/azylman/aerial"
              className="action-btn"
              target="_blank"
              rel="noopener noreferrer"
              aria-label="GitHub Repository"
            >
              <span>GitHub</span>
            </a>
            <button
              type="button"
              className="action-btn theme-toggle-btn"
              onClick={toggleTheme}
              aria-label={`Switch to ${theme === 'dark' ? 'light' : 'dark'} theme`}
              title={`Theme: ${theme === 'dark' ? 'Catppuccin Mocha' : 'Polar Slate'}`}
            >
              {theme === 'dark' ? '☀️' : '🌙'}
            </button>
          </div>
        </div>
      </header>

      {/* Main Content Area */}
      <main className="site-main" id="main-content">
        {route.view === 'landing' ? (
          <LandingView />
        ) : (
          <HandbookView chapterId={route.chapterId} />
        )}
      </main>

      {/* Site Footer */}
      <footer className="site-footer" role="contentinfo">
        <div className="footer-content">
          <p>AERIAL // Autonomous AI Personal Assistant — Inspired by XVX-016 Gundam Aerial</p>
          <div className="footer-links">
            <a href="#/">Overview</a>
            <a href="#/docs/overview">Handbook</a>
            <a href="https://github.com/azylman/aerial" target="_blank" rel="noopener noreferrer">
              GitHub
            </a>
          </div>
        </div>
      </footer>
    </div>
  );
}
