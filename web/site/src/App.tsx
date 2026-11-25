import { type ChangeEvent, useEffect, useState } from 'react';
import { useAerialTheme } from './theme';

export { useAerialTheme };

export type RouteView = 'landing' | 'docs';

export interface RouteState {
  view: RouteView;
  chapterId?: string;
}

export interface ChapterInfo {
  id: string;
  number: number;
  title: string;
  summary: string;
}

export const CHAPTERS: readonly ChapterInfo[] = [
  {
    id: 'overview',
    number: 1,
    title: 'Overview & Why Aerial',
    summary: 'The closed-loop thesis, real-world autonomous workflows, glass-box operations, and architecture philosophy.',
  },
  {
    id: 'getting-started',
    number: 2,
    title: 'Getting Started & Two-Repo GitOps',
    summary: 'Single-node k3s bootstrap, aerial vs. aerial-config split, Infisical PushSecret bootstrap, and cluster overrides.',
  },
  {
    id: 'application-platform',
    number: 3,
    title: 'The Self-Hosted Application Platform',
    summary: 'NGINX Gateway Fabric, CoreDNS mesh, CloudNativePG PostgreSQL 19 + pgvector, Restate durable execution, and NATS JetStream.',
  },
  {
    id: 'software-factory',
    number: 4,
    title: 'The Autonomous Software Factory',
    summary: 'Vikunja DAG ready queue, tiered multi-agent review, Vanadis multi-PR DAG orchestration, and push GitOps remediation.',
  },
  {
    id: 'multimodal-agent',
    number: 5,
    title: 'The Multimodal AI Agent',
    summary: 'Eri stateless ingress, Eri-Restate sessions, Coven ACT-R memory & skill synthesis, Haro scheduler, Discord, voice, and browser automation.',
  },
  {
    id: 'security-guardrails',
    number: 6,
    title: 'Security & Defense-in-Depth Guardrails',
    summary: 'Read-only repository mounts, child-process secret scrubbing, hook-guard pre-tool interceptors, and admin RBAC.',
  },
  {
    id: 'customization-day2',
    number: 7,
    title: 'Customization & Day-2 Operations',
    summary: 'Authoring SKILL.md runbooks, registering Streamable HTTP MCP servers, channel lifecycle webhooks, and homelab workloads.',
  },
] as const;

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
  const parts = clean.split('/').filter(Boolean);
  if (parts[0] === 'docs') {
    const chapterId = parts[1] || 'overview';
    return {
      view: 'docs',
      chapterId,
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
    if (typeof window.scrollTo === 'function') {
      window.scrollTo(0, 0);
    }
  }, [route.view, route.chapterId]);

  const handleChapterJump = (event: ChangeEvent<HTMLSelectElement>) => {
    const nextChapter = event.target.value;
    if (nextChapter) {
      window.location.hash = `#/docs/${nextChapter}`;
    }
  };

  const currentIndex = CHAPTERS.findIndex((ch) => ch.id === currentChapter.id);
  const prevChapter = currentIndex > 0 ? CHAPTERS[currentIndex - 1] : null;
  const nextChapter = currentIndex < CHAPTERS.length - 1 ? CHAPTERS[currentIndex + 1] : null;

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
          <section className="landing-view" data-testid="landing-view">
            <div className="hero-section">
              <div className="hero-tag">
                <span>XVX-016 Gundam Aerial</span>
                <span>•</span>
                <span>Production Architecture</span>
              </div>
              <h1 className="hero-title">Autonomous AI Personal Assistant & Software Factory</h1>
              <p className="hero-subtitle">
                A closed-loop, self-hosted platform pairing continuous software delivery with multimodal intelligence.
                From Discord and voice interactions to multi-PR DAG builds and automated GitHub review loops.
              </p>
              <div className="hero-actions">
                <a href="#/docs/overview" className="btn-primary">
                  Read the Handbook
                </a>
                <a href="#/docs/getting-started" className="btn-secondary">
                  Getting Started Guide
                </a>
              </div>
            </div>

            <div className="features-grid">
              <div className="feature-card">
                <div className="feature-card-header">
                  <span className="feature-icon" role="img" aria-label="factory">
                    ⚡
                  </span>
                  <h2 className="feature-title">Autonomous Software Factory</h2>
                </div>
                <p className="feature-desc">
                  Vikunja DAG backlog, tiered multi-agent review panel (the girl gang), Vanadis multi-chunk builds, and
                  automated CI self-healing remediation wakes.
                </p>
              </div>

              <div className="feature-card">
                <div className="feature-card-header">
                  <span className="feature-icon" role="img" aria-label="platform">
                    🪐
                  </span>
                  <h2 className="feature-title">Self-Hosted Platform</h2>
                </div>
                <p className="feature-desc">
                  3-node NixOS k3s cluster, CloudNativePG PostgreSQL 19 + pgvector, Restate durable execution, NATS
                  JetStream, and VictoriaMetrics telemetry.
                </p>
              </div>

              <div className="feature-card">
                <div className="feature-card-header">
                  <span className="feature-icon" role="img" aria-label="agent">
                    🛰️
                  </span>
                  <h2 className="feature-title">Multimodal Intelligence</h2>
                </div>
                <p className="feature-desc">
                  Stateless Eri ingress, Coven ACT-R memory decay & procedural skill synthesis, Haro scheduler, Discord,
                  voice, and headless browser automation.
                </p>
              </div>
            </div>
          </section>
        ) : (
          <section className="handbook-view" data-testid="handbook-view">
            <div className="handbook-container">
              <aside className="handbook-sidebar" aria-label="Handbook chapters">
                <div className="handbook-sidebar-title">Deep-Dive Handbook</div>
                <ul className="handbook-nav-list">
                  {CHAPTERS.map((ch) => (
                    <li key={ch.id}>
                      <a
                        href={`#/docs/${ch.id}`}
                        className={`handbook-nav-link ${ch.id === currentChapter.id ? 'active' : ''}`}
                        aria-current={ch.id === currentChapter.id ? 'page' : undefined}
                      >
                        <span>Ch. {ch.number}:</span>
                        <span>{ch.title}</span>
                      </a>
                    </li>
                  ))}
                </ul>
              </aside>

              <article className="handbook-content">
                <div className="chapter-badge">CHAPTER {currentChapter.number}</div>
                <h1 className="chapter-heading">{currentChapter.title}</h1>
                <div className="chapter-body">
                  <p>{currentChapter.summary}</p>
                  <p>
                    This chapter provides detailed technical specifications, architectural diagrams, and step-by-step
                    configuration procedures for Aerial's production systems.
                  </p>
                </div>

                <nav className="chapter-nav-buttons" aria-label="Chapter pagination">
                  {prevChapter ? (
                    <a href={`#/docs/${prevChapter.id}`} className="btn-secondary" aria-label="Previous chapter">
                      ← Ch. {prevChapter.number}: {prevChapter.title}
                    </a>
                  ) : (
                    <div />
                  )}
                  {nextChapter ? (
                    <a href={`#/docs/${nextChapter.id}`} className="btn-secondary" aria-label="Next chapter">
                      Ch. {nextChapter.number}: {nextChapter.title} →
                    </a>
                  ) : (
                    <div />
                  )}
                </nav>
              </article>
            </div>
          </section>
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
