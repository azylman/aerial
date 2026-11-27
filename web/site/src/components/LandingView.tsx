import type { ReactNode } from 'react';
import { ArchitectureMap } from './ArchitectureMap';
import { GlassBoxShowcase } from './GlassBoxShowcase';
import { FactoryStepper } from './FactoryStepper';
import { ComparisonMatrix } from './ComparisonMatrix';
import { QuickstartSteps } from './QuickstartSteps';
import { UseCaseGallery, USE_CASE_SCENARIOS } from './UseCaseGallery';
import { FaqSection, FAQ_ITEMS, type FaqItem } from './FaqSection';
import { useCopyToClipboard } from '../useCopyToClipboard';

export interface PillarSpecRow { id: string; capability: string; tech: string; detail: string }
export interface PillarCard { id: string; title: string; description: string; specs: readonly PillarSpecRow[] }

export const PILLAR_CARDS: readonly PillarCard[] = [
  { id: 'platform', title: 'Homelab Application Platform', description: 'Declarative cluster foundation for stateful apps, durable workflows, and unified telemetry.', specs: [
    { id: 'data-vectors', capability: 'Data & Vectors', tech: 'Postgres 19', detail: 'Managed relational + vector storage' },
    { id: 'durable-state', capability: 'Durable State', tech: 'Restate + NATS', detail: 'Journaled execution & JetStream' },
    { id: 'gitops-otel', capability: 'GitOps & OTel', tech: 'Flux + OTel', detail: 'Push deploy + metrics, logs, traces' },
  ] },
  { id: 'factory', title: 'Autonomous Software Factory', description: 'End-to-end multi-PR code generation, peer review, verification, and continuous delivery.', specs: [
    { id: 'dag-builds', capability: 'DAG Builds', tech: 'Vikunja + Restate', detail: 'Cross-repo dependency sequencing' },
    { id: 'code-review', capability: 'Code & Review', tech: 'Claude + Codex', detail: 'Isolated clones & multi-agent panel' },
    { id: 'auto-recovery', capability: 'Auto-Recovery', tech: 'GitHub Actions', detail: 'Proactive wakes on failing checks' },
  ] },
  { id: 'agent', title: 'AI Agent', description: 'Always-on chat, room voice, and phone operator with browser tools and long-term memory.', specs: [
    { id: 'voice-chat', capability: 'Voice & Chat', tech: 'Discord + Voice', detail: 'Threads, room audio & phone calls' },
    { id: 'web-timers', capability: 'Web & Timers', tech: 'CDP + Cron', detail: 'Browser automation & schedules' },
    { id: 'recall-skills', capability: 'Recall & Skills', tech: 'ACT-R + SKILL.md', detail: 'Semantic memory & modular skills' },
  ] },
] as const;

const PILLAR_ICONS: Readonly<Record<string, ReactNode>> = {
  platform: <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><circle cx="12" cy="12" r="7" /><path d="M3.5 15.5c2 2.5 8.5 1.5 14-2s7.5-8.5 5.5-11" /></svg>,
  factory: <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" /></svg>,
  agent: <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d="M12 2a3 3 0 0 0-3 3v7a3 3 0 0 0 6 0V5a3 3 0 0 0-3-3Z" /><path d="M19 10v2a7 7 0 0 1-14 0v-2" /><line x1="12" x2="12" y1="19" y2="22" /></svg>,
};

export {
  ArchitectureMap,
  GlassBoxShowcase,
  FactoryStepper,
  ComparisonMatrix,
  QuickstartSteps,
  UseCaseGallery,
  USE_CASE_SCENARIOS,
  FaqSection,
  FAQ_ITEMS,
  type FaqItem,
};

export const HERO_INSTALL_COMMAND =
  'GITHUB_PAT="your-github-pat" DISCORD_BOT_TOKEN="your-discord-bot-token" bash -c "$(curl -fsSL https://raw.githubusercontent.com/azylman/aerial/main/scripts/quickstart.sh)"';

export function LandingView() {
  const { copy, isCopied, isFailed } = useCopyToClipboard();

  return (
    <section className="landing-view" data-testid="landing-view">
      {/* Hero Section */}
      <div className="hero-section" data-testid="landing-hero">
        <div className="hero-tag">
          <span>XVX-016 Gundam Aerial</span>
          <span>•</span>
          <span>Self-Hosted on Kubernetes (k3s)</span>
        </div>
        <h1 className="hero-title">Homelab Application Platform, Software Factory & AI Agent</h1>
        <p className="hero-subtitle">
          A self-hosted platform where your AI agent manages everyday tasks, automates web and phone errands, and builds and operates stateful homelab apps.
          Talk over Discord or voice to browse the web, schedule persistent routines, or ship multi-PR builds via push GitOps.
        </p>

        {/* Quick-Install Terminal Pill */}
        <div className="hero-install-pill" data-testid="hero-install-pill">
          <span className="install-pill-badge">One-Line Install</span>
          <span className="install-pill-prompt" aria-hidden="true">
            $
          </span>
          <code className="install-pill-code">{HERO_INSTALL_COMMAND}</code>
          <div className="install-pill-actions">
            <button
              type="button"
              className={`install-copy-btn ${isCopied() ? 'copied' : ''} ${isFailed() ? 'failed' : ''}`}
              onClick={() => copy(HERO_INSTALL_COMMAND)}
              aria-label="Copy bootstrap command"
              data-testid="hero-install-copy-btn"
            >
              <span aria-hidden="true">{isCopied() ? '✓' : isFailed() ? '⚠️' : '📋'}</span>
              <span>{isCopied() ? 'Copied!' : isFailed() ? 'Copy failed' : 'Copy'}</span>
            </button>
            <a href="#quickstart" className="install-guide-link" data-testid="hero-install-jump">
              3-Step Guide ↓
            </a>
          </div>
        </div>

        <div className="hero-actions">
          <a href="#/docs/overview" className="btn-primary" data-testid="hero-cta-handbook">
            Read the Handbook
          </a>
          <a
            href="https://github.com/azylman/aerial"
            className="btn-secondary"
            target="_blank"
            rel="noopener noreferrer"
            data-testid="hero-cta-github"
          >
            GitHub Repository ↗
          </a>
        </div>
      </div>

      {/* 3 Spec-Sheet Pillar Cards */}
      <div className="features-grid" data-testid="pillar-cards">
        {PILLAR_CARDS.map((pillar) => (
          <div key={pillar.id} className="feature-card" data-testid={`pillar-card-${pillar.id}`}>
            <div className="feature-card-header">
              <span className="feature-icon-box" aria-hidden="true">{PILLAR_ICONS[pillar.id]}</span>
              <h2 className="feature-title">{pillar.title}</h2>
            </div>
            <p className="feature-desc">{pillar.description}</p>
            <div className="pillar-spec-list">
              {pillar.specs.map((spec) => (
                <div key={spec.id} className="pillar-spec-row" data-testid={`pillar-spec-${spec.id}`}>
                  <div className="pillar-spec-top">
                    <span className="pillar-spec-capability"><span className="pillar-spec-check" aria-hidden="true">✓</span><span>{spec.capability}</span></span>
                    <span className="pillar-spec-tech">{spec.tech}</span>
                  </div>
                  <div className="pillar-spec-detail">{spec.detail}</div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>

      {/* Interactive Use-Case Gallery */}
      <section className="landing-section" id="use-cases" data-testid="section-use-cases">
        <UseCaseGallery />
      </section>

      {/* Interactive Architecture Map */}
      <section className="landing-section" id="architecture" data-testid="section-architecture">
        <ArchitectureMap />
      </section>

      {/* Interactive Factory Stepper */}
      <section className="landing-section" id="software-factory" data-testid="section-factory">
        <FactoryStepper />
      </section>

      {/* Interactive Glass-Box Showcase */}
      <section className="landing-section" id="observability" data-testid="section-showcase">
        <GlassBoxShowcase />
      </section>

      {/* Filterable Competitor Comparison Matrix */}
      <section className="landing-section" id="comparison" data-testid="section-comparison">
        <ComparisonMatrix />
      </section>

      {/* Single-Node k3s Quickstart */}
      <section className="landing-section" id="quickstart" data-testid="section-quickstart">
        <QuickstartSteps />
      </section>

      {/* Frequently Asked Questions */}
      <section className="landing-section" id="faq" data-testid="section-faq">
        <FaqSection />
      </section>

      {/* Bottom CTA Banner */}
      <div className="bottom-cta-banner" data-testid="bottom-cta">
        <div className="cta-banner-content">
          <h2 className="cta-title">Dive Deeper into the Architecture</h2>
          <p className="cta-desc">
            Explore the complete 7-chapter architectural specification and living runbooks covering Two-Repo GitOps,
            Restate durable execution, defense-in-depth security guardrails, and custom SKILL.md authoring.
          </p>
          <div className="cta-actions">
            <a href="#/docs/overview" className="btn-primary">
              Explore Ch. 1: Overview & Philosophy →
            </a>
            <a href="#/docs/application-platform" className="btn-secondary">
              Ch. 3: Application Platform →
            </a>
          </div>
        </div>
      </div>
    </section>
  );
}
