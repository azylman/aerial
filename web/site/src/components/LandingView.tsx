import { ArchitectureMap } from './ArchitectureMap';
import { GlassBoxShowcase } from './GlassBoxShowcase';
import { FactoryStepper } from './FactoryStepper';
import { ComparisonMatrix } from './ComparisonMatrix';
import { QUICKSTART_STEPS, QuickstartSteps } from './QuickstartSteps';
import { UseCaseGallery, USE_CASE_SCENARIOS } from './UseCaseGallery';
import { FaqSection, FAQ_ITEMS, type FaqItem } from './FaqSection';
import { useCopyToClipboard } from '../useCopyToClipboard';

export interface IntegrationBadge {
  id: string;
  group: string;
  label: string;
}

export const INTEGRATION_BADGES: readonly IntegrationBadge[] = [
  // Surfaces
  { id: 'discord-gateway', group: 'Surfaces', label: 'Discord Gateway' },
  { id: 'webrtc-kiosk-voice', group: 'Surfaces', label: 'WebRTC & Touch Kiosk Voice' },
  { id: 'pstn-voice-calls', group: 'Surfaces', label: 'PSTN Phone Calls' },
  { id: 'console-hud', group: 'Surfaces', label: 'Headful Browser (CDP) & Web HUD' },

  // AI Engines
  { id: 'claude-code', group: 'AI Engines', label: 'Claude Code' },
  { id: 'codex-cli', group: 'AI Engines', label: 'Codex CLI' },
  { id: 'google-antigravity', group: 'AI Engines', label: 'Google Antigravity' },
  { id: 'local-onnx-speech', group: 'AI Engines', label: 'Local CUDA ONNX Speech' },

  // Core Stack
  { id: 'k3s-kubernetes', group: 'Core Stack', label: 'Kubernetes (k3s)' },
  { id: 'restate-durable', group: 'Core Stack', label: 'Restate Durable Execution' },
  { id: 'cnpg-postgres', group: 'Core Stack', label: 'CloudNativePG Postgres 19' },
  { id: 'flux-infisical', group: 'Core Stack', label: 'Flux CD & Infisical' },
] as const;

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

export const HERO_INSTALL_COMMAND = QUICKSTART_STEPS[0]!.command;

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
          <span className="install-pill-badge">Step 1</span>
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
          <a href="#/docs/getting-started" className="btn-secondary" data-testid="hero-cta-quickstart">
            Getting Started Guide
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

      {/* Supported Surfaces, Engines & Integrations Strip */}
      <div className="integration-strip" data-testid="integration-strip">
        <div className="integration-strip-label">Supported Surfaces, Engines & Integrations</div>
        <div className="integration-badges-grid">
          {INTEGRATION_BADGES.map((badge) => (
            <span key={badge.id} className="integration-badge" data-testid={`integration-badge-${badge.id}`}>
              <span className="badge-group">{badge.group}</span>
              <span className="badge-separator">•</span>
              <span className="badge-label">{badge.label}</span>
            </span>
          ))}
        </div>
      </div>

      {/* 3 Pillar Feature Cards */}
      <div className="features-grid" data-testid="pillar-cards">
        <div className="feature-card">
          <div className="feature-card-header">
            <span className="feature-icon" role="img" aria-label="platform">
              🪐
            </span>
            <h2 className="feature-title">Homelab Application Platform</h2>
          </div>
          <p className="feature-desc">
            Kubernetes (k3s) fabric with NGINX Gateway Fabric, CloudNativePG PostgreSQL 19 + pgvector, Restate durable
            workflow virtual objects, NATS JetStream, and VictoriaMetrics telemetry.
          </p>
        </div>

        <div className="feature-card">
          <div className="feature-card-header">
            <span className="feature-icon" role="img" aria-label="factory">
              ⚡
            </span>
            <h2 className="feature-title">Autonomous Software Factory</h2>
          </div>
          <p className="feature-desc">
            Vikunja DAG backlog, tiered multi-agent review panel, Restate multi-PR DAG builds,
            scale-to-zero in-cluster ARC runners, and proactive CI failure remediation wakes.
          </p>
        </div>

        <div className="feature-card">
          <div className="feature-card-header">
            <span className="feature-icon" role="img" aria-label="agent">
              🛰️
            </span>
            <h2 className="feature-title">AI Agent</h2>
          </div>
          <p className="feature-desc">
            Talk over Discord, room voice, or phone calls to automate browser errands, manage calendars, and run persistent
            schedules with long-term semantic memory and SKILL.md skills.
          </p>
        </div>
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
