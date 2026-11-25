import { useEffect, useRef, useState } from 'react';
import { ArchitectureMap } from './ArchitectureMap';
import { GlassBoxShowcase } from './GlassBoxShowcase';
import { FactoryStepper } from './FactoryStepper';
import { ComparisonMatrix } from './ComparisonMatrix';
import { QuickstartSteps } from './QuickstartSteps';
import { UseCaseGallery, USE_CASE_SCENARIOS } from './UseCaseGallery';

export {
  ArchitectureMap,
  GlassBoxShowcase,
  FactoryStepper,
  ComparisonMatrix,
  QuickstartSteps,
  UseCaseGallery,
  USE_CASE_SCENARIOS,
};

export const HERO_INSTALL_COMMAND =
  'curl -sfL https://get.k3s.io | sh -s - --disable traefik && flux bootstrap github --path=k8s';

export function LandingView() {
  const [copied, setCopied] = useState<boolean>(false);
  const [copyFailed, setCopyFailed] = useState<boolean>(false);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, []);

  const handleCopyInstall = async () => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
      timeoutRef.current = null;
    }
    try {
      if (!navigator.clipboard?.writeText) {
        throw new Error('Clipboard API not available');
      }
      await navigator.clipboard.writeText(HERO_INSTALL_COMMAND);
      setCopyFailed(false);
      setCopied(true);
      timeoutRef.current = setTimeout(() => {
        setCopied(false);
        timeoutRef.current = null;
      }, 2500);
    } catch {
      setCopied(false);
      setCopyFailed(true);
      timeoutRef.current = setTimeout(() => {
        setCopyFailed(false);
        timeoutRef.current = null;
      }, 2500);
    }
  };

  return (
    <section className="landing-view" data-testid="landing-view">
      {/* Hero Section */}
      <div className="hero-section" data-testid="landing-hero">
        <div className="hero-tag">
          <span>XVX-016 Gundam Aerial</span>
          <span>•</span>
          <span>Production Architecture</span>
        </div>
        <h1 className="hero-title">Autonomous AI Personal Assistant & Software Factory</h1>
        <p className="hero-subtitle">
          A closed-loop, self-hosted platform pairing continuous software delivery with multimodal intelligence.
          From Discord and voice interactions to multi-PR DAG builds, automated GitHub review loops, and push GitOps.
        </p>

        {/* Quick-Install Terminal Pill */}
        <div className="hero-install-pill" data-testid="hero-install-pill">
          <span className="install-pill-prompt" aria-hidden="true">
            $
          </span>
          <code className="install-pill-code">{HERO_INSTALL_COMMAND}</code>
          <div className="install-pill-actions">
            <button
              type="button"
              className={`install-copy-btn ${copied ? 'copied' : ''} ${copyFailed ? 'failed' : ''}`}
              onClick={handleCopyInstall}
              aria-label="Copy bootstrap command"
              data-testid="hero-install-copy-btn"
            >
              <span aria-hidden="true">{copied ? '✓' : copyFailed ? '⚠️' : '📋'}</span>
              <span>{copied ? 'Copied!' : copyFailed ? 'Copy failed' : 'Copy'}</span>
            </button>
            <a href="#quickstart" className="install-guide-link" data-testid="hero-install-jump">
              4-Step Guide ↓
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

      {/* 3 Pillar Feature Cards */}
      <div className="features-grid" data-testid="pillar-cards">
        <div className="feature-card">
          <div className="feature-card-header">
            <span className="feature-icon" role="img" aria-label="platform">
              🪐
            </span>
            <h2 className="feature-title">Self-Hosted Platform</h2>
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
            <h2 className="feature-title">Multimodal AI Agent</h2>
          </div>
          <p className="feature-desc">
            Stateless HTTP ingress, ACT-R cognitive memory with procedural skill synthesis, persistent cron and event-watch scheduling,
            Discord Gateway, real-time voice, and headful browser control.
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
