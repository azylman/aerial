import { ArchitectureMap } from './ArchitectureMap';
import { GlassBoxShowcase } from './GlassBoxShowcase';
import { FactoryStepper } from './FactoryStepper';
import { ComparisonMatrix } from './ComparisonMatrix';
import { QuickstartSteps } from './QuickstartSteps';

export { ArchitectureMap, GlassBoxShowcase, FactoryStepper, ComparisonMatrix, QuickstartSteps };

export function LandingView() {
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
            Vikunja DAG backlog, tiered multi-agent review panel, Vanadis multi-PR DAG builds,
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
            Stateless Eri ingress, Coven ACT-R memory decay & procedural skill synthesis, Haro persistent scheduler,
            Discord Gateway, real-time voice, and headful browser control.
          </p>
        </div>
      </div>

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
