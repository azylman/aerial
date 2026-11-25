import { useState } from 'react';

export interface FactoryStage {
  id: number;
  title: string;
  subtitle: string;
  icon: string;
  component: string;
  inputs: readonly string[];
  actions: readonly string[];
  outputs: readonly string[];
  guardrails: readonly string[];
}

export const FACTORY_STAGES: readonly FactoryStage[] = [
  {
    id: 1,
    title: 'Vikunja DAG Ready Queue',
    subtitle: 'Dependency-aware backlog discovery',
    icon: '📋',
    component: 'Vikunja + docket-mcp + PostgreSQL 19',
    inputs: [
      'Feature specifications & architectural epics',
      'Bug reports & mid-flight out-of-scope discoveries',
      'Dependency relationships (blocks / blocked_by)',
    ],
    actions: [
      'Calculates unblocked topological ready queue',
      'Executes hybrid RRF text & vector search (pgvector + FTS)',
      'Parks blocked work without token exhaustion across sessions',
    ],
    outputs: [
      'Next ready task metadata & acceptance criteria',
      'Unblocked execution target for agent dispatch',
    ],
    guardrails: [
      'Atomic DAG state persisted in PostgreSQL 19',
      'Zero loss of backlog lineage across daemon rotations',
    ],
  },
  {
    id: 2,
    title: 'Ephemeral Scratch & Multi-Agent Review',
    subtitle: 'Isolated worktrees & multi-agent audits',
    icon: '🧪',
    component: 'scripts/aerial-pr.sh init + Subagent Review Panel',
    inputs: [
      'Target repository (aerial, aerial-config, mirrormere)',
      'Ready task directive & acceptance criteria',
    ],
    actions: [
      'Clones isolated ephemeral worktree in /data/scratch/',
      'Authors code, tests, and declarative schema.sql migrations',
      'Dispatches parallel multi-agent review panel (architecture, security, test coverage)',
      'Runs local hermetic verification (./scripts/verify.sh --staged)',
    ],
    outputs: [
      'Passing test suite & AST-compliant diffs',
      'Structured PR description with Inverted Pyramid format',
    ],
    guardrails: [
      'Live repositories mounted strictly read-only (:ro)',
      'Child-process secret scrubbing before engine spawn',
      'Zero-bypass local pre-commit verification',
    ],
  },
  {
    id: 3,
    title: 'Vanadis + Restate + NATS Multi-PR Pipeline',
    subtitle: 'Durable cross-repo DAG orchestration',
    icon: '🏗️',
    component: 'vanadis + vanadis-restate + NATS JetStream',
    inputs: [
      'Multi-chunk build plan (e.g. upstream engine PR before downstream config PR)',
      'Ordered chunk DAG dependencies and repository targets',
    ],
    actions: [
      'Restate Build and BuildRouter virtual objects coordinate chunk sequence',
      'Publishes aerial.pr.> event facts to NATS JetStream durable stream',
      'Coordinates GitHub auto-squash upon CI check approval',
    ],
    outputs: [
      'Sequenced PR progression across multiple repositories',
      'Automated squash merges without manual operator handoffs',
    ],
    guardrails: [
      'RocksDB-backed durable virtual objects prevent duplicate chunk runs',
      'Replay-safe state machines survive pod rescheduling',
    ],
  },
  {
    id: 4,
    title: 'GitHub Actions CI & Automated Code Review',
    subtitle: 'Dynamic runners & RoboRev checks',
    icon: '⚙️',
    component: 'GitHub Actions + ARC In-Cluster Runners + RoboRev',
    inputs: [
      'Pull requests submitted to azylman/aerial or user forks',
      'GitHub webhook notifications (check_run, workflow_run, pull_request)',
    ],
    actions: [
      'Executes CI test suites on self-hosted ARC runners across same-repo and fork PRs',
      'Runs Bazel remote cache, Go test suites, and Atlas schema linting',
      'Executes RoboRev automated LLM review passes with change requests',
    ],
    outputs: [
      'Signed green commit statuses and check_run approvals',
      'Actionable review findings or automatic merge authorization',
    ],
    guardrails: [
      'Branch protection rulesets and required status checks',
      'Image publish and secret-dependent steps strictly restricted to same-repo PRs and main',
    ],
  },
  {
    id: 5,
    title: 'Self-Healing Remediation & Flux Push GitOps',
    subtitle: 'Zero-touch deploy or automatic branch repair',
    icon: '🔄',
    component: 'synapse + Flux CD + vanadis-restate',
    inputs: [
      'GitHub push events or failed CI/RoboRev check runs',
      'edge-gateway webhook forwarder',
    ],
    actions: [
      'Success path: vanadis-restate updates image tags and triggers instant Flux GitOps reconcile',
      'Failure path: On CI failure, vanadis-restate wakes eri-restate with failure logs',
      'Agent inspects failure in scratch, writes targeted fix, and re-verifies automatically',
    ],
    outputs: [
      'Zero-downtime cluster rollout via Flux kustomize-controller',
      'Self-healing branch fix commit pushed without human intervention',
    ],
    guardrails: [
      'Zero plaintext token invariant enforced by sanitizers',
      'Last Known Good Configuration (LKGC) fallback in memory',
    ],
  },
];

export function FactoryStepper() {
  const [activeStageId, setActiveStageId] = useState<number>(1);
  const [remediationMode, setRemediationMode] = useState<boolean>(false);

  const currentStage = FACTORY_STAGES.find((s) => s.id === activeStageId) || FACTORY_STAGES[0]!;

  const handleNext = () => {
    setActiveStageId((prev) => Math.min(prev + 1, FACTORY_STAGES.length));
  };

  const handlePrev = () => {
    setActiveStageId((prev) => Math.max(prev - 1, 1));
  };

  return (
    <div className="factory-stepper-container" data-testid="factory-stepper">
      <div className="factory-stepper-header">
        <div className="section-eyebrow">Spec-to-Production Loop</div>
        <h2 className="section-title">The Autonomous Software Factory</h2>
        <p className="section-subtitle">
          From unblocked issue graphs to durable multi-PR rollouts and push GitOps delivery. Follow the 5-stage
          pipeline or simulate our reactive self-healing remediation loop.
        </p>

        <div className="stepper-mode-controls">
          <button
            type="button"
            className={`btn-secondary mode-toggle-btn ${remediationMode ? 'active-remediation' : ''}`}
            onClick={() => setRemediationMode(!remediationMode)}
            data-testid="toggle-remediation"
            aria-pressed={remediationMode}
          >
            <span role="img" aria-label="healing">
              {remediationMode ? '⚡' : '🩹'}
            </span>
            <span>
              {remediationMode ? 'Hide Self-Healing Flow' : 'Simulate Failure Remediation Loop'}
            </span>
          </button>
        </div>
      </div>

      {/* Remediation Flow Callout Banner */}
      {remediationMode && (
        <div className="remediation-banner" data-testid="remediation-banner">
          <div className="banner-icon">🔄</div>
          <div className="banner-content">
            <h4 className="banner-title">Autonomous Self-Healing Loop Active</h4>
            <p className="banner-text">
              When <strong>Stage 4 (GitHub Actions CI or RoboRev)</strong> detects a test failure or code change request,
              the pipeline does <strong>not</strong> break or wait for human triage. <strong>synapse</strong> captures the{' '}
              <code>check_run.completed</code> webhook, <strong>vanadis-restate</strong> extracts failure logs, and wakes{' '}
              <strong>eri-restate</strong> with exact line coordinates. The agent automatically initializes a scratch fix in{' '}
              <strong>Stage 2</strong>, pushes a remediation commit, and restarts Stage 4 verification!
            </p>
            <div className="remediation-diagram-pills">
              <span className="diagram-pill pill-fail">Stage 4: CI / RoboRev Fails</span>
              <span className="diagram-arrow">➔</span>
              <span className="diagram-pill pill-wake">Synapse / Restate Wake</span>
              <span className="diagram-arrow">➔</span>
              <span className="diagram-pill pill-scratch">Stage 2: Scratch Fix</span>
              <span className="diagram-arrow">➔</span>
              <span className="diagram-pill pill-retry">Stage 4: Auto Re-Verify</span>
              <span className="diagram-arrow">➔</span>
              <span className="diagram-pill pill-success">Stage 5: Flux Deploy</span>
            </div>
          </div>
        </div>
      )}

      {/* Stepper Progress Bar */}
      <div className="stepper-progress-bar" role="tablist" aria-label="Software Factory Stages">
        {FACTORY_STAGES.map((stage) => {
          const isActive = stage.id === activeStageId;
          const isDone = stage.id < activeStageId;

          return (
            <button
              key={stage.id}
              type="button"
              role="tab"
              aria-selected={isActive}
              className={`stage-step-btn ${isActive ? 'active' : ''} ${isDone ? 'completed' : ''}`}
              onClick={() => setActiveStageId(stage.id)}
              data-testid={`stage-btn-${stage.id}`}
            >
              <div className="step-circle" aria-hidden="true">
                {isDone ? '✓' : stage.id}
              </div>
              <div className="step-info">
                <span className="step-number">STAGE {stage.id}</span>
                <span className="step-label">{stage.title}</span>
              </div>
            </button>
          );
        })}
      </div>

      {/* Stage Inspection Details Card */}
      <div className="stage-detail-card" data-testid="stage-detail-content">
        <div className="stage-detail-header">
          <div className="stage-header-title-group">
            <span className="stage-big-icon" aria-hidden="true">
              {currentStage.icon}
            </span>
            <div>
              <span className="stage-component-tag">{currentStage.component}</span>
              <h3 className="stage-card-title">
                Stage {currentStage.id}: {currentStage.title}
              </h3>
              <p className="stage-card-subtitle">{currentStage.subtitle}</p>
            </div>
          </div>

          <div className="stage-nav-actions">
            <button
              type="button"
              className="btn-secondary nav-btn"
              onClick={handlePrev}
              disabled={activeStageId === 1}
              aria-label="Previous Stage"
              data-testid="prev-stage-btn"
            >
              ← Previous
            </button>
            <button
              type="button"
              className="btn-primary nav-btn"
              onClick={handleNext}
              disabled={activeStageId === FACTORY_STAGES.length}
              aria-label="Next Stage"
              data-testid="next-stage-btn"
            >
              Next →
            </button>
          </div>
        </div>

        <div className="stage-grid-sections">
          <div className="stage-grid-col">
            <h4 className="col-heading">Inputs & Triggers</h4>
            <ul className="stage-item-list">
              {currentStage.inputs.map((item, i) => (
                <li key={i}>{item}</li>
              ))}
            </ul>
          </div>

          <div className="stage-grid-col">
            <h4 className="col-heading">Core Pipeline Actions</h4>
            <ul className="stage-item-list">
              {currentStage.actions.map((item, i) => (
                <li key={i}>{item}</li>
              ))}
            </ul>
          </div>

          <div className="stage-grid-col">
            <h4 className="col-heading">Outputs & Artifacts</h4>
            <ul className="stage-item-list">
              {currentStage.outputs.map((item, i) => (
                <li key={i}>{item}</li>
              ))}
            </ul>
          </div>

          <div className="stage-grid-col highlight-col">
            <h4 className="col-heading">Guardrails & Safety</h4>
            <ul className="stage-item-list">
              {currentStage.guardrails.map((item, i) => (
                <li key={i}>{item}</li>
              ))}
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}
