import { useState } from 'react';

export interface UseCaseScenario {
  id: string;
  category: string;
  title: string;
  prompt: string;
  engineSteps: readonly string[];
  outcome: string;
  stackBadges: readonly string[];
}

export const USE_CASE_SCENARIOS: readonly UseCaseScenario[] = [
  {
    id: 'personal-agent-errands',
    category: 'Everyday AI Agent',
    title: 'Web Browser, Phone Call & Persistent Schedule Errands',
    prompt:
      'Check the city permit portal every morning, call the office if our review status changes, and schedule a Friday summary.',
    engineSteps: [
      'Navigate the municipal portal in a headful browser session and extract current application status.',
      'Register a persistent daily heartbeat schedule in PostgreSQL and place an outbound PSTN phone call when status changes.',
      'Store extracted reference details in hybrid semantic memory and post the weekly summary to Discord.',
    ],
    outcome:
      'Everyday web errands, phone calls, and recurring schedules execute durably across restarts with full semantic memory.',
    stackBadges: [
      'Headful Browser (CDP)',
      'PSTN Telephony',
      'Persistent Scheduler',
      'Hybrid RRF Memory',
    ],
  },
  {
    id: 'incident-triage',
    category: 'Observability & Triage',
    title: 'Automated Telemetry & Log Root-Cause Triage',
    prompt:
      'Why did voice interaction latency spike during the database backup window at midnight?',
    engineSteps: [
      'Execute LogsQL queries in VictoriaLogs to extract error spikes and query VictoriaMetrics for connection pool exhaustion.',
      'Trace distributed OpenTelemetry spans in VictoriaTraces to correlate database lock duration with request queue depth.',
      'Publish incident diagnosis with timeline chart and recommended connection pool patch to development channel.',
    ],
    outcome:
      'Engineers receive root-cause diagnosis and reproduction steps within seconds without manual metric correlation.',
    stackBadges: [
      'VictoriaMetrics',
      'VictoriaLogs',
      'VictoriaTraces',
      'OpenTelemetry Collector',
    ],
  },
  {
    id: 'multi-pr-migration',
    category: 'Software Factory',
    title: 'Multi-PR DAG Delivery & Automated CI Verification',
    prompt:
      'Add a vector embeddings column to PostgreSQL and update API gateway routes across both repositories.',
    engineSteps: [
      'Author declarative schema updates in isolated scratch workspaces and verify with multi-agent peer review.',
      'Sequence multi-PR build workflows in Restate and remediate failing GitHub Actions CI checks automatically.',
      'Deploy verified pull requests with push-driven Flux CD rollout across engine and configuration repositories.',
    ],
    outcome:
      'Multi-repository changes deploy sequentially with zero downtime and verified database schema compatibility.',
    stackBadges: [
      'Restate',
      'CloudNativePG Postgres 19',
      'Flux CD',
      'NGINX Gateway Fabric',
    ],
  },
];

const DEFAULT_SCENARIO: UseCaseScenario = USE_CASE_SCENARIOS[0]!;

export function UseCaseGallery() {
  const [activeScenarioId, setActiveScenarioId] = useState<string>(DEFAULT_SCENARIO.id);

  const activeScenario =
    USE_CASE_SCENARIOS.find((s) => s.id === activeScenarioId) ?? DEFAULT_SCENARIO;

  return (
    <div className="usecase-gallery" data-testid="use-case-gallery">
      <div className="usecase-header">
        <div className="section-eyebrow">Day-in-the-Life Workflows</div>
        <h2 className="section-title">What Aerial Does in Practice</h2>
        <p className="section-subtitle">
          Explore how Aerial handles everyday browser, phone, and scheduling errands alongside autonomous code delivery, cross-repository releases, and incident triage.
        </p>

        {/* Scenario selector tabs */}
        <div className="usecase-tabs" role="tablist" aria-label="Use cases navigation">
          {USE_CASE_SCENARIOS.map((scenario) => {
            const isSelected = activeScenario.id === scenario.id;
            return (
              <button
                key={scenario.id}
                type="button"
                role="tab"
                aria-selected={isSelected}
                aria-controls="usecase-active-panel"
                className={`usecase-tab-btn ${isSelected ? 'active' : ''}`}
                onClick={() => setActiveScenarioId(scenario.id)}
                data-testid={`usecase-tab-${scenario.id}`}
              >
                <span className="usecase-tab-cat">{scenario.category}</span>
                <span className="usecase-tab-title">{scenario.title}</span>
              </button>
            );
          })}
        </div>
      </div>

      {/* Interactive split panel */}
      <div
        className="usecase-panel"
        id="usecase-active-panel"
        role="tabpanel"
        data-testid="usecase-active-panel"
      >
        {/* Left/Top Prompt Card */}
        <div className="usecase-prompt-card">
          <div className="usecase-card-header">
            <span className="badge badge-accent">{activeScenario.category}</span>
            <h3 className="usecase-title">{activeScenario.title}</h3>
          </div>

          <div className="usecase-user-prompt">
            <div className="prompt-label">User Request (Discord / Voice)</div>
            <blockquote className="prompt-bubble" data-testid="usecase-prompt">
              "{activeScenario.prompt}"
            </blockquote>
          </div>

          <div className="usecase-stack-container">
            <span className="stack-label">Stack Components:</span>
            <div className="stack-badges">
              {activeScenario.stackBadges.map((badge, idx) => (
                <span key={idx} className="badge badge-subtle">
                  {badge}
                </span>
              ))}
            </div>
          </div>
        </div>

        {/* Right/Bottom Autonomous Execution Trace & Outcome */}
        <div className="usecase-trace-card">
          <div className="trace-header">
            <span className="trace-label">Autonomous Execution Trace</span>
            <span className="trace-status-pill">3 Steps Autonomous</span>
          </div>

          <ol className="trace-steps-list">
            {activeScenario.engineSteps.map((step, idx) => (
              <li key={idx} className="trace-step-item" data-testid={`usecase-step-${idx}`}>
                <span className="trace-step-num">{idx + 1}</span>
                <span className="trace-step-text">{step}</span>
              </li>
            ))}
          </ol>

          <div className="usecase-outcome-banner" data-testid="usecase-outcome">
            <span className="outcome-icon" aria-hidden="true">
              ✓
            </span>
            <div className="outcome-content">
              <span className="outcome-label">Verified Outcome:</span>
              <span className="outcome-text">{activeScenario.outcome}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
