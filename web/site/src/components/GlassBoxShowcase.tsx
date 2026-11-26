import { useState } from 'react';

export type ShowcaseTab = 'console' | 'agentsview' | 'restate' | 'vikunja' | 'observability';

export interface ShowcaseTabInfo {
  id: ShowcaseTab;
  title: string;
  badge: string;
  icon: string;
  route: string;
  description: string;
}

export const SHOWCASE_TABS: readonly ShowcaseTabInfo[] = [
  {
    id: 'console',
    title: 'Operator Console',
    badge: 'Live HUD',
    icon: '🎛️',
    route: 'http://<node-ip>/console/',
    description: 'Real-time mission control HUD displays active agent turns, multi-PR builds, schedules, and memory facts.',
  },
  {
    id: 'agentsview',
    title: 'Session & Trace Inspector',
    badge: 'AgentsView',
    icon: '🔍',
    route: 'http://<node-ip>/agentsview/',
    description: 'Inspect full multi-turn conversation transcripts, subagent execution trees, and tool call span timelines.',
  },
  {
    id: 'restate',
    title: 'Restate Durable Execution',
    badge: 'Restate UI',
    icon: '⚡',
    route: 'http://<node-ip>/restate/',
    description: 'Monitor RocksDB virtual object state machines, invocation replay logs, and durable workflow journals.',
  },
  {
    id: 'vikunja',
    title: 'DAG Backlog Board',
    badge: 'Vikunja',
    icon: '📋',
    route: 'Service/vikunja',
    description: 'Manage Kanban, Gantt, and dependency task graphs with hybrid PostgreSQL 19 text and vector search.',
  },
  {
    id: 'observability',
    title: 'Unified Observability',
    badge: 'Grafana & VMUI',
    icon: '📊',
    route: 'http://<node-ip>/grafana/',
    description: 'Query pre-provisioned Grafana dashboards, VictoriaLogs LogsQL analytics, and VictoriaTraces OTLP waterfalls.',
  },
];

export function GlassBoxShowcase() {
  const [activeTab, setActiveTab] = useState<ShowcaseTab>('console');

  // Console sub-state
  const [consoleView, setConsoleView] = useState<'turn' | 'builds'>('turn');

  // AgentsView sub-state
  const [activeStepIndex, setActiveStepIndex] = useState<number>(2);

  // Restate sub-state
  const [restateViewMode, setRestateViewMode] = useState<'journal' | 'state'>('journal');

  // Vikunja sub-state
  const [selectedTaskId, setSelectedTaskId] = useState<string>('task-103');

  // Observability sub-state
  const [telemetryMode, setTelemetryMode] = useState<'metrics' | 'logs' | 'traces'>('metrics');

  const currentTabInfo = SHOWCASE_TABS.find((t) => t.id === activeTab) || SHOWCASE_TABS[0]!;

  return (
    <div className="glassbox-container" data-testid="glassbox-showcase">
      <div className="glassbox-header">
        <div className="section-eyebrow">Glass-Box Operations</div>
        <h2 className="section-title">Built-In Web Observability</h2>
        <p className="section-subtitle">
          Aerial provides dedicated web interfaces for every subsystem across your local network.
        </p>

        {/* Tab Selection Bar */}
        <div className="showcase-nav-tabs" role="tablist" aria-label="Observability interfaces">
          {SHOWCASE_TABS.map((tab) => {
            const isActive = tab.id === activeTab;
            return (
              <button
                key={tab.id}
                type="button"
                role="tab"
                aria-selected={isActive}
                className={`showcase-nav-btn ${isActive ? 'active' : ''}`}
                onClick={() => setActiveTab(tab.id)}
                data-testid={`tab-${tab.id}`}
              >
                <span className="tab-icon" aria-hidden="true">
                  {tab.icon}
                </span>
                <span className="tab-title">{tab.title}</span>
                <span className="tab-badge">{tab.badge}</span>
              </button>
            );
          })}
        </div>
      </div>

      {/* Screen Frame Mockup */}
      <div className="mockup-frame">
        <div className="mockup-chrome">
          <div className="chrome-controls" aria-hidden="true">
            <span className="control-dot dot-close" />
            <span className="control-dot dot-minimize" />
            <span className="control-dot dot-maximize" />
          </div>
          <div className="chrome-address-bar">
            <span className="address-lock" aria-hidden="true">
              🔒
            </span>
            <span className="address-url">{currentTabInfo.route}</span>
          </div>
          <div className="chrome-service-title">{currentTabInfo.title}</div>
        </div>

        <div className="mockup-body" data-testid="showcase-body">
          {/* TAB 1: OPERATOR CONSOLE */}
          {activeTab === 'console' && (
            <div className="mockup-view console-view" data-testid="view-console">
              <div className="console-hud-top">
                <div className="hud-metric-card">
                  <span className="hud-metric-label">CLUSTER STATUS</span>
                  <span className="hud-metric-val val-ok">HEALTHY (3 Nodes)</span>
                </div>
                <div className="hud-metric-card">
                  <span className="hud-metric-label">ACTIVE ENGINE</span>
                  <span className="hud-metric-val">agy // Gemini 3.8 Flash</span>
                </div>
                <div className="hud-metric-card">
                  <span className="hud-metric-label">RESTATE CLUSTER</span>
                  <span className="hud-metric-val val-ok">ACTIVE (RocksDB OK)</span>
                </div>
                <div className="hud-metric-card">
                  <span className="hud-metric-label">POSTGRES 19</span>
                  <span className="hud-metric-val val-ok">cnpg-cluster (pgvector)</span>
                </div>
              </div>

              <div className="console-subnav">
                <button
                  type="button"
                  className={`subnav-btn ${consoleView === 'turn' ? 'active' : ''}`}
                  onClick={() => setConsoleView('turn')}
                  data-testid="console-btn-turn"
                >
                  Active Turn HUD
                </button>
                <button
                  type="button"
                  className={`subnav-btn ${consoleView === 'builds' ? 'active' : ''}`}
                  onClick={() => setConsoleView('builds')}
                  data-testid="console-btn-builds"
                >
                  Multi-PR Build Queue (1 Active)
                </button>
              </div>

              {consoleView === 'turn' ? (
                <div className="console-turn-panel" data-testid="console-turn-content">
                  <div className="turn-card">
                    <div className="turn-card-header">
                      <span className="badge badge-accent">TURN #42</span>
                      <span className="turn-channel">#aerial-dev // thread: 1466-public-launch</span>
                      <span className="turn-duration">1.24s elapsed</span>
                    </div>
                    <div className="turn-prompt-preview">
                      <span className="prompt-label">USER DIRECTIVE:</span>
                      <code>[BUILD CHUNK EXECUTION] Chunk 6: Build Interactive Landing Page Showcase</code>
                    </div>
                    <div className="turn-stats-grid">
                      <div className="turn-stat-item">
                        <span className="stat-label">Model Context</span>
                        <span className="stat-value">12.4k tokens</span>
                      </div>
                      <div className="turn-stat-item">
                        <span className="stat-label">Cognitive Memory</span>
                        <span className="stat-value">4 facts retrieved (ACT-R 0.5)</span>
                      </div>
                      <div className="turn-stat-item">
                        <span className="stat-label">Staged Diff</span>
                        <span className="stat-value">+5 files (verified)</span>
                      </div>
                      <div className="turn-stat-item">
                        <span className="stat-label">Guardrail State</span>
                        <span className="stat-value val-ok">Enforced (:ro mounts active)</span>
                      </div>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="console-builds-panel" data-testid="console-builds-content">
                  <div className="build-card">
                    <div className="build-card-header">
                      <span className="badge badge-accent">BUILD build-1795589299-bb34c38b</span>
                      <span className="build-title">Public Launch Docs and React Site</span>
                      <span className="badge badge-success">5 / 8 CHUNKS COMPLETE</span>
                    </div>
                    <div className="chunks-pipeline-bar">
                      <span className="chunk-pill chunk-done">Ch.1: Schema Migration</span>
                      <span className="chunk-pill chunk-done">Ch.2: Verify Prune</span>
                      <span className="chunk-pill chunk-done">Ch.3: Gateway Routes</span>
                      <span className="chunk-pill chunk-done">Ch.4: Refresh README</span>
                      <span className="chunk-pill chunk-done">Ch.5: Scaffold Site</span>
                      <span className="chunk-pill chunk-active">Ch.6: Landing Views</span>
                      <span className="chunk-pill chunk-queued">Ch.7: Handbook</span>
                      <span className="chunk-pill chunk-queued">Ch.8: Verification</span>
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* TAB 2: AGENTSVIEW SESSION & TRACE INSPECTOR */}
          {activeTab === 'agentsview' && (
            <div className="mockup-view agentsview-view" data-testid="view-agentsview">
              <div className="agentsview-layout">
                <div className="steps-timeline" aria-label="Turn execution steps">
                  <div className="timeline-title">TURN EXECUTION TIMELINE</div>
                  {[
                    { step: 1, name: 'User Directive Received', type: 'event', duration: '12ms' },
                    { step: 2, name: 'Tool: issue_ready', type: 'tool', duration: '18ms' },
                    { step: 3, name: 'Tool: aerial_pr_init', type: 'tool', duration: '210ms' },
                    { step: 4, name: 'Subagent: Multi-Agent Review', type: 'subagent', duration: '1,420ms' },
                    { step: 5, name: 'Tool: verify_and_submit', type: 'tool', duration: '450ms' },
                  ].map((s, idx) => (
                    <button
                      key={s.step}
                      type="button"
                      className={`timeline-step-btn ${activeStepIndex === idx ? 'active' : ''}`}
                      onClick={() => setActiveStepIndex(idx)}
                      data-testid={`step-btn-${idx}`}
                    >
                      <span className="step-num">{s.step}</span>
                      <div className="step-meta">
                        <span className="step-name">{s.name}</span>
                        <span className="step-dur">{s.duration}</span>
                      </div>
                    </button>
                  ))}
                </div>

                <div className="step-details-pane" data-testid="step-details">
                  <div className="step-header">
                    <span className="badge badge-accent">STEP #{activeStepIndex + 1} INSPECTOR</span>
                    <span className="step-status val-ok">STATUS: 200 SUCCESS</span>
                  </div>
                  {activeStepIndex === 0 && (
                    <div className="step-body">
                      <h4>Event: User Directive Ingress</h4>
                      <p>Received directive from channel #aerial-dev via webhook gateway.</p>
                      <pre>
                        <code>{`{\n  "channel": "aerial-dev",\n  "directive": "[BUILD CHUNK EXECUTION] ...",\n  "author": "operator (admin: true)"\n}`}</code>
                      </pre>
                    </div>
                  )}
                  {activeStepIndex === 1 && (
                    <div className="step-body">
                      <h4>Tool Execution: issue_ready</h4>
                      <p>Queried Vikunja task graph via issue-tracker-mcp for unblocked backlog items.</p>
                      <pre>
                        <code>{`{\n  "query": "unblocked_tasks",\n  "result_count": 1,\n  "next_task": "chunk-6-site-interactive-landing-views"\n}`}</code>
                      </pre>
                    </div>
                  )}
                  {activeStepIndex === 2 && (
                    <div className="step-body">
                      <h4>Tool Execution: aerial_pr_init</h4>
                      <p>Initialized isolated ephemeral scratch clone in /data/scratch/ for safe branch authoring.</p>
                      <pre>
                        <code>{`{\n  "repo": "aerial",\n  "scratch_dir": "/data/scratch/chunk-6-site-interactive-landing-views-aerial",\n  "branch": "feat/chunk-6..."\n}`}</code>
                      </pre>
                    </div>
                  )}
                  {activeStepIndex === 3 && (
                    <div className="step-body">
                      <h4>Subagent: Multi-Agent Review Panel</h4>
                      <p>Dispatched parallel subagents to audit architecture contracts, test isolation, and security guardrails.</p>
                      <pre>
                        <code>{`{\n  "panel": ["architecture", "security", "testing"],\n  "verdict": "APPROVED",\n  "findings": []\n}`}</code>
                      </pre>
                    </div>
                  )}
                  {activeStepIndex === 4 && (
                    <div className="step-body">
                      <h4>Tool Execution: verify_and_submit</h4>
                      <p>Ran ./scripts/verify.sh --staged and submitted scratch PR to multi-PR build queue.</p>
                      <pre>
                        <code>{`{\n  "verify_status": "PASS",\n  "pr_number": 1466,\n  "turn_action": "end_turn"\n}`}</code>
                      </pre>
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* TAB 3: RESTATE DURABLE EXECUTION UI */}
          {activeTab === 'restate' && (
            <div className="mockup-view restate-view" data-testid="view-restate">
              <div className="restate-toolbar">
                <span className="restate-object-pill">
                  Virtual Object: <strong>Build / build-1795589299-bb34c38b</strong>
                </span>
                <div className="restate-view-switch">
                  <button
                    type="button"
                    className={`subnav-btn ${restateViewMode === 'journal' ? 'active' : ''}`}
                    onClick={() => setRestateViewMode('journal')}
                    data-testid="restate-btn-journal"
                  >
                    Execution Journal
                  </button>
                  <button
                    type="button"
                    className={`subnav-btn ${restateViewMode === 'state' ? 'active' : ''}`}
                    onClick={() => setRestateViewMode('state')}
                    data-testid="restate-btn-state"
                  >
                    RocksDB State Snapshot
                  </button>
                </div>
              </div>

              {restateViewMode === 'journal' ? (
                <div className="restate-journal" data-testid="restate-journal-content">
                  <div className="journal-entry">
                    <span className="journal-time">+0.012s</span>
                    <span className="journal-fn">startBuild()</span>
                    <span className="journal-status val-ok">COMPLETED</span>
                    <span className="journal-note">Initialized state in RocksDB persistent storage</span>
                  </div>
                  <div className="journal-entry">
                    <span className="journal-time">+0.045s</span>
                    <span className="journal-fn">appendChunk(chunk-1)</span>
                    <span className="journal-status val-ok">COMPLETED</span>
                    <span className="journal-note">Enqueued chunk-1-schema-migration (repo: aerial)</span>
                  </div>
                  <div className="journal-entry">
                    <span className="journal-time">+0.089s</span>
                    <span className="journal-fn">dispatchChunk(chunk-1)</span>
                    <span className="journal-status val-ok">COMPLETED</span>
                    <span className="journal-note">Dispatched wake to agent-worker execution engine</span>
                  </div>
                  <div className="journal-entry">
                    <span className="journal-time">+4.120s</span>
                    <span className="journal-fn">onChunkCompleted(chunk-1)</span>
                    <span className="journal-status val-ok">COMPLETED</span>
                    <span className="journal-note">GitHub PR #1462 auto-squashed; NATS aerial.pr.completed published</span>
                  </div>
                  <div className="journal-entry active">
                    <span className="journal-time">+8.540s</span>
                    <span className="journal-fn">dispatchChunk(chunk-6)</span>
                    <span className="journal-status val-warn">RUNNING</span>
                    <span className="journal-note">chunk-6-site-interactive-landing-views executing</span>
                  </div>
                </div>
              ) : (
                <div className="restate-json-view" data-testid="restate-state-content">
                  <pre>
                    <code>{`{\n  "build_id": "build-1795589299-bb34c38b",\n  "title": "Public Launch Docs and React Site",\n  "status": "RUNNING",\n  "active_chunk_id": "chunk-6-site-interactive-landing-views",\n  "chunks": [\n    {"id": "chunk-1", "status": "COMPLETED", "pr": 1462},\n    {"id": "chunk-2", "status": "COMPLETED", "pr": 1463},\n    {"id": "chunk-3", "status": "COMPLETED", "pr": 1464},\n    {"id": "chunk-4", "status": "COMPLETED", "pr": 1465},\n    {"id": "chunk-5", "status": "COMPLETED", "pr": 1466},\n    {"id": "chunk-6", "status": "RUNNING", "pr": null}\n  ],\n  "auto_squash": true\n}`}</code>
                  </pre>
                </div>
              )}
            </div>
          )}

          {/* TAB 4: DAG BACKLOG BOARD (VIKUNJA) */}
          {activeTab === 'vikunja' && (
            <div className="mockup-view vikunja-view" data-testid="view-vikunja">
              <div className="kanban-board">
                <div className="kanban-col">
                  <div className="kanban-col-header">
                    <span>READY (UNBLOCKED)</span>
                    <span className="col-count">1</span>
                  </div>
                  <button
                    type="button"
                    className={`task-card ${selectedTaskId === 'task-103' ? 'selected' : ''}`}
                    onClick={() => setSelectedTaskId('task-103')}
                    data-testid="card-task-103"
                  >
                    <div className="task-header">
                      <span className="task-id">#103</span>
                      <span className="task-priority">HIGH</span>
                    </div>
                    <div className="task-name">Build Interactive Landing Page Showcase</div>
                    <div className="task-tags">
                      <span className="tag">frontend</span>
                      <span className="tag">tier-3</span>
                    </div>
                    <div className="task-rrf">Hybrid RRF Match: 0.96</div>
                  </button>
                </div>

                <div className="kanban-col">
                  <div className="kanban-col-header">
                    <span>IN PROGRESS</span>
                    <span className="col-count">1</span>
                  </div>
                  <button
                    type="button"
                    className={`task-card ${selectedTaskId === 'task-102' ? 'selected' : ''}`}
                    onClick={() => setSelectedTaskId('task-102')}
                    data-testid="card-task-102"
                  >
                    <div className="task-header">
                      <span className="task-id">#102</span>
                      <span className="task-priority">MED</span>
                    </div>
                    <div className="task-name">Declarative Atlas Schema Validation in CI</div>
                    <div className="task-tags">
                      <span className="tag">ci</span>
                      <span className="tag">actions</span>
                    </div>
                    <div className="task-rrf">Depends on: #101 (Done)</div>
                  </button>
                </div>

                <div className="kanban-col">
                  <div className="kanban-col-header">
                    <span>SHIPPED / DONE</span>
                    <span className="col-count">1</span>
                  </div>
                  <button
                    type="button"
                    className={`task-card ${selectedTaskId === 'task-101' ? 'selected' : ''}`}
                    onClick={() => setSelectedTaskId('task-101')}
                    data-testid="card-task-101"
                  >
                    <div className="task-header">
                      <span className="task-id">#101</span>
                      <span className="task-priority val-ok">DONE</span>
                    </div>
                    <div className="task-name">Configure Flux Kustomization Prune Protection</div>
                    <div className="task-tags">
                      <span className="tag">k8s</span>
                      <span className="tag">flux</span>
                    </div>
                    <div className="task-rrf">Prune protection enabled</div>
                  </button>
                </div>
              </div>
            </div>
          )}

          {/* TAB 5: UNIFIED OBSERVABILITY (GRAFANA & VMUI) */}
          {activeTab === 'observability' && (
            <div className="mockup-view observability-view" data-testid="view-observability">
              <div className="obs-toolbar">
                <div className="obs-mode-buttons">
                  <button
                    type="button"
                    className={`subnav-btn ${telemetryMode === 'metrics' ? 'active' : ''}`}
                    onClick={() => setTelemetryMode('metrics')}
                    data-testid="obs-btn-metrics"
                  >
                    VictoriaMetrics TSDB
                  </button>
                  <button
                    type="button"
                    className={`subnav-btn ${telemetryMode === 'logs' ? 'active' : ''}`}
                    onClick={() => setTelemetryMode('logs')}
                    data-testid="obs-btn-logs"
                  >
                    VictoriaLogs LogsQL
                  </button>
                  <button
                    type="button"
                    className={`subnav-btn ${telemetryMode === 'traces' ? 'active' : ''}`}
                    onClick={() => setTelemetryMode('traces')}
                    data-testid="obs-btn-traces"
                  >
                    VictoriaTraces OTLP
                  </button>
                </div>
              </div>

              {telemetryMode === 'metrics' && (
                <div className="obs-metrics-grid" data-testid="obs-metrics-content">
                  <div className="obs-metric-box">
                    <span className="metric-title">VOICE TTFR (TIME-TO-FIRST-RESPONSE)</span>
                    <span className="metric-big-val">74 ms</span>
                    <span className="metric-trend val-ok">↓ 12ms vs p95 baseline</span>
                  </div>
                  <div className="obs-metric-box">
                    <span className="metric-title">RESTATE INVOCATIONS / SEC</span>
                    <span className="metric-big-val">142 tps</span>
                    <span className="metric-trend val-ok">Zero retries</span>
                  </div>
                  <div className="obs-metric-box">
                    <span className="metric-title">POSTGRES CNPG POOL</span>
                    <span className="metric-big-val">6 / 64 active</span>
                    <span className="metric-trend val-ok">pgvector index warm</span>
                  </div>
                  <div className="obs-metric-box">
                    <span className="metric-title">PROACTIVE CI REMEDIATION RATE</span>
                    <span className="metric-big-val">100%</span>
                    <span className="metric-trend val-ok">Autonomous branch fixes</span>
                  </div>
                </div>
              )}

              {telemetryMode === 'logs' && (
                <div className="obs-logs-container" data-testid="obs-logs-content">
                  <div className="logsql-query-bar">
                    <code>_stream:{'{app="agent-worker"}'} level:info OR "wake"</code>
                  </div>
                  <div className="log-entries-list">
                    <div className="log-line">
                      <span className="log-ts">+14.210s</span>
                      <span className="log-level info">INFO</span>
                      <span className="log-app">[agent-worker]</span>
                      <span className="log-msg">TurnSession initialized: turn_id=turn-42 session=ThreadSession</span>
                    </div>
                    <div className="log-line">
                      <span className="log-ts">+14.480s</span>
                      <span className="log-level info">INFO</span>
                      <span className="log-app">[agent-worker]</span>
                      <span className="log-msg">memory_hybrid_query matched 4 active ACT-R declarative chunks</span>
                    </div>
                    <div className="log-line">
                      <span className="log-ts">+15.110s</span>
                      <span className="log-level info">INFO</span>
                      <span className="log-app">[build-orchestrator]</span>
                      <span className="log-msg">Building chunk chunk-6-site-interactive-landing-views</span>
                    </div>
                  </div>
                </div>
              )}

              {telemetryMode === 'traces' && (
                <div className="obs-traces-container" data-testid="obs-traces-content">
                  <div className="trace-header">
                    <span>Trace: <strong>7f9a1b4e2c8d...</strong></span>
                    <span>Duration: <strong>1,842 ms</strong></span>
                  </div>
                  <div className="trace-waterfall">
                    <div className="trace-span root-span">
                      <div className="span-info">
                        <span className="span-name">gateway.edge_webhook</span>
                        <span className="span-dur">1,842 ms</span>
                      </div>
                      <div className="span-bar" style={{ width: '100%' }} />
                    </div>
                    <div className="trace-span child-1">
                      <div className="span-info">
                        <span className="span-name">restate.dispatch_turn</span>
                        <span className="span-dur">24 ms</span>
                      </div>
                      <div className="span-bar" style={{ width: '12%', marginLeft: '2%' }} />
                    </div>
                    <div className="trace-span child-2">
                      <div className="span-info">
                        <span className="span-name">agent.turn_runner</span>
                        <span className="span-dur">1,790 ms</span>
                      </div>
                      <div className="span-bar" style={{ width: '92%', marginLeft: '5%' }} />
                    </div>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
