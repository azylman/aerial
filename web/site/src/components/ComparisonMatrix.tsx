import { useState, useMemo } from 'react';

export type CompetitorCategory = 'all' | 'homelab' | 'factory' | 'agent';

export interface Competitor {
  id: string;
  name: string;
  category: 'homelab' | 'factory' | 'agent';
  categoryLabel: string;
  strengths: readonly string[];
  limitations: readonly string[];
  aerialAdvantage: string;
}

export interface DimensionComparison {
  dimension: string;
  homelab: string;
  factory: string;
  agent: string;
  aerial: string;
}

export const COMPETITORS: readonly Competitor[] = [
  {
    id: 'home-ops',
    name: 'home-ops / khuedoan/homelab',
    category: 'homelab',
    categoryLabel: 'Homelab & PaaS',
    strengths: [
      'Declarative GitOps using Flux CD or Argo CD',
      'Mature Kubernetes infrastructure and network policies',
      'Community cluster blueprints with extensive documentation',
    ],
    limitations: [
      'Treats applications as static targets deployed by humans',
      'Lacks autonomous coding agents and backlog discovery',
      'Requires manual multi-repository rollout coordination',
    ],
    aerialAdvantage:
      'Aerial builds on Flux CD GitOps. It embeds an autonomous software factory that writes, tests, and deploys services to the cluster.',
  },
  {
    id: 'coolify-dokploy',
    name: 'Coolify / Dokploy',
    category: 'homelab',
    categoryLabel: 'Homelab & PaaS',
    strengths: [
      'Intuitive web dashboards for single-server container deployment',
      'Simple Git push webhooks and database provisioning',
      'Clean developer ergonomics for application hosting',
    ],
    limitations: [
      'Lacks durable workflow state machines and event streams',
      'Contains no autonomous agent loop or DAG task scheduling',
      'Focuses on single-server Docker Compose rather than Kubernetes clusters',
    ],
    aerialAdvantage:
      'Aerial runs on Kubernetes with Restate virtual objects, PostgreSQL 19 with pgvector, and NATS JetStream for multi-node clusters.',
  },
  {
    id: 'gas-town-beads',
    name: 'Gas Town / Beads',
    category: 'factory',
    categoryLabel: 'Software Factory',
    strengths: [
      'Git-backed issue graphs and DAG dependency tracking',
      'Unblocked task resolution for coding agents',
      'Clear focus on local development workflows',
    ],
    limitations: [
      'Runs in local development sandboxes without live deployment targets',
      'Lacks self-hosted GitOps and push deployment pipelines',
      'Contains no persistent multimodal assistant or device integrations',
    ],
    aerialAdvantage:
      'Aerial pairs a Vikunja task graph with Restate multi-PR pipelines and Flux CD deployments. It takes tasks from backlog to production.',
  },
  {
    id: 'attractor-openhands',
    name: 'StrongDM Attractor / OpenHands',
    category: 'factory',
    categoryLabel: 'Software Factory',
    strengths: [
      'Autonomous coding agent execution loops',
      'Containerized development environments and terminal sandboxes',
      'Interactive debugging and benchmark evaluation suites',
    ],
    limitations: [
      'Operates in ephemeral testbeds without persistent platform state',
      'Lacks native GitOps delivery, telephony, and voice interfaces',
      'Omits cognitive temporal memory decay and procedural skill synthesis',
    ],
    aerialAdvantage:
      'Aerial connects code authoring with cluster operations. It provides proactive CI remediation, Discord and voice interaction, and ACT-R memory.',
  },
  {
    id: 'hermes-openclaw',
    name: 'Hermes Agent / OpenClaw',
    category: 'agent',
    categoryLabel: 'AI Agent',
    strengths: [
      'Extensible chat daemons and channel plugins',
      'Support for local LLM inference engines',
      'Community tool registries and assistant scripting',
    ],
    limitations: [
      'Ephemeral background daemons without durable state machines',
      'Lacks multi-PR build orchestration and Kubernetes delivery',
      'Omits read-only mounts and pre-tool security firewalls',
    ],
    aerialAdvantage:
      'Restate virtual objects, read-only repository mounts, secret scrubbing, and VictoriaMetrics telemetry protect the platform.',
  },
];

export const DIMENSIONS: readonly DimensionComparison[] = [
  {
    dimension: 'Primary Architecture',
    homelab: 'Declarative GitOps or container app dashboards',
    factory: 'Terminal loops or standalone runner sandboxes',
    agent: 'Chat daemon or long-polling desktop service',
    aerial: 'Unified k3s GitOps platform + Restate durable execution + multimodal agent',
  },
  {
    dimension: 'Stateful Orchestration',
    homelab: 'Kubernetes controllers or Docker Swarm',
    factory: 'In-process memory or workspace file state',
    agent: 'Local SQLite or filesystem session files',
    aerial: 'Restate RocksDB-backed virtual objects + NATS JetStream durable event streaming',
  },
  {
    dimension: 'Delivery & Backlog',
    homelab: 'Manual human commits and pull request reviews',
    factory: 'Issue graphs or terminal prompt queues',
    agent: 'Conversational chat reminders or simple tasks',
    aerial: 'Integrated Vikunja + PostgreSQL 19 hybrid text and vector search queue',
  },
  {
    dimension: 'Multi-PR Workflows',
    homelab: 'Manual multi-repository coordination',
    factory: 'Single-repository focus or manual sequence',
    agent: 'Subagents without cross-repository coordination',
    aerial: 'Restate state machines coordinating multi-PR DAG rollouts',
  },
  {
    dimension: 'Deployment & Self-Healing',
    homelab: 'GitOps sync alerts with manual triage',
    factory: 'Sandbox testbeds without live cluster GitOps',
    agent: 'No built-in Kubernetes delivery',
    aerial: 'Push GitOps via Flux CD with automated CI failure remediation',
  },
  {
    dimension: 'Glass-Box Observability',
    homelab: 'Cluster Grafana and Prometheus monitoring',
    factory: 'Command line logs or local task inspectors',
    agent: 'Chat logs or terminal output',
    aerial: 'Operator Console, AgentsView transcripts, Restate UI, and VictoriaMetrics telemetry',
  },
  {
    dimension: 'Security & Guardrails',
    homelab: 'Standard Linux and Kubernetes access controls',
    factory: 'Container sandboxes or workspace boundaries',
    agent: 'Direct tool execution or simple prompts',
    aerial: 'Read-only repository mounts, secret scrubbing, and pre-tool hook interceptors',
  },
];

export function ComparisonMatrix() {
  const [selectedCategory, setSelectedCategory] = useState<CompetitorCategory>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [viewMode, setViewMode] = useState<'cards' | 'matrix'>('cards');

  const filteredCompetitors = useMemo(() => {
    return COMPETITORS.filter((comp) => {
      const matchesCat = selectedCategory === 'all' || comp.category === selectedCategory;
      const q = searchQuery.toLowerCase().trim();
      if (!q) return matchesCat;
      const matchesSearch =
        comp.name.toLowerCase().includes(q) ||
        comp.categoryLabel.toLowerCase().includes(q) ||
        comp.aerialAdvantage.toLowerCase().includes(q) ||
        comp.strengths.some((s) => s.toLowerCase().includes(q)) ||
        comp.limitations.some((l) => l.toLowerCase().includes(q));
      return matchesCat && matchesSearch;
    });
  }, [selectedCategory, searchQuery]);

  const filteredDimensions = useMemo(() => {
    const q = searchQuery.toLowerCase().trim();
    if (!q) return DIMENSIONS;
    return DIMENSIONS.filter((dim) => {
      const matchesDim = dim.dimension.toLowerCase().includes(q);
      const matchesAerial = dim.aerial.toLowerCase().includes(q);
      const matchesHomelab =
        (selectedCategory === 'all' || selectedCategory === 'homelab') &&
        dim.homelab.toLowerCase().includes(q);
      const matchesFactory =
        (selectedCategory === 'all' || selectedCategory === 'factory') &&
        dim.factory.toLowerCase().includes(q);
      const matchesAgent =
        (selectedCategory === 'all' || selectedCategory === 'agent') &&
        dim.agent.toLowerCase().includes(q);

      return matchesDim || matchesAerial || matchesHomelab || matchesFactory || matchesAgent;
    });
  }, [searchQuery, selectedCategory]);

  const matrixGridCols = selectedCategory === 'all' ? '180px 1fr 1fr 1fr 1.3fr' : '200px 1fr 1.3fr';

  return (
    <div className="comparison-container" data-testid="comparison-matrix">
      <div className="comparison-header">
        <div className="section-eyebrow">Market Positioning</div>
        <h2 className="section-title">Why Aerial & How It Compares</h2>
        <p className="section-subtitle">
          GitOps platforms lack autonomous coding agents. Standalone coding agents lack production clusters. Aerial unifies
          platform infrastructure, software delivery, and autonomous agents.
        </p>

        {/* Filter and Search Controls */}
        <div className="comparison-controls">
          <div className="category-filter-tabs" role="tablist" aria-label="Competitor categories">
            <button
              type="button"
              role="tab"
              aria-selected={selectedCategory === 'all'}
              className={`filter-btn ${selectedCategory === 'all' ? 'active' : ''}`}
              onClick={() => setSelectedCategory('all')}
              data-testid="comp-filter-all"
            >
              All Categories
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={selectedCategory === 'homelab'}
              className={`filter-btn ${selectedCategory === 'homelab' ? 'active' : ''}`}
              onClick={() => setSelectedCategory('homelab')}
              data-testid="comp-filter-homelab"
            >
              Homelab & PaaS
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={selectedCategory === 'factory'}
              className={`filter-btn ${selectedCategory === 'factory' ? 'active' : ''}`}
              onClick={() => setSelectedCategory('factory')}
              data-testid="comp-filter-factory"
            >
              Software Factories
            </button>
            <button
              type="button"
              role="tab"
              aria-selected={selectedCategory === 'agent'}
              className={`filter-btn ${selectedCategory === 'agent' ? 'active' : ''}`}
              onClick={() => setSelectedCategory('agent')}
              data-testid="comp-filter-agent"
            >
              AI Agents
            </button>
          </div>

          <div className="comparison-right-controls">
            <input
              type="search"
              className="search-input"
              placeholder={
                viewMode === 'matrix'
                  ? 'Filter comparison dimensions by keyword...'
                  : 'Filter by keyword or competitor...'
              }
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              aria-label={
                viewMode === 'matrix'
                  ? 'Filter comparison dimensions by keyword'
                  : 'Filter competitors by keyword'
              }
              data-testid="comp-search-input"
            />
            <div className="view-mode-toggle" role="group" aria-label="Comparison view mode">
              <button
                type="button"
                className={`view-toggle-btn ${viewMode === 'cards' ? 'active' : ''}`}
                onClick={() => setViewMode('cards')}
                data-testid="view-cards-btn"
              >
                Cards
              </button>
              <button
                type="button"
                className={`view-toggle-btn ${viewMode === 'matrix' ? 'active' : ''}`}
                onClick={() => setViewMode('matrix')}
                data-testid="view-matrix-btn"
              >
                Matrix
              </button>
            </div>
          </div>
        </div>
      </div>

      {viewMode === 'cards' ? (
        <div className="competitor-cards-grid" data-testid="competitor-cards">
          {filteredCompetitors.map((comp) => (
            <div key={comp.id} className="competitor-card" data-testid={`comp-card-${comp.id}`}>
              <div className="card-top">
                <span className={`comp-category-tag tag-${comp.category}`}>{comp.categoryLabel}</span>
                <h3 className="comp-name">{comp.name}</h3>
              </div>

              <div className="comp-columns">
                <div className="comp-subcol">
                  <h4 className="subcol-heading ok-heading">What They Do Well</h4>
                  <ul className="comp-list">
                    {comp.strengths.map((s, idx) => (
                      <li key={idx}>{s}</li>
                    ))}
                  </ul>
                </div>

                <div className="comp-subcol">
                  <h4 className="subcol-heading warn-heading">What&apos;s Missing</h4>
                  <ul className="comp-list">
                    {comp.limitations.map((l, idx) => (
                      <li key={idx}>{l}</li>
                    ))}
                  </ul>
                </div>
              </div>

              <div className="aerial-advantage-box">
                <span className="advantage-label">AERIAL&apos;S CLOSED-LOOP ADVANTAGE:</span>
                <p className="advantage-text">{comp.aerialAdvantage}</p>
              </div>
            </div>
          ))}
          {filteredCompetitors.length === 0 && (
            <div className="empty-results-message">No competitors matched your search query.</div>
          )}
        </div>
      ) : (
        <div className="matrix-table-wrapper" data-testid="comparison-table">
          <div className="matrix-grid">
            <div className="matrix-row matrix-header-row" style={{ gridTemplateColumns: matrixGridCols }}>
              <div className="matrix-cell cell-dimension">Capability Dimension</div>
              {(selectedCategory === 'all' || selectedCategory === 'homelab') && (
                <div className="matrix-cell cell-competitor">Homelab & PaaS</div>
              )}
              {(selectedCategory === 'all' || selectedCategory === 'factory') && (
                <div className="matrix-cell cell-competitor">Software Factories</div>
              )}
              {(selectedCategory === 'all' || selectedCategory === 'agent') && (
                <div className="matrix-cell cell-competitor">AI Agents</div>
              )}
              <div className="matrix-cell cell-aerial">AERIAL (Closed-Loop)</div>
            </div>

            {filteredDimensions.map((dim, idx) => (
              <div
                key={idx}
                className="matrix-row"
                style={{ gridTemplateColumns: matrixGridCols }}
                data-testid={`matrix-row-${idx}`}
              >
                <div className="matrix-cell cell-dimension">
                  <strong>{dim.dimension}</strong>
                </div>
                {(selectedCategory === 'all' || selectedCategory === 'homelab') && (
                  <div className="matrix-cell cell-competitor">{dim.homelab}</div>
                )}
                {(selectedCategory === 'all' || selectedCategory === 'factory') && (
                  <div className="matrix-cell cell-competitor">{dim.factory}</div>
                )}
                {(selectedCategory === 'all' || selectedCategory === 'agent') && (
                  <div className="matrix-cell cell-competitor">{dim.agent}</div>
                )}
                <div className="matrix-cell cell-aerial highlight-aerial">{dim.aerial}</div>
              </div>
            ))}
            {filteredDimensions.length === 0 && (
              <div className="empty-results-message">No comparison dimensions matched your search query.</div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
