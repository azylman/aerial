import { useState, type KeyboardEvent } from 'react';

export type PillarFilter = 'all' | 'platform' | 'factory' | 'agent';

export interface TopologyNode {
  id: string;
  name: string;
  displayName?: string;
  pillar: 'platform' | 'factory' | 'agent';
  role: string;
  ports: string;
  displayPorts?: string;
  description: string;
  chapterId: string;
  chapterTitle: string;
  x: number;
  y: number;
  width: number;
  height: number;
  icon: string;
}

export interface TopologyEdge {
  id: string;
  from: string;
  to: string;
  label: string;
  pillar: 'platform' | 'factory' | 'agent' | 'all';
  path: string;
}

export const TOPOLOGY_NODES: readonly TopologyNode[] = [
  {
    id: 'k3s-nginx',
    name: 'k3s / NGINX Gateway',
    pillar: 'platform',
    role: 'Kubernetes Fabric & Gateway API Ingress',
    ports: 'Gateway API (LAN & Edge HTTPRoutes)',
    displayPorts: 'Gateway API HTTPRoutes',
    description:
      'Single-node or multi-node k3s cluster pairs NGINX Gateway Fabric with CoreDNS for local applications and external edge webhooks.',
    chapterId: 'application-platform',
    chapterTitle: 'The Self-Hosted Application Platform',
    x: 35,
    y: 50,
    width: 270,
    height: 72,
    icon: '🪐',
  },
  {
    id: 'cnpg-postgres',
    name: 'CloudNativePG Postgres 19 + pgvector',
    pillar: 'platform',
    role: 'Relational State, Embeddings & Hybrid RRF',
    ports: 'SQL State, pgvector & Prometheus Metrics',
    displayPorts: 'SQL & pgvector Index',
    description:
      'CloudNativePG PostgreSQL 19 with pgvector stores session memory, task graphs, and hybrid vector search indexes.',
    chapterId: 'application-platform',
    chapterTitle: 'The Self-Hosted Application Platform',
    x: 35,
    y: 160,
    width: 270,
    height: 72,
    icon: '🗄️',
  },
  {
    id: 'restate-nats',
    name: 'Restate + NATS JetStream',
    pillar: 'platform',
    role: 'Durable Workflows & Event Streaming',
    ports: 'RocksDB Virtual Objects & Durable Streams',
    displayPorts: 'Virtual Objects & Streams',
    description:
      'Restate durable virtual objects and NATS JetStream streams execute resilient event propagation and distributed transactions.',
    chapterId: 'application-platform',
    chapterTitle: 'The Self-Hosted Application Platform',
    x: 35,
    y: 270,
    width: 270,
    height: 72,
    icon: '⚡',
  },
  {
    id: 'victoria-telemetry',
    name: 'VictoriaMetrics / Logs / Traces',
    pillar: 'platform',
    role: 'Unified Metrics, Logs & OTLP Tracing',
    ports: 'TSDB Metrics, LogsQL & OTLP Tracing',
    displayPorts: 'Metrics, LogsQL & OTLP',
    description:
      'VictoriaMetrics provides 60-day LogsQL log retention, 30-day OTLP distributed tracing, and automated anomaly alert rules.',
    chapterId: 'application-platform',
    chapterTitle: 'The Self-Hosted Application Platform',
    x: 35,
    y: 380,
    width: 270,
    height: 72,
    icon: '📊',
  },
  {
    id: 'vikunja-docket',
    name: 'Vikunja DAG Backlog',
    pillar: 'factory',
    role: 'DAG Backlog Graph & Ready Queue',
    ports: 'REST API & Streamable HTTP Issue MCP',
    displayPorts: 'REST API & Issue MCP',
    description:
      'Vikunja tracks dependency tasks with PostgreSQL 19 hybrid search and exposes an unblocked ready queue for autonomous task dispatch.',
    chapterId: 'software-factory',
    chapterTitle: 'The Autonomous Software Factory',
    x: 345,
    y: 80,
    width: 270,
    height: 72,
    icon: '📋',
  },
  {
    id: 'vanadis-restate',
    name: 'Multi-PR Build Orchestrator',
    pillar: 'factory',
    role: 'Multi-PR Build DAG Orchestration',
    ports: 'Restate Build Workflows & JetStream Bridge',
    displayPorts: 'Restate Build Workflows',
    description:
      'The build orchestrator coordinates multi-chunk pull requests via Restate virtual objects, updates image tags, and dispatches CI remediation wakes.',
    chapterId: 'software-factory',
    chapterTitle: 'The Autonomous Software Factory',
    x: 345,
    y: 200,
    width: 270,
    height: 72,
    icon: '🏗️',
  },
  {
    id: 'synapse-flux',
    name: 'Event Router + Flux CD',
    pillar: 'factory',
    role: 'Discord Gateway & Push GitOps',
    ports: 'Discord Gateway, GitHub Webhooks & Push GitOps',
    displayPorts: 'Webhooks & Push GitOps',
    description:
      'The event router forwards GitHub webhooks and Discord events to Restate workflows while Flux CD delivers declarative GitOps updates.',
    chapterId: 'software-factory',
    chapterTitle: 'The Autonomous Software Factory',
    x: 345,
    y: 320,
    width: 270,
    height: 72,
    icon: '🔄',
  },
  {
    id: 'eri-restate',
    name: 'Agent Execution Engine',
    pillar: 'agent',
    role: 'Stateless Ingress & Multi-Engine Worker',
    ports: 'Stateless HTTP Ingress & Restate Worker',
    displayPorts: 'HTTP Ingress & Worker',
    description:
      'A stateless HTTP ingress front door pairs with a durable worker to execute coding engines under security guardrails.',
    chapterId: 'multimodal-agent',
    chapterTitle: 'The Multimodal AI Agent',
    x: 655,
    y: 130,
    width: 270,
    height: 72,
    icon: '🛰️',
  },
  {
    id: 'coven-haro',
    name: 'ACT-R Memory & Persistent Scheduler',
    displayName: 'Memory & Scheduler',
    pillar: 'agent',
    role: 'Cognitive Memory & Persistent Timers',
    ports: 'Hybrid RRF Recall & Durable Cron Watches',
    displayPorts: 'Hybrid RRF & Cron Watches',
    description:
      'The memory service decays facts with ACT-R algorithms while the persistent scheduler evaluates cron jobs, heartbeats, and external event triggers.',
    chapterId: 'multimodal-agent',
    chapterTitle: 'The Multimodal AI Agent',
    x: 655,
    y: 270,
    width: 270,
    height: 72,
    icon: '🧠',
  },
];

export const TOPOLOGY_EDGES: readonly TopologyEdge[] = [
  {
    id: 'edge-ingress-eri',
    from: 'k3s-nginx',
    to: 'eri-restate',
    label: 'HTTP Ingress & Webhooks',
    pillar: 'agent',
    path: 'M 305 86 C 330 86, 330 50, 360 50 L 610 50 C 635 50, 635 146, 655 146',
  },
  {
    id: 'edge-eri-postgres',
    from: 'eri-restate',
    to: 'cnpg-postgres',
    label: 'Session State & Vectors',
    pillar: 'platform',
    path: 'M 655 166 C 630 166, 630 176, 605 176 L 350 176 C 325 176, 325 196, 305 196',
  },
  {
    id: 'edge-eri-restate',
    from: 'eri-restate',
    to: 'restate-nats',
    label: 'Durable Sessions',
    pillar: 'platform',
    path: 'M 655 186 C 630 186, 630 296, 605 296 L 340 296 C 320 296, 320 290, 305 290',
  },
  {
    id: 'edge-eri-coven',
    from: 'eri-restate',
    to: 'coven-haro',
    label: 'Memory Recall & Crons',
    pillar: 'agent',
    path: 'M 790 202 L 790 270',
  },
  {
    id: 'edge-vikunja-vanadis',
    from: 'vikunja-docket',
    to: 'vanadis-restate',
    label: 'DAG Ready Tasks',
    pillar: 'factory',
    path: 'M 480 152 L 480 200',
  },
  {
    id: 'edge-vanadis-synapse',
    from: 'vanadis-restate',
    to: 'synapse-flux',
    label: 'Push GitOps Trigger',
    pillar: 'factory',
    path: 'M 480 272 L 480 320',
  },
  {
    id: 'edge-vanadis-eri',
    from: 'vanadis-restate',
    to: 'eri-restate',
    label: 'CI Self-Healing Remediation',
    pillar: 'factory',
    path: 'M 615 236 C 635 236, 635 166, 655 166',
  },
  {
    id: 'edge-platform-telemetry',
    from: 'restate-nats',
    to: 'victoria-telemetry',
    label: 'Metrics & Spans',
    pillar: 'platform',
    path: 'M 170 342 L 170 380',
  },
];

export function ArchitectureMap() {
  const [activePillar, setActivePillar] = useState<PillarFilter>('all');
  const [selectedNodeId, setSelectedNodeId] = useState<string>('eri-restate');

  const selectedNode = TOPOLOGY_NODES.find((n) => n.id === selectedNodeId) ?? TOPOLOGY_NODES[0]!;

  const handleNodeClick = (nodeId: string) => {
    setSelectedNodeId(nodeId);
  };

  const handleNodeKeyDown = (e: KeyboardEvent, nodeId: string) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      setSelectedNodeId(nodeId);
    }
  };

  const isNodeDimmed = (node: TopologyNode) => {
    if (activePillar === 'all') return false;
    return node.pillar !== activePillar;
  };

  const isEdgeDimmed = (edge: TopologyEdge) => {
    if (activePillar === 'all') return false;
    return edge.pillar !== activePillar && edge.pillar !== 'all';
  };

  return (
    <div className="arch-map-container" data-testid="architecture-map">
      <div className="arch-map-header">
        <div className="section-eyebrow">Interactive Topology Map</div>
        <h2 className="section-title">The Three Pillars of Aerial</h2>
        <p className="section-subtitle">
          Click any component node to inspect its runtime boundaries, network interfaces, and architectural specifications,
          or filter the topology by pillar.
        </p>

        {/* Pillar Filter Tabs */}
        <div className="pillar-filter-bar" role="tablist" aria-label="Filter topology by pillar">
          <button
            type="button"
            role="tab"
            aria-selected={activePillar === 'all'}
            className={`pillar-filter-btn ${activePillar === 'all' ? 'active' : ''}`}
            onClick={() => setActivePillar('all')}
            data-testid="filter-all"
          >
            All Pillars (9 Nodes)
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={activePillar === 'platform'}
            className={`pillar-filter-btn ${activePillar === 'platform' ? 'active' : ''}`}
            onClick={() => setActivePillar('platform')}
            data-testid="filter-platform"
          >
            🪐 Application Platform
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={activePillar === 'factory'}
            className={`pillar-filter-btn ${activePillar === 'factory' ? 'active' : ''}`}
            onClick={() => setActivePillar('factory')}
            data-testid="filter-factory"
          >
            ⚡ Software Factory
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={activePillar === 'agent'}
            className={`pillar-filter-btn ${activePillar === 'agent' ? 'active' : ''}`}
            onClick={() => setActivePillar('agent')}
            data-testid="filter-agent"
          >
            🛰️ Multimodal Agent
          </button>
        </div>
      </div>

      {/* SVG Topology Canvas */}
      <div className="arch-svg-wrapper">
        <svg
          viewBox="0 0 960 500"
          className="arch-svg"
          role="group"
          aria-label="Aerial 3-Pillar Architecture Topology Map"
        >
          <defs>
            <linearGradient id="edge-gradient-accent" x1="0%" y1="0%" x2="100%" y2="100%">
              <stop offset="0%" stopColor="var(--ae-accent)" stopOpacity="0.8" />
              <stop offset="100%" stopColor="var(--ae-info)" stopOpacity="0.8" />
            </linearGradient>
            <marker
              id="arrowhead"
              markerWidth="8"
              markerHeight="6"
              refX="7"
              refY="3"
              orient="auto"
              markerUnits="strokeWidth"
            >
              <polygon points="0 0, 8 3, 0 6" fill="var(--ae-accent)" />
            </marker>
          </defs>

          {/* Pillar Boundary Guides */}
          <g className="pillar-column-guides" aria-hidden="true">
            <rect x="25" y="20" width="290" height="460" rx="12" className="pillar-col-bg pillar-platform-bg" />
            <text x="40" y="42" className="pillar-col-title">
              PILLAR 1: PLATFORM RUNTIME
            </text>

            <rect x="335" y="20" width="290" height="460" rx="12" className="pillar-col-bg pillar-factory-bg" />
            <text x="350" y="42" className="pillar-col-title">
              PILLAR 2: SOFTWARE FACTORY
            </text>

            <rect x="645" y="20" width="290" height="460" rx="12" className="pillar-col-bg pillar-agent-bg" />
            <text x="660" y="42" className="pillar-col-title">
              PILLAR 3: MULTIMODAL AGENT
            </text>
          </g>

          {/* Data Flow Edges */}
          <g className="topology-edges">
            {TOPOLOGY_EDGES.map((edge) => {
              const dimmed = isEdgeDimmed(edge);
              return (
                <g key={edge.id} className={`edge-group ${dimmed ? 'dimmed' : ''}`} data-testid={`edge-${edge.id}`}>
                  <path
                    d={edge.path}
                    fill="none"
                    stroke={dimmed ? 'var(--ae-border)' : 'var(--ae-accent)'}
                    strokeWidth={dimmed ? '1.5' : '2.5'}
                    strokeDasharray={edge.label.includes('Remediation') ? '6 4' : undefined}
                    markerEnd="url(#arrowhead)"
                    className="edge-path"
                  />
                </g>
              );
            })}
          </g>

          {/* Architecture Nodes */}
          <g className="topology-nodes">
            {TOPOLOGY_NODES.map((node) => {
              const isSelected = node.id === selectedNode.id;
              const isDimmed = isNodeDimmed(node);

              return (
                <g
                  key={node.id}
                  transform={`translate(${node.x}, ${node.y})`}
                  className={`node-group ${isSelected ? 'selected' : ''} ${isDimmed ? 'dimmed' : ''}`}
                  onClick={() => handleNodeClick(node.id)}
                  onKeyDown={(e) => handleNodeKeyDown(e, node.id)}
                  tabIndex={0}
                  role="button"
                  aria-pressed={isSelected}
                  aria-label={`${node.name} (${node.pillar} pillar)`}
                  data-testid={`node-${node.id}`}
                >
                  <rect
                    width={node.width}
                    height={node.height}
                    rx="8"
                    className="node-rect"
                  />
                  <text x="14" y="28" className="node-icon">
                    {node.icon}
                  </text>
                  <text x="44" y="27" className="node-title">
                    {node.displayName || node.name}
                  </text>
                  <text x="44" y="47" className="node-role">
                    {node.role}
                  </text>
                  <text x="44" y="62" className="node-ports">
                    {node.displayPorts || node.ports}
                  </text>
                </g>
              );
            })}
          </g>
        </svg>
      </div>

      {/* Inspector Details Panel */}
      <div className="arch-inspector-card" data-testid="node-inspector">
        <div className="inspector-header">
          <div className="inspector-title-area">
            <span className="inspector-icon" aria-hidden="true">
              {selectedNode.icon}
            </span>
            <div>
              <div className="inspector-eyebrow">
                <span className={`pillar-tag pillar-tag-${selectedNode.pillar}`}>
                  {selectedNode.pillar.toUpperCase()} PILLAR
                </span>
                <span className="inspector-ports">{selectedNode.ports}</span>
              </div>
              <h3 className="inspector-name">{selectedNode.name}</h3>
            </div>
          </div>
          <a
            href={`#/docs/${selectedNode.chapterId}`}
            className="btn-secondary inspector-link"
            aria-label={`Read deep-dive for ${selectedNode.name} in handbook`}
          >
            <span>Read Ch: {selectedNode.chapterTitle.split('&')[0]?.trim()}</span>
            <span aria-hidden="true">→</span>
          </a>
        </div>

        <p className="inspector-desc">{selectedNode.description}</p>
        <div className="inspector-footer">
          <span className="inspector-role-label">Primary Responsibility:</span>
          <span className="inspector-role-value">{selectedNode.role}</span>
        </div>
      </div>
    </div>
  );
}
