export interface CodeSnippet {
  readonly language?: string;
  readonly code: string;
  readonly caption?: string;
}

export interface ChapterCallout {
  readonly type: 'note' | 'tip' | 'warning' | 'important';
  readonly title: string;
  readonly text: string;
}

export interface ChapterSection {
  readonly id: string;
  readonly title: string;
  readonly lead?: string;
  readonly paragraphs: readonly string[];
  readonly bullets?: readonly string[];
  readonly codeSnippets?: readonly CodeSnippet[];
  readonly callout?: ChapterCallout;
}

export interface HandbookChapter {
  readonly id: string;
  readonly number: number;
  readonly title: string;
  readonly summary: string;
  readonly sections: readonly ChapterSection[];
}

export const HANDBOOK_CHAPTERS: readonly HandbookChapter[] = [
  {
    id: 'overview',
    number: 1,
    title: 'Overview & Why Aerial',
    summary:
      'The closed-loop thesis, real-world autonomous workflows, glass-box operations, and architecture philosophy.',
    sections: [
      {
        id: 'closed-loop-thesis',
        title: 'The Closed-Loop Thesis',
        lead:
          'Open-loop chatbots generate text without execution feedback. Autonomous software engineering requires real-world feedback loops: hermetic compilation, unit testing, schema verification, and automated deployment.',
        paragraphs: [
          'Conventional AI coding assistants operate in an open loop. Language models generate candidate code and leave it in editor buffers. Humans must compile, test, and deploy this code manually. Regressions multiply rapidly without direct compiler feedback.',
          'Aerial uses closed-loop automation. Autonomous engineering requires unassisted feedback from compilation toolchains, unit test runners, and production telemetry. Every proposed change must build cleanly. Changes must pass test suites, complete multi-agent code reviews, and merge through event-driven GitOps pipelines.',
        ],
        bullets: [
          'Hermetic Verification: Every code change compiles and passes tests before opening a pull request.',
          'Declarative Schema Evolution: Databases update through declarative schema diffs validated during testing.',
          'Automated Self-Healing: In-cluster deployment failures wake agent remediation loops automatically.',
          'Production-Grade Tooling: Agents execute real compiler toolchains, package managers, and container runtimes.',
        ],
        codeSnippets: [
          {
            language: 'bash',
            caption: 'Hermetic staged verification workflow',
            code: `# Run staged verification before any commit or PR creation
./scripts/verify.sh --staged

# Execute full package test suite
bazel test //services/... //pkg/...`,
          },
        ],
        callout: {
          type: 'important',
          title: 'The Closed-Loop Invariant',
          text:
            'In Aerial, agents never commit or ship code on assertion alone. Compiler diagnostics and hermetic test runs validate every change before opening a pull request.',
        },
      },
      {
        id: 'autonomous-workflows',
        title: 'Real-World Autonomous Workflows',
        lead:
          'Aerial advances beyond single-file autocompletion. The platform orchestrates multi-PR cross-repository task graphs, tiered complexity reviews, and automated backlog processing.',
        paragraphs: [
          'Real software systems do not live in single files. Production engineering spans multiple repositories, database schema migrations, and sequential rollouts. Introducing a feature requires updating upstream service APIs, waiting for deployment, and enabling downstream configurations in consumer repositories.',
          'Aerial coordinates distributed multi-chunk build pipelines with durable state machines. Agents author, validate, and execute complex multi-repository dependency graphs. The system eliminates foreground polling loops and prevents context window exhaustion.',
        ],
        bullets: [
          'Task Graph Queue: Work organizes into dependency graphs and queues based on blocker resolution.',
          'Multi-Chunk Builds: Features spanning upstream engines and downstream configurations run in cohesive multi-chunk plans.',
          'Multi-Agent Review: Tiered review panels inspect plans and diffs across architecture, security, and edge cases.',
          'Autonomous Remediation: Green CI checks merge automatically, while test failures wake remediation agents with exact failure logs.',
        ],
        callout: {
          type: 'tip',
          title: 'Two-Step Feature Rule',
          text:
            'Upstream services ship schema fields and endpoints first. Downstream configuration repositories enable features only after upstream merges deploy successfully.',
        },
      },
      {
        id: 'glass-box-operations',
        title: 'Glass-Box Operations & Observability',
        lead:
          'Aerial rejects opaque cloud agent platforms. Every turn, tool invocation, token count, and runtime trace remains visible on your self-hosted infrastructure.',
        paragraphs: [
          'Proprietary agent platforms hide prompt assembly, intermediate tool steps, and failure traces behind opaque web portals. Users lack diagnostic visibility when agents hallucinate or loop endlessly.',
          'Aerial delivers glass-box observability. Every conversational turn, tool execution, and background task streams structured telemetry into self-hosted stores. The telemetry stack includes VictoriaMetrics, VictoriaLogs, VictoriaTraces, and the AgentsView transcript visualizer.',
        ],
        bullets: [
          'Live Web Dashboard: AgentsView renders real-time conversation transcripts, tool calls, and subagent trees.',
          'Log Retention & Search: VictoriaLogs captures container output with 60-day retention and fast LogsQL querying.',
          'Distributed Tracing: VictoriaTraces records OpenTelemetry spans across HTTP ingress, durable virtual objects, and tool calls.',
          'Metric Scraping: VictoriaMetrics collects CPU, memory, token usage, and latency metrics through Prometheus scrapers.',
        ],
        codeSnippets: [
          {
            language: 'logsql',
            caption: 'VictoriaLogs LogsQL query for scoped container log forensics',
            code: `_stream:{kubernetes.container_name="<container-name>"} level:in(WARN, ERROR)
| fields _time, kubernetes.container_name, level, _msg
| sort by (_time) desc
| limit 50`,
          },
        ],
      },
      {
        id: 'architecture-philosophy',
        title: 'Architecture Philosophy & Decoupled Repositories',
        lead:
          'Aerial separates generic platform logic from private user configurations through a strict multi-repository architecture and ephemeral scratch workspaces.',
        paragraphs: [
          'Personal assistant projects often entangle private details into core application source code. Hardcoding personal names, addresses, device identifiers, and secret keys prevents code sharing and complicates upgrades.',
          'Aerial enforces a decoupled two-repository model. The core monorepo contains domain-agnostic infrastructure code, microservices, and base system rules. The user configuration repository contains private persona rules, custom domain skills, service configurations, and Kubernetes cluster overrides.',
        ],
        bullets: [
          'Domain-Agnostic Core: The core repository contains zero personal data, real names, or private credentials.',
          'Private Config Repository: Personal rules, custom skill runbooks, and cluster overrides live in a private GitOps repository.',
          'Repository Integrity: Pre-tool security hooks guard base mount directories against in-place mutations.',
          'Ephemeral Scratch Workspaces: Agents perform modifications in isolated scratch clones initialized through helper scripts.',
        ],
      },
    ],
  },
  {
    id: 'getting-started',
    number: 2,
    title: 'Getting Started & Two-Repo GitOps',
    summary:
      'Single-node k3s bootstrap, repository decoupling, Infisical secret management, agent CLI verification, and multi-node hardware scaling.',
    sections: [
      {
        id: 'k3s-bootstrap',
        title: 'Single-Node k3s Bootstrap',
        lead:
          'Aerial runs on a single Linux host running k3s with Traefik disabled. NGINX Gateway Fabric handles inbound HTTP and HTTPS gateway traffic directly.',
        paragraphs: [
          'Aerial does not require managed cloud Kubernetes providers. A commodity computer, homelab server, or virtual machine running Linux hosts the platform and software factory.',
          'The bootstrap process disables Traefik. Aerial standardizes on NGINX Gateway Fabric for Kubernetes Gateway API routing across local networks and external edge tunnels.',
        ],
        bullets: [
          'Minimum Hardware: 4 vCPU cores, 8 GB memory, and 60 GB solid-state disk storage.',
          'Network Ingress: NGINX Gateway Fabric manages inbound traffic on host interfaces.',
          'Cluster Management: Standard kubectl, Helm, and Flux CLI binaries administer the cluster.',
        ],
        codeSnippets: [
          {
            language: 'bash',
            caption: 'Install single-node k3s with Traefik disabled',
            code: `# Install k3s with Traefik disabled so NGINX Gateway Fabric manages web ingress
curl -sfL https://get.k3s.io | sh -s - \\
  --disable traefik \\
  --write-kubeconfig ~/.kube/config \\
  --write-kubeconfig-mode 600

# Verify cluster nodes
kubectl get nodes -o wide`,
          },
        ],
      },
      {
        id: 'two-repo-gitops',
        title: 'The Two-Repo GitOps Architecture',
        lead:
          'Fork the configuration template to establish your private configuration repository, seed bootstrap credentials, and bootstrap Flux CD.',
        paragraphs: [
          'Aerial uses GitOps for continuous delivery. Flux CD reconciles two Kustomization resources: operator infrastructure and core platform services.',
          'Users fork azylman/aerial-config-example into a private configuration repository. Bootstrap secrets are seeded into Kubernetes via Secret/aerial-bootstrap-secrets, and Flux CD continuously reconciles user overrides on top of upstream base manifests.',
        ],
        bullets: [
          'Zero Plaintext Tokens: In-memory authentication passes tokens during bootstrap without writing credentials to disk.',
          'Declarative Configuration: Configuration files declare model selection, timezone settings, channel policies, and administrator lists.',
          'Automated Reconciliation: Flux continuously monitors the repository and synchronizes manifest updates within minutes.',
        ],
        codeSnippets: [
          {
            language: 'bash',
            caption: 'Seed bootstrap secrets and bootstrap Flux CD on your private configuration repository',
            code: `# Set bootstrap credentials in memory
export GITHUB_PAT="your-github-pat"
export DISCORD_BOT_TOKEN="your-discord-bot-token"

# Seed bootstrap secrets for in-cluster Infisical auto-bootstrap
kubectl create secret generic aerial-bootstrap-secrets \\
  --from-literal=DISCORD_BOT_TOKEN="$DISCORD_BOT_TOKEN" \\
  --from-literal=GITHUB_PAT="$GITHUB_PAT"

# Install Flux CLI and bootstrap your private repository
curl -s https://fluxcd.io/install.sh | sudo bash
GITHUB_TOKEN="$GITHUB_PAT" flux bootstrap github \\
  --owner="your-github-username" \\
  --repository="my-aerial-config" \\
  --branch=main \\
  --path=k8s \\
  --personal`,
          },
        ],
      },
      {
        id: 'infisical-secrets',
        title: 'Infisical Machine Identity & Secret Bootstrap',
        lead:
          'Aerial centralizes secret management with Infisical and External Secrets Operator, auto-bootstrapping credentials in-cluster without host CLI dependencies.',
        paragraphs: [
          'An in-cluster Infisical instance stores application credentials, bot tokens, and API keys. Job/infisical-bootstrap automatically initializes Infisical, creates project aerial and environment default, derives AERIAL_CONFIG_REPO_URL from GitRepository/flux-system, generates Secret/infisical-universal-auth, and ingests seeded secrets from Secret/aerial-bootstrap-secrets.',
          'External Secrets Operator monitors Infisical and generates scoped Kubernetes Secrets for platform services. External Secrets Operator also provisions database, messaging, and webhook credentials automatically and pushes them into Infisical via PushSecret.',
        ],
        bullets: [
          'In-Cluster Bootstrap: Job/infisical-bootstrap automatically initializes Infisical and creates Machine Identity credentials.',
          'Automatic Repo URL Derivation: Derives AERIAL_CONFIG_REPO_URL from GitRepository/flux-system without manual configuration.',
          'Zero Host CLI Dependencies: Automatically ingests DISCORD_BOT_TOKEN and GITHUB_PAT from Secret/aerial-bootstrap-secrets without requiring infisical CLI or jq.',
          'Scoped Distribution: Each service pod receives only its declared secret environment variables.',
        ],
        codeSnippets: [
          {
            language: 'bash',
            caption: 'In-cluster Infisical bootstrap and secret management',
            code: `# Initial credentials are seeded during Flux bootstrap via:
kubectl create secret generic aerial-bootstrap-secrets \\
  --from-literal=DISCORD_BOT_TOKEN="$DISCORD_BOT_TOKEN" \\
  --from-literal=GITHUB_PAT="$GITHUB_PAT"

# Job/infisical-bootstrap initializes Infisical, creates Secret/infisical-universal-auth,
# upserts DISCORD_BOT_TOKEN and GITHUB_PAT into Infisical, and deletes aerial-bootstrap-secrets automatically.`,
          },
        ],
      },
      {
        id: 'cli-auth-verify',
        title: 'Agent CLI Authentication & Rollout Verification',
        lead:
          'Authenticate the agent execution engine inside the worker container and verify Kubernetes pod readiness and GitOps sync state.',
        paragraphs: [
          'Platform workloads deploy through Flux CD after secret configuration. The final step authenticates the execution engine inside the agent worker container and verifies rollout health.',
          'Aerial supports multiple execution engines including Google Antigravity, Claude Code, and Codex. Start the interactive authentication flow in the worker container. Then inspect pod readiness and Flux synchronization to confirm a healthy cluster.',
        ],
        bullets: [
          'Execution Engine Authentication: Authenticate your selected agent CLI inside the agent worker container.',
          'Pod Health Inspection: Confirm that core services and operators report running status.',
          'Flux Reconciliation: Verify that all GitOps Kustomizations report ready and synchronized state.',
        ],
        codeSnippets: [
          {
            language: 'bash',
            caption: 'Authenticate agent CLI and verify cluster rollout',
            code: `# 1. Authenticate execution engine in eri-restate pod
kubectl exec -it $(kubectl get pod -l app.kubernetes.io/name=eri-restate -o name | head -n1) -c eri-restate -- agy

# 2. Verify all pods and Flux Kustomizations are healthy
kubectl get pods -o wide
flux get kustomizations`,
          },
        ],
      },
      {
        id: 'multi-node-scaling',
        title: 'Scaling to Multi-Node via cluster-overrides.yaml',
        lead:
          'Expand clusters across specialized storage, compute, and display nodes without modifying upstream base manifests.',
        paragraphs: [
          'Aerial scales to heterogeneous multi-node clusters as hardware requirements expand. Nodes accept specialized roles: storage nodes for databases, compute nodes for machine learning, and display nodes for kiosks.',
          'A single configuration overlay file defines all cluster-specific node selectors, tolerations, replica counts, and storage classes. Upstream base manifests remain hardware-agnostic.',
        ],
        bullets: [
          'Storage Tier: Pin relational databases, event streams, and journal stores to high-speed solid-state drives.',
          'Compute Tier: Route speech recognition, voice synthesis, and embedding models to dedicated GPU nodes.',
          'Prune Protection: Set kustomize.toolkit.fluxcd.io/prune to disabled to protect persistent resources during GitOps sync.',
        ],
        callout: {
          type: 'tip',
          title: 'Resource Adoption Safeguard',
          text:
            'When moving resources into cluster overrides, set the annotation kustomize.toolkit.fluxcd.io/prune to disabled. Use this annotation to prevent Flux kustomize-controller from deleting active workloads or storage during cluster cutovers.',
        },
      },
    ],
  },
  {
    id: 'application-platform',
    number: 3,
    title: 'The Self-Hosted Application Platform',
    summary:
      'Gateway routing, CoreDNS service discovery, CloudNativePG PostgreSQL with vector search, Restate durable execution, NATS streaming, and unified telemetry.',
    sections: [
      {
        id: 'ingress-mesh',
        title: 'NGINX Gateway Fabric & CoreDNS Service Mesh',
        lead:
          'Kubernetes Gateway API routing and internal CoreDNS rewrites provide zero-configuration service discovery across cluster workloads.',
        paragraphs: [
          'Aerial implements Kubernetes Gateway API specifications using NGINX Gateway Fabric. Two gateway classes isolate network traffic. The local gateway routes LAN traffic to dashboards and APIs, while the edge gateway receives external webhooks through secure tunnel proxies.',
          'CoreDNS internal rewrite rules resolve dot-aerial hostnames directly to cluster services. Workloads communicate across the mesh without hardcoded IP addresses or external DNS lookups.',
        ],
        bullets: [
          'Local Gateway: Manages private network routing for dashboards, APIs, and real-time event streams.',
          'Edge Gateway: Ingests inbound webhooks from external services through secure tunnel proxies.',
          'Internal Mesh: Resolves dot-aerial hostnames automatically across all pods and agent processes.',
        ],
        codeSnippets: [
          {
            language: 'yaml',
            caption: 'CoreDNS rewrite configuration for *.aerial mesh',
            code: `apiVersion: v1
kind: ConfigMap
metadata:
  name: coredns-custom
  namespace: kube-system
data:
  aerial.override: |
    rewrite stop {
        name regex restate\\.aerial\\.?$ restate.restate-green.svc.cluster.local.
        answer name (.*)\\.restate-green\\.svc\\.cluster\\.local\\.?$ restate.aerial.
    }
    rewrite stop {
        name regex restate-green\\.aerial\\.?$ restate.restate-green.svc.cluster.local.
        answer name (.*)\\.restate-green\\.svc\\.cluster\\.local\\.?$ restate-green.aerial.
    }
    rewrite stop {
        name regex (.*)\\.aerial\\.?$ {1}.default.svc.cluster.local.
        answer name (.*)\\.default\\.svc\\.cluster\\.local\\.?$ {1}.aerial.
    }`,
          },
        ],
      },
      {
        id: 'state-persistence',
        title: 'CloudNativePG PostgreSQL 19 & Restate Durable Execution',
        lead:
          'Relational records, vector embeddings, and long-running distributed workflows execute on high-availability PostgreSQL and Restate.',
        paragraphs: [
          'CloudNativePG manages relational data in a high-availability PostgreSQL cluster with vector search capabilities. The architecture enforces Single-Service Table Ownership: exactly one service owns and queries each database table. Services communicate through APIs rather than shared database queries.',
          'Restate powers stateful distributed agent workflows. Turns, multi-chunk builds, and schedule evaluations run as durable virtual objects. Workflows survive container crashes, node drains, and network timeouts with automatic journal replay.',
        ],
        bullets: [
          'CloudNativePG: Provides automated failover, rolling updates, write-ahead log archiving, and semantic vector indexing.',
          'Single Table Ownership: Microservices interact through typed APIs rather than shared database tables.',
          'Durable Workflows: Restate virtual objects execute workflows with deterministic replay.',
          'Crash-Resilient State: Completed steps replay from journals. Interrupted attempts rerun in full up to the crash-loop cap, requiring idempotent tools.',
        ],
        callout: {
          type: 'note',
          title: 'Durable Execution Guarantee',
          text:
            'Restate replays completed steps from journals across restarts. Interrupted attempts rerun in full up to the crash-loop cap. Tool calls may repeat during retries. Side-effecting tools must be idempotent or check state before mutating.',
        },
      },
      {
        id: 'event-streaming',
        title: 'NATS JetStream Distributed Event Backbone',
        lead:
          'Durable message streams with consumer acknowledgments connect decoupled services and trigger reactive wakes.',
        paragraphs: [
          'Platform services communicate asynchronously through an in-cluster NATS JetStream cluster. Durable message streams decouple event producers from downstream consumer workers.',
          'Streams capture system event notices, asynchronous job dispatches, debounced telemetry updates, and persisted execution transcripts. Persistent storage protects message journals during upgrades.',
        ],
        bullets: [
          'At-Least-Once Delivery: JetStream guarantees message delivery with consumer sequence tracking.',
          'Stream Deduplication: High-volume webhook events coalesce to prevent redundant agent processing.',
          'Persistent Reliability: NATS operates on durable storage with zero data loss during restarts.',
        ],
      },
      {
        id: 'observability-stack',
        title: 'VictoriaMetrics, VictoriaLogs & OpenTelemetry Tracing',
        lead:
          'A self-contained telemetry platform stores system metrics, application logs, and distributed traces with automatic credential redaction.',
        paragraphs: [
          'Observability runs natively on the cluster. VictoriaMetrics stores time-series metrics scraped through the Prometheus operator. VictoriaLogs captures node and container log streams with sixty-day retention.',
          'An OpenTelemetry Collector routes distributed traces to VictoriaTraces, stripping registered secret tokens and encoded variants from spans. Fluent Bit filters container logs using exact registered values, sensitive JSON fields, and PEM private keys before writing to VictoriaLogs.',
        ],
        bullets: [
          'Self-Hosted Telemetry: The platform provides full observability without recurring cloud monitoring subscriptions.',
          'High-Speed LogsQL: VictoriaLogs executes fast regex and facet queries across millions of log records.',
          'Credential Redaction: Trace processors mask registered tokens and encoded variants, while log filters sanitize registered secrets, JSON fields, and PEM keys.',
        ],
      },
    ],
  },
  {
    id: 'software-factory',
    number: 4,
    title: 'The Autonomous Software Factory',
    summary:
      'Task graphs in Vikunja, tiered complexity, multi-agent review panels, Restate multi-PR DAG orchestration, GitHub Actions CI, Ariga Atlas declarative migrations, and event-driven push GitOps.',
    sections: [
      {
        id: 'docket-queue',
        title: 'Vikunja Task Graph & DAG Ready Queue',
        lead:
          'Aerial organizes autonomous engineering work into a directed acyclic graph of tasks tracked in Vikunja and updated through an issue tracker MCP server.',
        paragraphs: [
          'Autonomous software engineering requires structured task management rather than unstructured prompts. Aerial organizes roadmaps and maintenance sweeps into a task graph inside an in-cluster Vikunja instance.',
          'Tasks define explicit blocker relationships. The ready queue surfaces only tasks whose prerequisites have finished. Agents execute complex roadmaps sequentially without attempting blocked tasks.',
        ],
        bullets: [
          'Issue Tracker Tools: Agents query ready tasks, inspect details, add comments, and close resolved issues.',
          'Context Preservation: Agents park unfinished sub-goals into the task graph before context windows exhaust.',
          'Dependency Resolution: Parent task completions unblock dependent child tasks automatically.',
        ],
        codeSnippets: [
          {
            language: 'json',
            caption: 'Task graph ready tool query',
            code: `{
  "tool": "ready",
  "arguments": {}
}`,
          },
        ],
      },
      {
        id: 'tiered-complexity',
        title: 'Tiered Complexity & Multi-Agent Review Panel',
        lead:
          'Engineering tasks follow a tiered complexity classification with adversarial multi-agent plan and diff reviews for non-trivial modifications.',
        paragraphs: [
          'Aerial prevents unstable modifications by enforcing a strict tiering rubric. Tiers range from trivial documentation edits to complex multi-package refactors and database schema migrations.',
          'Non-trivial tasks require review from a multi-agent review panel. Specialized subagents evaluate plans and code diffs across architecture contracts, security invariants, test coverage, and operational simplicity.',
        ],
        bullets: [
          'Tier Classification: Direct verification gates minor edits, while complex work requires multi-agent reviews.',
          'Adversarial Review: Subagent panels evaluate plans and diffs across architecture, security, and testing.',
          'Multi-Chunk Execution: Complex cross-package changes execute as coordinated multi-chunk builds.',
          'Dynamic Re-Tiering: Discovered complexity promotes in-flight tasks to higher tiers automatically.',
        ],
        callout: {
          type: 'important',
          title: 'Adversarial Multi-Agent Review Panel',
          text:
            'The subagent review panel provides independent, adversarial review passes across architecture, security, and testing before code merges.',
        },
      },
      {
        id: 'vanadis-orchestration',
        title: 'Restate Multi-PR Cross-Repository Orchestration',
        lead:
          'A Restate build pipeline coordinates multi-repository rollouts, dependency ordering, and staged progress tracking without holding active agent sessions open.',
        paragraphs: [
          'Features often span multiple repositories, such as adding an API endpoint in the engine and updating configuration in the user repository. Executing these steps in one turn risks context window exhaustion.',
          'The multi-PR build orchestrator divides builds into distinct chunks. Each chunk targets a repository, specifies dependencies, and provides instructions. The orchestrator triggers dependent chunks as upstream changes merge.',
        ],
        bullets: [
          'Multi-Repository Builds: Coordinates dependency-ordered changes across core engines and user configuration repositories.',
          'Non-Blocking Execution: Agent turns end immediately upon plan submission, conserving reasoning tokens.',
          'Dynamic Chunk Amendment: Operators can update pending chunks or append corrective steps during execution.',
        ],
        codeSnippets: [
          {
            language: 'bash',
            caption: 'Enqueue a multi-chunk build plan',
            code: `# Enqueue a multi-chunk build plan across repositories
scripts/aerial-build.sh enqueue /path/to/build-plan.json

# Check live build status
scripts/aerial-build.sh status <build-id>`,
          },
        ],
      },
      {
        id: 'ci-cd-gitops',
        title: 'Declarative Migrations, GitHub Actions CI & Event-Driven GitOps',
        lead:
          'Ariga Atlas declarative migrations, automated verification in GitHub Actions, and event-driven GitOps complete the software factory loop.',
        paragraphs: [
          'Database schemas declare table structures in a central SQL specification. Startup code never executes imperative database mutations. Development tools generate declarative migration files using Ariga Atlas, while automated verification validates migration directories.',
          'Continuous integration executes automated test suites, coverage checks, and LLM code reviews in GitHub Actions upon every pull request.',
          'Event-driven webhook routers notify Flux CD upon pull request merges, triggering immediate cluster reconciliation. Automated monitors wake remediation agents when test runs or automated reviews fail.',
        ],
        bullets: [
          'Ariga Atlas: Generates declarative schema diffs with automated migration directory validation in test pipelines.',
          'Automated CI & Code Review: Runs test suites, schema linters, and automated review passes in GitHub Actions.',
          'Event-Driven Push GitOps: Webhook routers trigger Flux reconciliation immediately upon pull request merges.',
          'Self-Healing CI Wakes: Automated test failures wake targeted remediation agents with complete log diagnostics.',
        ],
      },
    ],
  },
  {
    id: 'multimodal-agent',
    number: 5,
    title: 'The Multimodal AI Agent',
    summary:
      'Stateless HTTP ingress, durable Restate sessions, multi-engine CLI execution, hybrid RRF and ACT-R memory, persistent scheduling, voice, telephony, and browser automation.',
    sections: [
      {
        id: 'stateless-ingress',
        title: 'Stateless HTTP Ingress & Durable Restate Sessions',
        lead:
          'A stateless HTTP ingress service provides a lightweight front door paired with durable, fault-tolerant Restate execution sessions.',
        paragraphs: [
          'User interactions enter through the HTTP ingress service. This stateless service handles incoming chat messages, interactions, voice queries, and system prompts. Fast local language model evaluations triage incoming messages before waking heavy reasoning agents.',
          'The Restate execution worker manages durable virtual objects. Channel sessions maintain conversation context across cluster restarts. Asynchronous background jobs and agent execution run under supervised lifecycle management.',
        ],
        bullets: [
          'Stateless Ingress: The ingress service boots quickly and scales independently of agent execution workers.',
          'Durable Sessions: Virtual objects retain turn context and message history across cluster restarts.',
          'System-1 Triage: Fast local models filter non-actionable background chatter without waking reasoning engines.',
        ],
      },
      {
        id: 'multi-engine-cli',
        title: 'Multi-Engine CLI Orchestration & Hot-Swapping',
        lead:
          'Hot-swap between Google Antigravity, Claude Code, Codex, and other CLI engines with identical memory and tool interfaces.',
        paragraphs: [
          'Aerial prevents lock-in to specific model providers or proprietary agent harnesses. The execution runtime abstracts CLI agents through a unified runner interface. Supported engines receive identical tools, skills, rules, and memory context.',
          'Configuration files declare the default reasoning engine and models. Channel policies configure wake triggers, reasoning effort levels, and ambient classification thresholds.',
        ],
        bullets: [
          'Engine Agnostic: Standardized process orchestration, stream handling, and pre-tool interception.',
          'Declarative Configuration: Configure active engines and default models in configuration files.',
          'Subprocess Management: Agent CLI processes run in scoped process groups inside worker container boundaries.',
        ],
        codeSnippets: [
          {
            language: 'yaml',
            caption: 'Engine and channel configuration in config.yaml',
            code: `# Configure active default reasoning model and CLI engine
model: gemini-3.8-flash
engine: agy

# Channels require a default policy and optional per-channel policies
channels:
  default:
    mode: threads
    wake_mode: mention
  aerial-dev:
    mode: channel
    wake_mode: all
    effort: high`,
          },
        ],
      },
      {
        id: 'coven-memory',
        title: 'Hybrid RRF + ACT-R Memory & Skill Synthesis',
        lead:
          'Long-term semantic memory combines PostgreSQL full-text search and vector embeddings with human-like cognitive activation decay.',
        paragraphs: [
          'The cognitive memory service provides durable memory and procedural learning. Vector search combines with PostgreSQL full-text search through Reciprocal Rank Fusion. This hybrid retrieval prevents semantic drift.',
          'Stored memories decay over time using ACT-R cognitive activation formulas. Frequently and recently accessed facts remain prominent, while transient details fade. Background processes analyze completed execution transcripts to synthesize reusable skills.',
        ],
        bullets: [
          'Hybrid RRF Search: Combines exact keyword matching with semantic embeddings for precise retrieval.',
          'ACT-R Activation Decay: Models memory strength mathematically based on frequency and recency of use.',
          'Skill Synthesis: Distills successful multi-step debugging workflows into permanent runbook files.',
        ],
      },
      {
        id: 'multimodal-io',
        title: 'Persistent Scheduling, Discord, Voice, PSTN & Browser Automation',
        lead:
          'Full duplex interaction spans Discord, real-time voice, outbound telephone calls, persistent cron schedules, and headless web browsing.',
        paragraphs: [
          'Aerial interacts across physical and digital modalities. The persistent scheduler manages recurring cron schedules, one-shot reminders, silent heartbeat monitors, and event watches across container restarts.',
          'Real-time voice runs through a CUDA speech service hosting neural speech recognition and speech synthesis models. Outbound telephone calls execute through a PSTN telephony service, while headless browser automation handles web tasks.',
        ],
        bullets: [
          'Persistent Scheduler: Provides durable scheduling tools for recurring jobs, silent polling, and event watches.',
          'Real-Time Voice: Delivers low-latency speech recognition and voice synthesis with fast response times.',
          'Telephony Service: Executes outbound and inbound telephone calls for appointments, reservations, and inquiries.',
          'Browser Automation: Automates headless browser tasks with session hygiene and stealth interaction.',
        ],
      },
    ],
  },
  {
    id: 'security-guardrails',
    number: 6,
    title: 'Security & Defense-in-Depth Guardrails',
    summary:
      'Pre-tool repository protection, child-process secret scrubbing, compiled Go hook-guard interceptors, multi-layer telemetry redaction, and administrator privilege enforcement.',
    sections: [
      {
        id: 'filesystem-immutability',
        title: 'Repository Integrity & Ephemeral Scratch Workspaces',
        lead:
          'Base repositories remain protected from direct mutation; all agent modifications occur inside isolated ephemeral scratch workspaces.',
        paragraphs: [
          'Granting autonomous AI agents direct write access to primary codebases creates substantial risk. Erroneous commands or hallucinatory actions could corrupt git histories, delete files, or overwrite configurations.',
          'Aerial prohibits agents from mutating base repository mounts directly. A compiled pre-tool hook blocks destructive commands targeting base directories. Agents must initialize scratch workspaces, test modifications in isolation, and submit pull requests through GitOps workflows.',
        ],
        bullets: [
          'Pre-Tool Enforcement: The hook-guard binary intercepts and denies direct writes to base mounts.',
          'Scratch Isolation: Every build and task executes in a dedicated scratch directory.',
          'GitOps Boundary: Changes reach production strictly through reviewed and verified pull requests.',
        ],
        codeSnippets: [
          {
            language: 'bash',
            caption: 'Scratch workspace initialization pattern',
            code: `# Initialize an isolated scratch workspace for safe code modification
/share/aerial/scripts/aerial-pr.sh init aerial

# In-place modifications to /share/ are intercepted and denied by hook-guard`,
          },
        ],
      },
      {
        id: 'credential-sanitization',
        title: 'Zero Plaintext Tokens & Multi-Layer Redaction',
        lead:
          'Credentials never touch disk in plaintext, while outbound log streams, trace spans, and chat messages undergo automated redaction.',
        paragraphs: [
          'Aerial enforces a Zero Plaintext Token Invariant. Authentication tokens never persist into git configuration files on disk. Git authentication uses ephemeral in-memory HTTP basic auth headers generated for network calls.',
          'The runner scrubs sensitive environment variables from child processes before spawning agent CLI workers. This scrubbing prevents subprocesses and tools from inheriting database credentials, host tokens, or API keys.',
          'Telemetry pipelines sanitize outbound logs and traces using registered secret values. The tracing collector masks raw tokens and encoded variants, while the logging pipeline redacts sensitive JSON fields and private keys.',
        ],
        bullets: [
          'Zero Disk Tokens: No authentication tokens persist in git configs, shell histories, or workspace files.',
          'Child-Process Secret Scrubbing: Removes API keys, database credentials, and secret tokens from child environments.',
          'Trace Span Masking: Sanitizes sensitive credentials and encoded variants across OpenTelemetry trace spans.',
          'Log Pipeline Filtering: Redacts raw tokens, escaped strings, and private key blocks before writing logs.',
          'Redaction Bundle: Mounts shared secrets in memory to populate pipeline redaction targets dynamically.',
        ],
      },
      {
        id: 'hook-guard',
        title: 'Compiled Go hook-guard Pre-Tool Interception',
        lead:
          'A compiled Go binary intercepts every tool call before execution to enforce operational invariants and block hazardous commands.',
        paragraphs: [
          'Prompt instructions alone cannot guarantee safe agent operations. Aerial compiles a native Go pre-tool hook binary into execution environments. The hook intercepts and inspects every command before execution.',
          'The hook deterministically rejects prohibited actions before commands run. Prohibited actions include ephemeral CLI scheduling tools that bypass durable storage, dangerous filesystem mutations, and attempts to read credential stores.',
        ],
        bullets: [
          'Deterministic Interception: A compiled Go binary executes prior to every agent tool call.',
          'Prohibited Tool Blocking: Enforces persistent scheduling invariants and blocks ephemeral background timers.',
          'Parameter Sanitization: Adjusts command timeouts and flags to maintain system responsiveness.',
        ],
      },
      {
        id: 'rbac-privilege',
        title: 'Admin RBAC & Privilege Boundary Enforcement',
        lead:
          'Multi-user message authentication validates administrator privileges before executing sensitive operations.',
        paragraphs: [
          'In multi-user chat channels, unauthorized users must not trigger system updates, restart containers, or modify configuration files.',
          'Aerial validates administrator status against the configured administrator list and injects a verified privilege flag into the agent context. Security rules require agents to restrict sensitive operations to verified administrators while rejecting unauthorized requests.',
        ],
        bullets: [
          'Role-Based Context: Inbound message turns carry a verified privilege flag from configuration lists.',
          'Security Rule Enforcement: Instructions direct agents to reject sensitive modifications from non-admin users.',
          'Polite Rejection: Unauthorized privileged attempts receive succinct rejections without executing requested changes.',
        ],
      },
    ],
  },
  {
    id: 'customization-day2',
    number: 7,
    title: 'Customization & Day-2 Operations',
    summary:
      'Authoring skill runbooks, registering Streamable HTTP MCP servers, configuring channel lifecycle webhooks, and integrating homelab workloads.',
    sections: [
      {
        id: 'authoring-skills',
        title: 'Authoring Progressive Disclosure SKILL.md Runbooks',
        lead:
          'Extend platform capabilities with custom skills structured with YAML frontmatter and progressive disclosure.',
        paragraphs: [
          'Skills are modular directories containing instructions, reference documentation, and automation scripts. They teach agents how to interact with specialized domains, external web portals, and private APIs.',
          'Progressive disclosure keeps prompt tokens minimal. Skills declare names and descriptions in frontmatter. Full skill instructions and reference materials load into context only when descriptions match the active request.',
        ],
        bullets: [
          'YAML Frontmatter: Defines name and description metadata to indicate when skills activate.',
          'Progressive Disclosure: Heavy instructions and scripts remain on disk until explicitly needed.',
          'Custom Skills Directory: User skills reside in the configuration repository and override built-in skills.',
        ],
        codeSnippets: [
          {
            language: 'markdown',
            caption: 'Sample SKILL.md structure with progressive disclosure',
            code: `---
name: homelab-backup
description: Run hermetic offsite backups of CloudNativePG and Restate journals to Wasabi S3. Use when user asks to backup cluster or during weekly maintenance.
---

# Homelab Backup Runbook

## Verification Procedure
1. Verify CloudNativePG backup status:
   kubectl cnpg status postgres-19
2. Trigger on-demand backup:
   kubectl cnpg backup postgres-19`,
          },
        ],
      },
      {
        id: 'mcp-servers',
        title: 'Registering Streamable HTTP MCP Servers',
        lead:
          'Integrate third-party tools, hardware bridges, and external APIs using Model Context Protocol streamable servers.',
        paragraphs: [
          'Aerial uses Model Context Protocol to provide agents with typed, schema-validated tool interfaces. Users register custom Streamable HTTP MCP servers in configuration files under target categories.',
          'Agents discover tools lazily through standard schema endpoints. This mechanism enables agents to call calendar services, external web APIs, and cluster monitors without modifying application code.',
        ],
        bullets: [
          'Streamable HTTP: Provides tool invocations over standard HTTP connections.',
          'Declarative Registration: Add servers to configuration files under common or scoped categories.',
          'Automatic Schema Generation: Tool definitions and argument validation schemas generate dynamically.',
        ],
        codeSnippets: [
          {
            language: 'yaml',
            caption: 'Registering custom MCP server in config.yaml',
            code: `mcp_servers:
  common:
    google-workspace:
      serverUrl: http://google-mcp.aerial/mcp
    custom-api:
      serverUrl: http://api-bridge.aerial/mcp`,
          },
        ],
      },
      {
        id: 'channel-webhooks',
        title: 'Channel Lifecycle Webhooks (on_wake, pre_turn, post_turn)',
        lead:
          'Inspect, gate, and enrich agent turns dynamically through external HTTP lifecycle webhooks.',
        paragraphs: [
          'Aerial supports lifecycle hooks that enable external services to participate in conversational turns. Channels configure lifecycle HTTP endpoints for wake decisions, turn preparation, and post-turn notifications.',
          'The wake hook determines whether inbound messages should wake agents. The pre-turn hook injects real-time context directly into agent prompts. The post-turn hook receives execution metadata and token counts for external telemetry.',
        ],
        bullets: [
          'on_wake: Overrides ambient classification decisions to wake, drop, or classify turns.',
          'pre_turn: Injects dynamic context or applies programmatic rate limits.',
          'post_turn: Dispatches notifications for external telemetry, billing, or audit pipelines.',
        ],
      },
      {
        id: 'homelab-workloads',
        title: 'Integrating External APIs & Custom Homelab Workloads',
        lead:
          'Connect external webhook event watches, third-party APIs, and custom microservices to extend your self-hosted platform.',
        paragraphs: [
          'Aerial coordinates everyday personal workflows alongside custom homelab applications. Agents monitor external APIs, automate browser errands, manage calendars, and supervise custom cluster workloads.',
          'Persistent event watches trigger agent turns when external events occur. Examples include tracking package or permit updates, processing inbound webhooks, and running scheduled maintenance.',
        ],
        bullets: [
          'External API Connectors: Queries and mutates third-party services through custom Model Context Protocol servers.',
          'Browser & Phone Errands: Automates multi-step web flows and outbound telephone calls with persistent session state.',
          'Event-Driven Routines: Executes deterministic workflows in response to inbound webhooks and scheduled heartbeats.',
        ],
        callout: {
          type: 'tip',
          title: 'Day-2 Operations Best Practice',
          text:
            'Deploy custom microservices into the default namespace with gateway routes to ensure immediate reachability across the internal service mesh.',
        },
      },
    ],
  },
];

/**
 * Filter chapters and sections based on a client-side search query.
 * Matches chapter title, summary, number, or section contents (title, lead, paragraphs, bullets, code, callout).
 * Returns matching chapters with filtered sections if matched at section level,
 * or full chapters if matched at chapter level.
 */
export function filterChapters(
  chapters: readonly HandbookChapter[],
  query: string
): readonly HandbookChapter[] {
  const q = query.trim().toLowerCase();
  if (!q) {
    return chapters;
  }

  const chapterNumMatch = q.match(/^(?:chapter|ch\.?)\s*(\d+)$/i);
  const targetChapterNumber = chapterNumMatch ? parseInt(chapterNumMatch[1]!, 10) : null;

  return chapters
    .map((chapter) => {
      const titleMatch = chapter.title.toLowerCase().includes(q);
      const summaryMatch = chapter.summary.toLowerCase().includes(q);
      const numberMatch =
        targetChapterNumber !== null && chapter.number === targetChapterNumber;

      const matchingSections = chapter.sections.filter((sec) => {
        if (sec.title.toLowerCase().includes(q)) return true;
        if (sec.lead && sec.lead.toLowerCase().includes(q)) return true;
        if (sec.paragraphs.some((p) => p.toLowerCase().includes(q))) return true;
        if (sec.bullets && sec.bullets.some((b) => b.toLowerCase().includes(q))) return true;
        if (
          sec.codeSnippets &&
          sec.codeSnippets.some(
            (c) =>
              c.code.toLowerCase().includes(q) ||
              (c.caption && c.caption.toLowerCase().includes(q))
          )
        ) {
          return true;
        }
        if (
          sec.callout &&
          (sec.callout.title.toLowerCase().includes(q) ||
            sec.callout.text.toLowerCase().includes(q))
        ) {
          return true;
        }
        return false;
      });

      if (titleMatch || summaryMatch || numberMatch) {
        return chapter;
      }

      if (matchingSections.length > 0) {
        return {
          ...chapter,
          sections: matchingSections,
        };
      }

      return null;
    })
    .filter((ch): ch is HandbookChapter => ch !== null);
}

/**
 * Resolves a section ID to its containing chapter ID.
 */
export function findChapterForSection(sectionId: string): string | undefined {
  const sId = sectionId.toLowerCase();
  for (const ch of HANDBOOK_CHAPTERS) {
    if (ch.sections.some((s) => s.id.toLowerCase() === sId)) {
      return ch.id;
    }
  }
  return undefined;
}
