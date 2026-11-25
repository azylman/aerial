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
          'Open-loop chatbots generate text that silently rots. Autonomous software engineering requires real-world feedback loops: hermetic compilation, unit testing, schema verification, and automated deployment.',
        paragraphs: [
          'Conventional AI coding assistants operate in an open loop: a language model generates candidate code, leaves it in an editor buffer or chat transcript, and leaves compilation, testing, dependency management, and deployment entirely to human engineers. Without real-world execution feedback, hallucinated APIs, broken imports, and subtle runtime regressions compound rapidly.',
          'Aerial is built on a closed-loop thesis: autonomous engineering cannot succeed without direct, unassisted feedback from compilation toolchains (Bazel, Go, Rust, TypeScript), hermetic test runners, database migration verifiers, and production telemetry. When an agent proposes a change, the proposal must build cleanly, pass unit and integration test gates, undergo multi-agent code review, and merge through event-driven GitOps pipelines.',
        ],
        bullets: [
          'Hermetic Verification: Every code change compiles and passes tests before a PR is opened or merged.',
          'Declarative Schema Evolution: Application databases update via declarative migrations verified against temporary database containers.',
          'Automated Self-Healing: In-cluster deployment failures or CI check breakages wake agent remediation loops automatically.',
          'Production-Grade Tooling: Real compiler toolchains, package managers, and container runtimes rather than simulated mock environments.',
        ],
        codeSnippets: [
          {
            language: 'bash',
            caption: 'Hermetic staged verification workflow',
            code: `# Run staged verification before any commit or PR creation
./scripts/verify.sh --staged

# Execute full package test suite with race detector and coverage
bazel test //services/... //pkg/...`,
          },
        ],
        callout: {
          type: 'important',
          title: 'The Closed-Loop Invariant',
          text:
            'In Aerial, code is never committed or shipped on assertion alone. Every change must be validated by compiler diagnostics and hermetic test execution before opening a pull request.',
        },
      },
      {
        id: 'autonomous-workflows',
        title: 'Real-World Autonomous Workflows',
        lead:
          'Aerial moves beyond single-file autocompletion to multi-PR cross-repository DAG orchestration, tiered complexity reviews, and automated backlog processing.',
        paragraphs: [
          'Real software systems do not live in single isolated files. Meaningful engineering work routinely spans multiple repositories, database schema migrations, and sequential rollouts. Introducing a new feature often requires shipping an upstream service API or database migration, waiting for deployment, and only then enabling downstream configurations in consumer repositories.',
          'Aerial introduces Vanadis, a distributed multi-chunk build orchestrator backed by Restate. Vanadis enables agents to author, validate, and execute complex multi-repository DAG rollouts without keeping heavy, expensive language model context windows open in foreground polling loops.',
        ],
        bullets: [
          'Task DAG Queue: Work is structured in Vikunja task graphs and queued through docket-mcp based on dependency readiness.',
          'Multi-Chunk Builds: Features spanning upstream engine and downstream user configuration are enqueued as cohesive multi-chunk Vanadis plans.',
          'Multi-Agent Review: Tiered review panels (the Girl Gang) inspect plans and diffs across architecture, security, and edge-case angles.',
          'Autonomous Remediation: Green CI checks merge automatically, while test failures immediately wake targeted remediation agents with exact failure logs.',
        ],
        callout: {
          type: 'tip',
          title: 'Two-Step Feature Rule',
          text:
            'When introducing a change across multiple repositories, upstream services ship schema fields and endpoints first; downstream configuration repositories enable features only after upstream merges succeed.',
        },
      },
      {
        id: 'glass-box-operations',
        title: 'Glass-Box Operations & Observability',
        lead:
          'Aerial rejects black-box SaaS agent clouds. Every turn, tool invocation, token count, and runtime trace is transparently observable in real time on your own infrastructure.',
        paragraphs: [
          'Proprietary agent platforms hide prompt assembly, intermediate tool steps, and failure traces behind opaque web portals. When an agent hallucinates, enters an infinite loop, or takes an unexpected action, the user has zero diagnostic visibility.',
          'Aerial implements glass-box operations. Every conversational turn, tool execution, subprocess invocation, and background task outputs structured telemetry into a self-hosted observability stack comprising VictoriaMetrics, VictoriaLogs, VictoriaTraces, and the AgentsView transcript visualizer.',
        ],
        bullets: [
          'Live Web Dashboard: AgentsView renders real-time conversation transcripts, tool calls, and subagent trees.',
          'Log Retention & Search: VictoriaLogs captures container stdout/stderr with 60-day retention, searchable via high-performance LogsQL.',
          'Distributed Tracing: VictoriaTraces records OpenTelemetry spans across HTTP ingress, Restate virtual objects, and tool invocations.',
          'Metric Scraping: VictoriaMetrics and vm-operator collect CPU, memory, token usage, and latency metrics via Prometheus-compatible scrapers.',
        ],
        codeSnippets: [
          {
            language: 'logsql',
            caption: 'VictoriaLogs LogsQL query for agent tool execution',
            code: `_stream:{service="eri-restate"} AND "tool_call" 
| unpack_json 
| stats by (tool_name) count() as invocations, avg(duration_ms) as avg_latency`,
          },
        ],
      },
      {
        id: 'architecture-philosophy',
        title: 'Architecture Philosophy & Decoupled Repositories',
        lead:
          'Aerial separates generic platform logic from private user customizations through a strict multi-repository architecture and physical filesystem immutability.',
        paragraphs: [
          'A key failure mode of personal assistant projects is entangling private user details—names, home addresses, smart home device IDs, personal API keys—into the core application source code. This prevents the codebase from being open-sourced, shared, or cleanly upgraded.',
          'Aerial enforces a strict two-repository model. The core monorepo (azylman/aerial) contains 100% domain-agnostic, generic infrastructure code, microservices, and base system rules. The user configuration repository (aerial-config) contains private user persona rules, smart home skills, service configurations, and Kubernetes cluster overrides.',
        ],
        bullets: [
          'Domain-Agnostic Core: azylman/aerial has zero personal data, real names, or private credentials.',
          'Private Config Repository: All personal persona rules, household runbooks, and cluster-specific overrides live in a private GitOps repository.',
          'Physical Immutability: Base repositories are mounted strictly read-only (:ro) inside execution containers to prevent tampering.',
          'Ephemeral Scratch Workspaces: Agents perform all modifications in ephemeral scratch clones managed via scripts/aerial-pr.sh.',
        ],
      },
    ],
  },
  {
    id: 'getting-started',
    number: 2,
    title: 'Getting Started & Two-Repo GitOps',
    summary:
      'Single-node k3s bootstrap, aerial vs. aerial-config split, Infisical PushSecret bootstrap, and multi-node scaling via cluster-overrides.yaml.',
    sections: [
      {
        id: 'k3s-bootstrap',
        title: 'Single-Node k3s Bootstrap',
        lead:
          'Aerial runs out-of-the-box on a single vanilla Linux host running k3s with Traefik disabled, binding NGINX Gateway Fabric directly to ports 80 and 443.',
        paragraphs: [
          'To minimize operational overhead, Aerial does not require a complex cloud managed Kubernetes provider. A single commodity PC, homelab server, or cloud VM running Ubuntu 24.04+, Debian 12+, or NixOS is sufficient to run the entire application platform and software factory.',
          'The installation disables Traefik because Aerial uses NGINX Gateway Fabric as its standardized Gateway API implementation for both internal LAN routing and external edge tunnels.',
        ],
        bullets: [
          'Minimum Requirements: 4 vCPU cores, 8 GB RAM, 60 GB NVMe or SSD storage.',
          'Network Ports: Ports 80 and 443 bound on the host interface by NGINX Gateway Fabric.',
          'Standard Tooling: Standard kubectl, Helm, and Flux CLI binaries manage the cluster.',
        ],
        codeSnippets: [
          {
            language: 'bash',
            caption: 'Install single-node k3s with Traefik disabled',
            code: `# Install k3s with Traefik disabled so NGINX Gateway Fabric owns port 80/443
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
          'Fork the configuration template to establish your private configuration repository, customize user parameters, and bootstrap Flux CD.',
        paragraphs: [
          'Aerial uses GitOps as its continuous delivery mechanism. Flux CD reconciles two Kustomization resources: aerial-operators (CRDs, operators, and foundational infrastructure) and aerial-core (platform microservices and databases).',
          'Users fork azylman/aerial-config-example into a private repository (e.g. my-aerial-config). When Flux CD reconciles, it layers the user overrides in aerial-config on top of the upstream aerial base manifests.',
        ],
        bullets: [
          'Zero Plaintext Tokens: GITHUB_PAT is passed in-memory during bootstrap and never written to disk in .git/config.',
          'Declarative Configuration: config.yaml configures model selection, timezone, Discord channels, and admin users.',
          'Automated Sync: Flux continuously watches the repository and reconciles manifest changes within minutes.',
        ],
        codeSnippets: [
          {
            language: 'bash',
            caption: 'Bootstrap Flux CD on your private configuration repository',
            code: `# Set GitHub personal access token in memory
export GITHUB_PAT="your-github-pat"

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
          'Aerial utilizes Infisical and External Secrets Operator for centralized secret management, preventing plaintext secrets from ever touching Git repositories.',
        paragraphs: [
          'All application credentials, Discord bot tokens, and API keys are stored in an in-cluster Infisical instance. External Secrets Operator (ESO) watches Infisical and creates scoped Kubernetes Secrets consumed by microservices.',
          'Bootstrapping Infisical involves creating a Machine Identity, granting it read access to the aerial project, and saving its credentials as a Kubernetes Secret.',
        ],
        bullets: [
          'Universal Auth: Kubernetes workloads authenticate to Infisical via Machine Identity tokens.',
          'Automated Generation: Database passwords, internal webhook secrets, and NATS credentials generate automatically.',
          'Scoped Distribution: Each microservice receives only the environment variables it requires.',
        ],
        codeSnippets: [
          {
            language: 'bash',
            caption: 'Bootstrap Infisical and store credentials',
            code: `# 1. Create Machine Identity Token Auth credential
export INFISICAL_TOKEN="$(infisical bootstrap \\
  --domain http://<cluster-ip> \\
  --email admin@example.com \\
  --password 'your-master-password' \\
  --organization aerial \\
  | jq -r '.identity.credentials.token')"

# 2. Store identity token in Kubernetes
kubectl create secret generic infisical-universal-auth \\
  --from-literal=INFISICAL_TOKEN="$INFISICAL_TOKEN"

# 3. Populate Discord Bot Token and GitHub PAT
infisical secrets set --env=default \\
  DISCORD_BOT_TOKEN="your-discord-token" \\
  GITHUB_PAT="$GITHUB_PAT"`,
          },
        ],
      },
      {
        id: 'multi-node-scaling',
        title: 'Scaling to Multi-Node via cluster-overrides.yaml',
        lead:
          'Expand your cluster across specialized storage, compute, and kiosk nodes without modifying upstream base manifests.',
        paragraphs: [
          'As your hardware grows, Aerial scales seamlessly to heterogeneous multi-node clusters. Nodes can be assigned distinct roles: storage-tier NVMe nodes for databases and persistent queues, GPU compute nodes for local speech and LLM inference, and lightweight touch kiosk displays.',
          'All cluster-specific node selectors, tolerations, replica counts, and storage classes are defined in aerial-config/k8s/cluster-overrides.yaml. Upstream base manifests remain completely hardware-agnostic.',
        ],
        bullets: [
          'Storage Tier: Pin CloudNativePG, NATS JetStream, and Restate RocksDB to high-speed NVMe storage.',
          'Compute Tier: Route local ONNX speech recognition (Parakeet), TTS (Kokoro), and embedding models to NVIDIA GPU nodes.',
          'Prune Protection: Protect persistent volumes and custom runner scale sets during GitOps reconciliation using kustomize.toolkit.fluxcd.io/prune: disabled.',
        ],
        callout: {
          type: 'tip',
          title: 'Zero-Downtime Resource Adoption',
          text:
            'When moving resources from base manifests into cluster overrides, set metadata.annotations["kustomize.toolkit.fluxcd.io/prune"] = "disabled" to prevent Flux kustomize-controller from pruning active pods or storage during cutovers.',
        },
      },
    ],
  },
  {
    id: 'application-platform',
    number: 3,
    title: 'The Self-Hosted Application Platform',
    summary:
      'NGINX Gateway Fabric LAN/edge gateways, CoreDNS *.aerial service mesh, CloudNativePG PostgreSQL 19 + pgvector, Restate durable execution, NATS JetStream streams, Infisical + External Secrets Operator, and VictoriaMetrics/Logs/Traces + OTLP redaction.',
    sections: [
      {
        id: 'ingress-mesh',
        title: 'NGINX Gateway Fabric & CoreDNS Service Mesh',
        lead:
          'A unified Kubernetes Gateway API implementation and internal DNS rewrite mesh provide zero-configuration service discovery.',
        paragraphs: [
          'Aerial employs NGINX Gateway Fabric to implement Kubernetes Gateway API HTTPRoute specifications. Two distinct gateway classes isolate traffic: the LAN Gateway (binding ports 80/443 on the local network for console, documentation, and agent views) and the Edge Gateway (exposing port 4021 for secure tunnel webhooks from Discord, GitHub, and external services).',
          'CoreDNS is configured with internal rewrite rules that map short *.aerial hostnames directly to cluster services (e.g. coven.aerial -> coven.default.svc.cluster.local), eliminating hardcoded IP addresses or external DNS lookups for internal microservice communication.',
        ],
        bullets: [
          'LAN Gateway: High-performance local routing for dashboards, REST APIs, and SSE event streams.',
          'Edge Gateway: Strict webhook receiver gateway with TLS termination and IP filtering.',
          'Internal Mesh: Automatic *.aerial DNS resolution across all pods and agent CLI processes.',
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
      name regex (.*)\\.aerial {1}.default.svc.cluster.local
      answer auto
    }`,
          },
        ],
      },
      {
        id: 'state-persistence',
        title: 'CloudNativePG PostgreSQL 19 & Restate Durable Execution',
        lead:
          'Relational data, vector embeddings, and long-running distributed workflows run on high-availability PostgreSQL 19 and Restate.',
        paragraphs: [
          'Persistent relational data is managed by CloudNativePG, deploying a high-availability PostgreSQL 19 cluster with pgvector support. Aerial enforces a strict Single-Service Table Ownership invariant: each database table is owned and queried by exactly one microservice, preventing spaghetti cross-service SQL joins.',
          'For stateful distributed agent workflows, Aerial runs Restate backed by RocksDB. Restate provides durable execution: turns, multi-chunk builds, and schedule evaluations are modelled as durable virtual objects that survive pod crashes, node drains, and network timeouts with automatic replay.',
        ],
        bullets: [
          'CloudNativePG: Automated failover, rolling updates, WAL archiving, and pgvector semantic indexing.',
          'Single Table Ownership: Microservices interact via typed HTTP and MCP APIs rather than shared SQL tables.',
          'Restate Durable Workflows: ThreadSession, BuildRouter, and HaroSchedule virtual objects execute with deterministic replay.',
          'Crash-Resilient State: Long-running agent reasoning turns resume exactly where they suspended without duplicate tool calls.',
        ],
        callout: {
          type: 'note',
          title: 'Durable Execution Guarantee',
          text:
            'If an agent turn experiences a node reboot or container restart mid-execution, Restate re-hydrates the exact step from its RocksDB journal without re-executing completed side-effecting tool calls.',
        },
      },
      {
        id: 'event-streaming',
        title: 'NATS JetStream Distributed Event Backbone',
        lead:
          'Durable event streaming with consumer acknowledgments connects decoupled microservices and reactive wakes.',
        paragraphs: [
          'Microservices communicate asynchronously through an in-cluster NATS 2 JetStream cluster. Durable message streams decouple event producers (such as Discord Gateway receivers and camera vision detectors) from consumers (agent dispatchers and transcription indexers).',
          'Key streams include AERIAL_EVENTS (system-wide event notices), AERIAL_JOBS (asynchronous task dispatches), AERIAL_COALESCED (debounced telemetry events), and AERIAL_TRANSCRIPTS (persisted agent execution logs).',
        ],
        bullets: [
          'At-Least-Once Delivery: JetStream guarantees message delivery with consumer sequence tracking.',
          'Stream Deduplication: High-volume edge webhooks are coalesced to prevent LLM reasoning storms.',
          'Stateful Set Reliability: NATS runs on persistent NVMe storage with zero data loss across upgrades.',
        ],
      },
      {
        id: 'observability-stack',
        title: 'VictoriaMetrics, VictoriaLogs & OpenTelemetry Tracing',
        lead:
          'A complete, self-contained telemetry platform stores metrics, logs, and distributed traces with automatic credential redaction.',
        paragraphs: [
          'Observability is native to the platform. VictoriaMetrics stores Prometheus-compatible metrics scraped via vm-operator. VictoriaLogs ingests all node and container stdout/stderr through Fluent Bit with 60-day local retention.',
          'Distributed tracing is provided by an OpenTelemetry Collector routing to VictoriaTraces. Every span passes through an in-memory redaction processor that strips sensitive authentication tokens (GitHub PATs, Discord secrets, bearer tokens) across plaintext, URL-encoded, base64, and hexadecimal representations before persisting.',
        ],
        bullets: [
          'Zero-Cloud Telemetry: 100% self-hosted monitoring with zero recurring SaaS costs.',
          'LogsQL Search: Sub-second regex and facet search across millions of historical log lines.',
          'Automatic Redaction: Sensitive credentials sanitized in-flight across all trace spans and log lines.',
        ],
      },
    ],
  },
  {
    id: 'software-factory',
    number: 4,
    title: 'The Autonomous Software Factory',
    summary:
      'Vikunja + docket-mcp DAG ready queue, tiered complexity & multi-agent review, Vanadis + vanadis-restate multi-PR DAG orchestration, GitHub Actions CI & automated code review with optional self-hosted ARC runners, Ariga Atlas declarative migrations, and synapse + Flux CD event-driven push GitOps with self-healing wakes.',
    sections: [
      {
        id: 'docket-queue',
        title: 'Vikunja & docket-mcp DAG Ready Queue',
        lead:
          'Aerial organizes autonomous engineering work as a directed acyclic graph (DAG) of tasks tracked in Vikunja and manipulated via docket-mcp.',
        paragraphs: [
          'Autonomous software engineering cannot rely on chaotic, unstructured chat prompts. To execute large features and maintenance sweeps, Aerial structures work as a task graph inside an in-cluster Vikunja instance exposed to agents via docket-mcp.',
          'Tasks define explicit blocker relationships. The ready queue query surfaces only tasks whose prerequisite dependencies have fully resolved, allowing agents to work through complex software roadmaps deterministically without getting stuck on unready prerequisites.',
        ],
        bullets: [
          'docket-mcp Tools: ready, list, show, create, update, link, comment, and close.',
          'Token Exhaustion Parking: When an agent approaches context limits, unfinished sub-goals are persisted to Docket rather than lost.',
          'DAG Dependencies: Parent tasks automatically unblock child tasks upon completion.',
        ],
        codeSnippets: [
          {
            language: 'json',
            caption: 'docket-mcp ready tool query',
            code: `{
  "tool": "ready",
  "arguments": {
    "project_id": 1,
    "limit": 5
  }
}`,
          },
        ],
      },
      {
        id: 'tiered-complexity',
        title: 'Tiered Complexity & Multi-Agent Review Panel',
        lead:
          'Engineering tasks are classified into distinct tiers with rigorous multi-agent plan and diff reviews for non-trivial modifications.',
        paragraphs: [
          'Aerial prevents reckless code modifications by enforcing a strict tiering rubric: Tier 0 (trivial configuration and documentation tweaks ≤ 5 LOC), Tier 1 (targeted single-package bugfixes and features), Tier 2 (cross-package refactors), and Tier 3 (architectural changes and schema migrations).',
          'Tier 1 and above require review by the Girl Gang—a panel of specialized subagents evaluating the plan and diff from distinct angles: Architecture & Contracts, Security & Invariants, Test Coverage & Edge Cases, and Operational Simplicity.',
        ],
        bullets: [
          'Tier 0: Direct execution with verify.sh validation (single PR).',
          'Tier 1: Girl Gang plan review + verification before commit.',
          'Tier 2-3: Formal multi-agent review panel + multi-chunk Vanadis build.',
          'Dynamic Scope Re-Tiering: If unexpected complexity is uncovered during execution, the task is immediately promoted to a higher tier.',
        ],
        callout: {
          type: 'important',
          title: 'The Girl Gang Moniker',
          text:
            'The subagent review panel is formally designated as the Girl Gang (or the gang), providing independent, adversarial review passes before code is merged.',
        },
      },
      {
        id: 'vanadis-orchestration',
        title: 'Vanadis Multi-PR Cross-Repository Orchestration',
        lead:
          'Vanadis coordinates multi-repository rollouts, dependency ordering, and atomic progress tracking without holding active agent sessions open.',
        paragraphs: [
          'When a feature spans multiple repositories (e.g. implementing an endpoint in aerial and enabling its configuration in aerial-config), executing it sequentially in a single turn is impossible or risks token exhaustion.',
          'Vanadis manages builds comprising multiple chunks. Each chunk targets a specific repository, defines dependencies on prior chunks, and includes instructions and target symbols. When a chunk completes and merges, Vanadis automatically triggers the next dependent chunk.',
        ],
        bullets: [
          'Multi-Repo Builds: Coordinated changes across aerial, aerial-config, mirrormere, and aerial-sidecars.',
          'Non-Blocking Execution: Turns end immediately upon enqueueing or submitting, freeing up execution agents.',
          'Chunk Amending: If requirements change, pending chunks can be amended or new corrective chunks appended mid-flight.',
        ],
        codeSnippets: [
          {
            language: 'bash',
            caption: 'Enqueue a multi-chunk Vanadis build plan',
            code: `# Enqueue a multi-chunk build plan across repositories
scripts/aerial-build.sh enqueue /path/to/build-plan.json

# Check live build status
scripts/aerial-build.sh status <build-id>`,
          },
        ],
      },
      {
        id: 'ci-cd-gitops',
        title: 'Declarative Migrations, Hybrid CI & Event-Driven GitOps',
        lead:
          'Ariga Atlas declarative migrations, dynamic runner routing in GitHub Actions, and Synapse event-driven GitOps complete the factory loop.',
        paragraphs: [
          'Database schemas are defined declaratively in schema.sql. Imperative schema mutations (such as runtime table or column alterations in startup code) are strictly forbidden. Ariga Atlas computes migration diffs hermetically against a temporary PostgreSQL container during verify.sh.',
          'Continuous Integration uses dynamic runs-on routing: internal runs on azylman/aerial leverage high-speed local Actions Runner Controller (ARC) self-hosted runners, while external fork PRs and public users fall back to GitHub-hosted ubuntu-latest runners using the setup-toolchain composite hydration action.',
          'Once CI passes, Synapse receives GitHub push webhooks, notifies Flux CD via its webhook receiver, and triggers immediate cluster reconciliation. If CI or RoboRev review fails, Synapse automatically dispatches a remediation wake to the agent.',
        ],
        bullets: [
          'Ariga Atlas: Declarative schema diffing with zero manual migration SQL errors.',
          'Dynamic Runner Routing: Blazing fast ARC runners for maintainers; zero-cost ubuntu-latest fallback for forks.',
          'Event-Driven Push GitOps: Flux syncs within seconds of merge via webhook receivers.',
          'Self-Healing CI Wakes: Build failures trigger proactive remediation turns with full log context.',
        ],
      },
    ],
  },
  {
    id: 'multimodal-agent',
    number: 5,
    title: 'The Multimodal AI Agent',
    summary:
      'Eri stateless front door & eri-restate durable sessions, multi-engine CLI execution, coven hybrid RRF + ACT-R memory & procedural skill synthesis, haro persistent crons/heartbeats/event watches, and Discord, real-time voice, prospera PSTN, and kernel-mcp browser automation.',
    sections: [
      {
        id: 'stateless-ingress',
        title: 'Eri Stateless Ingress & Durable Restate Sessions',
        lead:
          'Eri provides a lightweight, stateless HTTP ingress front door paired with durable, fault-tolerant Restate execution sessions.',
        paragraphs: [
          'User interaction begins at Eri (:8080), a stateless Go microservice that handles incoming Discord messages, interactions, voice queries, and system prompts. Eri performs lightweight token authentication, rate limiting, and System-1 ambient triage before delegating heavy reasoning to Eri-Restate.',
          'Eri-Restate executes durable Restate virtual objects: ThreadSession manages Discord channel threads, DispatchSession coordinates asynchronous background jobs, and SessionManager lifecycle manages execution agents.',
        ],
        bullets: [
          'Stateless Ingress: Eri boots in milliseconds and scales independently of execution agents.',
          'Durable Sessions: ThreadSession retains turn context and message history across cluster restarts.',
          'System-1 Ambient Triage: Fast local LLM evaluation filters out non-actionable background chatter without waking heavy reasoning models.',
        ],
      },
      {
        id: 'multi-engine-cli',
        title: 'Multi-Engine CLI Orchestration & Hot-Swapping',
        lead:
          'Hot-swap between Google Antigravity (agy), Claude Code, Codex, and Muse CLI engines with identical memory and tool interfaces.',
        paragraphs: [
          'Aerial is not locked into a single proprietary model provider or agent CLI harness. The execution runtime abstracts CLI agents through a unified TurnRunner interface. Whether running Google Antigravity (agy), Anthropic Claude Code, OpenAI Codex, or local open-weights engines, all agents receive identical tools, skills, rules, and memory context.',
          'Engines can be dynamically switched per channel or even per task based on quota, speed, and reasoning depth requirements.',
        ],
        bullets: [
          'Engine Agnostic: Standardized process orchestration, I/O streaming, and pre-tool interception.',
          'Dynamic Switching: Seamlessly swap engines in config.yaml without altering skills or prompt rules.',
          'Process Isolation: Agent CLI processes execute in isolated containers with strict resource quotas.',
        ],
        codeSnippets: [
          {
            language: 'yaml',
            caption: 'Engine configuration in config.yaml',
            code: `# Configure active default reasoning model and CLI engine
model: gemini-3.8-flash
engine: agy

# Per-channel overrides
channels:
  aerial-dev:
    engine: agy
    wake_mode: all
  general:
    engine: claude
    wake_mode: mention`,
          },
        ],
      },
      {
        id: 'coven-memory',
        title: 'Coven Hybrid RRF + ACT-R Memory & Skill Synthesis',
        lead:
          'Long-term semantic memory combining keyword BM25 and vector embeddings with human-like cognitive activation decay.',
        paragraphs: [
          'Coven is Aerial’s cognitive memory and procedural learning service. Unlike naive vector databases that suffer from semantic drift, Coven uses Reciprocal Rank Fusion (RRF) combining BM25 full-text keyword search and pgvector cosine similarity.',
          'Memory items decay over time following cognitive ACT-R activation formulas: frequently retrieved and recently accessed facts remain prominent, while obsolete or transient details naturally fade. Furthermore, Coven periodically analyzes completed execution transcripts to synthesize reusable procedural skills.',
        ],
        bullets: [
          'Hybrid RRF Search: Combines exact keyword matches with semantic embeddings for superior retrieval precision.',
          'ACT-R Activation Decay: Mathematically models memory strength based on frequency and recency of use.',
          'Skill Synthesis: Successful multi-step debugging workflows are distilled into permanent SKILL.md runbooks.',
        ],
      },
      {
        id: 'multimodal-io',
        title: 'Haro Scheduling, Discord, Voice, PSTN & Browser Automation',
        lead:
          'Full duplex interaction across Discord, real-time voice, outbound phone calls, persistent cron schedules, and stealth web browsing.',
        paragraphs: [
          'Aerial interacts across physical and digital modalities. Haro provides persistent scheduling surviving container restarts (recurring crons, one-shot reminders, silent heartbeat monitors, and event watches).',
          'Voice interaction runs through Senses, a Rust CUDA ONNX microservice hosting NeMo Parakeet speech recognition and Kokoro text-to-speech. Outbound PSTN telephone calls and SMS are handled by Prospera, while Kernel provides stealth headless Chromium browser automation for web tasks.',
        ],
        bullets: [
          'Haro Scheduler: Persistent, durable scheduling tools (schedule_recurring, schedule_heartbeat, watch_event).',
          'Real-Time Voice: Low-latency CUDA speech-to-text and voice synthesis with sub-500ms response times.',
          'Prospera Telephony: Automated outbound voice calls and SMS for real-world appointments and inquiries.',
          'Kernel Browser: Headless browser automation with CDP hygiene and stealth execution.',
        ],
      },
    ],
  },
  {
    id: 'security-guardrails',
    number: 6,
    title: 'Security & Defense-in-Depth Guardrails',
    summary:
      'Read-only repository mounts, child-process secret scrubbing, compiled Go hook-guard pre-tool interceptors, multi-encoding log/span/Discord secret redaction, and admin RBAC.',
    sections: [
      {
        id: 'filesystem-immutability',
        title: 'Physical Immutability & Ephemeral Scratch Workspaces',
        lead:
          'Base repositories and credential volumes mount read-only into execution pods; all agent work occurs in ephemeral scratch workspaces.',
        paragraphs: [
          'Granting an autonomous AI agent unrestricted write access to your primary codebase or server filesystem is an enormous security hazard. A buggy prompt or hallucinatory command could delete files, corrupt git history, or overwrite critical configuration.',
          'In Aerial, production repository volumes (/share/aerial and /share/aerial-config) are mounted strictly read-only (:ro). Local git mutations or file writes directly to /share/* fail with EROFS (Read-only file system). Agents must clone isolated scratch workspaces via scripts/aerial-pr.sh, test changes in isolation, and submit PRs through standard GitOps pull request workflows.',
        ],
        bullets: [
          'Physical Kernel Immutability: Read-only container mounts prevent accidental filesystem destruction.',
          'Scratch Isolation: Every build and task runs in a dedicated ephemeral directory in /data/scratch/.',
          'GitOps Boundary: Changes enter production strictly through reviewed and verified pull requests.',
        ],
        codeSnippets: [
          {
            language: 'bash',
            caption: 'Scratch workspace initialization pattern',
            code: `# Initialize an isolated scratch workspace for safe code modification
/share/aerial/scripts/aerial-pr.sh init aerial

# Direct modifications to /share/ are physically blocked
# touch /share/aerial/test.txt -> EROFS: Read-only file system`,
          },
        ],
      },
      {
        id: 'credential-sanitization',
        title: 'Zero Plaintext Tokens & Multi-Encoding Redaction',
        lead:
          'Credentials never touch disk in plaintext, and all outbound log streams, trace spans, and chat messages undergo real-time redaction.',
        paragraphs: [
          'Aerial enforces a Zero Plaintext Token Invariant: authentication tokens (e.g. GITHUB_PAT) are never saved into .git/config on disk. Git authentication uses ephemeral in-memory HTTP basic auth headers injected JIT during network calls.',
          'Outbound telemetry is sanitized using multi-encoding regex maskers. Redaction rules detect and mask tokens in plaintext, URL-encoded form, base64 encoding, and hex byte arrays before data leaves container boundaries.',
        ],
        bullets: [
          'Zero Disk Tokens: No tokens stored in git configs, shell histories, or workspace files.',
          'Multi-Encoding Redaction: Sanitizes tokens across ASCII, URL-encoded, Base64, and Hex formats.',
          'Redaction Bundle: Shared secrets are mounted in-memory at /secrets to configure regex masks dynamically.',
        ],
      },
      {
        id: 'hook-guard',
        title: 'Compiled Go hook-guard Pre-Tool Interception',
        lead:
          'A compiled Go binary intercepts every tool call before execution to enforce operational invariants and block hazardous commands.',
        paragraphs: [
          'Relying solely on LLM prompt instructions to prevent unsafe actions is insufficient. Aerial compiles a native Go pre-tool hook (hook-guard) into execution environments. Before any tool call executes, hook-guard inspects the command and arguments.',
          'Prohibited actions—such as ephemeral CLI scheduling commands that bypass durable Haro storage, dangerous filesystem mutations, or attempts to access internal credential stores—are rejected deterministically at the kernel boundary before the shell is invoked.',
        ],
        bullets: [
          'Deterministic Interception: Compiled Go binary runs prior to every tool execution.',
          'Prohibited Tool Blocking: Enforces Haro scheduling invariants and prevents ephemeral cron creation.',
          'Argument Sanitization: Auto-tunes command timeouts and parameters to maintain system responsiveness.',
        ],
      },
      {
        id: 'rbac-privilege',
        title: 'Admin RBAC & Privilege Boundary Enforcement',
        lead:
          'Multi-user message authentication validates administrator privileges before executing sensitive operations.',
        paragraphs: [
          'In multi-user Discord channels or shared household environments, unauthorized users must not be permitted to trigger system updates, restart containers, or modify configuration files.',
          'Aerial tags every inbound message with an is_admin flag verified against the admin_users list in config.yaml. Informational queries, smart home status checks, and public conversations are open to all users; sensitive infrastructure modifications, cron adjustments, and GitOps syncs require verified admin privileges.',
        ],
        bullets: [
          'Role-Based Access Control: Granular separation between standard conversational use and admin operations.',
          'Polite Rejection: Unauthorized privileged attempts are rejected succinctly without executing sensitive commands.',
          'Audit Logging: Privileged commands record structured audit logs with user IDs and timestamps.',
        ],
      },
    ],
  },
  {
    id: 'customization-day2',
    number: 7,
    title: 'Customization & Day-2 Operations',
    summary:
      'Authoring SKILL.md runbooks, registering Streamable HTTP MCP servers, channel lifecycle webhooks on_wake/pre_turn/post_turn, and adding homelab workloads.',
    sections: [
      {
        id: 'authoring-skills',
        title: 'Authoring Progressive Disclosure SKILL.md Runbooks',
        lead:
          'Extend Aerial’s capabilities with custom skills structured with YAML frontmatter, execution triggers, and progressive disclosure.',
        paragraphs: [
          'Skills are modular folders of instructions, reference documentation, and automation scripts that teach Aerial how to interact with specialized domains, home automation devices, or private APIs.',
          'Aerial uses progressive disclosure to keep prompt tokens lightweight: skills declare triggers and summaries in SKILL.md frontmatter. Full skill instructions, reference manuals, and helper scripts are loaded into agent context only when relevant to the active user request.',
        ],
        bullets: [
          'YAML Frontmatter: Defines name, description, and keyword trigger patterns.',
          'Progressive Disclosure: Heavy instructions and scripts remain on disk until activated.',
          'Custom Skills Directory: Private skills reside in aerial-config/custom-skills/ and take precedence over built-in skills.',
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
2. Trigger on-demand WAL archive sync:
   kubectl exec -it postgres-19-1 -- barman-cloud-wal-archive /var/lib/postgresql/data`,
          },
        ],
      },
      {
        id: 'mcp-servers',
        title: 'Registering Streamable HTTP MCP Servers',
        lead:
          'Integrate third-party tools, hardware bridges, and external APIs using Model Context Protocol (MCP) streamable servers.',
        paragraphs: [
          'Aerial leverages Model Context Protocol (MCP) to provide agents with typed, schema-validated tool interfaces. In addition to core built-in MCP microservices (docket-mcp, discord-mcp, kernel-mcp, infisical-mcp), users can register custom Streamable HTTP and SSE MCP servers in config.yaml.',
          'Tools are lazily discovered via standard schema endpoints, enabling agents to call custom home automation endpoints, financial APIs, or hardware monitoring tools without modifying application code.',
        ],
        bullets: [
          'Streamable HTTP / SSE: High-performance streaming tool invocations over HTTP.',
          'Declarative Registration: Add new servers to config.yaml under mcp_servers.',
          'Automatic Schema Generation: Tool definitions and argument validation schemas generate dynamically.',
        ],
        codeSnippets: [
          {
            language: 'yaml',
            caption: 'Registering custom MCP server in config.yaml',
            code: `mcp_servers:
  home-assistant:
    url: http://ha-mcp.aerial:8080/sse
    transport: sse
  custom-iot:
    url: http://iot-bridge.aerial:4000/mcp
    transport: streamable_http`,
          },
        ],
      },
      {
        id: 'channel-webhooks',
        title: 'Channel Lifecycle Webhooks (on_wake, pre_turn, post_turn)',
        lead:
          'Inspect, gate, and enrich agent turns dynamically through external HTTP lifecycle webhooks.',
        paragraphs: [
          'Aerial supports lifecycle hooks that allow external microservices or custom logic to participate in conversational turns. Channels can be configured with on_wake, pre_turn, and post_turn HTTP endpoints.',
          'The on_wake hook decides whether an inbound message should wake the agent or be dropped. The pre_turn hook can inject dynamic context (such as real-time sensor readings or user calendar events) directly into the agent’s prompt. The post_turn hook receives turn metadata and token counts for external logging.',
        ],
        bullets: [
          'on_wake: Override ambient classifier decisions (wake, drop, or classify).',
          'pre_turn: Inject real-time context or rate-limit turns programmatically.',
          'post_turn: Fire-and-forget notification for external telemetry, billing, or audit pipelines.',
        ],
      },
      {
        id: 'homelab-workloads',
        title: 'Integrating Smart Home & Custom Homelab Workloads',
        lead:
          'Connect Home Assistant automations, camera vision event watches, and custom microservices to build a complete home intelligence platform.',
        paragraphs: [
          'With its self-hosted Kubernetes foundation and extensible MCP interface, Aerial serves as the central brain of a modern smart home. Agents monitor Home Assistant entity states, adjust climate controls, inspect security camera feeds via the Vision microservice, and coordinate household routines.',
          'Persistent Haro event watches allow Aerial to trigger automations when specific real-world events occur—such as welcoming family members home, notifying of delivered packages, or running nocturnal security audits.',
        ],
        bullets: [
          'Home Assistant Integration: Bi-directional entity monitoring and automation control.',
          'Vision Triaging: Real-time object detection and event triage from UniFi Protect camera feeds.',
          'Event-Driven Routines: Deterministic reaction to physical smart home sensor events.',
        ],
        callout: {
          type: 'tip',
          title: 'Day-2 Operations Best Practice',
          text:
            'Always deploy custom microservices and hardware bridges into the default namespace with an HTTPRoute on the LAN gateway to make them immediately accessible to Aerial via the internal *.aerial service mesh.',
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

  return chapters
    .map((chapter) => {
      const titleMatch = chapter.title.toLowerCase().includes(q);
      const summaryMatch = chapter.summary.toLowerCase().includes(q);
      const numberMatch =
        `chapter ${chapter.number}`.includes(q) ||
        `ch. ${chapter.number}`.includes(q) ||
        `ch ${chapter.number}`.includes(q);

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
