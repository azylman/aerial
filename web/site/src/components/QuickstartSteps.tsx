import { useEffect, useRef, useState } from 'react';

export interface QuickstepItem {
  id: number;
  title: string;
  badge: string;
  summary: string;
  command: string;
  notes: readonly string[];
}

export const QUICKSTART_STEPS: readonly QuickstepItem[] = [
  {
    id: 1,
    title: 'Install Single-Node k3s',
    badge: 'Vanilla k3s',
    summary:
      'Install vanilla k3s with Traefik disabled (NGINX Gateway Fabric handles ingress routing) and standard user kubeconfig.',
    command:
      'curl -sfL https://get.k3s.io | sh -s - --disable traefik --write-kubeconfig ~/.kube/config --write-kubeconfig-mode 600',
    notes: [
      'No specialized node labels or hardware affinity required out-of-the-box',
      'Port 80/443 bound by NGINX Gateway Fabric LAN gateway',
    ],
  },
  {
    id: 2,
    title: 'Fork Config & Bootstrap via Flux CD',
    badge: 'Two-Repo GitOps',
    summary:
      'Fork azylman/aerial-config-example to create your private config repo, customize config.yaml with your Discord administrator ID (admin_users) and persona rules, then bootstrap Flux CD.',
    command: `export GITHUB_PAT="your-github-pat"\n\ncurl -s https://fluxcd.io/install.sh | sudo bash\nGITHUB_TOKEN="$GITHUB_PAT" flux bootstrap github \\\n  --owner=your-username \\\n  --repository=my-aerial-config \\\n  --branch=main \\\n  --path=k8s \\\n  --personal`,
    notes: [
      'Customize config.yaml with your Discord administrator ID (admin_users) and persona rules before bootstrap',
      'Zero plaintext token stored on disk; GITHUB_PAT passed in-memory during bootstrap',
      'Flux immediately reconciles base operators, databases, and platform services',
    ],
  },
  {
    id: 3,
    title: 'Bootstrap Infisical & Populate Secrets',
    badge: 'Zero-Plaintext Vault',
    summary:
      'Initialize Infisical machine identity, store the auth token in Kubernetes, and set your Discord and GitHub tokens.',
    command: `# 1. Bootstrap Infisical and store the returned Machine Identity Token Auth token\nexport INFISICAL_TOKEN="$(infisical bootstrap \\\n  --domain http://<node-ip> \\\n  --email admin@example.com \\\n  --password 'your-admin-password' \\\n  --organization aerial \\\n  | jq -r '.identity.credentials.token')"\n\nkubectl create secret generic infisical-universal-auth \\\n  --from-literal=INFISICAL_TOKEN="$INFISICAL_TOKEN"\n\n# 2. Populate external application secrets in Infisical (project slug aerial-8-jn8, default environment)\ninfisical secrets set --env=default \\\n  DISCORD_BOT_TOKEN="your-discord-bot-token" \\\n  GITHUB_PAT="$GITHUB_PAT"`,
    notes: [
      'External Secrets Operator auto-generates PostgreSQL, NATS, and webhook secrets',
      'Secrets sync in-memory into scoped Kubernetes Secrets with zero disk leakage',
    ],
  },
  {
    id: 4,
    title: 'Authenticate Agent CLI & Verify Rollout',
    badge: 'Engine Auth & Verify',
    summary:
      'Authenticate your preferred execution engine (agy, Claude Code, Codex, or Muse) and verify pod and GitOps health.',
    command: `# 1. Authenticate agy via Google OAuth inside the execution worker pod\nkubectl exec -it $(kubectl get pod -l app.kubernetes.io/name=eri-restate -o name | head -n1) -c eri-restate -- agy\n\n# 2. Verify all pods and Flux Kustomizations are healthy\nkubectl get pods -o wide\nflux get kustomizations`,
    notes: [
      'Interactive login credentials persist in /data/runtimes/ on the worker hostPath; static tokens mount read-only (:ro) via eri-secret',
      'Ready to message @Aerial in Discord or trigger voice interactions!',
    ],
  },
];

export function QuickstartSteps() {
  const [copiedId, setCopiedId] = useState<number | null>(null);
  const [copyFailedId, setCopyFailedId] = useState<number | null>(null);
  const [activeStepTab, setActiveStepTab] = useState<number>(1);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, []);

  const handleCopy = async (id: number, text: string) => {
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
      timeoutRef.current = null;
    }
    try {
      if (!navigator.clipboard?.writeText) {
        throw new Error('Clipboard API not available');
      }
      await navigator.clipboard.writeText(text);
      setCopyFailedId(null);
      setCopiedId(id);
      timeoutRef.current = setTimeout(() => {
        setCopiedId(null);
        timeoutRef.current = null;
      }, 2500);
    } catch {
      setCopiedId(null);
      setCopyFailedId(id);
      timeoutRef.current = setTimeout(() => {
        setCopyFailedId(null);
        timeoutRef.current = null;
      }, 2500);
    }
  };

  return (
    <div className="quickstart-container" data-testid="quickstart-steps">
      <div className="quickstart-header">
        <div className="section-eyebrow">Production Deployment</div>
        <h2 className="section-title">Single-Node k3s Quickstart</h2>
        <p className="section-subtitle">
          Aerial boots out-of-the-box on a single vanilla Linux machine or homelab node in under 10 minutes, and
          scales cleanly to multi-node clusters via declarative Flux overlays.
        </p>

        {/* Quick Tabs to jump between steps */}
        <div className="quickstart-nav" role="tablist" aria-label="Quickstart steps navigation">
          {QUICKSTART_STEPS.map((s) => (
            <button
              key={s.id}
              type="button"
              role="tab"
              aria-selected={activeStepTab === s.id}
              className={`quick-step-nav-btn ${activeStepTab === s.id ? 'active' : ''}`}
              onClick={() => setActiveStepTab(s.id)}
              data-testid={`quick-tab-${s.id}`}
            >
              <span className="step-badge-num">{s.id}</span>
              <span className="step-tab-title">{s.title}</span>
            </button>
          ))}
        </div>
      </div>

      {/* Terminal Steps Stream */}
      <div className="terminal-steps-list">
        {QUICKSTART_STEPS.filter((s) => s.id === activeStepTab).map((step) => {
          const isCopied = copiedId === step.id;
          const isFailed = copyFailedId === step.id;

          return (
            <div key={step.id} className="terminal-step-card" data-testid={`step-card-${step.id}`}>
              <div className="terminal-card-header">
                <div className="terminal-title-group">
                  <span className="step-circle-badge">Step {step.id}</span>
                  <h3 className="terminal-step-title">{step.title}</h3>
                  <span className="badge badge-accent">{step.badge}</span>
                </div>

                <button
                  type="button"
                  className={`copy-code-btn ${isCopied ? 'copied' : ''} ${isFailed ? 'failed' : ''}`}
                  onClick={() => handleCopy(step.id, step.command)}
                  aria-label={`Copy command for Step ${step.id}`}
                  data-testid={`copy-btn-${step.id}`}
                >
                  <span aria-hidden="true">{isCopied ? '✓' : isFailed ? '⚠️' : '📋'}</span>
                  <span>
                    {isCopied
                      ? 'Copied to Clipboard!'
                      : isFailed
                        ? 'Copy failed, select manually'
                        : 'Copy Command'}
                  </span>
                </button>
              </div>

              <p className="terminal-summary">{step.summary}</p>

              <div className="terminal-code-block">
                <div className="terminal-shell-bar">
                  <span className="shell-lang">bash</span>
                  <span className="shell-tag">terminal</span>
                </div>
                <pre>
                  <code>{step.command}</code>
                </pre>
              </div>

              <div className="terminal-notes">
                <span className="notes-label">Operational Notes:</span>
                <ul className="notes-list">
                  {step.notes.map((note, idx) => (
                    <li key={idx}>{note}</li>
                  ))}
                </ul>
              </div>
            </div>
          );
        })}
      </div>

      {/* Scaling Callout */}
      <div className="scaling-callout" data-testid="scaling-callout">
        <div className="callout-icon" aria-hidden="true">
          💡
        </div>
        <div className="callout-text">
          <strong>Ready to scale to a multi-node cluster?</strong> Layer declarative patches in{' '}
          <code>aerial-config/k8s/cluster-overrides.yaml</code> to enable CloudNativePG PostgreSQL 2-instance HA, pin GPU
          inference sidecars to compute nodes, or connect touch kiosks with zero changes to base manifests.
        </div>
      </div>
    </div>
  );
}
