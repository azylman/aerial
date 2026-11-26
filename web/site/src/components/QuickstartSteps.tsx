import { useState } from 'react';
import { useCopyToClipboard } from '../useCopyToClipboard';

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
      'Install k3s with Traefik disabled and configure standard user permissions for kubeconfig.',
    command:
      'curl -sfL https://get.k3s.io | sh -s - --disable traefik --write-kubeconfig ~/.kube/config --write-kubeconfig-mode 600',
    notes: [
      'Deploy without specialized node labels or hardware affinity.',
      'NGINX Gateway Fabric binds standard HTTP and HTTPS ports.',
    ],
  },
  {
    id: 2,
    title: 'Fork Config & Bootstrap via Flux CD',
    badge: 'Two-Repo GitOps',
    summary:
      'Fork azylman/aerial-config-example to create your private config repository and bootstrap Flux CD.',
    command: `export GITHUB_PAT="your-github-pat"\n\ncurl -s https://fluxcd.io/install.sh | sudo bash\nGITHUB_TOKEN="$GITHUB_PAT" flux bootstrap github \\\n  --owner=your-username \\\n  --repository=my-aerial-config \\\n  --branch=main \\\n  --path=k8s \\\n  --personal`,
    notes: [
      'Set admin_users in config.yaml to your Discord user ID before running bootstrap.',
      'Pass tokens in memory to prevent writing plaintext secrets to disk.',
      'Flux reconciles base operators, databases, and platform services automatically.',
    ],
  },
  {
    id: 3,
    title: 'Bootstrap Infisical & Populate Secrets',
    badge: 'Zero-Plaintext Vault',
    summary:
      'Initialize Infisical machine identity, store the authentication token in Kubernetes, and save API credentials.',
    command: `# 1. Bootstrap Infisical and store the returned Machine Identity Token Auth token\nexport INFISICAL_TOKEN="$(infisical bootstrap \\\n  --domain http://<node-ip> \\\n  --email admin@example.com \\\n  --password 'your-admin-password' \\\n  --organization aerial \\\n  | jq -r '.identity.credentials.token')"\n\nkubectl create secret generic infisical-universal-auth \\\n  --from-literal=INFISICAL_TOKEN="$INFISICAL_TOKEN"\n\n# 2. Populate external application secrets in Infisical (project slug aerial, default environment)\ninfisical secrets set --env=default \\\n  DISCORD_BOT_TOKEN="your-discord-bot-token" \\\n  GITHUB_PAT="$GITHUB_PAT"`,
    notes: [
      'External Secrets Operator generates database, message broker, and webhook credentials.',
      'Synchronize secrets directly into scoped Kubernetes Secrets without disk storage.',
    ],
  },
  {
    id: 4,
    title: 'Authenticate Agent CLI & Verify Rollout',
    badge: 'Engine Auth & Verify',
    summary:
      'Authenticate your preferred execution engine and verify cluster and GitOps health.',
    command: `# 1. Authenticate agy via Google OAuth inside the execution worker pod\nkubectl exec -it $(kubectl get pod -l app.kubernetes.io/name=eri-restate -o name | head -n1) -c eri-restate -- agy\n\n# 2. Verify all pods and Flux Kustomizations are healthy\nkubectl get pods -o wide\nflux get kustomizations`,
    notes: [
      'Store credentials in runtime directories and mount static secrets with read-only permissions.',
      'Message the assistant in Discord or initiate voice interactions.',
    ],
  },
];

export function QuickstartSteps() {
  const [activeStepTab, setActiveStepTab] = useState<number>(1);
  const { copy, isCopied, isFailed } = useCopyToClipboard<number>();

  return (
    <div className="quickstart-container" data-testid="quickstart-steps">
      <div className="quickstart-header">
        <div className="section-eyebrow">Homelab & Cluster Deployment</div>
        <h2 className="section-title">Single-Node k3s Quickstart</h2>
        <p className="section-subtitle">
          Aerial runs on a single Linux machine and scales to multi-node clusters through declarative Flux overlays.
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
          const isCopiedStep = isCopied(step.id);
          const isFailedStep = isFailed(step.id);

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
                  className={`copy-code-btn ${isCopiedStep ? 'copied' : ''} ${isFailedStep ? 'failed' : ''}`}
                  onClick={() => copy(step.command, step.id)}
                  aria-label={`Copy command for Step ${step.id}`}
                  data-testid={`copy-btn-${step.id}`}
                >
                  <span aria-hidden="true">{isCopiedStep ? '✓' : isFailedStep ? '⚠️' : '📋'}</span>
                  <span>
                    {isCopiedStep
                      ? 'Copied to Clipboard!'
                      : isFailedStep
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
          <code>aerial-config/k8s/cluster-overrides.yaml</code> to enable high-availability PostgreSQL clusters.
          Pin GPU inference sidecars to compute nodes without altering base manifests.
        </div>
      </div>
    </div>
  );
}
