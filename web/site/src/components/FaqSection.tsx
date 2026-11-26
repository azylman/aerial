import { useState } from 'react';

export interface FaqItem {
  id: string;
  question: string;
  answer: string;
  category: string;
}

export const FAQ_ITEMS: readonly FaqItem[] = [
  {
    id: 'why-k3s-restate',
    category: 'Architecture',
    question: 'Why does Aerial use k3s and Restate instead of a single Docker container and SQLite?',
    answer:
      'Standard Docker setups lose execution state during process crashes or daemon restarts. Restate provides durable virtual objects and execution logs backed by RocksDB. Kubernetes orchestrates decoupled microservices and provides declarative Gateway API routing. PostgreSQL with vector index extensions delivers robust concurrent multi-session memory storage. This architecture ensures high availability and safe autonomous operation.',
  },
  {
    id: 'hardware-requirements',
    category: 'Self-Hosting',
    question: 'Can I run Aerial on a single mini-PC or Linux VPS without a dedicated GPU?',
    answer:
      'Yes, you can run Aerial on a single x86_64 or ARM64 host with 4 CPU cores and 16 gigabytes of RAM. A dedicated GPU is optional for speech recognition and local neural voice synthesis. Aerial connects to external API providers for speech synthesis when local acceleration is unavailable. The single-node k3s bootstrap script configures the entire stack in minutes.',
  },
  {
    id: 'two-repo-privacy',
    category: 'Security & GitOps',
    question: 'How does the Two-Repo GitOps model keep personal data and secrets out of the engine repository?',
    answer:
      'The architecture splits code across an open-source engine repository and a private user configuration repository. The public repository contains only generic services, platform manifests, and domain-agnostic skills. The private repository holds personal personas, private device identifiers, local rules, and custom integrations. Infisical holds credentials and External Secrets Operator syncs them into scoped Kubernetes Secrets, keeping keys out of version control.',
  },
  {
    id: 'supported-engines',
    category: 'AI Engines',
    question: 'Which coding CLI engines and LLM providers work with Aerial?',
    answer:
      'Aerial runs pluggable agent CLI execution engines including Claude Code, Codex CLI, and Google Antigravity. The platform routes model calls to Anthropic, OpenAI, and Google Cloud providers. You can configure preferred models and per-channel reasoning tiers in your user configuration repository. Local ONNX and GGUF inference run speech processing and ambient triage tasks on premises.',
  },
  {
    id: 'self-healing-safety',
    category: 'Software Factory',
    question: 'How does Aerial prevent autonomous code changes from breaking the live cluster?',
    answer:
      'Autonomous workflows execute exclusively inside ephemeral scratch workspaces isolated from the live system. An adversarial multi-agent review panel inspects every proposed change across code quality and security. Local verification scripts run strict unit tests and regression checks before creating pull requests. Flux CD synchronizes container images and manifests only after all automated continuous integration checks pass.',
  },
] as const;

export function FaqSection() {
  const [expandedId, setExpandedId] = useState<string | null>('why-k3s-restate');

  const toggleItem = (id: string) => {
    setExpandedId((prev) => (prev === id ? null : id));
  };

  return (
    <div className="faq-section" data-testid="faq-section">
      <div className="section-header">
        <span className="section-eyebrow">Technical FAQ</span>
        <h2 className="section-title">Frequently Asked Questions</h2>
        <p className="section-subtitle">
          Answers to common technical questions about architecture, self-hosting, and autonomous safety.
        </p>
      </div>

      <div className="faq-list" role="region" aria-label="Frequently Asked Questions">
        {FAQ_ITEMS.map((item) => {
          const isExpanded = expandedId === item.id;
          return (
            <div
              key={item.id}
              className={`faq-item ${isExpanded ? 'expanded' : ''}`}
              data-testid={`faq-item-${item.id}`}
            >
              <button
                type="button"
                className="faq-question-btn"
                onClick={() => toggleItem(item.id)}
                aria-expanded={isExpanded}
                aria-controls={`faq-answer-${item.id}`}
                data-testid={`faq-btn-${item.id}`}
              >
                <span className="faq-category-pill">{item.category}</span>
                <span className="faq-question-text">{item.question}</span>
                <span className="faq-chevron" aria-hidden="true">
                  {isExpanded ? '▲' : '▼'}
                </span>
              </button>
              {isExpanded && (
                <div
                  id={`faq-answer-${item.id}`}
                  className="faq-answer-panel"
                  data-testid={`faq-answer-${item.id}`}
                  role="region"
                >
                  <p>{item.answer}</p>
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
