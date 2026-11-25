import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ArchitectureMap } from './ArchitectureMap';
import { ComparisonMatrix } from './ComparisonMatrix';
import { FactoryStepper } from './FactoryStepper';
import { GlassBoxShowcase } from './GlassBoxShowcase';
import {
  FAQ_ITEMS,
  FaqSection,
  HERO_INSTALL_COMMAND,
  INTEGRATION_BADGES,
  LandingView,
} from './LandingView';
import { QuickstartSteps } from './QuickstartSteps';
import { UseCaseGallery } from './UseCaseGallery';

describe('LandingView Component', () => {
  afterEach(() => {
    cleanup();
  });

  it('renders landing hero, pillar features, and all showcase sections', () => {
    render(<LandingView />);

    expect(within(screen.getByTestId('section-use-cases')).getByTestId('use-case-gallery')).toBeTruthy();
    expect(within(screen.getByTestId('section-architecture')).getByTestId('architecture-map')).toBeTruthy();
    expect(within(screen.getByTestId('section-factory')).getByTestId('factory-stepper')).toBeTruthy();
    expect(within(screen.getByTestId('section-showcase')).getByTestId('glassbox-showcase')).toBeTruthy();
    expect(within(screen.getByTestId('section-comparison')).getByTestId('comparison-matrix')).toBeTruthy();
    expect(within(screen.getByTestId('section-quickstart')).getByTestId('quickstart-steps')).toBeTruthy();
    expect(within(screen.getByTestId('section-faq')).getByTestId('faq-section')).toBeTruthy();
  });
});

describe('ArchitectureMap Component', () => {
  afterEach(() => {
    cleanup();
  });

  it('renders 9 topology nodes and handles pillar filtering', () => {
    render(<ArchitectureMap />);

    const nodeEri = screen.getByTestId('node-eri-restate');
    const nodeK3s = screen.getByTestId('node-k3s-nginx');
    const nodeVikunja = screen.getByTestId('node-vikunja-docket');

    // Default inspector node
    const inspector = screen.getByTestId('node-inspector');
    expect(within(inspector).getByText('Agent Execution Engine')).toBeTruthy();

    // Filter by platform pillar
    fireEvent.click(screen.getByTestId('filter-platform'));
    expect(nodeK3s.classList.contains('dimmed')).toBe(false);
    expect(nodeEri.classList.contains('dimmed')).toBe(true);

    // Filter by factory pillar
    fireEvent.click(screen.getByTestId('filter-factory'));
    expect(nodeVikunja.classList.contains('dimmed')).toBe(false);
    expect(nodeK3s.classList.contains('dimmed')).toBe(true);

    // Filter by agent pillar
    fireEvent.click(screen.getByTestId('filter-agent'));
    expect(nodeEri.classList.contains('dimmed')).toBe(false);
    expect(nodeVikunja.classList.contains('dimmed')).toBe(true);

    // Filter all
    fireEvent.click(screen.getByTestId('filter-all'));
    expect(nodeEri.classList.contains('dimmed')).toBe(false);
    expect(nodeK3s.classList.contains('dimmed')).toBe(false);
  });

  it('selects nodes via mouse click and keyboard events', () => {
    render(<ArchitectureMap />);

    const nodeK3s = screen.getByTestId('node-k3s-nginx');
    fireEvent.click(nodeK3s);

    const inspector = screen.getByTestId('node-inspector');
    expect(inspector.textContent).toContain('k3s / NGINX Gateway');
    expect(inspector.textContent).toContain('Gateway API (LAN & Edge HTTPRoutes)');

    // Select with Enter key
    const nodeVikunja = screen.getByTestId('node-vikunja-docket');
    fireEvent.keyDown(nodeVikunja, { key: 'Enter' });
    expect(inspector.textContent).toContain('Vikunja DAG Backlog');

    // Select with Space key
    const nodePostgres = screen.getByTestId('node-cnpg-postgres');
    fireEvent.keyDown(nodePostgres, { key: ' ' });
    expect(inspector.textContent).toContain('CloudNativePG Postgres 19');

    // Other key should not select
    fireEvent.keyDown(nodeK3s, { key: 'Escape' });
    expect(inspector.textContent).toContain('CloudNativePG Postgres 19');
  });
});

describe('GlassBoxShowcase Component', () => {
  afterEach(() => {
    cleanup();
  });

  it('switches between all 5 showcase tabs and renders their mockups', () => {
    render(<GlassBoxShowcase />);

    // Default tab: Console
    expect(screen.getByTestId('view-console')).toBeTruthy();
    expect(screen.getByTestId('console-turn-content')).toBeTruthy();

    // Toggle Console sub-view to Builds
    fireEvent.click(screen.getByTestId('console-btn-builds'));
    expect(screen.getByTestId('console-builds-content')).toBeTruthy();
    fireEvent.click(screen.getByTestId('console-btn-turn'));
    expect(screen.getByTestId('console-turn-content')).toBeTruthy();

    // Switch to AgentsView tab
    fireEvent.click(screen.getByTestId('tab-agentsview'));
    expect(screen.getByTestId('view-agentsview')).toBeTruthy();

    // Click through steps in AgentsView
    for (let i = 0; i < 5; i++) {
      fireEvent.click(screen.getByTestId(`step-btn-${i}`));
      expect(screen.getByTestId('step-details').textContent).toContain(`STEP #${i + 1}`);
    }

    // Switch to Restate tab
    fireEvent.click(screen.getByTestId('tab-restate'));
    expect(screen.getByTestId('view-restate')).toBeTruthy();
    expect(screen.getByTestId('restate-journal-content')).toBeTruthy();

    // Switch Restate view mode to RocksDB state
    fireEvent.click(screen.getByTestId('restate-btn-state'));
    expect(screen.getByTestId('restate-state-content')).toBeTruthy();
    fireEvent.click(screen.getByTestId('restate-btn-journal'));
    expect(screen.getByTestId('restate-journal-content')).toBeTruthy();

    // Switch to Vikunja tab
    fireEvent.click(screen.getByTestId('tab-vikunja'));
    expect(screen.getByTestId('view-vikunja')).toBeTruthy();

    // Test selection changes
    fireEvent.click(screen.getByTestId('card-task-102'));
    expect(screen.getByTestId('card-task-102').classList.contains('selected')).toBe(true);

    fireEvent.click(screen.getByTestId('card-task-101'));
    expect(screen.getByTestId('card-task-101').classList.contains('selected')).toBe(true);

    fireEvent.click(screen.getByTestId('card-task-103'));
    expect(screen.getByTestId('card-task-103').classList.contains('selected')).toBe(true);

    // Switch to Observability tab
    fireEvent.click(screen.getByTestId('tab-observability'));
    expect(screen.getByTestId('view-observability')).toBeTruthy();
    expect(screen.getByTestId('obs-metrics-content')).toBeTruthy();

    // Toggle Observability modes
    fireEvent.click(screen.getByTestId('obs-btn-logs'));
    expect(screen.getByTestId('obs-logs-content')).toBeTruthy();

    fireEvent.click(screen.getByTestId('obs-btn-traces'));
    expect(screen.getByTestId('obs-traces-content')).toBeTruthy();

    fireEvent.click(screen.getByTestId('obs-btn-metrics'));
    expect(screen.getByTestId('obs-metrics-content')).toBeTruthy();
  });
});

describe('FactoryStepper Component', () => {
  afterEach(() => {
    cleanup();
  });

  it('navigates through all 5 stages using step buttons and navigation arrows', () => {
    render(<FactoryStepper />);

    const detail = screen.getByTestId('stage-detail-content');
    expect(detail.textContent).toContain('Stage 1: Vikunja DAG Ready Queue');

    const nextBtn = screen.getByTestId('next-stage-btn');
    const prevBtn = screen.getByTestId('prev-stage-btn');

    // Previous is initially disabled at Stage 1
    expect(prevBtn.hasAttribute('disabled')).toBe(true);

    // Navigate Forward: 1 -> 2 -> 3 -> 4 -> 5
    fireEvent.click(nextBtn);
    expect(detail.textContent).toContain('Stage 2: Ephemeral Scratch & Multi-Agent Review');
    expect(prevBtn.hasAttribute('disabled')).toBe(false);

    fireEvent.click(nextBtn);
    expect(detail.textContent).toContain('Stage 3: Restate + NATS Multi-PR Build Pipeline');

    fireEvent.click(nextBtn);
    expect(detail.textContent).toContain('Stage 4: GitHub Actions CI & Automated Code Review');

    fireEvent.click(nextBtn);
    expect(detail.textContent).toContain('Stage 5: Self-Healing Remediation & Flux Push GitOps');
    expect(nextBtn.hasAttribute('disabled')).toBe(true);

    // Navigate Backward: 5 -> 4
    fireEvent.click(prevBtn);
    expect(detail.textContent).toContain('Stage 4: GitHub Actions CI & Automated Code Review');

    // Jump directly to Stage 2
    fireEvent.click(screen.getByTestId('stage-btn-2'));
    expect(detail.textContent).toContain('Stage 2: Ephemeral Scratch & Multi-Agent Review');
  });

  it('toggles self-healing remediation flow callout banner', () => {
    render(<FactoryStepper />);

    expect(screen.queryByTestId('remediation-banner')).toBeNull();

    const toggleBtn = screen.getByTestId('toggle-remediation');
    fireEvent.click(toggleBtn);

    expect(screen.getByTestId('remediation-banner')).toBeTruthy();

    fireEvent.click(toggleBtn);
    expect(screen.queryByTestId('remediation-banner')).toBeNull();
  });
});

describe('ComparisonMatrix Component', () => {
  afterEach(() => {
    cleanup();
  });

  it('filters competitors by category and search keyword in cards view', () => {
    render(<ComparisonMatrix />);

    // Default: all categories
    expect(screen.getByTestId('comp-card-home-ops')).toBeTruthy();
    expect(screen.getByTestId('comp-card-gas-town-beads')).toBeTruthy();
    expect(screen.getByTestId('comp-card-hermes-openclaw')).toBeTruthy();

    // Filter: Homelab & PaaS
    fireEvent.click(screen.getByTestId('comp-filter-homelab'));
    expect(screen.getByTestId('comp-card-home-ops')).toBeTruthy();
    expect(screen.queryByTestId('comp-card-gas-town-beads')).toBeNull();

    // Filter: Software Factories
    fireEvent.click(screen.getByTestId('comp-filter-factory'));
    expect(screen.getByTestId('comp-card-gas-town-beads')).toBeTruthy();
    expect(screen.queryByTestId('comp-card-home-ops')).toBeNull();

    // Filter: AI Agents
    fireEvent.click(screen.getByTestId('comp-filter-agent'));
    expect(screen.getByTestId('comp-card-hermes-openclaw')).toBeTruthy();
    expect(screen.queryByTestId('comp-card-gas-town-beads')).toBeNull();

    // Filter back to all
    fireEvent.click(screen.getByTestId('comp-filter-all'));

    // Search query
    const searchInput = screen.getByTestId('comp-search-input');
    fireEvent.change(searchInput, { target: { value: 'coolify' } });
    expect(screen.getByTestId('comp-card-coolify-dokploy')).toBeTruthy();
    expect(screen.queryByTestId('comp-card-home-ops')).toBeNull();

    // Empty search match
    fireEvent.change(searchInput, { target: { value: 'nonexistent-software-tool' } });
    expect(screen.getByText(/No competitors matched your search query/i)).toBeTruthy();
  });

  it('switches between Cards and Matrix table view modes with category and search filtering', () => {
    render(<ComparisonMatrix />);

    expect(screen.getByTestId('competitor-cards')).toBeTruthy();
    expect(screen.queryByTestId('comparison-table')).toBeNull();

    // Switch to Matrix view
    fireEvent.click(screen.getByTestId('view-matrix-btn'));
    expect(screen.getByTestId('comparison-table')).toBeTruthy();
    expect(screen.queryByTestId('competitor-cards')).toBeNull();
    expect(screen.getByTestId('matrix-row-0')).toBeTruthy();
    expect(screen.getByTestId('comp-search-input').getAttribute('placeholder')).toBe(
      'Filter comparison dimensions by keyword...'
    );

    // In Matrix view, filter by category
    fireEvent.click(screen.getByTestId('comp-filter-homelab'));
    const table = screen.getByTestId('comparison-table');
    expect(within(table).getByText('Homelab & PaaS')).toBeTruthy();
    expect(within(table).queryByText('Software Factories')).toBeNull();

    // In Matrix view, filter by search keyword
    const searchInput = screen.getByTestId('comp-search-input');
    fireEvent.change(searchInput, { target: { value: 'orchestration' } });
    expect(screen.getByText('Stateful Orchestration')).toBeTruthy();
    expect(screen.queryByText('Primary Architecture')).toBeNull();

    // Category-scoped search: 'sqlite' is only present in the AI Agents column, hidden under homelab filter
    fireEvent.change(searchInput, { target: { value: 'sqlite' } });
    expect(screen.getByText(/No comparison dimensions matched your search query/i)).toBeTruthy();

    // Reset filter to all: now 'sqlite' matches
    fireEvent.click(screen.getByTestId('comp-filter-all'));
    expect(screen.getByText('Stateful Orchestration')).toBeTruthy();

    // Unmatched search in Matrix view
    fireEvent.change(searchInput, { target: { value: 'unmatched-matrix-query' } });
    expect(screen.getByText(/No comparison dimensions matched your search query/i)).toBeTruthy();

    // Switch back to Cards view
    fireEvent.click(screen.getByTestId('view-cards-btn'));
    expect(screen.getByTestId('competitor-cards')).toBeTruthy();
    expect(screen.getByTestId('comp-search-input').getAttribute('placeholder')).toBe(
      'Filter by keyword or competitor...'
    );
  });
});

describe('QuickstartSteps Component', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    cleanup();
  });

  it('switches steps, copies command to clipboard, and cancels pending timeout on re-copy', async () => {
    const writeTextMock = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, {
      clipboard: {
        writeText: writeTextMock,
      },
    });

    render(<QuickstartSteps />);

    expect(screen.getByTestId('step-card-1')).toBeTruthy();

    // Click copy button on Step 1
    const copyBtn1 = screen.getByTestId('copy-btn-1');
    await act(async () => {
      fireEvent.click(copyBtn1);
    });

    expect(writeTextMock).toHaveBeenCalledWith(expect.stringContaining('curl -sfL https://get.k3s.io'));
    expect(screen.getByText('Copied to Clipboard!')).toBeTruthy();

    // Click copy again after 2000ms (resets timer for full 2500ms)
    act(() => {
      vi.advanceTimersByTime(2000);
    });
    await act(async () => {
      fireEvent.click(copyBtn1);
    });

    // 1000ms later (3000ms after first click), should STILL be copied
    act(() => {
      vi.advanceTimersByTime(1000);
    });
    expect(screen.getByText('Copied to Clipboard!')).toBeTruthy();

    // Advance remaining 1500ms -> reverts to Copy Command
    act(() => {
      vi.advanceTimersByTime(1500);
    });
    expect(screen.getByText('Copy Command')).toBeTruthy();

    // Switch to Step 2
    fireEvent.click(screen.getByTestId('quick-tab-2'));
    expect(screen.getByTestId('step-card-2')).toBeTruthy();
    expect(screen.queryByTestId('step-card-1')).toBeNull();
  });

  it('handles clipboard failure gracefully and cleans up timer on unmount', async () => {
    const writeTextMock = vi.fn().mockRejectedValue(new Error('Permission denied'));
    Object.assign(navigator, {
      clipboard: {
        writeText: writeTextMock,
      },
    });

    const { unmount } = render(<QuickstartSteps />);

    const copyBtn = screen.getByTestId('copy-btn-1');
    await act(async () => {
      fireEvent.click(copyBtn);
    });

    expect(screen.queryByText('Copied to Clipboard!')).toBeNull();
    expect(screen.getByText('Copy failed, select manually')).toBeTruthy();

    // Advance 2500ms -> reverts to Copy Command
    act(() => {
      vi.advanceTimersByTime(2500);
    });
    expect(screen.getByText('Copy Command')).toBeTruthy();

    // Trigger failure again to test unmount cleanup
    await act(async () => {
      fireEvent.click(copyBtn);
    });
    expect(screen.getByText('Copy failed, select manually')).toBeTruthy();

    // Verify timer is registered before unmount
    expect(vi.getTimerCount()).toBe(1);

    // Unmount cancels pending timer via effect cleanup
    unmount();
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe('LandingView Hero Quick-Install Pill', () => {
  afterEach(() => {
    cleanup();
  });

  it('renders quick-install pill with bootstrap command, copy button, and anchor jump', () => {
    render(<LandingView />);

    const pill = screen.getByTestId('hero-install-pill');
    expect(pill.textContent).toContain(HERO_INSTALL_COMMAND);

    const jumpLink = screen.getByTestId('hero-install-jump');
    expect(jumpLink.getAttribute('href')).toBe('#quickstart');
  });

  it('copies hero install command to clipboard and updates button label', async () => {
    const writeTextMock = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, {
      clipboard: {
        writeText: writeTextMock,
      },
    });

    render(<LandingView />);

    const copyBtn = screen.getByTestId('hero-install-copy-btn');
    expect(copyBtn.textContent).toContain('Copy');

    await act(async () => {
      fireEvent.click(copyBtn);
    });

    expect(writeTextMock).toHaveBeenCalledWith(HERO_INSTALL_COMMAND);
    expect(copyBtn.textContent).toContain('Copied!');
  });

  it('handles hero install clipboard failure gracefully', async () => {
    const writeTextMock = vi.fn().mockRejectedValue(new Error('Permission denied'));
    Object.assign(navigator, {
      clipboard: {
        writeText: writeTextMock,
      },
    });

    render(<LandingView />);

    const copyBtn = screen.getByTestId('hero-install-copy-btn');
    await act(async () => {
      fireEvent.click(copyBtn);
    });

    expect(copyBtn.textContent).toContain('Copy failed');
  });
});

describe('UseCaseGallery Component', () => {
  afterEach(() => {
    cleanup();
  });

  it('renders default scenario and switches tabs across all 4 scenarios', () => {
    render(<UseCaseGallery />);

    const promptEl = screen.getByTestId('usecase-prompt');
    const outcomeEl = screen.getByTestId('usecase-outcome');

    // Default scenario: ci-remediation
    expect(screen.getByTestId('usecase-tab-ci-remediation').classList.contains('active')).toBe(true);
    expect(promptEl.textContent).toContain('Fix the failing unit tests');
    expect(outcomeEl.textContent).toContain('Automated code review passes');
    expect(screen.getByTestId('usecase-step-0').textContent).toContain('Parse compiler failure logs');

    // Switch to multi-pr-migration
    fireEvent.click(screen.getByTestId('usecase-tab-multi-pr-migration'));
    expect(screen.getByTestId('usecase-tab-multi-pr-migration').classList.contains('active')).toBe(true);
    expect(screen.getByTestId('usecase-prompt').textContent).toContain('vector embeddings column');
    expect(screen.getByTestId('usecase-outcome').textContent).toContain('Multi-repository changes deploy sequentially');
    expect(screen.getByTestId('usecase-step-1').textContent).toContain('Enqueue multi-stage build workflow');

    // Switch to incident-triage
    fireEvent.click(screen.getByTestId('usecase-tab-incident-triage'));
    expect(screen.getByTestId('usecase-tab-incident-triage').classList.contains('active')).toBe(true);
    expect(screen.getByTestId('usecase-prompt').textContent).toContain('Why did voice interaction latency spike');
    expect(screen.getByTestId('usecase-outcome').textContent).toContain('Engineers receive root-cause diagnosis');
    expect(screen.getByTestId('usecase-step-2').textContent).toContain('Publish incident diagnosis');

    // Switch to smart-home-scheduler
    fireEvent.click(screen.getByTestId('usecase-tab-smart-home-scheduler'));
    expect(screen.getByTestId('usecase-tab-smart-home-scheduler').classList.contains('active')).toBe(true);
    expect(screen.getByTestId('usecase-prompt').textContent).toContain('Turn off hallway lights');
    expect(screen.getByTestId('usecase-outcome').textContent).toContain('Routines survive service restarts');
    expect(screen.getByTestId('usecase-step-0').textContent).toContain('Schedule durable one-shot timer');
  });
});

describe('LandingView Integration Strip', () => {
  afterEach(() => {
    cleanup();
  });

  it('renders integration badge strip with 12 badges across Surfaces, AI Engines, and Core Stack', () => {
    render(<LandingView />);

    const strip = screen.getByTestId('integration-strip');
    expect(strip).toBeTruthy();
    expect(strip.textContent).toContain('Supported Surfaces, Engines & Integrations');

    expect(INTEGRATION_BADGES).toHaveLength(12);

    for (const badge of INTEGRATION_BADGES) {
      const badgeEl = screen.getByTestId(`integration-badge-${badge.id}`);
      expect(badgeEl).toBeTruthy();
      expect(badgeEl.textContent).toContain(badge.group);
      expect(badgeEl.textContent).toContain(badge.label);
    }
  });
});

describe('FaqSection Component', () => {
  afterEach(() => {
    cleanup();
  });

  it('renders header, 5 FAQ items, and handles accordion toggle state across all items', () => {
    render(<FaqSection />);

    const section = screen.getByTestId('faq-section');
    expect(section).toBeTruthy();
    expect(within(section).getByText('Technical FAQ')).toBeTruthy();
    expect(within(section).getByText('Frequently Asked Questions')).toBeTruthy();

    expect(FAQ_ITEMS).toHaveLength(5);

    // First item is expanded by default
    const firstBtn = screen.getByTestId(`faq-btn-${FAQ_ITEMS[0]!.id}`);
    expect(firstBtn.getAttribute('aria-expanded')).toBe('true');
    expect(screen.getByTestId(`faq-answer-${FAQ_ITEMS[0]!.id}`)).toBeTruthy();
    expect(screen.getByTestId(`faq-answer-${FAQ_ITEMS[0]!.id}`).textContent).toContain(FAQ_ITEMS[0]!.answer);

    // Clicking first item collapses it
    fireEvent.click(firstBtn);
    expect(firstBtn.getAttribute('aria-expanded')).toBe('false');
    expect(screen.queryByTestId(`faq-answer-${FAQ_ITEMS[0]!.id}`)).toBeNull();

    // Re-expanding first item
    fireEvent.click(firstBtn);
    expect(firstBtn.getAttribute('aria-expanded')).toBe('true');
    expect(screen.getByTestId(`faq-answer-${FAQ_ITEMS[0]!.id}`)).toBeTruthy();

    // Clicking through remaining items: each expands when clicked
    for (let i = 1; i < FAQ_ITEMS.length; i++) {
      const item = FAQ_ITEMS[i]!;
      const btn = screen.getByTestId(`faq-btn-${item.id}`);
      expect(btn.getAttribute('aria-expanded')).toBe('false');
      expect(screen.queryByTestId(`faq-answer-${item.id}`)).toBeNull();

      fireEvent.click(btn);
      expect(btn.getAttribute('aria-expanded')).toBe('true');
      const answerPanel = screen.getByTestId(`faq-answer-${item.id}`);
      expect(answerPanel).toBeTruthy();
      expect(answerPanel.textContent).toContain(item.answer);
    }
  });
});

describe('ASD-STE100 and zero-codename compliance', () => {
  const CODENAME_REGEX = /\b(eri|coven|haro|vanadis|synapse|prospera|senses|pjev|docket-mcp|docket-store)\b/i;
  const PORT_REGEX = /:[0-9]{2,5}\b/;

  it('verifies FAQ_ITEMS contain zero internal codenames and zero port numbers', () => {
    for (const item of FAQ_ITEMS) {
      expect(item.question).not.toMatch(CODENAME_REGEX);
      expect(item.question).not.toMatch(PORT_REGEX);
      expect(item.answer).not.toMatch(CODENAME_REGEX);
      expect(item.answer).not.toMatch(PORT_REGEX);
      expect(item.category).not.toMatch(CODENAME_REGEX);
      expect(item.category).not.toMatch(PORT_REGEX);
    }
  });

  it('verifies FAQ_ITEMS conform to ASD-STE100 sentence length (<= 25 words per sentence)', () => {
    for (const item of FAQ_ITEMS) {
      // Split answer into sentences
      const sentences = item.answer
        .split(/(?<=[.!?])\s+/)
        .map((s) => s.trim())
        .filter((s) => s.length > 0);

      expect(sentences.length).toBeLessThanOrEqual(6);

      for (const sentence of sentences) {
        const words = sentence
          .replace(/[.,/#!$%^&*;:{}=\-_`~()?"']/g, ' ')
          .trim()
          .split(/\s+/)
          .filter(Boolean);
        expect(
          words.length,
          `Sentence in FAQ item "${item.id}" exceeds 25 words: "${sentence}" (${words.length} words)`
        ).toBeLessThanOrEqual(25);
      }

      // Check question word count
      const qWords = item.question
        .replace(/[.,/#!$%^&*;:{}=\-_`~()?"']/g, ' ')
        .trim()
        .split(/\s+/)
        .filter(Boolean);
      expect(
        qWords.length,
        `Question "${item.question}" exceeds 25 words (${qWords.length} words)`
      ).toBeLessThanOrEqual(25);
    }
  });

  it('verifies INTEGRATION_BADGES contain zero internal codenames and zero port numbers', () => {
    for (const badge of INTEGRATION_BADGES) {
      expect(badge.label).not.toMatch(CODENAME_REGEX);
      expect(badge.label).not.toMatch(PORT_REGEX);
      expect(badge.group).not.toMatch(CODENAME_REGEX);
      expect(badge.group).not.toMatch(PORT_REGEX);
    }
  });
});
