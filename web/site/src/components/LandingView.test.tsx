import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ArchitectureMap } from './ArchitectureMap';
import { ComparisonMatrix } from './ComparisonMatrix';
import { FactoryStepper } from './FactoryStepper';
import { GlassBoxShowcase } from './GlassBoxShowcase';
import { LandingView } from './LandingView';
import { QuickstartSteps } from './QuickstartSteps';

describe('LandingView Component', () => {
  afterEach(() => {
    cleanup();
  });

  it('renders landing hero, pillar features, and all showcase sections', () => {
    render(<LandingView />);

    expect(screen.getByTestId('landing-view')).toBeTruthy();
    expect(screen.getByTestId('landing-hero')).toBeTruthy();
    expect(screen.getByTestId('pillar-cards')).toBeTruthy();
    expect(screen.getByTestId('section-architecture')).toBeTruthy();
    expect(screen.getByTestId('section-factory')).toBeTruthy();
    expect(screen.getByTestId('section-showcase')).toBeTruthy();
    expect(screen.getByTestId('section-comparison')).toBeTruthy();
    expect(screen.getByTestId('section-quickstart')).toBeTruthy();
    expect(screen.getByTestId('bottom-cta')).toBeTruthy();

    expect(screen.getByTestId('hero-cta-handbook').getAttribute('href')).toBe('#/docs/overview');
    expect(screen.getByTestId('hero-cta-quickstart').getAttribute('href')).toBe('#/docs/getting-started');
  });
});

describe('ArchitectureMap Component', () => {
  afterEach(() => {
    cleanup();
  });

  it('renders 9 topology nodes and handles pillar filtering', () => {
    render(<ArchitectureMap />);

    expect(screen.getByTestId('architecture-map')).toBeTruthy();
    expect(screen.getByTestId('filter-all')).toBeTruthy();

    const nodeEri = screen.getByTestId('node-eri-restate');
    const nodeK3s = screen.getByTestId('node-k3s-nginx');
    const nodeVikunja = screen.getByTestId('node-vikunja-docket');

    expect(nodeEri).toBeTruthy();
    expect(nodeK3s).toBeTruthy();
    expect(nodeVikunja).toBeTruthy();

    // Default inspector node
    const inspector = screen.getByTestId('node-inspector');
    expect(inspector).toBeTruthy();
    expect(within(inspector).getByText('Eri + Eri-Restate')).toBeTruthy();

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
    expect(inspector.textContent).toContain(':80 / :443');

    // Select with Enter key
    const nodeVikunja = screen.getByTestId('node-vikunja-docket');
    fireEvent.keyDown(nodeVikunja, { key: 'Enter' });
    expect(inspector.textContent).toContain('Vikunja + Docket MCP');

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

    fireEvent.click(screen.getByTestId('card-task-103'));
    expect(screen.getByTestId('card-task-103').classList.contains('selected')).toBe(true);

    fireEvent.click(screen.getByTestId('card-task-102'));
    expect(screen.getByTestId('card-task-102').classList.contains('selected')).toBe(true);

    fireEvent.click(screen.getByTestId('card-task-101'));
    expect(screen.getByTestId('card-task-101').classList.contains('selected')).toBe(true);

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

    expect(screen.getByTestId('factory-stepper')).toBeTruthy();
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
    expect(detail.textContent).toContain('Stage 3: Vanadis + Restate + NATS Multi-PR Pipeline');

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
    expect(screen.getByText(/Autonomous Self-Healing Loop Active/i)).toBeTruthy();

    fireEvent.click(toggleBtn);
    expect(screen.queryByTestId('remediation-banner')).toBeNull();
  });
});

describe('ComparisonMatrix Component', () => {
  afterEach(() => {
    cleanup();
  });

  it('filters competitors by category and search keyword', () => {
    render(<ComparisonMatrix />);

    expect(screen.getByTestId('comparison-matrix')).toBeTruthy();

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

    // Clear search
    fireEvent.change(searchInput, { target: { value: '' } });
  });

  it('switches between Cards and Matrix table view modes', () => {
    render(<ComparisonMatrix />);

    expect(screen.getByTestId('competitor-cards')).toBeTruthy();
    expect(screen.queryByTestId('comparison-table')).toBeNull();

    // Switch to Matrix view
    fireEvent.click(screen.getByTestId('view-matrix-btn'));
    expect(screen.getByTestId('comparison-table')).toBeTruthy();
    expect(screen.queryByTestId('competitor-cards')).toBeNull();
    expect(screen.getByTestId('matrix-row-0')).toBeTruthy();

    // Switch back to Cards view
    fireEvent.click(screen.getByTestId('view-cards-btn'));
    expect(screen.getByTestId('competitor-cards')).toBeTruthy();
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

  it('switches steps and copies command to clipboard', async () => {
    const writeTextMock = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, {
      clipboard: {
        writeText: writeTextMock,
      },
    });

    render(<QuickstartSteps />);

    expect(screen.getByTestId('quickstart-steps')).toBeTruthy();
    expect(screen.getByTestId('step-card-1')).toBeTruthy();

    // Click copy button on Step 1
    const copyBtn1 = screen.getByTestId('copy-btn-1');
    await act(async () => {
      fireEvent.click(copyBtn1);
    });

    expect(writeTextMock).toHaveBeenCalledWith(expect.stringContaining('curl -sfL https://get.k3s.io'));
    expect(screen.getByText('Copied to Clipboard!')).toBeTruthy();

    // Advance timer to test timeout reset
    act(() => {
      vi.advanceTimersByTime(3000);
    });
    expect(screen.getByText('Copy Command')).toBeTruthy();

    // Switch to Step 2
    fireEvent.click(screen.getByTestId('quick-tab-2'));
    expect(screen.getByTestId('step-card-2')).toBeTruthy();
    expect(screen.queryByTestId('step-card-1')).toBeNull();

    // Switch to Step 3
    fireEvent.click(screen.getByTestId('quick-tab-3'));
    expect(screen.getByTestId('step-card-3')).toBeTruthy();

    // Switch to Step 4
    fireEvent.click(screen.getByTestId('quick-tab-4'));
    expect(screen.getByTestId('step-card-4')).toBeTruthy();
  });

  it('handles clipboard failure gracefully', async () => {
    const writeTextMock = vi.fn().mockRejectedValue(new Error('Permission denied'));
    Object.assign(navigator, {
      clipboard: {
        writeText: writeTextMock,
      },
    });

    render(<QuickstartSteps />);

    const copyBtn = screen.getByTestId('copy-btn-1');
    await act(async () => {
      fireEvent.click(copyBtn);
    });

    expect(screen.getByText('Copied to Clipboard!')).toBeTruthy();
  });

  it('handles consecutive copies and missing clipboard API gracefully', async () => {
    // Delete clipboard
    // @ts-expect-error test undefined clipboard
    delete navigator.clipboard;

    render(<QuickstartSteps />);

    const copyBtn = screen.getByTestId('copy-btn-1');
    await act(async () => {
      fireEvent.click(copyBtn);
    });

    expect(screen.getByText('Copied to Clipboard!')).toBeTruthy();

    // Advance timer partially
    act(() => {
      vi.advanceTimersByTime(1000);
    });

    // Switch step while copied
    fireEvent.click(screen.getByTestId('quick-tab-2'));
    const copyBtn2 = screen.getByTestId('copy-btn-2');
    await act(async () => {
      fireEvent.click(copyBtn2);
    });

    // Advance timer so first step's timer fires (id 1 !== id 2)
    act(() => {
      vi.advanceTimersByTime(1600);
    });
    expect(screen.getByText('Copied to Clipboard!')).toBeTruthy();

    // Advance timer so second step's timer fires
    act(() => {
      vi.advanceTimersByTime(2000);
    });
    expect(screen.getByText('Copy Command')).toBeTruthy();
  });
});
