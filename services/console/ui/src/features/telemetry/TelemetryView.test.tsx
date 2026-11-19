import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { Profiler, type ProfilerOnRenderCallback } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { BuildsAPIResponse, ClusterResponse, DeploymentStatus, SchedulesAPIResponse, ServiceStatus } from '../../api/types';
import { useConsoleStore } from '../../store/store';
import { Deployments, DeployBadge } from './Deployments';
import { openServiceDrawer } from './openService';
import { ServiceDiagnostics } from './ServiceDiagnostics';
import { HealthyCount, ServiceCard, ServicesGrid } from './ServicesGrid';
import TelemetryView, { OverviewBuildsSection, OverviewSchedulesSection } from './TelemetryView';

const svc = (name: string, status: string, uptime: number): ServiceStatus => ({
  name,
  status,
  uptime_seconds: uptime,
  last_check_time: '2026-11-08T12:00:00Z',
});

const SERVICES: readonly ServiceStatus[] = [
  svc('aerial-eri', 'healthy', 3600),
  svc('aerial-vision', 'degraded', 43871),
  svc('aerial-kernel-mcp', 'down', 0),
  svc('aerial-console', 'healthy', 125),
];

const DEPLOYS: readonly DeploymentStatus[] = [
  {
    id: 'dep-console',
    service: 'console',
    repository: 'azylman/aerial',
    commit: 'a1b2c3d',
    commit_msg: 'feat: shell',
    stage: 'swapping',
    progress: 90,
    steps: [
      { name: 'CI Build & GHCR', icon: '⚙️', status: 'completed' },
      { name: 'Container Swap', icon: '🔄', status: 'active' },
    ],
    matrix_jobs: [
      { name: 'lint', status: 'completed', type: 'ci', duration: '31s' },
      { name: 'aerial-console', status: 'active', type: 'container' },
    ],
    started_at: '2026-11-08T11:57:00Z',
  },
  { id: 'dep-eri', service: 'eri', commit: 'b2c3d4e', commit_msg: 'fix(eri): retry', stage: 'live', progress: 100, steps: null, started_at: '2026-11-08T11:00:00Z' },
  { id: 'dep-coven', service: 'coven', commit: 'd4e5f60', stage: 'failed', progress: 30, steps: null, started_at: '2026-11-08T11:30:00Z' },
];

const cluster = (over: Partial<ClusterResponse> = {}): ClusterResponse => ({
  system_time: '2026-11-08T12:00:00Z',
  cluster_status: 'degraded',
  active_tasks_count: 0,
  active_tasks: [],
  services: SERVICES,
  deployments: DEPLOYS,
  quick_launch_links: [],
  git_sync: { status: 'synced', max_lag_seconds: 0, last_sync_time: '2026-11-08T12:00:00Z' },
  ongoing_deploy: '',
  is_deploying: true,
  ...over,
});

const seed = (data: ClusterResponse): void => act(() => useConsoleStore.getState().setCluster(data));

const fetchMock = vi.fn<(url: string, init?: RequestInit) => Promise<Response>>();
const pending = (): Promise<Response> => new Promise(() => undefined);

beforeEach(() => {
  useConsoleStore.setState(useConsoleStore.getInitialState(), true);
  fetchMock.mockReset();
  fetchMock.mockImplementation(pending);
  vi.stubGlobal('fetch', fetchMock);
  vi.useFakeTimers({ now: Date.parse('2026-11-08T12:00:00Z'), toFake: ['setInterval', 'clearInterval', 'Date'] });
});
afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe('ServiceCard states', () => {
  it.each([
    { name: 'aerial-eri', wantDot: 'pulse-dot healthy', wantBadge: 'badge healthy', wantText: 'HEALTHY', wantUptime: '1h 0m' },
    { name: 'aerial-vision', wantDot: 'pulse-dot degraded', wantBadge: 'badge degraded', wantText: 'DEGRADED', wantUptime: '12h 11m' },
    { name: 'aerial-kernel-mcp', wantDot: 'pulse-dot down', wantBadge: 'badge down', wantText: 'DOWN', wantUptime: '0s' },
    { name: 'aerial-console', wantDot: 'pulse-dot healthy', wantBadge: 'badge healthy', wantText: 'HEALTHY', wantUptime: '2m 5s' },
  ])('$name renders $wantText', ({ name, wantDot, wantBadge, wantText, wantUptime }) => {
    render(<ServicesGrid />);
    seed(cluster());
    const card = screen.getByRole('button', { name: `${name} diagnostics` });
    expect(card.querySelector('.title')?.textContent).toBe(name);
    expect(card.querySelector('.pulse-dot')?.className).toBe(wantDot);
    const badge = card.querySelector('.badge');
    expect([badge?.className, badge?.textContent]).toEqual([wantBadge, wantText]);
    expect(within(card).getByText(wantUptime)).toBeTruthy();
  });
});

describe('ServicesGrid and HealthyCount', () => {
  it.each([
    { name: 'before the first poll', data: null, wantCards: 0, wantCount: '0 / 0 HEALTHY' },
    { name: 'fixture snapshot', data: cluster(), wantCards: 4, wantCount: '2 / 4' },
    { name: 'snapshot without services', data: cluster({ services: [] }), wantCards: 0, wantCount: '0 / 0' },
  ])('$name', ({ data, wantCards, wantCount }) => {
    const { container } = render(
      <>
        <HealthyCount />
        <ServicesGrid />
      </>,
    );
    if (data !== null) seed(data);
    expect(container.querySelectorAll('.service-card')).toHaveLength(wantCards);
    expect(container.querySelector('#active-count')?.textContent).toBe(wantCount);
  });
});

describe('service card render isolation', () => {
  const vision = SERVICES[1] as ServiceStatus;
  it.each([
    { name: 'identical payload re-renders no card', next: cluster(), want: {} },
    {
      name: 'one changed service re-renders exactly that card',
      next: cluster({ services: SERVICES.map((s) => (s.name === 'aerial-vision' ? { ...vision, status: 'healthy' } : s)) }),
      want: { 'aerial-vision': 1 },
    },
    {
      name: 'a deployment change re-renders no card',
      next: cluster({ deployments: DEPLOYS.slice(1) }),
      want: {},
    },
  ])('$name', ({ next, want }) => {
    seed(cluster());
    const renders: Record<string, number> = {};
    const onRender: ProfilerOnRenderCallback = (id, phase) => {
      if (phase !== 'mount') renders[id] = (renders[id] ?? 0) + 1;
    };
    render(
      <>
        {SERVICES.map((s) => (
          <Profiler key={s.name} id={s.name} onRender={onRender}>
            <ServiceCard name={s.name} />
          </Profiler>
        ))}
      </>,
    );
    seed(structuredClone(next));
    expect(renders).toEqual(want);
  });

  it('an identical poll commits nothing anywhere in the telemetry view', () => {
    seed(cluster());
    let commits = 0;
    render(
      <Profiler id="telemetry" onRender={() => commits++}>
        <TelemetryView />
      </Profiler>,
    );
    commits = 0;
    seed(structuredClone(cluster()));
    expect(commits).toBe(0);
  });
});

describe('opening the service drawer', () => {
  it.each([
    { name: 'click', key: null, want: { kind: 'service', name: 'aerial-vision' } },
    { name: 'Enter', key: 'Enter', want: { kind: 'service', name: 'aerial-vision' } },
    { name: 'Space', key: ' ', want: { kind: 'service', name: 'aerial-vision' } },
    { name: 'other key', key: 'a', want: { kind: 'closed' } },
  ])('card $name', ({ key, want }) => {
    render(<ServicesGrid />);
    seed(cluster());
    const card = screen.getByRole('button', { name: 'aerial-vision diagnostics' });
    act(() => {
      if (key === null) fireEvent.click(card);
      else fireEvent.keyDown(card, { key });
    });
    expect(useConsoleStore.getState().drawer).toEqual(want);
  });

  it.each([
    { query: 'vision', want: { kind: 'service', name: 'aerial-vision' } },
    { query: 'AERIAL-KERNEL-MCP', want: { kind: 'service', name: 'aerial-kernel-mcp' } },
    { query: 'lint', want: { kind: 'closed' } },
  ])('openServiceDrawer($query)', ({ query, want }) => {
    seed(cluster());
    openServiceDrawer(query);
    expect(useConsoleStore.getState().drawer).toEqual(want);
  });
});

describe('ServiceDiagnostics', () => {
  it.each([
    {
      name: 'degraded service',
      target: 'aerial-vision',
      want: ['aerial-vision', 'DEGRADED', '12h 11m', 'aerial-net:internal (Docker Bridge)'],
      wantHealthClass: 'val text-danger',
      wantLog: '> Service node aerial-vision initialized via Docker Supervisor.',
    },
    {
      name: 'healthy service',
      target: 'aerial-eri',
      want: ['aerial-eri', 'HEALTHY', '1h 0m', 'aerial-net:internal (Docker Bridge)'],
      wantHealthClass: 'val text-success',
      wantLog: '> Service node aerial-eri initialized via Docker Supervisor.',
    },
  ])('$name', ({ target, want, wantHealthClass, wantLog }) => {
    seed(cluster());
    const { container } = render(<ServiceDiagnostics target={{ name: target }} />);
    expect([...container.querySelectorAll('.diag-item .lbl')].map((e) => e.textContent)).toEqual([
      'SERVICE NAME',
      'HEALTH STATUS',
      'UPTIME CONTEXT',
      'DISCOVERY NODE',
    ]);
    expect([...container.querySelectorAll('.diag-item .val')].map((e) => e.textContent)).toEqual(want);
    expect(container.querySelectorAll('.diag-item .val')[1]?.className).toBe(wantHealthClass);
    expect([...container.querySelectorAll('.console-box > div')].map((e) => e.textContent)).toEqual([
      '[TELEMETRY LOG STREAM]',
      wantLog,
      '> Healthcheck ping: 200 OK (latency < 2ms)',
      '> Permet link established. Memory footprint nominal.',
    ]);
  });

  it('renders nothing for a service that left the snapshot', () => {
    seed(cluster());
    const { container } = render(<ServiceDiagnostics target={{ name: 'aerial-gone' }} />);
    expect(container.innerHTML).toBe('');
  });
});

describe('Deployments', () => {
  it.each([
    { name: 'before the first poll', data: null, wantBadge: 'IN SYNC', wantFull: 0, wantCompact: 0, wantIdle: 0 },
    { name: 'no deployments', data: cluster({ deployments: [] }), wantBadge: 'SYSTEM IN SYNC', wantFull: 0, wantCompact: 0, wantIdle: 1 },
    { name: 'fixture deployments', data: cluster(), wantBadge: 'CI BUILD FAILED', wantFull: 1, wantCompact: 2, wantIdle: 0 },
    {
      name: 'single live deploy without links renders one full card',
      data: cluster({ deployments: [{ id: 'd', service: 'eri', commit: 'v1-tag', stage: 'live', progress: 100, steps: null, started_at: '' }] }),
      wantBadge: 'ALL SERVICES LIVE (STACK SYNCED)',
      wantFull: 1,
      wantCompact: 0,
      wantIdle: 0,
    },
  ])('$name', ({ data, wantBadge, wantFull, wantCompact, wantIdle }) => {
    const { container } = render(
      <>
        <DeployBadge />
        <Deployments />
      </>,
    );
    if (data !== null) seed(data);
    expect(container.querySelector('#deploy-count-badge')?.textContent?.trim()).toBe(wantBadge);
    expect(container.querySelectorAll('.deploy-card')).toHaveLength(wantFull);
    expect(container.querySelectorAll('.deploy-card-compact')).toHaveLength(wantCompact);
    expect(container.querySelectorAll('.deploy-idle-card')).toHaveLength(wantIdle);
  });

  it('renders the full pipeline card', () => {
    const { container } = render(<Deployments />);
    seed(cluster());
    const card = container.querySelector('.deploy-card') as HTMLElement;
    expect(card.className).toBe('deploy-card stage-swapping');
    expect(card.querySelector('.deploy-card-laser')).toBeNull();
    expect(card.querySelector('.deploy-repo-badge')?.textContent?.trim()).toBe('azylman/aerial');
    expect(within(card).getByRole('link', { name: 'a1b2c3d ↗' }).getAttribute('href')).toBe('https://github.com/azylman/aerial/commit/a1b2c3d');
    expect(card.querySelector('.timer-text')?.textContent?.trim()).toBe('03:00s');
    expect(card.querySelector('.deploy-stage-badge')?.textContent?.trim()).toBe('SWAPPING');
    expect([...card.querySelectorAll('.step-name')].map((e) => e.textContent)).toEqual(['CI Build & GHCR', 'Container Swap']);
    expect([...card.querySelectorAll('.step-icon')].map((e) => e.textContent)).toEqual(['⚙️', '🔄']);
    expect([...card.querySelectorAll('.matrix-chip')].map((e) => e.textContent?.trim())).toEqual(['lint (31s)', 'aerial-console']);
    expect((card.querySelector('.deploy-progress-fill') as HTMLElement).style.width).toBe('90%');
    expect(within(card).getByText('"feat: shell"')).toBeTruthy();
    expect([...container.querySelectorAll('.compact-title')].map((e) => e.textContent)).toEqual(['eri • fix(eri): retry', 'coven']);
    expect([...container.querySelectorAll('.compact-badge')].map((e) => e.textContent?.trim())).toEqual(['LIVE', 'FAILED']);
  });

  it.each([
    { name: 'container chip click', chip: 'aerial-console', event: 'click', want: { kind: 'service', name: 'aerial-console' } },
    { name: 'container chip Enter', chip: 'aerial-console', event: 'enter', want: { kind: 'service', name: 'aerial-console' } },
    { name: 'chip without a service', chip: 'lint (31s)', event: 'click', want: { kind: 'closed' } },
  ])('$name', ({ chip, event, want }) => {
    render(<Deployments />);
    seed(cluster());
    const el = screen.getByRole('button', { name: chip });
    act(() => {
      if (event === 'click') fireEvent.click(el);
      else fireEvent.keyDown(el, { key: 'Enter' });
    });
    expect(useConsoleStore.getState().drawer).toEqual(want);
  });
});

const sampleBuilds = (over: Partial<BuildsAPIResponse> = {}): BuildsAPIResponse => ({
  status: 'ok',
  total: 3,
  running_count: 2,
  queued_count: 1,
  completed_count: 0,
  builds: [
    {
      id: 'build-01',
      thread_id: '1',
      tier: 1,
      title: 'Console Bento View',
      objective: 'Ship Bento',
      status: 'running',
      created_at: '2026-11-08T12:00:00Z',
      updated_at: '2026-11-08T12:00:00Z',
      total_chunks: 3,
      completed_chunks: 1,
      running_chunks: 1,
      pending_chunks: 1,
      failed_chunks: 0,
      chunks: [
        {
          id: 'chunk-1',
          build_id: 'build-01',
          chunk_index: 0,
          title: 'Wire Contract',
          status: 'completed',
          started_at: '2026-11-08T12:00:00Z',
          completed_at: '2026-11-08T12:05:00Z',
          created_at: '2026-11-08T12:00:00Z',
          updated_at: '2026-11-08T12:05:00Z',
        },
        {
          id: 'chunk-2',
          build_id: 'build-01',
          chunk_index: 1,
          title: 'Inline Build DAG',
          status: 'in_progress',
          depends_on: ['chunk-1'],
          started_at: '2026-11-08T12:05:00Z',
          completed_at: '',
          created_at: '2026-11-08T12:00:00Z',
          updated_at: '2026-11-08T12:05:00Z',
        },
        {
          id: 'chunk-3',
          build_id: 'build-01',
          chunk_index: 2,
          title: 'Unit Tests',
          status: 'pending',
          depends_on: ['chunk-2'],
          started_at: '',
          completed_at: '',
          created_at: '2026-11-08T12:00:00Z',
          updated_at: '2026-11-08T12:05:00Z',
        },
      ],
    },
    {
      id: 'build-02',
      thread_id: '2',
      tier: 2,
      title: 'Scheduler Jitter',
      objective: 'Fix jitter',
      status: 'queued',
      created_at: '2026-11-08T12:00:00Z',
      updated_at: '2026-11-08T12:00:00Z',
      total_chunks: 2,
      completed_chunks: 0,
      running_chunks: 0,
      pending_chunks: 2,
      failed_chunks: 0,
    },
    {
      id: 'build-03',
      thread_id: '3',
      tier: 1,
      title: '',
      objective: 'Build without title',
      status: 'completed',
      created_at: '2026-11-08T12:00:00Z',
      updated_at: '2026-11-08T12:00:00Z',
      total_chunks: 1,
      completed_chunks: 1,
      running_chunks: 0,
      pending_chunks: 0,
      failed_chunks: 0,
    },
  ],
  ...over,
});

const sampleSchedules = (over: Partial<SchedulesAPIResponse> = {}): SchedulesAPIResponse => ({
  status: 'ok',
  system_time: '2026-11-08T12:00:00Z',
  summary: {
    total_active: 4,
    cron_count: 4,
    one_shot_count: 0,
    total_runs_24h: 10,
    success_rate_24h: 100,
  },
  crons: [
    {
      id: 'cron-1',
      channel_id: 'ch-1',
      title_prefix: 'Morning standup',
      cron_expr: '0 9 * * *',
      cron_description: 'Every day at 9am',
      prompt: 'Standup',
      timezone: 'UTC',
      next_run_at: '2026-11-08T13:00:00Z',
      enabled: true,
      created_at: '2026-11-01T12:00:00Z',
    },
    {
      id: 'cron-2',
      channel_id: 'ch-1',
      title_prefix: '',
      cron_expr: '*/15 * * * *',
      cron_description: 'Every 15 min',
      prompt: 'Check',
      timezone: 'UTC',
      next_run_at: '2026-11-08T13:00:00Z',
      enabled: true,
      created_at: '2026-11-01T12:00:00Z',
    },
    {
      id: 'cron-3',
      channel_id: 'ch-1',
      title_prefix: 'Evening review',
      cron_expr: '0 18 * * *',
      cron_description: '',
      prompt: 'Review',
      timezone: 'UTC',
      next_run_at: '2026-11-08T18:00:00Z',
      enabled: true,
      created_at: '2026-11-01T12:00:00Z',
    },
    {
      id: 'cron-4',
      channel_id: 'ch-1',
      title_prefix: 'Weekly summary',
      cron_expr: '0 12 * * 5',
      cron_description: 'Fridays at noon',
      prompt: 'Summary',
      timezone: 'UTC',
      next_run_at: '2026-11-13T12:00:00Z',
      enabled: true,
      created_at: '2026-11-01T12:00:00Z',
    },
  ],
  one_shots: [],
  ...over,
});

const seedBuilds = (data: BuildsAPIResponse): void => act(() => useConsoleStore.getState().setBuilds(data));
const seedSchedules = (data: SchedulesAPIResponse): void => act(() => useConsoleStore.getState().setSchedules(data));

describe('OverviewBuildsSection', () => {
  it('renders idle empty state when no builds exist', () => {
    const { container } = render(<OverviewBuildsSection />);
    expect(screen.getByText('ALL BUILDS IDLE // NO QUEUED RUNS')).toBeTruthy();
    expect(container.querySelector('.section-badge')?.textContent).toBe('0 ACTIVE');
    expect(container.querySelectorAll('.overview-build-row')).toHaveLength(0);
  });

  it('renders populated build rows with active builds prioritized first, toggles inline DAG expansion on click, and navigates to builds tab', () => {
    const { container } = render(<OverviewBuildsSection />);
    seedBuilds(
      sampleBuilds({
        builds: [
          {
            id: 'build-completed-newest',
            thread_id: 'th-0',
            tier: 1,
            title: 'Recent Completed Build',
            objective: 'Done',
            status: 'completed',
            queue_state: 'completed',
            total_chunks: 2,
            completed_chunks: 2,
            running_chunks: 0,
            pending_chunks: 0,
            failed_chunks: 0,
            created_at: '2026-11-08T12:05:00Z',
            updated_at: '2026-11-08T12:10:00Z',
          },
          ...sampleBuilds().builds,
        ],
      }),
    );
    expect(container.querySelector('.section-badge')?.textContent).toBe('3 ACTIVE');
    const rows = container.querySelectorAll('.overview-build-row');
    expect(rows).toHaveLength(3);
    expect(within(rows[0] as HTMLElement).getByText('Console Bento View')).toBeTruthy();
    expect(within(rows[0] as HTMLElement).getByText('RUNNING')).toBeTruthy();
    expect(within(rows[0] as HTMLElement).getByText('1/3 chunks')).toBeTruthy();
    expect(within(rows[2] as HTMLElement).getByText('Recent Completed Build')).toBeTruthy();

    const header = rows[0]?.querySelector('.overview-build-header') as HTMLElement;
    expect(header.getAttribute('aria-expanded')).toBe('false');
    expect(container.querySelector('.overview-build-dag')).toBeNull();

    act(() => header.click());
    expect(header.getAttribute('aria-expanded')).toBe('true');
    const dag = container.querySelector('.overview-build-dag') as HTMLElement;
    expect(dag).toBeTruthy();
    expect(within(dag).getByText('Wire Contract')).toBeTruthy();
    expect(within(dag).getByText('Inline Build DAG')).toBeTruthy();

    const chunkNode = dag.querySelector('.build-dag-node[data-chunk-id="chunk-1"]') as HTMLElement;
    act(() => chunkNode.click());
    expect(useConsoleStore.getState().drawer).toEqual({ kind: 'build', id: 'build-01', chunkId: 'chunk-1' });

    act(() => header.click());
    expect(header.getAttribute('aria-expanded')).toBe('false');
    expect(container.querySelector('.overview-build-dag')).toBeNull();

    act(() => screen.getByRole('button', { name: 'Builds ↗' }).click());
    expect(useConsoleStore.getState().ui.activeTab).toBe('builds');
  });

  it('fetches detail fallback exactly once when expanding a chunk-less build, showing loading then DAG, and aborts on collapse', async () => {
    let abortObserved = false;
    fetchMock.mockImplementation((_url: string, init?: RequestInit) => {
      init?.signal?.addEventListener('abort', () => {
        abortObserved = true;
      });
      return pending();
    });

    const { container } = render(<OverviewBuildsSection />);
    seedBuilds(sampleBuilds());
    const rows = container.querySelectorAll('.overview-build-row');
    const header2 = rows[1]?.querySelector('.overview-build-header') as HTMLElement;

    // Expand chunk-less build-02
    act(() => header2.click());
    expect(header2.getAttribute('aria-expanded')).toBe('true');
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/builds/build-02');

    // Loading state is displayed while in flight
    expect(within(rows[1] as HTMLElement).getByText('LOADING BUILD CHUNKS...')).toBeTruthy();

    // Collapse while still in flight -> aborts the controller signal
    act(() => header2.click());
    expect(header2.getAttribute('aria-expanded')).toBe('false');
    expect(abortObserved).toBe(true);
  });

  it('handles racing detail fetches without overwriting the currently expanded build', async () => {
    let resolveB: ((res: Response) => void) | null = null;
    let resolveA: ((res: Response) => void) | null = null;
    fetchMock.mockImplementation((url: string) => {
      if (url.includes('build-02')) {
        return new Promise<Response>((resolve) => {
          resolveA = resolve;
        });
      }
      return new Promise<Response>((resolve) => {
        resolveB = resolve;
      });
    });

    const { container } = render(<OverviewBuildsSection />);
    seedBuilds(sampleBuilds());
    const rows = container.querySelectorAll('.overview-build-row');
    const headerA = rows[1]?.querySelector('.overview-build-header') as HTMLElement; // build-02
    const headerB = rows[2]?.querySelector('.overview-build-header') as HTMLElement; // build-03

    // Expand build A (build-02)
    act(() => headerA.click());
    expect(fetchMock).toHaveBeenCalledWith('/api/builds/build-02', expect.any(Object));

    // Switch to build B (build-03)
    act(() => headerB.click());
    expect(fetchMock).toHaveBeenCalledWith('/api/builds/build-03', expect.any(Object));

    // B resolves first with chunk B
    await act(async () => {
      resolveB?.(
        new Response(
          JSON.stringify({
            status: 'ok',
            build: {
              id: 'build-03',
              thread_id: '3',
              tier: 1,
              title: 'Build 3',
              objective: 'Build without title',
              status: 'completed',
              created_at: '2026-11-08T12:00:00Z',
              updated_at: '2026-11-08T12:00:00Z',
            },
            chunks: [
              {
                id: 'chunk-b',
                build_id: 'build-03',
                chunk_index: 0,
                title: 'Chunk B Result',
                status: 'completed',
                started_at: '2026-11-08T12:00:00Z',
                completed_at: '2026-11-08T12:05:00Z',
                created_at: '2026-11-08T12:00:00Z',
                updated_at: '2026-11-08T12:05:00Z',
              },
            ],
          }),
        ),
      );
    });
    expect(within(rows[2] as HTMLElement).getByText('Chunk B Result')).toBeTruthy();

    // A resolves late: since A was aborted when switching to B, A does not overwrite B
    await act(async () => {
      resolveA?.(
        new Response(
          JSON.stringify({
            status: 'ok',
            build: {
              id: 'build-02',
              thread_id: '2',
              tier: 2,
              title: 'Build 2',
              objective: 'Fix jitter',
              status: 'queued',
              created_at: '2026-11-08T12:00:00Z',
              updated_at: '2026-11-08T12:00:00Z',
            },
            chunks: [
              {
                id: 'chunk-a',
                build_id: 'build-02',
                chunk_index: 0,
                title: 'Chunk A Result',
                status: 'completed',
                started_at: '2026-11-08T12:00:00Z',
                completed_at: '2026-11-08T12:05:00Z',
                created_at: '2026-11-08T12:00:00Z',
                updated_at: '2026-11-08T12:05:00Z',
              },
            ],
          }),
        ),
      );
    });

    // B still shows Chunk B Result
    expect(within(rows[2] as HTMLElement).getByText('Chunk B Result')).toBeTruthy();
  });

  it('displays error state when fallback fetch fails and empty state when build has zero chunks', async () => {
    fetchMock.mockImplementation(() => Promise.reject(new Error('vanadis unavailable')));

    const { container } = render(<OverviewBuildsSection />);
    seedBuilds(sampleBuilds());
    const rows = container.querySelectorAll('.overview-build-row');
    const header2 = rows[1]?.querySelector('.overview-build-header') as HTMLElement;

    // Expand build-02: fetch fails
    await act(async () => {
      header2.click();
    });
    expect(within(rows[1] as HTMLElement).getByText('FAILED TO LOAD BUILD CHUNKS // vanadis unavailable')).toBeTruthy();

    // Resolve with legitimate 0 chunks
    act(() =>
      useConsoleStore.getState().setBuildDetail({
        status: 'ok',
        build: {
          id: 'build-02',
          thread_id: '2',
          tier: 2,
          title: 'Scheduler Jitter',
          objective: 'Fix jitter',
          status: 'queued',
          created_at: '2026-11-08T12:00:00Z',
          updated_at: '2026-11-08T12:00:00Z',
        },
        chunks: [],
      }),
    );
    expect(within(rows[1] as HTMLElement).getByText('NO CHUNKS REGISTERED')).toBeTruthy();
  });

  it('handles builds with empty chunk arrays by showing loading while fetching, error on failure, and empty state on success', async () => {
    let rejectFetch: ((err: Error) => void) | null = null;
    fetchMock.mockImplementation(
      () =>
        new Promise<Response>((_resolve, reject) => {
          rejectFetch = reject;
        }),
    );

    const { container } = render(<OverviewBuildsSection />);
    const baseBuild = sampleBuilds().builds[0] as NonNullable<typeof sampleBuilds.prototype>;
    seedBuilds(
      sampleBuilds({
        builds: [
          {
            ...baseBuild,
            id: 'build-empty-chunks',
            title: 'Build With Empty Chunks Array',
            chunks: [],
          },
        ],
      }),
    );
    const row = container.querySelector('.overview-build-row') as HTMLElement;
    const header = row.querySelector('.overview-build-header') as HTMLElement;

    // Expand build with chunks: []
    act(() => header.click());
    expect(header.getAttribute('aria-expanded')).toBe('true');
    expect(within(row).getByText('LOADING BUILD CHUNKS...')).toBeTruthy();
    expect(within(row).queryByText('NO CHUNKS REGISTERED')).toBeNull();

    // Reject the fetch -> shows error state, never empty state
    await act(async () => {
      rejectFetch?.(new Error('chunk fetch failed'));
    });
    expect(within(row).getByText('FAILED TO LOAD BUILD CHUNKS // chunk fetch failed')).toBeTruthy();
    expect(within(row).queryByText('NO CHUNKS REGISTERED')).toBeNull();

    // Re-resolve with empty chunks detail response -> now shows legitimate empty state
    act(() =>
      useConsoleStore.getState().setBuildDetail({
        status: 'ok',
        build: {
          id: 'build-empty-chunks',
          thread_id: '1',
          tier: 1,
          title: 'Build With Empty Chunks Array',
          objective: 'Test',
          status: 'running',
          created_at: '2026-11-08T12:00:00Z',
          updated_at: '2026-11-08T12:00:00Z',
        },
        chunks: [],
      }),
    );
    expect(within(row).getByText('NO CHUNKS REGISTERED')).toBeTruthy();
  });

  it('isolates detail errors from earlier failures when expanding an unrelated chunk-less build', async () => {
    // Seed an existing detail error for unrelated build-01
    act(() => useConsoleStore.getState().failBuildDetail('Build-01 network failure'));
    expect(useConsoleStore.getState().builds.detail.error).toBe('Build-01 network failure');

    // Fetch mock keeps the new request pending
    fetchMock.mockImplementation(pending);

    const { container } = render(<OverviewBuildsSection />);
    seedBuilds(sampleBuilds());
    const rows = container.querySelectorAll('.overview-build-row');
    const header2 = rows[1]?.querySelector('.overview-build-header') as HTMLElement; // build-02 (chunk-less)

    // Expand build-02: must render LOADING, never build-01's error
    act(() => header2.click());
    expect(within(rows[1] as HTMLElement).getByText('LOADING BUILD CHUNKS...')).toBeTruthy();
    expect(within(rows[1] as HTMLElement).queryByText(/Build-01 network failure/)).toBeNull();
  });
});

describe('OverviewSchedulesSection', () => {
  it('renders idle empty state when no schedules exist', () => {
    const { container } = render(<OverviewSchedulesSection />);
    expect(screen.getByText('ALL ROUTINES IDLE // NO ACTIVE CRONS')).toBeTruthy();
    expect(container.querySelector('.section-badge')?.textContent).toBe('0 ACTIVE');
    expect(container.querySelectorAll('.overview-schedule-row')).toHaveLength(0);
  });

  it('renders populated routine rows, toggles prompt expansion on click, and navigates to schedules tab', () => {
    const { container } = render(<OverviewSchedulesSection />);
    seedSchedules(sampleSchedules());
    expect(container.querySelector('.section-badge')?.textContent).toBe('4 ACTIVE');
    const rows = container.querySelectorAll('.overview-schedule-row');
    expect(rows).toHaveLength(4);
    expect(within(rows[0] as HTMLElement).getByText('Morning standup')).toBeTruthy();
    expect(within(rows[0] as HTMLElement).getByText('0 9 * * * (UTC)')).toBeTruthy();
    expect(within(rows[0] as HTMLElement).getByText('Every day at 9am')).toBeTruthy();
    expect(within(rows[1] as HTMLElement).getByText('Recurring Routine')).toBeTruthy();

    expect(within(rows[0] as HTMLElement).queryByText('Standup')).toBeNull();
    act(() => (rows[0] as HTMLElement).click());
    expect(within(rows[0] as HTMLElement).getByText('PROMPT DIRECTIVE')).toBeTruthy();
    expect(within(rows[0] as HTMLElement).getByText('Standup')).toBeTruthy();
    act(() => (rows[0] as HTMLElement).click());
    expect(within(rows[0] as HTMLElement).queryByText('Standup')).toBeNull();

    act(() => screen.getByRole('button', { name: 'Schedules ↗' }).click());
    expect(useConsoleStore.getState().ui.activeTab).toBe('schedules');
  });
});

describe('TelemetryView', () => {
  it('renders the five sections and expands to the tasks tab', () => {
    render(<TelemetryView />);
    seed(cluster());
    expect(screen.getAllByRole('heading', { level: 2 }).map((h) => h.textContent)).toEqual([
      'Active Agent Turns',
      'Vanadis Build Pipeline',
      'Upcoming Schedules',
      'Cluster Service Matrix',
      'Deployment Pipeline',
    ]);
    expect(screen.getByText('GIT SYNC: IN SYNC (Δ 0s)')).toBeTruthy();
    expect(screen.getByText('ALL WORKERS IDLE // NO PENDING TURNS')).toBeTruthy();
    act(() => screen.getByRole('button', { name: 'Tasks ↗' }).click());
    expect(useConsoleStore.getState().ui.activeTab).toBe('tasks');
  });
});
