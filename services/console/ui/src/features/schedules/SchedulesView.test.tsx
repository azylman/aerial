import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { Profiler, type ProfilerOnRenderCallback } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type {
  CronSchedule,
  OneShotSchedule,
  ScheduleRun,
  ScheduleRunsAPIResponse,
  SchedulesAPIResponse,
} from '../../api/types';
import { useKeyboardShortcuts } from '../../hooks/useKeyboardShortcuts';
import { useConsoleStore } from '../../store/store';
import { recordServerTime } from './clock';
import { CountdownBadge } from './Countdown';
import { syncScheduleRuns, syncSchedules } from './data';
import SchedulesView from './SchedulesView';

// Every Icon render is recorded by name, which pins down exactly which components re-rendered:
// countdown leaves render only the `timer` glyph for the fixtures below, cards render the
// alarm/calendar/clipboard/target glyphs and run cards their status glyphs.
const iconRenders = vi.hoisted((): string[] => []);
vi.mock('../../components/Icon', () => ({
  Icon: ({ name }: { name: string }) => {
    iconRenders.push(name);
    return <svg data-icon={name} />;
  },
}));

// Wall-clock fixtures are built from local-time components so expectations hold in any TZ.
const at = (h: number, mi: number, sec = 0): string => new Date(2026, 10, 8, h, mi, sec).toISOString();
const NOW = Date.parse(at(12, 0));

const cron = (id: string, over: Partial<CronSchedule> = {}): CronSchedule => ({
  id,
  target_id: `2000${id}`,
  channel_id: `3000${id}`,
  title_prefix: `Routine ${id}`,
  cron_expr: '0 9 * * *',
  cron_description: 'Every day at 9:00 AM',
  prompt: `Run routine ${id}`,
  timezone: 'UTC',
  next_run_at: at(12, 10),
  enabled: true,
  created_at: '2026-11-01T00:00:00Z',
  ...over,
});

const once = (id: string, over: Partial<OneShotSchedule> = {}): OneShotSchedule => ({
  id,
  thread_id: `4000000000${id.length}`,
  prompt: `Reminder ${id}`,
  run_at: at(12, 30),
  created_at: at(0, 0),
  ...over,
});

const run = (id: string, over: Partial<ScheduleRun> = {}): ScheduleRun => ({
  id,
  schedule_id: 'cron-a',
  schedule_type: 'cron',
  title: `Run ${id}`,
  target_id: '200',
  thread_id: '600000000000000000',
  prompt: `Prompt ${id}`,
  status: 'completed',
  started_at: at(11, 0),
  duration_ms: 25564,
  ...over,
});

const CRONS: readonly CronSchedule[] = [
  cron('cron-a', { title_prefix: 'Morning standup digest' }),
  cron('cron-b', { title_prefix: 'Garden check', prompt: 'Check the garden sensors', timezone: 'America/Los_Angeles' }),
  cron('cron-c', {
    title_prefix: '',
    cron_description: '',
    cron_expr: '*/15 * * * *',
    channel_id: '',
    target_id: '',
    enabled: false,
  }),
];
const ONE_SHOTS: readonly OneShotSchedule[] = [
  once('once-a', { prompt: 'Water the garden basil', thread_id: '400000000000000000' }),
  once('once-b', { thread_id: 'not-a-thread' }),
];
const RUNS: readonly ScheduleRun[] = [
  run('run-1', { status: 'running', duration_ms: 0 }),
  run('run-2', { error: 'timeout talking to brain', status: 'failed' }),
  run('run-3', {
    title: '',
    schedule_id: '',
    thread_id: '',
    target_id: '',
    schedule_type: '',
    prompt: 'garden digest',
  }),
];

const schedules = (crons = CRONS, oneShots = ONE_SHOTS): SchedulesAPIResponse => ({
  status: 'ok',
  system_time: at(12, 0),
  summary: {
    total_active: crons.length + oneShots.length,
    cron_count: crons.length,
    one_shot_count: oneShots.length,
    total_runs_24h: 50,
    success_rate_24h: 94,
  },
  crons,
  one_shots: oneShots,
});
const runs = (list = RUNS, total = list.length): ScheduleRunsAPIResponse => ({
  status: 'ok',
  total,
  limit: 50,
  offset: 0,
  runs: list,
});

const seed = (s: SchedulesAPIResponse | null, r: ScheduleRunsAPIResponse | null): void =>
  act(() => {
    if (s !== null) useConsoleStore.getState().setSchedules(s);
    if (r !== null) useConsoleStore.getState().setScheduleRuns(r);
  });

const fetchMock = vi.fn<(url: string) => Promise<Response>>();
const pending = (): Promise<Response> => new Promise(() => undefined);
const json = (status: number, body: unknown): Promise<Response> =>
  Promise.resolve(new Response(JSON.stringify(body), { status }));

beforeEach(() => {
  useConsoleStore.setState(useConsoleStore.getInitialState(), true);
  iconRenders.length = 0;
  fetchMock.mockReset();
  fetchMock.mockImplementation(pending);
  vi.stubGlobal('fetch', fetchMock);
  vi.useFakeTimers({ now: NOW, toFake: ['setInterval', 'clearInterval', 'Date'] });
});
afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
  recordServerTime(at(12, 0), NOW);
});

const texts = (container: HTMLElement, selector: string): string[] =>
  [...container.querySelectorAll(selector)].map((el) => (el.textContent ?? '').replace(/\s+/g, ' ').trim());

describe('SchedulesView list lifecycle', () => {
  it.each([
    {
      name: 'loading before the first answer',
      list: null,
      error: null,
      wantSkeletons: 4,
      wantAlert: 0,
      wantBanner: 0,
      wantCards: 0,
      wantEmpty: 0,
      wantResults: 'SHOWING 0 / 0 ITEMS',
    },
    {
      name: 'error with nothing cached',
      list: null,
      error: 'Brain offline',
      wantSkeletons: 0,
      wantAlert: 1,
      wantBanner: 0,
      wantCards: 0,
      wantEmpty: 0,
      wantResults: 'SHOWING 0 / 0 ITEMS',
    },
    {
      name: 'error over an empty cache',
      list: schedules([], []),
      error: 'Brain offline',
      wantSkeletons: 0,
      wantAlert: 1,
      wantBanner: 0,
      wantCards: 0,
      wantEmpty: 0,
      wantResults: 'SHOWING 0 / 0 ITEMS',
    },
    {
      name: 'stale cache keeps cards under the banner',
      list: schedules(),
      error: 'Brain offline',
      wantSkeletons: 0,
      wantAlert: 0,
      wantBanner: 1,
      wantCards: 5,
      wantEmpty: 0,
      wantResults: 'SHOWING 5 / 5 ACTIVE ROUTINES',
    },
    {
      name: 'nothing scheduled',
      list: schedules([], []),
      error: null,
      wantSkeletons: 0,
      wantAlert: 0,
      wantBanner: 0,
      wantCards: 0,
      wantEmpty: 1,
      wantResults: 'SHOWING 0 / 0 ACTIVE ROUTINES',
    },
  ])('$name', ({ list, error, wantSkeletons, wantAlert, wantBanner, wantCards, wantEmpty, wantResults }) => {
    const { container } = render(<SchedulesView />);
    act(() => {
      if (list !== null) useConsoleStore.getState().setSchedules(list);
      if (error !== null) useConsoleStore.getState().failSchedules(error);
    });
    expect([
      container.querySelectorAll('.fact-card-skeleton').length,
      texts(container, '.permet-alert-box p').length,
      container.querySelectorAll('.reconnect-banner').length,
      container.querySelectorAll('.schedule-card').length,
      container.querySelectorAll('#schedules-grid .empty-state-box').length,
      screen.getByText(/^SHOWING/).textContent,
    ]).toEqual([wantSkeletons, wantAlert, wantBanner, wantCards, wantEmpty, wantResults]);
    if (wantAlert > 0) expect(texts(container, '.permet-alert-box p')).toEqual(['Brain offline']);
  });

  it.each([
    { name: 'retry button', button: 'RETRY SYNCHRONIZATION' },
    { name: 'sync button', button: 'SYNC SCHEDULES' },
  ])('$name polls schedules and runs again', async ({ button }) => {
    // The mount polls settle first (schedules failing): a click while they are in flight joins them.
    fetchMock.mockImplementation((url) =>
      url.startsWith('/api/schedules/runs') ? json(200, runs()) : json(503, { error: 'Brain offline' }),
    );
    render(<SchedulesView />);
    await act(() =>
      vi.waitFor(() => {
        const { list, runs: feed } = useConsoleStore.getState().schedules;
        expect([list.error, feed.data === null]).toEqual(['Brain offline', false]);
      }),
    );
    fetchMock.mockImplementation(pending);
    fetchMock.mockClear();
    fireEvent.click(screen.getByRole('button', { name: new RegExp(button) }));
    fireEvent.click(screen.getByRole('button', { name: new RegExp(button) }));
    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      '/api/schedules',
      '/api/schedules/runs?limit=50&offset=0',
      '/api/schedules',
      '/api/schedules/runs?limit=50&offset=0',
    ]);
  });

  // Responses settle so the poller is idle when the interval fires (it skips ticks while a
  // request is in flight); the 9.999s row pins the cadence to exactly 10s.
  it.each([
    { name: 'mount only, just before the interval', elapsedMs: 9_999, wantRounds: 1 },
    { name: 'mount plus one 10s tick', elapsedMs: 10_000, wantRounds: 2 },
    { name: 'mount plus two 10s ticks', elapsedMs: 20_000, wantRounds: 3 },
  ])('polls both endpoints: $name', async ({ elapsedMs, wantRounds }) => {
    fetchMock.mockImplementation((url) => json(200, url.startsWith('/api/schedules/runs') ? runs() : schedules()));
    render(<SchedulesView />);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(elapsedMs);
    });
    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual(
      Array.from({ length: wantRounds }, () => ['/api/schedules', '/api/schedules/runs?limit=50&offset=0']).flat(),
    );
  });
});

describe('SchedulesView content', () => {
  it('renders metrics, cards and runs field for field', () => {
    const { container } = render(<SchedulesView />);
    seed(schedules(), runs(RUNS, 120));
    expect(texts(container, '.summary-card .value')).toEqual(['3', '2', 'in 10m 0s', '94.0%']);
    expect(texts(container, '.summary-card .sub-label')).toEqual([
      'RECURRING ROUTINES',
      'ONE-SHOT REMINDERS',
      'UPCOMING RUN',
      '50 RUNS LOGGED (24H)',
    ]);
    expect(texts(container, '.cat-pill')).toEqual(['ALL 5', 'CRON ROUTINES 3', 'ONE-SHOT 2', 'RUN AUDIT LOG 120']);
    expect(texts(container, '.section-badge')).toEqual(['5 ACTIVE', '3 RUNS']);

    const card = (el: Element) => ({
      className: el.className,
      badges: texts(el as HTMLElement, '.schedule-badge-group > span'),
      dot: el.querySelector('.pulse-dot')?.className,
      countdown: texts(el as HTMLElement, '.schedule-countdown-badge'),
      title: texts(el as HTMLElement, '.schedule-title'),
      human: texts(el as HTMLElement, '.schedule-cron-human'),
      meta: texts(el as HTMLElement, '.schedule-meta-item'),
      link: el.querySelector('.schedule-thread-link')?.getAttribute('href') ?? null,
      prompt: el.querySelector('.schedule-prompt-body')?.textContent,
      footer: texts(el as HTMLElement, '.schedule-card-footer > span'),
    });
    expect([...container.querySelectorAll('.schedule-card')].map(card)).toEqual([
      {
        className: 'schedule-card cron-card',
        badges: ['CRON', 'UTC'],
        dot: 'pulse-dot healthy',
        countdown: ['in 10m 0s'],
        title: ['Morning standup digest'],
        human: ['Every day at 9:00 AM'],
        meta: ['CRON: 0 9 * * *', 'TARGET: 3000cron-a'],
        link: null,
        prompt: '\n                        Run routine cron-a\n                    ',
        footer: ['NEXT: Nov 8 12:10:00 PM', 'ID: cron-a'],
      },
      {
        className: 'schedule-card cron-card',
        badges: ['CRON', 'America/Los_Angeles'],
        dot: 'pulse-dot healthy',
        countdown: ['in 10m 0s'],
        title: ['Garden check'],
        human: ['Every day at 9:00 AM'],
        meta: ['CRON: 0 9 * * *', 'TARGET: 3000cron-b'],
        link: null,
        prompt: '\n                        Check the garden sensors\n                    ',
        footer: ['NEXT: Nov 8 12:10:00 PM', 'ID: cron-b'],
      },
      {
        className: 'schedule-card cron-card',
        badges: ['CRON', 'UTC'],
        dot: 'pulse-dot unhealthy',
        countdown: ['in 10m 0s'],
        title: ['RECURRING ROUTINE'],
        human: ['Every 15 minutes'],
        meta: ['CRON: */15 * * * *', 'TARGET: N/A'],
        link: null,
        prompt: '\n                        Run routine cron-c\n                    ',
        footer: ['NEXT: Nov 8 12:10:00 PM', 'ID: cron-c'],
      },
      {
        className: 'schedule-card oneshot-card',
        badges: ['ONE-SHOT'],
        dot: 'pulse-dot healthy',
        countdown: ['in 30m 0s'],
        title: ['ONE-TIME REMINDER'],
        human: ['Single Execution Timer'],
        meta: ['THREAD: #40000000 ↗'],
        link: '/agentsview/?thread=400000000000000000',
        prompt: '\n                        Water the garden basil\n                    ',
        footer: ['RUN AT: Nov 8 12:30:00 PM', 'ID: once-a'],
      },
      {
        className: 'schedule-card oneshot-card',
        badges: ['ONE-SHOT'],
        dot: 'pulse-dot healthy',
        countdown: ['in 30m 0s'],
        title: ['ONE-TIME REMINDER'],
        human: ['Single Execution Timer'],
        meta: ['THREAD: not-a-thread'],
        link: null,
        prompt: '\n                        Reminder once-b\n                    ',
        footer: ['RUN AT: Nov 8 12:30:00 PM', 'ID: once-b'],
      },
    ]);

    const runCard = (el: Element) => ({
      className: el.className,
      header: texts(el as HTMLElement, '.run-header-left > span, .run-header-right > span'),
      chip: el.querySelector('.run-status-chip')?.className,
      error: texts(el as HTMLElement, '.run-error-box'),
      footer: texts(el as HTMLElement, '.run-card-footer span'),
    });
    expect([...container.querySelectorAll('.run-card')].map(runCard)).toEqual([
      {
        className: 'run-card run-running',
        header: ['RUNNING', 'Run run-1', 'CRON', '1h AGO'],
        chip: 'run-status-chip chip-running',
        error: [],
        footer: ['TARGET: 200 • THREAD #60000000 ↗', 'RUN ID: run-1'],
      },
      {
        className: 'run-card run-failed',
        header: ['FAILED', 'Run run-2', 'CRON', '25.6s', '1h AGO'],
        chip: 'run-status-chip chip-failed',
        error: ['ERROR: timeout talking to brain'],
        footer: ['TARGET: 200 • THREAD #60000000 ↗', 'RUN ID: run-2'],
      },
      {
        className: 'run-card run-completed',
        header: ['COMPLETED', 'Schedule Run', 'CRON', '25.6s', '1h AGO'],
        chip: 'run-status-chip chip-completed',
        error: [],
        footer: ['TARGET: N/A', 'RUN ID: run-3'],
      },
    ]);
  });

  it('shows the empty run feed only once runs have loaded', () => {
    const { container } = render(<SchedulesView />);
    seed(schedules(), null);
    expect(container.querySelector('#runs-feed-container')?.childElementCount).toBe(0);
    seed(null, runs([], 0));
    expect(texts(container, '#runs-feed-container .empty-state-title')).toEqual(['NO RECENT EXECUTION RUNS']);
    expect(texts(container, '.cat-pill')[3]).toBe('RUN AUDIT LOG 0');
  });

  it.each([
    { name: 'healthy', rate: 94, want: 'value text-success' },
    { name: 'amber', rate: 80, want: 'value schedules-rate-amber' },
    { name: 'failing', rate: 50, want: 'value text-danger' },
  ])('success rate tone: $name', ({ rate, want }) => {
    const { container } = render(<SchedulesView />);
    const base = schedules();
    seed({ ...base, summary: { ...base.summary, success_rate_24h: rate } }, null);
    expect(container.querySelector('#schedules-success-rate')?.className).toBe(want);
  });

  it('shows NO UPCOMING when nothing is scheduled', () => {
    const { container } = render(<SchedulesView />);
    seed(schedules([], []), null);
    expect(texts(container, '#schedules-next-trigger')).toEqual(['NO UPCOMING']);
  });
});

const ids = (container: HTMLElement): string[] => texts(container, '.schedule-id-chip code');

describe('SchedulesView filters', () => {
  it.each([
    {
      name: 'all',
      pill: null,
      search: '',
      wantIds: ['cron-a', 'cron-b', 'cron-c', 'once-a', 'once-b'],
      wantRuns: 3,
      wantGrid: true,
      wantFeed: true,
      wantResults: 'SHOWING 5 / 5 ACTIVE ROUTINES',
    },
    {
      name: 'cron pill',
      pill: 'CRON ROUTINES',
      search: '',
      wantIds: ['cron-a', 'cron-b', 'cron-c'],
      wantRuns: 0,
      wantGrid: true,
      wantFeed: false,
      wantResults: 'SHOWING 3 / 5 ACTIVE ROUTINES',
    },
    {
      name: 'one-shot pill',
      pill: 'ONE-SHOT',
      search: '',
      wantIds: ['once-a', 'once-b'],
      wantRuns: 0,
      wantGrid: true,
      wantFeed: false,
      wantResults: 'SHOWING 2 / 5 ACTIVE ROUTINES',
    },
    {
      name: 'runs pill',
      pill: 'RUN AUDIT LOG',
      search: '',
      wantIds: [],
      wantRuns: 3,
      wantGrid: false,
      wantFeed: true,
      wantResults: 'SHOWING 5 / 5 ACTIVE ROUTINES',
    },
    {
      name: 'search across kinds',
      pill: null,
      search: '  GARDEN ',
      wantIds: ['cron-b', 'once-a'],
      wantRuns: 1,
      wantGrid: true,
      wantFeed: true,
      wantResults: 'SHOWING 2 / 5 ACTIVE ROUTINES',
    },
    {
      name: 'search by timezone',
      pill: null,
      search: 'los_angeles',
      wantIds: ['cron-b'],
      wantRuns: 0,
      wantGrid: true,
      wantFeed: true,
      wantResults: 'SHOWING 1 / 5 ACTIVE ROUTINES',
    },
    {
      name: 'search plus one-shot pill',
      pill: 'ONE-SHOT',
      search: 'garden',
      wantIds: ['once-a'],
      wantRuns: 0,
      wantGrid: true,
      wantFeed: false,
      wantResults: 'SHOWING 1 / 5 ACTIVE ROUTINES',
    },
    {
      name: 'search with no match',
      pill: null,
      search: 'zzz',
      wantIds: [],
      wantRuns: 0,
      wantGrid: true,
      wantFeed: true,
      wantResults: 'SHOWING 0 / 5 ACTIVE ROUTINES',
    },
  ])('$name', ({ pill, search, wantIds, wantRuns, wantGrid, wantFeed, wantResults }) => {
    const { container } = render(<SchedulesView />);
    seed(schedules(), runs());
    if (pill !== null) fireEvent.click(screen.getByRole('button', { name: new RegExp(`^${pill}`) }));
    if (search)
      fireEvent.change(screen.getByRole('textbox', { name: 'Search schedules' }), { target: { value: search } });
    expect({
      ids: ids(container),
      runs: container.querySelectorAll('.run-card').length,
      grid: container.querySelector('#schedules-grid') !== null,
      feed: container.querySelector('#runs-feed-container') !== null,
      results: screen.getByText(/^SHOWING/).textContent,
      active: texts(container, '.cat-pill.active'),
    }).toEqual({
      ids: wantIds,
      runs: wantRuns,
      grid: wantGrid,
      feed: wantFeed,
      results: wantResults,
      active: [texts(container, '.cat-pill').find((t) => t.startsWith(pill ?? 'ALL'))],
    });
  });

  it('highlights matches and clears them with the clear button', () => {
    const { container } = render(<SchedulesView />);
    seed(schedules(), runs());
    const input = screen.getByRole<HTMLInputElement>('textbox', { name: 'Search schedules' });
    fireEvent.change(input, { target: { value: 'Garden' } });
    expect(texts(container, 'mark.cyber-highlight')).toEqual(['Garden', 'garden', 'garden', 'garden']);
    fireEvent.click(screen.getByRole('button', { name: 'Clear search' }));
    expect([
      input.value,
      document.activeElement === input,
      container.querySelectorAll('mark').length,
      ids(container).length,
    ]).toEqual(['', true, 0, 5]);
    expect(screen.queryByRole('button', { name: 'Clear search' })).toBeNull();
  });

  // A query change reaches only the toolbar (search, clear and sync icons) and the highlight
  // leaves: cards that stay visible keep their DOM node and their bodies do not re-render.
  it.each([
    {
      name: 'narrowing',
      from: 'g',
      to: 'garden',
      wantIds: ['cron-b', 'once-a'],
      wantMarks: ['Garden', 'garden', 'garden'],
    },
    {
      name: 'same normalized query',
      from: 'garden',
      to: '  GARDEN ',
      wantIds: ['cron-b', 'once-a'],
      wantMarks: ['Garden', 'garden', 'garden'],
    },
  ])('$name keeps visible cards in place', ({ from, to, wantIds, wantMarks }) => {
    const { container } = render(<SchedulesView />);
    seed(schedules(), runs());
    const input = screen.getByRole<HTMLInputElement>('textbox', { name: 'Search schedules' });
    fireEvent.change(input, { target: { value: from } });
    const cardFor = (id: string) =>
      [...container.querySelectorAll('.schedule-card')].find((el) => el.textContent?.includes(`ID: ${id}`));
    const before = cardFor('cron-b');
    iconRenders.length = 0;
    fireEvent.change(input, { target: { value: to } });
    expect({
      ids: ids(container),
      sameNode: cardFor('cron-b') === before,
      marks: texts(container, '#schedules-grid mark.cyber-highlight'),
      icons: iconRenders,
    }).toEqual({ ids: wantIds, sameNode: true, marks: wantMarks, icons: ['search', 'x', 'refresh'] });
  });

  it("'/' focuses the search box and Escape clears it", () => {
    function Harness() {
      useKeyboardShortcuts('schedules');
      return <SchedulesView />;
    }
    const { container } = render(<Harness />);
    seed(schedules(), runs());
    const input = screen.getByRole<HTMLInputElement>('textbox', { name: 'Search schedules' });
    fireEvent.keyDown(window, { key: '/' });
    expect(document.activeElement).toBe(input);
    fireEvent.change(input, { target: { value: 'zzz' } });
    expect(ids(container)).toEqual([]);
    fireEvent.keyDown(window, { key: 'Escape' });
    expect([input.value, document.activeElement === input, ids(container).length]).toEqual(['', false, 5]);
  });
});

describe('SchedulesView render isolation', () => {
  it.each([
    // 5 card countdowns + the NEXT TRIGGER value, all in the `timer` state.
    {
      name: '1s tick re-renders only the countdown leaves',
      advanceMs: 1000,
      next: null,
      wantIcons: ['timer', 'timer', 'timer', 'timer', 'timer', 'timer'],
    },
    { name: 'identical payload renders nothing', advanceMs: 0, next: schedules(), wantIcons: [] },
    {
      name: 'one changed cron re-renders only its card',
      advanceMs: 0,
      next: schedules(CRONS.map((c) => (c.id === 'cron-b' ? { ...c, prompt: 'changed' } : c))),
      wantIcons: ['alarm', 'calendar', 'clipboard'],
    },
  ])('$name', ({ advanceMs, next, wantIcons }) => {
    render(<SchedulesView />);
    seed(schedules(), runs());
    iconRenders.length = 0;
    act(() => {
      vi.advanceTimersByTime(advanceMs);
      if (next !== null) {
        useConsoleStore.getState().setSchedules(structuredClone(next));
        useConsoleStore.getState().setScheduleRuns(structuredClone(runs()));
      }
    });
    expect(iconRenders).toEqual(wantIcons);
  });

  it('identical polls commit nothing and a schedules update leaves the runs feed alone', () => {
    const commits: string[] = [];
    const onRender: ProfilerOnRenderCallback = (id) => commits.push(id);
    render(
      <Profiler id="view" onRender={onRender}>
        <SchedulesView />
      </Profiler>,
    );
    seed(schedules(), runs());
    commits.length = 0;
    seed(structuredClone(schedules()), structuredClone(runs()));
    expect(commits).toEqual([]);

    iconRenders.length = 0;
    seed(schedules(CRONS.slice(0, 1)), null);
    expect(iconRenders.filter((n) => ['sync', 'alert', 'bolt'].includes(n))).toEqual([]);
  });
});

describe('server clock drift', () => {
  it.each([
    { name: 'server 20s ahead', systemTime: at(12, 0, 20), want: 'in 10s' },
    { name: 'in sync', systemTime: at(12, 0), want: 'in 30s' },
    { name: 'unparseable server time is ignored', systemTime: 'garbage', want: 'in 30s' },
  ])('$name', ({ systemTime, want }) => {
    const { container } = render(<CountdownBadge target={at(12, 0, 30)} />);
    act(() => recordServerTime(systemTime, NOW));
    expect(container.textContent?.trim()).toBe(want);
  });
});

describe('schedule polls', () => {
  it.each([
    {
      name: 'schedules success',
      sync: 'schedules',
      status: 200,
      body: schedules(),
      want: { list: true, listError: null, runs: false, runsError: null },
    },
    {
      name: 'schedules failure',
      sync: 'schedules',
      status: 503,
      body: { error: 'brain down' },
      want: { list: false, listError: 'brain down', runs: false, runsError: null },
    },
    {
      name: 'runs success',
      sync: 'runs',
      status: 200,
      body: runs(),
      want: { list: false, listError: null, runs: true, runsError: null },
    },
    {
      name: 'runs failure',
      sync: 'runs',
      status: 500,
      body: {},
      want: { list: false, listError: null, runs: false, runsError: 'HTTP 500' },
    },
  ] as const)('$name', async ({ sync, status, body, want }) => {
    fetchMock.mockImplementation(() => json(status, body));
    await (sync === 'schedules' ? syncSchedules : syncScheduleRuns)(new AbortController().signal);
    const s = useConsoleStore.getState().schedules;
    expect({
      list: s.list.data !== null,
      listError: s.list.error,
      runs: s.runs.data !== null,
      runsError: s.runs.error,
    }).toEqual(want);
  });
});
