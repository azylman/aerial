import { act, cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { ActiveTaskStatus, ClusterResponse } from '../../api/types';
import { useConsoleStore } from '../../store/store';
import { QueueBadge, TaskQueue } from './TaskQueue';
import TasksView from './TasksView';

// Every Icon render is recorded by name, which pins down exactly which components re-rendered:
// only the elapsed-ticker leaf renders the `timer` glyph, rows render trigger/status/chat glyphs.
const iconRenders = vi.hoisted((): string[] => []);
vi.mock('../../components/Icon', () => ({
  Icon: ({ name }: { name: string }) => {
    iconRenders.push(name);
    return <svg data-icon={name} />;
  },
}));

const task = (id: string, over: Partial<ActiveTaskStatus>): ActiveTaskStatus => ({
  id,
  thread_id: `10000000000000000${id}`,
  session_id: `session-${id}`,
  author_name: 'pilot',
  prompt: `Task ${id}`,
  summary: 'working',
  status: 'PROCESSING',
  retry_count: 0,
  trigger_type: 'discord',
  created_at: '2026-11-08T11:58:00Z',
  started_at: '2026-11-08T11:59:00Z',
  ...over,
});

const TASKS: readonly ActiveTaskStatus[] = [
  task('1', {}),
  task('2', { author_name: 'scheduler', trigger_type: 'cron', started_at: '2026-11-08T11:58:00Z' }),
  task('3', { retry_count: 1, trigger_type: 'reminder' }),
  task('4', { status: 'PENDING', summary: 'queued', trigger_type: 'http', session_id: '' }),
  task('5', { status: 'PENDING', summary: 'queued', thread_id: '' }),
];

const cluster = (tasks: readonly ActiveTaskStatus[]): ClusterResponse =>
  ({ system_time: '2026-11-08T12:00:00Z', active_tasks: tasks, services: [], deployments: [] }) as unknown as ClusterResponse;

const seed = (tasks: readonly ActiveTaskStatus[]): void => act(() => useConsoleStore.getState().setCluster(cluster(tasks)));

beforeEach(() => {
  useConsoleStore.setState(useConsoleStore.getInitialState(), true);
  iconRenders.length = 0;
  vi.useFakeTimers({ now: Date.parse('2026-11-08T12:00:00Z'), toFake: ['setInterval', 'clearInterval', 'Date'] });
});
afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

describe('TaskQueue', () => {
  it('renders the idle card without tasks', () => {
    render(<TaskQueue />);
    expect(screen.getByText('ALL WORKERS IDLE // NO PENDING TURNS')).toBeTruthy();
    expect(screen.getByText('DISPATCH POLLING QUEUE (1s)')).toBeTruthy();
  });

  it('renders one card per task, field for field with renderActiveTasks', () => {
    const { container } = render(<TaskQueue />);
    seed(TASKS);
    const cards = [...container.querySelectorAll('.task-card')];
    const row = (c: Element) => ({
      className: c.className,
      laser: c.querySelector('.task-card-laser') !== null,
      trigger: [c.querySelector('.task-trigger-badge')?.getAttribute('data-trigger'), c.querySelector('.task-trigger-badge')?.textContent?.trim()],
      author: c.querySelector('.task-author')?.textContent,
      retry: c.querySelector('.task-retry-badge')?.textContent?.trim() ?? null,
      timer: c.querySelector('.timer-text')?.textContent?.trim() ?? null,
      pill: c.querySelector('.task-status-pill')?.textContent?.trim(),
      prompt: c.querySelector('.task-prompt-box')?.textContent,
      footer: c.querySelector('.task-card-footer > div')?.textContent,
      inspect: [c.querySelector('.task-inspect-btn')?.textContent?.trim(), c.querySelector('.task-inspect-btn')?.getAttribute('href')],
    });
    expect(cards.map(row)).toEqual([
      {
        className: 'task-card status-processing processing',
        laser: true,
        trigger: ['discord', 'DISCORD'],
        author: 'pilot',
        retry: null,
        timer: '01:00s',
        pill: 'RUNNING',
        prompt: '> working',
        footer: 'THREAD: 100000000000000001 SESSION: session-1',
        inspect: ['INSPECT IN AGENTSVIEW ↗', '/agentsview/sessions/antigravity-cli%3Asession-1'],
      },
      {
        className: 'task-card status-processing processing',
        laser: true,
        trigger: ['cron', 'CRON'],
        author: 'scheduler',
        retry: null,
        timer: '02:00s',
        pill: 'RUNNING',
        prompt: '> working',
        footer: 'THREAD: 100000000000000002 SESSION: session-2',
        inspect: ['INSPECT IN AGENTSVIEW ↗', '/agentsview/sessions/antigravity-cli%3Asession-2'],
      },
      {
        className: 'task-card status-processing processing',
        laser: true,
        trigger: ['reminder', 'REMINDER'],
        author: 'pilot',
        retry: 'RETRY #1',
        timer: '01:00s',
        pill: 'RUNNING',
        prompt: '> working',
        footer: 'THREAD: 100000000000000003 SESSION: session-3',
        inspect: ['INSPECT IN AGENTSVIEW ↗', '/agentsview/sessions/antigravity-cli%3Asession-3'],
      },
      {
        className: 'task-card status-pending pending',
        laser: false,
        trigger: ['http', 'API'],
        author: 'pilot',
        retry: null,
        timer: null,
        pill: 'QUEUED',
        prompt: '> queued',
        footer: 'THREAD: 100000000000000004',
        inspect: ['OPEN AGENTSVIEW ↗', '/agentsview/'],
      },
      {
        className: 'task-card status-pending pending',
        laser: false,
        trigger: ['discord', 'DISCORD'],
        author: 'pilot',
        retry: null,
        timer: null,
        pill: 'QUEUED',
        prompt: '> queued',
        footer: 'THREAD: N/A SESSION: session-5',
        inspect: ['INSPECT IN AGENTSVIEW ↗', '/agentsview/sessions/antigravity-cli%3Asession-5'],
      },
    ]);
  });
});

describe('task render isolation', () => {
  it.each([
    { name: '1s tick re-renders only the ticker leaves', advanceMs: 1000, next: null, wantIcons: ['timer', 'timer', 'timer'] },
    { name: 'identical poll renders nothing', advanceMs: 0, next: TASKS, wantIcons: [] },
    {
      name: 'one changed task re-renders only its row',
      advanceMs: 0,
      next: TASKS.map((t) => (t.id === '5' ? { ...t, summary: 'still queued' } : t)),
      wantIcons: ['chat', 'hourglass', 'chat'],
    },
  ])('$name', ({ advanceMs, next, wantIcons }) => {
    render(<TaskQueue />);
    seed(TASKS);
    iconRenders.length = 0;
    act(() => {
      vi.advanceTimersByTime(advanceMs);
      if (next !== null) useConsoleStore.getState().setCluster(structuredClone(cluster(next)));
    });
    expect(iconRenders).toEqual(wantIcons);
  });
});

describe('QueueBadge', () => {
  it.each([
    { name: 'before data', tasks: null, want: ['section-badge', '0 IN PROGRESS'] },
    { name: 'running and queued', tasks: TASKS, want: ['section-badge active', '3 RUNNING (+2 QUEUED)'] },
    { name: 'queued only', tasks: TASKS.slice(3), want: ['section-badge building', '2 IN QUEUE'] },
  ])('$name', ({ tasks, want }) => {
    const { container } = render(<QueueBadge id="tasks-view-badge" />);
    if (tasks !== null) seed(tasks);
    const badge = container.querySelector('#tasks-view-badge');
    expect([badge?.className, badge?.textContent?.trim()]).toEqual(want);
  });
});

describe('TasksView', () => {
  it.each([
    { name: 'idle', tasks: [], want: ['0 / 4 WORKERS', 'REAL-TIME DISPATCH', '0 TURNS'], wantCards: 0 },
    { name: 'busy', tasks: TASKS, want: ['3 / 4 WORKERS', '3 PROCESSING TURNS', '2 TURNS'], wantCards: 5 },
  ])('$name cockpit', ({ tasks, want, wantCards }) => {
    const { container } = render(<TasksView />);
    seed(tasks);
    for (const text of want) expect(screen.getByText(text)).toBeTruthy();
    expect(screen.getAllByRole('heading', { level: 2 }).map((h) => h.textContent)).toEqual([
      'LIVE DISPATCH STREAM',
      'RECENT TASK EXECUTION HISTORY',
    ]);
    expect(container.querySelectorAll('.task-card')).toHaveLength(wantCards);
    expect(screen.getByText('ONLINE')).toBeTruthy();
  });
});
