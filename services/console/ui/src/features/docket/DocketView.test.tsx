import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { DocketSearchAPIResponse, DocketTaskDTO, DocketTasksAPIResponse, DocketTaskSummaryDTO } from '../../api/types';
import { useKeyboardShortcuts } from '../../hooks/useKeyboardShortcuts';
import { type DrawerState, useConsoleStore } from '../../store/store';
import DocketView from './DocketView';
import { TaskDetailPanel } from './TaskDetail';

const NOW = Date.parse('2026-11-08T12:00:00Z');

const node = (id: number, status = 'open', extra: Partial<DocketTaskSummaryDTO> = {}): DocketTaskSummaryDTO => ({
  id,
  title: `Task ${id}`,
  status,
  ...extra,
});
const LIST: DocketTasksAPIResponse = {
  status: 'ok',
  tasks: [node(3), node(5, 'open', { blocked_by: [3, 4] }), node(4, 'completed'), node(9, 'open', { blocked_by: [5] }), node(2, 'open', { title: '' })],
};
const COMPLETED: DocketTasksAPIResponse = { status: 'ok', tasks: [node(4, 'completed')] };
const HITS: DocketSearchAPIResponse = {
  status: 'ok',
  results: [
    { id: 9, title: 'Task 9', status: 'open', score: 0.9 },
    { id: 3, title: 'Task 3', status: 'open', score: 0.7 },
  ],
};
const TASK: DocketTaskDTO = {
  id: 5,
  title: 'Wire the tab',
  status: 'open',
  description: '## Goal\n\nShip **it**.',
  created_at: '2026-11-08T10:00:00Z',
  updated_at: '2026-11-08T11:00:00Z',
  parent: node(1),
  blocked_by: [node(3), node(4, 'completed')],
  blocks: [node(9)],
  subtasks: [node(6)],
  related: [node(8, 'wont_do', { title: '' })],
  comments: [
    { id: 51, author: 'aerial', comment: 'Started with `tests`.', created_at: '2026-11-08T11:30:00Z' },
    { id: 52, comment: 'Anonymous note', created_at: '2026-11-08T09:00:00Z' },
  ],
};

const BODIES: Readonly<Record<string, readonly [number, unknown]>> = {
  '/api/docket/tasks?status=open': [200, LIST],
  '/api/docket/tasks?status=completed': [200, COMPLETED],
  '/api/docket/search?q=task+9&status=open': [200, HITS],
  // Open hits under the completed pill: the status guard drops them all.
  '/api/docket/search?q=task+9&status=completed': [200, HITS],
  '/api/docket/tasks/5': [200, { status: 'ok', task: TASK }],
  '/api/docket/tasks/6': [200, { status: 'ok', task: { ...TASK, id: 6, title: '', description: '', parent: undefined, blocked_by: undefined, blocks: undefined, subtasks: undefined, related: undefined, comments: undefined } }],
  '/api/docket/tasks/7': [404, { status: 'error', error: 'Task not found' }],
  '/api/docket/tasks/8': [503, { status: 'degraded', error: 'Docket service unreachable. Retrying...' }],
  '/api/docket/tasks/10': [200, { status: 'ok' }],
};

let requests: string[] = [];

beforeEach(() => {
  useConsoleStore.setState(useConsoleStore.getInitialState(), true);
  window.history.replaceState(null, '', '/');
  requests = [];
  vi.stubGlobal(
    'fetch',
    vi.fn((url: string) => {
      requests.push(url);
      const [status, body] = BODIES[url] ?? [500, { error: 'no fixture' }];
      return Promise.resolve(new Response(JSON.stringify(body), { status }));
    }),
  );
  vi.useFakeTimers({ now: NOW, toFake: ['Date', 'setInterval', 'clearInterval', 'setTimeout', 'clearTimeout'] });
});
afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

async function settle(): Promise<void> {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(0);
  });
}

const cards = (): string[] => [...document.querySelectorAll('.docket-card')].map((c) => c.getAttribute('data-task-id') ?? '');
const text = (selector: string): string => (document.querySelector(selector)?.textContent ?? '').replace(/\s+/g, ' ').trim();

function Shortcuts() {
  useKeyboardShortcuts('docket');
  return null;
}

describe('DocketView list states', () => {
  it.each<{ name: string; data: DocketTasksAPIResponse | null; error: string | null; wantCards: string[]; wantBody: string; wantCount: string }>([
    { name: 'loading', data: null, error: null, wantCards: [], wantBody: 'LOADING TASKS...', wantCount: '0 TASKS' },
    { name: 'failed first poll', data: null, error: 'HTTP 503', wantCards: [], wantBody: 'DOCKET UNAVAILABLE // HTTP 503', wantCount: '0 TASKS' },
    { name: 'empty', data: { status: 'ok', tasks: [] }, error: null, wantCards: [], wantBody: 'NO TASKS', wantCount: '0 TASKS' },
    { name: 'ready, newest first, open only', data: LIST, error: null, wantCards: ['9', '5', '3', '2'], wantBody: '', wantCount: '4 TASKS' },
    { name: 'stale list under a failed poll', data: LIST, error: 'HTTP 503', wantCards: ['9', '5', '3', '2'], wantBody: '', wantCount: '4 TASKSSTALE // HTTP 503' },
  ])('$name', ({ data, error, wantCards, wantBody, wantCount }) => {
    // The mount poll never settles, so the seeded state is what renders.
    vi.stubGlobal('fetch', vi.fn(() => new Promise(() => undefined)));
    act(() => {
      if (data !== null) useConsoleStore.getState().setDocketTasks(data);
      if (error !== null) useConsoleStore.getState().failDocketTasks(error);
    });
    render(<DocketView />);
    expect(cards()).toEqual(wantCards);
    expect(text('.docket-empty')).toBe(wantBody);
    expect(text('.results-telemetry-bar')).toBe(wantCount);
  });
});

type Step =
  | { readonly pill: string }
  | { readonly type: string }
  | { readonly advance: number }
  | { readonly key: string }
  | { readonly clear: true };

async function runStep(step: Step): Promise<void> {
  if ('pill' in step) {
    fireEvent.click(within(screen.getByRole('toolbar', { name: 'Task status filter' })).getByRole('button', { name: step.pill }));
  } else if ('type' in step) {
    fireEvent.change(screen.getByRole('searchbox', { name: 'Search tasks' }), { target: { value: step.type } });
  } else if ('key' in step) {
    fireEvent.keyDown(window, { key: step.key });
  } else if ('clear' in step) {
    fireEvent.click(screen.getByRole('button', { name: 'Clear search' }));
  } else {
    await act(async () => {
      await vi.advanceTimersByTimeAsync(step.advance);
    });
  }
  await settle();
}

describe('DocketView interactions', () => {
  it.each<{ name: string; steps: readonly Step[]; wantRequests: string[]; wantCards: string[]; wantCount: string; wantPressed: string }>([
    { name: 'mount polls the open list', steps: [], wantRequests: ['/api/docket/tasks?status=open'], wantCards: ['9', '5', '3', '2'], wantCount: '4 TASKS', wantPressed: 'OPEN' },
    {
      name: 'the list re-polls every 30s',
      steps: [{ advance: 29_999 }, { advance: 1 }],
      wantRequests: ['/api/docket/tasks?status=open', '/api/docket/tasks?status=open'],
      wantCards: ['9', '5', '3', '2'],
      wantCount: '4 TASKS',
      wantPressed: 'OPEN',
    },
    {
      name: 'a pill fetches its status at once',
      steps: [{ pill: 'COMPLETED' }],
      wantRequests: ['/api/docket/tasks?status=open', '/api/docket/tasks?status=completed'],
      wantCards: ['4'],
      wantCount: '1 TASK',
      wantPressed: 'COMPLETED',
    },
    {
      name: 'a query waits 300ms, then shows search hits in score order',
      steps: [{ type: ' task 9 ' }, { advance: 299 }, { advance: 1 }],
      wantRequests: ['/api/docket/tasks?status=open', '/api/docket/search?q=task+9&status=open'],
      wantCards: ['9', '3'],
      wantCount: '2 RESULTS',
      wantPressed: 'OPEN',
    },
    {
      name: 'typing again within 300ms restarts the wait',
      steps: [{ type: 'task' }, { advance: 200 }, { type: 'task 9' }, { advance: 200 }],
      wantRequests: ['/api/docket/tasks?status=open'],
      wantCards: ['9', '5', '3', '2'],
      wantCount: '4 TASKS',
      wantPressed: 'OPEN',
    },
    {
      name: 'a search with no hits in the pill is empty',
      steps: [{ pill: 'COMPLETED' }, { type: 'task 9' }, { advance: 300 }],
      wantRequests: ['/api/docket/tasks?status=open', '/api/docket/tasks?status=completed', '/api/docket/search?q=task+9&status=completed'],
      wantCards: [],
      wantCount: '0 RESULTS',
      wantPressed: 'COMPLETED',
    },
    {
      name: 'a failed search shows the error',
      steps: [{ type: 'nope' }, { advance: 300 }],
      wantRequests: ['/api/docket/tasks?status=open', '/api/docket/search?q=nope&status=open'],
      wantCards: [],
      wantCount: '0 RESULTS',
      wantPressed: 'OPEN',
    },
    {
      name: 'the clear button returns to the list at once',
      steps: [{ type: 'task 9' }, { advance: 300 }, { clear: true }],
      wantRequests: ['/api/docket/tasks?status=open', '/api/docket/search?q=task+9&status=open', '/api/docket/tasks?status=open'],
      wantCards: ['9', '5', '3', '2'],
      wantCount: '4 TASKS',
      wantPressed: 'OPEN',
    },
    {
      name: 'Escape in the search box clears it',
      steps: [{ key: '/' }, { type: 'task 9' }, { advance: 300 }, { key: 'Escape' }],
      wantRequests: ['/api/docket/tasks?status=open', '/api/docket/search?q=task+9&status=open', '/api/docket/tasks?status=open'],
      wantCards: ['9', '5', '3', '2'],
      wantCount: '4 TASKS',
      wantPressed: 'OPEN',
    },
  ])('$name', async ({ steps, wantRequests, wantCards, wantCount, wantPressed }) => {
    render(
      <>
        <Shortcuts />
        <DocketView />
      </>,
    );
    await settle();
    for (const step of steps) await runStep(step);
    expect(requests).toEqual(wantRequests);
    expect(cards()).toEqual(wantCards);
    expect(text('.results-telemetry-bar')).toBe(wantCount);
    expect(text('.cat-pill[aria-pressed="true"]')).toBe(wantPressed);
  });
});

describe('TaskCard', () => {
  it.each<{ name: string; id: string; wantText: string; wantDrawer: DrawerState }>([
    { name: 'blocked card shows its blocker count and opens the drawer', id: '5', wantText: '#5Task 5OPENBLOCKED BY 2', wantDrawer: { kind: 'task', id: 5 } },
    { name: 'unblocked card has no blocker chip', id: '3', wantText: '#3Task 3OPEN', wantDrawer: { kind: 'task', id: 3 } },
    { name: 'untitled card falls back', id: '2', wantText: '#2Untitled taskOPEN', wantDrawer: { kind: 'task', id: 2 } },
  ])('$name', async ({ id, wantText, wantDrawer }) => {
    render(<DocketView />);
    await settle();
    const card = document.querySelector(`.docket-card[data-task-id="${id}"]`) as HTMLElement;
    expect(card.textContent).toBe(wantText);
    fireEvent.click(card);
    expect(useConsoleStore.getState().drawer).toEqual(wantDrawer);
  });
});

describe('TaskDetailPanel', () => {
  it.each<{
    name: string;
    id: number;
    wantTexts: string[];
    wantNodes: Readonly<Record<string, string[]>>;
    wantComments: number;
  }>([
    {
      name: 'renders status, markdown description, graph and comments',
      id: 5,
      wantTexts: ['Wire the tab', 'Untitled task', 'Goal', 'COMMENTS (2)', 'aerial', '30m AGO', 'unknown', '3h AGO', 'Anonymous note'],
      wantNodes: { center: ['5'], parent: ['1'], blocked_by: ['3', '4'], blocks: ['9'], subtasks: ['6'], related: ['8'] },
      wantComments: 2,
    },
    {
      name: 'omits empty groups and falls back for a bare task',
      id: 6,
      wantTexts: ['Untitled task', 'No description provided', 'COMMENTS (0)', 'NO COMMENTS'],
      wantNodes: { center: ['6'], parent: [], blocked_by: [], blocks: [], subtasks: [], related: [] },
      wantComments: 0,
    },
    { name: 'a 404 renders the not-found state', id: 7, wantTexts: ['TASK #7 NOT FOUND'], wantNodes: { center: [] }, wantComments: 0 },
    { name: 'a 200 without a task reads as not found', id: 10, wantTexts: ['TASK #10 NOT FOUND'], wantNodes: { center: [] }, wantComments: 0 },
    {
      name: 'an upstream failure renders the error state',
      id: 8,
      wantTexts: ['FAILED TO LOAD TASK', 'Docket service unreachable. Retrying...'],
      wantNodes: { center: [] },
      wantComments: 0,
    },
    { name: 'shows a loading row until the fetch lands', id: 99, wantTexts: ['LOADING TASK #99...'], wantNodes: { center: [] }, wantComments: 0 },
  ])('$name', async ({ id, wantTexts, wantNodes, wantComments }) => {
    if (id === 99) vi.stubGlobal('fetch', vi.fn(() => new Promise(() => undefined)));
    render(<TaskDetailPanel target={{ id }} />);
    await settle();
    for (const t of wantTexts) expect(screen.getAllByText(t).length, t).toBeGreaterThan(0);
    for (const [group, ids] of Object.entries(wantNodes)) {
      const selector = group === 'center' ? '.docket-graph-center .docket-node' : `.docket-graph-${group} .docket-node`;
      expect([...document.querySelectorAll(selector)].map((n) => n.getAttribute('data-task-id') ?? n.querySelector('.docket-node-id')?.textContent?.slice(1))).toEqual(ids);
    }
    expect(document.querySelectorAll('.docket-comment')).toHaveLength(wantComments);
  });

  it.each<{ name: string; selector: string; wantDrawer: DrawerState; wantRequests: string[] }>([
    { name: 'a parent node retargets the drawer', selector: '.docket-graph-parent .docket-node', wantDrawer: { kind: 'task', id: 1 }, wantRequests: ['/api/docket/tasks/5'] },
    { name: 'a subtask node retargets the drawer', selector: '.docket-graph-subtasks .docket-node', wantDrawer: { kind: 'task', id: 6 }, wantRequests: ['/api/docket/tasks/5'] },
  ])('$name', async ({ selector, wantDrawer, wantRequests }) => {
    act(() => useConsoleStore.getState().openDrawer({ kind: 'task', id: 5 }));
    render(<TaskDetailPanel target={{ id: 5 }} />);
    await settle();
    fireEvent.click(document.querySelector(selector) as Element);
    expect(useConsoleStore.getState().drawer).toEqual(wantDrawer);
    expect(requests).toEqual(wantRequests);
  });

  it('re-polls every 15s and restarts its poll when retargeted', async () => {
    const { rerender } = render(<TaskDetailPanel target={{ id: 5 }} />);
    await settle();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(15_000);
    });
    rerender(<TaskDetailPanel target={{ id: 6 }} />);
    await settle();
    expect(requests).toEqual(['/api/docket/tasks/5', '/api/docket/tasks/5', '/api/docket/tasks/6']);
    expect(text('.docket-detail-title')).toBe('Untitled task');
  });
});
