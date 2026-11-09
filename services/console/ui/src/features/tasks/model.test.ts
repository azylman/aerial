import { describe, expect, it } from 'vitest';
import type { ActiveTaskStatus } from '../../api/types';
import { queueBadge, queueCounts, queueKpis, taskCardModel } from './model';

const task = (over: Partial<ActiveTaskStatus>): ActiveTaskStatus => ({
  id: 't',
  thread_id: '100',
  author_name: 'pilot',
  prompt: 'draft the plan',
  summary: 'working',
  status: 'PROCESSING',
  retry_count: 0,
  trigger_type: 'discord',
  created_at: '2026-11-08T11:58:00Z',
  started_at: '2026-11-08T11:59:00Z',
  ...over,
});

describe('queueCounts', () => {
  it.each([
    { name: 'no snapshot', tasks: null, want: { running: 0, pending: 0 } },
    { name: 'empty', tasks: [], want: { running: 0, pending: 0 } },
    {
      name: 'mixed statuses',
      tasks: [task({ status: 'PROCESSING' }), task({ status: 'PENDING' }), task({ status: 'PENDING' }), task({ status: 'DONE' })],
      want: { running: 1, pending: 2 },
    },
  ])('$name', ({ tasks, want }) => {
    expect(queueCounts(tasks)).toEqual(want);
  });
});

describe('queueBadge', () => {
  it.each([
    { running: 3, pending: 2, want: { icon: 'bolt', text: '3 RUNNING (+2 QUEUED)', className: 'section-badge active' } },
    { running: 1, pending: 0, want: { icon: 'bolt', text: '1 RUNNING', className: 'section-badge active' } },
    { running: 0, pending: 4, want: { icon: 'hourglass', text: '4 IN QUEUE', className: 'section-badge building' } },
    { running: 0, pending: 0, want: { icon: null, text: '0 IN PROGRESS', className: 'section-badge' } },
  ])('$running running / $pending pending', ({ running, pending, want }) => {
    expect(queueBadge({ running, pending })).toEqual(want);
  });
});

describe('queueKpis', () => {
  it.each([
    {
      running: 0,
      pending: 0,
      want: { workers: '0 / 4 WORKERS', workersClass: 'summary-value text-success', workersSub: 'REAL-TIME DISPATCH', queued: '0 TURNS' },
    },
    {
      running: 1,
      pending: 1,
      want: { workers: '1 / 4 WORKERS', workersClass: 'summary-value text-cyan', workersSub: '1 PROCESSING TURN', queued: '1 TURN' },
    },
    {
      running: 3,
      pending: 2,
      want: { workers: '3 / 4 WORKERS', workersClass: 'summary-value text-cyan', workersSub: '3 PROCESSING TURNS', queued: '2 TURNS' },
    },
  ])('$running running / $pending pending', ({ running, pending, want }) => {
    expect(queueKpis({ running, pending })).toEqual(want);
  });
});

describe('taskCardModel', () => {
  it.each([
    {
      name: 'running task with a session',
      task: task({ session_id: 'session-1', retry_count: 2 }),
      want: {
        processing: true,
        cardClass: 'task-card status-processing processing',
        pillClass: 'task-status-pill processing',
        pillIcon: 'bolt',
        pillText: 'RUNNING',
        author: 'pilot',
        summary: 'working',
        timerStart: '2026-11-08T11:59:00Z',
        retry: 2,
        session: 'session-1',
        inspectUrl: '/agentsview/sessions/antigravity-cli%3Asession-1',
      },
    },
    {
      name: 'running task without started_at falls back to created_at',
      task: task({ started_at: '', session_id: '  ' }),
      want: {
        processing: true,
        cardClass: 'task-card status-processing processing',
        pillClass: 'task-status-pill processing',
        pillIcon: 'bolt',
        pillText: 'RUNNING',
        author: 'pilot',
        summary: 'working',
        timerStart: '2026-11-08T11:58:00Z',
        retry: 0,
        session: null,
        inspectUrl: '/agentsview/',
      },
    },
    {
      name: 'queued task shows the prompt when the summary is empty',
      task: task({ status: 'PENDING', summary: '', author_name: '' }),
      want: {
        processing: false,
        cardClass: 'task-card status-pending pending',
        pillClass: 'task-status-pill pending',
        pillIcon: 'hourglass',
        pillText: 'QUEUED',
        author: 'System',
        summary: 'draft the plan',
        timerStart: null,
        retry: 0,
        session: null,
        inspectUrl: '/agentsview/',
      },
    },
    {
      name: 'unknown status with no text at all',
      task: task({ status: 'retrying', summary: '', prompt: '' }),
      want: {
        processing: false,
        cardClass: 'task-card status-unknown',
        pillClass: 'task-status-pill pending',
        pillIcon: null,
        pillText: 'RETRYING',
        author: 'pilot',
        summary: 'Agent Task',
        timerStart: null,
        retry: 0,
        session: null,
        inspectUrl: '/agentsview/',
      },
    },
    {
      name: 'missing status reads UNKNOWN',
      task: task({ status: '' }),
      want: {
        processing: false,
        cardClass: 'task-card status-unknown',
        pillClass: 'task-status-pill pending',
        pillIcon: null,
        pillText: 'UNKNOWN',
        author: 'pilot',
        summary: 'working',
        timerStart: null,
        retry: 0,
        session: null,
        inspectUrl: '/agentsview/',
      },
    },
  ])('$name', ({ task: t, want }) => {
    expect(taskCardModel(t)).toEqual(want);
  });
});
