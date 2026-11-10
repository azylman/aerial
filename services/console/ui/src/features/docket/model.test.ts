import { describe, expect, it } from 'vitest';
import type { DocketStatusFilter, DocketTaskDTO, DocketTaskLookup, DocketTaskSummaryDTO, DocketTasksAPIResponse } from '../../api/types';
import {
  blockedByCount,
  graphGroups,
  matchesStatus,
  normalizeQuery,
  resolveTaskDetail,
  resultsLabel,
  statusLabel,
  taskMissing,
  visibleTasks,
} from './model';

const node = (id: number, status = 'open'): DocketTaskSummaryDTO => ({ id, title: `Task ${id}`, status });
const task = (id: number, extra: Partial<DocketTaskDTO> = {}): DocketTaskDTO => ({
  id,
  title: `Task ${id}`,
  status: 'open',
  created_at: '2026-11-08T10:00:00Z',
  updated_at: '2026-11-08T11:00:00Z',
  ...extra,
});
const list = (...tasks: DocketTaskSummaryDTO[]): DocketTasksAPIResponse => ({ status: 'ok', tasks });

describe('statusLabel', () => {
  it.each([
    { status: 'open', want: 'OPEN' },
    { status: 'completed', want: 'COMPLETED' },
    { status: 'wont_do', want: "WON'T DO" },
    { status: 'all', want: 'ALL' },
    { status: 'blocked', want: 'BLOCKED' },
  ])('$status -> $want', ({ status, want }) => {
    expect(statusLabel(status)).toBe(want);
  });
});

describe('matchesStatus', () => {
  it.each<{ status: string; filter: DocketStatusFilter; want: boolean }>([
    { status: 'open', filter: 'open', want: true },
    { status: 'completed', filter: 'open', want: false },
    { status: 'wont_do', filter: 'wont_do', want: true },
    { status: 'completed', filter: 'all', want: true },
  ])('$status in $filter -> $want', ({ status, filter, want }) => {
    expect(matchesStatus(status, filter)).toBe(want);
  });
});

describe('visibleTasks', () => {
  it.each<{ name: string; data: DocketTasksAPIResponse | null; filter: DocketStatusFilter; want: number[] }>([
    { name: 'no payload yet', data: null, filter: 'open', want: [] },
    { name: 'newest first', data: list(node(3), node(12), node(7)), filter: 'open', want: [12, 7, 3] },
    { name: 'drops tasks outside the pill (stale payload)', data: list(node(3), node(4, 'completed')), filter: 'open', want: [3] },
    { name: 'all keeps every status', data: list(node(3), node(4, 'completed'), node(5, 'wont_do')), filter: 'all', want: [5, 4, 3] },
    { name: 'empty list', data: list(), filter: 'completed', want: [] },
  ])('$name', ({ data, filter, want }) => {
    expect(visibleTasks(data, filter).map((t) => t.id)).toEqual(want);
  });
});

describe('blockedByCount', () => {
  it.each([
    { name: 'no field', task: node(1), want: 0 },
    { name: 'empty', task: { ...node(1), blocked_by: [] }, want: 0 },
    { name: 'two blockers', task: { ...node(1), blocked_by: [4, 5] }, want: 2 },
  ])('$name', ({ task: t, want }) => {
    expect(blockedByCount(t)).toBe(want);
  });
});

describe('normalizeQuery', () => {
  it.each([
    { search: '', want: '' },
    { search: '   ', want: '' },
    { search: '  Kiosk Widget ', want: 'Kiosk Widget' },
  ])('"$search" -> "$want"', ({ search, want }) => {
    expect(normalizeQuery(search)).toBe(want);
  });
});

describe('resultsLabel', () => {
  it.each([
    { count: 0, searching: false, want: '0 TASKS' },
    { count: 1, searching: false, want: '1 TASK' },
    { count: 21, searching: false, want: '21 TASKS' },
    { count: 1, searching: true, want: '1 RESULT' },
    { count: 4, searching: true, want: '4 RESULTS' },
  ])('$count searching=$searching -> $want', ({ count, searching, want }) => {
    expect(resultsLabel(count, searching)).toBe(want);
  });
});

const FOUND: DocketTaskLookup = { id: 7, task: task(7, { title: 'Wire the tab' }) };
const GONE: DocketTaskLookup = { id: 7, task: null };

describe('resolveTaskDetail', () => {
  it.each<{ name: string; id: number; detail: DocketTaskLookup | null; want: number | null }>([
    { name: 'nothing fetched', id: 7, detail: null, want: null },
    { name: 'this task', id: 7, detail: FOUND, want: 7 },
    { name: 'another task (stale)', id: 8, detail: FOUND, want: null },
    { name: 'not found', id: 7, detail: GONE, want: null },
  ])('$name', ({ id, detail, want }) => {
    expect(resolveTaskDetail(id, detail)?.id ?? null).toBe(want);
  });
});

describe('taskMissing', () => {
  it.each<{ name: string; id: number; detail: DocketTaskLookup | null; want: boolean }>([
    { name: 'nothing fetched', id: 7, detail: null, want: false },
    { name: 'found', id: 7, detail: FOUND, want: false },
    { name: 'not found for this id', id: 7, detail: GONE, want: true },
    { name: 'not found for another id', id: 8, detail: GONE, want: false },
  ])('$name', ({ id, detail, want }) => {
    expect(taskMissing(id, detail)).toBe(want);
  });
});

describe('graphGroups', () => {
  it.each<{ name: string; task: DocketTaskDTO; want: readonly (readonly [string, readonly number[]])[] }>([
    { name: 'isolated task has no groups', task: task(1), want: [] },
    {
      name: 'every group in reading order',
      task: task(5, {
        related: [node(9)],
        subtasks: [node(6), node(7)],
        blocks: [node(8)],
        blocked_by: [node(3), node(4)],
        parent: node(1),
      }),
      want: [
        ['PARENT', [1]],
        ['BLOCKED BY', [3, 4]],
        ['BLOCKS', [8]],
        ['SUBTASKS', [6, 7]],
        ['RELATED', [9]],
      ],
    },
    { name: 'empty arrays are omitted', task: task(5, { blocks: [], subtasks: [node(6)] }), want: [['SUBTASKS', [6]]] },
  ])('$name', ({ task: t, want }) => {
    expect(graphGroups(t).map((g) => [g.label, g.nodes.map((n) => n.id)])).toEqual(want);
  });
});
