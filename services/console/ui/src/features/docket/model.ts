// Pure derivations for the read-only Docket tab: status pills, list ordering, the blocked-by
// count on a card, the drawer's task lookup and the per-task dependency neighbourhood.

import type {
  DocketStatusFilter,
  DocketTaskDTO,
  DocketTaskLookup,
  DocketTaskSummaryDTO,
  DocketTasksAPIResponse,
} from '../../api/types';

export interface StatusPill {
  readonly filter: DocketStatusFilter;
  readonly label: string;
}

/** The status pills in display order; the first is the default. */
export const STATUS_PILLS: readonly StatusPill[] = [
  { filter: 'open', label: 'OPEN' },
  { filter: 'completed', label: 'COMPLETED' },
  { filter: 'wont_do', label: "WON'T DO" },
  { filter: 'all', label: 'ALL' },
];

export const DEFAULT_STATUS: DocketStatusFilter = 'open';

/** Chip text for a task status; unknown statuses are shown uppercased. */
export function statusLabel(status: string): string {
  return STATUS_PILLS.find((p) => p.filter === status && p.filter !== 'all')?.label ?? status.toUpperCase();
}

/** Whether a task's status belongs to the pill (guards against a stale payload of another pill). */
export const matchesStatus = (status: string, filter: DocketStatusFilter): boolean => filter === 'all' || status === filter;

/** The list payload's tasks in the pill, newest (highest id) first. */
export function visibleTasks(data: DocketTasksAPIResponse | null, filter: DocketStatusFilter): readonly DocketTaskSummaryDTO[] {
  if (data === null) return [];
  return data.tasks.filter((t) => matchesStatus(t.status, filter)).sort((a, b) => b.id - a.id);
}

/** Number of tasks blocking this one. */
export const blockedByCount = (task: { readonly blocked_by?: readonly number[] }): number => task.blocked_by?.length ?? 0;

/** The search box text as sent to the search endpoint. */
export const normalizeQuery = (search: string): string => search.trim();

/** "N TASKS" / "N RESULTS" line under the pills. */
export function resultsLabel(count: number, searching: boolean): string {
  const noun = searching ? 'RESULT' : 'TASK';
  return `${count} ${noun}${count === 1 ? '' : 'S'}`;
}

/** The drawer's task once the lookup for exactly this id has arrived, else null. */
export function resolveTaskDetail(id: number, detail: DocketTaskLookup | null): DocketTaskDTO | null {
  return detail?.task?.id === id ? detail.task : null;
}

/** Whether the console answered 404 for this id. */
export const taskMissing = (id: number, detail: DocketTaskLookup | null): boolean => detail?.id === id && detail.task === null;

/** Drawer header: the id until the task arrives, then the id and title. */
export function taskDrawerTitle(id: number, detail: DocketTaskLookup | null): string {
  const task = resolveTaskDetail(id, detail);
  return task === null || task.title === '' ? `TASK // #${id}` : `TASK // #${id} ${task.title.toUpperCase()}`;
}

export type GraphGroupKey = 'parent' | 'blocked_by' | 'blocks' | 'subtasks' | 'related';

export interface GraphGroup {
  readonly key: GraphGroupKey;
  readonly label: string;
  readonly nodes: readonly DocketTaskSummaryDTO[];
}

const GROUP_LABELS: Readonly<Record<GraphGroupKey, string>> = {
  parent: 'PARENT',
  blocked_by: 'BLOCKED BY',
  blocks: 'BLOCKS',
  subtasks: 'SUBTASKS',
  related: 'RELATED',
};

/** The task's non-empty neighbour groups, in reading order (parent, blocked by, blocks, subtasks, related). */
export function graphGroups(task: DocketTaskDTO): readonly GraphGroup[] {
  const all: readonly (readonly [GraphGroupKey, readonly DocketTaskSummaryDTO[]])[] = [
    ['parent', task.parent === undefined ? [] : [task.parent]],
    ['blocked_by', task.blocked_by ?? []],
    ['blocks', task.blocks ?? []],
    ['subtasks', task.subtasks ?? []],
    ['related', task.related ?? []],
  ];
  return all.filter(([, nodes]) => nodes.length > 0).map(([key, nodes]) => ({ key, label: GROUP_LABELS[key], nodes }));
}
