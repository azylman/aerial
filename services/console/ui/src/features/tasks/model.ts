import type { ActiveTaskStatus } from '../../api/types';
import type { IconName } from '../../components/Icon';
import { formatAgentsviewSessionUrl } from '../../lib/formatters';

// Pure view models for the active task queue, ported from renderActiveTasks in static/app.js.
// Components select primitives or reconciled task objects and derive everything else here.

export interface QueueCounts {
  readonly running: number;
  readonly pending: number;
}

export function queueCounts(tasks: readonly ActiveTaskStatus[] | null): QueueCounts {
  let running = 0;
  let pending = 0;
  for (const task of tasks ?? []) {
    if (task.status === 'PROCESSING') running++;
    else if (task.status === 'PENDING') pending++;
  }
  return { running, pending };
}

export interface BadgeModel {
  readonly icon: IconName | null;
  readonly text: string;
  readonly className: string;
}

/** The `N RUNNING (+M QUEUED)` section badge shared by the telemetry and tasks views. */
export function queueBadge({ running, pending }: QueueCounts): BadgeModel {
  if (running > 0) {
    return {
      icon: 'bolt',
      text: `${running} RUNNING${pending > 0 ? ` (+${pending} QUEUED)` : ''}`,
      className: 'section-badge active',
    };
  }
  if (pending > 0) return { icon: 'hourglass', text: `${pending} IN QUEUE`, className: 'section-badge building' };
  return { icon: null, text: '0 IN PROGRESS', className: 'section-badge' };
}

export interface QueueKpis {
  readonly workers: string;
  readonly workersClass: string;
  readonly workersSub: string;
  readonly queued: string;
}

/** Tasks cockpit KPI texts (tasks-kpi-workers, tasks-kpi-workers-sub, tasks-kpi-queued). */
export function queueKpis({ running, pending }: QueueCounts): QueueKpis {
  return {
    workers: `${running} / 4 WORKERS`,
    workersClass: running > 0 ? 'summary-value text-cyan' : 'summary-value text-success',
    workersSub: running > 0 ? `${running} PROCESSING TURN${running > 1 ? 'S' : ''}` : 'REAL-TIME DISPATCH',
    queued: `${pending} TURN${pending === 1 ? '' : 'S'}`,
  };
}

export interface TaskCardModel {
  readonly processing: boolean;
  readonly cardClass: string;
  readonly pillClass: string;
  readonly pillIcon: IconName | null;
  readonly pillText: string;
  readonly author: string;
  readonly summary: string;
  /** Start time for the elapsed ticker; null when the task is not running. */
  readonly timerStart: string | null;
  readonly retry: number;
  readonly session: string | null;
  readonly inspectUrl: string;
}

/** One task card, field for field with the per-task template in renderActiveTasks. */
export function taskCardModel(task: ActiveTaskStatus): TaskCardModel {
  const processing = task.status === 'PROCESSING';
  const pending = task.status === 'PENDING';
  const session = typeof task.session_id === 'string' && task.session_id.trim() !== '' ? task.session_id : null;
  const start = task.started_at || task.created_at;
  return {
    processing,
    cardClass: processing
      ? 'task-card status-processing processing'
      : pending
        ? 'task-card status-pending pending'
        : 'task-card status-unknown',
    pillClass: processing ? 'task-status-pill processing' : 'task-status-pill pending',
    pillIcon: processing ? 'bolt' : pending ? 'hourglass' : null,
    pillText: processing ? 'RUNNING' : pending ? 'QUEUED' : (task.status || 'UNKNOWN').toUpperCase(),
    author: task.author_name || 'System',
    summary: task.summary || task.prompt || 'Agent Task',
    timerStart: processing && start ? start : null,
    retry: Number(task.retry_count) || 0,
    session,
    inspectUrl: formatAgentsviewSessionUrl(session),
  };
}
