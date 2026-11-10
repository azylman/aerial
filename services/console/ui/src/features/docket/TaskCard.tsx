import { memo } from 'react';
import type { DocketTaskSummaryDTO } from '../../api/types';
import { useConsoleStore } from '../../store/store';
import { blockedByCount, statusLabel } from './model';

/** Opens (or retargets) the drawer on a task. */
export function openTask(id: number): void {
  useConsoleStore.getState().openDrawer({ kind: 'task', id });
}

/** Status chip shared by the cards, the drawer and the graph nodes. */
export function StatusChip({ status }: { readonly status: string }) {
  return <span className={`docket-status docket-status-${status}`}>{statusLabel(status)}</span>;
}

/** The card fields both the list (TaskSummary) and search hits (ScoredTaskSummary) carry. */
export type CardTask = Pick<DocketTaskSummaryDTO, 'id' | 'title' | 'status' | 'blocked_by'>;

/** One task row; memoized on the reconciled task, so unchanged polls skip it. */
export const TaskCard = memo(function TaskCard({ task }: { readonly task: CardTask }) {
  const blockers = blockedByCount(task);
  return (
    <button type="button" className="docket-card" data-task-id={task.id} onClick={() => openTask(task.id)}>
      <span className="docket-card-id">#{task.id}</span>
      <span className="docket-card-title">{task.title || 'Untitled task'}</span>
      <span className="docket-card-meta">
        <StatusChip status={task.status} />
        {blockers > 0 ? <span className="docket-blocked">BLOCKED BY {blockers}</span> : null}
      </span>
    </button>
  );
});
