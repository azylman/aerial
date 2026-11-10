import { memo, useMemo } from 'react';
import type { DocketTaskCommentDTO, DocketTaskDTO } from '../../api/types';
import { Icon } from '../../components/Icon';
import { useLiveNow } from '../../hooks/useLiveNow';
import { usePollingSync } from '../../hooks/usePollingSync';
import { formatTimeAgo } from '../../lib/formatters';
import { Markdown } from '../../lib/markdown';
import { type ConsoleState, useConsoleStore } from '../../store/store';
import { syncDocketTask } from './data';
import { resolveTaskDetail, taskMissing } from './model';
import { StatusChip } from './TaskCard';
import { TaskGraph } from './TaskGraph';
import './docket.css';

/** The drawer re-fetches its task every 15s while open and visible. */
export const TASK_DETAIL_POLL_MS = 15000;

const selectDetail = (s: ConsoleState) => s.docket.detail.data;
const selectDetailError = (s: ConsoleState): string | null => s.docket.detail.error;

/** A comment's age: the only part of the drawer on the shared 1s clock. */
function CommentAge({ at }: { readonly at: string }) {
  const now = useLiveNow();
  return <span className="docket-comment-age">{formatTimeAgo(at, now)}</span>;
}

function Comment({ comment }: { readonly comment: DocketTaskCommentDTO }) {
  return (
    <li className="docket-comment">
      <div className="docket-comment-head">
        <span className="docket-comment-author">{comment.author || 'unknown'}</span>
        <CommentAge at={comment.created_at} />
      </div>
      <div className="docket-markdown markdown-body">
        <Markdown source={comment.comment} />
      </div>
    </li>
  );
}

const TaskDetailContent = memo(function TaskDetailContent({ task }: { readonly task: DocketTaskDTO }) {
  const comments = task.comments ?? [];
  return (
    <div className="docket-detail">
      <div className="docket-detail-head">
        <StatusChip status={task.status} />
        <span className="docket-card-id">#{task.id}</span>
      </div>
      <h3 className="docket-detail-title">{task.title || 'Untitled task'}</h3>
      <section className="docket-detail-section">
        <span className="docket-section-label">DESCRIPTION</span>
        <div className="docket-markdown markdown-body">
          <Markdown source={task.description || 'No description provided'} />
        </div>
      </section>
      <section className="docket-detail-section">
        <span className="docket-section-label">DEPENDENCIES</span>
        <TaskGraph task={task} />
      </section>
      <section className="docket-detail-section">
        <span className="docket-section-label">COMMENTS ({comments.length})</span>
        {comments.length === 0 ? (
          <div className="docket-empty">NO COMMENTS</div>
        ) : (
          <ul className="docket-comments">
            {comments.map((c) => (
              <Comment key={c.id} comment={c} />
            ))}
          </ul>
        )}
      </section>
    </div>
  );
});

function TaskDetail({ id }: { readonly id: number }) {
  const poll = useMemo(() => syncDocketTask(id), [id]);
  usePollingSync(poll, TASK_DETAIL_POLL_MS, true);

  const detail = useConsoleStore(selectDetail);
  const error = useConsoleStore(selectDetailError);
  const task = resolveTaskDetail(id, detail);

  if (task !== null) return <TaskDetailContent task={task} />;
  if (taskMissing(id, detail)) {
    return (
      <div className="docket-state">
        <span className="docket-state-icon">
          <Icon name="ban" />
        </span>
        <p>TASK #{id} NOT FOUND</p>
        <span className="docket-state-sub">It may have been deleted or moved out of the project.</span>
      </div>
    );
  }
  if (error !== null) {
    return (
      <div className="docket-state">
        <span className="docket-state-icon text-danger">
          <Icon name="alert" />
        </span>
        <p>FAILED TO LOAD TASK</p>
        <span className="docket-state-sub">{error}</span>
      </div>
    );
  }
  return (
    <div className="docket-state">
      <span className="pulse-dot healthy" /> <span>LOADING TASK #{id}...</span>
    </div>
  );
}

/**
 * Task drawer body. Keyed by id, so retargeting from a graph node restarts the poll for the new
 * task and aborts the old one.
 */
export const TaskDetailPanel = memo(function TaskDetailPanel({ target }: { readonly target: { readonly id: number } }) {
  return <TaskDetail key={target.id} id={target.id} />;
});
