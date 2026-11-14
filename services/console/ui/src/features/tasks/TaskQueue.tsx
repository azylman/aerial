import { memo } from 'react';
import { useShallow } from 'zustand/react/shallow';
import type { ActiveTaskStatus } from '../../api/types';
import { TimerBadge } from '../../components/ElapsedTicker';
import { Glyph, GlyphText } from '../../components/Glyph';
import { getTriggerBadge } from '../../lib/formatters';
import { type ConsoleState, useConsoleStore } from '../../store/store';
import { type QueueCounts, queueBadge, queueCounts, taskCardModel } from './model';
import './tasks.css';

const EMPTY_TASKS: readonly ActiveTaskStatus[] = Object.freeze([]);

const selectTasks = (s: ConsoleState): readonly ActiveTaskStatus[] => s.status.cluster.data?.active_tasks ?? EMPTY_TASKS;

/**
 * One task row. Rows are memoized on the reconciled task object, so an unchanged poll renders
 * nothing; the running stopwatch lives in its own leaf and the 1s tick never reaches the row.
 */
export const TaskCard = memo(function TaskCard({ task }: { readonly task: ActiveTaskStatus }) {
  const m = taskCardModel(task);
  const trigger = getTriggerBadge(task.trigger_type);
  return (
    <div className={m.cardClass}>
      {m.processing ? <div className="task-card-laser" /> : null}
      <div className="task-card-header">
        <div className="task-meta-left">
          <span className={`task-trigger-badge ${trigger.css}`} data-trigger={trigger.css}>
            <Glyph name={trigger.icon} /> {trigger.text}
          </span>
          <span className="task-author">{m.author}</span>
          {m.retry > 0 ? (
            <span className="task-retry-badge" title="Retry Attempt">
              <Glyph name="refresh" /> RETRY #{m.retry}
            </span>
          ) : null}
        </div>
        <div className="card-header-right">
          {m.timerStart === null ? null : <TimerBadge startedAt={m.timerStart} />}
          <span className={m.pillClass}>
            <GlyphText icon={m.pillIcon} text={m.pillText} />
          </span>
        </div>
      </div>
      <div className="task-prompt-box">&gt; {m.summary}</div>
      <div className="task-card-footer">
        <div>
          <span>
            THREAD: {task.thread_id ? <code className="task-thread-id">{task.thread_id}</code> : 'N/A'}
          </span>
          {m.session === null ? null : (
            <>
              {' '}
              <span className="task-session">
                SESSION: <code className="task-session-id">{m.session}</code>
              </span>
            </>
          )}
        </div>
        {m.session === null ? (
          <a
            href={m.inspectUrl}
            target="_blank"
            rel="noopener noreferrer"
            className="task-inspect-btn active"
            title="Session allocating - Click to open Agentsview"
          >
            <Glyph name="chat" /> OPEN AGENTSVIEW ↗
          </a>
        ) : (
          <a href={m.inspectUrl} target="_blank" rel="noopener noreferrer" className="task-inspect-btn active">
            <Glyph name="chat" /> INSPECT IN AGENTSVIEW ↗
          </a>
        )}
      </div>
    </div>
  );
});

function TaskIdleCard() {
  return (
    <div className="task-idle-card">
      <div className="idle-indicator">
        <span className="pulse-dot healthy" />
        <span>ALL WORKERS IDLE // NO PENDING TURNS</span>
      </div>
      <div>DISPATCH POLLING QUEUE (1s)</div>
    </div>
  );
}

/** The active-task list, shared by the telemetry and tasks views. */
export const TaskQueue = memo(function TaskQueue() {
  const tasks = useConsoleStore(selectTasks);
  if (tasks.length === 0) return <TaskIdleCard />;
  return tasks.map((task) => <TaskCard key={task.id} task={task} />);
});

/** Running/queued counts as primitives: the badge re-renders only when a count changes. */
export function useQueueCounts(): QueueCounts {
  return useConsoleStore(useShallow((s) => queueCounts(selectTasks(s))));
}

export const QueueBadge = memo(function QueueBadge({ id }: { readonly id: string }) {
  const badge = queueBadge(useQueueCounts());
  return (
    <span className={badge.className} id={id}>
      <GlyphText icon={badge.icon} text={badge.text} />
    </span>
  );
});
