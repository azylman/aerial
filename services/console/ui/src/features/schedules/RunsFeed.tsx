import { memo } from 'react';
import type { ScheduleRun } from '../../api/types';
import { Glyph } from '../../components/Glyph';
import { useLiveNow } from '../../hooks/useLiveNow';
import { formatDuration, formatTimeAgo } from '../../lib/formatters';
import { QueryHighlight } from '../../lib/search';
import { linkableThread, runStatus } from './model';

/** Relative start time; a leaf on the shared 1s clock so the run card itself never re-renders on a tick. */
const RunAge = memo(function RunAge({ at }: { readonly at: string }) {
  return <span className="run-time-badge">{formatTimeAgo(at, useLiveNow())}</span>;
});

/** One execution run (renderScheduleRuns), memoized on the reconciled run; highlights follow HighlightQuery. */
export const RunCard = memo(function RunCard({ run }: { readonly run: ScheduleRun }) {
  const status = runStatus(run.status);
  const thread = linkableThread(run.thread_id);
  return (
    <div className={`run-card run-${status.key}`}>
      <div className="run-card-header">
        <div className="run-header-left">
          <span className={`run-status-chip ${status.chipClass}`}>
            <Glyph name={status.icon} /> {status.label}
          </span>
          <span className="run-title">
            <QueryHighlight text={run.title || run.schedule_id || 'Schedule Run'} />
          </span>
          <span className="run-type-tag">{(run.schedule_type || 'cron').toUpperCase()}</span>
        </div>
        <div className="run-header-right">
          {run.duration_ms ? (
            <span className="run-duration-badge">
              <Glyph name="timer" /> {formatDuration(run.duration_ms)}
            </span>
          ) : null}
          <RunAge at={run.started_at} />
        </div>
      </div>
      <div className="run-prompt-preview">
        <div className="run-prompt-text">
          <QueryHighlight text={run.prompt || ''} />
        </div>
        {run.error ? (
          <div className="run-error-box">
            <span className="run-error-label">ERROR:</span>{' '}
            <span className="run-error-text">
              <QueryHighlight text={run.error} />
            </span>
          </div>
        ) : null}
      </div>
      <div className="run-card-footer">
        <div className="run-meta-left">
          <span>
            TARGET: <code>{run.target_id || run.thread_id || 'N/A'}</code>
            {thread === null ? null : (
              <>
                {' • '}
                <a href={`/agentsview/?thread=${thread}`} className="run-thread-link" target="_blank" rel="noopener">
                  THREAD #{thread.slice(0, 8)} ↗
                </a>
              </>
            )}
          </span>
        </div>
        <div className="run-meta-right">
          <span>
            RUN ID: <code>{String(run.id)}</code>
          </span>
        </div>
      </div>
    </div>
  );
});
