import { memo } from 'react';
import type { CronSchedule, OneShotSchedule } from '../../api/types';
import { CopyButton } from '../../components/CopyButton';
import { Glyph } from '../../components/Glyph';
import { formatCronExpression } from '../../lib/cron';
import { formatTimestamp } from '../../lib/formatters';
import { Highlight } from '../../lib/search';
import { CountdownBadge } from './Countdown';
import { cronTarget, linkableThread } from './model';

// Cron and one-shot cards (renderScheduleCards). Each card is memoized on its reconciled schedule
// object and the normalized query, so polls that leave a schedule unchanged and keystrokes that
// do not change the query skip it; the countdown is a leaf on the shared 1s clock.

// The vanilla template interpolates the prompt between indented lines inside a `pre-wrap` box, so
// that whitespace is part of what renders (a blank first line and an indent). Kept for parity.
const PROMPT_LEAD = '\n                        ';
const PROMPT_TAIL = '\n                    ';

function PromptBox({ text, query }: { readonly text: string; readonly query: string }) {
  return (
    <div className="schedule-prompt-wrap">
      <div className="schedule-prompt-header">
        <span className="prompt-lbl">PROMPT DIRECTIVE</span>
        <CopyButton text={text} ariaLabel="Copy prompt text" className="prompt-copy-btn" />
      </div>
      <div className="schedule-prompt-body">
        {PROMPT_LEAD}
        <Highlight text={text} query={query} />
        {PROMPT_TAIL}
      </div>
    </div>
  );
}

export const CronCard = memo(function CronCard({
  cron,
  query,
}: {
  readonly cron: CronSchedule;
  readonly query: string;
}) {
  return (
    <div className="schedule-card cron-card">
      <div className="schedule-card-header">
        <div className="schedule-badge-group">
          <span className="schedule-type-badge cron">
            <Glyph name="alarm" /> CRON
          </span>
          <span className="schedule-tz-badge">{cron.timezone || 'UTC'}</span>
        </div>
        <div className="schedule-status-group">
          <span
            className={cron.enabled ? 'pulse-dot healthy' : 'pulse-dot unhealthy'}
            title={cron.enabled ? 'Active & Enabled' : 'Disabled'}
          />
          <CountdownBadge target={cron.next_run_at} />
        </div>
      </div>
      <div className="schedule-title-bar">
        <h3 className="schedule-title">
          <Highlight text={cron.title_prefix || 'RECURRING ROUTINE'} query={query} />
        </h3>
        <div className="schedule-cron-human">
          <Glyph name="calendar" /> {cron.cron_description || formatCronExpression(cron.cron_expr)}
        </div>
      </div>
      <div className="schedule-meta-bar">
        <span className="schedule-meta-item">
          <strong>CRON:</strong> <code>{cron.cron_expr}</code>
        </span>
        <span className="schedule-meta-item">
          <strong>TARGET:</strong> <code>{cronTarget(cron) || 'N/A'}</code>
        </span>
      </div>
      <PromptBox text={cron.prompt || ''} query={query} />
      <div className="schedule-card-footer">
        <span>NEXT: {formatTimestamp(cron.next_run_at)}</span>
        <span className="schedule-id-chip">
          ID: <code>{cron.id}</code>
        </span>
      </div>
    </div>
  );
});

function ThreadRef({ threadId }: { readonly threadId: string }) {
  const linkable = linkableThread(threadId);
  if (linkable === null) return <code>{threadId}</code>;
  return (
    <a href={`/agentsview/?thread=${linkable}`} className="schedule-thread-link" target="_blank" rel="noopener">
      #{linkable.slice(0, 8)} ↗
    </a>
  );
}

export const OneShotCard = memo(function OneShotCard({
  oneShot,
  query,
}: {
  readonly oneShot: OneShotSchedule;
  readonly query: string;
}) {
  return (
    <div className="schedule-card oneshot-card">
      <div className="schedule-card-header">
        <div className="schedule-badge-group">
          <span className="schedule-type-badge oneshot">
            <Glyph name="bolt" /> ONE-SHOT
          </span>
        </div>
        <div className="schedule-status-group">
          <span className="pulse-dot healthy" title="Pending execution" />
          <CountdownBadge target={oneShot.run_at} />
        </div>
      </div>
      <div className="schedule-title-bar">
        <h3 className="schedule-title">ONE-TIME REMINDER</h3>
        <div className="schedule-cron-human">
          <Glyph name="target" /> Single Execution Timer
        </div>
      </div>
      <div className="schedule-meta-bar">
        <span className="schedule-meta-item">
          <strong>THREAD:</strong> <ThreadRef threadId={oneShot.thread_id || 'N/A'} />
        </span>
      </div>
      <PromptBox text={oneShot.prompt || ''} query={query} />
      <div className="schedule-card-footer">
        <span>RUN AT: {formatTimestamp(oneShot.run_at)}</span>
        <span className="schedule-id-chip">
          ID: <code>{oneShot.id}</code>
        </span>
      </div>
    </div>
  );
});
