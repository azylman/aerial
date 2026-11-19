import { memo, useMemo, useState } from 'react';
import type { CronSchedule, VanadisBuildDTO } from '../../api/types';
import { GitSyncBadge } from '../../components/GitSyncBadge';
import { Icon } from '../../components/Icon';
import {
  type BuildQueueState,
  classifyBuildQueueState,
  computeBuildChunkStats,
  getBuildStatusBadgeInfo,
} from '../../lib/builds';
import { buildsBadgeCount } from '../../store/selectors';
import { useConsoleStore } from '../../store/store';
import '../../styles/sections.css';
import { QueueBadge, TaskQueue } from '../tasks/TaskQueue';
import { DeployBadge, Deployments } from './Deployments';
import { HealthyCount, ServicesGrid } from './ServicesGrid';
import './telemetry.css';

const expandTasks = (): void => useConsoleStore.getState().setActiveTab('tasks');
const viewAllBuilds = (): void => useConsoleStore.getState().setActiveTab('builds');
const viewSchedules = (): void => useConsoleStore.getState().setActiveTab('schedules');

const EMPTY_BUILDS: readonly VanadisBuildDTO[] = Object.freeze([]);
const EMPTY_CRONS: readonly CronSchedule[] = Object.freeze([]);
const BUILD_ORDER: Readonly<Record<BuildQueueState, number>> = { running: 0, queued: 1, completed: 2 };

export const OverviewBuildsSection = memo(function OverviewBuildsSection() {
  const builds = useConsoleStore((s) => s.builds.list.data?.builds ?? EMPTY_BUILDS);
  const data = useConsoleStore((s) => s.builds.list.data);
  const activeCount = buildsBadgeCount(data);
  const topBuilds = useMemo(
    () =>
      [...builds]
        .sort((a, b) => BUILD_ORDER[classifyBuildQueueState(a)] - BUILD_ORDER[classifyBuildQueueState(b)])
        .slice(0, 3),
    [builds],
  );

  return (
    <section className="hud-section bento-cell bento-span-2 overview-bento-section" aria-label="Vanadis Build Pipeline Overview">
      <div className="section-title-bar section-header">
        <div className="section-title">
          <span className="title-icon">
            <Icon name="wrench" />
          </span>
          <h2>Vanadis Build Pipeline</h2>
        </div>
        <div className="section-actions section-header-actions">
          <span id="overview-builds-badge" className="section-badge badge">{activeCount} ACTIVE</span>
          <button type="button" className="task-inspect-btn expand-tasks-btn" onClick={viewAllBuilds}>
            <span>Builds ↗</span>
          </button>
        </div>
      </div>
      <div className="overview-bento-body">
        {topBuilds.length === 0 ? (
          <div className="overview-idle-row">ALL BUILDS IDLE // NO QUEUED RUNS</div>
        ) : (
          topBuilds.map((build, i) => {
            const stats = computeBuildChunkStats(build);
            const badge = getBuildStatusBadgeInfo(build);
            return (
              <button
                key={build.id}
                type="button"
                className={i === 0 ? 'overview-build-row dag-box' : 'overview-build-row'}
                onClick={() => useConsoleStore.getState().openDrawer({ kind: 'build', id: build.id })}
              >
                <div className="overview-row-left">
                  <span className="overview-row-title">{build.title || `Build #${build.id}`}</span>
                </div>
                <div className="overview-row-right">
                  <span className={`overview-status-pill status-${badge.tone}`}>{badge.label}</span>
                  <span className="overview-chunk-progress">
                    {stats.completed}/{stats.total} chunks
                  </span>
                </div>
              </button>
            );
          })
        )}
      </div>
    </section>
  );
});

export const OverviewSchedulesSection = memo(function OverviewSchedulesSection() {
  const crons = useConsoleStore((s) => s.schedules.list.data?.crons ?? EMPTY_CRONS);
  const summary = useConsoleStore((s) => s.schedules.list.data?.summary ?? null);
  const activeCount = summary ? summary.total_active : crons.length;
  const topCrons = useMemo(() => crons.slice(0, 4), [crons]);
  const [expandedId, setExpandedId] = useState<string | null>(null);

  return (
    <section className="hud-section bento-cell bento-span-2 overview-bento-section" aria-label="Upcoming Scheduled Routines Overview">
      <div className="section-title-bar section-header">
        <div className="section-title">
          <span className="title-icon">
            <Icon name="alarm" />
          </span>
          <h2>Upcoming Schedules</h2>
        </div>
        <div className="section-actions section-header-actions">
          <span id="overview-schedules-badge" className="section-badge badge">{activeCount} ACTIVE</span>
          <button type="button" className="task-inspect-btn expand-tasks-btn" onClick={viewSchedules}>
            <span>Schedules ↗</span>
          </button>
        </div>
      </div>
      <div className="overview-bento-body">
        {topCrons.length === 0 ? (
          <div className="overview-idle-row">ALL ROUTINES IDLE // NO ACTIVE CRONS</div>
        ) : (
          topCrons.map((cron) => {
            const desc = cron.cron_description || (cron as unknown as { description?: string }).description;
            const exprWithTz = cron.timezone ? `${cron.cron_expr} (${cron.timezone})` : cron.cron_expr;
            const expanded = expandedId === cron.id;
            return (
              <button
                key={cron.id}
                type="button"
                className={expanded ? 'overview-schedule-row expanded' : 'overview-schedule-row'}
                aria-expanded={expanded}
                onClick={() => setExpandedId(expanded ? null : cron.id)}
              >
                <div className="overview-schedule-header">
                  <div className="overview-row-left">
                    <span className="overview-row-title">{cron.title_prefix || 'Recurring Routine'}</span>
                  </div>
                  <div className="overview-row-right">
                    <span className="overview-cron-expr">{exprWithTz}</span>
                    {desc ? <span className="overview-cron-desc">{desc}</span> : null}
                    <span className="overview-schedule-chevron" aria-hidden="true">
                      {expanded ? '▴' : '▾'}
                    </span>
                  </div>
                </div>
                {expanded ? (
                  <div className="overview-schedule-prompt">
                    <span className="overview-schedule-prompt-label">PROMPT DIRECTIVE</span>
                    <div className="overview-schedule-prompt-body">{cron.prompt || 'No prompt configured'}</div>
                  </div>
                ) : null}
              </button>
            );
          })
        )}
      </div>
    </section>
  );
});

/**
 * The telemetry view (#telemetry-view). The view itself subscribes to nothing: every badge, list and card
 * selects its own slice of the status snapshot, so the shell of the view renders once.
 */
export default function TelemetryView() {
  return (
    <div className="overview-bento-grid">
      <section className="hud-section bento-cell bento-span-2 active-tasks-section" aria-live="polite">
        <div className="section-title-bar section-header">
          <div className="section-title">
            <span className="title-icon">
              <Icon name="bolt" />
            </span>
            <h2>Active Agent Turns</h2>
          </div>
          <div className="section-actions section-header-actions">
            <QueueBadge id="tasks-count-badge" />
            <button type="button" className="task-inspect-btn expand-tasks-btn" onClick={expandTasks}>
              <span>Tasks ↗</span>
            </button>
          </div>
        </div>
        <div className="active-tasks-container">
          <TaskQueue />
        </div>
      </section>

      <OverviewBuildsSection />
      <OverviewSchedulesSection />

      <section className="hud-section bento-cell bento-span-3 services-grid-section">
        <div className="section-title-bar section-header">
          <div className="section-title">
            <span className="title-icon">
              <Icon name="bolt" />
            </span>
            <h2>Cluster Service Matrix</h2>
          </div>
          <HealthyCount />
        </div>
        <ServicesGrid />
      </section>

      <section className="hud-section bento-cell bento-span-3 deployment-pipeline-section">
        <div className="section-title-bar section-header">
          <div className="section-title">
            <span className="title-icon">
              <Icon name="rocket" />
            </span>
            <h2>Deployment Pipeline</h2>
          </div>
          <div className="section-actions section-header-badges">
            <GitSyncBadge />
            <DeployBadge />
          </div>
        </div>
        <Deployments />
      </section>
    </div>
  );
}
