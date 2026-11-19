import { memo, useMemo } from 'react';
import type { CronSchedule, VanadisBuildDTO } from '../../api/types';
import { GitSyncBadge } from '../../components/GitSyncBadge';
import { Icon } from '../../components/Icon';
import { computeBuildChunkStats, getBuildStatusBadgeInfo } from '../../lib/builds';
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

export const OverviewBuildsSection = memo(function OverviewBuildsSection() {
  const builds = useConsoleStore((s) => s.builds.list.data?.builds ?? EMPTY_BUILDS);
  const data = useConsoleStore((s) => s.builds.list.data);
  const activeCount = buildsBadgeCount(data);
  const topBuilds = useMemo(() => builds.slice(0, 3), [builds]);

  return (
    <section className="overview-bento-section" aria-label="Vanadis Build Queue Overview">
      <div className="section-title-bar">
        <div className="section-title">
          <span className="title-icon">
            <Icon name="wrench" />
          </span>
          <h2>VANADIS BUILD QUEUE</h2>
        </div>
        <div className="section-actions">
          <span className="section-badge">{activeCount} ACTIVE</span>
          <button type="button" className="task-inspect-btn expand-tasks-btn" onClick={viewAllBuilds}>
            <span>VIEW ALL BUILDS ↗</span>
          </button>
        </div>
      </div>
      <div className="overview-bento-body">
        {topBuilds.length === 0 ? (
          <div className="overview-idle-row">ALL BUILDS IDLE // NO QUEUED RUNS</div>
        ) : (
          topBuilds.map((build) => {
            const stats = computeBuildChunkStats(build);
            const badge = getBuildStatusBadgeInfo(build);
            return (
              <button
                key={build.id}
                type="button"
                className="overview-build-row"
                onClick={() => useConsoleStore.getState().openDrawer({ kind: 'build', id: build.id })}
              >
                <div className="overview-row-left">
                  <span className="overview-row-title">BUILD // {build.title || build.id}</span>
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

  return (
    <section className="overview-bento-section" aria-label="Upcoming Scheduled Routines Overview">
      <div className="section-title-bar">
        <div className="section-title">
          <span className="title-icon">
            <Icon name="alarm" />
          </span>
          <h2>UPCOMING SCHEDULED ROUTINES</h2>
        </div>
        <div className="section-actions">
          <span className="section-badge">{activeCount} ACTIVE</span>
          <button type="button" className="task-inspect-btn expand-tasks-btn" onClick={viewSchedules}>
            <span>VIEW SCHEDULES ↗</span>
          </button>
        </div>
      </div>
      <div className="overview-bento-body">
        {topCrons.length === 0 ? (
          <div className="overview-idle-row">ALL ROUTINES IDLE // NO ACTIVE CRONS</div>
        ) : (
          topCrons.map((cron) => (
            <div key={cron.id} className="overview-schedule-row">
              <div className="overview-row-left">
                <span className="overview-row-title">ROUTINE // {cron.title_prefix || 'RECURRING ROUTINE'}</span>
              </div>
              <div className="overview-row-right">
                <span className="overview-cron-expr">{cron.cron_expr}</span>
                {cron.cron_description ? (
                  <span className="overview-cron-desc">{cron.cron_description}</span>
                ) : null}
              </div>
            </div>
          ))
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
    <>
      <section className="active-tasks-section" aria-live="polite">
        <div className="section-title-bar">
          <div className="section-title">
            <span className="title-icon">
              <Icon name="bolt" />
            </span>
            <h2>LIVE AGENT EXECUTION QUEUE</h2>
          </div>
          <div className="section-actions">
            <QueueBadge id="tasks-count-badge" />
            <button type="button" className="task-inspect-btn expand-tasks-btn" onClick={expandTasks}>
              <span>EXPAND VIEW ↗</span>
            </button>
          </div>
        </div>
        <div className="active-tasks-container">
          <TaskQueue />
        </div>
      </section>

      <section className="deployment-pipeline-section">
        <div className="section-title-bar">
          <div className="section-title">
            <span className="title-icon">
              <Icon name="rocket" />
            </span>
            <h2>DEPLOYMENT PIPELINE</h2>
          </div>
          <div className="section-actions">
            <DeployBadge />
            <GitSyncBadge />
          </div>
        </div>
        <Deployments />
      </section>

      <div className="overview-bento-grid">
        <OverviewBuildsSection />
        <OverviewSchedulesSection />
      </div>

      <main className="services-grid-section">
        <div className="section-title-bar">
          <div className="section-title">
            <span className="title-icon">
              <Icon name="bolt" />
            </span>
            <h2>ACTIVE STACK COMPONENTS</h2>
          </div>
          <HealthyCount />
        </div>
        <ServicesGrid />
      </main>
    </>
  );
}
