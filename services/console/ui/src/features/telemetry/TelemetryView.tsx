import { memo, useEffect, useMemo, useState } from 'react';
import type { CronSchedule, VanadisBuildDTO, VanadisChunkDTO } from '../../api/types';
import { Icon } from '../../components/Icon';
import {
  compareBuilds,
  computeBuildChunkStats,
  getBuildStatusBadgeInfo,
} from '../../lib/builds';
import { buildsBadgeCount } from '../../store/selectors';
import { useConsoleStore } from '../../store/store';
import '../../styles/sections.css';
import { QueueBadge, TaskQueue } from '../tasks/TaskQueue';
import { BuildDagGraph } from '../builds/BuildDetail';
import { resolveBuildDetail } from '../builds/model';
import { syncBuildDetail } from '../builds/data';
import { DeployBadge, Deployments } from './Deployments';
import { HealthyCount, ServicesGrid } from './ServicesGrid';
import './telemetry.css';

const expandTasks = (): void => useConsoleStore.getState().setActiveTab('tasks');
const viewAllBuilds = (): void => useConsoleStore.getState().setActiveTab('builds');
const viewSchedules = (): void => useConsoleStore.getState().setActiveTab('schedules');

const EMPTY_BUILDS: readonly VanadisBuildDTO[] = Object.freeze([]);
const EMPTY_CHUNKS: readonly VanadisChunkDTO[] = Object.freeze([]);
const EMPTY_CRONS: readonly CronSchedule[] = Object.freeze([]);

export const OverviewBuildsSection = memo(function OverviewBuildsSection() {
  const builds = useConsoleStore((s) => s.builds.list.data?.builds ?? EMPTY_BUILDS);
  const data = useConsoleStore((s) => s.builds.list.data);
  const detailData = useConsoleStore((s) => s.builds.detail.data);
  const activeCount = buildsBadgeCount(data);
  const topBuilds = useMemo(
    () =>
      [...builds]
        .sort(compareBuilds)
        .slice(0, 3),
    [builds],
  );
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [failed, setFailed] = useState<{ readonly id: string; readonly error: string } | null>(null);

  const targetBuild = useMemo(
    () => (expandedId ? topBuilds.find((b) => b.id === expandedId) : undefined),
    [topBuilds, expandedId],
  );
  const hasInlineChunks = Boolean(targetBuild?.chunks && targetBuild.chunks.length > 0);
  const hasDetailMatch = (detailData?.build?.id ?? detailData?.entry?.id) === expandedId;

  useEffect(() => {
    if (!expandedId || hasInlineChunks || hasDetailMatch) {
      return;
    }
    const controller = new AbortController();
    void syncBuildDetail(expandedId)(controller.signal).finally(() => {
      if (!controller.signal.aborted) {
        const latest = useConsoleStore.getState().builds.detail;
        if (latest.error) {
          setFailed({ id: expandedId, error: latest.error });
        }
      }
    });
    return () => {
      controller.abort();
    };
  }, [expandedId, hasInlineChunks, hasDetailMatch]);

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
            const expanded = expandedId === build.id;
            const model = resolveBuildDetail(build.id, detailData, build);
            const resolvedChunks = model?.chunks ?? EMPTY_CHUNKS;
            const isBuildDetailMatch = (detailData?.build?.id ?? detailData?.entry?.id) === build.id;
            const isLoaded = (build.chunks?.length ?? 0) > 0 || isBuildDetailMatch;
            const hasFailed = !isLoaded && failed?.id === build.id;
            const isPending = !isLoaded && !hasFailed;

            const rowClass = [
              'overview-build-row',
              i === 0 ? 'dag-box' : null,
              expanded ? 'expanded' : null,
            ].filter(Boolean).join(' ');

            return (
              <div key={build.id} className={rowClass}>
                <button
                  type="button"
                  className="overview-build-header"
                  aria-expanded={expanded}
                  onClick={() => {
                    setExpandedId(expanded ? null : build.id);
                    setFailed(null);
                  }}
                >
                  <div className="overview-row-left">
                    <span className="overview-row-title">{build.title || `Build #${build.id}`}</span>
                  </div>
                  <div className="overview-row-right">
                    <span className={`overview-status-pill status-${badge.tone}`}>{badge.label}</span>
                    <span className="overview-chunk-progress">
                      {stats.completed}/{stats.total} chunks
                    </span>
                    <span className="overview-schedule-chevron" aria-hidden="true">
                      {expanded ? '▴' : '▾'}
                    </span>
                  </div>
                </button>
                {expanded ? (
                  <div className="overview-build-dag">
                    {resolvedChunks.length > 0 ? (
                      <BuildDagGraph buildId={build.id} chunks={resolvedChunks} />
                    ) : isPending ? (
                      <div className="overview-build-loading">
                        <span className="pulse-dot healthy" /> LOADING BUILD CHUNKS...
                      </div>
                    ) : hasFailed ? (
                      <div className="overview-build-error">FAILED TO LOAD BUILD CHUNKS // {failed.error}</div>
                    ) : (
                      <div className="overview-build-empty">NO CHUNKS REGISTERED</div>
                    )}
                  </div>
                ) : null}
              </div>
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
            <DeployBadge />
          </div>
        </div>
        <Deployments />
      </section>
    </div>
  );
}
