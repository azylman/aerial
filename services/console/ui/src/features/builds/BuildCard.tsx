import { type KeyboardEvent, memo } from 'react';
import type { VanadisBuildDTO } from '../../api/types';
import { GlyphText } from '../../components/Glyph';
import { useLiveNow } from '../../hooks/useLiveNow';
import {
  type BuildChunkStats,
  classifyBuildQueueState,
  computeBuildChunkStats,
  getBuildStatusBadgeInfo,
  getBuildTimingInfo,
} from '../../lib/builds';
import { useConsoleStore } from '../../store/store';

/** Status pill shared by the queue cards and the drawer header (getBuildStatusBadgeInfo). */
export function BuildStatusBadge({ build }: { readonly build: VanadisBuildDTO }) {
  const badge = getBuildStatusBadgeInfo(build);
  return (
    <span className={`build-status-badge status-${badge.tone}`}>
      <GlyphText icon={badge.icon} text={badge.label} />
    </span>
  );
}

/** Completed-chunk header and gradient bar shared by the cards and the drawer. */
export function ChunkProgress({ stats, suffix }: { readonly stats: BuildChunkStats; readonly suffix: string }) {
  return (
    <>
      <div className="build-chunk-progress-header">
        <span className="build-chunk-progress-label">
          {stats.completed} / {stats.total} {suffix}
        </span>
        <span className="build-chunk-progress-pct">{stats.percent}%</span>
      </div>
      <div className="build-progress-bar-bg">
        <div className="build-progress-bar-fill" style={{ width: `${stats.percent}%` }} />
      </div>
    </>
  );
}

function DurationBadge({ text }: { readonly text: string }) {
  return (
    <span className="build-duration-badge">
      <GlyphText icon="timer" text={text} />
    </span>
  );
}

/** A running build's duration: the only part of a card on the shared 1s clock. */
const LiveDuration = memo(function LiveDuration({ build }: { readonly build: VanadisBuildDTO }) {
  const now = useLiveNow();
  return <DurationBadge text={getBuildTimingInfo(build, now).durationText} />;
});

function openBuild(id: string): void {
  if (id) useConsoleStore.getState().openDrawer({ kind: 'build', id });
}

/**
 * One build queue card. Memoized on the reconciled build object, so a poll that leaves
 * the build unchanged skips it entirely; only a running build's duration ticks. Clicking (or
 * Enter/Space on the focused card) opens the build's detail drawer.
 */
export const BuildCard = memo(function BuildCard({ build }: { readonly build: VanadisBuildDTO }) {
  const stats = computeBuildChunkStats(build);
  // Start and end never depend on the clock; only a running build's duration does.
  const timing = getBuildTimingInfo(build, 0);
  const running = classifyBuildQueueState(build) === 'running';
  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>): void => {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    event.preventDefault();
    openBuild(build.id);
  };

  return (
    <div
      className="build-card"
      data-build-id={build.id || ''}
      role="button"
      tabIndex={0}
      onClick={() => openBuild(build.id)}
      onKeyDown={onKeyDown}
    >
      <div className="build-card-header">
        <div className="build-badge-group">
          <BuildStatusBadge build={build} />
        </div>
        {running ? <LiveDuration build={build} /> : <DurationBadge text={timing.durationText} />}
      </div>
      <div className="build-card-body">
        <h3 className="build-title">{build.title || build.id || 'Untitled Build'}</h3>
        <div className="build-timing-meta">
          <span className="build-time-item">
            <strong>START:</strong> {timing.startText}
          </span>
          {timing.endText ? (
            <span className="build-time-item">
              <strong>END:</strong> {timing.endText}
            </span>
          ) : null}
        </div>
      </div>
      <div className="build-chunk-progress-wrap">
        <ChunkProgress stats={stats} suffix="CHUNKS" />
      </div>
    </div>
  );
});
