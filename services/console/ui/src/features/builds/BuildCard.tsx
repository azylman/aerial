import { type KeyboardEvent, type MouseEvent, memo, useCallback, useMemo, useState } from 'react';
import type { VanadisBuildDTO, VanadisChunkDTO } from '../../api/types';
import { GlyphText } from '../../components/Glyph';
import { useLiveNow } from '../../hooks/useLiveNow';
import { usePollingSync } from '../../hooks/usePollingSync';
import {
  type BuildChunkStats,
  classifyBuildQueueState,
  computeBuildChunkStats,
  getBuildStatusBadgeInfo,
  getBuildTimingInfo,
} from '../../lib/builds';
import { useConsoleStore } from '../../store/store';
import { BUILD_DETAIL_POLL_MS, BuildDagGraph } from './BuildDetail';
import { syncBuildDetail } from './data';

const EMPTY_CHUNKS: readonly VanadisChunkDTO[] = Object.freeze([]);

function BuildCardInlineDag({ build }: { readonly build: VanadisBuildDTO }) {
  const detailData = useConsoleStore((s) => s.builds.detail.data);
  const [failed, setFailed] = useState<string | null>(null);
  const hasInlineChunks = Boolean(build.chunks && build.chunks.length > 0);
  const isDetailMatch = (detailData?.build?.id ?? detailData?.entry?.id) === build.id;

  const poll = useMemo(() => (build.id ? syncBuildDetail(build.id) : null), [build.id]);
  const fetchDetail = useCallback(
    async (signal: AbortSignal) => {
      if (!poll) return;
      setFailed(null);
      await poll(signal);
      if (!signal.aborted) {
        const latest = useConsoleStore.getState().builds.detail;
        if (latest.error) setFailed(latest.error);
      }
    },
    [poll],
  );

  usePollingSync(fetchDetail, BUILD_DETAIL_POLL_MS, Boolean(build.id && !hasInlineChunks));

  const resolvedChunks = hasInlineChunks
    ? (build.chunks ?? EMPTY_CHUNKS)
    : isDetailMatch
      ? (detailData?.chunks ?? detailData?.build?.chunks ?? EMPTY_CHUNKS)
      : EMPTY_CHUNKS;
  const isLoaded = hasInlineChunks || isDetailMatch;
  const hasFailed = !isLoaded && failed !== null;
  const isPending = !isLoaded && !hasFailed;

  return (
    <div className="build-card-dag">
      {resolvedChunks.length > 0 ? (
        <BuildDagGraph buildId={build.id} chunks={resolvedChunks} />
      ) : isPending ? (
        <div className="overview-build-loading">
          <span className="pulse-dot healthy" /> LOADING BUILD CHUNKS...
        </div>
      ) : hasFailed ? (
        <div className="overview-build-error">FAILED TO LOAD BUILD CHUNKS // {failed}</div>
      ) : (
        <div className="overview-build-empty">NO CHUNKS REGISTERED</div>
      )}
    </div>
  );
}

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

export interface BuildCardProps {
  readonly build: VanadisBuildDTO;
  readonly expanded: boolean;
  readonly onToggle: (id: string) => void;
}

/**
 * One build queue card. Memoized on the reconciled build object, so a poll that leaves
 * the build unchanged skips it entirely; only a running build's duration ticks. Clicking (or
 * Enter/Space on the focused card) expands the inline chunk DAG; clicking a chunk node in the
 * DAG opens that chunk's detail overlay.
 */
export const BuildCard = memo(function BuildCard({ build, expanded, onToggle }: BuildCardProps) {
  const stats = computeBuildChunkStats(build);
  // Start and end never depend on the clock; only a running build's duration does.
  const timing = getBuildTimingInfo(build, 0);
  const running = classifyBuildQueueState(build) === 'running';

  const toggle = (): void => {
    if (build.id) onToggle(build.id);
  };

  const onSummaryKeyDown = (event: KeyboardEvent<HTMLElement>): void => {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    event.preventDefault();
    toggle();
  };

  const onCardClick = (event: MouseEvent<HTMLDivElement>): void => {
    if (event.target === event.currentTarget) toggle();
  };

  const onCardKeyDown = (event: KeyboardEvent<HTMLDivElement>): void => {
    if (event.target !== event.currentTarget) return;
    if (event.key !== 'Enter' && event.key !== ' ') return;
    event.preventDefault();
    toggle();
  };

  return (
    <div
      className={expanded ? 'build-card expanded' : 'build-card'}
      data-build-id={build.id || ''}
      onClick={onCardClick}
      onKeyDown={onCardKeyDown}
    >
      <div
        className="build-card-summary"
        role="button"
        tabIndex={0}
        aria-expanded={expanded}
        onClick={toggle}
        onKeyDown={onSummaryKeyDown}
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
      {expanded ? <BuildCardInlineDag build={build} /> : null}
    </div>
  );
});
