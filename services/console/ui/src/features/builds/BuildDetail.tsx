import { memo, useMemo } from 'react';
import type { VanadisBuildDTO, VanadisChunkDTO } from '../../api/types';
import { Icon } from '../../components/Icon';
import { usePollingSync } from '../../hooks/usePollingSync';
import { computeBuildChunkStats } from '../../lib/builds';
import { InlineMarkdown, Markdown } from '../../lib/markdown';
import { type ConsoleState, useConsoleStore } from '../../store/store';
import { BuildStatusBadge, ChunkProgress } from './BuildCard';
import { syncBuildDetail } from './data';
import { buildChunkDag, chunkNumber, chunkPrUrl, chunkStatusBadge, findBuild, resolveBuildDetail } from './model';
import './builds.css';

/** The drawer re-fetches its build at the queue's cadence while open and visible. */
export const BUILD_DETAIL_POLL_MS = 5000;

const selectDetail = (s: ConsoleState) => s.builds.detail.data;
const selectDetailError = (s: ConsoleState): string | null => s.builds.detail.error;

function TagSection({ label, tags, className }: { readonly label: string; readonly tags: readonly string[]; readonly className: string }) {
  return (
    <div className="chunk-section">
      <span className="chunk-section-label">{label}</span>
      <div className="chunk-tags-list">
        {tags.map((tag, i) => (
          <span key={i} className={className}>
            {tag}
          </span>
        ))}
      </div>
    </div>
  );
}

/** Interactive DAG graph of build chunks rendered across rank rows with SVG bezier connectors. */
export function BuildDagGraph({
  buildId,
  chunks,
  activeChunkId,
}: {
  readonly buildId: string;
  readonly chunks: readonly VanadisChunkDTO[];
  readonly activeChunkId?: string | undefined;
}) {
  const dag = useMemo(() => buildChunkDag(chunks), [chunks]);
  if (dag.ranks.length === 0) return null;

  return (
    <div className="build-dag-graph">
      {dag.edges.length > 0 ? (
        <svg className="build-dag-edges" viewBox="0 0 100 100" preserveAspectRatio="none" aria-hidden="true">
          {dag.edges.map((edge) => {
            const fromX = ((edge.fromIndex + 0.5) / edge.fromCount) * 100;
            const fromY = ((edge.fromRank + 0.5) / dag.ranks.length) * 100;
            const toX = ((edge.toIndex + 0.5) / edge.toCount) * 100;
            const toY = ((edge.toRank + 0.5) / dag.ranks.length) * 100;
            const midY = (fromY + toY) / 2;
            const d = `M ${fromX} ${fromY} C ${fromX} ${midY}, ${toX} ${midY}, ${toX} ${toY}`;
            return <path key={`${edge.fromId}->${edge.toId}`} className="build-dag-edge" d={d} />;
          })}
        </svg>
      ) : null}
      {dag.ranks.map((rankNodes, rankIdx) => (
        <div key={rankIdx} className="build-dag-rank" data-rank={rankIdx}>
          {rankNodes.map((node) => {
            const isActive =
              activeChunkId !== undefined && (activeChunkId === node.chunk.id || activeChunkId === String(node.number));
            return (
              <button
                key={node.chunk.id}
                type="button"
                className={`build-dag-node${isActive ? ' active' : ''}`}
                data-chunk-id={node.chunk.id}
                data-chunk-index={node.number}
                onClick={() =>
                  useConsoleStore.getState().openDrawer({ kind: 'build', id: buildId, chunkId: node.chunk.id })
                }
              >
                <span className="chunk-index-badge">#{node.number}</span>
                <span className="chunk-title">{node.chunk.title || `Chunk ${node.number}`}</span>
                <span className={node.status.className}>{node.status.label}</span>
              </button>
            );
          })}
        </div>
      ))}
    </div>
  );
}

/** One chunk card; memoized on the reconciled chunk so unchanged polls skip it. */
export const ChunkCard = memo(function ChunkCard({
  chunk,
  position,
  defaultOpen,
}: {
  readonly chunk: VanadisChunkDTO;
  readonly position: number;
  readonly defaultOpen?: boolean;
}) {
  const number = chunkNumber(chunk, position);
  const status = chunkStatusBadge(chunk.status);
  const prUrl = chunkPrUrl(chunk);
  const files = chunk.target_files ?? [];
  const symbols = chunk.target_symbols ?? [];
  const criteria = chunk.acceptance_criteria ?? [];
  return (
    <div className="chunk-card" data-chunk-index={number}>
      <div className="chunk-header">
        <div className="chunk-header-left">
          <span className="chunk-index-badge">#{number}</span>
          <h4 className="chunk-title">{chunk.title || `Chunk ${number}`}</h4>
        </div>
        <div className="chunk-header-right">
          {chunk.effort ? <span className="chunk-effort-badge">{chunk.effort}</span> : null}
          {chunk.repo ? <span className="chunk-repo-badge">{chunk.repo}</span> : null}
          <span className={status.className}>{status.label}</span>
          {prUrl === null ? null : (
            <a className="chunk-pr-link" href={prUrl} target="_blank" rel="noopener noreferrer">
              #{chunk.pr_number} <Icon name="external" size={11} />
            </a>
          )}
        </div>
      </div>
      <div className="chunk-body">
        {chunk.instructions ? (
          <details className="chunk-section chunk-instructions-details" open={defaultOpen ? true : undefined}>
            <summary className="chunk-section-label chunk-instructions-toggle">
              <span className="chunk-instructions-chevron">
                <Icon name="chevron-right" size={11} />
              </span>{' '}
              INSTRUCTIONS
            </summary>
            <div className="chunk-instructions-box markdown-body">
              <Markdown source={chunk.instructions} />
            </div>
          </details>
        ) : null}
        {files.length > 0 ? <TagSection label="TARGET FILES" tags={files} className="chunk-tag file-tag" /> : null}
        {symbols.length > 0 ? <TagSection label="TARGET SYMBOLS" tags={symbols} className="chunk-tag symbol-tag" /> : null}
        {criteria.length > 0 ? (
          <div className="chunk-section">
            <span className="chunk-section-label">ACCEPTANCE CRITERIA</span>
            <ul className="chunk-criteria-list markdown-body">
              {criteria.map((c, i) => (
                <li key={i}>
                  <InlineMarkdown source={c} />
                </li>
              ))}
            </ul>
          </div>
        ) : null}
      </div>
    </div>
  );
});

export interface ContentProps {
  readonly build: VanadisBuildDTO;
  readonly chunks: readonly VanadisChunkDTO[];
  readonly chunkId?: string | undefined;
}

/** Build detail content: summary header, topological DAG graph, and chunk detail overlay. */
export const BuildDetailContent = memo(function BuildDetailContent({ build, chunks, chunkId }: ContentProps) {
  const stats = computeBuildChunkStats({ ...build, chunks });
  const selectedIndex =
    chunkId !== undefined
      ? chunks.findIndex((c, i) => c.id === chunkId || String(chunkNumber(c, i)) === chunkId)
      : -1;
  const selectedChunk = selectedIndex >= 0 ? chunks[selectedIndex] : undefined;

  return (
    <>
      <div className="build-drawer-header-summary">
        <div className="build-drawer-title-row">
          <BuildStatusBadge build={build} />
          <span className="build-tier-badge">TIER {String(build.tier || 0)}</span>
          <span className="build-id-pill">{build.id || ''}</span>
        </div>
        <h3 className="build-drawer-title">{build.title || build.id}</h3>
        <div className="build-drawer-section">
          <span className="build-drawer-section-label">OBJECTIVE</span>
          <div className="build-objective-text markdown-body">
            <Markdown source={build.objective || 'No objective provided'} />
          </div>
        </div>
        {build.shared_context ? (
          <div className="build-drawer-section">
            <span className="build-drawer-section-label">SHARED BUILD CONTEXT</span>
            <div className="build-context-box markdown-body">
              <Markdown source={build.shared_context} />
            </div>
          </div>
        ) : null}
        <div className="build-drawer-progress-bar">
          <ChunkProgress stats={stats} suffix="CHUNKS COMPLETED" />
        </div>
      </div>
      <div className="build-drawer-chunks-section">
        <h4 className="chunks-section-title">BUILD CHUNK DAG ({chunks.length})</h4>
        <div className="chunks-list">
          {chunks.length === 0 ? (
            <div className="drawer-no-chunks">NO CHUNKS REGISTERED</div>
          ) : (
            <>
              <BuildDagGraph buildId={build.id} chunks={chunks} activeChunkId={chunkId} />
              {selectedChunk !== undefined ? (
                <div className="chunk-detail-panel">
                  <button
                    type="button"
                    className="chunk-back-btn"
                    onClick={() => useConsoleStore.getState().openDrawer({ kind: 'build', id: build.id })}
                  >
                    ← Back to Build DAG
                  </button>
                  <ChunkCard chunk={selectedChunk} position={selectedIndex} defaultOpen />
                </div>
              ) : null}
            </>
          )}
        </div>
      </div>
    </>
  );
});

export function BuildDetail({ id, chunkId }: { readonly id: string; readonly chunkId?: string | undefined }) {
  const poll = useMemo(() => syncBuildDetail(id), [id]);
  usePollingSync(poll, BUILD_DETAIL_POLL_MS, true);

  const detail = useConsoleStore(selectDetail);
  const error = useConsoleStore(selectDetailError);
  const entry = useConsoleStore((s) => findBuild(s.builds.list.data, id));
  const model = useMemo(() => resolveBuildDetail(id, detail, entry), [id, detail, entry]);

  if (model !== null) return <BuildDetailContent build={model.build} chunks={model.chunks} chunkId={chunkId} />;
  if (error !== null) {
    return (
      <div className="empty-state">
        <span className="empty-icon text-danger">
          <Icon name="alert" />
        </span>
        <p>FAILED TO LOAD BUILD DETAILS</p>
        <span className="empty-sub">{error || 'Network error'}</span>
      </div>
    );
  }
  return (
    <div className="drawer-loading-box">
      <span className="pulse-dot healthy" /> <span>LOADING BUILD TELEMETRY // {id}...</span>
    </div>
  );
}

/**
 * Build drawer body. It renders the queue entry at once, then the
 * fetched detail, and keeps polling the build while the drawer is open; closing the drawer (or
 * hiding the page) aborts the in-flight request. Keyed by id, so switching builds restarts the poll.
 */
export const BuildDetailPanel = memo(function BuildDetailPanel({
  target,
}: {
  readonly target: { readonly id: string; readonly chunkId?: string | undefined };
}) {
  const storeChunkId = useConsoleStore((s) =>
    s.drawer.kind === 'build' && s.drawer.id === target.id ? s.drawer.chunkId : undefined,
  );
  const chunkId = target.chunkId ?? storeChunkId;
  return <BuildDetail key={target.id} id={target.id} chunkId={chunkId} />;
});
