import { memo, type RefObject, useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from 'react';
import type { BuildsAPIResponse, VanadisBuildDTO } from '../../api/types';
import { Icon } from '../../components/Icon';
import { useFirstPassLimit } from '../../hooks/useFirstPassLimit';
import { useSearchShortcutTarget } from '../../hooks/useKeyboardShortcuts';
import { usePollingSync } from '../../hooks/usePollingSync';
import { useTabActive } from '../../hooks/useTabActive';
import { type ConsoleState, useConsoleStore } from '../../store/store';
import '../../styles/controls.css';
import '../../styles/sections.css';
import { BuildCard } from './BuildCard';
import { syncBuilds } from './data';
import {
  BUILD_FILTERS,
  type BuildFilter,
  chunkProgress,
  filterBuilds,
  type IndexedBuild,
  indexBuilds,
  normalizeQuery,
  pillCounts,
  resultsLabel,
  showsReconnect,
} from './model';
import './builds.css';

// The drawer body ships in this chunk; App lazy-loads it from here.
export { BuildDetailPanel } from './BuildDetail';

/** Same cadence as the vanilla build queue tab (buildsPollInterval). */
export const BUILDS_POLL_MS = 5000;

const EMPTY_BUILDS: readonly VanadisBuildDTO[] = Object.freeze([]);

const selectData = (s: ConsoleState): BuildsAPIResponse | null => s.builds.list.data;
const selectBuilds = (s: ConsoleState): readonly VanadisBuildDTO[] => s.builds.list.data?.builds ?? EMPTY_BUILDS;
const selectLoaded = (s: ConsoleState): boolean => s.builds.list.data !== null;
const selectReconnect = (s: ConsoleState): boolean => showsReconnect(s.builds.list.data, s.builds.list.error);

/** Polls the queue now (SYNC BUILDS); a newer click or unmount cancels the previous one. */
function useManualSync(): () => void {
  const inflight = useRef<AbortController | null>(null);
  useEffect(() => () => inflight.current?.abort(), []);
  return useCallback(() => {
    inflight.current?.abort();
    const controller = new AbortController();
    inflight.current = controller;
    void syncBuilds(controller.signal);
  }, []);
}

const ReconnectBanner = memo(function ReconnectBanner() {
  const show = useConsoleStore(selectReconnect);
  if (!show) return null;
  return (
    <div className="reconnect-banner">
      <span className="reconnect-icon">
        <Icon name="bolt" />
      </span>{' '}
      <span className="reconnect-text">RECONNECTING TO VANADIS // STALE-WHILE-REVALIDATE ACTIVE</span>
    </div>
  );
});

function MetricCard({ label, value, sub, id, className = 'value' }: {
  readonly label: string;
  readonly value: string | number;
  readonly sub: string;
  readonly id: string;
  readonly className?: string;
}) {
  return (
    <div className="summary-card">
      <div className="card-glow" />
      <span className="label">{label}</span>{' '}
      <span className={className} id={id}>
        {value}
      </span>{' '}
      <span className="sub-label">{sub}</span>
    </div>
  );
}

/** The four summary cards (updateBuildsMetrics). */
const BuildsMetrics = memo(function BuildsMetrics({ index }: { readonly index: readonly IndexedBuild[] }) {
  const running = useConsoleStore((s) => selectData(s)?.running_count ?? 0);
  const queued = useConsoleStore((s) => selectData(s)?.queued_count ?? 0);
  const completed = useConsoleStore((s) => selectData(s)?.completed_count ?? 0);
  return (
    <section className="summary-bar">
      <MetricCard label="RUNNING BUILDS" value={running} sub="ACTIVE PIPELINES" id="builds-running-count" />
      <MetricCard label="QUEUED BUILDS" value={queued} sub="PENDING DISPATCH" id="builds-queued-count" />
      <MetricCard label="COMPLETED BUILDS" value={completed} sub="FINISHED RUNS" id="builds-completed-count" />
      <MetricCard
        label="CHUNK PROGRESS"
        value={chunkProgress(index)}
        sub="COMPLETED / TOTAL"
        id="builds-chunks-progress"
        className="value text-success"
      />
    </section>
  );
});

interface ToolbarProps {
  readonly search: string;
  readonly onSearch: (value: string) => void;
  readonly onRefresh: () => void;
  readonly inputRef: RefObject<HTMLInputElement | null>;
}

/** Search box, clear and sync buttons; memoized so polls never re-render it. */
const BuildsToolbar = memo(function BuildsToolbar({ search, onSearch, onRefresh, inputRef }: ToolbarProps) {
  return (
    <section className="builds-toolbar-card">
      <div className="toolbar-search-wrap">
        <span className="search-icon">
          <Icon name="search" />
        </span>
        <input
          ref={inputRef}
          type="text"
          id="builds-search-input"
          className="cyber-input"
          placeholder="SEARCH BUILDS, REPOS, OBJECTIVES (PRESS '/' TO FOCUS)..."
          aria-label="Search builds"
          autoComplete="off"
          spellCheck={false}
          value={search}
          onChange={(e) => onSearch(e.target.value)}
        />
        {search ? (
          <button
            type="button"
            className="search-clear-btn"
            aria-label="Clear search"
            onClick={() => {
              onSearch('');
              inputRef.current?.focus();
            }}
          >
            <Icon name="x" size={18} />
          </button>
        ) : null}
      </div>
      <div className="toolbar-actions">
        <button type="button" className="cyber-btn-secondary" title="Sync builds from Vanadis" onClick={onRefresh}>
          <span className="refresh-icon">
            <Icon name="refresh" />
          </span>{' '}
          SYNC BUILDS
        </button>
      </div>
    </section>
  );
});

interface PillsProps {
  readonly index: readonly IndexedBuild[];
  readonly filter: BuildFilter;
  readonly onSelect: (filter: BuildFilter) => void;
}

/** Queue-state pills with their counts (renderBuildsPills). */
const BuildsPills = memo(function BuildsPills({ index, filter, onSelect }: PillsProps) {
  const data = useConsoleStore(selectData);
  const counts = pillCounts(data, index);
  return (
    <div className="category-pills-bar" role="toolbar" aria-label="Build Queue Filter Pills">
      {BUILD_FILTERS.map((pill) => (
        <button
          key={pill}
          type="button"
          className={pill === filter ? 'cat-pill active' : 'cat-pill'}
          aria-pressed={pill === filter}
          onClick={() => onSelect(pill)}
        >
          {pill} <span className="pill-count">{counts[pill]}</span>
        </button>
      ))}
    </div>
  );
});

function BuildsSkeleton() {
  return [0, 1, 2, 3].map((i) => (
    <div key={i} className="fact-card-skeleton">
      <div className="skeleton-box skeleton-build-title" />
      <div className="skeleton-box skeleton-build-body" />
      <div className="skeleton-box skeleton-build-meta" />
    </div>
  ));
}

function NoBuilds() {
  return (
    <div className="task-idle-card builds-empty-card">
      <div className="idle-indicator">
        <span className="pulse-dot healthy" /> <span>NO BUILDS MATCHING SELECTION</span>
      </div>
      <div>STATUS: ZERO ACTIVE QUEUED ENTRIES</div>
    </div>
  );
}

/** About one screen of build cards; the rest arrive right after the first paint. */
const FIRST_PASS_BUILDS = 10;

/**
 * The card grid: skeleton until the first payload, then cards (memoized per build) or the empty
 * row. The first pass with builds renders about one screen of cards and fills in after it paints.
 */
const BuildGrid = memo(function BuildGrid({ builds }: { readonly builds: readonly IndexedBuild[] }) {
  const loaded = useConsoleStore(selectLoaded);
  const shown = useFirstPassLimit(loaded ? builds.length : 0, FIRST_PASS_BUILDS);
  let body;
  if (!loaded) body = <BuildsSkeleton />;
  else if (builds.length === 0) body = <NoBuilds />;
  else body = builds.slice(0, shown).map((b) => <BuildCard key={b.build.id} build={b.build} />);
  return (
    <div className="builds-grid" id="builds-grid" aria-live="polite">
      {body}
    </div>
  );
});

/**
 * Port of #builds-view. Pill and search are local state; the visible cards derive from the
 * deferred search text, so a keystroke paints the input at once and the grid follows as a
 * background render. Builds are indexed once per (reconciled) object and cards are memoized per
 * build, so an unchanged poll renders nothing and a changed build re-renders only its own card.
 */
export default function BuildsView() {
  usePollingSync(syncBuilds, BUILDS_POLL_MS, useTabActive());
  const refresh = useManualSync();

  const [filter, setFilter] = useState<BuildFilter>('ALL');
  const [search, setSearch] = useState('');
  const query = normalizeQuery(useDeferredValue(search));
  const inputRef = useRef<HTMLInputElement>(null);
  const clearSearch = useCallback(() => setSearch(''), []);
  useSearchShortcutTarget('builds', inputRef, clearSearch);

  const builds = useConsoleStore(selectBuilds);
  const index = useMemo(() => indexBuilds(builds), [builds]);
  const visible = useMemo(() => filterBuilds(index, filter, query), [index, filter, query]);

  return (
    <>
      <ReconnectBanner />

      <BuildsMetrics index={index} />

      <BuildsToolbar search={search} onSearch={setSearch} onRefresh={refresh} inputRef={inputRef} />

      <BuildsPills index={index} filter={filter} onSelect={setFilter} />

      <div className="results-telemetry-bar">
        <span id="builds-results-count">{resultsLabel(visible.length, index.length)}</span>
        <span className="telemetry-hint">PRESS [ESC] TO CLEAR SEARCH</span>
      </div>

      <section className="builds-grid-section">
        <BuildGrid builds={visible} />
      </section>
    </>
  );
}
