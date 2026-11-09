import { memo, type RefObject, useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from 'react';
import type { FactItem, FactsAPIResponse } from '../../api/types';
import { Glyph } from '../../components/Glyph';
import { Icon } from '../../components/Icon';
import { VirtualList } from '../../components/VirtualList';
import { useSearchShortcutTarget } from '../../hooks/useKeyboardShortcuts';
import { type ConsoleState, useConsoleStore } from '../../store/store';
import '../../styles/controls.css';
import '../../styles/sections.css';
import { syncFacts } from './data';
import { FactCard } from './FactCard';
import {
  categoryCounts,
  FACT_CATEGORIES,
  type FactCategory,
  filterFacts,
  type IndexedFact,
  indexFacts,
  memoryMetrics,
  memoryPhase,
  normalizeQuery,
  resultsLabel,
} from './model';
import './memory.css';

const EMPTY_FACTS: readonly FactItem[] = Object.freeze([]);

const selectData = (s: ConsoleState): FactsAPIResponse | null => s.memory.facts.data;
const selectFacts = (s: ConsoleState): readonly FactItem[] => s.memory.facts.data?.facts ?? EMPTY_FACTS;
const selectError = (s: ConsoleState): string | null => s.memory.facts.error;

/** Grid geometry from .memory-grid: repeat(auto-fill, minmax(340px, 1fr)) with a 1.25rem gap. */
const CARD_MIN_WIDTH = 340;
const CARD_GAP = 20;
/** A one-line fact card; the list switches to measured heights once any row has mounted. */
const CARD_ESTIMATE = 190;
const OVERSCAN_ROWS = 2;

/** Before the first payload the vanilla bar holds only the static ALL pill. */
const IDLE_CATEGORIES: readonly FactCategory[] = ['ALL'];

/**
 * When the archive was last synced, for the SYNCED label and card ages (renderFactCards ages
 * every card from its render time). Re-stamped whenever a new payload lands, and by `touch`
 * after a manual sync whose payload was identical.
 */
function useSyncStamp(data: FactsAPIResponse | null): readonly [number, () => void] {
  const [stamp, setStamp] = useState(() => ({ data, at: Date.now() }));
  if (stamp.data !== data) setStamp({ data, at: Date.now() });
  const touch = useCallback(() => setStamp((prev) => ({ data: prev.data, at: Date.now() })), []);
  return [stamp.at, touch];
}

/** Fetches now (SYNC ARCHIVE, RETRY); a newer click or unmount cancels the previous one. */
function useManualSync(onSynced: () => void): () => void {
  const inflight = useRef<AbortController | null>(null);
  useEffect(() => () => inflight.current?.abort(), []);
  return useCallback(() => {
    inflight.current?.abort();
    const controller = new AbortController();
    inflight.current = controller;
    void syncFacts(controller.signal).then(() => {
      if (!controller.signal.aborted && selectError(useConsoleStore.getState()) === null) onSynced();
    });
  }, [onSynced]);
}

interface MetricsProps {
  readonly index: readonly IndexedFact[];
  readonly syncedAt: number;
}

/** The four summary cards (updateMemoryMetrics / renderMemoryError's status badge). */
const MemoryMetrics = memo(function MemoryMetrics({ index, syncedAt }: MetricsProps) {
  const data = useConsoleStore(selectData);
  const failed = useConsoleStore((s) => selectError(s) !== null);
  const metrics = memoryMetrics(data, index);
  return (
    <section className="summary-bar">
      <div className="summary-card">
        <div className="card-glow" />
        <span className="label">TOTAL MEMORY NODES</span>{' '}
        <span className="value" id="memory-total-count">
          {metrics.total}
        </span>{' '}
        <span className="sub-label">{metrics.reinforced}</span>
      </div>
      <div className="summary-card">
        <div className="card-glow" />
        <span className="label">PERMET SYNAPSE STORE</span>{' '}
        <span className={failed ? 'value text-danger' : 'value text-success'}>{failed ? 'DEGRADED' : 'OPTIMAL'}</span>{' '}
        <span className="sub-label">{data === null ? 'SYNCED' : `SYNCED ${new Date(syncedAt).toLocaleTimeString()}`}</span>
      </div>
      <div className="summary-card">
        <div className="card-glow" />
        <span className="label">AVG REINFORCEMENTS</span>{' '}
        <span className="value" id="memory-avg-importance">
          {metrics.average}
        </span>{' '}
        <span className="sub-label">EMPIRICAL ACTIVATION</span>
      </div>
      <div className="summary-card">
        <div className="card-glow" />
        <span className="label">VECTOR EMBEDDING MODEL</span> <span className="value">OLLAMA</span>{' '}
        <span className="sub-label">all-minilm:latest (384d)</span>
      </div>
    </section>
  );
});

interface ToolbarProps {
  readonly search: string;
  readonly onSearch: (value: string) => void;
  readonly onRefresh: () => void;
  readonly inputRef: RefObject<HTMLInputElement | null>;
}

/** Search box, clear and sync buttons; memoized so list updates never re-render it. */
const MemoryToolbar = memo(function MemoryToolbar({ search, onSearch, onRefresh, inputRef }: ToolbarProps) {
  return (
    <section className="memory-toolbar-card">
      <div className="toolbar-search-wrap">
        <span className="search-icon">
          <Icon name="search" />
        </span>
        <input
          ref={inputRef}
          type="text"
          id="memory-search-input"
          className="cyber-input"
          placeholder="SEARCH FACTS OR NODE IDS (PRESS '/' TO FOCUS)..."
          aria-label="Search memory facts"
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
        <button type="button" className="cyber-btn-secondary" title="Sync memory from brain" onClick={onRefresh}>
          <span className="refresh-icon">
            <Icon name="refresh" />
          </span>{' '}
          SYNC ARCHIVE
        </button>
      </div>
    </section>
  );
});

interface PillsProps {
  readonly index: readonly IndexedFact[];
  readonly loaded: boolean;
  readonly category: FactCategory;
  readonly onSelect: (category: FactCategory) => void;
}

/** Category pills with their counts (renderCategoryPills). */
const CategoryPills = memo(function CategoryPills({ index, loaded, category, onSelect }: PillsProps) {
  const counts = useMemo(() => categoryCounts(index), [index]);
  return (
    <div className="category-pills-bar" role="toolbar" aria-label="Category Filters">
      {(loaded ? FACT_CATEGORIES : IDLE_CATEGORIES).map((cat) => (
        <button
          key={cat}
          type="button"
          className={cat === category ? 'cat-pill active' : 'cat-pill'}
          aria-pressed={cat === category}
          onClick={() => onSelect(cat)}
        >
          {cat} <span className="pill-count">{counts[cat]}</span>
        </button>
      ))}
    </div>
  );
});

function FactsSkeleton() {
  return (
    <div className="memory-grid">
      {[0, 1, 2, 3, 4, 5].map((i) => (
        <div key={i} className="fact-card-skeleton">
          <div className="skeleton-box skeleton-node-title" />
          <div className="skeleton-box skeleton-node-body" />
          <div className="skeleton-box skeleton-node-meta" />
        </div>
      ))}
    </div>
  );
}

function MemoryError({ message, onRetry }: { readonly message: string; readonly onRetry: () => void }) {
  return (
    <div className="memory-grid">
      <div className="permet-alert-box">
        <h3>
          <Glyph name="bolt" /> PERMET LINK SEVERED // BRAIN OFFLINE
        </h3>
        <p>{message}</p>
        <button type="button" className="cyber-btn-secondary" onClick={onRetry}>
          <span className="refresh-icon">
            <Icon name="bolt" />
          </span>{' '}
          RETRY SYNCHRONIZATION
        </button>
      </div>
    </div>
  );
}

function NoMatches() {
  return (
    <div className="memory-grid">
      <div className="empty-state-box">
        <div className="empty-state-glyph">
          <Glyph name="sparkle" />
        </div>
        <div className="empty-state-title">NO PERMET MEMORY NODES MATCH CRITERIA</div>
        <div className="empty-state-hint">Try adjusting your keyword filter or switching category pills.</div>
      </div>
    </div>
  );
}

const factKey = (f: IndexedFact): string => String(f.fact.id);

interface ListProps {
  readonly facts: readonly IndexedFact[];
  readonly query: string;
  readonly now: number;
}

/** The windowed card grid; only cards near the viewport are mounted. */
const FactList = memo(function FactList({ facts, query, now }: ListProps) {
  const renderItem = useCallback(
    (f: IndexedFact) => <FactCard fact={f.fact} count={f.count} query={query} now={now} />,
    [query, now],
  );
  return (
    <VirtualList
      items={facts}
      getKey={factKey}
      estimateHeight={CARD_ESTIMATE}
      overscan={OVERSCAN_ROWS}
      renderItem={renderItem}
      minItemWidth={CARD_MIN_WIDTH}
      gap={CARD_GAP}
      className="memory-list"
      ariaLabel="Memory facts"
    />
  );
});

/**
 * Port of #memory-view. Pill and search are local state; the visible list derives from the
 * deferred search, so a keystroke paints the input at once and the list follows as a background
 * render. Facts are indexed and sorted once per payload, filtered in one pass per change and
 * rendered through VirtualList, so the archive's cost no longer grows with its size.
 */
export default function MemoryView() {
  const data = useConsoleStore(selectData);
  const facts = useConsoleStore(selectFacts);
  const error = useConsoleStore(selectError);
  const [syncedAt, touchSynced] = useSyncStamp(data);
  const refresh = useManualSync(touchSynced);

  // The shell's boot load failed: opening the tab retries it (fetchFacts on enter until loaded).
  useEffect(() => {
    const { data: cached, error: failed } = useConsoleStore.getState().memory.facts;
    if (cached === null && failed !== null) refresh();
  }, [refresh]);

  const [category, setCategory] = useState<FactCategory>('ALL');
  const [search, setSearch] = useState('');
  const deferred = useDeferredValue(search);
  const query = normalizeQuery(deferred);
  const inputRef = useRef<HTMLInputElement>(null);
  const clearSearch = useCallback(() => setSearch(''), []);
  useSearchShortcutTarget('memory', inputRef, clearSearch);

  const index = useMemo(() => indexFacts(facts), [facts]);
  const visible = useMemo(() => filterFacts(index, category, query), [index, category, query]);
  const phase = memoryPhase(data !== null, error, visible.length);

  return (
    <>
      <MemoryMetrics index={index} syncedAt={syncedAt} />

      <MemoryToolbar search={search} onSearch={setSearch} onRefresh={refresh} inputRef={inputRef} />

      <CategoryPills index={index} loaded={data !== null} category={category} onSelect={setCategory} />

      <div className="results-telemetry-bar">
        <span aria-live="polite">{resultsLabel(visible.length, index.length)}</span>
        <span className="telemetry-hint">PRESS [ESC] TO CLEAR SEARCH</span>
      </div>

      {phase === 'loading' ? <FactsSkeleton /> : null}
      {phase === 'error' ? <MemoryError message={error ?? ''} onRetry={refresh} /> : null}
      {phase === 'empty' ? <NoMatches /> : null}
      {phase === 'ready' ? <FactList facts={visible} query={deferred.trim()} now={syncedAt} /> : null}
    </>
  );
}
