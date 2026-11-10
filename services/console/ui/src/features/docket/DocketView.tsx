import { memo, type RefObject, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { DocketScoredTaskDTO, DocketStatusFilter } from '../../api/types';
import { Icon } from '../../components/Icon';
import { useSearchShortcutTarget } from '../../hooks/useKeyboardShortcuts';
import { usePollingSync } from '../../hooks/usePollingSync';
import { useTabActive } from '../../hooks/useTabActive';
import { type ConsoleState, useConsoleStore } from '../../store/store';
import '../../styles/controls.css';
import { syncDocketSearch, syncDocketTasks } from './data';
import { DEFAULT_STATUS, matchesStatus, normalizeQuery, resultsLabel, STATUS_PILLS, visibleTasks } from './model';
import { type CardTask, TaskCard } from './TaskCard';
import './docket.css';

// The drawer body ships in this chunk; App lazy-loads it from here.
export { TaskDetailPanel } from './TaskDetail';

/** The list re-fetches every 30s while the tab is shown. */
export const DOCKET_POLL_MS = 30000;

/** Typing pauses this long before a search request goes out. */
export const SEARCH_DEBOUNCE_MS = 300;

const EMPTY_HITS: readonly DocketScoredTaskDTO[] = Object.freeze([]);

const selectList = (s: ConsoleState) => s.docket.list;
const selectSearch = (s: ConsoleState) => s.docket.search;

/** `value` once it has held still for SEARCH_DEBOUNCE_MS; clearing it takes effect at once. */
function useDebouncedQuery(value: string): string {
  const [settled, setSettled] = useState(value);
  useEffect(() => {
    const id = window.setTimeout(() => setSettled(value), SEARCH_DEBOUNCE_MS);
    return () => window.clearTimeout(id);
  }, [value]);
  return value === '' ? '' : settled;
}

interface ToolbarProps {
  readonly search: string;
  readonly onSearch: (value: string) => void;
  readonly inputRef: RefObject<HTMLInputElement | null>;
}

const DocketToolbar = memo(function DocketToolbar({ search, onSearch, inputRef }: ToolbarProps) {
  return (
    <section className="docket-toolbar">
      <div className="toolbar-search-wrap">
        <span className="search-icon">
          <Icon name="search" />
        </span>
        <input
          ref={inputRef}
          type="search"
          id="docket-search-input"
          className="cyber-input"
          placeholder="SEARCH TASKS (PRESS '/' TO FOCUS)..."
          aria-label="Search tasks"
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
    </section>
  );
});

const StatusPills = memo(function StatusPills({
  filter,
  onSelect,
}: {
  readonly filter: DocketStatusFilter;
  readonly onSelect: (filter: DocketStatusFilter) => void;
}) {
  return (
    <div className="category-pills-bar docket-pills" role="toolbar" aria-label="Task status filter">
      {STATUS_PILLS.map((pill) => (
        <button
          key={pill.filter}
          type="button"
          className={pill.filter === filter ? 'cat-pill active' : 'cat-pill'}
          aria-pressed={pill.filter === filter}
          onClick={() => onSelect(pill.filter)}
        >
          {pill.label}
        </button>
      ))}
    </div>
  );
});

/** Cards, a loading row before the first payload, or the empty / error row. */
function TaskCards({
  tasks,
  loaded,
  error,
  searching,
}: {
  readonly tasks: readonly CardTask[];
  readonly loaded: boolean;
  readonly error: string | null;
  readonly searching: boolean;
}) {
  let body;
  if (!loaded && error === null) body = <div className="docket-empty">LOADING TASKS...</div>;
  else if (!loaded) body = <div className="docket-empty docket-error">DOCKET UNAVAILABLE // {error}</div>;
  else if (tasks.length === 0) body = <div className="docket-empty">{searching ? 'NO MATCHING TASKS' : 'NO TASKS'}</div>;
  else body = tasks.map((t) => <TaskCard key={t.id} task={t} />);
  return (
    <>
      <div className="results-telemetry-bar">
        <span id="docket-results-count">{resultsLabel(tasks.length, searching)}</span>
        {loaded && error !== null ? <span className="docket-error">STALE // {error}</span> : null}
      </div>
      <div className="docket-list" aria-live="polite">
        {body}
      </div>
    </>
  );
}

/** One pill's task list, polled while its tab and pill are shown; keyed by pill so a switch fetches at once. */
function TaskList({ filter }: { readonly filter: DocketStatusFilter }) {
  const poll = useMemo(() => syncDocketTasks(filter), [filter]);
  usePollingSync(poll, DOCKET_POLL_MS, useTabActive());
  const { data, error } = useConsoleStore(selectList);
  const tasks = useMemo(() => visibleTasks(data, filter), [data, filter]);
  return <TaskCards tasks={tasks} loaded={data !== null} error={error} searching={false} />;
}

/** Search hits (score order) for one query and pill; keyed by both so each runs once. */
function SearchResults({ query, filter }: { readonly query: string; readonly filter: DocketStatusFilter }) {
  useEffect(() => {
    const controller = new AbortController();
    void syncDocketSearch(query, filter)(controller.signal);
    return () => controller.abort();
  }, [query, filter]);
  const { data, error } = useConsoleStore(selectSearch);
  const hits = data?.results ?? EMPTY_HITS;
  const tasks = useMemo(() => hits.filter((h) => matchesStatus(h.status, filter)), [hits, filter]);
  return <TaskCards tasks={tasks} loaded={data !== null} error={error} searching />;
}

/**
 * The read-only Docket tab: status pills, a debounced semantic search box, and the task list (or
 * the search hits while a query is set). Cards open the task drawer.
 */
export default function DocketView() {
  const [filter, setFilter] = useState<DocketStatusFilter>(DEFAULT_STATUS);
  const [search, setSearch] = useState('');
  const query = useDebouncedQuery(normalizeQuery(search));
  const inputRef = useRef<HTMLInputElement>(null);
  const clearSearch = useCallback(() => setSearch(''), []);
  useSearchShortcutTarget('docket', inputRef, clearSearch);

  return (
    <>
      <DocketToolbar search={search} onSearch={setSearch} inputRef={inputRef} />
      <StatusPills filter={filter} onSelect={setFilter} />
      <section className="docket-section">
        {query === '' ? (
          <TaskList key={filter} filter={filter} />
        ) : (
          <SearchResults key={`${filter}\u0000${query}`} query={query} filter={filter} />
        )}
      </section>
    </>
  );
}
