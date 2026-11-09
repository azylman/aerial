import { memo, type RefObject, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { CronSchedule, OneShotSchedule, ScheduleRun, ScheduleSummaryMetrics } from '../../api/types';
import { Glyph } from '../../components/Glyph';
import { Icon } from '../../components/Icon';
import { useFirstPassLimit } from '../../hooks/useFirstPassLimit';
import { useSearchShortcutTarget } from '../../hooks/useKeyboardShortcuts';
import { usePollingSync } from '../../hooks/usePollingSync';
import { HighlightQuery } from '../../lib/search';
import { type ConsoleState, useConsoleStore } from '../../store/store';
import '../../styles/controls.css';
import '../../styles/sections.css';
import { NextTriggerValue } from './Countdown';
import { syncScheduleRuns, syncSchedules } from './data';
import {
  earliestTrigger,
  filterCrons,
  filterOneShots,
  filterRuns,
  normalizeQuery,
  resultsLabel,
  runsLoggedLabel,
  SCHEDULE_FILTERS,
  type ScheduleFilter,
  showsGrid,
  showsRuns,
  successRate,
} from './model';
import { RunCard } from './RunsFeed';
import { CronCard, OneShotCard } from './ScheduleCards';
import './schedules.css';

/** Same cadence as the vanilla schedules tab (schedulesPollInterval). */
export const SCHEDULES_POLL_MS = 10_000;

const EMPTY_CRONS: readonly CronSchedule[] = Object.freeze([]);
const EMPTY_ONE_SHOTS: readonly OneShotSchedule[] = Object.freeze([]);
const EMPTY_RUNS: readonly ScheduleRun[] = Object.freeze([]);

const selectCrons = (s: ConsoleState): readonly CronSchedule[] => s.schedules.list.data?.crons ?? EMPTY_CRONS;
const selectOneShots = (s: ConsoleState): readonly OneShotSchedule[] =>
  s.schedules.list.data?.one_shots ?? EMPTY_ONE_SHOTS;
const selectSummary = (s: ConsoleState): ScheduleSummaryMetrics | null => s.schedules.list.data?.summary ?? null;
const selectRuns = (s: ConsoleState): readonly ScheduleRun[] => s.schedules.runs.data?.runs ?? EMPTY_RUNS;
const selectRunsLoaded = (s: ConsoleState): boolean => s.schedules.runs.data !== null;
const selectListError = (s: ConsoleState): string | null => s.schedules.list.error;
const selectScheduleCount = (s: ConsoleState): number => selectCrons(s).length + selectOneShots(s).length;
const selectRunsPillCount = (s: ConsoleState): number => s.schedules.runs.data?.total || selectRuns(s).length;

/**
 * Lifecycle of the schedules list (fetchSchedules): skeleton until the first answer, the error
 * box when a poll fails with nothing cached, and the reconnect banner over a stale cached list.
 */
export type ListPhase = 'loading' | 'ready' | 'stale' | 'error';

export function listPhase(s: ConsoleState): ListPhase {
  const { data, error } = s.schedules.list;
  if (error === null) return data === null ? 'loading' : 'ready';
  return selectScheduleCount(s) > 0 ? 'stale' : 'error';
}

/** Polls both endpoints now (SYNC and RETRY buttons); a newer click or unmount cancels the last. */
function useManualSync(): () => void {
  const inflight = useRef<AbortController | null>(null);
  useEffect(() => () => inflight.current?.abort(), []);
  return useCallback(() => {
    inflight.current?.abort();
    const controller = new AbortController();
    inflight.current = controller;
    void syncSchedules(controller.signal);
    void syncScheduleRuns(controller.signal);
  }, []);
}

/** The four summary cards; subscribes to the summary and the schedule lists only. */
const ScheduleMetrics = memo(function ScheduleMetrics() {
  const summary = useConsoleStore(selectSummary);
  const crons = useConsoleStore(selectCrons);
  const oneShots = useConsoleStore(selectOneShots);
  const next = useMemo(() => earliestTrigger(crons, oneShots), [crons, oneShots]);
  const rate = successRate(summary);
  return (
    <section className="summary-bar">
      <div className="summary-card">
        <div className="card-glow" />
        <span className="label">ACTIVE CRONS</span>{' '}
        <span className="value" id="schedules-crons-count">
          {summary === null ? '--' : (summary.cron_count ?? crons.length)}
        </span>{' '}
        <span className="sub-label">RECURRING ROUTINES</span>
      </div>
      <div className="summary-card">
        <div className="card-glow" />
        <span className="label">PENDING ONCE</span>{' '}
        <span className="value" id="schedules-oneshot-count">
          {summary === null ? '--' : (summary.one_shot_count ?? oneShots.length)}
        </span>{' '}
        <span className="sub-label">ONE-SHOT REMINDERS</span>
      </div>
      <div className="summary-card">
        <div className="card-glow" />
        <span className="label">NEXT TRIGGER</span> <NextTriggerValue target={next} />{' '}
        <span className="sub-label">UPCOMING RUN</span>
      </div>
      <div className="summary-card">
        <div className="card-glow" />
        <span className="label">24H SUCCESS RATE</span>{' '}
        <span className={rate.className} id="schedules-success-rate">
          {rate.text}
        </span>{' '}
        <span className="sub-label">{runsLoggedLabel(summary)}</span>
      </div>
    </section>
  );
});

interface PillsProps {
  readonly filter: ScheduleFilter;
  readonly onSelect: (filter: ScheduleFilter) => void;
}

/** Category pills with their counts (renderSchedulePills). */
const SchedulePills = memo(function SchedulePills({ filter, onSelect }: PillsProps) {
  const crons = useConsoleStore((s) => selectCrons(s).length);
  const oneShots = useConsoleStore((s) => selectOneShots(s).length);
  const runs = useConsoleStore(selectRunsPillCount);
  const counts: Readonly<Record<ScheduleFilter, number>> = {
    ALL: crons + oneShots,
    CRON: crons,
    ONE_SHOT: oneShots,
    RUNS: runs,
  };
  return (
    <div className="category-pills-bar" role="toolbar" aria-label="Schedule Category Filters">
      {SCHEDULE_FILTERS.map((pill) => (
        <button
          key={pill.id}
          type="button"
          className={pill.id === filter ? 'cat-pill active' : 'cat-pill'}
          aria-pressed={pill.id === filter}
          onClick={() => onSelect(pill.id)}
        >
          {pill.label} <span className="pill-count">{counts[pill.id]}</span>
        </button>
      ))}
    </div>
  );
});

const ActiveBadge = memo(function ActiveBadge() {
  const total = useConsoleStore((s) => selectSummary(s)?.total_active ?? 0);
  return <span className="section-badge">{total} ACTIVE</span>;
});

function SchedulesSkeleton() {
  return [0, 1, 2, 3].map((i) => (
    <div key={i} className="fact-card-skeleton">
      <div className="skeleton-box skeleton-title" />
      <div className="skeleton-box skeleton-body" />
      <div className="skeleton-box skeleton-meta" />
    </div>
  ));
}

function SchedulesError({ message, onRetry }: { readonly message: string; readonly onRetry: () => void }) {
  return (
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
  );
}

function EmptyState({
  icon,
  title,
  hint,
}: {
  readonly icon: 'timer' | 'clipboard';
  readonly title: string;
  readonly hint: string;
}) {
  return (
    <div className="empty-state-box">
      <div className="empty-state-glyph">
        <Glyph name={icon} />
      </div>
      <div className="empty-state-title">{title}</div>
      <div className="empty-state-hint">{hint}</div>
    </div>
  );
}

interface GridProps {
  readonly phase: ListPhase;
  readonly crons: readonly CronSchedule[];
  readonly oneShots: readonly OneShotSchedule[];
  readonly onRetry: () => void;
}

/** About one screen of schedule cards, counted across crons then one-shots; the rest follow the first paint. */
const FIRST_PASS_SCHEDULES = 12;
/** About one screen of run cards; the rest follow the first paint. */
const FIRST_PASS_RUNS = 10;

const ScheduleGrid = memo(function ScheduleGrid({ phase, crons, oneShots, onRetry }: GridProps) {
  const error = useConsoleStore(selectListError);
  const listed = phase === 'loading' || phase === 'error' ? 0 : crons.length + oneShots.length;
  const shown = useFirstPassLimit(listed, FIRST_PASS_SCHEDULES);
  const shownCrons = Math.min(crons.length, shown);
  let body;
  if (phase === 'loading') body = <SchedulesSkeleton />;
  else if (phase === 'error') body = <SchedulesError message={error ?? ''} onRetry={onRetry} />;
  else if (crons.length + oneShots.length === 0) {
    body = (
      <EmptyState
        icon="timer"
        title="NO ACTIVE SCHEDULED ROUTINES MATCH CRITERIA"
        hint="Try adjusting your keyword filter or switching category pills."
      />
    );
  } else {
    body = (
      <>
        {crons.slice(0, shownCrons).map((cron) => (
          <CronCard key={cron.id} cron={cron} />
        ))}
        {oneShots.slice(0, shown - shownCrons).map((oneShot) => (
          <OneShotCard key={oneShot.id} oneShot={oneShot} />
        ))}
      </>
    );
  }
  return (
    <div className="schedules-grid" id="schedules-grid" aria-live="polite">
      {body}
    </div>
  );
});

const RunsList = memo(function RunsList({ runs }: { readonly runs: readonly ScheduleRun[] }) {
  const loaded = useConsoleStore(selectRunsLoaded);
  const shown = useFirstPassLimit(loaded ? runs.length : 0, FIRST_PASS_RUNS);
  return (
    <div className="runs-feed-container" id="runs-feed-container" aria-live="polite">
      {!loaded ? null : runs.length === 0 ? (
        <EmptyState
          icon="clipboard"
          title="NO RECENT EXECUTION RUNS"
          hint="Schedule execution history and outcome logs will appear here."
        />
      ) : (
        runs.slice(0, shown).map((run) => <RunCard key={run.id} run={run} />)
      )}
    </div>
  );
});

interface ToolbarProps {
  readonly search: string;
  readonly onSearch: (value: string) => void;
  readonly onRefresh: () => void;
  readonly inputRef: RefObject<HTMLInputElement | null>;
}

/** Search box, clear and sync buttons; memoized so polls never re-render it. */
const SearchToolbar = memo(function SearchToolbar({ search, onSearch, onRefresh, inputRef }: ToolbarProps) {
  return (
    <section className="schedules-toolbar-card">
      <div className="toolbar-search-wrap">
        <span className="search-icon">
          <Icon name="search" />
        </span>
        <input
          ref={inputRef}
          type="text"
          id="schedules-search-input"
          className="cyber-input"
          placeholder="SEARCH SCHEDULES, PROMPTS, TARGET IDs (PRESS '/' TO FOCUS)..."
          aria-label="Search schedules"
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
        <button type="button" className="cyber-btn-secondary" title="Sync schedules from brain" onClick={onRefresh}>
          <span className="refresh-icon">
            <Icon name="refresh" />
          </span>{' '}
          SYNC SCHEDULES
        </button>
      </div>
    </section>
  );
});

/** Static section heading; memoized so a re-render of the view leaves it alone. */
const SectionTitle = memo(function SectionTitle({
  icon,
  title,
}: {
  readonly icon: 'alarm' | 'clipboard';
  readonly title: string;
}) {
  return (
    <div className="section-title">
      <span className="title-icon">
        <Icon name={icon} />
      </span>
      <h2>{title}</h2>
    </div>
  );
});

/**
 * Port of #schedules-view. Pill and search are local state and a keystroke filters and paints in
 * the same frame as the input. That stays cheap because cards are memoized per schedule (keyed by
 * id, so a card that stays visible keeps its DOM) and take no query prop: the query reaches only
 * their highlight leaves through HighlightQuery, and matching runs against cached haystacks. A
 * deferred query would cost a second render and frame per keystroke, which measured slower than
 * the render it defers. Countdowns are 1s-clock leaves.
 */
export default function SchedulesView() {
  usePollingSync(syncSchedules, SCHEDULES_POLL_MS, true);
  usePollingSync(syncScheduleRuns, SCHEDULES_POLL_MS, true);
  const refresh = useManualSync();

  const [filter, setFilter] = useState<ScheduleFilter>('ALL');
  const [search, setSearch] = useState('');
  const query = normalizeQuery(search);
  const inputRef = useRef<HTMLInputElement>(null);
  const clearSearch = useCallback(() => setSearch(''), []);
  useSearchShortcutTarget('schedules', inputRef, clearSearch);

  const phase = useConsoleStore(listPhase);
  const crons = useConsoleStore(selectCrons);
  const oneShots = useConsoleStore(selectOneShots);
  const runs = useConsoleStore(selectRuns);
  const visibleCrons = useMemo(() => filterCrons(crons, filter, query), [crons, filter, query]);
  const visibleOneShots = useMemo(() => filterOneShots(oneShots, filter, query), [oneShots, filter, query]);
  const visibleRuns = useMemo(() => filterRuns(runs, query), [runs, query]);

  return (
    <HighlightQuery value={query}>
      {phase === 'stale' ? (
        <div className="reconnect-banner">
          <span className="reconnect-icon">
            <Icon name="bolt" />
          </span>{' '}
          <span className="reconnect-text">RECONNECTING TO BRAIN // STALE-WHILE-REVALIDATE ACTIVE</span>
        </div>
      ) : null}

      <ScheduleMetrics />

      <SearchToolbar search={search} onSearch={setSearch} onRefresh={refresh} inputRef={inputRef} />

      <SchedulePills filter={filter} onSelect={setFilter} />

      <div className="results-telemetry-bar">
        <span>
          {resultsLabel(
            phase === 'ready' || phase === 'stale',
            visibleCrons.length + visibleOneShots.length,
            crons.length + oneShots.length,
          )}
        </span>
        <span className="telemetry-hint">PRESS [ESC] TO CLEAR SEARCH</span>
      </div>

      {showsGrid(filter) ? (
        <section className="schedules-grid-section">
          <div className="section-title-bar">
            <SectionTitle icon="alarm" title="ACTIVE SCHEDULED ROUTINES" />
            <ActiveBadge />
          </div>
          <ScheduleGrid phase={phase} crons={visibleCrons} oneShots={visibleOneShots} onRetry={refresh} />
        </section>
      ) : null}

      {showsRuns(filter) ? (
        <section className="runs-feed-section">
          <div className="section-title-bar">
            <SectionTitle icon="clipboard" title="EXECUTION RUN AUDIT LOG" />
            <span className="section-badge">{visibleRuns.length} RUNS</span>
          </div>
          <RunsList runs={visibleRuns} />
        </section>
      ) : null}
    </HighlightQuery>
  );
}
