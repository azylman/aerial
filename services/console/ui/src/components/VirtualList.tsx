import { type ReactNode, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { useTabActive } from '../hooks/useTabActive';
import './VirtualList.css';

// Windowed list for long collections (the memory archive). Only the rows intersecting the
// viewport plus `overscan` rows on each side are mounted. Mounted rows sit in normal flow between
// two spacers (the list's top and bottom padding) that stand in for the unmounted rows, sized from
// row heights measured while they were mounted (cached by row key) or `estimateHeight` for rows
// never mounted. Because mounted rows are in flow, their size changes reflow in CSS with no JS
// round trip, and the browser's scroll anchoring keeps the visible rows still when a row above
// them mounts at a height different from its estimate.
//
// The list scrolls with the page: the window is the part of the
// list inside the browser viewport, so any scrolling ancestor (the document or a nested container;
// scroll listeners are captured) moves it.
//
// With `minItemWidth`, rows lay items out like `grid-template-columns: repeat(auto-fill,
// minmax(minItemWidth, 1fr))`: the column count follows the list width, and each row is one grid
// line whose height is its tallest item, exactly as the browser would lay out the full grid.

export interface VirtualListProps<T> {
  readonly items: readonly T[];
  readonly getKey: (item: T) => string;
  /**
   * Height (px) assumed for rows never mounted. Prefer the tall side of typical: measuring can then
   * only shrink the list, so a jump to the end (End key, scrollbar drag) settles on the true bottom.
   */
  readonly estimateHeight: number;
  /** Extra rows mounted above and below the viewport. */
  readonly overscan: number;
  readonly renderItem: (item: T) => ReactNode;
  /** Minimum column width (px) for a multi-column grid; omitted means one column. */
  readonly minItemWidth?: number;
  /** Gap (px) between rows and between columns. */
  readonly gap?: number;
  readonly className?: string;
  readonly ariaLabel: string;
}

/** auto-fill column count for `width`: as many `minItemWidth` columns as fit, at least one. */
export function columnCount(width: number, minItemWidth: number | undefined, gap: number): number {
  if (minItemWidth === undefined || minItemWidth <= 0) return 1;
  return Math.max(1, Math.floor((width + gap) / (minItemWidth + gap)));
}

/** Row keys for `keys` laid out `columns` per row: a row's key identifies its exact contents. */
export function rowKeys(keys: readonly string[], columns: number): readonly string[] {
  if (columns === 1) return keys;
  const rows: string[] = [];
  for (let i = 0; i < keys.length; i += columns) rows.push(keys.slice(i, i + columns).join('\u0000'));
  return rows;
}

export type HeightCache = ReadonlyMap<string, number>;

export const EMPTY_HEIGHTS: HeightCache = new Map();

/** `cache` with `measured` (row key, px) applied; the same map when nothing changed. */
export function withHeights(cache: HeightCache, measured: readonly (readonly [string, number])[]): HeightCache {
  const changed = measured.filter(([key, h]) => cache.get(key) !== h);
  if (changed.length === 0) return cache;
  const next = new Map(cache);
  for (const [key, h] of changed) next.set(key, h);
  return next;
}

export interface Layout {
  /** Top offset of each row. */
  readonly starts: readonly number[];
  readonly heights: readonly number[];
  readonly total: number;
}

/** Row offsets from cached heights, `estimate` standing in for rows never measured. */
export function computeLayout(keys: readonly string[], measured: HeightCache, estimate: number, gap: number): Layout {
  const starts: number[] = [];
  const heights: number[] = [];
  let y = 0;
  for (const key of keys) {
    const h = measured.get(key) ?? estimate;
    starts.push(y);
    heights.push(h);
    y += h + gap;
  }
  return { starts, heights, total: keys.length > 0 ? y - gap : 0 };
}

export interface RowRange {
  /** First mounted row (inclusive). */
  readonly start: number;
  /** Last mounted row (exclusive). */
  readonly end: number;
}

export const EMPTY_RANGE: RowRange = Object.freeze({ start: 0, end: 0 });

/**
 * Rows intersecting [top, bottom) (list-relative px), widened by `overscan` rows each side. A list
 * entirely below (or above) the viewport still mounts the `overscan` rows nearest to it, so the
 * first cards are ready as the list scrolls in.
 */
export function windowRange(layout: Layout, top: number, bottom: number, overscan: number): RowRange {
  const n = layout.starts.length;
  if (n === 0) return EMPTY_RANGE;
  // First row whose bottom edge is below `top`.
  let lo = 0;
  let hi = n;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    if ((layout.starts[mid] ?? 0) + (layout.heights[mid] ?? 0) <= top) lo = mid + 1;
    else hi = mid;
  }
  let end = lo;
  while (end < n && (layout.starts[end] ?? 0) < bottom) end++;
  return { start: Math.max(0, Math.min(lo, n - 1) - overscan), end: Math.min(n, Math.max(end, 1) + overscan) };
}

export interface Spacers {
  readonly top: number;
  readonly bottom: number;
}

/** Padding standing in for the rows before and after `range`. */
export function spacers(layout: Layout, range: RowRange): Spacers {
  if (range.end <= range.start) return { top: 0, bottom: layout.total };
  const last = range.end - 1;
  return {
    top: layout.starts[range.start] ?? 0,
    bottom: layout.total - (layout.starts[last] ?? 0) - (layout.heights[last] ?? 0),
  };
}

const sameRange = (a: RowRange, b: RowRange): boolean => a.start === b.start && a.end === b.end;

const borderBoxHeight = (entry: ResizeObserverEntry): number =>
  entry.borderBoxSize?.[0]?.blockSize ?? entry.target.getBoundingClientRect().height;

export function VirtualList<T>({
  items,
  getKey,
  estimateHeight,
  overscan,
  renderItem,
  minItemWidth,
  gap = 0,
  className,
  ariaLabel,
}: VirtualListProps<T>) {
  const listRef = useRef<HTMLDivElement>(null);
  const [columns, setColumns] = useState(1);
  const [heights, setHeights] = useState<HeightCache>(EMPTY_HEIGHTS);
  const [range, setRange] = useState<RowRange>(EMPTY_RANGE);
  const [focusedRow, setFocusedRow] = useState<string | null>(null);

  const keys = useMemo(() => items.map(getKey), [items, getKey]);
  const rows = useMemo(() => rowKeys(keys, columns), [keys, columns]);
  const layout = useMemo(() => computeLayout(rows, heights, estimateHeight, gap), [rows, heights, estimateHeight, gap]);

  // The latest values for the (stable) DOM listeners below, refreshed on every commit.
  const live = useRef({ layout, overscan, minItemWidth, gap, columns });
  useLayoutEffect(() => {
    live.current = { layout, overscan, minItemWidth, gap, columns };
  });

  /** Recomputes the column count and window from the list's current geometry. */
  const sync = useCallback((): void => {
    const el = listRef.current;
    if (el === null) return;
    const rect = el.getBoundingClientRect();
    const cur = live.current;
    const cols = columnCount(rect.width, cur.minItemWidth, cur.gap);
    if (cols !== cur.columns) {
      // Heights at another column count describe other rows at another width.
      setHeights(EMPTY_HEIGHTS);
      setColumns(cols);
    }
    const next = windowRange(cur.layout, -rect.top, window.innerHeight - rect.top, cur.overscan);
    setRange((prev) => (sameRange(prev, next) ? prev : next));
  }, []);

  // One ResizeObserver for the list (width -> columns) and every mounted row (height cache).
  const observer = useRef<ResizeObserver | null>(null);
  if (observer.current === null && typeof ResizeObserver !== 'undefined') {
    observer.current = new ResizeObserver((entries) => {
      const measured: [string, number][] = [];
      let resized = false;
      for (const entry of entries) {
        const key = (entry.target as HTMLElement).dataset['rowKey'];
        if (key === undefined) resized = true;
        else measured.push([key, borderBoxHeight(entry)]);
      }
      if (measured.length > 0) setHeights((prev) => withHeights(prev, measured));
      if (resized) sync();
    });
  }

  // A hidden tab panel keeps the list mounted (content-visibility: hidden); its geometry is
  // meaningless there, so it listens only while its tab is shown and re-windows on reveal.
  const tabActive = useTabActive();
  useEffect(() => {
    if (!tabActive) return undefined;
    const ro = observer.current;
    const el = listRef.current;
    if (ro !== null && el !== null) ro.observe(el);
    window.addEventListener('scroll', sync, { passive: true, capture: true });
    window.addEventListener('resize', sync, { passive: true });
    return () => {
      window.removeEventListener('scroll', sync, { capture: true });
      window.removeEventListener('resize', sync);
      if (ro !== null && el !== null) ro.unobserve(el);
    };
  }, [sync, tabActive]);

  // A new layout (items, columns or measurements changed) moves rows, and a revealed tab moves
  // the list back into flow: re-window before paint.
  useLayoutEffect(() => {
    if (tabActive) sync();
  }, [layout, sync, tabActive]);

  const observeRow = useCallback((el: HTMLDivElement | null) => {
    const ro = observer.current;
    if (el === null || ro === null) return undefined;
    ro.observe(el);
    return () => ro.unobserve(el);
  }, []);

  const end = Math.min(range.end, rows.length);
  const start = Math.min(range.start, end);
  const pad = spacers(layout, { start, end });
  const template = minItemWidth === undefined ? undefined : `repeat(${columns}, minmax(${minItemWidth}px, 1fr))`;

  const renderRow = (r: number, detached: boolean): ReactNode => {
    const key = rows[r] as string;
    const first = r * columns;
    return (
      <div
        key={key}
        ref={observeRow}
        role="presentation"
        className={detached ? 'virtual-list-row detached' : 'virtual-list-row'}
        data-row-key={key}
        style={{ gridTemplateColumns: template, columnGap: gap, top: detached ? layout.starts[r] : undefined }}
      >
        {items.slice(first, first + columns).map((item, i) => (
          <div
            key={keys[first + i]}
            role="listitem"
            className="virtual-list-cell"
            aria-setsize={items.length}
            aria-posinset={first + i + 1}
          >
            {renderItem(item)}
          </div>
        ))}
      </div>
    );
  };

  // The row holding keyboard focus stays mounted (out of flow, at its offset) while scrolled out
  // of the window, so focus is never dropped to <body>.
  const focusedIndex = focusedRow === null ? -1 : rows.indexOf(focusedRow);
  const detached = focusedIndex >= 0 && (focusedIndex < start || focusedIndex >= end) ? focusedIndex : -1;
  const mounted: ReactNode[] = [];
  for (let r = start; r < end; r++) mounted.push(renderRow(r, false));
  if (detached >= 0) mounted.push(renderRow(detached, true));

  return (
    <div
      ref={listRef}
      role="list"
      aria-label={ariaLabel}
      className={className === undefined ? 'virtual-list' : `virtual-list ${className}`}
      style={{ paddingTop: pad.top, paddingBottom: pad.bottom, rowGap: gap }}
      onFocus={(e) => {
        const row = (e.target as HTMLElement).closest<HTMLElement>('[data-row-key]');
        setFocusedRow(row?.dataset['rowKey'] ?? null);
      }}
      onBlur={(e) => {
        if (!e.currentTarget.contains(e.relatedTarget as Node | null)) setFocusedRow(null);
      }}
    >
      {mounted}
    </div>
  );
}
