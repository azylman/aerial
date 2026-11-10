import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  columnCount,
  computeLayout,
  EMPTY_HEIGHTS,
  rowKeys,
  spacers,
  VirtualList,
  windowRange,
  withHeights,
} from './VirtualList';
import { TabActiveContext } from '../hooks/useTabActive';

// One fake ResizeObserver per list: tests deliver entries through `resize`.
class FakeResizeObserver {
  static last: FakeResizeObserver | null = null;
  readonly observed = new Set<Element>();
  constructor(private readonly callback: ResizeObserverCallback) {
    FakeResizeObserver.last = this;
  }
  observe(el: Element): void {
    this.observed.add(el);
  }
  unobserve(el: Element): void {
    this.observed.delete(el);
  }
  disconnect(): void {
    this.observed.clear();
  }
  resize(targets: readonly (readonly [Element, number])[]): void {
    const entries = targets.map(([target, h]) => ({ target, borderBoxSize: [{ blockSize: h, inlineSize: 0 }] }));
    this.callback(entries as unknown as ResizeObserverEntry[], this as unknown as ResizeObserver);
  }
}

// Geometry of the list element: its top relative to the viewport and its width.
const geometry = { top: 0, width: 400 };

beforeEach(() => {
  geometry.top = 0;
  geometry.width = 400;
  vi.stubGlobal('ResizeObserver', FakeResizeObserver);
  vi.spyOn(HTMLElement.prototype, 'getBoundingClientRect').mockImplementation(function (this: HTMLElement) {
    const isList = this.getAttribute('role') === 'list';
    return { top: isList ? geometry.top : 0, width: isList ? geometry.width : 0, height: 0 } as DOMRect;
  });
  Object.defineProperty(window, 'innerHeight', { configurable: true, value: 768 });
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

const ITEMS: readonly string[] = Array.from({ length: 150 }, (_, i) => `item-${i}`);
const ident = (s: string): string => s;
const renderItem = (s: string) => (
  <span>
    {s} <button type="button">copy {s}</button>
  </span>
);

function renderList(props: { minItemWidth?: number; gap?: number } = {}) {
  return render(
    <VirtualList
      items={ITEMS}
      getKey={ident}
      estimateHeight={100}
      overscan={3}
      renderItem={renderItem}
      ariaLabel="Facts"
      {...props}
    />,
  );
}

const positions = (): number[] =>
  screen.getAllByRole('listitem').map((el) => Number(el.getAttribute('aria-posinset')));
const range = (from: number, to: number): number[] => Array.from({ length: to - from + 1 }, (_, i) => from + i);

describe('VirtualList window', () => {
  it.each([
    { name: 'at the top: 8 visible rows + 3 overscan', top: 0, want: range(1, 11) },
    { name: 'scrolled to the middle', top: -5000, want: range(48, 61) },
    { name: 'scrolled past the end', top: -20000, want: range(147, 150) },
    { name: 'list below the viewport mounts the first overscan rows', top: 2000, want: range(1, 4) },
  ])('$name', ({ top, want }) => {
    renderList();
    act(() => {
      geometry.top = top;
      window.dispatchEvent(new Event('scroll'));
    });
    expect(positions()).toEqual(want);
    expect(screen.getAllByRole('listitem')[0]?.getAttribute('aria-setsize')).toBe('150');
  });

  it.each([
    { name: 'a shown tab follows the scroll', tabs: [true], want: range(48, 61) },
    { name: 'a hidden tab ignores the scroll', tabs: [false], want: [] },
    { name: 'revealing the tab re-windows at the current position', tabs: [false, true], want: range(48, 61) },
  ])('$name', ({ tabs, want }) => {
    const inTab = (active: boolean | undefined) => (
      <TabActiveContext value={active ?? true}>
        <VirtualList items={ITEMS} getKey={ident} estimateHeight={100} overscan={3} renderItem={renderItem} ariaLabel="Facts" />
      </TabActiveContext>
    );
    const { rerender } = render(inTab(tabs[0]));
    act(() => {
      geometry.top = -5000;
      window.dispatchEvent(new Event('scroll'));
    });
    for (const active of tabs.slice(1)) rerender(inTab(active));
    expect(screen.queryAllByRole('listitem').map((el) => Number(el.getAttribute('aria-posinset')))).toEqual(want);
  });

  it('pads for the unmounted rows from estimates and measured heights', () => {
    renderList();
    const list = screen.getByRole('list', { name: 'Facts' });
    expect(list.style.paddingTop).toBe('0px');
    expect(list.style.paddingBottom).toBe('13900px');
    const firstRow = list.querySelector('[data-row-key="item-0"]') as Element;
    act(() => FakeResizeObserver.last?.resize([[firstRow, 250]]));
    act(() => {
      geometry.top = -6000;
      window.dispatchEvent(new Event('scroll'));
    });
    // Row 58 is the first visible; with 3 rows of overscan rows 55+ mount, padded by row 0
    // (250px) and rows 1-54 (100px each).
    expect(positions()[0]).toBe(56);
    expect(list.style.paddingTop).toBe('5650px');
  });

  it('lays rows out as an auto-fill grid that follows the list width', () => {
    geometry.width = 1100;
    renderList({ minItemWidth: 340, gap: 20 });
    const row = screen.getByRole('list').querySelector('.virtual-list-row') as HTMLElement;
    expect(row.style.gridTemplateColumns).toBe('repeat(3, minmax(340px, 1fr))');
    expect(row.children).toHaveLength(3);
    act(() => {
      geometry.width = 500;
      FakeResizeObserver.last?.resize([[screen.getByRole('list'), 0]]);
    });
    expect((screen.getByRole('list').querySelector('.virtual-list-row') as HTMLElement).style.gridTemplateColumns).toBe(
      'repeat(1, minmax(340px, 1fr))',
    );
  });

  it('keeps the focused row mounted after it scrolls out of the window', () => {
    renderList();
    const button = screen.getByRole('button', { name: 'copy item-2' });
    act(() => button.focus());
    act(() => {
      geometry.top = -9000;
      window.dispatchEvent(new Event('scroll'));
    });
    expect(screen.getByRole('button', { name: 'copy item-2' })).toBe(document.activeElement);
    expect(button.closest('.virtual-list-row')?.className).toBe('virtual-list-row detached');
    act(() => {
      fireEvent.blur(button, { relatedTarget: null });
    });
    expect(screen.queryByRole('button', { name: 'copy item-2' })).toBeNull();
  });

  it('stops observing rows that unmount', () => {
    const view = renderList();
    const observed = FakeResizeObserver.last?.observed;
    expect(observed?.size).toBe(12); // the list + 11 rows
    view.unmount();
    expect(observed?.size).toBe(0);
  });
});

describe('columnCount', () => {
  it.each([
    { width: 1100, min: 340, gap: 20, want: 3 },
    { width: 1059, min: 340, gap: 20, want: 2 },
    { width: 300, min: 340, gap: 20, want: 1 },
    { width: 1100, min: undefined, gap: 20, want: 1 },
    { width: 1100, min: 0, gap: 0, want: 1 },
  ])('$width / $min', ({ width, min, gap, want }) => {
    expect(columnCount(width, min, gap)).toBe(want);
  });
});

describe('rowKeys', () => {
  it.each([
    { columns: 1, want: ['a', 'b', 'c', 'd', 'e'] },
    { columns: 2, want: ['a\u0000b', 'c\u0000d', 'e'] },
  ])('$columns columns', ({ columns, want }) => {
    expect(rowKeys(['a', 'b', 'c', 'd', 'e'], columns)).toEqual(want);
  });
});

describe('withHeights', () => {
  const base = new Map([['a', 10]]);
  it.each([
    { name: 'unchanged keeps the map', measured: [['a', 10]] as const, same: true, want: { a: 10 } },
    { name: 'new height copies', measured: [['a', 12], ['b', 5]] as const, same: false, want: { a: 12, b: 5 } },
  ])('$name', ({ measured, same, want }) => {
    const next = withHeights(base, measured);
    expect(next === base).toBe(same);
    expect(Object.fromEntries(next)).toEqual(want);
  });
});

describe('layout math', () => {
  const layout = computeLayout(['a', 'b', 'c', 'd'], new Map([['b', 50]]), 100, 10);

  it('computes offsets from measured and estimated heights', () => {
    expect(layout).toEqual({ starts: [0, 110, 170, 280], heights: [100, 50, 100, 100], total: 380 });
    expect(computeLayout([], EMPTY_HEIGHTS, 100, 10).total).toBe(0);
  });

  it.each([
    { name: 'top', top: 0, bottom: 150, overscan: 0, want: { start: 0, end: 2 } },
    { name: 'middle with overscan', top: 165, bottom: 200, overscan: 1, want: { start: 1, end: 4 } },
    { name: 'gap between rows', top: 105, bottom: 108, overscan: 0, want: { start: 1, end: 1 } },
    { name: 'empty list', top: 0, bottom: 100, overscan: 2, want: { start: 0, end: 0 }, empty: true },
  ])('windowRange $name', ({ top, bottom, overscan, want, empty }) => {
    const l = empty === true ? computeLayout([], EMPTY_HEIGHTS, 100, 10) : layout;
    expect(windowRange(l, top, bottom, overscan)).toEqual(want);
  });

  it.each([
    { name: 'middle rows', range: { start: 1, end: 3 }, want: { top: 110, bottom: 110 } },
    { name: 'nothing mounted', range: { start: 0, end: 0 }, want: { top: 0, bottom: 380 } },
  ])('spacers $name', ({ range: r, want }) => {
    expect(spacers(layout, r)).toEqual(want);
  });
});
