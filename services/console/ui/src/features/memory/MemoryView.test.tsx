import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { Profiler, type ProfilerOnRenderCallback } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { FactItem, FactsAPIResponse } from '../../api/types';
import { useKeyboardShortcuts } from '../../hooks/useKeyboardShortcuts';
import { useConsoleStore } from '../../store/store';
import MemoryView from './MemoryView';

const NOW = Date.parse('2026-11-08T12:00:00Z');

const fact = (id: number, text: string, count: number, last?: string): FactItem => ({
  id,
  fact_text: text,
  reinforce_count: count,
  created_at: '2026-11-06T12:00:00Z',
  ...(last === undefined ? {} : { last_reinforced_at: last }),
});

// Importance order: 12, 7, 9, 30, 4.
const FACTS: readonly FactItem[] = [
  fact(4, 'Avoids cumin in recipes', 1),
  fact(9, 'Prefers oat milk in lattes', 3, '2026-11-08T09:00:00Z'),
  fact(12, 'Builds ship through Vanadis', 5, '2026-11-08T11:59:30Z'),
  fact(30, 'Oat milk is the default', 1),
  fact(7, 'Uses dark mode everywhere', 3, '2026-11-08T11:00:00Z'),
];
const payload = (facts: readonly FactItem[] = FACTS): FactsAPIResponse => ({
  facts: facts.map((f) => ({ ...f })),
  total: facts.length,
  limit: 500,
  offset: 0,
  status: 'ok',
});

const fetchMock = vi.fn<(url: string) => Promise<Response>>();
const pending = (): Promise<Response> => new Promise(() => undefined);
const json = (body: unknown, status = 200): Promise<Response> =>
  Promise.resolve(new Response(JSON.stringify(body), { status }));
const writeText = vi.fn<(text: string) => Promise<void>>();

beforeEach(() => {
  useConsoleStore.setState(useConsoleStore.getInitialState(), true);
  fetchMock.mockReset();
  fetchMock.mockImplementation(pending);
  vi.stubGlobal('fetch', fetchMock);
  writeText.mockReset();
  writeText.mockResolvedValue(undefined);
  Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText } });
  vi.useFakeTimers({ now: NOW, toFake: ['Date'] });
});
afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

const seed = (data: FactsAPIResponse): void => act(() => useConsoleStore.getState().setFacts(data));
const nodes = (): string[] => [...document.querySelectorAll('.fact-category-tag')].map((el) => el.textContent ?? '');
const text = (selector: string): string => (document.querySelector(selector)?.textContent ?? '').replace(/\s+/g, ' ').trim();
const search = (): HTMLInputElement => screen.getByRole('textbox', { name: 'Search memory facts' });

function Shortcuts() {
  useKeyboardShortcuts('memory');
  return null;
}

describe('MemoryView lifecycle', () => {
  it.each([
    { name: 'loading', data: null, error: null, wantSkeletons: 6, wantAlert: '', wantStatus: 'OPTIMAL', wantTotal: '--', wantPills: 1 },
    { name: 'error', data: null, error: 'Brain offline', wantSkeletons: 0, wantAlert: 'Brain offline', wantStatus: 'DEGRADED', wantTotal: '--', wantPills: 1 },
    { name: 'error over cached facts', data: payload(), error: 'Brain offline', wantSkeletons: 0, wantAlert: 'Brain offline', wantStatus: 'DEGRADED', wantTotal: '5', wantPills: 3 },
    { name: 'ready', data: payload(), error: null, wantSkeletons: 0, wantAlert: '', wantStatus: 'OPTIMAL', wantTotal: '5', wantPills: 3 },
  ])('$name', ({ data, error, wantSkeletons, wantAlert, wantStatus, wantTotal, wantPills }) => {
    act(() => {
      if (data !== null) useConsoleStore.getState().setFacts(data);
      if (error !== null) useConsoleStore.getState().failFacts(error);
    });
    render(<MemoryView />);
    expect(document.querySelectorAll('.fact-card-skeleton')).toHaveLength(wantSkeletons);
    expect(text('.permet-alert-box p')).toBe(wantAlert);
    expect(text('.text-success, .text-danger')).toBe(wantStatus);
    expect(text('#memory-total-count')).toBe(wantTotal);
    expect(document.querySelectorAll('.cat-pill')).toHaveLength(wantPills);
  });

  it('shows metrics, the sync time and cards in importance order', () => {
    seed(payload());
    render(<MemoryView />);
    expect(screen.getByText('3 REINFORCED')).toBeTruthy();
    expect(text('#memory-avg-importance')).toBe('2.60x');
    expect(screen.getByText(`SYNCED ${new Date(NOW).toLocaleTimeString()}`)).toBeTruthy();
    expect(nodes()).toEqual(['NODE #12', 'NODE #7', 'NODE #9', 'NODE #30', 'NODE #4']);
    expect(text('.fact-card')).toBe('NODE #12 5xBuilds ship through Vanadis 30s AGO COPY');
    expect(document.querySelectorAll('.fact-card.high-importance')).toHaveLength(3);
    expect(text('.results-telemetry-bar span')).toBe('SHOWING 5 / 5 NODES');
  });
});

describe('MemoryView filters', () => {
  it.each([
    { name: 'reinforced pill', pill: 'REINFORCED', query: '', want: ['NODE #12', 'NODE #7', 'NODE #9'], results: 'SHOWING 3 / 5 NODES', marks: [] },
    { name: 'base pill', pill: 'BASE', query: '', want: ['NODE #30', 'NODE #4'], results: 'SHOWING 2 / 5 NODES', marks: [] },
    { name: 'search highlights case-insensitively', pill: 'ALL', query: ' oat MILK ', want: ['NODE #9', 'NODE #30'], results: 'SHOWING 2 / 5 NODES', marks: ['oat milk', 'Oat milk'] },
    { name: 'search by node id', pill: 'ALL', query: '30', want: ['NODE #30'], results: 'SHOWING 1 / 5 NODES', marks: [] },
    { name: 'pill and search combine', pill: 'REINFORCED', query: 'oat', want: ['NODE #9'], results: 'SHOWING 1 / 5 NODES', marks: ['oat'] },
    { name: 'no matches', pill: 'BASE', query: 'vanadis', want: [], results: 'SHOWING 0 / 5 NODES', marks: [] },
  ])('$name', ({ pill, query, want, results, marks }) => {
    seed(payload());
    render(<MemoryView />);
    fireEvent.click(screen.getByRole('button', { name: new RegExp(`^${pill} `) }));
    fireEvent.change(search(), { target: { value: query } });
    expect(nodes()).toEqual(want);
    expect(text('.results-telemetry-bar span')).toBe(results);
    expect([...document.querySelectorAll('mark.cyber-highlight')].map((m) => m.textContent)).toEqual(marks);
    expect(screen.getByRole('button', { name: new RegExp(`^${pill} `) }).getAttribute('aria-pressed')).toBe('true');
    expect(document.querySelectorAll('.empty-state-box')).toHaveLength(want.length === 0 ? 1 : 0);
  });

  it.each([
    { name: 'clear button', how: 'button' },
    { name: 'Escape in the search box', how: 'escape' },
  ])('clears the search with the $name', ({ how }) => {
    seed(payload());
    render(
      <>
        <Shortcuts />
        <MemoryView />
      </>,
    );
    fireEvent.change(search(), { target: { value: 'cumin' } });
    expect(nodes()).toEqual(['NODE #4']);
    if (how === 'button') {
      fireEvent.click(screen.getByRole('button', { name: 'Clear search' }));
    } else {
      search().focus();
      fireEvent.keyDown(window, { key: 'Escape' });
    }
    expect(search().value).toBe('');
    expect(nodes()).toHaveLength(5);
  });
});

describe('MemoryView rendering cost', () => {
  it('an identical payload re-renders nothing', () => {
    seed(payload());
    const renders: string[] = [];
    const onRender: ProfilerOnRenderCallback = (id) => renders.push(id);
    render(
      <Profiler id="memory" onRender={onRender}>
        <MemoryView />
      </Profiler>,
    );
    renders.length = 0;
    seed(payload());
    expect(renders).toEqual([]);
    seed(payload([...FACTS, fact(50, 'A new fact', 1)]));
    expect(renders.length).toBeGreaterThan(0);
    expect(nodes()).toHaveLength(6);
  });
});

describe('MemoryView actions', () => {
  it('copies a fact', async () => {
    seed(payload());
    render(<MemoryView />);
    await act(async () => {
      fireEvent.click(screen.getAllByRole('button', { name: 'Copy fact text' })[0] as HTMLElement);
      await Promise.resolve();
    });
    expect(writeText.mock.calls).toEqual([['Builds ship through Vanadis']]);
  });

  it.each([
    { name: 'sync button refetches and re-stamps', button: 'SYNC ARCHIVE', error: null },
    { name: 'retry after a failure', button: 'RETRY SYNCHRONIZATION', error: 'Brain offline' },
  ])('$name', async ({ button, error }) => {
    seed(payload());
    if (error !== null) act(() => useConsoleStore.getState().failFacts(error));
    render(<MemoryView />);
    fetchMock.mockImplementation(() => json(payload()));
    vi.setSystemTime(NOW + 60_000);
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: new RegExp(button) }));
      await vi.waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    });
    await act(async () => {
      await vi.waitFor(() => expect(screen.getByText(`SYNCED ${new Date(NOW + 60_000).toLocaleTimeString()}`)).toBeTruthy());
    });
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/facts');
    expect(useConsoleStore.getState().memory.facts.error).toBeNull();
  });

  it('a failed sync keeps the previous stamp', async () => {
    seed(payload());
    render(<MemoryView />);
    fetchMock.mockImplementation(() => json({ error: 'nope' }, 502));
    vi.setSystemTime(NOW + 60_000);
    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: /SYNC ARCHIVE/ }));
      await vi.waitFor(() => expect(useConsoleStore.getState().memory.facts.error).not.toBeNull());
    });
    expect(screen.getByText(`SYNCED ${new Date(NOW).toLocaleTimeString()}`)).toBeTruthy();
  });

  it.each([
    { name: 'retries a failed boot load on open', data: null, error: 'Brain offline', wantFetches: 1 },
    { name: 'does not refetch loaded facts on open', data: payload(), error: null, wantFetches: 0 },
    { name: 'leaves an in-flight boot load alone', data: null, error: null, wantFetches: 0 },
  ])('$name', ({ data, error, wantFetches }) => {
    act(() => {
      if (data !== null) useConsoleStore.getState().setFacts(data);
      if (error !== null) useConsoleStore.getState().failFacts(error);
    });
    render(<MemoryView />);
    expect(fetchMock).toHaveBeenCalledTimes(wantFetches);
  });
});
