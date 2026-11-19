import { act, cleanup, render, screen, within } from '@testing-library/react';
import { Profiler } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { BuildsAPIResponse, ClusterResponse, FactsAPIResponse, SchedulesAPIResponse } from '../api/types';
import { useConsoleStore } from '../store/store';
import { Header } from './Header';

// Every Icon render is recorded as "name:size", which pins down exactly which components
// re-rendered: the brand badge is the only 18px icon, tab buttons render 14px icons.
const iconRenders = vi.hoisted((): string[] => []);
vi.mock('./Icon', () => ({
  Icon: ({ name, size = 14 }: { name: string; size?: number }) => {
    iconRenders.push(`${name}:${size}`);
    return <svg data-icon={name} />;
  },
}));

const CLUSTER = {
  system_time: '2026-11-08T12:00:00Z',
  active_tasks: [{ id: 't1' }, { id: 't2' }],
  services: [
    { name: 'eri', status: 'healthy' },
    { name: 'haro', status: 'healthy' },
    { name: 'coven', status: 'healthy' },
    { name: 'vision', status: 'down' },
  ],
  quick_launch_links: [
    { name: 'Console', url: '/console/', icon: '◆', target: '_self' },
    { name: 'Grafana', url: '/grafana/', description: 'Dashboards' },
    { name: 'Custom', url: 'https://example.test/', is_custom: true },
  ],
} as unknown as ClusterResponse;

const SCHEDULES = { summary: { total_active: 40 }, crons: [], one_shots: [] } as unknown as SchedulesAPIResponse;
const FACTS: FactsAPIResponse = { facts: [], total: 150, limit: 0, offset: 0 };
const BUILDS = { running_count: 2, queued_count: 3, builds: [] } as unknown as BuildsAPIResponse;

function badges(): Record<string, string | null> {
  const out: Record<string, string | null> = {};
  for (const tab of screen.getAllByRole('tab')) {
    const label = tab.querySelector('span:nth-child(2)')?.textContent ?? '';
    out[label] = tab.querySelector('.tab-badge')?.textContent ?? null;
  }
  return out;
}

describe('Header', () => {
  beforeEach(() => {
    useConsoleStore.setState(useConsoleStore.getInitialState(), true);
    window.history.replaceState(null, '', '/console/next/');
    iconRenders.length = 0;
  });
  afterEach(cleanup);

  it('matches the static header before any data arrives', () => {
    render(<Header />);
    expect(screen.getByRole('heading', { level: 1 }).textContent).toBe('Aerial Console');
    expect(screen.getByText('Ready')).toBeTruthy();
    expect(badges()).toEqual({ Overview: null, Tasks: '0', Schedules: '0', Memory: '0', Builds: '0', Docket: null });
    expect(screen.queryByRole('navigation', { name: 'Tactical Gateway Launcher' })).toBeNull();
    const telemetry = screen.getByRole('tab', { name: /Overview/ });
    expect(telemetry.getAttribute('aria-selected')).toBe('true');
    expect(telemetry.getAttribute('aria-controls')).toBe('telemetry-view');
  });

  it('renders badge counts, permet score and dock from the store', () => {
    render(<Header />);
    act(() => {
      const s = useConsoleStore.getState();
      s.setCluster(CLUSTER);
      s.setSchedules(SCHEDULES);
      s.setFacts(FACTS);
      s.setBuilds(BUILDS);
    });
    expect(badges()).toEqual({ Overview: null, Tasks: '2', Schedules: '40', Memory: '150', Builds: '5', Docket: null });
    const score = screen.getByText('3 / 4 Healthy');
    expect(score.getAttribute('style')).toContain('var(--accent-amber)');

    const dock = screen.getByRole('navigation', { name: 'Tactical Gateway Launcher' });
    const chips = within(dock).getAllByRole('link');
    expect(chips.map((a) => [a.textContent, a.className, a.getAttribute('target'), a.getAttribute('rel'), a.getAttribute('title')])).toEqual([
      ['◆ Console', 'dock-chip active', '_self', null, null],
      ['Grafana', 'dock-chip', '_blank', 'noopener noreferrer', 'Dashboards'],
      ['Custom', 'dock-chip', '_blank', 'noopener noreferrer', null],
    ]);
    expect(chips.map((a) => a.querySelector('.ping-dot')?.className)).toEqual(['ping-dot', 'ping-dot', 'ping-dot custom']);
  });

  it('clicking a tab navigates through the store', () => {
    const scrollTo = vi.spyOn(window, 'scrollTo').mockImplementation(() => undefined);
    render(<Header />);
    act(() => screen.getByRole('tab', { name: /Builds/ }).click());
    expect(useConsoleStore.getState().ui.activeTab).toBe('builds');
    expect(screen.getByRole('tab', { name: /Builds/ }).className).toBe('view-tab-btn active');
    expect(screen.getByRole('tab', { name: /Overview/ }).getAttribute('aria-selected')).toBe('false');
    expect(scrollTo).not.toHaveBeenCalled();

    Object.defineProperty(window, 'scrollY', { value: 240, configurable: true });
    act(() => screen.getByRole('tab', { name: /Builds/ }).click());
    expect(scrollTo).toHaveBeenCalledWith({ top: 0, left: 0, behavior: 'instant' });
    Object.defineProperty(window, 'scrollY', { value: 0, configurable: true });
  });
});

interface IsolationRow {
  name: string;
  update: () => void;
  /** Icon renders caused by the update: which tab buttons (or the brand) re-rendered. */
  wantIcons: string[];
  /** Commits of the header subtree. */
  wantCommits: number;
}

const isolationRows: IsolationRow[] = [
  {
    name: 'ui slice: only the two affected tab buttons re-render, never the header shell',
    update: () => useConsoleStore.getState().setActiveTab('memory'),
    wantIcons: ['bolt:14', 'brain:14'],
    wantCommits: 1,
  },
  {
    name: 'drawer slice: nothing in the header renders',
    update: () => useConsoleStore.getState().openDrawer({ kind: 'service', name: 'eri' }),
    wantIcons: [],
    wantCommits: 0,
  },
  {
    name: 'identical status poll: nothing renders',
    update: () => useConsoleStore.getState().setCluster(structuredClone(CLUSTER)),
    wantIcons: [],
    wantCommits: 0,
  },
  {
    name: 'schedule run poll: nothing renders',
    update: () => useConsoleStore.getState().setScheduleRuns({ status: 'ok', total: 0, limit: 50, offset: 0, runs: [] }),
    wantIcons: [],
    wantCommits: 0,
  },
  {
    name: 'facts poll: only the memory badge button re-renders',
    update: () => useConsoleStore.getState().setFacts({ ...FACTS, total: 151 }),
    wantIcons: ['brain:14'],
    wantCommits: 1,
  },
];

describe('Header render isolation', () => {
  beforeEach(() => {
    useConsoleStore.setState(useConsoleStore.getInitialState(), true);
    useConsoleStore.getState().setCluster(CLUSTER);
    useConsoleStore.getState().setFacts(FACTS);
  });
  afterEach(cleanup);

  it.each(isolationRows)('$name', ({ update, wantIcons, wantCommits }) => {
    let commits = 0;
    render(
      <Profiler id="header" onRender={() => commits++}>
        <Header />
      </Profiler>,
    );
    iconRenders.length = 0;
    commits = 0;

    act(update);

    expect(iconRenders.sort()).toEqual(wantIcons);
    expect(commits).toBe(wantCommits);
  });
});
