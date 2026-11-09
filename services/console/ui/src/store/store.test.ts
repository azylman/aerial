import { beforeEach, describe, expect, it } from 'vitest';
import type { ConsoleActions, ConsoleState, Resource } from './store';
import { fault, settle, useConsoleStore } from './store';

describe('settle', () => {
  it.each([
    {
      name: 'first snapshot is stored as-is',
      prev: { data: null, error: null },
      data: { n: [1] },
      want: { data: { n: [1] }, error: null },
      wantPrev: false,
    },
    {
      name: 'equal snapshot returns prev',
      prev: { data: { n: [1] }, error: null },
      data: { n: [1] },
      want: { data: { n: [1] }, error: null },
      wantPrev: true,
    },
    {
      name: 'equal snapshot clears a stale error',
      prev: { data: { n: [1] }, error: 'HTTP 503' },
      data: { n: [1] },
      want: { data: { n: [1] }, error: null },
      wantPrev: false,
    },
    {
      name: 'changed snapshot replaces data',
      prev: { data: { n: [1] }, error: null },
      data: { n: [2] },
      want: { data: { n: [2] }, error: null },
      wantPrev: false,
    },
  ])('$name', ({ prev, data, want, wantPrev }) => {
    const got = settle<{ n: number[] }>(prev, data);
    expect(got).toEqual(want);
    expect(got === prev).toBe(wantPrev);
  });
});

describe('fault', () => {
  it.each([
    { name: 'records error and keeps data', prev: { data: 1, error: null }, error: 'HTTP 502', want: { data: 1, error: 'HTTP 502' }, wantPrev: false },
    { name: 'same error is a no-op', prev: { data: 1, error: 'HTTP 502' }, error: 'HTTP 502', want: { data: 1, error: 'HTTP 502' }, wantPrev: true },
    { name: 'new error replaces old', prev: { data: null, error: 'a' }, error: 'b', want: { data: null, error: 'b' }, wantPrev: false },
  ])('$name', ({ prev, error, want, wantPrev }) => {
    const got = fault<number>(prev, error);
    expect(got).toEqual(want);
    expect(got === prev).toBe(wantPrev);
  });
});

type SliceName = keyof ConsoleState;
const SLICES: readonly SliceName[] = ['status', 'schedules', 'memory', 'builds', 'drawer', 'ui'];

const CLUSTER = { system_time: 't1', services: [{ name: 'eri', status: 'healthy' }] };
const RUNS = { status: 'ok', total: 1, limit: 50, offset: 0, runs: [{ id: 'r1', status: 'success' }] };
const ok = <T>(data: T): Resource<T> => ({ data, error: null });

interface ActionRow {
  name: string;
  seed: object;
  action: keyof ConsoleActions;
  arg: unknown;
  wantNotified: boolean;
  wantSlice: SliceName;
  wantValue: unknown;
}

// Each row seeds the store, dispatches one action with a freshly cloned argument, and asserts
// the touched slice value, whether subscribers were notified, and that sibling slices kept
// their references (the isolation contract that keeps unrelated views from re-rendering).
const actionRows: ActionRow[] = [
  { name: 'setCluster stores first snapshot', seed: {}, action: 'setCluster', arg: CLUSTER, wantNotified: true, wantSlice: 'status', wantValue: { cluster: ok(CLUSTER) } },
  { name: 'setCluster with equal data is silent', seed: { status: { cluster: ok(CLUSTER) } }, action: 'setCluster', arg: CLUSTER, wantNotified: false, wantSlice: 'status', wantValue: { cluster: ok(CLUSTER) } },
  { name: 'failCluster keeps stale data', seed: { status: { cluster: ok(CLUSTER) } }, action: 'failCluster', arg: 'HTTP 503', wantNotified: true, wantSlice: 'status', wantValue: { cluster: { data: CLUSTER, error: 'HTTP 503' } } },
  { name: 'setSchedules', seed: {}, action: 'setSchedules', arg: { status: 'ok', crons: [] }, wantNotified: true, wantSlice: 'schedules', wantValue: { list: ok({ status: 'ok', crons: [] }), runs: { data: null, error: null } } },
  { name: 'failSchedules', seed: {}, action: 'failSchedules', arg: 'down', wantNotified: true, wantSlice: 'schedules', wantValue: { list: { data: null, error: 'down' }, runs: { data: null, error: null } } },
  { name: 'setScheduleRuns', seed: {}, action: 'setScheduleRuns', arg: RUNS, wantNotified: true, wantSlice: 'schedules', wantValue: { list: { data: null, error: null }, runs: ok(RUNS) } },
  { name: 'failScheduleRuns repeated is silent', seed: { schedules: { list: { data: null, error: null }, runs: { data: null, error: 'x' } } }, action: 'failScheduleRuns', arg: 'x', wantNotified: false, wantSlice: 'schedules', wantValue: { list: { data: null, error: null }, runs: { data: null, error: 'x' } } },
  { name: 'setFacts', seed: {}, action: 'setFacts', arg: { facts: [{ id: 1 }], total: 1 }, wantNotified: true, wantSlice: 'memory', wantValue: { facts: ok({ facts: [{ id: 1 }], total: 1 }) } },
  { name: 'failFacts', seed: {}, action: 'failFacts', arg: 'boom', wantNotified: true, wantSlice: 'memory', wantValue: { facts: { data: null, error: 'boom' } } },
  { name: 'setBuilds', seed: {}, action: 'setBuilds', arg: { builds: [{ id: 'b1' }] }, wantNotified: true, wantSlice: 'builds', wantValue: { list: ok({ builds: [{ id: 'b1' }] }), detail: { data: null, error: null } } },
  { name: 'failBuilds', seed: {}, action: 'failBuilds', arg: 'boom', wantNotified: true, wantSlice: 'builds', wantValue: { list: { data: null, error: 'boom' }, detail: { data: null, error: null } } },
  { name: 'setBuildDetail', seed: {}, action: 'setBuildDetail', arg: { status: 'ok', chunks: [] }, wantNotified: true, wantSlice: 'builds', wantValue: { list: { data: null, error: null }, detail: ok({ status: 'ok', chunks: [] }) } },
  { name: 'failBuildDetail', seed: {}, action: 'failBuildDetail', arg: 'Build not found', wantNotified: true, wantSlice: 'builds', wantValue: { list: { data: null, error: null }, detail: { data: null, error: 'Build not found' } } },
  { name: 'openDrawer service', seed: {}, action: 'openDrawer', arg: { kind: 'service', name: 'eri' }, wantNotified: true, wantSlice: 'drawer', wantValue: { kind: 'service', name: 'eri' } },
  { name: 'openDrawer same service is silent', seed: { drawer: { kind: 'service', name: 'eri' } }, action: 'openDrawer', arg: { kind: 'service', name: 'eri' }, wantNotified: false, wantSlice: 'drawer', wantValue: { kind: 'service', name: 'eri' } },
  { name: 'openDrawer other service', seed: { drawer: { kind: 'service', name: 'eri' } }, action: 'openDrawer', arg: { kind: 'service', name: 'haro' }, wantNotified: true, wantSlice: 'drawer', wantValue: { kind: 'service', name: 'haro' } },
  { name: 'openDrawer same build is silent', seed: { drawer: { kind: 'build', id: 'b1' } }, action: 'openDrawer', arg: { kind: 'build', id: 'b1' }, wantNotified: false, wantSlice: 'drawer', wantValue: { kind: 'build', id: 'b1' } },
  { name: 'openDrawer switches kind', seed: { drawer: { kind: 'build', id: 'b1' } }, action: 'openDrawer', arg: { kind: 'service', name: 'b1' }, wantNotified: true, wantSlice: 'drawer', wantValue: { kind: 'service', name: 'b1' } },
  { name: 'closeDrawer', seed: { drawer: { kind: 'build', id: 'b1' } }, action: 'closeDrawer', arg: undefined, wantNotified: true, wantSlice: 'drawer', wantValue: { kind: 'closed' } },
  { name: 'closeDrawer when closed is silent', seed: {}, action: 'closeDrawer', arg: undefined, wantNotified: false, wantSlice: 'drawer', wantValue: { kind: 'closed' } },
  { name: 'setActiveTab', seed: {}, action: 'setActiveTab', arg: 'memory', wantNotified: true, wantSlice: 'ui', wantValue: { activeTab: 'memory' } },
  { name: 'setActiveTab same tab is silent', seed: {}, action: 'setActiveTab', arg: 'telemetry', wantNotified: false, wantSlice: 'ui', wantValue: { activeTab: 'telemetry' } },
];

describe('useConsoleStore actions', () => {
  beforeEach(() => {
    useConsoleStore.setState(useConsoleStore.getInitialState(), true);
  });

  it.each(actionRows)('$name', ({ seed, action, arg, wantNotified, wantSlice, wantValue }) => {
    useConsoleStore.setState(structuredClone(seed) as Partial<ConsoleState>);
    const before = useConsoleStore.getState();
    let notified = 0;
    const unsubscribe = useConsoleStore.subscribe(() => {
      notified++;
    });

    const dispatch = before[action] as (value?: unknown) => void;
    dispatch(structuredClone(arg));
    unsubscribe();

    const after = useConsoleStore.getState();
    expect(after[wantSlice]).toEqual(wantValue);
    expect(notified).toBe(wantNotified ? 1 : 0);
    for (const slice of SLICES) {
      if (slice !== wantSlice) expect(after[slice], slice).toBe(before[slice]);
    }
  });

  it('starts on telemetry with every resource empty and the drawer closed', () => {
    const { status, schedules, memory, builds, drawer, ui } = useConsoleStore.getState();
    expect({ status, schedules, memory, builds, drawer, ui }).toEqual({
      status: { cluster: { data: null, error: null } },
      schedules: { list: { data: null, error: null }, runs: { data: null, error: null } },
      memory: { facts: { data: null, error: null } },
      builds: { list: { data: null, error: null }, detail: { data: null, error: null } },
      drawer: { kind: 'closed' },
      ui: { activeTab: 'telemetry' },
    });
  });
});
