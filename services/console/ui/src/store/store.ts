import { create } from 'zustand';
import type {
  BuildDetailAPIResponse,
  BuildsAPIResponse,
  ClusterResponse,
  DocketSearchAPIResponse,
  DocketTaskLookup,
  DocketTasksAPIResponse,
  FactsAPIResponse,
  ScheduleRunsAPIResponse,
  SchedulesAPIResponse,
} from '../api/types';
import { reconcile } from './reconcile';

/** Last poll outcome for one endpoint. `data` survives errors so views keep the stale snapshot. */
export interface Resource<T> {
  readonly data: T | null;
  readonly error: string | null;
}

export type TabId = 'telemetry' | 'tasks' | 'schedules' | 'memory' | 'builds' | 'docket';

export type DrawerState =
  | { readonly kind: 'closed' }
  | { readonly kind: 'service'; readonly name: string }
  | { readonly kind: 'build'; readonly id: string }
  | { readonly kind: 'task'; readonly id: number };

export interface ConsoleState {
  readonly status: { readonly cluster: Resource<ClusterResponse> };
  readonly schedules: {
    readonly list: Resource<SchedulesAPIResponse>;
    readonly runs: Resource<ScheduleRunsAPIResponse>;
  };
  readonly memory: { readonly facts: Resource<FactsAPIResponse> };
  readonly builds: {
    readonly list: Resource<BuildsAPIResponse>;
    readonly detail: Resource<BuildDetailAPIResponse>;
  };
  readonly docket: {
    readonly list: Resource<DocketTasksAPIResponse>;
    readonly search: Resource<DocketSearchAPIResponse>;
    readonly detail: Resource<DocketTaskLookup>;
  };
  readonly drawer: DrawerState;
  readonly ui: { readonly activeTab: TabId };
}

export interface ConsoleActions {
  readonly setCluster: (data: ClusterResponse) => void;
  readonly failCluster: (error: string) => void;
  readonly setSchedules: (data: SchedulesAPIResponse) => void;
  readonly failSchedules: (error: string) => void;
  readonly setScheduleRuns: (data: ScheduleRunsAPIResponse) => void;
  readonly failScheduleRuns: (error: string) => void;
  readonly setFacts: (data: FactsAPIResponse) => void;
  readonly failFacts: (error: string) => void;
  readonly setBuilds: (data: BuildsAPIResponse) => void;
  readonly failBuilds: (error: string) => void;
  readonly setBuildDetail: (data: BuildDetailAPIResponse) => void;
  readonly failBuildDetail: (error: string) => void;
  readonly setDocketTasks: (data: DocketTasksAPIResponse) => void;
  readonly failDocketTasks: (error: string) => void;
  readonly setDocketSearch: (data: DocketSearchAPIResponse) => void;
  readonly failDocketSearch: (error: string) => void;
  readonly setDocketTask: (data: DocketTaskLookup) => void;
  readonly failDocketTask: (error: string) => void;
  readonly openDrawer: (drawer: Exclude<DrawerState, { kind: 'closed' }>) => void;
  readonly closeDrawer: () => void;
  readonly setActiveTab: (tab: TabId) => void;
}

export type ConsoleStore = ConsoleState & ConsoleActions;

const EMPTY_RESOURCE: Resource<never> = Object.freeze({ data: null, error: null });
const CLOSED_DRAWER: DrawerState = Object.freeze({ kind: 'closed' });

/** Successful poll: structurally share with the previous snapshot and clear any error. */
export function settle<T>(prev: Resource<T>, data: T): Resource<T> {
  const merged = prev.data === null ? data : reconcile(prev.data, data);
  return merged === prev.data && prev.error === null ? prev : { data: merged, error: null };
}

/** Failed poll: keep the last good snapshot, record the error (same message is a no-op). */
export function fault<T>(prev: Resource<T>, error: string): Resource<T> {
  return prev.error === error ? prev : { data: prev.data, error };
}

function sameDrawer(a: DrawerState, b: DrawerState): boolean {
  if (a.kind === 'service' && b.kind === 'service') return a.name === b.name;
  if (a.kind === 'build' && b.kind === 'build') return a.id === b.id;
  if (a.kind === 'task' && b.kind === 'task') return a.id === b.id;
  return a.kind === b.kind;
}

export const useConsoleStore = create<ConsoleStore>()((set) => {
  // Every setter returns the current state object when nothing changed, which zustand treats as
  // a no-op: no listener runs, so an unchanged poll costs zero selector calls and zero renders.
  const patchSlice = <K extends 'status' | 'schedules' | 'memory' | 'builds' | 'docket', F extends keyof ConsoleState[K]>(
    slice: K,
    field: F,
    next: (prev: ConsoleState[K][F]) => ConsoleState[K][F],
  ): void =>
    set((state) => {
      const current = state[slice];
      const updated = next(current[field]);
      return updated === current[field] ? state : { [slice]: { ...current, [field]: updated } };
    });

  return {
    status: { cluster: EMPTY_RESOURCE },
    schedules: { list: EMPTY_RESOURCE, runs: EMPTY_RESOURCE },
    memory: { facts: EMPTY_RESOURCE },
    builds: { list: EMPTY_RESOURCE, detail: EMPTY_RESOURCE },
    docket: { list: EMPTY_RESOURCE, search: EMPTY_RESOURCE, detail: EMPTY_RESOURCE },
    drawer: CLOSED_DRAWER,
    ui: { activeTab: 'telemetry' },

    setCluster: (data) => patchSlice('status', 'cluster', (prev) => settle(prev, data)),
    failCluster: (error) => patchSlice('status', 'cluster', (prev) => fault(prev, error)),
    setSchedules: (data) => patchSlice('schedules', 'list', (prev) => settle(prev, data)),
    failSchedules: (error) => patchSlice('schedules', 'list', (prev) => fault(prev, error)),
    setScheduleRuns: (data) => patchSlice('schedules', 'runs', (prev) => settle(prev, data)),
    failScheduleRuns: (error) => patchSlice('schedules', 'runs', (prev) => fault(prev, error)),
    setFacts: (data) => patchSlice('memory', 'facts', (prev) => settle(prev, data)),
    failFacts: (error) => patchSlice('memory', 'facts', (prev) => fault(prev, error)),
    setBuilds: (data) => patchSlice('builds', 'list', (prev) => settle(prev, data)),
    failBuilds: (error) => patchSlice('builds', 'list', (prev) => fault(prev, error)),
    setBuildDetail: (data) => patchSlice('builds', 'detail', (prev) => settle(prev, data)),
    failBuildDetail: (error) => patchSlice('builds', 'detail', (prev) => fault(prev, error)),
    setDocketTasks: (data) => patchSlice('docket', 'list', (prev) => settle(prev, data)),
    failDocketTasks: (error) => patchSlice('docket', 'list', (prev) => fault(prev, error)),
    setDocketSearch: (data) => patchSlice('docket', 'search', (prev) => settle(prev, data)),
    failDocketSearch: (error) => patchSlice('docket', 'search', (prev) => fault(prev, error)),
    setDocketTask: (data) => patchSlice('docket', 'detail', (prev) => settle(prev, data)),
    failDocketTask: (error) => patchSlice('docket', 'detail', (prev) => fault(prev, error)),

    openDrawer: (drawer) => set((state) => (sameDrawer(state.drawer, drawer) ? state : { drawer })),
    closeDrawer: () => set((state) => (state.drawer.kind === 'closed' ? state : { drawer: CLOSED_DRAWER })),
    setActiveTab: (tab) => set((state) => (state.ui.activeTab === tab ? state : { ui: { ...state.ui, activeTab: tab } })),
  };
});
