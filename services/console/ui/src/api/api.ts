import type {
  BuildDetailAPIResponse,
  BuildsAPIResponse,
  ClusterResponse,
  DocketSearchAPIResponse,
  DocketStatusFilter,
  DocketTaskDetailAPIResponse,
  DocketTasksAPIResponse,
  FactsAPIResponse,
  ScheduleRunsAPIResponse,
  SchedulesAPIResponse,
} from './types';

/**
 * API prefix for the page's mount point: the console is served under /console behind the LAN
 * gateway and at the root when hit directly, and both prefixes route to the same API handlers.
 */
export function apiBase(pathname: string): string {
  return pathname === '/console' || pathname.startsWith('/console/') ? '/console' : '';
}

/** Non-2xx API response. `message` carries the handler's JSON `error` field when it sent one. */
export class ApiError extends Error {
  override readonly name = 'ApiError';
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

function errorMessage(status: number, body: string): string {
  try {
    const parsed: unknown = JSON.parse(body);
    if (typeof parsed === 'object' && parsed !== null && 'error' in parsed) {
      const { error } = parsed;
      if (typeof error === 'string' && error !== '') return error;
    }
  } catch {
    // Non-JSON error bodies (proxy pages, plain text) fall through to the status line.
  }
  return `HTTP ${status}`;
}

async function getJSON<T>(path: string, signal: AbortSignal): Promise<T> {
  const res = await fetch(apiBase(window.location.pathname) + path, { signal });
  if (!res.ok) throw new ApiError(res.status, errorMessage(res.status, await res.text()));
  return (await res.json()) as T;
}

function withAgent(path: string, agent?: string): string {
  if (!agent || agent === 'all') return path;
  const sep = path.includes('?') ? '&' : '?';
  return `${path}${sep}agent=${encodeURIComponent(agent)}`;
}

export const fetchStatus = (signal: AbortSignal, agent?: string): Promise<ClusterResponse> =>
  getJSON(withAgent('/api/status', agent), signal);

export const fetchFacts = (signal: AbortSignal, agent?: string): Promise<FactsAPIResponse> =>
  getJSON(withAgent('/api/facts', agent), signal);

export const fetchSchedules = (signal: AbortSignal, agent?: string): Promise<SchedulesAPIResponse> =>
  getJSON(withAgent('/api/schedules', agent), signal);

export const fetchScheduleRuns = (signal: AbortSignal, agent?: string): Promise<ScheduleRunsAPIResponse> =>
  getJSON(withAgent('/api/schedules/runs?limit=50&offset=0', agent), signal);

export const fetchBuilds = (signal: AbortSignal, agent?: string): Promise<BuildsAPIResponse> =>
  getJSON(withAgent('/api/builds', agent), signal);

export const fetchBuildDetail = (id: string, signal: AbortSignal): Promise<BuildDetailAPIResponse> =>
  getJSON(`/api/builds/${encodeURIComponent(id)}`, signal);

export const fetchDocketTasks = (status: DocketStatusFilter, signal: AbortSignal): Promise<DocketTasksAPIResponse> =>
  getJSON(`/api/docket/tasks?${new URLSearchParams({ status }).toString()}`, signal);

export const searchDocket = (q: string, status: DocketStatusFilter, signal: AbortSignal): Promise<DocketSearchAPIResponse> =>
  getJSON(`/api/docket/search?${new URLSearchParams({ q, status }).toString()}`, signal);

export const fetchDocketTask = (id: number, signal: AbortSignal): Promise<DocketTaskDetailAPIResponse> =>
  getJSON(`/api/docket/tasks/${id}`, signal);
