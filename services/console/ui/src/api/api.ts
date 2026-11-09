import type {
  BuildDetailAPIResponse,
  BuildsAPIResponse,
  ClusterResponse,
  FactsAPIResponse,
  ScheduleRunsAPIResponse,
  SchedulesAPIResponse,
} from './types';

/**
 * Port of getApiBase() from static/app.js: the console is mounted under /console behind the LAN
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

export const fetchStatus = (signal: AbortSignal): Promise<ClusterResponse> => getJSON('/api/status', signal);

export const fetchFacts = (signal: AbortSignal): Promise<FactsAPIResponse> => getJSON('/api/facts', signal);

export const fetchSchedules = (signal: AbortSignal): Promise<SchedulesAPIResponse> =>
  getJSON('/api/schedules', signal);

export const fetchScheduleRuns = (signal: AbortSignal): Promise<ScheduleRunsAPIResponse> =>
  getJSON('/api/schedules/runs?limit=50&offset=0', signal);

export const fetchBuilds = (signal: AbortSignal): Promise<BuildsAPIResponse> => getJSON('/api/builds', signal);

export const fetchBuildDetail = (id: string, signal: AbortSignal): Promise<BuildDetailAPIResponse> =>
  getJSON(`/api/builds/${encodeURIComponent(id)}`, signal);
