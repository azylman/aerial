import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  ApiError,
  apiBase,
  fetchBuildDetail,
  fetchBuilds,
  fetchFacts,
  fetchScheduleRuns,
  fetchSchedules,
  fetchStatus,
} from './api';

describe('apiBase', () => {
  it.each([
    { pathname: '/console/next/', want: '/console' },
    { pathname: '/console/next/memory', want: '/console' },
    { pathname: '/console/', want: '/console' },
    { pathname: '/console', want: '/console' },
    { pathname: '/next/', want: '' },
    { pathname: '/', want: '' },
    { pathname: '', want: '' },
    { pathname: '/consoles/next/', want: '' },
  ])('$pathname -> "$want"', ({ pathname, want }) => {
    expect(apiBase(pathname)).toBe(want);
  });
});

function stubFetch(status: number, body: string): ReturnType<typeof vi.fn> {
  const fn = vi.fn(async () => new Response(body, { status, headers: { 'Content-Type': 'application/json' } }));
  vi.stubGlobal('fetch', fn);
  return fn;
}

afterEach(() => {
  vi.unstubAllGlobals();
  window.history.replaceState(null, '', '/');
});

const OK_BODY = '{"status":"ok"}';

const FETCHERS = { fetchStatus, fetchFacts, fetchSchedules, fetchScheduleRuns, fetchBuilds } as const;

describe('typed fetchers', () => {
  it.each([
    { name: 'fetchStatus', fetcher: 'fetchStatus' as const, pathname: '/console/next/', wantURL: '/console/api/status' },
    { name: 'fetchStatus at root', fetcher: 'fetchStatus' as const, pathname: '/next/', wantURL: '/api/status' },
    { name: 'fetchFacts', fetcher: 'fetchFacts' as const, pathname: '/console/next/', wantURL: '/console/api/facts' },
    { name: 'fetchSchedules', fetcher: 'fetchSchedules' as const, pathname: '/console/next/', wantURL: '/console/api/schedules' },
    {
      name: 'fetchScheduleRuns',
      fetcher: 'fetchScheduleRuns' as const,
      pathname: '/console/next/',
      wantURL: '/console/api/schedules/runs?limit=50&offset=0',
    },
    { name: 'fetchBuilds', fetcher: 'fetchBuilds' as const, pathname: '/console/next/builds', wantURL: '/console/api/builds' },
  ])('$name requests $wantURL', async ({ fetcher, pathname, wantURL }) => {
    window.history.replaceState(null, '', pathname);
    const fetchMock = stubFetch(200, OK_BODY);
    const controller = new AbortController();

    await expect(FETCHERS[fetcher](controller.signal)).resolves.toEqual({ status: 'ok' });
    expect(fetchMock).toHaveBeenCalledWith(wantURL, { signal: controller.signal });
  });

  it.each([
    { id: 'build-1', wantURL: '/console/api/builds/build-1' },
    { id: 'a/b c', wantURL: '/console/api/builds/a%2Fb%20c' },
  ])('fetchBuildDetail($id) requests $wantURL', async ({ id, wantURL }) => {
    window.history.replaceState(null, '', '/console/next/');
    const fetchMock = stubFetch(200, OK_BODY);
    const controller = new AbortController();

    await expect(fetchBuildDetail(id, controller.signal)).resolves.toEqual({ status: 'ok' });
    expect(fetchMock).toHaveBeenCalledWith(wantURL, { signal: controller.signal });
  });

  it.each([
    { status: 404, body: '{"status":"error","error":"Build not found","chunks":[]}', wantMessage: 'Build not found' },
    { status: 503, body: '{"status":"degraded","error":""}', wantMessage: 'HTTP 503' },
    { status: 502, body: '<html>bad gateway</html>', wantMessage: 'HTTP 502' },
    { status: 500, body: '["not","an","object"]', wantMessage: 'HTTP 500' },
    { status: 500, body: 'null', wantMessage: 'HTTP 500' },
    { status: 405, body: '{"error":42}', wantMessage: 'HTTP 405' },
  ])('non-2xx $status rejects with ApiError "$wantMessage"', async ({ status, body, wantMessage }) => {
    stubFetch(status, body);

    const err: unknown = await fetchBuildDetail('x', new AbortController().signal).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(ApiError);
    expect(err).toMatchObject({ name: 'ApiError', status, message: wantMessage });
  });

  it('propagates aborts without wrapping', async () => {
    const controller = new AbortController();
    vi.stubGlobal(
      'fetch',
      vi.fn(async (_url: string, init: RequestInit) => {
        init.signal?.throwIfAborted();
        return new Response(OK_BODY);
      }),
    );
    controller.abort();

    const err: unknown = await fetchStatus(controller.signal).catch((e: unknown) => e);
    expect(err).not.toBeInstanceOf(ApiError);
    expect(err).toMatchObject({ name: 'AbortError' });
  });
});
