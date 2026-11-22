import { type Mock, vi } from 'vitest';

export type FetchMock = Mock<(url: string, init?: RequestInit) => Promise<Response>>;

/** A response that never settles: the request stays in flight for the rest of the test. */
export const pending = (): Promise<Response> => new Promise(() => undefined);

/** A JSON response with the given status. */
export const json = (status: number, body: unknown): Promise<Response> =>
  Promise.resolve(new Response(JSON.stringify(body), { status }));

/** A response whose body is sent verbatim (for non-JSON and malformed bodies). */
export const raw = (status: number, body: string): Promise<Response> =>
  Promise.resolve(new Response(body, { status, headers: { 'Content-Type': 'application/json' } }));

/**
 * Replaces the global fetch with a mock that answers with `respond` (every request stays pending by
 * default) and returns it. Callers restore the global with vi.unstubAllGlobals() in afterEach.
 */
export function stubFetch(respond: (url: string, init?: RequestInit) => Promise<Response> = pending): FetchMock {
  const fn: FetchMock = vi.fn(respond);
  vi.stubGlobal('fetch', fn);
  return fn;
}
