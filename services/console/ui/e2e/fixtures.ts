// Serves the committed fixtures for every /console/api/** request the console makes, and stubs the
// web-font CSS so rendering never depends on the network. Any API path without a fixture is
// answered 404 and recorded, so a UI that starts calling something new fails loudly.

import type { Page, Route } from '@playwright/test';
import buildDetails from './fixtures/build-details.json' with { type: 'json' };
import builds from './fixtures/builds.json' with { type: 'json' };
import docketSearch from './fixtures/docket-search.json' with { type: 'json' };
import docketTaskDetails from './fixtures/docket-task-details.json' with { type: 'json' };
import docketTasks from './fixtures/docket-tasks.json' with { type: 'json' };
import facts from './fixtures/facts.json' with { type: 'json' };
import scheduleRuns from './fixtures/schedule-runs.json' with { type: 'json' };
import schedules from './fixtures/schedules.json' with { type: 'json' };
import status from './fixtures/status.json' with { type: 'json' };

const FIXED: Readonly<Record<string, unknown>> = {
  '/console/api/status': status,
  '/console/api/schedules': schedules,
  '/console/api/schedules/runs': scheduleRuns,
  '/console/api/facts': facts,
  '/console/api/builds': builds,
  // Docket list and search answer per query string (pathname + search), like the real proxy.
  '/console/api/docket/tasks?status=open': docketTasks.open,
  '/console/api/docket/tasks?status=completed': docketTasks.completed,
  '/console/api/docket/tasks?status=wont_do': docketTasks.wont_do,
  '/console/api/docket/tasks?status=all': docketTasks.all,
  '/console/api/docket/search?q=kiosk&status=open': docketSearch['q=kiosk&status=open'],
};

const DETAIL_ROUTES: readonly (readonly [prefix: string, bodies: Readonly<Record<string, unknown>>])[] = [
  ['/console/api/builds/', buildDetails],
  ['/console/api/docket/tasks/', docketTaskDetails],
];

/**
 * Fixture body for an API URL, or undefined when the suite has none. An exact pathname + query
 * entry wins, then the bare pathname (queries ignored), then a per-id detail route.
 */
export function fixtureFor(pathname: string, search = ''): unknown {
  if (pathname + search in FIXED) return FIXED[pathname + search];
  if (pathname in FIXED) return FIXED[pathname];
  for (const [prefix, bodies] of DETAIL_ROUTES) {
    if (pathname.startsWith(prefix)) return bodies[decodeURIComponent(pathname.slice(prefix.length))];
  }
  return undefined;
}

/** Installs the routes on `page`; returns the list of unexpected API requests it saw. */
export async function serveFixtures(page: Page): Promise<string[]> {
  const unexpected: string[] = [];
  await page.route('**/console/api/**', async (route: Route) => {
    const { pathname, search } = new URL(route.request().url());
    const body = fixtureFor(pathname, search);
    if (body === undefined) {
      unexpected.push(`${route.request().method()} ${pathname}${search}`);
      await route.fulfill({ status: 404, contentType: 'application/json', body: '{"error":"no fixture"}' });
      return;
    }
    await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });
  });
  await page.route(/^https:\/\/fonts\.(googleapis|gstatic)\.com\//, (route) =>
    route.fulfill({ status: 200, contentType: 'text/css', body: '' }),
  );
  return unexpected;
}
