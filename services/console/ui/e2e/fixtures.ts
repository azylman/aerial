// Serves the committed fixtures for every /console/api/** request both UIs make, and stubs the
// web-font CSS so rendering never depends on the network. Any API path without a fixture is
// answered 404 and recorded, so a UI that starts calling something new fails loudly.

import type { Page, Route } from '@playwright/test';
import buildDetails from './fixtures/build-details.json' with { type: 'json' };
import builds from './fixtures/builds.json' with { type: 'json' };
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
};

const DETAILS: Readonly<Record<string, unknown>> = buildDetails;
const DETAIL_PREFIX = '/console/api/builds/';

/** Fixture body for an API pathname, or undefined when the suite has none. */
export function fixtureFor(pathname: string): unknown {
  if (pathname in FIXED) return FIXED[pathname];
  if (pathname.startsWith(DETAIL_PREFIX)) return DETAILS[decodeURIComponent(pathname.slice(DETAIL_PREFIX.length))];
  return undefined;
}

/** Installs the routes on `page`; returns the list of unexpected API requests it saw. */
export async function serveFixtures(page: Page): Promise<string[]> {
  const unexpected: string[] = [];
  await page.route('**/console/api/**', async (route: Route) => {
    const { pathname } = new URL(route.request().url());
    const body = fixtureFor(pathname);
    if (body === undefined) {
      unexpected.push(`${route.request().method()} ${pathname}`);
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
