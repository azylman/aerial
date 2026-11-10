// Runner for the console e2e table (e2e/console.ts). For every row x viewport it loads the React
// console at /console/ against the fixtures and a pinned clock, replays the row's steps, and
// asserts the row's expectations plus zero console/page errors and zero unexpected API calls.

import { expect, type Page, test } from '@playwright/test';
import { FIXTURE_NOW } from './constants.ts';
import { serveFixtures } from './fixtures.ts';
import { CONSOLE_ROWS, CONTEXT_OPTIONS, type Expectation, type Step, VIEWPORTS } from './console.ts';

const escapeRegExp = (s: string): string => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
const tabByLabel = (page: Page, label: string) => page.getByRole('tab', { name: new RegExp(escapeRegExp(label)) });

async function runStep(page: Page, step: Step): Promise<void> {
  switch (step.kind) {
    case 'clickTab':
      await tabByLabel(page, step.tab).click();
      return;
    case 'keyTab':
      await tabByLabel(page, step.tab).focus();
      await page.keyboard.press(step.key);
      return;
    case 'type':
      await page.getByPlaceholder(step.placeholder).fill(step.text);
      return;
    case 'pill':
      await page.locator('.cat-pill', { hasText: step.label }).first().click();
      return;
    case 'openDrawer':
      await page.getByText(step.text, { exact: true }).first().click();
      return;
    case 'click':
      await page.locator(step.selector).first().click();
      return;
    case 'press':
      await page.keyboard.press(step.key);
      return;
    case 'scrollTop':
      await page.evaluate(() => window.scrollTo({ top: 0, left: 0, behavior: 'instant' }));
      return;
    case 'scrollBottom':
      await page.evaluate(() =>
        window.scrollTo({ top: document.scrollingElement!.scrollHeight, left: 0, behavior: 'instant' }),
      );
      return;
  }
}

async function assertExpectation(page: Page, want: Expectation): Promise<void> {
  await expect(page).toHaveURL(new RegExp(`${escapeRegExp(want.hash)}$`));
  const selected = page.getByRole('tab', { selected: true });
  await expect(selected).toHaveCount(1);
  await expect(selected).toContainText(want.activeTab);
  for (const [label, count] of Object.entries(want.badges)) {
    await expect(tabByLabel(page, label).locator('.tab-badge'), `badge ${label}`).toHaveText(count);
  }
  for (const text of want.texts) {
    await expect(page.getByText(text, { exact: true }).first(), `text "${text}"`).toBeVisible();
  }
  for (const { selector, count } of want.counts) {
    await expect(page.locator(selector), `count ${selector}`).toHaveCount(count);
  }
}

for (const viewport of VIEWPORTS) {
  test.describe(`${viewport.name} ${viewport.width}x${viewport.height}`, () => {
    for (const row of CONSOLE_ROWS) {
      test(row.name, async ({ browser }) => {
        const context = await browser.newContext({
          ...CONTEXT_OPTIONS,
          viewport: { width: viewport.width, height: viewport.height },
        });
        try {
          const page = await context.newPage();
          const errors: string[] = [];
          page.on('console', (msg) => {
            if (msg.type() === 'error') errors.push(`console: ${msg.text()}`);
          });
          page.on('pageerror', (err) => errors.push(`pageerror: ${err.message}`));
          const unexpected = await serveFixtures(page);
          await page.clock.setFixedTime(FIXTURE_NOW);

          await page.goto(`/console/${row.start}`);
          for (const step of row.steps) await runStep(page, step);
          await assertExpectation(page, row.expect);

          expect(errors, 'console/page errors').toEqual([]);
          expect(unexpected, 'requests without a fixture').toEqual([]);
        } finally {
          await context.close();
        }
      });
    }
  });
}
