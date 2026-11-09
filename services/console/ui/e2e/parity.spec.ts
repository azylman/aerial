// Runner for the parity table (e2e/parity.ts). For every row x viewport it loads the vanilla and
// the React UI against identical fixtures and a pinned clock, replays the row's steps, asserts the
// row's expectations plus zero console/page errors and zero unexpected API calls on each, then
// pixel-compares the two screenshots with the masked selectors painted out.

import { type Browser, expect, type Page, test } from '@playwright/test';
import { compareShots } from './compare.ts';
import { FIXTURE_NOW } from './constants.ts';
import { serveFixtures } from './fixtures.ts';
import {
  CONTEXT_OPTIONS,
  type Expectation,
  MASKED_SELECTORS,
  MAX_DIFF_PERCENT,
  PARITY_ROWS,
  PENDING_VIEWS,
  type ParityRow,
  type Region,
  STABILIZE_CSS,
  type Step,
  type UiTarget,
  UIS,
  type Viewport,
  VIEWPORTS,
} from './parity.ts';

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
    case 'press':
      await page.keyboard.press(step.key);
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

async function capture(page: Page, region: Region): Promise<Buffer> {
  await page.mouse.move(0, 0);
  // Masks are scoped to the compared region: a masked element elsewhere on the page (e.g. in a
  // vanilla view body while the React view is pending) must not paint into the comparison.
  const scope = region === 'shell' ? page.locator('.hud-header') : page.locator('body');
  const options = {
    animations: 'disabled' as const,
    caret: 'hide' as const,
    scale: 'css' as const,
    mask: MASKED_SELECTORS.map((s) => scope.locator(s)),
    maskColor: '#ff00ff',
    style: STABILIZE_CSS,
  };
  return region === 'shell' ? scope.screenshot(options) : page.screenshot(options);
}

async function runUi(browser: Browser, ui: UiTarget, row: ParityRow, viewport: Viewport): Promise<Buffer> {
  const context = await browser.newContext({ ...CONTEXT_OPTIONS, viewport: { width: viewport.width, height: viewport.height } });
  try {
    const page = await context.newPage();
    const errors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') errors.push(`console: ${msg.text()}`);
    });
    page.on('pageerror', (err) => errors.push(`pageerror: ${err.message}`));
    const unexpected = await serveFixtures(page);
    await page.clock.setFixedTime(FIXTURE_NOW);

    await page.goto(`${ui.path}${row.start}`);
    for (const step of row.steps) await runStep(page, step);
    await assertExpectation(page, row.expect);
    const shot = await capture(page, row.region);

    expect(errors, `${ui.name} console/page errors`).toEqual([]);
    expect(unexpected, `${ui.name} requests without a fixture`).toEqual([]);
    return shot;
  } finally {
    await context.close();
  }
}

const slugify = (s: string): string => s.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');

for (const viewport of VIEWPORTS) {
  test.describe(`${viewport.name} ${viewport.width}x${viewport.height}`, () => {
    for (const row of PARITY_ROWS) {
      const pending = row.requires.filter((v) => PENDING_VIEWS.includes(v));
      test(row.name, async ({ browser }, testInfo) => {
        test.skip(pending.length > 0, `pending views: ${pending.join(', ')}`);
        const vanilla = await runUi(browser, UIS[0] as UiTarget, row, viewport);
        const react = await runUi(browser, UIS[1] as UiTarget, row, viewport);

        const slug = `${viewport.name}-${slugify(row.name)}`;
        const result = compareShots(slug, vanilla, react, viewport.width * viewport.height);
        testInfo.annotations.push({
          type: 'pixel-diff',
          description: `${result.percent.toFixed(3)}% (${result.diffPixels}px; vanilla ${result.sizes.vanilla}, react ${result.sizes.react})`,
        });
        expect(result.sizes.react, 'screenshot size').toBe(result.sizes.vanilla);
        expect(result.percent, `pixel diff, see ${result.files.diff}`).toBeLessThanOrEqual(MAX_DIFF_PERCENT);
      });
    }
  });
}
