// THE parity table. One declarative row per scenario drives BOTH UIs (vanilla at /console/,
// React at /console/next/) at both viewports; parity.spec.ts is the only runner. Expected values
// are literals derived from e2e/fixtures/*.json, never computed by UI code.
//
// Feature chunks extend this file in exactly two ways: remove their view from PENDING_VIEWS and
// append rows. MASKED_SELECTORS below is the complete list of allowed visual differences.

import type { BrowserContextOptions } from '@playwright/test';

export type ViewId = 'telemetry' | 'tasks' | 'schedules' | 'memory' | 'builds';

/** Views (and their drawers) the React UI does not ship yet; rows needing them are skipped. */
export const PENDING_VIEWS: readonly ViewId[] = [];

export interface Viewport {
  readonly name: string;
  readonly width: number;
  readonly height: number;
}

export const VIEWPORTS: readonly Viewport[] = [
  { name: 'desktop', width: 1280, height: 800 },
  { name: 'mobile', width: 390, height: 844 },
];

export interface UiTarget {
  readonly name: 'vanilla' | 'react';
  readonly path: string;
}

export const UIS: readonly UiTarget[] = [
  { name: 'vanilla', path: '/console/' },
  { name: 'react', path: '/console/next/' },
];

/** Both UIs render under identical, deterministic browser settings. */
export const CONTEXT_OPTIONS: BrowserContextOptions = {
  timezoneId: 'UTC',
  locale: 'en-US',
  colorScheme: 'dark',
  deviceScaleFactor: 1,
};

/** Interaction steps as data. Tabs are addressed by their visible label. */
export type Step =
  | { readonly kind: 'clickTab'; readonly tab: string }
  | { readonly kind: 'keyTab'; readonly tab: string; readonly key: 'Enter' | 'Space' }
  | { readonly kind: 'type'; readonly placeholder: string; readonly text: string }
  | { readonly kind: 'pill'; readonly label: string }
  | { readonly kind: 'openDrawer'; readonly text: string }
  | { readonly kind: 'press'; readonly key: string }
  /** Jumps the page to its top-left corner at once (no smooth or keyboard scroll animation). */
  | { readonly kind: 'scrollTop' }
  | { readonly kind: 'scrollBottom' };

export interface CountExpectation {
  readonly selector: string;
  readonly count: number;
}

export interface Expectation {
  /** location.hash after the steps. */
  readonly hash: string;
  /** Visible label of the single selected tab. */
  readonly activeTab: string;
  /** Tab label -> badge text, for every tab that carries a badge. */
  readonly badges: Readonly<Record<string, string>>;
  /** Exact element texts that must be visible. */
  readonly texts: readonly string[];
  /** Selector counts (class names are shared by both UIs). */
  readonly counts: readonly CountExpectation[];
}

/** 'shell' compares the header region only; 'viewport' compares the whole viewport. */
export type Region = 'shell' | 'viewport';

export interface ParityRow {
  readonly name: string;
  /** Hash to load ('' for the bare URL). */
  readonly start: string;
  /** Views that must be built for this row to run (see PENDING_VIEWS). */
  readonly requires: readonly ViewId[];
  readonly steps: readonly Step[];
  readonly expect: Expectation;
  readonly region: Region;
}

/**
 * The only allowed visual differences: glyph boxes where emoji chrome became inline SVG. Each is
 * painted over with the same mask color in both screenshots, and STABILIZE_CSS gives these boxes
 * one fixed size in both UIs so a glyph's advance width cannot shift the text next to it.
 */
export const MASKED_SELECTORS: readonly string[] = [
  '.brand-badge',
  '.tab-icon',
  '.title-icon',
  // Scoped to the open drawer: the vanilla drawer stays laid out (visibility: hidden) while closed.
  '.diagnostic-drawer.active .drawer-icon',
  '.diagnostic-drawer.active .drawer-close',
  '.search-icon',
  '.refresh-icon',
  '.reconnect-icon',
  '.gitsync-pill > span:first-child',
  // Badges and pills whose vanilla text starts or ends with an emoji/status glyph (⚡ ⏳ 🔄 💬 📦
  // 🕒 🚨 ✓ ✕ ○). The glyph sits inside the badge text, so the whole badge box is the glyph region;
  // its text is still asserted through the row expectations.
  '#tasks-count-badge',
  '#tasks-view-badge',
  '#deploy-count-badge',
  '.task-trigger-badge',
  '.task-retry-badge',
  '.task-status-pill',
  '.task-inspect-btn.active',
  '.timer-text',
  '.deploy-repo-badge',
  '.deploy-commit-time',
  '.deploy-stage-badge',
  '.step-status-badge',
  '.matrix-chip',
  '.compact-badge',
  // Schedules: badges, chips and buttons whose vanilla text starts with an emoji (⏱ ⚡ ⏰ 📋 ✓ 🔄
  // ⏳ 🚨), the cron/one-shot subtitle lines (📅 🎯 prefix) and the search clear glyph (×).
  '#schedules-next-trigger',
  '.schedule-countdown-badge',
  '.schedule-type-badge',
  '.schedule-cron-human',
  '.prompt-copy-btn',
  '.run-status-chip',
  '.run-duration-badge',
  '.search-clear-btn',
  // Memory: the fact card gauge (⚡ Nx), age (⏱ Nd AGO) and copy button (📋 COPY).
  '.fact-importance-gauge',
  '.fact-footer > span:first-child',
  '.fact-copy-btn',
  // Builds: status badges (⚡ ⏳ ✓ ✕ ⊘), the duration badge (⏱), chunk PR links (↗) and the
  // instructions disclosure chevron (▸). Drawer parts are scoped to the open drawer, like the
  // drawer chrome above; card parts only while no drawer is open, since masks paint over the
  // drawer backdrop and the card a click scrolled into view sits a few pixels apart in the two
  // pages before STABILIZE_CSS evens out the glyph boxes.
  'body:not(:has(.diagnostic-drawer.active)) .build-card .build-status-badge',
  'body:not(:has(.diagnostic-drawer.active)) .build-duration-badge',
  '.diagnostic-drawer.active .build-status-badge',
  '.diagnostic-drawer.active .chunk-pr-link',
  '.diagnostic-drawer.active .chunk-instructions-chevron',
];

/**
 * Applied to both UIs only while screenshotting: hides the moving scanline, fixes the masked glyph
 * boxes' size (glyph-bearing badges get one fixed box, so an emoji's advance or taller line box
 * cannot shift the content after the badge), and drops UA focus rings, whose colours follow `color-scheme` (the React console
 * declares `dark` per the Task #4 polish; the vanilla page is light-scheme), so a keyboard-focused
 * control would otherwise differ by design.
 *
 * The memory fact-card boxes get a box shorter than their natural line box, so stabilizing can only
 * shrink a card, never grow it. The archive is long and React mounts only the cards near the
 * viewport: a page scrolled to its end stays pinned to the bottom edge when cards shrink (in both
 * UIs), but growth would push the vanilla page's off-screen cards down while React's unmounted rows
 * keep their measured size.
 */
export const STABILIZE_CSS = `
.scanline { display: none !important; }
:focus-visible { outline: none !important; }
.tab-icon, .title-icon, .drawer-icon, .search-icon, .refresh-icon, .reconnect-icon,
.gitsync-pill > span:first-child, .chunk-instructions-chevron {
  display: inline-flex !important; flex: none !important; width: 16px !important; height: 16px !important;
  overflow: hidden !important; align-items: center !important; justify-content: center !important;
}
.drawer-close {
  display: inline-flex !important; width: 32px !important; height: 32px !important;
  padding: 0 !important; overflow: hidden !important;
}
#tasks-count-badge, #tasks-view-badge, #deploy-count-badge, .task-trigger-badge, .task-retry-badge,
.task-status-pill, .task-inspect-btn.active, .timer-text, .deploy-repo-badge, .deploy-commit-time,
.deploy-stage-badge, .step-status-badge, .matrix-chip, .compact-badge,
#schedules-next-trigger, .schedule-countdown-badge, .schedule-type-badge, .prompt-copy-btn, .run-status-chip,
.run-duration-badge, .build-status-badge, .build-duration-badge, .chunk-pr-link {
  display: inline-flex !important; flex: none !important; width: 8em !important; max-width: none !important;
  height: 2em !important; box-sizing: border-box !important; align-items: center !important;
  white-space: nowrap !important; overflow: hidden !important;
}
.schedule-cron-human {
  display: block !important; height: 1.5em !important; white-space: nowrap !important; overflow: hidden !important;
}
.search-clear-btn:not([style*="display: none"]) {
  display: inline-flex !important; width: 24px !important; height: 24px !important; padding: 0 !important;
  overflow: hidden !important;
}
.fact-importance-gauge, .fact-footer > span:first-child, .fact-copy-btn {
  display: inline-flex !important; flex: none !important; width: 8em !important; max-width: none !important;
  height: 1.5em !important; padding-top: 0 !important; padding-bottom: 0 !important; box-sizing: border-box !important;
  align-items: center !important; white-space: nowrap !important; overflow: hidden !important;
}
`;

/** Fail threshold: unmasked differing pixels as a share of the viewport. */
export const MAX_DIFF_PERCENT = 0.5;

// ---- Text parity ----
//
// The pixel gate tolerates a few words' worth of shift, so words running together (a missing
// space between inline spans) can slip under it. Each row therefore also compares the compared
// region's visible innerText in both UIs, after removing only the glyphs React renders as SVG.

/**
 * Emoji and symbol glyphs the vanilla text carries where React renders an SVG icon (the full
 * emoji -> icon table in src/components/Icon.tsx), plus variation selectors.
 */
const SVG_GLYPHS = /[⚡🎯⏰🧠🛠🚀📋🔍⟳🔄↗⬇✓✕×⏱⏳💬📦🟢🟡🚨⚠○⊘🩺🕒📅🌌⚙▾▸\uFE0E\uFE0F]/gu;

/**
 * Items of the one list React windows (the memory archive, src/components/VirtualList.tsx): React
 * mounts only the rows near the viewport while the vanilla grid renders every card, so the text
 * comparison leaves out these items when they sit entirely outside the viewport. Nothing else is
 * clipped, so text at the viewport edge elsewhere is still compared in full.
 */
export const VIRTUALIZED_ITEMS = '.fact-card';

/** Canonical form of a region's innerText for the vanilla/React text comparison. */
export function normalizeParityText(text: string): string {
  return text.replace(SVG_GLYPHS, '').replace(/\s+/g, ' ').trim();
}

const CONTEXT_BEFORE = 20;
const CONTEXT_AFTER = 40;

function wordOffset(words: readonly string[], index: number): number {
  return words.slice(0, index).reduce((offset, word) => offset + word.length + 1, 0);
}

/**
 * Compares two normalized texts word by word. Returns null when equal, otherwise a message naming
 * the first differing word and about 60 characters of context from each side.
 */
export function describeTextMismatch(vanilla: string, react: string): string | null {
  if (vanilla === react) return null;
  const a = vanilla.split(' ');
  const b = react.split(' ');
  let i = 0;
  while (a[i] === b[i]) i++;
  const context = (text: string, words: readonly string[]): string => {
    const at = wordOffset(words, i);
    return JSON.stringify(text.slice(Math.max(0, at - CONTEXT_BEFORE), at + CONTEXT_AFTER));
  };
  return [
    `text differs at word ${i + 1}: vanilla ${JSON.stringify(a[i] ?? '<end>')}, react ${JSON.stringify(b[i] ?? '<end>')}`,
    `  vanilla: ${context(vanilla, a)}`,
    `  react:   ${context(react, b)}`,
  ].join('\n');
}

// ---- Fixture-derived literals ----

/** 15 of 18 services healthy (status.json) -> 83% -> stable. */
const PERMET_STABLE = 'LVL 3 // STABLE';
/** Static header value: status is not polled on schedules/memory/builds deep links. */
const PERMET_STATIC = 'LVL 5 // ACTIVE';

/** status.json: 5 tasks; schedules.json: 28 crons + 12 one-shots; facts.json: 150; builds.json: 4 running + 6 queued. */
const BADGES_ALL = { 'ACTIVE TASKS': '5', SCHEDULES: '40', 'MEMORY ARCHIVE': '150', 'BUILD QUEUE': '10' };
/** Same, before any status poll (deep link to a non-status tab). */
const BADGES_NO_STATUS = { ...BADGES_ALL, 'ACTIVE TASKS': '0' };

const BRAND = ['PERMET COMMAND HUD // TACTICAL GATEWAY', 'PERMET SCORE'];
/** status.json quick_launch_links (all six have a name and url). */
const DOCK = ['🛰️ Console', '📈 Grafana', '📚 Docs', '🔭 Agentsview', '🏠 Homepage', '🏡 Home Assistant'];

const SHELL_COUNTS: readonly CountExpectation[] = [
  { selector: '.view-tab-btn', count: 5 },
  { selector: '.tab-badge', count: 4 },
  { selector: '.dock-chip', count: 6 },
  { selector: '.dock-chip.active', count: 1 },
  { selector: '.ping-dot.custom', count: 1 },
];

/** status.json services: 15 healthy, aerial-vision and aerial-prospera degraded, aerial-kernel-mcp down. */
const SERVICES_HEALTHY = '15 / 18';
/** dep-console-1 (building) is the only full card; its steps are CI, BUILD, PULL, SWAP. */
const PIPELINE_STEPS = ['CI', 'BUILD', 'PULL', 'SWAP'];

/**
 * Every deploy timer started minutes before FIXTURE_NOW (status.json), so none reads 00:00s. The
 * vanilla markup renders 00:00s until its 1s live-timer tick, so waiting for this keeps the text
 * comparison from catching the vanilla page mid-update.
 */
const DEPLOY_TIMERS_TICKED: CountExpectation = { selector: '#telemetry-view .deploy-timer-badge .timer-text:has-text("00:00s")', count: 0 };

const TELEMETRY_COUNTS: readonly CountExpectation[] = [
  DEPLOY_TIMERS_TICKED,
  { selector: '#telemetry-view .service-card', count: 18 },
  { selector: '#telemetry-view .badge.healthy', count: 15 },
  { selector: '#telemetry-view .badge.degraded', count: 2 },
  { selector: '#telemetry-view .badge.down', count: 1 },
  // 6 deployments: index 0 renders full; eri (live, index 1) and every later index render compact.
  { selector: '#telemetry-view .deploy-card', count: 1 },
  { selector: '#telemetry-view .deploy-card-compact', count: 5 },
  { selector: '#telemetry-view .step-panel', count: 4 },
  { selector: '#telemetry-view .step-panel.step-completed', count: 1 },
  { selector: '#telemetry-view .step-panel.step-active', count: 1 },
  { selector: '#telemetry-view .step-panel.step-pending', count: 2 },
  { selector: '#telemetry-view .deploy-timer-badge', count: 4 },
  { selector: '#telemetry-view .section-badge.failed', count: 1 },
  { selector: '#telemetry-view .task-card', count: 5 },
];

/**
 * status.json active_tasks: 3 PROCESSING (one retried once), 2 PENDING; their summaries (the
 * titles a row shows) are "working" and "queued". Scoped to the tasks view: the vanilla page keeps
 * a second copy of the queue in the hidden telemetry panel.
 */
const TASK_COUNTS: readonly CountExpectation[] = [
  { selector: '#tasks-view .task-card', count: 5 },
  { selector: '#tasks-view .task-card.processing', count: 3 },
  { selector: '#tasks-view .task-card.pending', count: 2 },
  { selector: '#tasks-view .task-prompt-box:text-is("> working")', count: 3 },
  { selector: '#tasks-view .task-prompt-box:text-is("> queued")', count: 2 },
  { selector: '#tasks-view .task-author:text-is("pilot")', count: 3 },
  { selector: '#tasks-view .task-author:text-is("scheduler")', count: 2 },
  { selector: '#tasks-view .task-retry-badge', count: 1 },
  { selector: '#tasks-view .deploy-timer-badge', count: 3 },
  { selector: '#tasks-view .task-trigger-badge[data-trigger="cron"]', count: 1 },
  { selector: '#tasks-view .task-trigger-badge[data-trigger="discord"]', count: 2 },
];
const TASK_TEXTS = ['LIVE DISPATCH STREAM', 'RECENT TASK EXECUTION HISTORY', '3 / 4 WORKERS', '3 PROCESSING TURNS', '2 TURNS'];

/** aerial-vision: degraded, 43871s uptime -> 12h 11m. */
const DRAWER_TEXTS = [
  'DIAGNOSTICS // AERIAL-VISION',
  'SERVICE NAME',
  'HEALTH STATUS',
  'UPTIME CONTEXT',
  'DISCOVERY NODE',
  'aerial-net:internal (Docker Bridge)',
  '[TELEMETRY LOG STREAM]',
  '> Service node aerial-vision initialized via Docker Supervisor.',
];

/**
 * schedules.json: 28 crons + 12 one-shots (40 active), 94% success over 50 runs in 24h;
 * schedule-runs.json: 50 runs (45 completed, 3 failed, 1 running, 1 enqueued). Counts use
 * :visible because the vanilla page hides (not removes) the section a pill switches off.
 */
// Rows with steps end with a deterministic scroll. Clicking a pill or filling the search scrolls
// it into view, and that offset depends on what was rendered above it at that instant (the
// vanilla NEXT TRIGGER only fills in on its 1s ticker, which can wrap it onto a second line).
// So each row then scrolls to the bottom of the page and re-selects the active SCHEDULES tab:
// the tab is now off screen, Playwright scrolls it into view, and its position in the identical
// header fixes the final offset. Re-selecting the tab keeps pill and search state.
const settleScroll = (): readonly Step[] => [{ kind: 'scrollBottom' }, { kind: 'clickTab', tab: 'SCHEDULES' }];
const SCHEDULES_SEARCH = "SEARCH SCHEDULES, PROMPTS, TARGET IDs (PRESS '/' TO FOCUS)...";
const SCHEDULE_HEADINGS = ['ACTIVE CRONS', 'PENDING ONCE', 'NEXT TRIGGER', '24H SUCCESS RATE', '94.0%', '50 RUNS LOGGED (24H)'];

const scheduleCounts = (crons: number, oneShots: number, runs: number, marks: number): readonly CountExpectation[] => [
  { selector: '#schedules-view .schedule-card.cron-card:visible', count: crons },
  { selector: '#schedules-view .schedule-card.oneshot-card:visible', count: oneShots },
  { selector: '#schedules-view .run-card:visible', count: runs },
  { selector: '#schedules-view mark.cyber-highlight:visible', count: marks },
  { selector: '#schedules-view .cat-pill.active', count: 1 },
];

const schedulesExpect = (texts: readonly string[], counts: readonly CountExpectation[]): Expectation => ({
  hash: '#schedules',
  activeTab: 'SCHEDULES',
  badges: BADGES_NO_STATUS,
  texts: [...SCHEDULE_HEADINGS, ...texts],
  // The earliest trigger (schedules.json: a cron due 11:58, 2 min before FIXTURE_NOW) reads OVERDUE.
  // The vanilla summary shows NO UPCOMING until its 1s ticker first runs with data, so waiting for
  // the overdue state keeps the text comparison from catching it mid-update.
  counts: [...counts, { selector: '#schedules-next-trigger.countdown-overdue', count: 1 }],
});

/** Grid and feed headings with their badges at the default (ALL, no search) state. */
const SCHEDULES_DEFAULT_TEXTS = [
  'ACTIVE SCHEDULED ROUTINES',
  '40 ACTIVE',
  'EXECUTION RUN AUDIT LOG',
  '50 RUNS',
  'SHOWING 40 / 40 ACTIVE ROUTINES',
  'Morning standup digest 1',
];

/**
 * facts.json: 150 facts, 50 reinforced (2-12x) and 100 base, mean 2.79x. In importance order the
 * first card is NODE #1099 (12x, "... Vanadis queue (#100)"), the first base card NODE #1118 and the
 * last card NODE #1019 ("... Vanadis queue (#20)"). Counts compare the reported totals, never
 * rendered cards: the React archive mounts only the cards near the viewport.
 */
const MEMORY_SEARCH = "SEARCH FACTS OR NODE IDS (PRESS '/' TO FOCUS)...";
const MEMORY_HEADINGS = [
  'TOTAL MEMORY NODES',
  '150',
  '50 REINFORCED',
  'PERMET SYNAPSE STORE',
  'OPTIMAL',
  'SYNCED 12:00:00 PM',
  'AVG REINFORCEMENTS',
  '2.79x',
  'VECTOR EMBEDDING MODEL',
];
// Rows with steps end on one deterministic offset. A pill click can leave the vanilla page
// anywhere (its grid re-renders under the pointer and Playwright scrolls to retry the click), so
// each row presses End, clicks the cards of the last row it shows, last card first, and then
// re-selects the active tab, which Playwright scrolls into view from below at the same place in
// both identical headers. The first click waits until the card exists, is visible and has stopped
// moving: React mounts it only once its window reaches the end, so this also proves the window
// follows the scroll and drops nothing. The two further clicks give the vanilla page (150 live
// cards, slow frames) time to run out an End scroll animation that can start after the first
// click already found its card, which would otherwise carry on past the tab click.
const settleMemory = (lastRow: readonly [string, string, string]): readonly Step[] => [
  { kind: 'press', key: 'End' },
  { kind: 'openDrawer', text: lastRow[2] },
  { kind: 'openDrawer', text: lastRow[0] },
  { kind: 'openDrawer', text: lastRow[1] },
  { kind: 'clickTab', tab: 'MEMORY ARCHIVE' },
];
/** The last row of each list (three columns on desktop; the last three cards on mobile). */
const LAST_ROW_ALL = ['NODE #1128', 'NODE #1080', 'NODE #1019'] as const;
const LAST_ROW_REINFORCED = ['NODE #1102', 'NODE #1057', 'NODE #1069'] as const;
const FIRST_FACT = 'Builds ship through the Vanadis queue (#100)';

const memoryExpect = (texts: readonly string[], activePill: string): Expectation => ({
  hash: '#memory',
  activeTab: 'MEMORY ARCHIVE',
  badges: BADGES_NO_STATUS,
  texts: [...MEMORY_HEADINGS, ...texts],
  counts: [
    { selector: '#memory-view .cat-pill', count: 3 },
    { selector: `#memory-view .cat-pill.active:has-text("${activePill}")`, count: 1 },
  ],
});

/**
 * builds.json: 25 builds. By queue bucket 4 running, 6 queued (build #5 reports `running` but every
 * chunk is still pending) and 15 completed (10 completed, 3 failed, 2 cancelled); 53 of 94 chunks
 * are done. Build #1 (running, chunks merged/merged/in_progress) started 11:10am, 50m before the
 * pinned clock; build #11 ran 1:10am-1:28am. Counts are scoped to the builds view: the vanilla page
 * keeps every other view in the DOM.
 */
const BUILDS_SEARCH = "SEARCH BUILDS, REPOS, OBJECTIVES (PRESS '/' TO FOCUS)...";
const BUILD_HEADINGS = [
  'RUNNING BUILDS',
  'ACTIVE PIPELINES',
  'QUEUED BUILDS',
  'PENDING DISPATCH',
  'COMPLETED BUILDS',
  'FINISHED RUNS',
  'CHUNK PROGRESS',
  '53/94',
  'COMPLETED / TOTAL',
];
const BUILD_METRIC_COUNTS: readonly CountExpectation[] = [
  { selector: '#builds-running-count:text-is("4")', count: 1 },
  { selector: '#builds-queued-count:text-is("6")', count: 1 },
  { selector: '#builds-completed-count:text-is("15")', count: 1 },
  { selector: '#builds-view .cat-pill', count: 4 },
  { selector: '#builds-view .cat-pill:has-text("ALL") .pill-count:text-is("25")', count: 1 },
  { selector: '#builds-view .cat-pill:has-text("RUNNING") .pill-count:text-is("4")', count: 1 },
  { selector: '#builds-view .cat-pill:has-text("QUEUED") .pill-count:text-is("6")', count: 1 },
  { selector: '#builds-view .cat-pill:has-text("COMPLETED") .pill-count:text-is("15")', count: 1 },
];

/** Card counts per status badge: running, queued, completed, failed, cancelled. */
const buildCards = (running: number, queued: number, completed: number, failed: number, cancelled: number): readonly CountExpectation[] => [
  { selector: '#builds-view .build-card', count: running + queued + completed + failed + cancelled },
  { selector: '#builds-view .build-status-badge.status-running', count: running },
  { selector: '#builds-view .build-status-badge.status-queued', count: queued },
  { selector: '#builds-view .build-status-badge.status-completed', count: completed },
  { selector: '#builds-view .build-status-badge.status-failed', count: failed },
  { selector: '#builds-view .build-status-badge.status-cancelled', count: cancelled },
];

const buildsExpect = (
  texts: readonly string[],
  activePill: string,
  cards: readonly CountExpectation[],
  extra: readonly CountExpectation[] = [],
): Expectation => ({
  hash: '#builds',
  activeTab: 'BUILD QUEUE',
  badges: BADGES_NO_STATUS,
  texts: [...BUILD_HEADINGS, ...texts],
  counts: [
    ...BUILD_METRIC_COUNTS,
    { selector: `#builds-view .cat-pill.active:has-text("${activePill}")`, count: 1 },
    ...cards,
    ...extra,
  ],
});

/** The default queue: build #1's card (stats, start, live duration) and a finished card's end time. */
const BUILDS_DEFAULT_TEXTS = [
  'SHOWING 25 / 25 BUILDS',
  'Console React rewrite #1',
  'START: 11/8 11:10am',
  '2 / 3 CHUNKS',
  '67%',
  'Console React rewrite #11',
  'END: 1:28am',
];
const BUILDS_DEFAULT_COUNTS: readonly CountExpectation[] = [
  { selector: '#builds-view .build-card[data-build-id="build-1794100000-c046de69"] .build-duration-badge:has-text("50m 0s")', count: 1 },
  { selector: '#builds-view .build-card[data-build-id="build-1794111110-2f3c2784"] .build-duration-badge:has-text("18m 0s")', count: 1 },
];
const ALL_BUILD_CARDS = buildCards(4, 6, 10, 3, 2);

/**
 * Rows with steps end on one deterministic offset. Playwright scrolls an element differently on
 * each retry of a click (end, center, start; horizontally too, the mobile page is a little wider
 * than the viewport) and the vanilla page needs retries while its panel fades in, so a pill click
 * or a fill can leave the two pages at different offsets. Each row therefore clicks its results
 * label (`label`, static text: this also blurs the search box), then jumps both pages to the very
 * top. (Pressing Home and re-focusing a header tab raced the animated keyboard scroll and left
 * the pages at different offsets on some runs.)
 */
const settleBuilds = (label: string): readonly Step[] => [
  { kind: 'openDrawer', text: label },
  { kind: 'scrollTop' },
];

/**
 * Opens build #1 from its card, waits for the fetched detail (the header switches from the id to
 * the title) and expands the first chunk's instructions from the keyboard: clicking the header
 * sets the focus starting point in both drawers, then Tab walks the close button, the first
 * chunk's PR link and its INSTRUCTIONS disclosure, and Enter opens it. An open drawer pins the
 * page (the React drawer contains overscroll), so the row first settles both pages at the top
 * (see settleBuilds); the card click then scrolls both the same way on its first attempt.
 */
const OPEN_BUILD_ONE: readonly Step[] = [
  ...settleBuilds('SHOWING 25 / 25 BUILDS'),
  { kind: 'openDrawer', text: 'Console React rewrite #1' },
  { kind: 'openDrawer', text: 'BUILD // CONSOLE REACT REWRITE #1' },
  { kind: 'press', key: 'Tab' },
  { kind: 'press', key: 'Tab' },
  { kind: 'press', key: 'Tab' },
  { kind: 'press', key: 'Enter' },
];

/**
 * build-details.json for build #1: tier 1, three chunks (two merged with PRs #1500 and #1501 in
 * azylman/aerial and azylman/aerial-config, one in progress in azylman/mirrormere), each with
 * markdown instructions: an h2, a bullet list with bold and inline code, and a fenced code block.
 */
const BUILD_DRAWER_TEXTS = [
  'BUILD // CONSOLE REACT REWRITE #1',
  'TIER 1',
  'build-1794100000-c046de69',
  'OBJECTIVE',
  'Ship console react rewrite with tests and docs.',
  'SHARED BUILD CONTEXT',
  'Layout: services/console/ui is a standalone Vite project.',
  '2 / 3 CHUNKS COMPLETED',
  'ORDERED BUILD CHUNKS (3)',
  'Scaffold for Console React rewrite',
  'Port logic for Console React rewrite',
  'Shell for Console React rewrite',
  'MERGED',
  'IN_PROGRESS',
  'low',
  'azylman/mirrormere',
  'Objective',
  'Implement chunk 1.',
  'keep the budget',
  'add tests',
  'const ok = true;',
  'TARGET FILES',
  'services/console/ui/src/file1.ts',
  'TARGET SYMBOLS',
  'symbol1',
  'ACCEPTANCE CRITERIA',
  'Coverage stays at 95%',
];
const BUILD_DRAWER_COUNTS: readonly CountExpectation[] = [
  { selector: '.diagnostic-drawer.active', count: 1 },
  { selector: '.diagnostic-drawer.active .chunk-card', count: 3 },
  { selector: '.diagnostic-drawer.active .chunk-status-badge.chunk-status-merged', count: 2 },
  { selector: '.diagnostic-drawer.active .chunk-status-badge.chunk-status-in_progress', count: 1 },
  { selector: '.diagnostic-drawer.active .chunk-pr-link', count: 2 },
  { selector: '.diagnostic-drawer.active .chunk-pr-link[href="https://github.com/azylman/aerial/pull/1500"]', count: 1 },
  { selector: '.diagnostic-drawer.active .chunk-pr-link[href="https://github.com/azylman/aerial-config/pull/1501"]', count: 1 },
  { selector: '.diagnostic-drawer.active .chunk-instructions-details', count: 3 },
  { selector: '.diagnostic-drawer.active .chunk-instructions-details[open]', count: 1 },
  { selector: '.diagnostic-drawer.active .chunk-instructions-details[open] h2.md-heading', count: 1 },
  { selector: '.diagnostic-drawer.active .chunk-instructions-details[open] ul.md-list > li', count: 2 },
  { selector: '.diagnostic-drawer.active .chunk-instructions-details[open] pre.md-code-block', count: 1 },
  { selector: '.diagnostic-drawer.active .chunk-tag.file-tag', count: 6 },
  { selector: '.diagnostic-drawer.active .chunk-tag.symbol-tag', count: 3 },
];

export const PARITY_ROWS: readonly ParityRow[] = [
  {
    name: 'shell: bare URL lands on telemetry',
    start: '',
    requires: [],
    steps: [],
    expect: { hash: '#telemetry', activeTab: 'TELEMETRY', badges: BADGES_ALL, texts: [...BRAND, PERMET_STABLE, ...DOCK], counts: SHELL_COUNTS },
    region: 'shell',
  },
  {
    name: 'shell: clicking a tab switches and updates the hash',
    start: '#telemetry',
    requires: [],
    steps: [{ kind: 'clickTab', tab: 'BUILD QUEUE' }],
    expect: { hash: '#builds', activeTab: 'BUILD QUEUE', badges: BADGES_ALL, texts: [...BRAND, PERMET_STABLE, ...DOCK], counts: SHELL_COUNTS },
    region: 'shell',
  },
  {
    name: 'shell: keyboard activation of a tab',
    start: '#tasks',
    requires: [],
    steps: [{ kind: 'keyTab', tab: 'MEMORY ARCHIVE', key: 'Enter' }],
    expect: { hash: '#memory', activeTab: 'MEMORY ARCHIVE', badges: BADGES_ALL, texts: [...BRAND, PERMET_STABLE, ...DOCK], counts: SHELL_COUNTS },
    region: 'shell',
  },
  {
    name: 'shell: schedules deep link never polls status',
    start: '#schedules',
    requires: [],
    steps: [],
    expect: {
      hash: '#schedules',
      activeTab: 'SCHEDULES',
      badges: BADGES_NO_STATUS,
      texts: [...BRAND, PERMET_STATIC],
      counts: [
        { selector: '.view-tab-btn', count: 5 },
        { selector: '.dock-chip', count: 0 },
      ],
    },
    region: 'shell',
  },
  {
    name: 'telemetry: git-sync badge shows the lag',
    start: '#telemetry',
    requires: ['telemetry'],
    steps: [],
    expect: {
      hash: '#telemetry',
      activeTab: 'TELEMETRY',
      badges: BADGES_ALL,
      texts: ['GIT SYNC: BEHIND (Δ 1m 15s)'],
      counts: [{ selector: '.gitsync-pill.lagging', count: 1 }, DEPLOY_TIMERS_TICKED],
    },
    region: 'viewport',
  },
  {
    name: 'telemetry: services grid, pipeline and task queue',
    start: '#telemetry',
    requires: ['telemetry'],
    steps: [],
    expect: {
      hash: '#telemetry',
      activeTab: 'TELEMETRY',
      badges: BADGES_ALL,
      texts: [
        'LIVE AGENT EXECUTION QUEUE',
        'DEPLOYMENT PIPELINE',
        'ACTIVE STACK COMPONENTS',
        SERVICES_HEALTHY,
        'aerial-vision',
        'aerial-kernel-mcp',
        'DEGRADED',
        'DOWN',
        ...PIPELINE_STEPS,
        'STARTED: 11:57:00 AM',
        '"feat(console/ui): app shell and parity suite"',
        'eri • fix(eri): retry transient ingress errors',
      ],
      counts: TELEMETRY_COUNTS,
    },
    region: 'viewport',
  },
  {
    name: 'tasks: live dispatch stream lists every active task',
    start: '#tasks',
    requires: ['tasks'],
    steps: [],
    expect: {
      hash: '#tasks',
      activeTab: 'ACTIVE TASKS',
      badges: BADGES_ALL,
      texts: TASK_TEXTS,
      counts: TASK_COUNTS,
    },
    region: 'viewport',
  },
  {
    name: 'telemetry: clicking a degraded service opens its diagnostics drawer',
    start: '#telemetry',
    requires: ['telemetry'],
    // Opening scrolls the card into view; an instant jump brings both pages back to the top so the
    // blurred backdrop compares the same region of each page (Home's animated keyboard scroll
    // raced the screenshot, see settleBuilds).
    steps: [
      { kind: 'openDrawer', text: 'aerial-vision' },
      { kind: 'scrollTop' },
    ],
    expect: {
      hash: '#telemetry',
      activeTab: 'TELEMETRY',
      badges: BADGES_ALL,
      texts: DRAWER_TEXTS,
      counts: [
        { selector: '.diagnostic-drawer.active', count: 1 },
        { selector: '.diagnostic-drawer.active .diag-item', count: 4 },
      ],
    },
    region: 'viewport',
  },
  {
    name: 'schedules: default view lists crons, one-shots and the run feed',
    start: '#schedules',
    requires: ['schedules'],
    steps: [],
    expect: schedulesExpect(SCHEDULES_DEFAULT_TEXTS, [
      ...scheduleCounts(28, 12, 50, 0),
      { selector: '#schedules-view .run-card.run-failed', count: 3 },
      { selector: '#schedules-view .run-card.run-running', count: 1 },
      { selector: '#schedules-view .run-card.run-enqueued', count: 1 },
      { selector: '#schedules-view .pulse-dot.healthy', count: 40 },
    ]),
    region: 'viewport',
  },
  {
    name: 'schedules: cron routines pill',
    start: '#schedules',
    requires: ['schedules'],
    steps: [
      { kind: 'pill', label: 'CRON ROUTINES' },
      ...settleScroll(),
    ],
    expect: schedulesExpect(['SHOWING 28 / 40 ACTIVE ROUTINES', '40 ACTIVE'], scheduleCounts(28, 0, 0, 0)),
    region: 'viewport',
  },
  {
    name: 'schedules: one-shot pill',
    start: '#schedules',
    requires: ['schedules'],
    steps: [
      { kind: 'pill', label: 'ONE-SHOT' },
      ...settleScroll(),
    ],
    expect: schedulesExpect(
      ['SHOWING 12 / 40 ACTIVE ROUTINES', 'ONE-TIME REMINDER', 'Reminder 1: water the basil'],
      scheduleCounts(0, 12, 0, 0),
    ),
    region: 'viewport',
  },
  {
    name: 'schedules: run audit log pill',
    start: '#schedules',
    requires: ['schedules'],
    steps: [
      { kind: 'pill', label: 'RUN AUDIT LOG' },
      ...settleScroll(),
    ],
    expect: schedulesExpect(['EXECUTION RUN AUDIT LOG', '50 RUNS'], scheduleCounts(0, 0, 50, 0)),
    region: 'viewport',
  },
  {
    name: 'schedules: all pill after another pill',
    start: '#schedules',
    requires: ['schedules'],
    steps: [
      { kind: 'pill', label: 'ONE-SHOT' },
      { kind: 'pill', label: 'ALL' },
      ...settleScroll(),
    ],
    expect: schedulesExpect(SCHEDULES_DEFAULT_TEXTS, scheduleCounts(28, 12, 50, 0)),
    region: 'viewport',
  },
  {
    // "plant" matches the 4 "Plant watering N" crons (title + prompt: 8 marks) and 6 of their runs
    // (title + prompt: 12 marks).
    name: 'schedules: search narrows cards and runs and highlights matches',
    start: '#schedules',
    requires: ['schedules'],
    steps: [
      { kind: 'type', placeholder: SCHEDULES_SEARCH, text: 'plant' },
      ...settleScroll(),
    ],
    expect: schedulesExpect(['SHOWING 4 / 40 ACTIVE ROUTINES', '6 RUNS', 'Plant'], scheduleCounts(4, 0, 6, 20)),
    region: 'viewport',
  },
  {
    name: 'schedules: Escape clears the search',
    start: '#schedules',
    requires: ['schedules'],
    steps: [
      { kind: 'type', placeholder: SCHEDULES_SEARCH, text: 'plant' },
      { kind: 'press', key: 'Escape' },
      ...settleScroll(),
    ],
    expect: schedulesExpect(SCHEDULES_DEFAULT_TEXTS, scheduleCounts(28, 12, 50, 0)),
    region: 'viewport',
  },
  {
    name: 'memory: default view shows metrics and the most reinforced facts first',
    start: '#memory',
    requires: ['memory'],
    steps: [],
    expect: memoryExpect(['SHOWING 150 / 150 NODES', 'NODE #1099', FIRST_FACT, 'NODE #1027'], 'ALL'),
    region: 'viewport',
  },
  {
    name: 'memory: reinforced pill',
    start: '#memory',
    requires: ['memory'],
    steps: [{ kind: 'pill', label: 'REINFORCED' }, ...settleMemory(LAST_ROW_REINFORCED)],
    expect: memoryExpect(['SHOWING 50 / 150 NODES', 'NODE #1099', FIRST_FACT], 'REINFORCED'),
    region: 'viewport',
  },
  {
    name: 'memory: base pill',
    start: '#memory',
    requires: ['memory'],
    // The base list ends on the same three cards as the full list.
    steps: [{ kind: 'pill', label: 'BASE' }, ...settleMemory(LAST_ROW_ALL)],
    expect: memoryExpect(['SHOWING 100 / 150 NODES', 'NODE #1118', 'Standup digest should stay under 200 words (#119)'], 'BASE'),
    region: 'viewport',
  },
  {
    name: 'memory: all pill after another pill',
    start: '#memory',
    requires: ['memory'],
    steps: [
      { kind: 'pill', label: 'BASE' },
      // With its count: the hidden vanilla schedules view also has an "ALL" pill, earlier in the page.
      { kind: 'pill', label: 'ALL 150' },
      ...settleMemory(LAST_ROW_ALL),
    ],
    expect: memoryExpect(['SHOWING 150 / 150 NODES', 'NODE #1099', FIRST_FACT], 'ALL'),
    region: 'viewport',
  },
  {
    // "oat milk" matches the 15 "Prefers oat milk in lattes" facts, NODE #1030 (12x) first.
    name: 'memory: search narrows the archive and highlights matches',
    start: '#memory',
    requires: ['memory'],
    // No keyboard scroll here: clicking the results count (below the fold on mobile) scrolls both
    // pages to the same place and moves focus out of the input, then the tab brings both to the top.
    steps: [
      { kind: 'type', placeholder: MEMORY_SEARCH, text: 'oat milk' },
      { kind: 'openDrawer', text: 'SHOWING 15 / 150 NODES' },
      { kind: 'clickTab', tab: 'MEMORY ARCHIVE' },
    ],
    expect: memoryExpect(['SHOWING 15 / 150 NODES', 'NODE #1030', 'oat milk'], 'ALL'),
    region: 'viewport',
  },
  {
    name: 'memory: Escape clears the search',
    start: '#memory',
    requires: ['memory'],
    steps: [
      { kind: 'type', placeholder: MEMORY_SEARCH, text: 'oat milk' },
      { kind: 'press', key: 'Escape' },
      // Waits for the full list (React renders the cleared search as a deferred update).
      { kind: 'openDrawer', text: 'SHOWING 150 / 150 NODES' },
      ...settleMemory(LAST_ROW_ALL),
    ],
    expect: memoryExpect(['SHOWING 150 / 150 NODES', 'NODE #1099', FIRST_FACT], 'ALL'),
    region: 'viewport',
  },
  {
    // The last card is reachable (see settleMemory), and scrolling back up re-mounts the first
    // cards in place. The row is compared at the top: at the very bottom the React page's height
    // still includes estimates for rows it never mounted, so its fractional height (and with it the
    // sub-pixel phase of every row) cannot match the vanilla page's to the pixel even though the
    // layout is the same.
    name: 'memory: scrolling to the bottom reaches the last fact and back',
    start: '#memory',
    requires: ['memory'],
    // Clicking the results count waits for the list before End.
    steps: [{ kind: 'openDrawer', text: 'SHOWING 150 / 150 NODES' }, ...settleMemory(LAST_ROW_ALL)],
    expect: memoryExpect(['SHOWING 150 / 150 NODES', 'NODE #1099', FIRST_FACT], 'ALL'),
    region: 'viewport',
  },
  {
    name: 'builds: default view shows metrics, pills and every build card',
    start: '#builds',
    requires: ['builds'],
    steps: [],
    expect: buildsExpect(BUILDS_DEFAULT_TEXTS, 'ALL', ALL_BUILD_CARDS, BUILDS_DEFAULT_COUNTS),
    region: 'viewport',
  },
  {
    name: 'builds: running pill',
    start: '#builds',
    requires: ['builds'],
    steps: [{ kind: 'pill', label: 'RUNNING' }, ...settleBuilds('SHOWING 4 / 25 BUILDS')],
    expect: buildsExpect(['SHOWING 4 / 25 BUILDS', 'Console React rewrite #1', 'Memory decay tuning #4'], 'RUNNING', buildCards(4, 0, 0, 0, 0)),
    region: 'viewport',
  },
  {
    name: 'builds: queued pill',
    start: '#builds',
    requires: ['builds'],
    steps: [{ kind: 'pill', label: 'QUEUED' }, ...settleBuilds('SHOWING 6 / 25 BUILDS')],
    expect: buildsExpect(['SHOWING 6 / 25 BUILDS', 'Synapse webhook hardening #5', '0 / 3 CHUNKS'], 'QUEUED', buildCards(0, 6, 0, 0, 0)),
    region: 'viewport',
  },
  {
    name: 'builds: completed pill includes failed and cancelled builds',
    start: '#builds',
    requires: ['builds'],
    steps: [{ kind: 'pill', label: 'COMPLETED' }, ...settleBuilds('SHOWING 15 / 25 BUILDS')],
    expect: buildsExpect(['SHOWING 15 / 25 BUILDS', 'Console React rewrite #11', 'END: 1:28am'], 'COMPLETED', buildCards(0, 0, 10, 3, 2)),
    region: 'viewport',
  },
  {
    name: 'builds: all pill after another pill',
    start: '#builds',
    requires: ['builds'],
    // With its count: the hidden vanilla schedules and memory views also have an "ALL" pill.
    steps: [
      { kind: 'pill', label: 'QUEUED' },
      { kind: 'pill', label: 'ALL 25' },
      ...settleBuilds('SHOWING 25 / 25 BUILDS'),
    ],
    expect: buildsExpect(BUILDS_DEFAULT_TEXTS, 'ALL', ALL_BUILD_CARDS),
    region: 'viewport',
  },
  {
    // "kiosk" matches the five "Kiosk widget refresh #N" builds (title and objective).
    name: 'builds: search narrows the queue',
    start: '#builds',
    requires: ['builds'],
    steps: [{ kind: 'type', placeholder: BUILDS_SEARCH, text: 'kiosk' }, ...settleBuilds('SHOWING 5 / 25 BUILDS')],
    expect: buildsExpect(['SHOWING 5 / 25 BUILDS', 'Kiosk widget refresh #3', 'Kiosk widget refresh #23'], 'ALL', buildCards(1, 1, 2, 1, 0)),
    region: 'viewport',
  },
  {
    // "sidecars" only appears in chunk repos (azylman/aerial-sidecars), so this proves repo search.
    name: 'builds: search matches chunk repos and Escape clears it',
    start: '#builds',
    requires: ['builds'],
    steps: [
      // Clicking the results count waits for the 19 matches; filling again re-focuses the input.
      { kind: 'type', placeholder: BUILDS_SEARCH, text: 'sidecars' },
      { kind: 'openDrawer', text: 'SHOWING 19 / 25 BUILDS' },
      { kind: 'type', placeholder: BUILDS_SEARCH, text: 'sidecars' },
      { kind: 'press', key: 'Escape' },
      // Waits for the full list (React renders the cleared search as a deferred update).
      ...settleBuilds('SHOWING 25 / 25 BUILDS'),
    ],
    expect: buildsExpect(BUILDS_DEFAULT_TEXTS, 'ALL', ALL_BUILD_CARDS),
    region: 'viewport',
  },
  {
    name: 'builds: clicking a multi-chunk build opens its detail drawer with rendered markdown',
    start: '#builds',
    requires: ['builds'],
    steps: OPEN_BUILD_ONE,
    expect: buildsExpect(BUILD_DRAWER_TEXTS, 'ALL', ALL_BUILD_CARDS, BUILD_DRAWER_COUNTS),
    region: 'viewport',
  },
  {
    name: 'builds: Escape closes the build drawer',
    start: '#builds',
    requires: ['builds'],
    steps: [...OPEN_BUILD_ONE, { kind: 'press', key: 'Escape' }],
    expect: buildsExpect(BUILDS_DEFAULT_TEXTS, 'ALL', ALL_BUILD_CARDS, [
      { selector: '.diagnostic-drawer.active', count: 0 },
    ]),
    // Closing leaves each page wherever its card click scrolled it (the vanilla page's click may
    // need retries while the drawer fades out), so this row compares the header: any drawer or
    // backdrop left behind would paint over it.
    region: 'shell',
  },
  {
    name: 'walk: every tab in order with no console or page errors',
    start: '',
    requires: ['telemetry', 'tasks', 'schedules', 'memory', 'builds'],
    steps: [
      { kind: 'clickTab', tab: 'ACTIVE TASKS' },
      { kind: 'clickTab', tab: 'SCHEDULES' },
      { kind: 'clickTab', tab: 'MEMORY ARCHIVE' },
      { kind: 'clickTab', tab: 'BUILD QUEUE' },
    ],
    expect: { ...buildsExpect(BUILDS_DEFAULT_TEXTS, 'ALL', ALL_BUILD_CARDS), badges: BADGES_ALL },
    region: 'viewport',
  },
];
