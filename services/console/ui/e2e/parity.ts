// THE parity table. One declarative row per scenario drives BOTH UIs (vanilla at /console/,
// React at /console/next/) at both viewports; parity.spec.ts is the only runner. Expected values
// are literals derived from e2e/fixtures/*.json, never computed by UI code.
//
// Feature chunks extend this file in exactly two ways: remove their view from PENDING_VIEWS and
// append rows. MASKED_SELECTORS below is the complete list of allowed visual differences.

import type { BrowserContextOptions } from '@playwright/test';

export type ViewId = 'telemetry' | 'tasks' | 'schedules' | 'memory' | 'builds';

/** Views (and their drawers) the React UI does not ship yet; rows needing them are skipped. */
export const PENDING_VIEWS: readonly ViewId[] = ['builds'];

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
  | { readonly kind: 'press'; readonly key: string };

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
.gitsync-pill > span:first-child {
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
.run-duration-badge {
  display: inline-flex !important; flex: none !important; width: 8em !important; max-width: none !important;
  height: 2em !important; box-sizing: border-box !important; align-items: center !important;
  white-space: nowrap !important; overflow: hidden !important;
}
.schedule-cron-human {
  display: block !important; height: 1.5em !important; white-space: nowrap !important; overflow: hidden !important;
}
.search-clear-btn {
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

const TELEMETRY_COUNTS: readonly CountExpectation[] = [
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
// So each row then clicks the id chip of the last card it shows (the runner's click-by-text
// step; chips have no handler, so this only scrolls far down) and re-selects the active SCHEDULES
// tab: the tab is now off screen, Playwright scrolls it into view, and its position in the
// identical header fixes the final offset. Re-selecting the tab keeps pill and search state.
const settleScroll = (lastChip: string): readonly Step[] => [
  { kind: 'openDrawer', text: lastChip },
  { kind: 'clickTab', tab: 'SCHEDULES' },
];
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
  counts,
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
      counts: [{ selector: '.gitsync-pill.lagging', count: 1 }],
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
    // Opening scrolls the card into view; Home brings both pages back to the top so the blurred
    // backdrop compares the same region of each page.
    steps: [
      { kind: 'openDrawer', text: 'aerial-vision' },
      { kind: 'press', key: 'Home' },
    ],
    expect: {
      hash: '#telemetry',
      activeTab: 'TELEMETRY',
      badges: BADGES_ALL,
      texts: DRAWER_TEXTS,
      counts: [
        // The React drawer opens through a document view transition; wait until it has settled.
        { selector: 'html:not(:active-view-transition)', count: 1 },
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
      ...settleScroll('cron-28'),
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
      ...settleScroll('once-12'),
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
      ...settleScroll('run-050'),
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
      ...settleScroll('run-050'),
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
      ...settleScroll('run-046'),
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
      ...settleScroll('run-050'),
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
];
