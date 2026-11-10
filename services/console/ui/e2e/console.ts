// The console e2e table. One declarative row per scenario drives the React console at /console/
// at both viewports; console.spec.ts is the only runner. Expected values are literals derived from
// e2e/fixtures/*.json, never computed by UI code.

import type { BrowserContextOptions } from '@playwright/test';

export interface Viewport {
  readonly name: string;
  readonly width: number;
  readonly height: number;
}

export const VIEWPORTS: readonly Viewport[] = [
  { name: 'desktop', width: 1280, height: 800 },
  { name: 'mobile', width: 390, height: 844 },
];

/** Every run renders under identical, deterministic browser settings. */
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
  /** Selector counts. */
  readonly counts: readonly CountExpectation[];
}

export interface ConsoleRow {
  readonly name: string;
  /** Hash to load ('' for the bare URL). */
  readonly start: string;
  readonly steps: readonly Step[];
  readonly expect: Expectation;
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

/** Every deploy timer started minutes before FIXTURE_NOW (status.json), so none reads 00:00s. */
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
 * titles a row shows) are "working" and "queued". Scoped to the tasks view: telemetry renders
 * the same queue.
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
 * :visible so only the sections a pill leaves on screen count.
 */
// Rows with steps end by scrolling to the bottom and re-selecting the active SCHEDULES tab, which
// proves re-selecting a tab keeps pill and search state.
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
// Rows with steps press End, click the cards of the last row the list shows, last card first, and
// then re-select the active tab. The first click waits until the card exists, is visible and has
// stopped moving: the archive mounts it only once its window reaches the end, so this proves the
// window follows the scroll and drops nothing.
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
 * pinned clock; build #11 ran 1:10am-1:28am. Counts are scoped to the builds view.
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
 * Rows with steps end by clicking their results label (`label`, static text: this waits for the
 * filtered list and blurs the search box), then jump the page to the very top.
 */
const settleBuilds = (label: string): readonly Step[] => [
  { kind: 'openDrawer', text: label },
  { kind: 'scrollTop' },
];

/**
 * Opens build #1 from its card, waits for the fetched detail (the header switches from the id to
 * the title) and expands the first chunk's instructions from the keyboard: clicking the header
 * sets the focus starting point in the drawer, then Tab walks the close button, the first chunk's
 * PR link and its INSTRUCTIONS disclosure, and Enter opens it. An open drawer pins the page (it
 * contains overscroll), so the row first settles the page at the top (see settleBuilds).
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

export const CONSOLE_ROWS: readonly ConsoleRow[] = [
  {
    name: 'shell: bare URL lands on telemetry',
    start: '',
    steps: [],
    expect: { hash: '#telemetry', activeTab: 'TELEMETRY', badges: BADGES_ALL, texts: [...BRAND, PERMET_STABLE, ...DOCK], counts: SHELL_COUNTS },
  },
  {
    name: 'shell: clicking a tab switches and updates the hash',
    start: '#telemetry',
    steps: [{ kind: 'clickTab', tab: 'BUILD QUEUE' }],
    expect: { hash: '#builds', activeTab: 'BUILD QUEUE', badges: BADGES_ALL, texts: [...BRAND, PERMET_STABLE, ...DOCK], counts: SHELL_COUNTS },
  },
  {
    name: 'shell: keyboard activation of a tab',
    start: '#tasks',
    steps: [{ kind: 'keyTab', tab: 'MEMORY ARCHIVE', key: 'Enter' }],
    expect: { hash: '#memory', activeTab: 'MEMORY ARCHIVE', badges: BADGES_ALL, texts: [...BRAND, PERMET_STABLE, ...DOCK], counts: SHELL_COUNTS },
  },
  {
    name: 'shell: schedules deep link never polls status',
    start: '#schedules',
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
  },
  {
    name: 'telemetry: git-sync badge shows the lag',
    start: '#telemetry',
    steps: [],
    expect: {
      hash: '#telemetry',
      activeTab: 'TELEMETRY',
      badges: BADGES_ALL,
      texts: ['GIT SYNC: BEHIND (Δ 1m 15s)'],
      counts: [{ selector: '.gitsync-pill.lagging', count: 1 }, DEPLOY_TIMERS_TICKED],
    },
  },
  {
    name: 'telemetry: services grid, pipeline and task queue',
    start: '#telemetry',
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
  },
  {
    name: 'tasks: live dispatch stream lists every active task',
    start: '#tasks',
    steps: [],
    expect: {
      hash: '#tasks',
      activeTab: 'ACTIVE TASKS',
      badges: BADGES_ALL,
      texts: TASK_TEXTS,
      counts: TASK_COUNTS,
    },
  },
  {
    name: 'telemetry: clicking a degraded service opens its diagnostics drawer',
    start: '#telemetry',
    // Opening scrolls the card into view; an instant jump brings the page back to the top.
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
  },
  {
    name: 'schedules: default view lists crons, one-shots and the run feed',
    start: '#schedules',
    steps: [],
    expect: schedulesExpect(SCHEDULES_DEFAULT_TEXTS, [
      ...scheduleCounts(28, 12, 50, 0),
      { selector: '#schedules-view .run-card.run-failed', count: 3 },
      { selector: '#schedules-view .run-card.run-running', count: 1 },
      { selector: '#schedules-view .run-card.run-enqueued', count: 1 },
      { selector: '#schedules-view .pulse-dot.healthy', count: 40 },
    ]),
  },
  {
    name: 'schedules: cron routines pill',
    start: '#schedules',
    steps: [
      { kind: 'pill', label: 'CRON ROUTINES' },
      ...settleScroll(),
    ],
    expect: schedulesExpect(['SHOWING 28 / 40 ACTIVE ROUTINES', '40 ACTIVE'], scheduleCounts(28, 0, 0, 0)),
  },
  {
    name: 'schedules: one-shot pill',
    start: '#schedules',
    steps: [
      { kind: 'pill', label: 'ONE-SHOT' },
      ...settleScroll(),
    ],
    expect: schedulesExpect(
      ['SHOWING 12 / 40 ACTIVE ROUTINES', 'ONE-TIME REMINDER', 'Reminder 1: water the basil'],
      scheduleCounts(0, 12, 0, 0),
    ),
  },
  {
    name: 'schedules: run audit log pill',
    start: '#schedules',
    steps: [
      { kind: 'pill', label: 'RUN AUDIT LOG' },
      ...settleScroll(),
    ],
    expect: schedulesExpect(['EXECUTION RUN AUDIT LOG', '50 RUNS'], scheduleCounts(0, 0, 50, 0)),
  },
  {
    name: 'schedules: all pill after another pill',
    start: '#schedules',
    steps: [
      { kind: 'pill', label: 'ONE-SHOT' },
      { kind: 'pill', label: 'ALL' },
      ...settleScroll(),
    ],
    expect: schedulesExpect(SCHEDULES_DEFAULT_TEXTS, scheduleCounts(28, 12, 50, 0)),
  },
  {
    // "plant" matches the 4 "Plant watering N" crons (title + prompt: 8 marks) and 6 of their runs
    // (title + prompt: 12 marks).
    name: 'schedules: search narrows cards and runs and highlights matches',
    start: '#schedules',
    steps: [
      { kind: 'type', placeholder: SCHEDULES_SEARCH, text: 'plant' },
      ...settleScroll(),
    ],
    expect: schedulesExpect(['SHOWING 4 / 40 ACTIVE ROUTINES', '6 RUNS', 'Plant'], scheduleCounts(4, 0, 6, 20)),
  },
  {
    name: 'schedules: Escape clears the search',
    start: '#schedules',
    steps: [
      { kind: 'type', placeholder: SCHEDULES_SEARCH, text: 'plant' },
      { kind: 'press', key: 'Escape' },
      ...settleScroll(),
    ],
    expect: schedulesExpect(SCHEDULES_DEFAULT_TEXTS, scheduleCounts(28, 12, 50, 0)),
  },
  {
    name: 'memory: default view shows metrics and the most reinforced facts first',
    start: '#memory',
    steps: [],
    expect: memoryExpect(['SHOWING 150 / 150 NODES', 'NODE #1099', FIRST_FACT, 'NODE #1027'], 'ALL'),
  },
  {
    name: 'memory: reinforced pill',
    start: '#memory',
    steps: [{ kind: 'pill', label: 'REINFORCED' }, ...settleMemory(LAST_ROW_REINFORCED)],
    expect: memoryExpect(['SHOWING 50 / 150 NODES', 'NODE #1099', FIRST_FACT], 'REINFORCED'),
  },
  {
    name: 'memory: base pill',
    start: '#memory',
    // The base list ends on the same three cards as the full list.
    steps: [{ kind: 'pill', label: 'BASE' }, ...settleMemory(LAST_ROW_ALL)],
    expect: memoryExpect(['SHOWING 100 / 150 NODES', 'NODE #1118', 'Standup digest should stay under 200 words (#119)'], 'BASE'),
  },
  {
    name: 'memory: all pill after another pill',
    start: '#memory',
    steps: [
      { kind: 'pill', label: 'BASE' },
      // With its count, so the label names exactly this pill.
      { kind: 'pill', label: 'ALL 150' },
      ...settleMemory(LAST_ROW_ALL),
    ],
    expect: memoryExpect(['SHOWING 150 / 150 NODES', 'NODE #1099', FIRST_FACT], 'ALL'),
  },
  {
    // "oat milk" matches the 15 "Prefers oat milk in lattes" facts, NODE #1030 (12x) first.
    name: 'memory: search narrows the archive and highlights matches',
    start: '#memory',
    // Clicking the results count (below the fold on mobile) moves focus out of the input, then the
    // tab brings the page back to the top.
    steps: [
      { kind: 'type', placeholder: MEMORY_SEARCH, text: 'oat milk' },
      { kind: 'openDrawer', text: 'SHOWING 15 / 150 NODES' },
      { kind: 'clickTab', tab: 'MEMORY ARCHIVE' },
    ],
    expect: memoryExpect(['SHOWING 15 / 150 NODES', 'NODE #1030', 'oat milk'], 'ALL'),
  },
  {
    name: 'memory: Escape clears the search',
    start: '#memory',
    steps: [
      { kind: 'type', placeholder: MEMORY_SEARCH, text: 'oat milk' },
      { kind: 'press', key: 'Escape' },
      // Waits for the full list (React renders the cleared search as a deferred update).
      { kind: 'openDrawer', text: 'SHOWING 150 / 150 NODES' },
      ...settleMemory(LAST_ROW_ALL),
    ],
    expect: memoryExpect(['SHOWING 150 / 150 NODES', 'NODE #1099', FIRST_FACT], 'ALL'),
  },
  {
    // The last card is reachable (see settleMemory), and scrolling back up re-mounts the first
    // cards in place.
    name: 'memory: scrolling to the bottom reaches the last fact and back',
    start: '#memory',
    // Clicking the results count waits for the list before End.
    steps: [{ kind: 'openDrawer', text: 'SHOWING 150 / 150 NODES' }, ...settleMemory(LAST_ROW_ALL)],
    expect: memoryExpect(['SHOWING 150 / 150 NODES', 'NODE #1099', FIRST_FACT], 'ALL'),
  },
  {
    name: 'builds: default view shows metrics, pills and every build card',
    start: '#builds',
    steps: [],
    expect: buildsExpect(BUILDS_DEFAULT_TEXTS, 'ALL', ALL_BUILD_CARDS, BUILDS_DEFAULT_COUNTS),
  },
  {
    name: 'builds: running pill',
    start: '#builds',
    steps: [{ kind: 'pill', label: 'RUNNING' }, ...settleBuilds('SHOWING 4 / 25 BUILDS')],
    expect: buildsExpect(['SHOWING 4 / 25 BUILDS', 'Console React rewrite #1', 'Memory decay tuning #4'], 'RUNNING', buildCards(4, 0, 0, 0, 0)),
  },
  {
    name: 'builds: queued pill',
    start: '#builds',
    steps: [{ kind: 'pill', label: 'QUEUED' }, ...settleBuilds('SHOWING 6 / 25 BUILDS')],
    expect: buildsExpect(['SHOWING 6 / 25 BUILDS', 'Synapse webhook hardening #5', '0 / 3 CHUNKS'], 'QUEUED', buildCards(0, 6, 0, 0, 0)),
  },
  {
    name: 'builds: completed pill includes failed and cancelled builds',
    start: '#builds',
    steps: [{ kind: 'pill', label: 'COMPLETED' }, ...settleBuilds('SHOWING 15 / 25 BUILDS')],
    expect: buildsExpect(['SHOWING 15 / 25 BUILDS', 'Console React rewrite #11', 'END: 1:28am'], 'COMPLETED', buildCards(0, 0, 10, 3, 2)),
  },
  {
    name: 'builds: all pill after another pill',
    start: '#builds',
    // With its count, so the label names exactly this pill.
    steps: [
      { kind: 'pill', label: 'QUEUED' },
      { kind: 'pill', label: 'ALL 25' },
      ...settleBuilds('SHOWING 25 / 25 BUILDS'),
    ],
    expect: buildsExpect(BUILDS_DEFAULT_TEXTS, 'ALL', ALL_BUILD_CARDS),
  },
  {
    // "kiosk" matches the five "Kiosk widget refresh #N" builds (title and objective).
    name: 'builds: search narrows the queue',
    start: '#builds',
    steps: [{ kind: 'type', placeholder: BUILDS_SEARCH, text: 'kiosk' }, ...settleBuilds('SHOWING 5 / 25 BUILDS')],
    expect: buildsExpect(['SHOWING 5 / 25 BUILDS', 'Kiosk widget refresh #3', 'Kiosk widget refresh #23'], 'ALL', buildCards(1, 1, 2, 1, 0)),
  },
  {
    // "sidecars" only appears in chunk repos (azylman/aerial-sidecars), so this proves repo search.
    name: 'builds: search matches chunk repos and Escape clears it',
    start: '#builds',
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
  },
  {
    name: 'builds: clicking a multi-chunk build opens its detail drawer with rendered markdown',
    start: '#builds',
    steps: OPEN_BUILD_ONE,
    expect: buildsExpect(BUILD_DRAWER_TEXTS, 'ALL', ALL_BUILD_CARDS, BUILD_DRAWER_COUNTS),
  },
  {
    name: 'builds: Escape closes the build drawer',
    start: '#builds',
    steps: [...OPEN_BUILD_ONE, { kind: 'press', key: 'Escape' }],
    expect: buildsExpect(BUILDS_DEFAULT_TEXTS, 'ALL', ALL_BUILD_CARDS, [
      { selector: '.diagnostic-drawer.active', count: 0 },
    ]),
  },
  {
    name: 'walk: every tab in order with no console or page errors',
    start: '',
    steps: [
      { kind: 'clickTab', tab: 'ACTIVE TASKS' },
      { kind: 'clickTab', tab: 'SCHEDULES' },
      { kind: 'clickTab', tab: 'MEMORY ARCHIVE' },
      { kind: 'clickTab', tab: 'BUILD QUEUE' },
    ],
    expect: { ...buildsExpect(BUILDS_DEFAULT_TEXTS, 'ALL', ALL_BUILD_CARDS), badges: BADGES_ALL },
  },
];
