// THE parity table. One declarative row per scenario drives BOTH UIs (vanilla at /console/,
// React at /console/next/) at both viewports; parity.spec.ts is the only runner. Expected values
// are literals derived from e2e/fixtures/*.json, never computed by UI code.
//
// Feature chunks extend this file in exactly two ways: remove their view from PENDING_VIEWS and
// append rows. MASKED_SELECTORS below is the complete list of allowed visual differences.

import type { BrowserContextOptions } from '@playwright/test';

export type ViewId = 'telemetry' | 'tasks' | 'schedules' | 'memory' | 'builds';

/** Views (and their drawers) the React UI does not ship yet; rows needing them are skipped. */
export const PENDING_VIEWS: readonly ViewId[] = ['telemetry', 'tasks', 'schedules', 'memory', 'builds'];

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
  '.drawer-icon',
  '.drawer-close',
  '.search-icon',
  '.refresh-icon',
  '.reconnect-icon',
  '.gitsync-pill > span:first-child',
];

/**
 * Applied to both UIs only while screenshotting: hides the moving scanline, fixes the masked glyph
 * boxes' size, and drops UA focus rings, whose colours follow `color-scheme` (the React console
 * declares `dark` per the Task #4 polish; the vanilla page is light-scheme), so a keyboard-focused
 * control would otherwise differ by design.
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
];
