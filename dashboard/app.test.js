/**
 * Permet HUD Native JavaScript Test Suite
 * Tests pure logic, sanitization, URL generation, and timestamp parsing.
 * Uses Node.js native test runner (`node:test` + `node:assert/strict`).
 * Zero external npm dependencies required.
 */
import test, { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Load app.js and extract pure functions in a clean sandbox
const appJsPath = path.join(__dirname, 'static', 'app.js');
const appJsCode = fs.readFileSync(appJsPath, 'utf8');

// Mock browser global environment for app.js evaluation
const sandbox = {
    window: {
        addEventListener: () => {},
        removeEventListener: () => {},
        location: { hash: '', search: '', pathname: '/' }
    },
    document: {
        getElementById: () => null,
        querySelectorAll: () => [],
        createElement: () => ({ setAttribute: () => {}, classList: { add: () => {}, remove: () => {} }, style: {} }),
        addEventListener: () => {},
    },
    navigator: { clipboard: { writeText: async () => {} } },
    setInterval: () => 1,
    clearInterval: () => {},
    setTimeout: () => 1,
    clearTimeout: () => {},
    fetch: () => Promise.resolve({ ok: true, json: async () => ({}) }),
    AbortController: globalThis.AbortController,
    Date: Date,
    Math: Math,
    String: String,
    Array: Array,
    Number: Number,
    encodeURIComponent: encodeURIComponent,
    console: { log: () => {}, warn: () => {}, error: () => {} }
};

vm.createContext(sandbox);
vm.runInContext(appJsCode, sandbox);

const {
    escapeHtml,
    formatUptime,
    formatElapsedTicker,
    getTriggerBadge,
    formatAgentsviewSessionUrl,
    parseValidTimestampMs,
    formatCommitTimestamp,
    formatDuration,
    formatTimestamp,
    formatTimeAgo,
    formatCountdown,
    formatGitSyncDelta,
    renderGitSyncBadge,
    renderQuickLaunchDock,
    parseFactImportance,
    compareFactsImportanceDesc,
    TABS
} = sandbox;

describe('Permet HUD Pure Logic Unit Tests', () => {

    describe('escapeHtml(str)', () => {
        it('escapes dangerous HTML special characters to prevent XSS', () => {
            const input = '<script>alert("XSS & \'attack\'")</script>';
            const expected = '&lt;script&gt;alert(&quot;XSS &amp; &#039;attack&#039;&quot;)&lt;/script&gt;';
            assert.equal(escapeHtml(input), expected);
        });

        it('handles non-string primitives safely', () => {
            assert.equal(escapeHtml(12345), '12345');
            assert.equal(escapeHtml(null), 'null');
            assert.equal(escapeHtml(undefined), 'undefined');
        });
    });

    describe('formatAgentsviewSessionUrl(sessionId)', () => {
        it('formats a raw session UUID into an antigravity-cli path', () => {
            const uuid = '7a874a91-3afb-4eb0-bb62-8d96b9a3ae0b';
            assert.equal(formatAgentsviewSessionUrl(uuid), `/conversations/sessions/antigravity-cli%3A${uuid}`);
        });

        it('preserves already prefixed session IDs while encoding special characters', () => {
            const sessionPath = 'antigravity-cli:7a874a91-3afb-4eb0-bb62-8d96b9a3ae0b';
            assert.equal(formatAgentsviewSessionUrl(sessionPath), '/conversations/sessions/antigravity-cli%3A7a874a91-3afb-4eb0-bb62-8d96b9a3ae0b');
        });

        it('strips leading and trailing slashes safely', () => {
            assert.equal(formatAgentsviewSessionUrl('/sessions/abc/'), '/conversations/sessions/antigravity-cli%3Asessions%2Fabc');
        });

        it('falls back to /conversations/ for null, undefined, or whitespace', () => {
            assert.equal(formatAgentsviewSessionUrl(null), '/conversations/');
            assert.equal(formatAgentsviewSessionUrl(undefined), '/conversations/');
            assert.equal(formatAgentsviewSessionUrl(''), '/conversations/');
            assert.equal(formatAgentsviewSessionUrl('   '), '/conversations/');
            assert.equal(formatAgentsviewSessionUrl(12345), '/conversations/');
        });
    });

    describe('parseValidTimestampMs(dateStr)', () => {
        it('parses valid ISO 8601 timestamps', () => {
            const iso = '2026-08-30T21:45:00Z';
            const ms = parseValidTimestampMs(iso);
            assert.equal(typeof ms, 'number');
            assert.ok(ms > 1700000000000);
        });

        it('rejects Go zero-time and pre-2020 epochs (prevents 2000-year drift)', () => {
            assert.equal(parseValidTimestampMs('0001-01-01T00:00:00Z'), null);
            assert.equal(parseValidTimestampMs('1970-01-01T00:00:00Z'), null);
            assert.equal(parseValidTimestampMs('2019-12-31T23:59:59Z'), null);
        });

        it('handles null, undefined, empty, and invalid dates cleanly', () => {
            assert.equal(parseValidTimestampMs(null), null);
            assert.equal(parseValidTimestampMs(undefined), null);
            assert.equal(parseValidTimestampMs(''), null);
            assert.equal(parseValidTimestampMs('invalid-date-string'), null);
            assert.equal(parseValidTimestampMs(12345), null);
        });
    });

    describe('formatUptime(seconds)', () => {
        it('formats sub-minute durations', () => {
            assert.equal(formatUptime(45), '45s');
        });

        it('formats minutes and seconds', () => {
            assert.equal(formatUptime(125), '2m 5s');
        });

        it('formats hours and minutes', () => {
            assert.equal(formatUptime(3665), '1h 1m');
        });

        it('formats days, hours, and minutes', () => {
            assert.equal(formatUptime(90060), '1d 1h 1m');
        });

        it('returns 0s for invalid or negative values', () => {
            assert.equal(formatUptime(-10), '0s');
            assert.equal(formatUptime(null), '0s');
            assert.equal(formatUptime(NaN), '0s');
        });
    });

    describe('formatElapsedTicker(seconds)', () => {
        it('formats minutes and seconds with leading zero', () => {
            assert.equal(formatElapsedTicker(65), '⏱ 01:05s');
            assert.equal(formatElapsedTicker(0), '⏱ 00:00s');
        });

        it('formats hours, minutes, and seconds when >= 1 hour', () => {
            assert.equal(formatElapsedTicker(3665), '⏱ 01:01:05');
            assert.equal(formatElapsedTicker(7200), '⏱ 02:00:00');
        });

        it('guards against null, negative, or NaN inputs', () => {
            assert.equal(formatElapsedTicker(null), '⏱ 00:00s');
            assert.equal(formatElapsedTicker(-5), '⏱ 00:00s');
            assert.equal(formatElapsedTicker(NaN), '⏱ 00:00s');
        });
    });

    describe('getTriggerBadge(triggerType)', () => {
        it('maps cron trigger to alarm icon and cron css', () => {
            const badge = getTriggerBadge('cron');
            assert.equal(badge.icon, '⏰');
            assert.equal(badge.text, 'CRON');
            assert.equal(badge.css, 'cron');
        });

        it('maps reminder trigger to stopwatch icon and reminder css', () => {
            const badge = getTriggerBadge('reminder');
            assert.equal(badge.icon, '⏱️');
            assert.equal(badge.text, 'REMINDER');
            assert.equal(badge.css, 'reminder');
        });

        it('maps http/api trigger to lightning icon and http css', () => {
            const badgeHttp = getTriggerBadge('http');
            assert.equal(badgeHttp.icon, '⚡');
            assert.equal(badgeHttp.text, 'API');

            const badgeApi = getTriggerBadge('api');
            assert.equal(badgeApi.icon, '⚡');
            assert.equal(badgeApi.text, 'API');
        });

        it('defaults to discord trigger for unknown or null triggers', () => {
            const badgeNull = getTriggerBadge(null);
            assert.equal(badgeNull.icon, '💬');
            assert.equal(badgeNull.text, 'DISCORD');
            assert.equal(badgeNull.css, 'discord');

            const badgeUnknown = getTriggerBadge('custom_unknown');
            assert.equal(badgeUnknown.icon, '💬');
            assert.equal(badgeUnknown.text, 'DISCORD');
        });
    });

    describe('formatDuration(durationMs)', () => {
        if (typeof formatDuration === 'function') {
            it('formats milliseconds under 1000ms', () => {
                assert.equal(formatDuration(450), '450ms');
            });

            it('formats seconds for durations under 1 minute', () => {
                assert.equal(formatDuration(4500), '4.5s');
            });

            it('formats minutes and seconds for durations over 1 minute', () => {
                assert.equal(formatDuration(65000), '1m 5s');
            });
        }
    });

    describe('formatCommitTimestamp(dateStr)', () => {
        it('formats valid ISO timestamp into localized date and time', () => {
            const iso = '2026-09-04T04:00:22Z';
            const formatted = formatCommitTimestamp(iso);
            assert.ok(typeof formatted === 'string' && formatted.length > 0);
            assert.ok(formatted.includes(':'));
        });

        it('returns empty string for Go zero-time and pre-2020 dates', () => {
            assert.equal(formatCommitTimestamp('0001-01-01T00:00:00Z'), '');
            assert.equal(formatCommitTimestamp('1970-01-01T00:00:00Z'), '');
        });

        it('returns empty string for null, undefined, or empty date', () => {
            assert.equal(formatCommitTimestamp(null), '');
            assert.equal(formatCommitTimestamp(undefined), '');
            assert.equal(formatCommitTimestamp(''), '');
            assert.equal(formatCommitTimestamp('invalid'), '');
        });
    });

    describe('Deployment Pipeline Link & Header Invariants', () => {
        it('verifies app.js no longer renders deploy-service-name header', () => {
            assert.equal(appJsCode.includes('deploy-service-name'), false, 'Found deploy-service-name in app.js');
        });

        it('validates hex SHA regex correctly identifies commit hashes', () => {
            const isHexSha = (commit) => /^[0-9a-f]{7,40}$/i.test(String(commit || '').trim());
            assert.equal(isHexSha('e056544'), true);
            assert.equal(isHexSha('e056544d32a5f9d8a4cc50915a20db5eaea7db1e'), true);
            assert.equal(isHexSha('latest'), false);
            assert.equal(isHexSha('dirty'), false);
            assert.equal(isHexSha(''), false);
            assert.equal(isHexSha(null), false);
            assert.equal(isHexSha(undefined), false);
        });
    });

    describe('Privacy & Zero-Hardcoded IP Guarantee', () => {
        it('verifies app.js contains zero private LAN IPs (192.168.x.x)', () => {
            const privateIpPattern = /192\.168\.\d+\.\d+/g;
            const matches = appJsCode.match(privateIpPattern);
            assert.equal(matches, null, `Found private IP leak in app.js: ${JSON.stringify(matches)}`);
        });
    });

    describe('formatGitSyncDelta(seconds)', () => {
        it('formats seconds correctly', () => {
            assert.equal(formatGitSyncDelta(0), 'Δ 0s');
            assert.equal(formatGitSyncDelta(45), 'Δ 45s');
            assert.equal(formatGitSyncDelta(60), 'Δ 1m');
            assert.equal(formatGitSyncDelta(125), 'Δ 2m 5s');
            assert.equal(formatGitSyncDelta(3600), 'Δ 1h');
            assert.equal(formatGitSyncDelta(3665), 'Δ 1h 1m');
        });

        it('handles negative, null, or invalid seconds safely', () => {
            assert.equal(formatGitSyncDelta(null), 'Δ 0s');
            assert.equal(formatGitSyncDelta(undefined), 'Δ 0s');
            assert.equal(formatGitSyncDelta(NaN), 'Δ 0s');
            assert.equal(formatGitSyncDelta(-10), 'Δ 0s');
        });
    });

    describe('renderQuickLaunchDock(links)', () => {
        it('renders quick launch chips and sets target and rel attributes', () => {
            const mockDock = { style: {}, innerHTML: '' };
            sandbox.document.getElementById = (id) => id === 'quick-launch-dock' ? mockDock : null;

            const links = [
                { name: 'DOCS', url: '/docs/', icon: '📚', target: '_blank', is_core: true },
                { name: 'HOME', url: 'https://home.zylman.com', icon: '🏠', target: '_blank', is_custom: true, description: 'Home Hub' },
                { name: 'TEST', url: '/ui-testing/', target: '_self' }
            ];

            renderQuickLaunchDock(links);

            assert.equal(mockDock.style.display, 'flex');
            assert.ok(mockDock.innerHTML.includes('href="/docs/"'));
            assert.ok(mockDock.innerHTML.includes('rel="noopener noreferrer"'));
            assert.ok(mockDock.innerHTML.includes('href="https://home.zylman.com"'));
            assert.ok(mockDock.innerHTML.includes('ping-dot custom'));
            assert.ok(mockDock.innerHTML.includes('title="Home Hub"'));
            assert.ok(mockDock.innerHTML.includes('target="_self"'));
        });

        it('hides dock when links list is empty', () => {
            const mockDock = { style: {}, innerHTML: '' };
            sandbox.document.getElementById = (id) => id === 'quick-launch-dock' ? mockDock : null;

            renderQuickLaunchDock([]);
            assert.equal(mockDock.style.display, 'none');
        });
    });

    describe('renderGitSyncBadge(gitSync)', () => {
        it('renders in-sync pill when status is synced and lag is 0', () => {
            const mockBadge = { className: '', innerHTML: '' };
            sandbox.document.getElementById = (id) => id === 'gitsync-badge' ? mockBadge : null;

            renderGitSyncBadge({ status: 'synced', max_lag_seconds: 0 });
            assert.equal(mockBadge.className, 'gitsync-pill');
            assert.ok(mockBadge.innerHTML.includes('IN SYNC (Δ 0s)'));
        });

        it('renders lagging pill when lag > 0', () => {
            const mockBadge = { className: '', innerHTML: '' };
            sandbox.document.getElementById = (id) => id === 'gitsync-badge' ? mockBadge : null;

            renderGitSyncBadge({ status: 'lagging', max_lag_seconds: 45 });
            assert.equal(mockBadge.className, 'gitsync-pill lagging');
            assert.ok(mockBadge.innerHTML.includes('BEHIND (Δ 45s)'));
        });

        it('renders error pill when status is error', () => {
            const mockBadge = { className: '', innerHTML: '' };
            sandbox.document.getElementById = (id) => id === 'gitsync-badge' ? mockBadge : null;

            renderGitSyncBadge({ status: 'error' });
            assert.equal(mockBadge.className, 'gitsync-pill error');
            assert.ok(mockBadge.innerHTML.includes('ERROR'));
        });
    });

    describe('4-View Navigation Architecture (TABS)', () => {
        it('includes telemetry, tasks, schedules, and memory tabs', () => {
            assert.ok(TABS.telemetry, 'Missing telemetry tab');
            assert.ok(TABS.tasks, 'Missing tasks tab');
            assert.ok(TABS.schedules, 'Missing schedules tab');
            assert.ok(TABS.memory, 'Missing memory tab');

            assert.equal(TABS.tasks.btnId, 'tab-tasks-btn');
            assert.equal(TABS.tasks.viewId, 'tasks-view');
            assert.equal(TABS.tasks.hash, '#tasks');
        });
    });

    describe('Header Tab Badge Metrics Synchronization', () => {
        it('updates #schedules-badge-count correctly from summary.total_active', () => {
            const elements = {};
            sandbox.document.getElementById = (id) => {
                if (!elements[id]) {
                    elements[id] = { textContent: '', className: '', style: {} };
                }
                return elements[id];
            };

            vm.runInContext(`
                schedulesState.summary = { total_active: 7, cron_count: 4, one_shot_count: 3 };
                schedulesState.crons = [1, 2, 3, 4];
                schedulesState.oneShots = [5, 6, 7];
                updateScheduleMetrics();
            `, sandbox);

            assert.equal(elements['schedules-badge-count'].textContent, '7');
            assert.equal(elements['schedules-active-badge'].textContent, '7 ACTIVE');
            assert.equal(elements['schedules-crons-count'].textContent, 4);
            assert.equal(elements['schedules-oneshot-count'].textContent, 3);
        });

        it('falls back to crons + oneShots length when summary is null or missing total_active', () => {
            const elements = {};
            sandbox.document.getElementById = (id) => {
                if (!elements[id]) {
                    elements[id] = { textContent: '', className: '', style: {} };
                }
                return elements[id];
            };

            vm.runInContext(`
                schedulesState.summary = null;
                schedulesState.crons = [{ id: 'cron-1' }, { id: 'cron-2' }];
                schedulesState.oneShots = [{ id: 'once-1' }];
                updateScheduleMetrics();
            `, sandbox);

            assert.equal(elements['schedules-badge-count'].textContent, '3');
            assert.equal(elements['schedules-active-badge'].textContent, '3 ACTIVE');
        });

        it('preserves string "0" for schedules when 0 active schedules exist', () => {
            const elements = {};
            sandbox.document.getElementById = (id) => {
                if (!elements[id]) {
                    elements[id] = { textContent: '', className: '', style: {} };
                }
                return elements[id];
            };

            vm.runInContext(`
                schedulesState.summary = { total_active: 0, cron_count: 0, one_shot_count: 0 };
                schedulesState.crons = [];
                schedulesState.oneShots = [];
                updateScheduleMetrics();
            `, sandbox);

            assert.equal(elements['schedules-badge-count'].textContent, '0');
            assert.equal(elements['schedules-active-badge'].textContent, '0 ACTIVE');
        });

        it('updates #memory-badge-count correctly from memoryState.totalCount', () => {
            const elements = {};
            sandbox.document.getElementById = (id) => {
                if (!elements[id]) {
                    elements[id] = { textContent: '', className: '', style: {} };
                }
                return elements[id];
            };

            vm.runInContext(`
                memoryState.totalCount = 42;
                memoryState.facts = [{ id: 'f1', category: 'core', importance: 1.0 }];
                updateMemoryMetrics();
            `, sandbox);

            assert.equal(elements['memory-badge-count'].textContent, '42');
            assert.equal(elements['memory-total-count'].textContent, 42);
        });

        it('falls back to facts.length when memoryState.totalCount is undefined or null', () => {
            const elements = {};
            sandbox.document.getElementById = (id) => {
                if (!elements[id]) {
                    elements[id] = { textContent: '', className: '', style: {} };
                }
                return elements[id];
            };

            vm.runInContext(`
                memoryState.totalCount = null;
                memoryState.facts = [
                    { id: 'f1', category: 'core', importance: 1.0 },
                    { id: 'f2', category: 'infra', importance: 0.8 }
                ];
                updateMemoryMetrics();
            `, sandbox);

            assert.equal(elements['memory-badge-count'].textContent, '2');
            assert.equal(elements['memory-total-count'].textContent, 2);
        });

        it('preserves string "0" for memory when 0 facts exist in memoryState', () => {
            const elements = {};
            sandbox.document.getElementById = (id) => {
                if (!elements[id]) {
                    elements[id] = { textContent: '', className: '', style: {} };
                }
                return elements[id];
            };

            vm.runInContext(`
                memoryState.totalCount = 0;
                memoryState.facts = [];
                updateMemoryMetrics();
            `, sandbox);

            assert.equal(elements['memory-badge-count'].textContent, '0');
            assert.equal(elements['memory-total-count'].textContent, 0);
        });

        it('gracefully handles missing DOM elements without throwing errors', () => {
            sandbox.document.getElementById = () => null;

            assert.doesNotThrow(() => {
                vm.runInContext(`
                    schedulesState.summary = { total_active: 5 };
                    updateScheduleMetrics();
                    memoryState.totalCount = 10;
                    updateMemoryMetrics();
                `, sandbox);
            });
        });

        it('updates #tasks-badge-count correctly in renderActiveTasks', () => {
            const elements = {};
            sandbox.document.getElementById = (id) => {
                if (!elements[id]) {
                    elements[id] = { textContent: '', className: '', style: {}, innerHTML: '' };
                }
                return elements[id];
            };

            vm.runInContext(`
                renderActiveTasks([
                    { id: 'task-1', status: 'PROCESSING' },
                    { id: 'task-2', status: 'PENDING' }
                ]);
            `, sandbox);

            assert.equal(elements['tasks-badge-count'].textContent, '2');
        });

        it('ensures TABS.memory.onEnter respects hasLoaded flag to avoid redundant fetches', () => {
            let fetchFactsCalled = 0;
            sandbox.fetchFacts = () => { fetchFactsCalled++; };

            vm.runInContext(`
                memoryState.hasLoaded = true;
                memoryState.facts = [];
                TABS.memory.onEnter();
            `, sandbox);
            assert.equal(fetchFactsCalled, 0, 'fetchFacts should not be called when hasLoaded is true');

            vm.runInContext(`
                memoryState.hasLoaded = false;
                memoryState.facts = [];
                TABS.memory.onEnter();
            `, sandbox);
            assert.equal(fetchFactsCalled, 1, 'fetchFacts should be called when hasLoaded is false');
        });

        it('verifies eager metrics fetch on bootstrap in app.js', () => {
            assert.ok(appJsCode.includes('fetchSchedules();\nfetchFacts();') || appJsCode.includes('fetchSchedules();\r\nfetchFacts();') || (appJsCode.includes('fetchSchedules()') && appJsCode.includes('fetchFacts()')), 'app.js should include eager metrics bootstrap');
        });
    });

    describe('Permet Memory Importance Sorting & Pipeline Invariants', () => {
        describe('parseFactImportance(val)', () => {
            it('parses valid numeric and floating point values', () => {
                assert.equal(parseFactImportance(1.0), 1.0);
                assert.equal(parseFactImportance(0.85), 0.85);
                assert.equal(parseFactImportance('0.95'), 0.95);
                assert.equal(parseFactImportance('1'), 1.0);
            });

            it('preserves valid 0.0 importance and does NOT coerce to 1.0', () => {
                assert.equal(parseFactImportance(0), 0.0);
                assert.equal(parseFactImportance(0.0), 0.0);
                assert.equal(parseFactImportance('0'), 0.0);
                assert.equal(parseFactImportance('0.0'), 0.0);
            });

            it('clamps importance values to [0.0, 1.0]', () => {
                assert.equal(parseFactImportance(1.5), 1.0);
                assert.equal(parseFactImportance(-0.5), 0.0);
            });

            it('handles null, undefined, empty string, and NaN safely with 0.0 fallback', () => {
                assert.equal(parseFactImportance(null), 0.0);
                assert.equal(parseFactImportance(undefined), 0.0);
                assert.equal(parseFactImportance(''), 0.0);
                assert.equal(parseFactImportance('invalid-number'), 0.0);
                assert.equal(parseFactImportance(NaN), 0.0);
            });
        });

        describe('compareFactsImportanceDesc(a, b)', () => {
            it('sorts facts in strictly descending order of importance', () => {
                const facts = [
                    { id: 1, fact_text: 'low', importance: 0.2 },
                    { id: 2, fact_text: 'max', importance: 1.0 },
                    { id: 3, fact_text: 'mid', importance: 0.5 },
                    { id: 4, fact_text: 'high', importance: 0.8 },
                    { id: 5, fact_text: 'very high', importance: 0.95 }
                ];

                const sorted = facts.slice().sort(compareFactsImportanceDesc);
                const order = sorted.map(f => f.importance);
                assert.deepEqual(order, [1.0, 0.95, 0.8, 0.5, 0.2]);
            });

            it('applies secondary tie-breaking by recency (created_at DESC)', () => {
                const facts = [
                    { id: 1, fact_text: 'older', importance: 0.8, created_at: '2026-09-04T12:00:00Z' },
                    { id: 2, fact_text: 'newer', importance: 0.8, created_at: '2026-09-05T12:00:00Z' }
                ];

                const sorted = facts.slice().sort(compareFactsImportanceDesc);
                assert.equal(sorted[0].id, 2);
                assert.equal(sorted[1].id, 1);
            });

            it('applies tertiary tie-breaking by numeric and alphanumeric ID (id DESC)', () => {
                const numericFacts = [
                    { id: 42, fact_text: 'lower id', importance: 0.8, created_at: '2026-09-05T12:00:00Z' },
                    { id: 105, fact_text: 'higher id', importance: 0.8, created_at: '2026-09-05T12:00:00Z' }
                ];
                const sortedNumeric = numericFacts.slice().sort(compareFactsImportanceDesc);
                assert.equal(sortedNumeric[0].id, 105);
                assert.equal(sortedNumeric[1].id, 42);

                const stringFacts = [
                    { id: 'fact-01', fact_text: 'lower id', importance: 0.8, created_at: '2026-09-05T12:00:00Z' },
                    { id: 'fact-99', fact_text: 'higher id', importance: 0.8, created_at: '2026-09-05T12:00:00Z' }
                ];
                const sortedString = stringFacts.slice().sort(compareFactsImportanceDesc);
                assert.equal(sortedString[0].id, 'fact-99');
                assert.equal(sortedString[1].id, 'fact-01');
            });

            it('handles missing, null, or string importance scores in sorting', () => {
                const facts = [
                    { id: 1, fact_text: 'missing', created_at: '2026-09-05T10:00:00Z' },
                    { id: 2, fact_text: 'string high', importance: '0.9', created_at: '2026-09-05T10:00:00Z' },
                    { id: 3, fact_text: 'zero', importance: 0, created_at: '2026-09-05T10:00:00Z' }
                ];

                const sorted = facts.slice().sort(compareFactsImportanceDesc);
                assert.equal(sorted[0].id, 2); // 0.9
                // Both id 1 (null -> 0.0) and id 3 (0 -> 0.0) tie on importance, sort by ID DESC
                assert.equal(sorted[1].id, 3);
                assert.equal(sorted[2].id, 1);
            });
        });

        describe('Filter Pipeline & Metrics Integration', () => {
            it('preserves descending importance sorting across category and query filtering', () => {
                const elements = {};
                sandbox.document.getElementById = (id) => {
                    if (!elements[id]) {
                        elements[id] = { textContent: '', className: '', style: {}, innerHTML: '' };
                    }
                    return elements[id];
                };

                const filtered = vm.runInContext(`
                    memoryState.facts = [
                        { id: 1, category: 'USER_PREFERENCE', fact_text: 'Alex prefers dark mode', importance: 0.5 },
                        { id: 2, category: 'USER_PREFERENCE', fact_text: 'Alex loves matcha latte', importance: 0.95 },
                        { id: 3, category: 'SYSTEM_CONFIG', fact_text: 'Ollama model setting', importance: 0.8 },
                        { id: 4, category: 'USER_PREFERENCE', fact_text: 'Alex drinks espresso', importance: 0.7 }
                    ].sort(compareFactsImportanceDesc);

                    memoryState.selectedCategory = 'USER_PREFERENCE';
                    memoryState.searchQuery = 'alex';
                    applyFilters();
                    memoryState.filteredFacts;
                `, sandbox);

                assert.equal(filtered.length, 3);
                assert.equal(filtered[0].id, 2); // 0.95
                assert.equal(filtered[1].id, 4); // 0.7
                assert.equal(filtered[2].id, 1); // 0.5
            });

            it('computes average importance correctly without corrupting 0.0 importance facts', () => {
                const elements = {};
                sandbox.document.getElementById = (id) => {
                    if (!elements[id]) {
                        elements[id] = { textContent: '', className: '', style: {}, innerHTML: '' };
                    }
                    return elements[id];
                };

                vm.runInContext(`
                    memoryState.totalCount = 4;
                    memoryState.facts = [
                        { id: 1, importance: 1.0 },
                        { id: 2, importance: 0.5 },
                        { id: 3, importance: 0.0 },
                        { id: 4, importance: 0.5 }
                    ];
                    updateMemoryMetrics();
                `, sandbox);

                // Total sum = 1.0 + 0.5 + 0.0 + 0.5 = 2.0. Avg = 2.0 / 4 = 0.50
                assert.equal(elements['memory-avg-importance'].textContent, '0.50');
            });
        });
    });
});
