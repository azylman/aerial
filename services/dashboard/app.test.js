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

const initialGetElementById = sandbox.document.getElementById;

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
    parseFactReinforceCount,
    parseFactImportance,
    compareFactsImportanceDesc,
    renderDeployments,
    TABS,
    buildsState,
    classifyBuildQueueState,
    computeBuildChunkStats,
    getBuildTimingInfo,
    renderBuildQueueCards,
    renderBuildDetailContent,
    renderBuildsPills,
    updateBuildsMetrics,
    fetchBuilds,
    openBuildDetailDrawer,
    setupBuildsControls
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
            assert.equal(formatAgentsviewSessionUrl(uuid), `/agentsview/sessions/antigravity-cli%3A${uuid}`);
        });

        it('preserves already prefixed session IDs while encoding special characters', () => {
            const sessionPath = 'antigravity-cli:7a874a91-3afb-4eb0-bb62-8d96b9a3ae0b';
            assert.equal(formatAgentsviewSessionUrl(sessionPath), '/agentsview/sessions/antigravity-cli%3A7a874a91-3afb-4eb0-bb62-8d96b9a3ae0b');
        });

        it('strips leading and trailing slashes safely', () => {
            assert.equal(formatAgentsviewSessionUrl('/sessions/abc/'), '/agentsview/sessions/antigravity-cli%3Asessions%2Fabc');
        });

        it('falls back to /agentsview/ for null, undefined, or whitespace', () => {
            assert.equal(formatAgentsviewSessionUrl(null), '/agentsview/');
            assert.equal(formatAgentsviewSessionUrl(undefined), '/agentsview/');
            assert.equal(formatAgentsviewSessionUrl(''), '/agentsview/');
            assert.equal(formatAgentsviewSessionUrl('   '), '/agentsview/');
            assert.equal(formatAgentsviewSessionUrl(12345), '/agentsview/');
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

    describe('formatTimeAgo(dateStr)', () => {
        it('formats valid relative time for recent timestamps', () => {
            const now = Date.now();
            assert.equal(formatTimeAgo(new Date(now - 10 * 1000).toISOString()), '10s AGO');
            assert.equal(formatTimeAgo(new Date(now - 120 * 1000).toISOString()), '2m AGO');
            assert.equal(formatTimeAgo(new Date(now - 7200 * 1000).toISOString()), '2h AGO');
            assert.equal(formatTimeAgo(new Date(now - 172800 * 1000).toISOString()), '2d AGO');
        });

        it('returns RECENT for Go zero-time and pre-2020 dates', () => {
            assert.equal(formatTimeAgo('0001-01-01T00:00:00Z'), 'RECENT');
            assert.equal(formatTimeAgo('1970-01-01T00:00:00Z'), 'RECENT');
        });

        it('returns RECENT for null, undefined, empty, or invalid date', () => {
            assert.equal(formatTimeAgo(null), 'RECENT');
            assert.equal(formatTimeAgo(undefined), 'RECENT');
            assert.equal(formatTimeAgo(''), 'RECENT');
            assert.equal(formatTimeAgo('invalid-date'), 'RECENT');
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

        it('verifies dynamic repo commit URL resolution', () => {
            const resolveCommitUrl = (repository, commit) => {
                const rawRepo = (typeof repository === 'string' && /^[a-zA-Z0-9._-]+\/[a-zA-Z0-9._-]+$/.test(repository.trim()))
                    ? repository.trim()
                    : 'azylman/aerial';
                const repoParts = rawRepo.split('/');
                return `https://github.com/${encodeURIComponent(repoParts[0])}/${encodeURIComponent(repoParts[1])}/commit/${encodeURIComponent(commit.trim())}`;
            };

            assert.equal(
                resolveCommitUrl('azylman/aerial', 'e056544'),
                'https://github.com/azylman/aerial/commit/e056544'
            );
            assert.equal(
                resolveCommitUrl('azylman/aerial-sidecars', 'a1b2c3d'),
                'https://github.com/azylman/aerial-sidecars/commit/a1b2c3d'
            );
            assert.equal(
                resolveCommitUrl('azylman/aerial-config', '9988776'),
                'https://github.com/azylman/aerial-config/commit/9988776'
            );
            assert.equal(
                resolveCommitUrl(null, 'e056544'),
                'https://github.com/azylman/aerial/commit/e056544'
            );
            assert.equal(
                resolveCommitUrl('invalid repo string with spaces', 'e056544'),
                'https://github.com/azylman/aerial/commit/e056544'
            );
        });

        it('verifies deploy-repo-badge rendered and deploy-service-name absent', () => {
            assert.equal(appJsCode.includes('deploy-repo-badge'), true, 'Expected deploy-repo-badge in app.js');
            assert.equal(appJsCode.includes('deploy-service-name'), false, 'Found forbidden deploy-service-name in app.js');
        });

        it('verifies dynamic active CI builds badge text pluralization', () => {
            const formatBuildingBadge = (deploys) => {
                const buildingCount = deploys.filter(dep => dep.stage === 'building' || dep.stage === 'queued').length;
                return buildingCount > 1 ? `⚡ ${buildingCount} CI BUILDS ACTIVE` : `⚡ 1 CI BUILD ACTIVE`;
            };

            assert.equal(formatBuildingBadge([{ stage: 'building' }]), '⚡ 1 CI BUILD ACTIVE');
            assert.equal(
                formatBuildingBadge([{ stage: 'building' }, { stage: 'queued' }]),
                '⚡ 2 CI BUILDS ACTIVE'
            );
            assert.equal(
                formatBuildingBadge([{ stage: 'building' }, { stage: 'building' }, { stage: 'building' }]),
                '⚡ 3 CI BUILDS ACTIVE'
            );
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
            sandbox.document.getElementById = () => mockDock;

            const links = [
                { name: 'DOCS', url: '/docs/', icon: '📚', target: '_blank', is_core: true },
                { name: 'HOME', url: 'https://home.zylman.com', icon: '🏠', target: '_blank', is_custom: true, description: 'Home Hub' },
                { name: 'OBSERVABILITY', url: '/grafana/', target: '_self' }
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
            sandbox.document.getElementById = () => mockDock;

            renderQuickLaunchDock([]);
            assert.equal(mockDock.style.display, 'none');
        });

        it('renders single portal link cleanly without extra external chips', () => {
            const mockDock = { style: {}, innerHTML: '' };
            sandbox.document.getElementById = () => mockDock;

            const links = [
                { name: 'PORTAL', url: '/', icon: '🏠', target: '_self', is_core: true }
            ];

            renderQuickLaunchDock(links);
            assert.equal(mockDock.style.display, 'flex');
            assert.ok(mockDock.innerHTML.includes('href="/"'));
            assert.ok(mockDock.innerHTML.includes('target="_self"'));
            assert.ok(!mockDock.innerHTML.includes('rel="noopener noreferrer"'));
            assert.ok(mockDock.innerHTML.includes('PORTAL'));
        });
    });

    describe('renderGitSyncBadge(gitSync)', () => {
        it('renders in-sync pill when status is synced and lag is 0', () => {
            const mockBadge = { className: '', innerHTML: '' };
            sandbox.document.getElementById = () => mockBadge;

            renderGitSyncBadge({ status: 'synced', max_lag_seconds: 0 });
            assert.equal(mockBadge.className, 'gitsync-pill');
            assert.ok(mockBadge.innerHTML.includes('IN SYNC (Δ 0s)'));
        });

        it('renders lagging pill when lag > 0', () => {
            const mockBadge = { className: '', innerHTML: '' };
            sandbox.document.getElementById = () => mockBadge;

            renderGitSyncBadge({ status: 'lagging', max_lag_seconds: 45 });
            assert.equal(mockBadge.className, 'gitsync-pill lagging');
            assert.ok(mockBadge.innerHTML.includes('BEHIND (Δ 45s)'));
        });

        it('renders error pill when status is error', () => {
            const mockBadge = { className: '', innerHTML: '' };
            sandbox.document.getElementById = () => mockBadge;

            renderGitSyncBadge({ status: 'error' });
            assert.equal(mockBadge.className, 'gitsync-pill error');
            assert.ok(mockBadge.innerHTML.includes('ERROR'));
        });
    });

    describe('4-View Navigation Architecture (TABS)', () => {
        it('includes telemetry, tasks, schedules, memory, and builds tabs', () => {
            assert.ok(TABS.telemetry, 'Missing telemetry tab');
            assert.ok(TABS.tasks, 'Missing tasks tab');
            assert.ok(TABS.schedules, 'Missing schedules tab');
            assert.ok(TABS.memory, 'Missing memory tab');
            assert.ok(TABS.builds, 'Missing builds tab');

            assert.equal(TABS.tasks.btnId, 'tab-tasks-btn');
            assert.equal(TABS.tasks.viewId, 'tasks-view');
            assert.equal(TABS.tasks.hash, '#tasks');
            assert.equal(TABS.builds.btnId, 'tab-builds-btn');
            assert.equal(TABS.builds.viewId, 'builds-view');
            assert.equal(TABS.builds.hash, '#builds');
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
                memoryState.facts = [{ id: 'f1', reinforce_count: 2 }];
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
                    { id: 'f1', reinforce_count: 2 },
                    { id: 'f2', reinforce_count: 1 }
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
            assert.ok(appJsCode.includes('fetchSchedules()') && appJsCode.includes('fetchFacts()') && appJsCode.includes('fetchBuilds()'), 'app.js should include eager metrics bootstrap');
        });
    });

    describe('Permet Memory Activity Metrics, Sorting & Pipeline Invariants', () => {
        describe('parseFactReinforceCount(val)', () => {
            it('parses valid positive integer and float counts', () => {
                assert.equal(parseFactReinforceCount(1), 1);
                assert.equal(parseFactReinforceCount(5), 5);
                assert.equal(parseFactReinforceCount('3'), 3);
                assert.equal(parseFactReinforceCount(2.9), 2);
                assert.equal(parseFactImportance(4), 4);
            });

            it('returns 1 for values <= 1, zero, or negative numbers', () => {
                assert.equal(parseFactReinforceCount(1), 1);
                assert.equal(parseFactReinforceCount(0), 1);
                assert.equal(parseFactReinforceCount(-5), 1);
                assert.equal(parseFactReinforceCount('0'), 1);
            });

            it('handles null, undefined, empty string, and NaN safely with 1 fallback', () => {
                assert.equal(parseFactReinforceCount(null), 1);
                assert.equal(parseFactReinforceCount(undefined), 1);
                assert.equal(parseFactReinforceCount(''), 1);
                assert.equal(parseFactReinforceCount('invalid-number'), 1);
                assert.equal(parseFactReinforceCount(NaN), 1);
            });
        });

        describe('compareFactsImportanceDesc(a, b)', () => {
            it('sorts facts in strictly descending order of reinforce_count', () => {
                const facts = [
                    { id: 1, fact_text: 'base', reinforce_count: 1 },
                    { id: 2, fact_text: 'max', reinforce_count: 10 },
                    { id: 3, fact_text: 'mid', reinforce_count: 3 },
                    { id: 4, fact_text: 'high', reinforce_count: 7 },
                    { id: 5, fact_text: 'reinforced', reinforce_count: 2 }
                ];

                const sorted = facts.slice().sort(compareFactsImportanceDesc);
                const order = sorted.map(f => f.reinforce_count);
                assert.deepEqual(order, [10, 7, 3, 2, 1]);
            });

            it('applies secondary tie-breaking by recency (last_reinforced_at fallback created_at DESC)', () => {
                const facts = [
                    { id: 1, fact_text: 'older', reinforce_count: 3, created_at: '2026-09-04T12:00:00Z' },
                    { id: 2, fact_text: 'newer reinforced', reinforce_count: 3, last_reinforced_at: '2026-09-06T12:00:00Z', created_at: '2026-09-01T12:00:00Z' },
                    { id: 3, fact_text: 'newer created', reinforce_count: 3, created_at: '2026-09-05T12:00:00Z' },
                    { id: 4, fact_text: 'zero reinforced time falls back to created_at', reinforce_count: 3, last_reinforced_at: '0001-01-01T00:00:00Z', created_at: '2026-09-07T12:00:00Z' }
                ];

                const sorted = facts.slice().sort(compareFactsImportanceDesc);
                assert.equal(sorted[0].id, 4);
                assert.equal(sorted[1].id, 2);
                assert.equal(sorted[2].id, 3);
                assert.equal(sorted[3].id, 1);
            });

            it('applies tertiary tie-breaking by numeric and alphanumeric ID (id DESC)', () => {
                const numericFacts = [
                    { id: 42, fact_text: 'lower id', reinforce_count: 2, created_at: '2026-09-05T12:00:00Z' },
                    { id: 105, fact_text: 'higher id', reinforce_count: 2, created_at: '2026-09-05T12:00:00Z' }
                ];
                const sortedNumeric = numericFacts.slice().sort(compareFactsImportanceDesc);
                assert.equal(sortedNumeric[0].id, 105);
                assert.equal(sortedNumeric[1].id, 42);

                const stringFacts = [
                    { id: 'fact-01', fact_text: 'lower id', reinforce_count: 2, created_at: '2026-09-05T12:00:00Z' },
                    { id: 'fact-99', reinforce_count: 2, created_at: '2026-09-05T12:00:00Z' }
                ];
                const sortedString = stringFacts.slice().sort(compareFactsImportanceDesc);
                assert.equal(sortedString[0].id, 'fact-99');
                assert.equal(sortedString[1].id, 'fact-01');
            });

            it('handles missing, null, or string reinforce counts in sorting', () => {
                const facts = [
                    { id: 1, fact_text: 'missing', created_at: '2026-09-05T10:00:00Z' },
                    { id: 2, fact_text: 'string high', reinforce_count: '5', created_at: '2026-09-05T10:00:00Z' },
                    { id: 3, fact_text: 'zero', reinforce_count: 0, created_at: '2026-09-05T10:00:00Z' }
                ];

                const sorted = facts.slice().sort(compareFactsImportanceDesc);
                assert.equal(sorted[0].id, 2); // 5
                // Both id 1 (null -> 1) and id 3 (0 -> 1) tie on count, sort by ID DESC
                assert.equal(sorted[1].id, 3);
                assert.equal(sorted[2].id, 1);
            });
        });

        describe('Filter Pipeline & Metrics Integration', () => {
            it('preserves descending reinforce sorting and filters by activity tier and text or node id', () => {
                const elements = {};
                sandbox.document.getElementById = (id) => {
                    if (!elements[id]) {
                        elements[id] = { textContent: '', className: '', style: {}, innerHTML: '' };
                    }
                    return elements[id];
                };

                const filtered = vm.runInContext(`
                    memoryState.facts = [
                        { id: 10, fact_text: 'User prefers dark mode', reinforce_count: 2 },
                        { id: 20, fact_text: 'User loves matcha latte', reinforce_count: 5 },
                        { id: 30, fact_text: 'Ollama model setting', reinforce_count: 1 },
                        { id: 40, fact_text: 'User drinks espresso', reinforce_count: 3 }
                    ].sort(compareFactsImportanceDesc);

                    memoryState.selectedCategory = 'REINFORCED';
                    memoryState.searchQuery = 'user';
                    applyFilters();
                    memoryState.filteredFacts;
                `, sandbox);

                assert.equal(filtered.length, 3);
                assert.equal(filtered[0].id, 20); // 5
                assert.equal(filtered[1].id, 40); // 3
                assert.equal(filtered[2].id, 10); // 2

                // Test searching by node ID
                const idSearch = vm.runInContext(`
                    memoryState.selectedCategory = 'ALL';
                    memoryState.searchQuery = '30';
                    applyFilters();
                    memoryState.filteredFacts;
                `, sandbox);
                assert.equal(idSearch.length, 1);
                assert.equal(idSearch[0].id, 30);

                // Test BASE tier filter
                const baseFiltered = vm.runInContext(`
                    memoryState.selectedCategory = 'BASE';
                    memoryState.searchQuery = '';
                    applyFilters();
                    memoryState.filteredFacts;
                `, sandbox);
                assert.equal(baseFiltered.length, 1);
                assert.equal(baseFiltered[0].id, 30);
            });

            it('computes average reinforcements and reinforced count accurately', () => {
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
                        { id: 1, reinforce_count: 4 },
                        { id: 2, reinforce_count: 2 },
                        { id: 3, reinforce_count: 1 },
                        { id: 4, reinforce_count: 1 }
                    ];
                    updateMemoryMetrics();
                `, sandbox);

                // Total sum = 4 + 2 + 1 + 1 = 8. Avg = 8 / 4 = 2.00 -> 2.00x
                assert.equal(elements['memory-avg-importance'].textContent, '2.00x');
                // Facts with reinforce_count >= 2: id 1 and id 2 -> 2
                assert.equal(elements['memory-category-count'].textContent, '2 REINFORCED');
            });

            it('renders activity tier category pills and fact cards with node badges', () => {
                const elements = {};
                sandbox.document.getElementById = (id) => {
                    if (!elements[id]) {
                        elements[id] = {
                            textContent: '',
                            className: '',
                            style: {},
                            innerHTML: '',
                            querySelectorAll: () => []
                        };
                    }
                    return elements[id];
                };

                vm.runInContext(`
                    memoryState.facts = [
                        { id: 101, fact_text: 'Reinforced fact', reinforce_count: 3, last_reinforced_at: '2026-09-01T00:00:00Z' },
                        { id: 102, fact_text: 'Base fact', reinforce_count: 1, created_at: '2026-09-01T00:00:00Z' }
                    ];
                    memoryState.selectedCategory = 'ALL';
                    memoryState.searchQuery = '';
                    renderCategoryPills();
                    applyFilters();
                `, sandbox);

                const pillsBarHtml = elements['category-pills-bar'].innerHTML;
                assert.ok(pillsBarHtml.includes('ALL'));
                assert.ok(pillsBarHtml.includes('REINFORCED'));
                assert.ok(pillsBarHtml.includes('BASE'));

                const gridHtml = elements['memory-cards-grid'].innerHTML;
                assert.ok(gridHtml.includes('high-importance'));
                assert.ok(gridHtml.includes('NODE #101'));
                assert.ok(gridHtml.includes('⚡ 3x'));
                assert.ok(gridHtml.includes('NODE #102'));
                assert.ok(gridHtml.includes('⚡ 1x'));
            });
        });
    });

    describe('Sandbox Environment & DOM Fallbacks', () => {
        it('exercises browser stub methods cleanly', async () => {
            sandbox.window.addEventListener('load', () => {});
            sandbox.window.removeEventListener('load', () => {});
            const elem = sandbox.document.createElement('div');
            elem.setAttribute('data-test', 'true');
            elem.classList.add('active');
            elem.classList.remove('active');
            sandbox.document.addEventListener('click', () => {});
            assert.deepEqual(sandbox.document.querySelectorAll(), []);
            assert.equal(initialGetElementById('missing'), null);
            sandbox.document.getElementById = (id) => ({ id });
            assert.ok(sandbox.document.getElementById('missing'));
            await sandbox.navigator.clipboard.writeText('hello');
            assert.equal(sandbox.setInterval(() => {}, 1000), 1);
            sandbox.clearInterval(1);
            assert.equal(sandbox.setTimeout(() => {}, 1000), 1);
            sandbox.clearTimeout(1);
            const fetchRes = await sandbox.fetch('/api');
            const data = await fetchRes.json();
            assert.deepEqual(data, {});
            sandbox.console.log('info');
            sandbox.console.warn('warning');
            sandbox.console.error('error');
        });
    });

    describe('Permet HUD Deployment Pipeline Rendering (renderDeployments)', () => {
        let elements;
        let originalCreateElement;
        let originalGetElementById;

        const createMockNode = (tag = 'div') => {
            const node = {
                tagName: tag.toUpperCase(),
                className: '',
                innerHTML: '',
                textContent: '',
                children: [],
                appendChild: (child) => {
                    node.children.push(child);
                    return child;
                }
            };
            return node;
        };

        const setupDOM = () => {
            elements = {
                'deployments-container': createMockNode('div'),
                'deploy-count-badge': createMockNode('span')
            };
            originalCreateElement = sandbox.document.createElement;
            originalGetElementById = sandbox.document.getElementById;
            sandbox.document.createElement = (tag) => createMockNode(tag);
            sandbox.document.getElementById = (id) => elements[id] || null;
        };

        const teardownDOM = () => {
            sandbox.document.createElement = originalCreateElement;
            sandbox.document.getElementById = originalGetElementById;
        };

        it('renders multiple .deploy-card elements when 2 in-flight deploys exist', () => {
            setupDOM();
            try {
                const deployments = [
                    {
                        commit: 'abc1234',
                        stage: 'swapping',
                        service: 'aerial-stack',
                        steps: [
                            { name: 'Commit Trigger', status: 'completed' },
                            { name: 'CI Build & GHCR', status: 'completed' },
                            { name: 'Hangar Sync', status: 'completed' },
                            { name: 'Container Swap', status: 'active' },
                            { name: 'Health Check', status: 'pending' }
                        ]
                    },
                    {
                        commit: 'def5678',
                        stage: 'building',
                        service: 'aerial-stack',
                        steps: [
                            { name: 'Commit Trigger', status: 'completed' },
                            { name: 'CI Build & GHCR', status: 'active' },
                            { name: 'Hangar Sync', status: 'pending' },
                            { name: 'Container Swap', status: 'pending' },
                            { name: 'Health Check', status: 'pending' }
                        ]
                    }
                ];

                renderDeployments(deployments);

                const container = elements['deployments-container'];
                assert.equal(container.children.length, 2);
                assert.ok(container.children[0].className.includes('deploy-card stage-swapping'));
                assert.ok(!container.children[0].className.includes('deploy-card-compact'));
                assert.ok(container.children[1].className.includes('deploy-card stage-building'));
                assert.ok(!container.children[1].className.includes('deploy-card-compact'));
            } finally {
                teardownDOM();
            }
        });

        it('renders 3rd+ or live deployments as .deploy-card-compact', () => {
            setupDOM();
            try {
                const deployments = [
                    { commit: 'aaa1111', stage: 'swapping', service: 'aerial-stack' },
                    { commit: 'bbb2222', stage: 'building', service: 'aerial-stack' },
                    { commit: 'ccc3333', stage: 'queued', service: 'aerial-stack' },
                    { commit: 'ddd4444', stage: 'live', service: 'aerial-stack' }
                ];

                renderDeployments(deployments);

                const container = elements['deployments-container'];
                assert.equal(container.children.length, 4);
                // First 2 should be expanded deploy cards
                assert.ok(container.children[0].className.includes('deploy-card stage-swapping'));
                assert.ok(!container.children[0].className.includes('deploy-card-compact'));
                assert.ok(container.children[1].className.includes('deploy-card stage-building'));
                assert.ok(!container.children[1].className.includes('deploy-card-compact'));
                // 3rd and 4th should be compact cards
                assert.ok(container.children[2].className.includes('deploy-card-compact'));
                assert.ok(container.children[2].innerHTML.includes('ccc3333'));
                assert.ok(container.children[3].className.includes('deploy-card-compact'));
                assert.ok(container.children[3].innerHTML.includes('ddd4444'));
            } finally {
                teardownDOM();
            }
        });

        it('renders live deployment as compact when multi-deploy occurs', () => {
            setupDOM();
            try {
                const deployments = [
                    { commit: 'aaa1111', stage: 'building', service: 'aerial-stack' },
                    { commit: 'bbb2222', stage: 'live', service: 'aerial-stack' }
                ];

                renderDeployments(deployments);

                const container = elements['deployments-container'];
                assert.equal(container.children.length, 2);
                assert.ok(container.children[0].className.includes('deploy-card stage-building'));
                assert.ok(container.children[1].className.includes('deploy-card-compact stage-live'));
            } finally {
                teardownDOM();
            }
        });

        it('prioritizes multi-deploy badge precedence and displays N DEPLOYS IN PROGRESS', () => {
            setupDOM();
            try {
                const deployments = [
                    { commit: 'aaa1111', stage: 'swapping', service: 'aerial-stack' },
                    { commit: 'bbb2222', stage: 'building', service: 'aerial-stack' }
                ];

                renderDeployments(deployments);

                const badge = elements['deploy-count-badge'];
                assert.equal(badge.textContent, '2 DEPLOYS IN PROGRESS');
                assert.equal(badge.className, 'section-badge active');
            } finally {
                teardownDOM();
            }
        });

        it('displays failed and degraded badges with higher precedence over active deploys', () => {
            setupDOM();
            try {
                const deployments = [
                    { commit: 'aaa1111', stage: 'failed', service: 'aerial-stack' },
                    { commit: 'bbb2222', stage: 'building', service: 'aerial-stack' }
                ];

                renderDeployments(deployments);

                const badge = elements['deploy-count-badge'];
                assert.equal(badge.textContent, '🚨 CI BUILD FAILED');
                assert.equal(badge.className, 'section-badge failed');
            } finally {
                teardownDOM();
            }
        });

        it('strictly scopes Step 2 to CI chips and Step 4 to container chips', () => {
            setupDOM();
            try {
                const deployments = [
                    {
                        commit: 'abc1234',
                        stage: 'swapping',
                        service: 'aerial-stack',
                        matrix_jobs: [
                            { name: 'test-backend', type: 'ci', status: 'completed' },
                            { name: 'lint-go', type: 'ci', status: 'completed' },
                            { name: 'aerial-brain', type: 'container', status: 'active' },
                            { name: 'aerial-dashboard', type: 'container', status: 'completed' }
                        ]
                    }
                ];

                renderDeployments(deployments);

                const container = elements['deployments-container'];
                assert.equal(container.children.length, 1);
                const html = container.children[0].innerHTML;

                // Step 2 panel must contain CI chips, not container chips
                const step2Index = html.indexOf('CI Build &amp; GHCR');
                const step4Index = html.indexOf('Container Swap');
                assert.ok(step2Index !== -1, 'Step 2 should be rendered');
                assert.ok(step4Index !== -1, 'Step 4 should be rendered');

                const step2Section = html.slice(step2Index, step4Index);
                const step4Section = html.slice(step4Index);

                assert.ok(step2Section.includes('test-backend'));
                assert.ok(step2Section.includes('lint-go'));
                assert.ok(!step2Section.includes('aerial-brain'));
                assert.ok(!step2Section.includes('aerial-dashboard'));

                assert.ok(step4Section.includes('aerial-brain'));
                assert.ok(step4Section.includes('aerial-dashboard'));
                assert.ok(!step4Section.includes('test-backend'));
                assert.ok(!step4Section.includes('lint-go'));
            } finally {
                teardownDOM();
            }
        });
    });

    describe('Vanadis Build Queue & Chunk Drawer Unit Tests', () => {
        describe('classifyBuildQueueState(build)', () => {
            it('classifies terminal statuses as completed', () => {
                assert.equal(classifyBuildQueueState({ status: 'completed' }), 'completed');
                assert.equal(classifyBuildQueueState({ status: 'merged' }), 'completed');
                assert.equal(classifyBuildQueueState({ status: 'failed' }), 'completed');
                assert.equal(classifyBuildQueueState({ status: 'cancelled' }), 'completed');
                assert.equal(classifyBuildQueueState({ status: 'canceled' }), 'completed');
                assert.equal(classifyBuildQueueState('COMPLETED'), 'completed');
            });

            it('classifies queued and pending statuses as queued', () => {
                assert.equal(classifyBuildQueueState({ status: 'queued' }), 'queued');
                assert.equal(classifyBuildQueueState({ status: 'pending' }), 'queued');
                assert.equal(classifyBuildQueueState('QUEUED'), 'queued');
            });

            it('classifies running builds with all pending chunks as queued', () => {
                const build = {
                    status: 'running',
                    chunks: [
                        { id: 'c1', status: 'pending' },
                        { id: 'c2', status: 'pending' }
                    ]
                };
                assert.equal(classifyBuildQueueState(build), 'queued');
            });

            it('classifies running builds with active or finished chunks as running', () => {
                const buildRunningChunk = {
                    status: 'running',
                    chunks: [
                        { id: 'c1', status: 'completed' },
                        { id: 'c2', status: 'running' },
                        { id: 'c3', status: 'pending' }
                    ]
                };
                assert.equal(classifyBuildQueueState(buildRunningChunk), 'running');

                const buildNoChunks = { status: 'running', chunks: [] };
                assert.equal(classifyBuildQueueState(buildNoChunks), 'running');
            });

            it('falls back to build.queue_state or defaults to queued', () => {
                assert.equal(classifyBuildQueueState({ status: 'unknown', queue_state: 'running' }), 'running');
                assert.equal(classifyBuildQueueState({ status: 'unknown' }), 'queued');
                assert.equal(classifyBuildQueueState(null), 'queued');
            });
        });

        describe('computeBuildChunkStats(build)', () => {
            it('returns zero stats for null or undefined input', () => {
                const stats = computeBuildChunkStats(null);
                assert.equal(stats.total, 0);
                assert.equal(stats.completed, 0);
                assert.equal(stats.percent, 0);
            });

            it('computes stats from chunk array accurately', () => {
                const build = {
                    chunks: [
                        { id: 'c1', status: 'completed' },
                        { id: 'c2', status: 'merged' },
                        { id: 'c3', status: 'running' },
                        { id: 'c4', status: 'in_progress' },
                        { id: 'c5', status: 'failed' },
                        { id: 'c6', status: 'pending' }
                    ]
                };
                const stats = computeBuildChunkStats(build);
                assert.equal(stats.total, 6);
                assert.equal(stats.completed, 2);
                assert.equal(stats.running, 2);
                assert.equal(stats.failed, 1);
                assert.equal(stats.pending, 1);
                assert.equal(stats.percent, 33);
            });

            it('falls back to precomputed build chunk properties', () => {
                const build = {
                    total_chunks: 4,
                    completed_chunks: 2,
                    running_chunks: 1,
                    pending_chunks: 1,
                    failed_chunks: 0
                };
                const stats = computeBuildChunkStats(build);
                assert.equal(stats.total, 4);
                assert.equal(stats.completed, 2);
                assert.equal(stats.percent, 50);
            });
        });

        describe('getBuildTimingInfo(build, nowMs)', () => {
            it('computes compact 12-hr am/pm start, end, and duration for same-day, cross-day, running, and queued builds', () => {
                const nullTiming = getBuildTimingInfo(null);
                assert.equal(nullTiming.startText, '--');
                assert.equal(nullTiming.endText, '');
                assert.equal(nullTiming.durationText, '--');

                const sameDayStart = new Date(2026, 9, 17, 8, 0, 5).toISOString();
                const sameDayEnd = new Date(2026, 9, 17, 14, 15, 35).toISOString();
                const completedBuild = {
                    status: 'completed',
                    created_at: sameDayStart,
                    updated_at: sameDayEnd,
                    chunks: [
                        { id: 'c1', status: 'merged', started_at: sameDayStart, updated_at: sameDayEnd }
                    ]
                };
                const compTiming = getBuildTimingInfo(completedBuild);
                assert.equal(compTiming.startText, '10/17 8:00am');
                assert.equal(compTiming.endText, '2:15pm');
                assert.equal(compTiming.durationText, '6h 15m');

                const crossDayStart = new Date(2026, 9, 17, 12, 30, 0).toISOString();
                const crossDayEnd = new Date(2026, 9, 18, 0, 5, 35).toISOString();
                const crossDayBuild = {
                    status: 'completed',
                    created_at: crossDayStart,
                    updated_at: crossDayEnd,
                    chunks: [
                        { id: 'c1', status: 'merged', started_at: crossDayStart, updated_at: crossDayEnd }
                    ]
                };
                const crossTiming = getBuildTimingInfo(crossDayBuild);
                assert.equal(crossTiming.startText, '10/17 12:30pm');
                assert.equal(crossTiming.endText, '10/18 12:05am');

                const runStart = new Date(2026, 9, 17, 8, 1, 0).toISOString();
                const runningBuild = {
                    status: 'running',
                    created_at: sameDayStart,
                    chunks: [
                        { id: 'c1', status: 'running', started_at: runStart },
                        { id: 'c2', status: 'pending', started_at: '0001-01-01T00:00:00Z' }
                    ]
                };
                const nowMs = new Date(2026, 9, 17, 8, 5, 12).getTime();
                const runTiming = getBuildTimingInfo(runningBuild, nowMs);
                assert.equal(runTiming.startText, '10/17 8:01am');
                assert.equal(runTiming.endText, '');
                assert.equal(runTiming.durationText, '4m 12s');

                const queuedBuild = {
                    status: 'queued',
                    created_at: sameDayStart,
                    chunks: [
                        { id: 'c1', status: 'pending', started_at: '0001-01-01T00:00:00Z' }
                    ]
                };
                const qTiming = getBuildTimingInfo(queuedBuild, nowMs);
                assert.equal(qTiming.startText, '10/17 8:00am');
                assert.equal(qTiming.endText, '');
                assert.equal(qTiming.durationText, '--');
            });
        });

        describe('renderBuildQueueCards() & Filtering', () => {
            it('renders build cards and updates results count', () => {
                const elements = {};
                sandbox.document.getElementById = (id) => {
                    if (!elements[id]) {
                        elements[id] = {
                            textContent: '',
                            innerHTML: '',
                            children: [],
                            appendChild: function(child) {
                                this.children.push(child);
                            }
                        };
                    }
                    return elements[id];
                };

                sandbox.document.createElement = (tag) => {
                    return {
                        tagName: tag.toUpperCase(),
                        className: '',
                        attributes: {},
                        setAttribute: function(k, v) { this.attributes[k] = v; },
                        getAttribute: function(k) { return this.attributes[k]; },
                        style: {},
                        innerHTML: ''
                    };
                };

                buildsState.builds = [
                    {
                        id: 'build-101',
                        tier: 2,
                        title: 'Add Status Dashboard View',
                        objective: 'Render HUD build queue',
                        status: 'running',
                        created_at: '2026-10-17T08:00:00Z',
                        total_chunks: 4,
                        completed_chunks: 2,
                        chunks: [
                            { id: 'c1', status: 'completed', repo: 'azylman/aerial', started_at: '2026-10-17T08:00:05Z', updated_at: '2026-10-17T08:05:00Z' },
                            { id: 'c2', status: 'completed', repo: 'azylman/aerial', started_at: '2026-10-17T08:05:05Z', updated_at: '2026-10-17T08:10:00Z' },
                            { id: 'c3', status: 'running', repo: 'azylman/aerial', started_at: '2026-10-17T08:10:05Z' },
                            { id: 'c4', status: 'pending', repo: 'azylman/aerial' }
                        ]
                    },
                    {
                        id: 'build-102',
                        tier: 1,
                        title: 'Fix nomad discovery',
                        objective: 'Repair discovery bug',
                        status: 'completed',
                        created_at: '2026-10-17T07:00:00Z',
                        updated_at: '2026-10-17T07:12:45Z',
                        total_chunks: 1,
                        completed_chunks: 1,
                        chunks: [
                            { id: 'c5', status: 'completed', repo: 'azylman/aerial-sidecars', started_at: '2026-10-17T07:00:00Z', updated_at: '2026-10-17T07:12:45Z' }
                        ]
                    }
                ];
                buildsState.selectedFilter = 'ALL';
                buildsState.searchQuery = '';

                renderBuildQueueCards();

                assert.equal(elements['builds-results-count'].textContent, 'SHOWING 2 / 2 BUILDS');
                assert.equal(elements['builds-grid'].children.length, 2);

                const card0 = elements['builds-grid'].children[0];
                assert.ok(card0.innerHTML.includes('Add Status Dashboard View'));
                assert.ok(!card0.innerHTML.includes('TIER 2'), 'top-level card should omit tier badge');
                assert.ok(!card0.innerHTML.includes('build-tier-badge'), 'top-level card should omit build-tier-badge');
                assert.ok(!card0.innerHTML.includes('build-101'), 'top-level card should omit build ID chip');
                assert.ok(!card0.innerHTML.includes('Render HUD build queue'), 'top-level card should omit objective preview');
                assert.ok(card0.innerHTML.includes('build-duration-badge'), 'top-level card should include duration badge');
                assert.ok(card0.innerHTML.includes('START:'), 'top-level card should include START time');
                assert.ok(!card0.innerHTML.includes('END:'), 'running top-level card should omit END time');
                assert.ok(card0.innerHTML.includes('2 / 4 CHUNKS'));
                assert.ok(!card0.innerHTML.includes('build-repo-badge'));
                assert.equal(card0.getAttribute('data-build-id'), 'build-101');
                assert.ok(typeof card0.onclick === 'function');

                const card1 = elements['builds-grid'].children[1];
                assert.ok(card1.innerHTML.includes('12m 45s'), 'completed card should display computed duration');
                assert.ok(card1.innerHTML.includes('END:'), 'completed top-level card should include END time');
            });

            it('filters build cards by queue state, search query, and chunk repo', () => {
                const elements = {};
                sandbox.document.getElementById = (id) => {
                    if (!elements[id]) {
                        elements[id] = {
                            textContent: '',
                            innerHTML: '',
                            children: [],
                            appendChild: function(child) {
                                this.children.push(child);
                            }
                        };
                    }
                    return elements[id];
                };

                buildsState.selectedFilter = 'COMPLETED';
                buildsState.searchQuery = 'nomad';

                renderBuildQueueCards();

                assert.equal(elements['builds-results-count'].textContent, 'SHOWING 1 / 2 BUILDS');
                assert.equal(elements['builds-grid'].children.length, 1);
                assert.ok(elements['builds-grid'].children[0].innerHTML.includes('Fix nomad discovery'));

                // Filter by chunk repo
                elements['builds-grid'].children = [];
                buildsState.selectedFilter = 'ALL';
                buildsState.searchQuery = 'aerial-sidecars';

                renderBuildQueueCards();

                assert.equal(elements['builds-results-count'].textContent, 'SHOWING 1 / 2 BUILDS');
                assert.equal(elements['builds-grid'].children.length, 1);
                assert.ok(elements['builds-grid'].children[0].innerHTML.includes('Fix nomad discovery'));

                // Reset filter and search
                buildsState.selectedFilter = 'ALL';
                buildsState.searchQuery = '';
            });
        });

        describe('renderBuildDetailContent(build, chunks)', () => {
            it('renders detailed build content and ordered chunks in drawer', () => {
                const build = {
                    id: 'build-999',
                    tier: 2,
                    title: 'Vanadis HUD Integration',
                    objective: 'Integrate build queue into dashboard',
                    shared_context: 'Vanadis runs on http://vanadis.aerial',
                    status: 'running'
                };
                const chunks = [
                    {
                        id: 'chunk-1',
                        chunk_index: 1,
                        title: 'Add queue endpoint',
                        status: 'completed',
                        effort: 'low',
                        repo: 'azylman/aerial',
                        pr_number: 779,
                        target_files: ['vanadis/main.go'],
                        target_symbols: ['buildsHandler'],
                        acceptance_criteria: ['unit tests pass'],
                        instructions: 'Implement GET /api/builds'
                    },
                    {
                        id: 'chunk-2',
                        chunk_index: 2,
                        title: 'Render UI drawer',
                        status: 'in_progress',
                        effort: 'medium',
                        repo: 'azylman/aerial',
                        target_files: ['dashboard/static/app.js'],
                        target_symbols: ['renderBuildDetailContent'],
                        acceptance_criteria: ['drawer opens'],
                        instructions: 'Add drawer rendering logic'
                    }
                ];

                const html = renderBuildDetailContent(build, chunks);

                assert.ok(html.includes('Vanadis HUD Integration'));
                assert.ok(html.includes('Integrate build queue into dashboard'));
                assert.ok(html.includes('Vanadis runs on http://vanadis.aerial'));
                assert.ok(html.includes('TIER 2'));
                assert.ok(!html.includes('build-repo-badge'));
                assert.ok(html.includes('chunk-repo-badge'));
                assert.ok(html.includes('#1'));
                assert.ok(html.includes('Add queue endpoint'));
                assert.ok(html.includes('#779 ↗'));
                assert.ok(html.includes('https://github.com/azylman/aerial/pull/779'));
                assert.ok(html.includes('vanadis/main.go'));
                assert.ok(html.includes('buildsHandler'));
                assert.ok(html.includes('unit tests pass'));
                assert.ok(html.includes('Implement GET /api/builds'));
                assert.ok(html.includes('<details class="chunk-section chunk-instructions-details">'));
                assert.ok(!html.includes('<details class="chunk-section chunk-instructions-details" open'));
                assert.ok(html.includes('<summary class="chunk-section-label chunk-instructions-toggle">'));
                assert.ok(html.includes('1 / 2 CHUNKS COMPLETED'));
            });

            it('escapes HTML special characters in build and chunk fields', () => {
                const dangerousBuild = {
                    id: 'build-<xss>',
                    title: '<script>alert("build")</script>',
                    objective: '<b>Bold</b> & dangerous',
                    status: 'running'
                };
                const dangerousChunks = [
                    {
                        chunk_index: 1,
                        title: '<img src=x onerror=alert(1)>',
                        status: 'pending',
                        instructions: '<script>evil()</script>'
                    }
                ];

                const html = renderBuildDetailContent(dangerousBuild, dangerousChunks);
                assert.ok(!html.includes('<script>alert("build")</script>'));
                assert.ok(html.includes('&lt;script&gt;alert(&quot;build&quot;)&lt;/script&gt;'));
                assert.ok(!html.includes('<img src=x onerror=alert(1)>'));
                assert.ok(!html.includes('<script>evil()</script>'));
            });

            it('renders markdown in objective, shared_context, instructions, and acceptance_criteria safely', () => {
                assert.equal(typeof sandbox.renderMarkdown, 'function', 'renderMarkdown must be exported');
                assert.equal(typeof sandbox.renderInlineMarkdown, 'function', 'renderInlineMarkdown must be exported');
                assert.equal(sandbox.renderMarkdown(null), '');
                assert.equal(sandbox.renderMarkdown(''), '');

                const mdSample = [
                    '### Build Spec',
                    'Use **strict** and *clean* `go test` with [Docs](https://github.com/azylman/aerial) and [Evil](javascript:alert(1)).',
                    '',
                    '- First bullet',
                    '- Second `code` bullet',
                    '',
                    '1. Step one',
                    '2. Step two',
                    '',
                    '> Quoted invariant',
                    '',
                    '```go',
                    'func main() { fmt.Println("<ok>") }',
                    '```'
                ].join('\n');

                const rendered = sandbox.renderMarkdown(mdSample);
                assert.ok(rendered.includes('<h3 class="md-heading md-h3">Build Spec</h3>'));
                assert.ok(rendered.includes('<strong>strict</strong>'));
                assert.ok(rendered.includes('<em>clean</em>'));
                assert.ok(rendered.includes('<code class="md-inline-code">go test</code>'));
                assert.ok(rendered.includes('<a href="https://github.com/azylman/aerial" target="_blank" rel="noopener noreferrer" class="md-link">Docs</a>'));
                assert.ok(!rendered.includes('href="javascript:'), 'javascript: URLs must be rejected');
                assert.ok(rendered.includes('<ul class="md-list"><li>First bullet</li><li>Second <code class="md-inline-code">code</code> bullet</li></ul>'));
                assert.ok(rendered.includes('<ol class="md-list md-ol"><li>Step one</li><li>Step two</li></ol>'));
                assert.ok(rendered.includes('<blockquote class="md-blockquote">Quoted invariant</blockquote>'));
                assert.ok(rendered.includes('<pre class="md-code-block"><code data-lang="go">func main() { fmt.Println(&quot;&lt;ok&gt;&quot;) }</code></pre>'));

                const build = {
                    id: 'build-md',
                    tier: 2,
                    title: 'Markdown Overlay Build',
                    objective: 'Render **bold objective** with `inlineCode`',
                    shared_context: '## Shared Header\n- Context item 1',
                    status: 'running'
                };
                const chunks = [
                    {
                        chunk_index: 1,
                        title: 'Markdown Chunk',
                        status: 'running',
                        acceptance_criteria: ['Verify `node --test` passes **cleanly**'],
                        instructions: '### Step 1\nRun `scripts/verify.sh --staged`'
                    }
                ];

                const html = renderBuildDetailContent(build, chunks);
                assert.ok(html.includes('class="build-objective-text markdown-body"'));
                assert.ok(html.includes('<strong>bold objective</strong>'));
                assert.ok(html.includes('<code class="md-inline-code">inlineCode</code>'));
                assert.ok(html.includes('class="build-context-box markdown-body"'));
                assert.ok(html.includes('<h2 class="md-heading md-h2">Shared Header</h2>'));
                assert.ok(html.includes('class="chunk-instructions-box markdown-body"'));
                assert.ok(html.includes('<h3 class="md-heading md-h3">Step 1</h3>'));
                assert.ok(html.includes('<li>Verify <code class="md-inline-code">node --test</code> passes <strong>cleanly</strong></li>'));
            });
        });

        describe('updateBuildsMetrics() & renderBuildsPills()', () => {
            it('updates summary counts, progress, and badge count', () => {
                const elements = {};
                sandbox.document.getElementById = (id) => {
                    if (!elements[id]) {
                        elements[id] = { textContent: '', className: '', style: {} };
                    }
                    return elements[id];
                };

                buildsState.builds = [
                    { id: 'b1', status: 'running', total_chunks: 2, completed_chunks: 1 },
                    { id: 'b2', status: 'queued', total_chunks: 3, completed_chunks: 0 },
                    { id: 'b3', status: 'completed', total_chunks: 1, completed_chunks: 1 }
                ];
                buildsState.runningCount = undefined;
                buildsState.queuedCount = undefined;
                buildsState.completedCount = undefined;
                buildsState.totalCount = 3;

                updateBuildsMetrics();

                assert.equal(elements['builds-running-count'].textContent, '1');
                assert.equal(elements['builds-queued-count'].textContent, '1');
                assert.equal(elements['builds-completed-count'].textContent, '1');
                assert.equal(elements['builds-chunks-progress'].textContent, '2/6');
                assert.equal(elements['builds-badge-count'].textContent, '2');

                assert.equal(elements['pill-count-builds-all'].textContent, '3');
                assert.equal(elements['pill-count-builds-running'].textContent, '1');
                assert.equal(elements['pill-count-builds-queued'].textContent, '1');
                assert.equal(elements['pill-count-builds-completed'].textContent, '1');
            });
        });

        describe('TABS.builds Lifecycle', () => {
            it('fetches builds and starts/stops polling on enter and leave', () => {
                assert.ok(TABS.builds, 'TABS.builds must be registered');
                assert.equal(typeof TABS.builds.onEnter, 'function');
                assert.equal(typeof TABS.builds.onLeave, 'function');

                vm.runInContext("fetchBuilds = () => { buildsState.fetchCount = (buildsState.fetchCount || 0) + 1; };", sandbox);

                TABS.builds.onEnter();
                assert.ok(sandbox.buildsState.fetchCount >= 1, 'onEnter should trigger fetchBuilds');

                TABS.builds.onLeave();
            });

            it('positions diagnostic-drawer near top of viewport and resets scrollTop on openBuildDetailDrawer', () => {
                const cssCode = fs.readFileSync(path.join(__dirname, 'static', 'style.css'), 'utf8');
                const drawerMatch = cssCode.match(/\.diagnostic-drawer\s*\{([^}]+)\}/);
                assert.ok(drawerMatch, '.diagnostic-drawer rule must exist');
                assert.ok(drawerMatch[1].includes('top: 5vh'), '.diagnostic-drawer must anchor near top of viewport');
                assert.ok(drawerMatch[1].includes('max-height: 90vh'), '.diagnostic-drawer must allow up to 90vh height');
                assert.ok(!drawerMatch[1].includes('height: 420px'), '.diagnostic-drawer must not hardcode 420px bottom tray height');

                const bodyEl = { innerHTML: '', scrollTop: 320 };
                const drawerEl = { classList: { add: () => {} } };
                const overlayEl = { classList: { add: () => {} } };
                const titleEl = { textContent: '' };
                sandbox.document.getElementById = (id) => {
                    if (id === 'drawer-body') return bodyEl;
                    if (id === 'diagnostic-drawer') return drawerEl;
                    if (id === 'drawer-overlay') return overlayEl;
                    if (id === 'drawer-service-name') return titleEl;
                    return null;
                };
                sandbox.fetch = () => Promise.resolve({ ok: true, json: () => Promise.resolve({ build: { id: 'b-top' }, chunks: [] }) });

                sandbox.openBuildDetailDrawer('b-top');
                assert.equal(bodyEl.scrollTop, 0, 'openBuildDetailDrawer should reset drawer-body scrollTop to 0');
            });

            it('renders .builds-grid as a single-column layout across all viewports', () => {
                const cssCode = fs.readFileSync(path.join(__dirname, 'static', 'style.css'), 'utf8');
                const buildsGridMatches = [...cssCode.matchAll(/\.builds-grid\s*\{([^}]+)\}/g)];
                assert.ok(buildsGridMatches.length >= 1, '.builds-grid rule must exist in style.css');
                for (const match of buildsGridMatches) {
                    assert.ok(match[1].includes('grid-template-columns: 1fr'), `.builds-grid must use single-column 1fr layout, got: ${match[1].trim()}`);
                    assert.ok(!match[1].includes('repeat('), `.builds-grid must not use multi-column repeat(), got: ${match[1].trim()}`);
                }
            });

            it('renders loading skeleton in index.html and renderBuildsSkeleton while preserving detail cache', () => {
                const htmlCode = fs.readFileSync(path.join(__dirname, 'static', 'index.html'), 'utf8');
                const gridIdx = htmlCode.indexOf('id="builds-grid"');
                assert.ok(gridIdx !== -1, '#builds-grid must exist in index.html');
                assert.ok(htmlCode.slice(gridIdx, gridIdx + 450).includes('fact-card-skeleton'), '#builds-grid should contain initial loading skeleton');
                assert.equal(typeof sandbox.renderBuildsSkeleton, 'function', 'renderBuildsSkeleton must be defined');

                const gridEl = { innerHTML: '' };
                sandbox.document.getElementById = (id) => (id === 'builds-grid' ? gridEl : null);
                sandbox.renderBuildsSkeleton();
                assert.ok(gridEl.innerHTML.includes('fact-card-skeleton'));
            });
        });
    });

});
