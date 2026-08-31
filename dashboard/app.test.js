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
    formatDuration,
    formatTimestamp,
    formatTimeAgo,
    formatCountdown
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
            assert.equal(formatAgentsviewSessionUrl(uuid), `/sessions/antigravity-cli%3A${uuid}`);
        });

        it('preserves already prefixed session IDs while encoding special characters', () => {
            const sessionPath = 'antigravity-cli:7a874a91-3afb-4eb0-bb62-8d96b9a3ae0b';
            assert.equal(formatAgentsviewSessionUrl(sessionPath), '/sessions/antigravity-cli%3A7a874a91-3afb-4eb0-bb62-8d96b9a3ae0b');
        });

        it('strips leading and trailing slashes safely', () => {
            assert.equal(formatAgentsviewSessionUrl('/sessions/abc/'), '/sessions/antigravity-cli%3Asessions%2Fabc');
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

    describe('Privacy & Zero-Hardcoded IP Guarantee', () => {
        it('verifies app.js contains zero private LAN IPs (192.168.x.x)', () => {
            const privateIpPattern = /192\.168\.\d+\.\d+/g;
            const matches = appJsCode.match(privateIpPattern);
            assert.equal(matches, null, `Found private IP leak in app.js: ${JSON.stringify(matches)}`);
        });
    });
});
