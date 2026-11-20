'use strict';

// Unit tests for the shared theme script: node --test web/theme/aerial-theme.test.js
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const theme = require('./aerial-theme.js');

const SOURCE = fs.readFileSync(path.join(__dirname, 'aerial-theme.js'), 'utf8');
const TOKENS = fs.readFileSync(path.join(__dirname, 'aerial-tokens.css'), 'utf8');

test('parseThemeCookie', async (t) => {
  const tests = [
    { name: 'single dark entry', cookie: 'theme=dark', want: 'dark' },
    { name: 'single light entry among others', cookie: 'a=1; theme=light; b=2', want: 'light' },
    { name: 'first entry wins over a later one', cookie: 'theme=light; theme=dark', want: 'light' },
    { name: 'invalid first value is skipped like Gatus', cookie: 'theme=sepia; theme=dark', want: 'dark' },
    { name: 'suffix-named cookie does not count', cookie: 'xtheme=light; theme=dark', want: 'dark' },
    { name: 'longer value is not a match', cookie: 'theme=darker', want: null },
    { name: 'empty value', cookie: 'theme=', want: null },
    { name: 'no theme cookie', cookie: 'session=abc', want: null },
    { name: 'empty string', cookie: '', want: null },
    { name: 'non-string input', cookie: undefined, want: null },
  ];
  for (const tc of tests) {
    await t.test(tc.name, () => {
      assert.equal(theme.parseThemeCookie(tc.cookie), tc.want);
    });
  }
});

test('countThemeCookies', async (t) => {
  const tests = [
    { name: 'none', cookie: 'a=1; b=2', want: 0 },
    { name: 'one', cookie: 'a=1; theme=dark', want: 1 },
    { name: 'two including an invalid value', cookie: 'theme=dark; a=1; theme=bogus', want: 2 },
    { name: 'suffix-named cookie ignored', cookie: 'xtheme=dark; theme=light', want: 1 },
    { name: 'empty string', cookie: '', want: 0 },
    { name: 'non-string input', cookie: null, want: 0 },
  ];
  for (const tc of tests) {
    await t.test(tc.name, () => {
      assert.equal(theme.countThemeCookies(tc.cookie), tc.want);
    });
  }
});

test('candidateCookieDomains', async (t) => {
  const tests = [
    { name: 'deep host under a two-label suffix', host: 'a.b.example.co.uk', want: ['co.uk', 'example.co.uk', 'b.example.co.uk', 'a.b.example.co.uk'] },
    { name: 'subdomain', host: 'app.example.com', want: ['example.com', 'app.example.com'] },
    { name: 'apex', host: 'example.com', want: ['example.com'] },
    { name: 'trailing dot and upper case', host: 'App.Example.COM.', want: ['example.com', 'app.example.com'] },
    { name: 'single label', host: 'intranet', want: [] },
    { name: 'localhost', host: 'localhost', want: [] },
    { name: 'IPv4', host: '192.168.1.20', want: [] },
    { name: 'IPv6', host: '[::1]', want: [] },
    { name: 'empty', host: '', want: [] },
    { name: 'non-string', host: undefined, want: [] },
  ];
  for (const tc of tests) {
    await t.test(tc.name, () => {
      assert.deepEqual(theme.candidateCookieDomains(tc.host), tc.want);
    });
  }
});

test('themeCookieDomain', async (t) => {
  const tests = [
    { name: 'skips a rejected public suffix', host: 'a.b.example.co.uk', accepted: ['example.co.uk', 'b.example.co.uk'], want: 'example.co.uk' },
    { name: 'takes the apex of a subdomain', host: 'status.example.com', accepted: ['example.com', 'status.example.com'], want: 'example.com' },
    { name: 'falls back to host-only when nothing sticks', host: 'status.example.com', accepted: [], want: '' },
    { name: 'host-only for localhost', host: 'localhost', accepted: ['localhost'], want: '' },
  ];
  for (const tc of tests) {
    await t.test(tc.name, () => {
      assert.equal(theme.themeCookieDomain(tc.host, (d) => tc.accepted.includes(d)), tc.want);
    });
  }
});

test('serializeThemeCookie', async (t) => {
  const tests = [
    { name: 'dark on a shared domain over https', args: ['dark', 'example.com', true], want: 'theme=dark; path=/; max-age=31536000; samesite=lax; domain=example.com; secure' },
    { name: 'light host-only over http', args: ['light', '', false], want: 'theme=light; path=/; max-age=31536000; samesite=lax' },
    { name: 'light on a shared domain over http', args: ['light', 'example.org', false], want: 'theme=light; path=/; max-age=31536000; samesite=lax; domain=example.org' },
    { name: 'rejects an unknown theme', args: ['sepia', 'example.com', true], wantErr: true },
  ];
  for (const tc of tests) {
    await t.test(tc.name, () => {
      if (tc.wantErr) {
        assert.throws(() => theme.serializeThemeCookie(...tc.args), TypeError);
      } else {
        assert.equal(theme.serializeThemeCookie(...tc.args), tc.want);
      }
    });
  }
});

test('resolveTheme', async (t) => {
  const tests = [
    { name: 'cookie beats stored and media', input: { cookie: 'theme=light', stored: 'dark', prefersDark: true }, want: 'light' },
    { name: 'stored beats media', input: { cookie: '', stored: 'light', prefersDark: true }, want: 'light' },
    { name: 'invalid stored falls through to media light', input: { cookie: 'a=1', stored: 'sepia', prefersDark: false }, want: 'light' },
    { name: 'media dark', input: { cookie: '', stored: null, prefersDark: true }, want: 'dark' },
    { name: 'unknown media defaults dark', input: { cookie: '', stored: null }, want: 'dark' },
    { name: 'no input defaults dark', input: undefined, want: 'dark' },
  ];
  for (const tc of tests) {
    await t.test(tc.name, () => {
      assert.equal(theme.resolveTheme(tc.input), tc.want);
    });
  }
});

test('aerial-tokens.css RGB triplets match their hex colours', () => {
  // Every literal hex primitive has a -rgb twin for rgb(var(--x) / a) consumers; derive the
  // triplet from the hex so the two can never drift apart.
  const hexDecls = [...TOKENS.matchAll(/(--ae-(?:dark|light)-[a-z-]+):\s*#([0-9a-f]{6});/g)];
  assert.ok(hexDecls.length > 0, 'expected hex primitives in aerial-tokens.css');
  for (const [, prop, hex] of hexDecls) {
    const want = [0, 2, 4].map((i) => parseInt(hex.slice(i, i + 2), 16)).join(' ');
    const m = new RegExp(`${prop}-rgb:\\s*([^;]+);`).exec(TOKENS);
    assert.ok(m, `${prop}-rgb is missing`);
    assert.equal(m[1].trim(), want, `${prop}-rgb should match #${hex}`);
  }
});

// A minimal cookie jar: Domain cookies are rejected for public suffixes or foreign hosts, an
// overwrite keeps the original position (Chrome keeps the creation time), and the getter lists
// entries oldest first.
function fakeBrowser(hostname, protocol, preset, publicSuffixes) {
  const jar = [];
  const writes = [];
  const listeners = {};
  const store = new Map();
  const put = (name, value, domain) => {
    const existing = jar.find((c) => c.name === name && c.domain === domain);
    if (existing) existing.value = value;
    else jar.push({ name, value, domain });
  };
  for (const c of preset) put(c.name, c.value, c.domain);
  const document = {
    visibilityState: 'visible',
    get cookie() {
      return jar.map((c) => `${c.name}=${c.value}`).join('; ');
    },
    set cookie(str) {
      writes.push(str);
      const [pair, ...attrs] = str.split(';').map((s) => s.trim());
      const eq = pair.indexOf('=');
      const name = pair.slice(0, eq);
      const value = pair.slice(eq + 1);
      let domain = '';
      let expire = false;
      for (const a of attrs) {
        const [k, v] = a.split('=');
        if (k.toLowerCase() === 'domain') domain = v;
        if (k.toLowerCase() === 'max-age' && v === '0') expire = true;
      }
      if (domain && (publicSuffixes.includes(domain) || !(hostname === domain || hostname.endsWith(`.${domain}`)))) return;
      if (expire) {
        const idx = jar.findIndex((c) => c.name === name && c.domain === domain);
        if (idx !== -1) jar.splice(idx, 1);
        return;
      }
      put(name, value, domain);
    },
    addEventListener(type, fn) {
      (listeners[type] ||= []).push(fn);
    },
    removeEventListener() {},
  };
  const window = {
    document,
    location: { hostname, protocol },
    localStorage: { getItem: (k) => (store.has(k) ? store.get(k) : null), setItem: (k, v) => store.set(k, String(v)) },
    matchMedia: () => ({ matches: true }),
    addEventListener(type, fn) {
      (listeners[type] ||= []).push(fn);
    },
    removeEventListener() {},
  };
  vm.runInNewContext(SOURCE, { window });
  return { api: window.AerialTheme, jar, writes, store, listeners, document };
}

test('browser runtime set()', async (t) => {
  const tests = [
    {
      name: 'writes the shared domain cookie and storage signal, then notifies',
      host: 'console.example.com',
      protocol: 'https:',
      preset: [],
      setTo: 'light',
      wantJar: [{ name: 'theme', value: 'light', domain: 'example.com' }],
      wantStored: 'light',
      wantNotified: ['light'],
    },
    {
      name: 'skips a public suffix when discovering the domain',
      host: 'status.home.example.co.uk',
      protocol: 'http:',
      preset: [],
      setTo: 'dark',
      wantJar: [{ name: 'theme', value: 'dark', domain: 'example.co.uk' }],
      wantStored: 'dark',
      wantNotified: ['dark'],
    },
    {
      name: 'unchanged value writes nothing and notifies nobody',
      host: 'console.example.com',
      protocol: 'https:',
      preset: [{ name: 'theme', value: 'dark', domain: 'example.com' }],
      setTo: 'dark',
      wantJar: [{ name: 'theme', value: 'dark', domain: 'example.com' }],
      wantStored: null,
      wantNotified: [],
    },
    {
      name: 'a stale host-only cookie is dropped on load so the shared one wins',
      host: 'status.example.com',
      protocol: 'https:',
      preset: [
        { name: 'theme', value: 'light', domain: '' },
        { name: 'theme', value: 'dark', domain: 'example.com' },
      ],
      setTo: 'dark',
      wantJar: [{ name: 'theme', value: 'dark', domain: 'example.com' }],
      wantStored: null,
      wantNotified: [],
    },
    {
      name: 'host-only fallback on localhost',
      host: 'localhost',
      protocol: 'http:',
      preset: [],
      setTo: 'light',
      wantJar: [{ name: 'theme', value: 'light', domain: '' }],
      wantStored: 'light',
      wantNotified: ['light'],
    },
    {
      name: 'an unknown theme is ignored',
      host: 'console.example.com',
      protocol: 'https:',
      preset: [],
      setTo: 'sepia',
      wantJar: [],
      wantStored: null,
      wantNotified: [],
    },
  ];
  for (const tc of tests) {
    await t.test(tc.name, () => {
      const b = fakeBrowser(tc.host, tc.protocol, tc.preset, ['com', 'co.uk', 'uk']);
      const notified = [];
      const unsubscribe = b.api.subscribe((v) => notified.push(v));
      b.api.set(tc.setTo);
      unsubscribe();
      assert.deepEqual(b.jar, tc.wantJar);
      assert.equal(b.store.has('aerial-theme') ? b.store.get('aerial-theme') : null, tc.wantStored);
      assert.deepEqual(notified, tc.wantNotified);
    });
  }
});

test('browser runtime live updates', async (t) => {
  // The shared cookie starts dark; cookieNow is what this tab reads when the event lands (a
  // storage event can arrive before the renderer sees the cookie another tab just wrote).
  const tests = [
    { name: 'storage event for the shared key', event: 'storage', payload: { key: 'aerial-theme', newValue: 'light' }, cookieNow: 'light', want: ['light'] },
    { name: 'storage event trusts its value while the cookie is still stale', event: 'storage', payload: { key: 'aerial-theme', newValue: 'light' }, cookieNow: 'dark', want: ['light'] },
    { name: 'storage event repeating the current theme is ignored', event: 'storage', payload: { key: 'aerial-theme', newValue: 'dark' }, cookieNow: 'dark', want: [] },
    { name: 'storage event without a theme value re-reads the cookie', event: 'storage', payload: { key: 'aerial-theme', newValue: null }, cookieNow: 'light', want: ['light'] },
    { name: 'storage event for another key is ignored', event: 'storage', payload: { key: 'other', newValue: 'light' }, cookieNow: 'light', want: [] },
    { name: 'tab focus re-reads the cookie', event: 'focus', payload: {}, cookieNow: 'light', want: ['light'] },
    { name: 'tab becoming visible re-reads the cookie', event: 'visibilitychange', payload: {}, cookieNow: 'light', want: ['light'] },
  ];
  for (const tc of tests) {
    await t.test(tc.name, () => {
      const b = fakeBrowser('console.example.com', 'https:', [{ name: 'theme', value: 'dark', domain: 'example.com' }], ['com']);
      const notified = [];
      b.api.subscribe((v) => notified.push(v));
      b.document.cookie = `theme=${tc.cookieNow}; path=/; domain=example.com`;
      for (const fn of b.listeners[tc.event] || []) fn(tc.payload);
      assert.deepEqual(notified, tc.want);
    });
  }
});
