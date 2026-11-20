/*
 * Shared light/dark theme state for every first-party Aerial web frontend.
 *
 * A framework-free classic script: no imports, no build step. Browsers get window.AerialTheme;
 * node (unit tests) gets the same object through module.exports. Pure helpers are exported next
 * to the browser API so they can be tested without a DOM.
 *
 * Canonical state is the cookie `theme=dark|light` on the widest parent domain the browser will
 * accept, discovered at runtime (never hardcoded). Gatus reads a cookie with that exact name on
 * load, so it follows the shared choice without any injected script. localStorage 'aerial-theme'
 * is the same-origin live signal: writing it fires 'storage' in every other same-origin tab.
 * Cross-subdomain tabs re-read the cookie on 'visibilitychange' and 'focus'. No polling.
 */
(function (root) {
  'use strict';

  var COOKIE_NAME = 'theme';
  var STORAGE_KEY = 'aerial-theme';
  var MAX_AGE_SECONDS = 31536000;
  var PROBE_COOKIE = '__aerial_theme_probe';
  // First entry named exactly `theme` whose value is dark or light, the same entry Gatus picks
  // with its `theme=(dark|light)` match, but anchored so `xtheme=` never counts.
  var THEME_COOKIE_RE = /(?:^|;\s*)theme=(dark|light)(?=;|\s|$)/;

  function normalizeTheme(value) {
    return value === 'dark' || value === 'light' ? value : null;
  }

  function parseThemeCookie(cookieString) {
    if (typeof cookieString !== 'string') return null;
    var match = THEME_COOKIE_RE.exec(cookieString);
    return match ? match[1] : null;
  }

  function countThemeCookies(cookieString) {
    if (typeof cookieString !== 'string' || cookieString === '') return 0;
    var parts = cookieString.split(';');
    var count = 0;
    for (var i = 0; i < parts.length; i++) {
      var entry = parts[i].trim();
      var eq = entry.indexOf('=');
      if (eq !== -1 && entry.slice(0, eq) === COOKIE_NAME) count++;
    }
    return count;
  }

  function isIPAddress(hostname) {
    return /^\d{1,3}(\.\d{1,3}){3}$/.test(hostname) || hostname.indexOf(':') !== -1 || hostname.charAt(0) === '[';
  }

  // Parent domains of hostname, shortest first, excluding single labels. The browser rejects
  // public suffixes (co.uk), so the first candidate that accepts a cookie is the shared domain.
  function candidateCookieDomains(hostname) {
    if (typeof hostname !== 'string') return [];
    var host = hostname.toLowerCase().replace(/\.$/, '');
    if (host === '' || host === 'localhost' || isIPAddress(host)) return [];
    var labels = host.split('.');
    var out = [];
    for (var n = 2; n <= labels.length; n++) {
      out.push(labels.slice(labels.length - n).join('.'));
    }
    return out;
  }

  // First candidate domain the probe accepts, or '' for a host-only cookie.
  function themeCookieDomain(hostname, accepts) {
    var candidates = candidateCookieDomains(hostname);
    for (var i = 0; i < candidates.length; i++) {
      if (accepts(candidates[i])) return candidates[i];
    }
    return '';
  }

  function serializeThemeCookie(theme, domain, secure) {
    var value = normalizeTheme(theme);
    if (value === null) throw new TypeError('aerial-theme: theme must be "dark" or "light"');
    var cookie = COOKIE_NAME + '=' + value + '; path=/; max-age=' + MAX_AGE_SECONDS + '; samesite=lax';
    if (domain) cookie += '; domain=' + domain;
    if (secure) cookie += '; secure';
    return cookie;
  }

  // Read priority: shared cookie, then the app's own stored key, then prefers-color-scheme, then dark.
  function resolveTheme(input) {
    var opts = input || {};
    var fromCookie = parseThemeCookie(opts.cookie);
    if (fromCookie !== null) return fromCookie;
    var fromStore = normalizeTheme(opts.stored);
    if (fromStore !== null) return fromStore;
    if (opts.prefersDark === false) return 'light';
    return 'dark';
  }

  var api = {
    COOKIE_NAME: COOKIE_NAME,
    STORAGE_KEY: STORAGE_KEY,
    parseThemeCookie: parseThemeCookie,
    countThemeCookies: countThemeCookies,
    candidateCookieDomains: candidateCookieDomains,
    themeCookieDomain: themeCookieDomain,
    serializeThemeCookie: serializeThemeCookie,
    resolveTheme: resolveTheme,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = api;
  }

  if (!root || typeof root.document === 'undefined' || typeof root.location === 'undefined') return;
  if (root.AerialTheme) return;

  var doc = root.document;
  var cachedDomain = null;
  var listeners = [];
  var lastNotified = null;

  function isSecure() {
    return root.location.protocol === 'https:';
  }

  function probeAccepts(domain) {
    var tail = '; domain=' + domain + '; path=/; samesite=lax' + (isSecure() ? '; secure' : '');
    try {
      doc.cookie = PROBE_COOKIE + '=1; max-age=10' + tail;
      var ok = new RegExp('(?:^|;\\s*)' + PROBE_COOKIE + '=1(?:;|$)').test(doc.cookie);
      if (ok) doc.cookie = PROBE_COOKIE + '=; max-age=0' + tail;
      return ok;
    } catch (_) {
      return false;
    }
  }

  function sharedDomain() {
    if (cachedDomain === null) cachedDomain = themeCookieDomain(root.location.hostname, probeAccepts);
    return cachedDomain;
  }

  function readCookies() {
    try {
      return doc.cookie;
    } catch (_) {
      return '';
    }
  }

  function readStorage(key) {
    try {
      return root.localStorage.getItem(key);
    } catch (_) {
      return null;
    }
  }

  function writeStorage(key, value) {
    try {
      root.localStorage.setItem(key, value);
    } catch (_) {
      // Storage may be disabled or full; the cookie alone still carries the choice.
    }
  }

  function prefersDark() {
    try {
      return typeof root.matchMedia === 'function' ? root.matchMedia('(prefers-color-scheme: dark)').matches : undefined;
    } catch (_) {
      return undefined;
    }
  }

  function writeThemeCookie(theme) {
    try {
      doc.cookie = serializeThemeCookie(theme, sharedDomain(), isSecure());
    } catch (_) {
      // Cookies may be blocked; nothing else to do.
    }
  }

  function get(appStorageKey) {
    var stored = appStorageKey ? normalizeTheme(readStorage(appStorageKey)) : null;
    if (stored === null) stored = readStorage(STORAGE_KEY);
    return resolveTheme({ cookie: readCookies(), stored: stored, prefersDark: prefersDark() });
  }

  // Chrome orders same-name cookies by path length and creation time, not by domain, so a stale
  // host-only `theme` can shadow the shared one. Drop the host-only copy and rewrite the shared
  // cookie whenever more than one is visible.
  function repairDuplicates(preferred) {
    var before = readCookies();
    if (countThemeCookies(before) <= 1) return;
    var fallback = preferred || parseThemeCookie(before);
    try {
      doc.cookie = COOKIE_NAME + '=; path=/; max-age=0';
    } catch (_) {
      return;
    }
    var resolved = preferred || parseThemeCookie(readCookies()) || fallback;
    if (resolved) writeThemeCookie(resolved);
  }

  function notify(theme) {
    lastNotified = theme;
    var snapshot = listeners.slice();
    for (var i = 0; i < snapshot.length; i++) snapshot[i](theme);
  }

  function set(theme) {
    var value = normalizeTheme(theme);
    if (value === null) return;
    repairDuplicates(null);
    if (parseThemeCookie(readCookies()) === value) return;
    writeThemeCookie(value);
    writeStorage(STORAGE_KEY, value);
    notify(value);
  }

  function recheck() {
    var current = get();
    if (current !== lastNotified) notify(current);
  }

  function onStorage(event) {
    if (event.key === STORAGE_KEY) recheck();
  }

  function onVisibility() {
    if (doc.visibilityState === 'visible') recheck();
  }

  function attach() {
    root.addEventListener('storage', onStorage);
    root.addEventListener('focus', recheck);
    doc.addEventListener('visibilitychange', onVisibility);
  }

  function detach() {
    root.removeEventListener('storage', onStorage);
    root.removeEventListener('focus', recheck);
    doc.removeEventListener('visibilitychange', onVisibility);
  }

  function subscribe(fn) {
    if (typeof fn !== 'function') return function () {};
    if (listeners.length === 0) {
      lastNotified = get();
      attach();
    }
    listeners.push(fn);
    return function unsubscribe() {
      var idx = listeners.indexOf(fn);
      if (idx === -1) return;
      listeners.splice(idx, 1);
      if (listeners.length === 0) detach();
    };
  }

  root.AerialTheme = {
    get: get,
    set: set,
    subscribe: subscribe,
    parseThemeCookie: parseThemeCookie,
    countThemeCookies: countThemeCookies,
    candidateCookieDomains: candidateCookieDomains,
    themeCookieDomain: themeCookieDomain,
    serializeThemeCookie: serializeThemeCookie,
    resolveTheme: resolveTheme,
  };

  repairDuplicates(null);
})(typeof window !== 'undefined' ? window : undefined);
