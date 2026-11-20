/*
 * Homepage theme sync, loaded through Homepage's supported custom.js hook.
 *
 * prepare-config.js prepends web/theme/aerial-theme.js, so window.AerialTheme is defined above
 * this file. custom.js runs after React hydration, so it never writes Homepage's 'theme-mode' key
 * or the html class while React is mounted. To change mode it clicks Homepage's own theme toggle,
 * letting Homepage's ThemeProvider update its state, class and storage itself. Only when that
 * toggle never shows up does it write 'theme-mode' and reload once (guarded per tab session).
 */
(function () {
  'use strict';

  var theme = window.AerialTheme;
  if (!theme || typeof theme.subscribe !== 'function') return;

  var HOMEPAGE_KEY = 'theme-mode';
  var RELOAD_FLAG = 'aerial-homepage-theme-reload';
  // The toggle is lazy-loaded after hydration; wait this long for it before reloading instead.
  var TOGGLE_WAIT_MS = 5000;
  var html = document.documentElement;

  // Homepage toggles the 'dark' class itself and its ThemeProvider adds 'light'; theme-* classes
  // are colour palettes and do not count.
  function currentMode() {
    if (html.classList.contains('dark')) return 'dark';
    if (html.classList.contains('light')) return 'light';
    return null;
  }

  function findToggle() {
    return document.querySelector('#theme .cursor-pointer');
  }

  function clickToggle(toggle) {
    // The toggle is an SVG icon, which has no .click(); React handles the bubbling event.
    toggle.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true, view: window }));
  }

  function readSession(key) {
    try {
      return window.sessionStorage.getItem(key);
    } catch (_) {
      return null;
    }
  }

  function writeSession(key, value) {
    try {
      if (value === null) window.sessionStorage.removeItem(key);
      else window.sessionStorage.setItem(key, value);
      return true;
    } catch (_) {
      return false;
    }
  }

  function reloadInto(target) {
    if (readSession(RELOAD_FLAG) !== null) return;
    if (!writeSession(RELOAD_FLAG, target)) return;
    try {
      window.localStorage.setItem(HOMEPAGE_KEY, target);
    } catch (_) {
      return;
    }
    window.location.reload();
  }

  // Returns true when Homepage already shows target or the toggle was clicked.
  function tryApply(target) {
    var mode = currentMode();
    if (mode === null) return false;
    if (mode === target) return true;
    var toggle = findToggle();
    if (!toggle) return false;
    clickToggle(toggle);
    return true;
  }

  var pendingTarget = null;
  var waitObserver = null;
  var waitTimer = null;

  function stopWaiting() {
    if (waitObserver) waitObserver.disconnect();
    if (waitTimer) clearTimeout(waitTimer);
    waitObserver = null;
    waitTimer = null;
    pendingTarget = null;
  }

  function apply(target) {
    if (tryApply(target)) {
      stopWaiting();
      return;
    }
    pendingTarget = target;
    if (waitObserver) return;
    waitObserver = new MutationObserver(function () {
      if (pendingTarget !== null && tryApply(pendingTarget)) stopWaiting();
    });
    waitObserver.observe(document.body || html, { childList: true, subtree: true });
    waitTimer = setTimeout(function () {
      var target = pendingTarget;
      stopWaiting();
      if (target !== null && currentMode() !== target) reloadInto(target);
    }, TOGGLE_WAIT_MS);
  }

  // Report Homepage's own toggle clicks to the shared state. set() is a no-op when the shared
  // value already matches, so the clicks made by apply() do not echo back.
  var lastMode = currentMode();
  new MutationObserver(function () {
    var mode = currentMode();
    if (mode === null || mode === lastMode) return;
    lastMode = mode;
    if (pendingTarget === null) theme.set(mode);
  }).observe(html, { attributes: true, attributeFilter: ['class'] });

  var initial = theme.get(HOMEPAGE_KEY);
  if (currentMode() === initial) writeSession(RELOAD_FLAG, null);
  apply(initial);

  theme.subscribe(apply);
})();
