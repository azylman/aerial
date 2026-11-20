/**
 * Docsify Theme Toggle Plugin
 *
 * Renders the light/dark toggle and keeps the page in sync with the shared Aerial theme
 * (window.AerialTheme from assets/aerial-theme.js). The initial theme is applied before paint by
 * the head script in index.html; this plugin only renders the button, calls AerialTheme.set on
 * click, and re-applies the theme when another tab or app changes it.
 */
(function () {
  'use strict';

  var SUN =
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" aria-hidden="true">' +
    '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></svg>';
  var MOON =
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
    '<path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/></svg>';

  var themeApi = window.AerialTheme;
  var applyTheme = window.aerialDocsApplyTheme;
  if (!themeApi || typeof applyTheme !== 'function') return;

  // The button shows the theme a click switches to.
  function render(button, theme) {
    var next = theme === 'light' ? 'dark' : 'light';
    button.innerHTML = next === 'light' ? SUN : MOON;
    button.setAttribute('aria-label', 'Switch to ' + next + ' theme');
    button.title = 'Switch to ' + next + ' theme';
  }

  window.$docsify = window.$docsify || {};
  window.$docsify.plugins = [].concat(function (hook) {
    hook.mounted(function () {
      var button = document.createElement('button');
      button.type = 'button';
      button.className = 'aerial-theme-toggle';
      render(button, themeApi.get());

      button.addEventListener('click', function () {
        // set() notifies subscribers, which re-applies and re-renders below.
        themeApi.set(themeApi.get() === 'light' ? 'dark' : 'light');
      });

      themeApi.subscribe(function (theme) {
        applyTheme(theme);
        render(button, theme);
      });

      document.body.appendChild(button);
    });
  }, window.$docsify.plugins || []);
})();
