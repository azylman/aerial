/**
 * Aerial Docsify Mermaid Plugin (Permet Cyberpunk Theme)
 * Custom async renderer with error boundaries and responsive SVG formatting.
 */
(function () {
  let diagramCount = 0;

  // Initialize Mermaid with Aerial Permet HUD Theme
  if (typeof window !== 'undefined' && window.mermaid) {
    window.mermaid.initialize({
      startOnLoad: false,
      securityLevel: 'loose',
      theme: 'base',
      themeVariables: {
        darkMode: true,
        background: '#0e121c',
        mainBkg: '#0e121c',
        primaryColor: '#141c2b',
        primaryTextColor: '#00f2fe',
        primaryBorderColor: '#00f2fe',
        lineColor: '#00f2fe',
        secondaryColor: '#1a102f',
        secondaryTextColor: '#ff007f',
        secondaryBorderColor: '#ff007f',
        tertiaryColor: '#080a0f',
        tertiaryTextColor: '#f0f4f8',
        tertiaryBorderColor: '#4d5b70',
        noteBkgColor: '#141c2b',
        noteTextColor: '#ffb700',
        noteBorderColor: '#ffb700',
        fontFamily: "'JetBrains Mono', -apple-system, BlinkMacSystemFont, monospace",
        fontSize: '13px'
      }
    });
  }

  // Custom Markdown Code Renderer for Mermaid
  window.$docsify = window.$docsify || {};
  window.$docsify.markdown = window.$docsify.markdown || {};
  const originalRenderer = window.$docsify.markdown.renderer || {};

  window.$docsify.markdown.renderer = Object.assign({}, originalRenderer, {
    code: function (code, lang) {
      if (lang === "mermaid") {
        const id = 'mermaid-diagram-' + (++diagramCount);
        return '<div class="mermaid-diagram" id="' + id + '" data-mermaid-code="' + encodeURIComponent(code) + '">' +
               '  <div class="mermaid-loading">RENDERING MERMAID DIAGRAM...</div>' +
               '</div>';
      }
      return (originalRenderer.code || this.origin.code).apply(this, arguments);
    }
  });

  // Docsify Lifecycle DoneEach Hook
  window.$docsify.plugins = [].concat(function (hook, vm) {
    hook.doneEach(function () {
      if (!window.mermaid) return;

      const diagrams = document.querySelectorAll('.mermaid-diagram');
      diagrams.forEach(function (el, index) {
        const rawCode = decodeURIComponent(el.getAttribute('data-mermaid-code') || '');
        if (!rawCode) return;

        const svgId = 'mermaid-svg-' + Date.now() + '-' + index;

        try {
          window.mermaid.render(svgId, rawCode)
            .then(function (result) {
              el.innerHTML = result.svg;
              const svg = el.querySelector('svg');
              if (svg) {
                svg.style.maxWidth = '100%';
                svg.style.height = 'auto';
                svg.style.display = 'block';
                svg.style.margin = '0 auto';
              }
            })
            .catch(function (error) {
              renderMermaidError(el, rawCode, error);
            });
        } catch (err) {
          renderMermaidError(el, rawCode, err);
        }
      });
    });
  }, window.$docsify.plugins || []);

  function renderMermaidError(container, rawCode, error) {
    console.warn('[Aerial Docs] Mermaid Syntax Error:', error);
    const errorMsg = (error && error.message) ? error.message : 'Invalid Mermaid syntax';
    container.innerHTML = 
      '<div class="mermaid-error-card">' +
      '  <div class="error-header">' +
      '    <span class="error-icon">⚠️</span>' +
      '    <span class="error-title">DIAGRAM RENDER FAILURE</span>' +
      '  </div>' +
      '  <div class="error-msg">' + escapeHtml(errorMsg) + '</div>' +
      '  <details class="error-raw">' +
      '    <summary>View Mermaid Source</summary>' +
      '    <pre><code>' + escapeHtml(rawCode) + '</code></pre>' +
      '  </details>' +
      '</div>';
  }

  function escapeHtml(str) {
    return String(str)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }
})();
