package main

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/azylman/aerial/pkg/httpkit"
)

var (
	cardPattern        = regexp.MustCompile(`\b(?:\d[ -]*?){13,16}\b`)
	asinPattern        = regexp.MustCompile(`^[A-Za-z0-9]{10}$`)
	evalBlockedPattern = regexp.MustCompile(`(?i)\b(?:fetch\s*\(|XMLHttpRequest\b)|(?:\.\s*|\?\.\s*)click\s*\(`)
)

// ValidateEvaluateExpression blocks in-page scraping bursts (fetch/XMLHttpRequest) and untrusted synthetic DOM clicks (.click()).
func ValidateEvaluateExpression(expr string) error {
	if evalBlockedPattern.MatchString(expr) {
		return fmt.Errorf("blocked unsafe browser_evaluate pattern: use browser_navigate/amazon_search for page loads and browser_click for trusted clicks")
	}
	return nil
}

// JitterCoord applies a deterministic, bounded interior offset when element dimensions exceed 6px.
func JitterCoord(x, y, width, height int) (int, int) {
	if width <= 6 || height <= 6 {
		return x, y
	}
	dx := 1 + ((x*31 + y*17 + width) % (width / 6))
	dy := 1 + ((y*29 + x*13 + height) % (height / 6))
	return x + dx, y - dy
}

// SanitizePII redacts potential 13-16 digit payment card numbers.
func SanitizePII(text string) string {
	if text == "" {
		return ""
	}
	return cardPattern.ReplaceAllStringFunc(text, func(m string) string {
		digits := 0
		for i := range len(m) {
			if m[i] >= '0' && m[i] <= '9' {
				digits++
			}
		}
		if digits >= 13 && digits <= 16 {
			return "[REDACTED_CARD]"
		}
		return m
	})
}

// ValidateASIN checks if the input is a valid 10-character alphanumeric Amazon ASIN.
func ValidateASIN(asin string) bool {
	return asinPattern.MatchString(asin)
}

// TruncateContent caps text to maxLength (defaulting to 15000 if <= 0), appending "\n...[truncated]" when truncated.
func TruncateContent(text string, maxLength int) (string, bool) {
	if maxLength <= 0 {
		maxLength = 15000
	}
	if len(text) > maxLength {
		return text[:maxLength] + "\n...[truncated]", true
	}
	return text, false
}

// RewriteWSUrl takes a WebSocket debugger URL returned by CDP and rewrites the scheme, host, and port.
func RewriteWSUrl(rawWS, host, port string) string {
	targetHost := host
	if port != "" {
		targetHost = net.JoinHostPort(host, port)
	}
	u, err := url.Parse(rawWS)
	if err != nil || u.Path == "" {
		if rawWS != "" && !strings.Contains(rawWS, "://") && strings.HasPrefix(rawWS, "/") {
			return fmt.Sprintf("ws://%s%s", targetHost, rawWS)
		}
		return "ws://" + targetHost
	}
	path := u.Path
	if u.RawQuery != "" {
		path = path + "?" + u.RawQuery
	}
	return fmt.Sprintf("ws://%s%s", targetHost, path)
}

// ResolveCDPHost resolves a hostname, returning IPv4 addresses first.
// Literal IPv4s, "localhost", and "::1" pass through directly.
func ResolveCDPHost(ctx context.Context, host string) string {
	h := strings.TrimSpace(host)
	if h == "" {
		return "127.0.0.1"
	}
	if h == "localhost" || h == "::1" {
		return h
	}
	if ip := net.ParseIP(h); ip != nil {
		return h
	}

	var resolver net.Resolver
	addrs, err := resolver.LookupHost(ctx, h)
	if err != nil || len(addrs) == 0 {
		return h
	}
	for _, addr := range addrs {
		if ip := net.ParseIP(addr); ip != nil && ip.To4() != nil {
			return addr
		}
	}
	return addrs[0]
}

// JavaScript expression builders.

// BuildGetContentScript builds a JS expression to clone target element or body, remove scripts/styles, and return { title, url, text }.
func BuildGetContentScript(selector string) string {
	selJSON := strconv.Quote(selector)
	return fmt.Sprintf(`(() => {
  const sel = %s;
  const el = (sel ? document.querySelector(sel) : document.body) || document.body;
  if (!el) {
    return { title: document.title || "", url: window.location.href || "", text: "" };
  }
  const clone = el.cloneNode(true);
  const toRemove = clone.querySelectorAll("script, style, noscript, svg");
  toRemove.forEach(n => n.remove());
  return {
    title: document.title || "",
    url: window.location.href || "",
    text: (clone.innerText || clone.textContent || "").trim()
  };
})()`, selJSON)
}

// ClickResult is the evaluated structure returned by BuildClickScript.
type ClickResult struct {
	Found   bool   `json:"found"`
	Tag     string `json:"tag"`
	Text    string `json:"text"`
	HasRect bool   `json:"hasRect"`
	X       int    `json:"x"`
	Y       int    `json:"y"`
	Width   int    `json:"width"`
	Height  int    `json:"height"`
}

// BuildClickScript builds a JS expression to scroll the selector into view and return geometry.
func BuildClickScript(selector string) string {
	selJSON := strconv.Quote(selector)
	return fmt.Sprintf(`(() => {
  const el = document.querySelector(%s);
  if (!el) return { found: false, tag: "", text: "", hasRect: false, x: 0, y: 0, width: 0, height: 0 };
  el.scrollIntoView({ behavior: "instant", block: "center" });
  const rect = el.getBoundingClientRect();
  const hasRect = !!(rect && rect.width > 0 && rect.height > 0);
  const x = hasRect ? Math.round(rect.left + rect.width / 2) : 0;
  const y = hasRect ? Math.round(rect.top + rect.height / 2) : 0;
  const width = hasRect ? Math.round(rect.width) : 0;
  const height = hasRect ? Math.round(rect.height) : 0;
  if (!hasRect) {
    try { el.click(); } catch (_) {}
  }
  return {
    found: true,
    tag: el.tagName ? el.tagName.toLowerCase() : "",
    text: (el.innerText || el.value || el.getAttribute("aria-label") || "").trim(),
    hasRect: hasRect,
    x: x,
    y: y,
    width: width,
    height: height
  };
})()`, selJSON)
}

// FocusResult is the evaluated structure returned by BuildFocusAndClearScript.
type FocusResult struct {
	Found   bool `json:"found"`
	HasRect bool `json:"hasRect"`
	X       int  `json:"x"`
	Y       int  `json:"y"`
}

// BuildFocusAndClearScript builds a JS expression to focus and optionally clear an input.
func BuildFocusAndClearScript(selector string, shouldClear bool) string {
	selJSON := strconv.Quote(selector)
	return fmt.Sprintf(`(() => {
  const el = document.querySelector(%s);
  if (!el) return { found: false, hasRect: false, x: 0, y: 0 };
  el.scrollIntoView({ behavior: "instant", block: "center" });
  el.focus();
  if (%t) {
    el.value = "";
  }
  const rect = el.getBoundingClientRect();
  const hasRect = !!(rect && rect.width > 0 && rect.height > 0);
  const x = hasRect ? Math.round(rect.left + rect.width / 2) : 0;
  const y = hasRect ? Math.round(rect.top + rect.height / 2) : 0;
  return { found: true, hasRect: hasRect, x: x, y: y };
})()`, selJSON, shouldClear)
}

// BuildDispatchInputEventsScript builds a JS expression to trigger bubbling input and change events.
func BuildDispatchInputEventsScript(selector string) string {
	selJSON := strconv.Quote(selector)
	return fmt.Sprintf(`(() => {
  const el = document.querySelector(%s);
  if (el) {
    el.dispatchEvent(new Event("input", { bubbles: true }));
    el.dispatchEvent(new Event("change", { bubbles: true }));
  }
})()`, selJSON)
}

// BuildAmazonSearchScript builds a JS expression extracting up to maxResults search results.
func BuildAmazonSearchScript(maxResults int) string {
	if maxResults <= 0 {
		maxResults = 5
	}
	return fmt.Sprintf(`(() => {
  const items = [];
  const nodes = document.querySelectorAll('[data-component-type="s-search-result"]');
  const limit = Math.min(nodes.length, %d);
  for (let i = 0; i < limit; i++) {
    const el = nodes[i];
    const asin = el.getAttribute('data-asin') || '';
    const spans = Array.from(el.querySelectorAll('h2 span')).map(s => (s.innerText || s.textContent || '').trim()).filter((v, idx, a) => v && a.indexOf(v) === idx);
    const titleEl = el.querySelector('h2 a span') || el.querySelector('h2');
    const title = spans.length ? spans.join(' ') : (titleEl ? titleEl.innerText.trim() : '');
    const priceEl = el.querySelector('.a-price .a-offscreen') || el.querySelector('.a-price');
    const price = priceEl ? priceEl.innerText.trim() : '';
    const ratingEl = el.querySelector('.a-icon-alt');
    const rating = ratingEl ? ratingEl.innerText.trim() : '';
    const reviewsEl = el.querySelector('.a-size-small .a-link-normal span') || el.querySelector('[aria-label*="ratings"]');
    const reviews = reviewsEl ? reviewsEl.innerText.trim() : '';
    const prime = !!(el.querySelector('.a-icon-prime') || el.querySelector('[aria-label="Amazon Prime"]'));
    const delivEl = el.querySelector('[data-cy="delivery-recipe"]');
    const delivery = delivEl ? (delivEl.innerText || delivEl.textContent || '').replace(/\s+/g, ' ').trim() : '';
    const linkEl = el.querySelector('h2 a') || (el.querySelector('h2') && el.querySelector('h2').closest('a')) || el.querySelector('a[href*="/dp/"]');
    let itemUrl = linkEl ? linkEl.getAttribute('href') : '';
    if (itemUrl && itemUrl.startsWith('/')) {
      itemUrl = 'https://www.amazon.com' + itemUrl;
    }
    items.push({ asin, title, price, rating, reviews, prime, delivery, url: itemUrl });
  }
  return items;
})()`, maxResults)
}

// BuildAmazonCartScript builds a JS expression extracting Amazon cart subtotal, count, and items.
func BuildAmazonCartScript() string {
	return `(() => {
  const subtotalEl = document.querySelector('#sc-subtotal-amount-activecart') || document.querySelector('#sc-subtotal-amount-buybox') || document.querySelector('.sc-price');
  const subtotal = subtotalEl ? subtotalEl.innerText.trim() : '';
  const countEl = document.querySelector('#sc-subtotal-label-activecart') || document.querySelector('#nav-cart-count');
  const countText = countEl ? countEl.innerText.trim() : '';

  const items = [];
  const itemNodes = document.querySelectorAll('[data-asin][data-bundle-asins], .sc-list-item, [data-asin]');
  itemNodes.forEach(el => {
    const asin = el.getAttribute('data-asin') || '';
    if (!asin || items.some(it => it.asin === asin)) return;
    const titleEl = el.querySelector('.a-truncate-full') || el.querySelector('.sc-product-title') || el.querySelector('.a-truncate-cut');
    const title = titleEl ? (titleEl.textContent || titleEl.innerText || '').trim() : '';
    const priceEl = el.querySelector('.sc-product-price') || el.querySelector('.a-price .a-offscreen');
    const price = priceEl ? priceEl.innerText.trim() : '';
    const qtyEl = el.querySelector('input[name="quantity"]') || el.querySelector('select[name="quantity"]') || el.querySelector('.a-dropdown-prompt') || el.querySelector('[data-quantity]');
    const quantity = qtyEl ? (qtyEl.value || qtyEl.innerText || qtyEl.getAttribute('data-quantity') || '1').trim() : '1';
    if (title || price) {
      items.push({ asin, title, price, quantity });
    }
  });

  return { subtotal, count: countText, items };
})()`
}

// BuildAmazonAddToCartScript builds a JS expression locating the add to cart button, expanding collapsed New buybox accordions if needed.
func BuildAmazonAddToCartScript() string {
	return `(() => {
  const sel = '#add-to-cart-button, input[name="submit.add-to-cart"], #buy-now-button';
  let btn = document.querySelector(sel);
  if (!btn) {
    const acc = document.querySelector('#newAccordionRow_0 a, #newAccordionRow_1 a, [data-csa-c-slot-id="newAccordionRow"] a');
    if (acc) { try { acc.click(); } catch (_) {} btn = document.querySelector(sel); }
  }
  const titleEl = document.querySelector('#productTitle');
  const productTitle = titleEl ? titleEl.innerText.trim() : '';
  if (!btn) return { found: false, hasRect: false, x: 0, y: 0, width: 0, height: 0, productTitle };
  btn.scrollIntoView({ behavior: 'instant', block: 'center' });
  const rect = btn.getBoundingClientRect();
  const hasRect = !!(rect && rect.width > 0 && rect.height > 0);
  const x = hasRect ? Math.round(rect.left + rect.width / 2) : 0;
  const y = hasRect ? Math.round(rect.top + rect.height / 2) : 0;
  const width = hasRect ? Math.round(rect.width) : 0;
  const height = hasRect ? Math.round(rect.height) : 0;
  if (!hasRect) {
    try { btn.click(); } catch (_) {}
  }
  return { found: true, hasRect, x, y, width, height, productTitle };
})()`
}

// BuildAmazonCartCountScript dismisses any post-add warranty drawer and extracts the active nav cart count.
func BuildAmazonCartCountScript() string {
	return `(() => {
  const noThanks = document.querySelector('#attachSiNoCoverage input, #attachSiNoCoverage-announce, button[id*="SiNoCoverage"]');
  if (noThanks) { try { noThanks.click(); } catch (_) {} }
  const el = document.querySelector('#nav-cart-count');
  return el ? el.innerText.trim() : '';
})()`
}

// Config represents service runtime configuration.
type Config struct {
	Port        string        `yaml:"port"`
	CDPHost     string        `yaml:"cdp_host"`
	CDPPort     string        `yaml:"cdp_port"`
	SettleDelay time.Duration `yaml:"settle_delay"`
}

// ConfigYAML is the file format for YAML configs.
type ConfigYAML struct {
	Port        string `yaml:"port"`
	CDPHost     string `yaml:"cdp_host"`
	CDPPort     string `yaml:"cdp_port"`
	SettleDelay string `yaml:"settle_delay"`
}

// parseURLHostPort extracts host and port from a URL string if valid.
func parseURLHostPort(raw string) (string, string) {
	if raw == "" {
		return "", ""
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", ""
	}
	host := u.Hostname()
	port := u.Port()
	return host, port
}

// LoadConfig loads configuration from path or defaults, applying env vars.
// Treats missing or empty /local/config.yaml as valid defaults.
func LoadConfig(path string) (*Config, error) {
	cfg := &Config{
		Port:        "4018",
		CDPHost:     "",
		CDPPort:     "9222",
		SettleDelay: 500 * time.Millisecond,
	}

	configPath := strings.TrimSpace(path)
	if configPath == "" {
		configPath = httpkit.ResolveConfigPathWithLocal("CONFIG_PATH", "/local/config.yaml", "/share/aerial-config/config.yaml", "config.yaml")
	}

	if err := applyYAMLFile(cfg, configPath); err != nil {
		return nil, err
	}

	applyEnvOverrides(cfg)
	return cfg, nil
}

func applyYAMLFile(cfg *Config, configPath string) error {
	if configPath == "" {
		return nil
	}
	fi, err := os.Stat(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to stat config file %s: %w", configPath, err)
	}
	if fi.Size() == 0 {
		return nil
	}
	data, readErr := os.ReadFile(configPath)
	if readErr != nil {
		return fmt.Errorf("failed to read config file %s: %w", configPath, readErr)
	}
	var ycfg ConfigYAML
	if err := yaml.Unmarshal(data, &ycfg); err != nil {
		return fmt.Errorf("failed to parse config YAML from %s: %w", configPath, err)
	}
	if v := strings.TrimSpace(ycfg.Port); v != "" {
		cfg.Port = v
	}
	if v := strings.TrimSpace(ycfg.CDPHost); v != "" {
		cfg.CDPHost = v
	}
	if v := strings.TrimSpace(ycfg.CDPPort); v != "" {
		cfg.CDPPort = v
	}
	if v := strings.TrimSpace(ycfg.SettleDelay); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.SettleDelay = d
		}
	}
	return nil
}

func applyEnvOverrides(cfg *Config) {
	for _, envURL := range []string{"KERNEL_CDP_URL", "KERNEL_API_URL"} {
		if val := strings.TrimSpace(os.Getenv(envURL)); val != "" {
			h, p := parseURLHostPort(val)
			if h != "" {
				cfg.CDPHost = h
			}
			if p != "" {
				cfg.CDPPort = p
			}
			break
		}
	}

	if val := strings.TrimSpace(os.Getenv("PORT")); val != "" {
		cfg.Port = val
	}
	if val := strings.TrimSpace(os.Getenv("CDP_HOST")); val != "" {
		cfg.CDPHost = val
	}
	if val := strings.TrimSpace(os.Getenv("CDP_PORT")); val != "" {
		cfg.CDPPort = val
	}
	if val := strings.TrimSpace(os.Getenv("SETTLE_DELAY")); val != "" {
		if d, err := time.ParseDuration(val); err == nil {
			cfg.SettleDelay = d
		}
	}
}
