package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"gopkg.in/yaml.v3"
)

// Stealth Chrome version constants.
const (
	DefaultChromeMajor = "152"
	DefaultChromeFull  = "152.0.7977.42"
	StealthUserAgent   = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"
)

// BrandVersion describes Chrome User-Agent metadata brand and version.
type BrandVersion struct {
	Brand   string `json:"brand"`
	Version string `json:"version"`
}

// UAMetadata describes client hint emulation parameters.
type UAMetadata struct {
	Brands          []BrandVersion `json:"brands"`
	FullVersionList []BrandVersion `json:"fullVersionList"`
	Platform        string         `json:"platform"`
	PlatformVersion string         `json:"platformVersion"`
	Architecture    string         `json:"architecture"`
	Model           string         `json:"model"`
	Mobile          bool           `json:"mobile"`
	Bitness         string         `json:"bitness"`
	Wow64           bool           `json:"wow64"`
}

// StealthUAMetadata defines the standard Linux x86_64 stealth metadata.
var StealthUAMetadata = UAMetadata{
	Brands: []BrandVersion{
		{Brand: "Not?A_Brand", Version: "24"},
		{Brand: "Chromium", Version: DefaultChromeMajor},
		{Brand: "Google Chrome", Version: DefaultChromeMajor},
	},
	FullVersionList: []BrandVersion{
		{Brand: "Not?A_Brand", Version: "24.0.0.0"},
		{Brand: "Chromium", Version: DefaultChromeFull},
		{Brand: "Google Chrome", Version: DefaultChromeFull},
	},
	Platform:        "Linux",
	PlatformVersion: "6.8.0",
	Architecture:    "x86",
	Model:           "",
	Mobile:          false,
	Bitness:         "64",
	Wow64:           false,
}

// StealthInitScript is evaluated on every new document and upon session attach.
// Notice single backslashes in regexes inside raw string literal.
const StealthInitScript = `(() => {
  try {
    if (navigator.webdriver !== false) {
      Object.defineProperty(Navigator.prototype, 'webdriver', {
        get: () => false, configurable: true, enumerable: true,
      });
    }
    if (!navigator.languages || navigator.languages.length < 2) {
      Object.defineProperty(Navigator.prototype, 'languages', {
        get: () => ['en-US', 'en'], configurable: true, enumerable: true,
      });
    }
    if (!window.chrome) {
      Object.defineProperty(window, 'chrome', {
        value: {}, writable: true, configurable: true, enumerable: true,
      });
    }
    if (!window.chrome.runtime || !window.chrome.runtime.OnInstalledReason) {
      window.chrome.runtime = {
        OnInstalledReason: { CHROME_UPDATE: 'chrome_update', INSTALL: 'install', SHARED_MODULE_UPDATE: 'shared_module_update', UPDATE: 'update' },
        OnRestartRequiredReason: { APP_UPDATE: 'app_update', OS_UPDATE: 'os_update', PERIODIC: 'periodic' },
        PlatformArch: { ARM: 'arm', ARM64: 'arm64', MIPS: 'mips', MIPS64: 'mips64', X86_32: 'x86-32', X86_64: 'x86-64' },
        PlatformNaclArch: { ARM: 'arm', MIPS: 'mips', MIPS64: 'mips64', X86_32: 'x86-32', X86_64: 'x86-64' },
        PlatformOs: { ANDROID: 'android', CROS: 'cros', LINUX: 'linux', MAC: 'mac', OPENBSD: 'openbsd', WIN: 'win' },
        RequestUpdateCheckStatus: { NO_UPDATE: 'no_update', THROTTLED: 'throttled', UPDATE_AVAILABLE: 'update_available' },
        id: undefined,
        connect: function connect() {},
        sendMessage: function sendMessage() {},
      };
    }
    const patchWebGL = (ctxProto) => {
      if (!ctxProto || !ctxProto.getParameter) return;
      const origGetParameter = ctxProto.getParameter;
      ctxProto.getParameter = new Proxy(origGetParameter, {
        apply(target, thisArg, args) {
          const parameter = args[0];
          const val = Reflect.apply(target, thisArg, args);
          if (parameter === 37445 && typeof val === 'string' && /Google Inc\. \(Google\)|SwiftShader|llvmpipe/i.test(val)) {
            return 'Google Inc. (Intel)';
          }
          if (parameter === 37446 && typeof val === 'string' && /SwiftShader|Subzero|llvmpipe/i.test(val)) {
            return 'ANGLE (Intel, Mesa Intel(R) UHD Graphics 630 (CFL GT2), OpenGL 4.6)';
          }
          return val;
        },
      });
    };
    if (typeof WebGLRenderingContext !== 'undefined') patchWebGL(WebGLRenderingContext.prototype);
    if (typeof WebGL2RenderingContext !== 'undefined') patchWebGL(WebGL2RenderingContext.prototype);
  } catch (_) {}
})();`

var (
	cardPattern = regexp.MustCompile(`\b(?:\d[ -]*?){13,16}\b`)
	asinPattern = regexp.MustCompile(`^[A-Za-z0-9]{10}$`)
)

// SanitizePII redacts potential 13-16 digit payment card numbers.
func SanitizePII(text string) string {
	if text == "" {
		return ""
	}
	return cardPattern.ReplaceAllStringFunc(text, func(m string) string {
		digits := 0
		for i := 0; i < len(m); i++ {
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
		return fmt.Sprintf("ws://%s", targetHost)
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
	selJSON, _ := json.Marshal(selector) //nolint:errcheck,errchkjson
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
})()`, string(selJSON))
}

// ClickResult is the evaluated structure returned by BuildClickScript.
type ClickResult struct {
	Found   bool   `json:"found"`
	Tag     string `json:"tag"`
	Text    string `json:"text"`
	HasRect bool   `json:"hasRect"`
	X       int    `json:"x"`
	Y       int    `json:"y"`
}

// BuildClickScript builds a JS expression to scroll the selector into view and return geometry.
func BuildClickScript(selector string) string {
	selJSON, _ := json.Marshal(selector) //nolint:errcheck,errchkjson
	return fmt.Sprintf(`(() => {
  const el = document.querySelector(%s);
  if (!el) return { found: false, tag: "", text: "", hasRect: false, x: 0, y: 0 };
  el.scrollIntoView({ behavior: "instant", block: "center" });
  const rect = el.getBoundingClientRect();
  const hasRect = !!(rect && rect.width > 0 && rect.height > 0);
  const x = hasRect ? Math.round(rect.left + rect.width / 2) : 0;
  const y = hasRect ? Math.round(rect.top + rect.height / 2) : 0;
  if (!hasRect) {
    try { el.click(); } catch (_) {}
  }
  return {
    found: true,
    tag: el.tagName ? el.tagName.toLowerCase() : "",
    text: (el.innerText || el.value || el.getAttribute("aria-label") || "").trim(),
    hasRect: hasRect,
    x: x,
    y: y
  };
})()`, string(selJSON))
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
	selJSON, _ := json.Marshal(selector) //nolint:errcheck,errchkjson
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
})()`, string(selJSON), shouldClear)
}

// BuildDispatchInputEventsScript builds a JS expression to trigger bubbling input and change events.
func BuildDispatchInputEventsScript(selector string) string {
	selJSON, _ := json.Marshal(selector) //nolint:errcheck,errchkjson
	return fmt.Sprintf(`(() => {
  const el = document.querySelector(%s);
  if (el) {
    el.dispatchEvent(new Event("input", { bubbles: true }));
    el.dispatchEvent(new Event("change", { bubbles: true }));
  }
})()`, string(selJSON))
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
    const titleEl = el.querySelector('h2 a span') || el.querySelector('h2');
    const title = titleEl ? titleEl.innerText.trim() : '';
    const priceEl = el.querySelector('.a-price .a-offscreen') || el.querySelector('.a-price');
    const price = priceEl ? priceEl.innerText.trim() : '';
    const ratingEl = el.querySelector('.a-icon-alt');
    const rating = ratingEl ? ratingEl.innerText.trim() : '';
    const reviewsEl = el.querySelector('.a-size-small .a-link-normal span') || el.querySelector('[aria-label*="ratings"]');
    const reviews = reviewsEl ? reviewsEl.innerText.trim() : '';
    const prime = !!(el.querySelector('.a-icon-prime') || el.querySelector('[aria-label="Amazon Prime"]'));
    const linkEl = el.querySelector('h2 a');
    let itemUrl = linkEl ? linkEl.getAttribute('href') : '';
    if (itemUrl && itemUrl.startsWith('/')) {
      itemUrl = 'https://www.amazon.com' + itemUrl;
    }
    items.push({ asin, title, price, rating, reviews, prime, url: itemUrl });
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
    const titleEl = el.querySelector('.sc-product-title') || el.querySelector('.a-truncate-cut');
    const title = titleEl ? titleEl.innerText.trim() : '';
    const priceEl = el.querySelector('.sc-product-price') || el.querySelector('.a-price .a-offscreen');
    const price = priceEl ? priceEl.innerText.trim() : '';
    const qtyEl = el.querySelector('input[name="quantity"]') || el.querySelector('.a-dropdown-prompt') || el.querySelector('[data-quantity]');
    const quantity = qtyEl ? (qtyEl.value || qtyEl.innerText || qtyEl.getAttribute('data-quantity') || '1').trim() : '1';
    if (title || price) {
      items.push({ asin, title, price, quantity });
    }
  });

  return { subtotal, count: countText, items };
})()`
}

// BuildAmazonAddToCartScript builds a JS expression locating the add to cart button.
func BuildAmazonAddToCartScript() string {
	return `(() => {
  const btn = document.querySelector('#add-to-cart-button') || document.querySelector('input[name="submit.add-to-cart"]') || document.querySelector('#buy-now-button');
  const titleEl = document.querySelector('#productTitle');
  const productTitle = titleEl ? titleEl.innerText.trim() : '';
  if (!btn) return { found: false, hasRect: false, x: 0, y: 0, productTitle };
  btn.scrollIntoView({ behavior: 'instant', block: 'center' });
  const rect = btn.getBoundingClientRect();
  const hasRect = !!(rect && rect.width > 0 && rect.height > 0);
  const x = hasRect ? Math.round(rect.left + rect.width / 2) : 0;
  const y = hasRect ? Math.round(rect.top + rect.height / 2) : 0;
  if (!hasRect) {
    try { btn.click(); } catch (_) {}
  }
  return { found: true, hasRect, x, y, productTitle };
})()`
}

// BuildAmazonCartCountScript extracts the active nav cart count.
func BuildAmazonCartCountScript() string {
	return `(() => {
  const el = document.querySelector('#nav-cart-count');
  return el ? el.innerText.trim() : '';
})()`
}

// Tool represents an MCP Tool definition.
type Tool struct {
	Name        string      `json:"name"`
	Description string      `json:"description"`
	InputSchema InputSchema `json:"inputSchema"`
}

// InputSchema defines the JSON schema for tool inputs.
type InputSchema struct {
	Type       string                 `json:"type"`
	Properties map[string]PropertyDef `json:"properties,omitempty"`
	Required   []string               `json:"required,omitempty"`
}

// PropertyDef describes a parameter field in the schema.
type PropertyDef struct {
	Type        string   `json:"type"`
	Description string   `json:"description,omitempty"`
	Enum        []string `json:"enum,omitempty"`
}

// Tools contains all 9 registered MCP tools with zero checkout blocks.
var Tools = []Tool{
	{
		Name:        "browser_navigate",
		Description: "Navigate the active browser page to a specified URL.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"url": {Type: "string", Description: "The absolute URL to navigate to (e.g. https://www.amazon.com)."},
			},
			Required: []string{"url"},
		},
	},
	{
		Name:        "browser_evaluate",
		Description: "Evaluate a JavaScript expression in the context of the active page.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"expression":    {Type: "string", Description: "JavaScript code to execute."},
				"awaitPromise":  {Type: "boolean", Description: "Whether to await promise result (default: true)."},
				"returnByValue": {Type: "boolean", Description: "Whether the result is returned by value (default: true)."},
			},
			Required: []string{"expression"},
		},
	},
	{
		Name:        "browser_get_content",
		Description: "Extract text content or HTML from the active page. Defaults to clean text capped at 15000 characters to prevent context bloat.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"selector":  {Type: "string", Description: "CSS selector (defaults to body)."},
				"maxLength": {Type: "number", Description: "Maximum characters to return (default: 15000)."},
			},
		},
	},
	{
		Name:        "browser_click",
		Description: "Click an element on the active page matching a CSS selector.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"selector": {Type: "string", Description: "CSS selector of the element to click."},
			},
			Required: []string{"selector"},
		},
	},
	{
		Name:        "browser_type",
		Description: "Type text into an input element on the active page with automatic input/change event dispatch.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"selector": {Type: "string", Description: "CSS selector of the input field."},
				"text":     {Type: "string", Description: "Text to input."},
				"clear":    {Type: "boolean", Description: "Clear existing text before typing (default: true)."},
			},
			Required: []string{"selector", "text"},
		},
	},
	{
		Name:        "browser_screenshot",
		Description: "Capture a screenshot of the current page viewport for visual verification.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"format": {Type: "string", Enum: []string{"png", "jpeg", "webp"}, Description: "Image format (default: png)."},
			},
		},
	},
	{
		Name:        "amazon_search",
		Description: "Search Amazon for a product and return parsed, structured product results (title, ASIN, price, prime eligibility, rating, URL).",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"query":      {Type: "string", Description: "Search keywords or product description."},
				"maxResults": {Type: "number", Description: "Maximum number of results to extract (default: 5)."},
			},
			Required: []string{"query"},
		},
	},
	{
		Name:        "amazon_get_cart",
		Description: "Inspect the current Amazon cart safely. Returns item count, subtotal, and list of items with title, price, quantity, and ASIN.",
		InputSchema: InputSchema{
			Type:       "object",
			Properties: map[string]PropertyDef{},
		},
	},
	{
		Name:        "amazon_add_to_cart",
		Description: "Add an item to the Amazon cart from the current product page or an ASIN/URL, then verify addition.",
		InputSchema: InputSchema{
			Type: "object",
			Properties: map[string]PropertyDef{
				"asin": {Type: "string", Description: "Amazon ASIN (10 alphanumeric characters)."},
			},
			Required: []string{"asin"},
		},
	},
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
		CDPHost:     "browser.aerial",
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
	for _, envURL := range []string{"KERNEL_CDP_URL", "KERNEL_API_URL", "STEEL_CDP_URL", "STEEL_API_URL"} {
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
