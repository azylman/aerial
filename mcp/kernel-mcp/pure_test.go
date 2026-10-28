package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestUnmarshalOptional(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		input       []byte
		initialVal  string
		expectedVal string
	}{
		{"valid json", []byte(`{"val":"ok"}`), "", "ok"},
		{"invalid json keeps destination", []byte(`invalid json`), "untouched", "untouched"},
		{"nil data keeps destination", nil, "untouched", "untouched"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var dest struct {
				Val string `json:"val"`
			}
			dest.Val = tc.initialVal
			unmarshalOptional(tc.input, &dest)
			if dest.Val != tc.expectedVal {
				t.Fatalf("expected val %q, got %q", tc.expectedVal, dest.Val)
			}
		})
	}
}

func TestSanitizePII(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"empty string", "", ""},
		{"no digits", "hello world", "hello world"},
		{"short numbers", "order #12345 total $99", "order #12345 total $99"},
		{"12 digits unchanged", "123456789012", "123456789012"},
		{"matched pattern non-card digits unchanged", "123-456-789-01", "123-456-789-01"},
		{"13 digits redacted", "4111222233334", "[REDACTED_CARD]"},
		{"15 digits with spaces redacted", "3782 822463 10005", "[REDACTED_CARD]"},
		{"16 digits with dashes redacted", "4111-2222-3333-4444", "[REDACTED_CARD]"},
		{"16 digits raw redacted", "4111222233334444", "[REDACTED_CARD]"},
		{"17 digits unchanged", "12345678901234567", "12345678901234567"},
		{"embedded in sentence", "Card 4111-2222-3333-4444 was charged $50", "Card [REDACTED_CARD] was charged $50"},
		{"dash format with invalid length", "12-34-56", "12-34-56"},
		{"digits with symbols outside range", "123-456-789-012 ", "123-456-789-012 "},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := SanitizePII(tc.input)
			if got != tc.expected {
				t.Fatalf("SanitizePII(%q) = %q, want %q", tc.input, got, tc.expected)
			}
		})
	}
}

func TestValidateASIN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		asin     string
		expected bool
	}{
		{"valid alphanumeric 10 chars", "B08N5WRWNW", true},
		{"valid numeric 10 chars", "1234567890", true},
		{"valid alpha 10 chars", "ABCDEFGHIJ", true},
		{"too short 9 chars", "B08N5WRWN", false},
		{"too long 11 chars", "B08N5WRWNWX", false},
		{"contains dash", "B08N5-RWNW", false},
		{"contains space", "B08N5 WRNW", false},
		{"empty string", "", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ValidateASIN(tc.asin)
			if got != tc.expected {
				t.Fatalf("ValidateASIN(%q) = %v, want %v", tc.asin, got, tc.expected)
			}
		})
	}
}

func TestTruncateContent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		input         string
		maxLen        int
		wantTruncated bool
		wantPrefix    string
	}{
		{"short content not truncated", "short", 10, false, "short"},
		{"exact length not truncated", "12345", 5, false, "12345"},
		{"over max truncated", "1234567890", 5, true, "12345\n...[truncated]"},
		{"zero limit defaults to 15000", strings.Repeat("a", 15001), 0, true, strings.Repeat("a", 15000) + "\n...[truncated]"},
		{"negative limit defaults to 15000", "short text", -1, false, "short text"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotText, gotTrunc := TruncateContent(tc.input, tc.maxLen)
			if gotTrunc != tc.wantTruncated {
				t.Fatalf("TruncateContent truncated = %v, want %v", gotTrunc, tc.wantTruncated)
			}
			if gotText != tc.wantPrefix {
				t.Fatalf("TruncateContent text = %q, want %q", gotText, tc.wantPrefix)
			}
		})
	}
}

func TestRewriteWSUrl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rawWS    string
		host     string
		port     string
		expected string
	}{
		{
			"url with no path and no trailing slash",
			"http://example.com",
			"browser.aerial",
			"9222",
			"ws://browser.aerial:9222",
		},
		{
			"standard cdp ws url",
			"ws://127.0.0.1:9222/devtools/page/abc123",
			"browser.aerial",
			"9222",
			"ws://browser.aerial:9222/devtools/page/abc123",
		},
		{
			"relative path only",
			"/devtools/page/target1",
			"localhost",
			"4000",
			"ws://localhost:4000/devtools/page/target1",
		},
		{
			"scheme with empty path",
			"ws://",
			"localhost",
			"4000",
			"ws://localhost:4000",
		},
		{
			"url with query params",
			"ws://oldhost:9000/ws?token=xyz",
			"newhost",
			"9222",
			"ws://newhost:9222/ws?token=xyz",
		},
		{
			"empty port",
			"ws://oldhost/path",
			"newhost",
			"",
			"ws://newhost/path",
		},
		{
			"empty raw url",
			"",
			"host.aerial",
			"9222",
			"ws://host.aerial:9222",
		},
		{
			"invalid raw url fallback",
			"http://[::1]:namedport",
			"browser.aerial",
			"9222",
			"ws://browser.aerial:9222",
		},
		{
			"invalid raw url with slash prefix",
			"http://[::1]:namedport/dev/page",
			"browser.aerial",
			"9222",
			"ws://browser.aerial:9222",
		},
		{
			"non-url path with leading slash",
			"/dev/page",
			"browser.aerial",
			"9222",
			"ws://browser.aerial:9222/dev/page",
		},
		{
			"invalid url parse with leading slash and newline",
			"/devtools/page\n",
			"browser.aerial",
			"9222",
			"ws://browser.aerial:9222/devtools/page\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := RewriteWSUrl(tc.rawWS, tc.host, tc.port)
			if got != tc.expected {
				t.Fatalf("RewriteWSUrl(%q, %q, %q) = %q, want %q", tc.rawWS, tc.host, tc.port, got, tc.expected)
			}
		})
	}
}

func TestResolveCDPHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		host     string
		expected string
	}{
		{"empty string defaults to 127.0.0.1", "", "127.0.0.1"},
		{"localhost passes through", "localhost", "localhost"},
		{"::1 passes through", "::1", "::1"},
		{"literal IPv4 passes through", "192.168.1.100", "192.168.1.100"},
		{"literal IPv6 passes through", "2001:db8::1", "2001:db8::1"},
		{"unresolvable domain returns host", "nonexistent.domain.invalid", "nonexistent.domain.invalid"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ResolveCDPHost(context.Background(), tc.host)
			if got != tc.expected {
				t.Fatalf("ResolveCDPHost(%q) = %q, want %q", tc.host, got, tc.expected)
			}
		})
	}

	if h, err := os.Hostname(); err == nil && h != "" {
		res := ResolveCDPHost(context.Background(), h)
		if res == "" {
			t.Fatalf("expected non-empty IP for hostname %s", h)
		}
	}
}

func TestScriptBuilders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		script   string
		contains string
	}{
		{"build get content script selector", BuildGetContentScript("#main"), "document.querySelector"},
		{"build get content script empty selector", BuildGetContentScript(""), "document.body"},
		{"build click script", BuildClickScript(".btn-buy"), "getBoundingClientRect"},
		{"build focus and clear true", BuildFocusAndClearScript("input#search", true), `el.value = ""`},
		{"build focus and clear false", BuildFocusAndClearScript("input#search", false), `false`},
		{"build dispatch input events", BuildDispatchInputEventsScript("input#name"), `Event("input"`},
		{"build amazon search script limit", BuildAmazonSearchScript(7), "Math.min(nodes.length, 7)"},
		{"build amazon search script default limit", BuildAmazonSearchScript(0), "Math.min(nodes.length, 5)"},
		{"build amazon search script delivery recipe", BuildAmazonSearchScript(5), `delivery-recipe`},
		{"build amazon cart script", BuildAmazonCartScript(), ".a-truncate-full"},
		{"build amazon add to cart script", BuildAmazonAddToCartScript(), "#newAccordionRow_0"},
		{"build amazon cart count script", BuildAmazonCartCountScript(), "#attachSiNoCoverage"},
		{"stealth init script webgpu and offscreen", StealthInitScript, "OffscreenCanvas"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if !strings.Contains(tc.script, tc.contains) {
				t.Fatalf("Script in %s did not contain %q", tc.name, tc.contains)
			}
		})
	}
}

func TestValidateEvaluateExpression(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		expr    string
		wantErr bool
	}{
		{"safe dom read", "document.querySelector('#price')?.innerText", false},
		{"blocked fetch", "fetch('https://www.amazon.com/s?k=ram')", true},
		{"blocked xhr", "new XMLHttpRequest()", true},
		{"blocked click", "btn?.click()", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if gotErr := ValidateEvaluateExpression(tc.expr) != nil; gotErr != tc.wantErr {
				t.Fatalf("ValidateEvaluateExpression(%q) gotErr = %v, want %v", tc.expr, gotErr, tc.wantErr)
			}
		})
	}
}

func TestJitterCoord(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		x, y, w, h  int
		wantChanged bool
	}{
		{"small unchanged", 100, 200, 6, 6, false},
		{"standard jittered", 200, 150, 120, 40, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gx, gy := JitterCoord(tc.x, tc.y, tc.w, tc.h)
			if (gx != tc.x || gy != tc.y) != tc.wantChanged {
				t.Fatalf("JitterCoord(%d,%d,%d,%d) = (%d,%d)", tc.x, tc.y, tc.w, tc.h, gx, gy)
			}
		})
	}
}

func TestToolsDeclaration(t *testing.T) {
	t.Parallel()

	if len(Tools) != 9 {
		t.Fatalf("Tools length = %d, want 9", len(Tools))
	}

	for _, tool := range Tools {
		if strings.Contains(strings.ToLower(tool.Description), "blocked on") {
			t.Fatalf("Tool %s contains stale checkout block description: %q", tool.Name, tool.Description)
		}
	}
}

func TestParseURLHostPort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		raw      string
		wantHost string
		wantPort string
	}{
		{"empty string", "", "", ""},
		{"with scheme", "http://browser.aerial:9222", "browser.aerial", "9222"},
		{"without scheme", "browser.aerial:9222", "browser.aerial", "9222"},
		{"invalid url", "http://[::1]:badport", "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h, p := parseURLHostPort(tc.raw)
			if h != tc.wantHost || p != tc.wantPort {
				t.Fatalf("parseURLHostPort(%q) = (%q, %q), want (%q, %q)", tc.raw, h, p, tc.wantHost, tc.wantPort)
			}
		})
	}
}

func TestLoadConfig(t *testing.T) {
	tmpDir := t.TempDir()

	validYAMLPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(validYAMLPath, []byte(`
port: "4099"
cdp_host: "custom.aerial"
cdp_port: "9333"
settle_delay: "250ms"
`), 0o600); err != nil {
		t.Fatal(err)
	}

	emptyYAMLPath := filepath.Join(tmpDir, "empty.yaml")
	if err := os.WriteFile(emptyYAMLPath, []byte(""), 0o600); err != nil {
		t.Fatal(err)
	}

	invalidYAMLPath := filepath.Join(tmpDir, "bad.yaml")
	if err := os.WriteFile(invalidYAMLPath, []byte(`port: [invalid`), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name        string
		path        string
		envKey      string
		envVal      string
		wantPort    string
		wantHost    string
		wantCDPPort string
		wantDelay   time.Duration
		wantErr     bool
	}{
		{
			name:        "nonexistent path returns default config",
			path:        filepath.Join(tmpDir, "does-not-exist.yaml"),
			wantPort:    "4018",
			wantHost:    "browser.aerial",
			wantCDPPort: "9222",
			wantDelay:   500 * time.Millisecond,
			wantErr:     false,
		},
		{
			name:        "empty path returns default config",
			path:        "",
			wantPort:    "4018",
			wantHost:    "browser.aerial",
			wantCDPPort: "9222",
			wantDelay:   500 * time.Millisecond,
			wantErr:     false,
		},
		{
			name:        "empty file returns default config",
			path:        emptyYAMLPath,
			wantPort:    "4018",
			wantHost:    "browser.aerial",
			wantCDPPort: "9222",
			wantDelay:   500 * time.Millisecond,
			wantErr:     false,
		},
		{
			name:        "valid YAML loaded successfully",
			path:        validYAMLPath,
			wantPort:    "4099",
			wantHost:    "custom.aerial",
			wantCDPPort: "9333",
			wantDelay:   250 * time.Millisecond,
			wantErr:     false,
		},
		{
			name:    "invalid YAML returns error",
			path:    invalidYAMLPath,
			wantErr: true,
		},
		{
			name:    "directory path returns read error",
			path:    tmpDir,
			wantErr: true,
		},
		{
			name:        "env PORT override",
			path:        validYAMLPath,
			envKey:      "PORT",
			envVal:      "5055",
			wantPort:    "5055",
			wantHost:    "custom.aerial",
			wantCDPPort: "9333",
			wantDelay:   250 * time.Millisecond,
			wantErr:     false,
		},
		{
			name:        "env CDP_HOST override",
			path:        validYAMLPath,
			envKey:      "CDP_HOST",
			envVal:      "env-cdp.lan",
			wantPort:    "4099",
			wantHost:    "env-cdp.lan",
			wantCDPPort: "9333",
			wantDelay:   250 * time.Millisecond,
			wantErr:     false,
		},
		{
			name:        "env CDP_PORT override",
			path:        validYAMLPath,
			envKey:      "CDP_PORT",
			envVal:      "9876",
			wantPort:    "4099",
			wantHost:    "custom.aerial",
			wantCDPPort: "9876",
			wantDelay:   250 * time.Millisecond,
			wantErr:     false,
		},
		{
			name:        "env KERNEL_CDP_URL override",
			path:        validYAMLPath,
			envKey:      "KERNEL_CDP_URL",
			envVal:      "http://cdp.lan:9999",
			wantPort:    "4099",
			wantHost:    "cdp.lan",
			wantCDPPort: "9999",
			wantDelay:   250 * time.Millisecond,
			wantErr:     false,
		},
		{
			name:        "env STEEL_CDP_URL override",
			path:        validYAMLPath,
			envKey:      "STEEL_CDP_URL",
			envVal:      "http://steel.lan:9111",
			wantPort:    "4099",
			wantHost:    "steel.lan",
			wantCDPPort: "9111",
			wantDelay:   250 * time.Millisecond,
			wantErr:     false,
		},
		{
			name:        "env SETTLE_DELAY override",
			path:        validYAMLPath,
			envKey:      "SETTLE_DELAY",
			envVal:      "10ms",
			wantPort:    "4099",
			wantHost:    "custom.aerial",
			wantCDPPort: "9333",
			wantDelay:   10 * time.Millisecond,
			wantErr:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("PORT", "")
			t.Setenv("CDP_HOST", "")
			t.Setenv("CDP_PORT", "")
			t.Setenv("KERNEL_CDP_URL", "")
			t.Setenv("KERNEL_API_URL", "")
			t.Setenv("STEEL_CDP_URL", "")
			t.Setenv("STEEL_API_URL", "")
			t.Setenv("SETTLE_DELAY", "")
			if tc.envKey != "" {
				t.Setenv(tc.envKey, tc.envVal)
			}
			cfg, err := LoadConfig(tc.path)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("LoadConfig(%q) expected error, got nil", tc.path)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadConfig(%q) unexpected error: %v", tc.path, err)
			}
			if cfg.Port != tc.wantPort {
				t.Fatalf("Port = %q, want %q", cfg.Port, tc.wantPort)
			}
			if cfg.CDPHost != tc.wantHost {
				t.Fatalf("CDPHost = %q, want %q", cfg.CDPHost, tc.wantHost)
			}
			if cfg.CDPPort != tc.wantCDPPort {
				t.Fatalf("CDPPort = %q, want %q", cfg.CDPPort, tc.wantCDPPort)
			}
			if cfg.SettleDelay != tc.wantDelay {
				t.Fatalf("SettleDelay = %v, want %v", cfg.SettleDelay, tc.wantDelay)
			}
		})
	}
}
