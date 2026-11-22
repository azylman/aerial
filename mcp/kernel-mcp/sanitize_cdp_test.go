package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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
		name, rawWS, host, port, want string
	}{
		{
			name:  "url with no path and no trailing slash",
			rawWS: "http://example.com", host: "browser.aerial", port: "9222", want: "ws://browser.aerial:9222",
		},
		{
			name:  "standard cdp ws url",
			rawWS: "ws://127.0.0.1:9222/devtools/page/abc123", host: "browser.aerial", port: "9222", want: "ws://browser.aerial:9222/devtools/page/abc123",
		},
		{
			name:  "relative path only",
			rawWS: "/devtools/page/target1", host: "localhost", port: "4000", want: "ws://localhost:4000/devtools/page/target1",
		},
		{name: "scheme with empty path", rawWS: "ws://", host: "localhost", port: "4000", want: "ws://localhost:4000"},
		{
			name:  "url with query params",
			rawWS: "ws://oldhost:9000/ws?token=xyz", host: "newhost", port: "9222", want: "ws://newhost:9222/ws?token=xyz",
		},
		{name: "empty port", rawWS: "ws://oldhost/path", host: "newhost", want: "ws://newhost/path"},
		{name: "empty raw url", host: "host.aerial", port: "9222", want: "ws://host.aerial:9222"},
		{name: "invalid raw url fallback", rawWS: "http://[::1]:namedport", host: "browser.aerial", port: "9222", want: "ws://browser.aerial:9222"},
		{
			name:  "invalid raw url with slash prefix",
			rawWS: "http://[::1]:namedport/dev/page", host: "browser.aerial", port: "9222", want: "ws://browser.aerial:9222",
		},
		{
			name:  "non-url path with leading slash",
			rawWS: "/dev/page", host: "browser.aerial", port: "9222", want: "ws://browser.aerial:9222/dev/page",
		},
		{
			name:  "invalid url parse with leading slash and newline",
			rawWS: "/devtools/page\n", host: "browser.aerial", port: "9222", want: "ws://browser.aerial:9222/devtools/page\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := RewriteWSUrl(tc.rawWS, tc.host, tc.port); got != tc.want {
				t.Fatalf("RewriteWSUrl(%q, %q, %q) = %q, want %q", tc.rawWS, tc.host, tc.port, got, tc.want)
			}
		})
	}
}

func TestResolveCDPHost(t *testing.T) {
	t.Parallel()

	errLookup := errors.New("lookup failed")
	tests := []struct {
		name        string
		host        string
		lookupAddrs []string
		lookupErr   error
		want        string
	}{
		{name: "empty string defaults to 127.0.0.1", want: "127.0.0.1"},
		{name: "localhost passes through", host: "localhost", want: "localhost"},
		{name: "::1 passes through", host: "::1", want: "::1"},
		{name: "literal IPv4 passes through", host: "192.0.2.100", want: "192.0.2.100"},
		{name: "literal IPv6 passes through", host: "2001:db8::1", want: "2001:db8::1"},
		{
			name: "mixed v6 and v4 returns first IPv4",
			host: "browser.example.com", lookupAddrs: []string{"2001:db8::2", "192.0.2.7", "192.0.2.8"}, want: "192.0.2.7",
		},
		{name: "v6 only returns first addr", host: "browser.example.com", lookupAddrs: []string{"2001:db8::3", "2001:db8::4"}, want: "2001:db8::3"},
		{name: "lookup error returns host", host: "browser.example.com", lookupErr: errLookup, want: "browser.example.com"},
		{name: "empty lookup result returns host", host: "browser.example.com", lookupAddrs: []string{}, want: "browser.example.com"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lookup := func(context.Context, string) ([]string, error) { return tc.lookupAddrs, tc.lookupErr }
			if got := ResolveCDPHost(context.Background(), tc.host, lookup); got != tc.want {
				t.Fatalf("ResolveCDPHost(%q) = %q, want %q", tc.host, got, tc.want)
			}
		})
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
	t.Parallel()

	const ms = time.Millisecond
	tmpDir := t.TempDir()
	files := map[string]string{
		"config.yaml": "port: \"4099\"\ncdp_host: \"custom.aerial\"\ncdp_port: \"9333\"\nsettle_delay: \"250ms\"\n",
		"empty.yaml":  "",
		"bad.yaml":    "port: [invalid",
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(tmpDir, name), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	valid := filepath.Join(tmpDir, "config.yaml")
	defaults := Config{"4018", "", "9222", 500 * ms}
	fromYAML := Config{"4099", "custom.aerial", "9333", 250 * ms}

	tests := []struct {
		name           string
		path           string
		envKey, envVal string
		want           Config
		wantErr        string
	}{
		{name: "nonexistent path returns default config", path: filepath.Join(tmpDir, "does-not-exist.yaml"), want: defaults},
		{name: "empty path returns default config", want: defaults},
		{name: "empty file returns default config", path: filepath.Join(tmpDir, "empty.yaml"), want: defaults},
		{name: "valid YAML loaded successfully", path: valid, want: fromYAML},
		{name: "invalid YAML returns error", path: filepath.Join(tmpDir, "bad.yaml"), wantErr: "failed to parse config YAML from"},
		{name: "directory path returns read error", path: tmpDir, wantErr: "failed to read config file"},
		{name: "env PORT override", path: valid, envKey: "PORT", envVal: "5055", want: Config{"5055", "custom.aerial", "9333", 250 * ms}},
		{
			name: "env CDP_HOST override",
			path: valid, envKey: "CDP_HOST", envVal: "env-cdp.lan", want: Config{"4099", "env-cdp.lan", "9333", 250 * ms},
		},
		{name: "env CDP_PORT override", path: valid, envKey: "CDP_PORT", envVal: "9876", want: Config{"4099", "custom.aerial", "9876", 250 * ms}},
		{
			name: "env KERNEL_CDP_URL override",
			path: valid, envKey: "KERNEL_CDP_URL", envVal: "http://cdp.lan:9999", want: Config{"4099", "cdp.lan", "9999", 250 * ms},
		},
		{
			name: "env KERNEL_API_URL override",
			path: valid, envKey: "KERNEL_API_URL", envVal: "http://kernel.lan:9111", want: Config{"4099", "kernel.lan", "9111", 250 * ms},
		},
		{
			name: "env SETTLE_DELAY override",
			path: valid, envKey: "SETTLE_DELAY", envVal: "10ms", want: Config{"4099", "custom.aerial", "9333", 10 * ms},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := map[string]string{tc.envKey: tc.envVal}
			cfg, err := LoadConfig(tc.path, func(k string) string { return env[k] })
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("LoadConfig(%q) error = %v, want substring %q", tc.path, err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadConfig(%q) unexpected error: %v", tc.path, err)
			}
			if !reflect.DeepEqual(*cfg, tc.want) {
				t.Fatalf("LoadConfig(%q) = %+v, want %+v", tc.path, *cfg, tc.want)
			}
		})
	}
}
