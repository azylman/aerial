package mcp

import (
	"net/url"
	"testing"
)

func TestIsClusterInternalHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		host string
		want bool
	}{
		{name: "empty string", host: "", want: false},
		{name: "whitespace only", host: "   ", want: false},
		{name: "localhost", host: "localhost", want: true},
		{name: "localhost with mixed case", host: "LocalHost", want: true},
		{name: "ipv4 loopback", host: "127.0.0.1", want: true},
		{name: "ipv6 loopback", host: "::1", want: true},
		{name: "aerial domain suffix", host: "coven.aerial", want: true},
		{name: "lan domain suffix", host: "haro.lan", want: true},
		{name: "cluster local suffix", host: "restate.cluster.local", want: true},
		{name: "svc domain suffix", host: "kubernetes-mcp.svc", want: true},
		{name: "svc cluster local full fqdn", host: "vanadis.default.svc.cluster.local", want: true},
		{name: "third party cloudflare mcp", host: "mcp.cloudflare.com", want: false},
		{name: "third party ubereats mcp", host: "mcp.ubereats.com", want: false},
		{name: "external example host", host: "example.com", want: false},
		{name: "evil lookalike domain", host: "evil-aerial.com", want: false},
		{name: "external public ip", host: "8.8.8.8", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isClusterInternalHost(tc.host)
			if got != tc.want {
				t.Errorf("isClusterInternalHost(%q) = %v, want %v", tc.host, got, tc.want)
			}
		})
	}
}

func TestTraceProxy_ShouldInjectBearer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		allowedHosts []string
		rawURL       string
		nilURL       bool
		want         bool
	}{
		{name: "nil url returns false", nilURL: true, want: false},
		{name: "empty hostname returns false", rawURL: "http:///path", want: false},
		{name: "default config allows cluster internal host", rawURL: "http://coven.aerial/mcp", want: true},
		{name: "default config rejects external host", rawURL: "https://mcp.cloudflare.com/mcp", want: false},
		{
			name:         "custom allowed host matches exact",
			allowedHosts: []string{"custom-upstream.internal"},
			rawURL:       "http://custom-upstream.internal/mcp",
			want:         true,
		},
		{
			name:         "custom allowed host matches wildcard pattern",
			allowedHosts: []string{"*.secure.corp"},
			rawURL:       "http://api.secure.corp/mcp",
			want:         true,
		},
		{
			name:         "custom allowed host rejects non-matching internal host",
			allowedHosts: []string{"custom-upstream.internal"},
			rawURL:       "http://coven.aerial/mcp",
			want:         false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := &TraceProxy{allowedAuthHosts: tc.allowedHosts}
			var u *url.URL
			if !tc.nilURL {
				var err error
				u, err = url.Parse(tc.rawURL)
				if err != nil {
					t.Fatalf("failed parsing url %q: %v", tc.rawURL, err)
				}
			}
			got := p.shouldInjectBearer(u)
			if got != tc.want {
				t.Errorf("shouldInjectBearer(%v) = %v, want %v", u, got, tc.want)
			}
		})
	}
}
