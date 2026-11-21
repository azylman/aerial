package githubapi

import (
	"context"
	"net/http"
	"testing"
)

func TestNewRequest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		apiURL   string
		token    string
		method   string
		path     string
		wantURL  string
		wantAuth string
		wantErr  bool
	}{
		{name: "empty base uses default", apiURL: "", token: "", method: http.MethodGet, path: "/repos/o/r", wantURL: "https://api.github.com/repos/o/r"},
		{name: "blank base uses default", apiURL: "   ", token: "tok", method: http.MethodGet, path: "/user", wantURL: "https://api.github.com/user", wantAuth: "Bearer tok"},
		{name: "custom base", apiURL: "http://gh.local", token: "abc", method: http.MethodPost, path: "/repos/o/r/issues", wantURL: "http://gh.local/repos/o/r/issues", wantAuth: "Bearer abc"},
		{name: "trailing slashes trimmed", apiURL: "http://gh.local//", token: "", method: http.MethodGet, path: "/x", wantURL: "http://gh.local/x"},
		{name: "token whitespace trimmed", apiURL: "http://gh.local", token: "  t1  ", method: http.MethodGet, path: "/x", wantURL: "http://gh.local/x", wantAuth: "Bearer t1"},
		{name: "whitespace-only token omits auth", apiURL: "http://gh.local", token: "   ", method: http.MethodGet, path: "/x", wantURL: "http://gh.local/x"},
		{name: "invalid method errors", apiURL: "http://gh.local", token: "", method: "BAD METHOD", path: "/x", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req, err := NewRequest(context.Background(), tc.apiURL, tc.token, tc.method, tc.path, http.NoBody)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NewRequest error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewRequest error = %v", err)
			}
			if got := req.URL.String(); got != tc.wantURL {
				t.Errorf("URL = %q, want %q", got, tc.wantURL)
			}
			if req.Method != tc.method {
				t.Errorf("Method = %q, want %q", req.Method, tc.method)
			}
			if got := req.Header.Get("Accept"); got != "application/vnd.github+json" {
				t.Errorf("Accept = %q, want application/vnd.github+json", got)
			}
			if got := req.Header.Get("Authorization"); got != tc.wantAuth {
				t.Errorf("Authorization = %q, want %q", got, tc.wantAuth)
			}
		})
	}
}

func TestIsRetryable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		resp *http.Response
		want bool
	}{
		{name: "nil response", resp: nil, want: false},
		{name: "200 ok", resp: &http.Response{StatusCode: http.StatusOK, Header: http.Header{}}, want: false},
		{name: "404 not found", resp: &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{}}, want: false},
		{name: "500 server error", resp: &http.Response{StatusCode: http.StatusInternalServerError, Header: http.Header{}}, want: true},
		{name: "503 unavailable", resp: &http.Response{StatusCode: http.StatusServiceUnavailable, Header: http.Header{}}, want: true},
		{name: "429 too many requests", resp: &http.Response{StatusCode: http.StatusTooManyRequests, Header: http.Header{}}, want: true},
		{name: "403 plain forbidden", resp: &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{}}, want: false},
		{name: "403 with retry-after", resp: &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{"Retry-After": {"30"}}}, want: true},
		{name: "403 rate limit exhausted", resp: &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{"X-Ratelimit-Remaining": {"0"}}}, want: true},
		{name: "403 rate limit remaining", resp: &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{"X-Ratelimit-Remaining": {"12"}}}, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsRetryable(tc.resp); got != tc.want {
				t.Errorf("IsRetryable = %v, want %v", got, tc.want)
			}
		})
	}
}
