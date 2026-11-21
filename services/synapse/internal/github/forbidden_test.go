package github

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// forbiddenFixture is one GitHub response shape served at /f.
type forbiddenFixture struct {
	status     int
	remaining  string
	retryAfter string
	body       string
}

func forbiddenServer(t *testing.T, f forbiddenFixture) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.remaining != "" {
			w.Header().Set("X-Ratelimit-Remaining", f.remaining)
		}
		if f.retryAfter != "" {
			w.Header().Set("Retry-After", f.retryAfter)
		}
		w.WriteHeader(f.status)
		_, _ = w.Write([]byte(f.body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestGitHubForbiddenClassification(t *testing.T) {
	t.Parallel()
	longMsg := strings.Repeat("é", 250)
	tests := []struct {
		name            string
		fixture         forbiddenFixture
		wantStatus      int
		wantRateLimited bool
		wantMessage     string
	}{
		{
			name:            "403 with zero remaining is a rate limit",
			fixture:         forbiddenFixture{status: 403, remaining: "0", body: `{"message":"API rate limit exceeded"}`},
			wantStatus:      403,
			wantRateLimited: true,
			wantMessage:     "github api rate limit (status 403, remaining: 0)",
		},
		{
			name:            "403 with Retry-After is a secondary rate limit",
			fixture:         forbiddenFixture{status: 403, remaining: "12", retryAfter: "60"},
			wantStatus:      403,
			wantRateLimited: true,
			wantMessage:     "github api rate limit (status 403, remaining: 12)",
		},
		{
			name:            "429 is a rate limit",
			fixture:         forbiddenFixture{status: 429},
			wantStatus:      429,
			wantRateLimited: true,
			wantMessage:     "github api rate limit (status 429, remaining: )",
		},
		{
			name:        "plain 403 carries GitHub's message",
			fixture:     forbiddenFixture{status: 403, remaining: "4999", body: `{"message":"Resource not accessible by integration"}`},
			wantStatus:  403,
			wantMessage: "github api forbidden (status 403): Resource not accessible by integration",
		},
		{
			name:        "plain 403 message capped at 200 runes",
			fixture:     forbiddenFixture{status: 403, body: `{"message":"` + longMsg + `"}`},
			wantStatus:  403,
			wantMessage: "github api forbidden (status 403): " + strings.Repeat("é", 200),
		},
		{
			name:        "plain 403 with non-JSON body omits raw bytes",
			fixture:     forbiddenFixture{status: 403, body: "<html>nope</html>"},
			wantStatus:  403,
			wantMessage: "github api forbidden (status 403): ",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := forbiddenServer(t, tc.fixture)
			client := NewClient(srv.Client(), srv.URL, "")
			var out map[string]any
			for _, err := range []error{
				client.GetJSON(context.Background(), "/f", 1<<20, &out),
				func() error { _, err := UpdatePRBranch(context.Background(), client, "azylman/aerial", 7); return err }(),
			} {
				statusErr, ok := errors.AsType[*StatusError](err)
				if !ok {
					t.Fatalf("err = %v, want *StatusError", err)
				}
				if statusErr.StatusCode != tc.wantStatus || statusErr.RateLimited != tc.wantRateLimited || statusErr.Message != tc.wantMessage {
					t.Errorf("StatusError = %+v, want {%d %q %v}", *statusErr, tc.wantStatus, tc.wantMessage, tc.wantRateLimited)
				}
				if IsRateLimitError(err) != tc.wantRateLimited {
					t.Errorf("IsRateLimitError = %v, want %v", !tc.wantRateLimited, tc.wantRateLimited)
				}
			}
		})
	}
}

func TestIsRateLimitError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "generic error", err: errors.New("boom"), want: false},
		{name: "plain 403", err: &StatusError{StatusCode: 403}, want: false},
		{name: "rate limited", err: &StatusError{StatusCode: 429, RateLimited: true}, want: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsRateLimitError(tc.err); got != tc.want {
				t.Errorf("IsRateLimitError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
