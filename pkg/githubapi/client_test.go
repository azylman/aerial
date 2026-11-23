package githubapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/githubapitest"
)

func TestGetJSON(t *testing.T) {
	t.Parallel()
	longMsg := strings.Repeat("é", 250)
	tests := []struct {
		name            string
		fixture         githubapitest.Response
		maxBytes        int64
		nilClient       bool
		closedServer    bool
		wantID          int
		wantStatus      int
		wantRateLimited bool
		wantRetryAfter  time.Duration
		wantMessage     string
		wantErrSub      string
	}{
		{name: "200 decodes body", fixture: githubapitest.Response{Status: 200, Body: `{"id":7}`}, maxBytes: 1 << 20, wantID: 7},
		{name: "zero maxBytes uses 10 MiB default", fixture: githubapitest.Response{Status: 200, Body: `{"id":8}`}, maxBytes: 0, wantID: 8},
		{name: "nil client uses default client", fixture: githubapitest.Response{Status: 200, Body: `{"id":9}`}, maxBytes: 1 << 20, nilClient: true, wantID: 9},
		{name: "maxBytes truncation fails decode", fixture: githubapitest.Response{Status: 200, Body: `{"id":10}`}, maxBytes: 3, wantErrSub: "unmarshal github api response"},
		{name: "malformed JSON fails decode", fixture: githubapitest.Response{Status: 200, Body: `{not json`}, maxBytes: 1 << 20, wantErrSub: "unmarshal github api response"},
		{name: "closed server is a request error", fixture: githubapitest.Response{Status: 200}, maxBytes: 1 << 20, closedServer: true, wantErrSub: "github api request failed"},
		{name: "404 is a status error", fixture: githubapitest.Response{Status: 404}, maxBytes: 1 << 20, wantStatus: 404, wantMessage: "github api returned status 404"},
		{name: "500 is a status error", fixture: githubapitest.Response{Status: 500}, maxBytes: 1 << 20, wantStatus: 500, wantMessage: "github api returned status 500"},
		{name: "403 with zero remaining is a rate limit", fixture: githubapitest.Response{Status: 403, Remaining: "0", Body: `{"message":"API rate limit exceeded"}`}, wantStatus: 403, wantRateLimited: true, wantRetryAfter: time.Minute, wantMessage: "github api rate limit (status 403, remaining: 0)"},
		{name: "403 with Retry-After is a secondary rate limit", fixture: githubapitest.Response{Status: 403, Remaining: "12", RetryAfter: "60"}, wantStatus: 403, wantRateLimited: true, wantRetryAfter: 60 * time.Second, wantMessage: "github api rate limit (status 403, remaining: 12)"},
		{name: "429 is a rate limit", fixture: githubapitest.Response{Status: 429}, wantStatus: 429, wantRateLimited: true, wantRetryAfter: time.Minute, wantMessage: "github api rate limit (status 429, remaining: )"},
		{name: "plain 403 carries GitHub's message", fixture: githubapitest.Response{Status: 403, Remaining: "4999", Body: `{"message":"Resource not accessible by integration"}`}, wantStatus: 403, wantMessage: "github api forbidden (status 403): Resource not accessible by integration"},
		{name: "plain 403 message capped at 200 runes", fixture: githubapitest.Response{Status: 403, Body: `{"message":"` + longMsg + `"}`}, wantStatus: 403, wantMessage: "github api forbidden (status 403): " + strings.Repeat("é", 200)},
		{name: "plain 403 with non-JSON body omits raw bytes", fixture: githubapitest.Response{Status: 403, Body: "<html>nope</html>"}, wantStatus: 403, wantMessage: "github api forbidden (status 403): "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := githubapitest.New(t, map[string][]githubapitest.Response{"GET /f": {tc.fixture}})
			client := api.Client()
			if tc.nilClient {
				client = nil
			}
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, api.URL()+"/f", http.NoBody)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			if tc.closedServer {
				api.Close()
			}
			var out struct {
				ID int `json:"id"`
			}
			got := describeGetJSON(GetJSON(context.Background(), client, req, tc.maxBytes, &out), out.ID)
			want := getJSONResult{id: tc.wantID, status: tc.wantStatus, rateLimited: tc.wantRateLimited, retryAfter: tc.wantRetryAfter, message: tc.wantMessage}
			if tc.wantErrSub != "" && strings.Contains(got.plainErr, tc.wantErrSub) {
				want.plainErr = got.plainErr
			}
			if got != want {
				t.Errorf("GetJSON result = %+v, want %+v (error containing %q)", got, want, tc.wantErrSub)
			}
		})
	}
}

// getJSONResult flattens one GetJSON outcome for a single comparison.
type getJSONResult struct {
	id          int
	status      int
	rateLimited bool
	retryAfter  time.Duration
	message     string
	plainErr    string
}

func describeGetJSON(err error, id int) getJSONResult {
	if statusErr, ok := errors.AsType[*StatusError](err); ok {
		return getJSONResult{status: statusErr.StatusCode, rateLimited: statusErr.RateLimited, retryAfter: statusErr.RetryAfter, message: statusErr.Error()}
	}
	if err != nil {
		return getJSONResult{plainErr: err.Error()}
	}
	return getJSONResult{id: id}
}

// failingReader errors on every read.
type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }

func TestForbiddenMessage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body io.Reader
		want string
	}{
		{name: "json message trimmed", body: strings.NewReader(`{"message":"  denied  "}`), want: "denied"},
		{name: "missing message field", body: strings.NewReader(`{"documentation_url":"x"}`), want: ""},
		{name: "non-json body", body: strings.NewReader("plain text"), want: ""},
		{name: "read error", body: failingReader{}, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := forbiddenMessage(tc.body); got != tc.want {
				t.Errorf("forbiddenMessage = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRateLimitDelay(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		err       error
		wantDelay time.Duration
		wantOK    bool
	}{
		{name: "nil error", err: nil, wantDelay: 0, wantOK: false},
		{name: "generic error", err: errors.New("boom"), wantDelay: 0, wantOK: false},
		{name: "non-rate-limited status error", err: &StatusError{StatusCode: 404, Message: "not found"}, wantDelay: 0, wantOK: false},
		{name: "rate-limited status error", err: &StatusError{StatusCode: 429, RateLimited: true, RetryAfter: 45 * time.Second}, wantDelay: 45 * time.Second, wantOK: true},
		{name: "wrapped rate-limited error", err: fmt.Errorf("outer: %w", &StatusError{StatusCode: 403, RateLimited: true, RetryAfter: 90 * time.Second}), wantDelay: 90 * time.Second, wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotDelay, gotOK := RateLimitDelay(tc.err)
			if gotDelay != tc.wantDelay || gotOK != tc.wantOK {
				t.Fatalf("RateLimitDelay() = (%v, %v), want (%v, %v)", gotDelay, gotOK, tc.wantDelay, tc.wantOK)
			}
		})
	}
}
