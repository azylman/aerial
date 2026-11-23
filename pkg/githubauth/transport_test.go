package githubauth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/tracing"
)

type fakeSource struct {
	value  string
	source string
	err    error

	mu          sync.Mutex
	invalidated []string
}

func (f *fakeSource) Token(context.Context) (string, string, error) {
	return f.value, f.source, f.err
}

func (f *fakeSource) Invalidate(v string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.invalidated = append(f.invalidated, v)
}

// headerRecorder captures the Authorization header seen by a test server.
type headerRecorder struct {
	mu   sync.Mutex
	auth []string
}

func (h *headerRecorder) record(r *http.Request) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.auth = append(h.auth, r.Header.Get("Authorization"))
}

func (h *headerRecorder) get() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.auth)
}

func TestTransportRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		srcValue        string
		srcSource       string
		srcErr          string
		apiHost         string // "{apiurl}" = API server URL, "{apihost}" = its host
		target          string // "{api}" prefix = API server URL
		upstreamStatus  int
		upstreamRemain  string
		wantErr         bool
		wantStatus      int
		wantAPIAuth     []string
		wantBlobAuth    []string
		wantInvalidated []string
		wantRequests    map[string]int
		wantRemaining   map[string]float64
	}{
		{
			name: "app token on api host", srcValue: "tok-1", srcSource: "app",
			apiHost: "{apihost}", target: "{api}/repos/o/r", upstreamStatus: 200, upstreamRemain: "7949",
			wantStatus: 200, wantAPIAuth: []string{"Bearer tok-1"},
			wantRequests: map[string]int{"app": 1}, wantRemaining: map[string]float64{"app": 7949},
		},
		{
			name: "api host given as URL", srcValue: "pat-a", srcSource: "pat",
			apiHost: "{apiurl}/", target: "{api}/repos/o/r", upstreamStatus: 200, upstreamRemain: "4999",
			wantStatus: 200, wantAPIAuth: []string{"Bearer pat-a"},
			wantRequests: map[string]int{"pat": 1}, wantRemaining: map[string]float64{"pat": 4999},
		},
		{
			name: "other host gets no token", srcValue: "tok-1", srcSource: "app",
			apiHost: "api.github.com", target: "{api}/repos/o/r", upstreamStatus: 200,
			wantStatus: 200, wantAPIAuth: []string{""},
		},
		{
			name: "redirect to blob host drops token", srcValue: "tok-1", srcSource: "app",
			apiHost: "{apihost}", target: "{api}/redirect", upstreamStatus: 200,
			wantStatus: 200, wantAPIAuth: []string{"Bearer tok-1"}, wantBlobAuth: []string{""},
			wantRequests: map[string]int{"app": 1},
		},
		{
			name: "401 invalidates the sent token", srcValue: "tok-1", srcSource: "app",
			apiHost: "{apihost}", target: "{api}/repos/o/r", upstreamStatus: 401, upstreamRemain: "bogus",
			wantStatus: 401, wantAPIAuth: []string{"Bearer tok-1"}, wantInvalidated: []string{"tok-1"},
			wantRequests: map[string]int{"app": 1},
		},
		{
			name: "401 without token invalidates nothing", srcValue: "", srcSource: "pat",
			apiHost: "{apihost}", target: "{api}/repos/o/r", upstreamStatus: 401,
			wantStatus: 401, wantAPIAuth: []string{""}, wantRequests: map[string]int{"pat": 1},
		},
		{
			name: "rate limit is returned untouched", srcValue: "tok-1", srcSource: "app",
			apiHost: "{apihost}", target: "{api}/repos/o/r", upstreamStatus: 403, upstreamRemain: "0",
			wantStatus: 403, wantAPIAuth: []string{"Bearer tok-1"},
			wantRequests: map[string]int{"app": 1}, wantRemaining: map[string]float64{"app": 0},
		},
		{
			name: "token error fails the request", srcErr: "githubauth: installation token unavailable",
			apiHost: "{apihost}", target: "{api}/repos/o/r", upstreamStatus: 200, wantErr: true,
		},
		{
			name: "base transport error", srcValue: "tok-1", srcSource: "app",
			apiHost: "127.0.0.1:1", target: "http://127.0.0.1:1/repos/o/r", wantErr: true,
			wantRequests: map[string]int{"app": 1},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api, apiSeen, blobSeen := newTransportServers(t, tc.upstreamStatus, tc.upstreamRemain)
			apiURL, err := url.Parse(api.URL)
			if err != nil {
				t.Fatalf("url.Parse: %v", err)
			}
			apiHost := strings.NewReplacer("{apiurl}", api.URL, "{apihost}", apiURL.Host).Replace(tc.apiHost)

			src := &fakeSource{value: tc.srcValue, source: tc.srcSource}
			if tc.srcErr != "" {
				src.err = errors.New(tc.srcErr)
			}
			metrics := newFakeMetrics()
			tr := NewTransport(nil, src, apiHost, WithMetrics(metrics))
			client := &http.Client{Transport: tr}
			t.Cleanup(tr.CloseIdleConnections)

			req, err := http.NewRequest(http.MethodGet, strings.Replace(tc.target, "{api}", api.URL, 1), http.NoBody)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			resp, err := client.Do(req)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Do() error = %v, wantErr %v", err, tc.wantErr)
			}
			if err == nil {
				resp.Body.Close()
				if resp.StatusCode != tc.wantStatus {
					t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
				}
			}
			if got := apiSeen.get(); !slices.Equal(got, tc.wantAPIAuth) {
				t.Fatalf("api Authorization = %q, want %q", got, tc.wantAPIAuth)
			}
			if got := blobSeen.get(); !slices.Equal(got, tc.wantBlobAuth) {
				t.Fatalf("blob Authorization = %q, want %q", got, tc.wantBlobAuth)
			}
			src.mu.Lock()
			gotInvalidated := slices.Clone(src.invalidated)
			src.mu.Unlock()
			if !slices.Equal(gotInvalidated, tc.wantInvalidated) {
				t.Fatalf("invalidated = %q, want %q", gotInvalidated, tc.wantInvalidated)
			}
			metrics.mu.Lock()
			defer metrics.mu.Unlock()
			assertCounts(t, "requests", metrics.requests, tc.wantRequests)
			assertCounts(t, "remaining", metrics.remaining, tc.wantRemaining)
		})
	}
}

// newTransportServers starts a blob server and an API server that redirects
// /redirect to the blob server and otherwise answers status with an optional
// X-Ratelimit-Remaining header. Both record the Authorization they receive.
func newTransportServers(t *testing.T, status int, remaining string) (*httptest.Server, *headerRecorder, *headerRecorder) {
	t.Helper()
	apiSeen, blobSeen := &headerRecorder{}, &headerRecorder{}
	blob := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		blobSeen.record(r)
	}))
	t.Cleanup(blob.Close)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiSeen.record(r)
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, blob.URL+"/blob", http.StatusFound)
			return
		}
		if remaining != "" {
			w.Header().Set("X-Ratelimit-Remaining", remaining)
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(api.Close)
	return api, apiSeen, blobSeen
}

// newAPIServer starts an API server answering status with an optional
// X-Ratelimit-Remaining header.
func newAPIServer(t *testing.T, status int, remaining string) *httptest.Server {
	t.Helper()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if remaining != "" {
			w.Header().Set("X-Ratelimit-Remaining", remaining)
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(api.Close)
	return api
}

func TestIsRateLimited(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		status  int
		headers map[string]string
		nilResp bool
		want    bool
	}{
		{"nil response", 0, nil, true, false},
		{"429", 429, nil, false, true},
		{"403 remaining zero", 403, map[string]string{"X-RateLimit-Remaining": "0"}, false, true},
		{"403 retry-after", 403, map[string]string{"Retry-After": "60", "X-RateLimit-Remaining": "12"}, false, true},
		{"403 not accessible by integration", 403, map[string]string{"X-RateLimit-Remaining": "7000"}, false, false},
		{"403 bare", 403, nil, false, false},
		{"200 remaining zero", 200, map[string]string{"X-RateLimit-Remaining": "0"}, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var resp *http.Response
			if !tc.nilResp {
				resp = &http.Response{StatusCode: tc.status, Header: http.Header{}}
				for k, v := range tc.headers {
					resp.Header.Set(k, v)
				}
			}
			if got := IsRateLimited(resp); got != tc.want {
				t.Fatalf("IsRateLimited() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRateLimitDelay(t *testing.T) {
	t.Parallel()
	baseNow := time.Unix(1700000000, 0)
	tests := []struct {
		name    string
		status  int
		headers map[string]string
		nilResp bool
		want    time.Duration
	}{
		{name: "nil response", nilResp: true, want: 0},
		{name: "not rate limited 200", status: 200, want: 0},
		{name: "not rate limited 403", status: 403, headers: map[string]string{"X-Ratelimit-Remaining": "100"}, want: 0},
		{name: "retry-after takes precedence", status: 429, headers: map[string]string{"Retry-After": "45", "X-Ratelimit-Reset": "1700000200"}, want: 45 * time.Second},
		{name: "retry-after invalid falls back to reset", status: 429, headers: map[string]string{"Retry-After": "abc", "X-Ratelimit-Reset": "1700000120"}, want: 120 * time.Second},
		{name: "reset in future", status: 403, headers: map[string]string{"X-Ratelimit-Remaining": "0", "X-Ratelimit-Reset": "1700000090"}, want: 90 * time.Second},
		{name: "reset in past defaults to 1 minute", status: 403, headers: map[string]string{"X-Ratelimit-Remaining": "0", "X-Ratelimit-Reset": "1699999900"}, want: time.Minute},
		{name: "no headers on 429 defaults to 1 minute", status: 429, want: time.Minute},
		{name: "delay capped at 1 hour", status: 429, headers: map[string]string{"Retry-After": "7200"}, want: time.Hour},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var resp *http.Response
			if !tc.nilResp {
				resp = &http.Response{StatusCode: tc.status, Header: http.Header{}}
				for k, v := range tc.headers {
					resp.Header.Set(k, v)
				}
			}
			if got := RateLimitDelay(resp, baseNow); got != tc.want {
				t.Fatalf("RateLimitDelay() = %v, want %v", got, tc.want)
			}
		})
	}
}

type wrapperTransport struct {
	inner http.RoundTripper
}

func (w *wrapperTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return w.inner.RoundTrip(r)
}

func (w *wrapperTransport) Unwrap() http.RoundTripper {
	return w.inner
}

func TestRateLimitResetAt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		setReset   int64
		wrapDepth  int
		nilTrip    bool
		wantActive bool
	}{
		{name: "nil roundtripper", nilTrip: true, wantActive: false},
		{name: "unconfigured transport", setReset: 0, wrapDepth: 0, wantActive: false},
		{name: "direct transport", setReset: 1700000500, wrapDepth: 0, wantActive: true},
		{name: "wrapped 1 layer", setReset: 1700000500, wrapDepth: 1, wantActive: true},
		{name: "wrapped 2 layers", setReset: 1700000500, wrapDepth: 2, wantActive: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilTrip {
				if got := RateLimitResetAt(nil); !got.IsZero() {
					t.Fatalf("RateLimitResetAt(nil) = %v, want zero", got)
				}
				return
			}
			tr := &Transport{}
			if tc.setReset > 0 {
				tr.rateLimitResetUnix.Store(tc.setReset)
			}
			var rt http.RoundTripper = tr
			for range tc.wrapDepth {
				rt = &wrapperTransport{inner: rt}
			}
			got := RateLimitResetAt(rt)
			if tc.wantActive {
				if got.Unix() != tc.setReset {
					t.Fatalf("RateLimitResetAt() = %v, want %v", got.Unix(), tc.setReset)
				}
			} else {
				if !got.IsZero() {
					t.Fatalf("RateLimitResetAt() = %v, want zero", got)
				}
			}
		})
	}
}

func TestTransportRateLimitState(t *testing.T) {
	t.Parallel()
	api := newAPIServer(t, 200, "5000")
	apiURL, err := url.Parse(api.URL)
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	tr := NewTransport(nil, &fakeSource{value: "tok", source: "app"}, apiURL.Host)
	tr.rateLimitResetUnix.Store(time.Now().Add(time.Minute).Unix())
	if tr.RateLimitResetAt().IsZero() {
		t.Fatal("expected active rate limit")
	}
	client := &http.Client{Transport: tr}
	req, err := http.NewRequest(http.MethodGet, api.URL+"/repos/o/r", http.NoBody)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()
	if !tr.RateLimitResetAt().IsZero() {
		t.Fatalf("RateLimitResetAt() = %v, want cleared to zero", tr.RateLimitResetAt())
	}
}

func TestRateLimitResetAtWithTracing(t *testing.T) {
	t.Parallel()
	api := newAPIServer(t, http.StatusTooManyRequests, "")
	apiURL, err := url.Parse(api.URL)
	if err != nil {
		t.Fatalf("url.Parse: %v", err)
	}
	tr := NewTransport(nil, &fakeSource{value: "tok", source: "app"}, apiURL.Host)
	client := tracing.Client(&http.Client{Transport: tr})

	req, err := http.NewRequest(http.MethodGet, api.URL+"/repos/o/r", http.NoBody)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	resp.Body.Close()

	if resetAt := RateLimitResetAt(client.Transport); resetAt.IsZero() {
		t.Fatal("expected non-zero rate-limit reset time through tracing-wrapped transport")
	}
}
