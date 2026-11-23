package githubauth

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Transport injects the TokenSource credential into requests bound for the
// GitHub API host and records per-source request and rate-limit metrics.
//
// It never retries and never swaps credentials on a rate limit: a 429 or a
// rate-limited 403 is returned to the caller as is (see IsRateLimited).
type Transport struct {
	base               http.RoundTripper
	src                TokenSource
	apiHost            string
	metrics            Metrics
	rateLimitResetUnix atomic.Int64
}

// TransportOption configures a Transport.
type TransportOption func(*Transport)

// WithMetrics records request and rate-limit metrics into m.
func WithMetrics(m Metrics) TransportOption {
	return func(t *Transport) { t.metrics = metricsOrNop(m) }
}

// NewTransport wraps base (nil means http.DefaultTransport). apiHost is the
// GitHub API host ("api.github.com", or a URL whose host is used); only
// requests whose host equals it carry the Authorization header, so
// redirects to other hosts (actions log blobs) never see the token.
func NewTransport(base http.RoundTripper, src TokenSource, apiHost string, opts ...TransportOption) *Transport {
	if base == nil {
		base = http.DefaultTransport
	}
	t := &Transport{
		base:    base,
		src:     src,
		apiHost: normalizeHost(apiHost),
		metrics: nopMetrics{},
	}
	for _, opt := range opts {
		opt(t)
	}
	return t
}

func normalizeHost(h string) string {
	h = strings.TrimSpace(h)
	if strings.Contains(h, "://") {
		if u, err := url.Parse(h); err == nil {
			h = u.Host
		}
	}
	return strings.ToLower(strings.TrimRight(h, "/"))
}

// RoundTrip implements http.RoundTripper.
func (t *Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL == nil || !strings.EqualFold(req.URL.Host, t.apiHost) {
		return t.base.RoundTrip(req)
	}
	value, source, err := t.src.Token(req.Context())
	if err != nil {
		return nil, fmt.Errorf("githubauth: obtain token: %w", err)
	}
	r := req.Clone(req.Context())
	if value != "" {
		r.Header.Set("Authorization", "Bearer "+value)
	}
	t.metrics.IncRequest(source)
	resp, err := t.base.RoundTrip(r)
	if err != nil {
		return nil, err
	}
	if remaining, perr := strconv.ParseFloat(resp.Header.Get("X-Ratelimit-Remaining"), 64); perr == nil {
		t.metrics.SetRateLimitRemaining(source, remaining)
		if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices && remaining > 0 {
			t.rateLimitResetUnix.Store(0)
		}
	}
	if IsRateLimited(resp) {
		t.rateLimitResetUnix.Store(time.Now().Add(RateLimitDelay(resp, time.Now())).Unix())
	}
	if resp.StatusCode == http.StatusUnauthorized && value != "" {
		t.src.Invalidate(value)
	}
	return resp, nil
}

// CloseIdleConnections forwards to the base transport if supported.
func (t *Transport) CloseIdleConnections() {
	if c, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		c.CloseIdleConnections()
	}
}

// RateLimitResetAt returns the timestamp at which the rate limit resets, or time.Time{} when not rate limited.
func (t *Transport) RateLimitResetAt() time.Time {
	sec := t.rateLimitResetUnix.Load()
	if sec <= 0 {
		return time.Time{}
	}
	return time.Unix(sec, 0)
}

// Unwrap returns the underlying RoundTripper.
func (t *Transport) Unwrap() http.RoundTripper {
	return t.base
}

// RateLimitResetAt returns the rate-limit reset timestamp reported by rt or by any
// transport in its Unwrap() chain, or time.Time{} if none is found or recorded.
func RateLimitResetAt(rt http.RoundTripper) time.Time {
	curr := rt
	for curr != nil {
		if rlr, ok := curr.(interface{ RateLimitResetAt() time.Time }); ok {
			return rlr.RateLimitResetAt()
		}
		u, ok := curr.(interface{ Unwrap() http.RoundTripper })
		if !ok {
			break
		}
		curr = u.Unwrap()
	}
	return time.Time{}
}

// RateLimitDelay calculates the duration to wait based on Retry-After or X-Ratelimit-Reset
// headers when resp is rate-limited. When !IsRateLimited(resp), it returns 0.
// If neither header yields a positive duration, it defaults to time.Minute;
// any positive result is clamped to a maximum of time.Hour.
func RateLimitDelay(resp *http.Response, now time.Time) time.Duration {
	if !IsRateLimited(resp) {
		return 0
	}
	d := parseRetryAfter(resp.Header.Get("Retry-After"))
	if d <= 0 {
		d = parseResetDelay(resp.Header.Get("X-Ratelimit-Reset"), now)
	}
	if d <= 0 {
		d = time.Minute
	}
	if d > time.Hour {
		d = time.Hour
	}
	return d
}

func parseRetryAfter(header string) time.Duration {
	sec, err := strconv.ParseInt(strings.TrimSpace(header), 10, 64)
	if err != nil || sec <= 0 {
		return 0
	}
	return time.Duration(sec) * time.Second
}

func parseResetDelay(header string, now time.Time) time.Duration {
	sec, err := strconv.ParseInt(strings.TrimSpace(header), 10, 64)
	if err != nil {
		return 0
	}
	diff := time.Unix(sec, 0).Sub(now)
	if diff <= 0 {
		return 0
	}
	return diff
}

// IsRateLimited reports whether resp is a GitHub rate-limit rejection: any
// 429, or a 403 with X-RateLimit-Remaining "0" or a Retry-After header
// (secondary limit). A plain 403 such as "Resource not accessible by
// integration" is a permission error, not a rate limit. Callers must back
// off on a rate limit and never retry it with the PAT.
func IsRateLimited(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		return true
	case http.StatusForbidden:
		return resp.Header.Get("X-Ratelimit-Remaining") == "0" || resp.Header.Get("Retry-After") != ""
	default:
		return false
	}
}
