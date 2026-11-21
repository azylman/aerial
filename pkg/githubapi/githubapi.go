// Package githubapi builds authenticated GitHub REST API requests and
// classifies retryable responses for every Aerial service that talks to GitHub.
package githubapi

import (
	"context"
	"io"
	"net/http"
	"strings"
)

// DefaultAPIURL is the public GitHub REST API base URL.
const DefaultAPIURL = "https://api.github.com"

// NewRequest builds a GitHub REST API request for path relative to apiURL.
// An empty apiURL uses DefaultAPIURL and trailing slashes are dropped. The
// request accepts application/vnd.github+json and carries a Bearer token when
// token is non-empty.
func NewRequest(ctx context.Context, apiURL, token, method, path string, body io.Reader) (*http.Request, error) {
	base := strings.TrimSpace(apiURL)
	if base == "" {
		base = DefaultAPIURL
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	if t := strings.TrimSpace(token); t != "" {
		req.Header.Set("Authorization", "Bearer "+t)
	}
	return req, nil
}

// IsRetryable reports whether a GitHub response warrants a retry: 5xx, 429,
// or a 403 that carries Retry-After or an exhausted rate limit.
func IsRetryable(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	if resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests {
		return true
	}
	if resp.StatusCode == http.StatusForbidden {
		return resp.Header.Get("Retry-After") != "" || resp.Header.Get("X-Ratelimit-Remaining") == "0"
	}
	return false
}
