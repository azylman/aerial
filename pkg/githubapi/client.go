package githubapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/githubauth"
)

// defaultMaxBytes caps a decoded response body when GetJSON gets maxBytes <= 0.
const defaultMaxBytes = 10 << 20

// maxForbiddenMessageRunes caps GitHub's error message in errors and logs.
const maxForbiddenMessageRunes = 200

// StatusError captures a non-success HTTP status code from the GitHub API.
// RateLimited is set when githubauth.IsRateLimited classified the response
// as a rate limit (429, or 403 with remaining 0 or Retry-After).
type StatusError struct {
	StatusCode  int
	Message     string
	RateLimited bool
	RetryAfter  time.Duration
}

func (e *StatusError) Error() string {
	return e.Message
}

// RateLimitDelay extracts the rate-limit retry delay from err if it wraps or is
// a rate-limited StatusError.
func RateLimitDelay(err error) (time.Duration, bool) {
	var se *StatusError
	if errors.As(err, &se) && se.RateLimited {
		return se.RetryAfter, true
	}
	return 0, false
}

// GetJSON executes req with client (http.DefaultClient when nil) and decodes
// the JSON response body, read up to maxBytes (10 MiB when maxBytes <= 0),
// into out. Rate-limited responses and plain 403s return a *StatusError
// carrying the classification; any other non-200 returns a *StatusError
// "github api returned status N".
func GetJSON(ctx context.Context, client *http.Client, req *http.Request, maxBytes int64, out any) error {
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("github api request failed: %w", err)
	}
	defer resp.Body.Close()

	if forbiddenErr := classifyForbidden(ctx, resp, req.URL.Path); forbiddenErr != nil {
		return forbiddenErr
	}
	if resp.StatusCode != http.StatusOK {
		return &StatusError{
			StatusCode: resp.StatusCode,
			Message:    fmt.Sprintf("github api returned status %d", resp.StatusCode),
		}
	}
	if maxBytes <= 0 {
		maxBytes = defaultMaxBytes
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if err != nil {
		return fmt.Errorf("read github api response: %w", err)
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("unmarshal github api response: %w", err)
	}
	return nil
}

// classifyForbidden returns a *StatusError for a rate-limited response (any
// 429, or a 403 githubauth.IsRateLimited flags) or a plain 403 permission
// error, and nil otherwise. A plain 403 logs GitHub's JSON message, capped;
// the request headers (and so the credential) are never logged.
func classifyForbidden(ctx context.Context, resp *http.Response, path string) error {
	if githubauth.IsRateLimited(resp) {
		return &StatusError{
			StatusCode:  resp.StatusCode,
			Message:     fmt.Sprintf("github api rate limit (status %d, remaining: %s)", resp.StatusCode, resp.Header.Get("X-Ratelimit-Remaining")),
			RateLimited: true,
			RetryAfter:  githubauth.RateLimitDelay(resp, time.Now()),
		}
	}
	if resp.StatusCode != http.StatusForbidden {
		return nil
	}
	msg := forbiddenMessage(resp.Body)
	slog.WarnContext(ctx, "[githubapi] forbidden", "path", path, "message", msg)
	return &StatusError{
		StatusCode: resp.StatusCode,
		Message:    "github api forbidden (status 403): " + msg,
	}
}

// forbiddenMessage extracts GitHub's JSON "message" field from body, capped
// at maxForbiddenMessageRunes. Non-JSON bodies yield an empty string so raw
// response bytes never reach logs.
func forbiddenMessage(body io.Reader) string {
	raw, err := io.ReadAll(io.LimitReader(body, 64<<10))
	if err != nil {
		return ""
	}
	var parsed struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(raw, &parsed) != nil {
		return ""
	}
	msg := strings.TrimSpace(parsed.Message)
	if r := []rune(msg); len(r) > maxForbiddenMessageRunes {
		msg = string(r[:maxForbiddenMessageRunes])
	}
	return msg
}
