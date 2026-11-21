package github

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/azylman/aerial/pkg/githubauth"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

// StatusError captures a non-success HTTP status code from the GitHub API.
// RateLimited is set when githubauth.IsRateLimited classified the response
// as a rate limit (429, or 403 with remaining 0 or Retry-After).
type StatusError struct {
	StatusCode  int
	Message     string
	RateLimited bool
}

func (e *StatusError) Error() string {
	return e.Message
}

// IsNotFound returns true if the error represents a GitHub 404 StatusNotFound response.
func IsNotFound(err error) bool {
	if statusErr, ok := errors.AsType[*StatusError](err); ok {
		return statusErr.StatusCode == http.StatusNotFound
	}
	return false
}

// VerifySignature validates a GitHub webhook HMAC-SHA256 signature header against secret and body.
func VerifySignature(secret, signatureHeader string, body []byte) bool {
	if strings.TrimSpace(secret) == "" || strings.TrimSpace(signatureHeader) == "" {
		return false
	}
	parts := strings.SplitN(signatureHeader, "=", 2)
	if len(parts) != 2 || parts[0] != "sha256" {
		return false
	}
	sigBytes, err := hex.DecodeString(parts[1])
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expectedMAC := mac.Sum(nil)
	return hmac.Equal(sigBytes, expectedMAC)
}

// GitHubIssueComment represents a comment on a GitHub issue or pull request.
type GitHubIssueComment struct {
	ID   int64  `json:"id"`
	Body string `json:"body"`
}

// GitHubCheckRunPR represents a PR association within a check run payload.
type GitHubCheckRunPR struct {
	Number int               `json:"number"`
	Head   model.GitHubPRRef `json:"head"`
	Base   model.GitHubPRRef `json:"base"`
}

// NewRequest creates an authenticated HTTP request targeting the GitHub REST API.
func NewRequest(ctx context.Context, apiURL, token, method, path string, body io.Reader) (*http.Request, error) {
	baseURL := strings.TrimRight(model.FirstNonEmpty(apiURL, "https://api.github.com"), "/")
	req, err := http.NewRequestWithContext(ctx, method, baseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	if strings.TrimSpace(token) != "" {
		req.Header.Set("Authorization", "token "+strings.TrimSpace(token))
	}
	return req, nil
}

// GetJSON executes an HTTP request and decodes the JSON response body up to maxBytes.
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
		maxBytes = 10 << 20
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

// maxForbiddenMessageRunes caps GitHub's error message in errors and logs.
const maxForbiddenMessageRunes = 200

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
		}
	}
	if resp.StatusCode != http.StatusForbidden {
		return nil
	}
	msg := forbiddenMessage(resp.Body)
	slog.WarnContext(ctx, "[synapse] [github] forbidden", "path", path, "message", msg)
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

// IsRateLimitError reports whether err is a GitHub rate-limit StatusError.
func IsRateLimitError(err error) bool {
	statusErr, ok := errors.AsType[*StatusError](err)
	return ok && statusErr.RateLimited
}

// Client wraps an HTTP client and credentials for GitHub REST API calls.
type Client struct {
	HTTPClient *http.Client
	APIURL     string
	Token      string
}

// NewClient returns an initialized GitHub REST client.
func NewClient(httpClient *http.Client, apiURL, token string) *Client {
	return &Client{
		HTTPClient: httpClient,
		APIURL:     apiURL,
		Token:      token,
	}
}

// NewRequest creates a request targeting this client's configured base API URL and token.
func (c *Client) NewRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	if c == nil {
		return NewRequest(ctx, "", "", method, path, body)
	}
	return NewRequest(ctx, c.APIURL, c.Token, method, path, body)
}

// GetJSON executes a GET request against path and decodes into out.
func (c *Client) GetJSON(ctx context.Context, path string, maxBytes int64, out any) error {
	req, err := c.NewRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return fmt.Errorf("create github api request: %w", err)
	}
	client := http.DefaultClient
	if c != nil && c.HTTPClient != nil {
		client = c.HTTPClient
	}
	return GetJSON(ctx, client, req, maxBytes, out)
}
