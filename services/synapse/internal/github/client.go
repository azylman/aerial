package github

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/azylman/aerial/pkg/githubapi"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

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
		return githubapi.NewRequest(ctx, "", "", method, path, body)
	}
	return githubapi.NewRequest(ctx, c.APIURL, c.Token, method, path, body)
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
	return githubapi.GetJSON(ctx, client, req, maxBytes, out)
}
