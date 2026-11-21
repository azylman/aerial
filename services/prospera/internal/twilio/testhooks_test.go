package twilio

import (
	"net/http"
	"strings"
)

// Test-only seams; production code never calls these.

// WithTwilioBaseURL overrides the Twilio API base URL.
func WithTwilioBaseURL(baseURL string) TwilioOption {
	return func(c *HTTPTwilioClient) {
		c.baseURL = strings.TrimRight(baseURL, "/")
	}
}

// WithTwilioHTTPClient overrides the HTTP client used for Twilio requests.
func WithTwilioHTTPClient(client *http.Client) TwilioOption {
	return func(c *HTTPTwilioClient) {
		c.httpClient = client
	}
}
