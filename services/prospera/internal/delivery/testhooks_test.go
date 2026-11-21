package delivery

import "net/http"

// Test-only seams; production code never calls these.

// WithReporterHTTPClient overrides both Reporter HTTP clients in unit tests.
func WithReporterHTTPClient(client *http.Client) ReporterOption {
	return func(r *Reporter) {
		r.internalClient, r.externalClient = client, client
	}
}

// WithReporterGeminiBaseURL overrides the Gemini REST base URL in unit tests.
func WithReporterGeminiBaseURL(baseURL string) ReporterOption {
	return func(r *Reporter) {
		r.geminiBaseURL = baseURL
	}
}
