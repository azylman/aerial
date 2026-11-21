// Package restateingress posts JSON requests to the Restate ingress and parses
// its error envelopes the same way for every service.
package restateingress

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// HTTPError is a non-2xx response from the Restate ingress.
type HTTPError struct {
	// Status is the HTTP status code.
	Status int
	// Message is the trimmed JSON "message" field, else the trimmed "error" field, else "".
	Message string
	// Body is the raw response body.
	Body string
}

// Error implements error.
func (e *HTTPError) Error() string {
	detail := e.Message
	if detail == "" {
		detail = e.Body
	}
	return fmt.Sprintf("restate ingress returned HTTP %d: %s", e.Status, detail)
}

// ParseHTTPError builds an HTTPError from a non-2xx status and body.
func ParseHTTPError(status int, body []byte) *HTTPError {
	out := &HTTPError{Status: status, Body: string(body)}
	var envelope struct {
		Message string `json:"message"`
		Error   string `json:"error"`
	}
	if json.Unmarshal(body, &envelope) == nil {
		out.Message = strings.TrimSpace(envelope.Message)
		if out.Message == "" {
			out.Message = strings.TrimSpace(envelope.Error)
		}
	}
	return out
}

// Post sends payload as JSON to endpoint and returns the response body.
// A nil client uses http.DefaultClient; a nil payload sends an empty body.
// idempotencyKey is sent as Idempotency-Key when non-blank. maxBody <= 0 reads
// the body unbounded. Marshal, transport and read errors are returned
// unwrapped; a non-2xx status returns *HTTPError.
func Post(ctx context.Context, client *http.Client, endpoint, idempotencyKey string, payload any, maxBody int64) ([]byte, error) {
	var body io.Reader = http.NoBody
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if key := strings.TrimSpace(idempotencyKey); key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var reader io.Reader = resp.Body
	if maxBody > 0 {
		reader = io.LimitReader(resp.Body, maxBody)
	}
	respBody, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, ParseHTTPError(resp.StatusCode, respBody)
	}
	return respBody, nil
}
