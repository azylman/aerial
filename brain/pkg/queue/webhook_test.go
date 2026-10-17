package queue

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/runner"
)

type errReader struct{}

func (errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("simulated read error")
}

func TestWebhookDispatcher_OnWake(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		serverResponse   WakeResponse
		expectedOverride string
		expectedReason   string
	}{
		{
			name:             "wake override",
			serverResponse:   WakeResponse{Override: "wake", Reason: "direct priority"},
			expectedOverride: "wake",
			expectedReason:   "direct priority",
		},
		{
			name:             "drop override",
			serverResponse:   WakeResponse{Override: "drop", Reason: "noise"},
			expectedOverride: "drop",
			expectedReason:   "noise",
		},
		{
			name:             "classify override",
			serverResponse:   WakeResponse{Override: "classify"},
			expectedOverride: "classify",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var receivedHeaderEvent, receivedHeaderAgent, receivedHeaderType string

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedHeaderEvent = r.Header.Get("X-Aerial-Event")
				receivedHeaderAgent = r.Header.Get("User-Agent")
				receivedHeaderType = r.Header.Get("Content-Type")

				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(tc.serverResponse)
			}))
			defer ts.Close()

			dispatcher := NewDefaultWebhookDispatcher()
			endpoint := &config.WebhookEndpoint{URL: ts.URL, TimeoutMs: 1000}
			req := &WakeRequest{
				ChannelID: "chan-123",
				ThreadID:  "th-456",
				Message:   db.Message{ID: "msg-789", Content: "test content"},
			}

			resp, err := dispatcher.CallWakeHook(context.Background(), endpoint, req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.Override != tc.expectedOverride {
				t.Errorf("expected override %q, got %q", tc.expectedOverride, resp.Override)
			}
			if resp.Reason != tc.expectedReason {
				t.Errorf("expected reason %q, got %q", tc.expectedReason, resp.Reason)
			}
			if receivedHeaderEvent != "on_wake" {
				t.Errorf("expected X-Aerial-Event on_wake, got %q", receivedHeaderEvent)
			}
			if receivedHeaderAgent != "aerial-brain/1.0" {
				t.Errorf("expected User-Agent 'aerial-brain/1.0', got %q", receivedHeaderAgent)
			}
			if !strings.HasPrefix(receivedHeaderType, "application/json") {
				t.Errorf("expected Content-Type application/json, got %q", receivedHeaderType)
			}

			rec := httptest.NewRecorder()
			metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			body := rec.Body.String()
			if !strings.Contains(body, `aerial_brain_webhooks_dispatched_total{hook="on_wake",status="success"}`) {
				t.Errorf("expected metrics to contain on_wake success counter, got:\n%s", body)
			}
		})
	}
}

func TestWebhookDispatcher_PreTurn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		serverResponse     PreTurnResponse
		expectedAllow      bool
		expectedAction     string
		expectedRetryAfter int
		expectedContext    string
		expectedReason     string
		expectedMetadata   map[string]any
	}{
		{
			name: "allow with injected context and metadata",
			serverResponse: PreTurnResponse{
				Allow:           true,
				Action:          "proceed",
				InjectedContext: "system instructions from hook",
				Metadata:        map[string]any{"lease_id": "lease-abc"},
			},
			expectedAllow:    true,
			expectedAction:   "proceed",
			expectedContext:  "system instructions from hook",
			expectedMetadata: map[string]any{"lease_id": "lease-abc"},
		},
		{
			name: "deny with retry backoff",
			serverResponse: PreTurnResponse{
				Allow:             false,
				Action:            "retry",
				RetryAfterSeconds: 15,
				Reason:            "rate limited",
			},
			expectedAllow:      false,
			expectedAction:     "retry",
			expectedRetryAfter: 15,
			expectedReason:     "rate limited",
		},
		{
			name: "deny with drop",
			serverResponse: PreTurnResponse{
				Allow:  false,
				Action: "drop",
				Reason: "moderated content",
			},
			expectedAllow:  false,
			expectedAction: "drop",
			expectedReason: "moderated content",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var receivedHeaderEvent, receivedHeaderAgent, receivedHeaderType string

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedHeaderEvent = r.Header.Get("X-Aerial-Event")
				receivedHeaderAgent = r.Header.Get("User-Agent")
				receivedHeaderType = r.Header.Get("Content-Type")

				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(tc.serverResponse)
			}))
			defer ts.Close()

			dispatcher := NewDefaultWebhookDispatcher()
			endpoint := &config.WebhookEndpoint{URL: ts.URL, TimeoutMs: 1000}

			req := &PreTurnRequest{
				ChannelID:  "chan-pre-1",
				ThreadID:   "th-pre-1",
				BurstCount: 2,
				Messages:   []*db.Message{{ID: "m-1", Content: "hello"}, {ID: "m-2", Content: "world"}},
				Prompt:     "combined prompt",
				RetryCount: 1,
			}
			resp, err := dispatcher.CallPreTurnHook(context.Background(), endpoint, req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.Allow != tc.expectedAllow {
				t.Errorf("expected allow %v, got %v", tc.expectedAllow, resp.Allow)
			}
			if resp.Action != tc.expectedAction {
				t.Errorf("expected action %q, got %q", tc.expectedAction, resp.Action)
			}
			if resp.RetryAfterSeconds != tc.expectedRetryAfter {
				t.Errorf("expected retry after %d, got %d", tc.expectedRetryAfter, resp.RetryAfterSeconds)
			}
			if resp.InjectedContext != tc.expectedContext {
				t.Errorf("expected injected context %q, got %q", tc.expectedContext, resp.InjectedContext)
			}
			if resp.Reason != tc.expectedReason {
				t.Errorf("expected reason %q, got %q", tc.expectedReason, resp.Reason)
			}
			for k, v := range tc.expectedMetadata {
				if resp.Metadata == nil || resp.Metadata[k] != v {
					t.Errorf("expected metadata key %s=%v, got %+v", k, v, resp.Metadata)
				}
			}
			if receivedHeaderEvent != "pre_turn" {
				t.Errorf("expected X-Aerial-Event pre_turn, got %q", receivedHeaderEvent)
			}
			if receivedHeaderAgent != "aerial-brain/1.0" {
				t.Errorf("expected User-Agent 'aerial-brain/1.0', got %q", receivedHeaderAgent)
			}
			if !strings.HasPrefix(receivedHeaderType, "application/json") {
				t.Errorf("expected Content-Type application/json, got %q", receivedHeaderType)
			}
		})
	}
}

func TestWebhookDispatcher_PostTurn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		serverResponse       PostTurnResponse
		expectedAcknowledged bool
		expectedMetadata     map[string]any
	}{
		{
			name: "post_turn success",
			serverResponse: PostTurnResponse{
				Acknowledged: true,
				Metadata:     map[string]any{"released": true},
			},
			expectedAcknowledged: true,
			expectedMetadata:     map[string]any{"released": true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var receivedHeaderEvent, receivedHeaderAgent, receivedHeaderType string

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedHeaderEvent = r.Header.Get("X-Aerial-Event")
				receivedHeaderAgent = r.Header.Get("User-Agent")
				receivedHeaderType = r.Header.Get("Content-Type")

				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(tc.serverResponse)
			}))
			defer ts.Close()

			dispatcher := NewDefaultWebhookDispatcher()
			endpoint := &config.WebhookEndpoint{URL: ts.URL, TimeoutMs: 1000}

			req := &PostTurnRequest{
				ChannelID:    "chan-post",
				ThreadID:     "th-post",
				Status:       "success",
				ResponseText: "completed response",
				DurationMs:   1250,
				TokenUsage:   runner.TokenUsage{InputTokens: 100, OutputTokens: 50, TotalTokens: 150},
				Metadata:     map[string]any{"lease_id": "lease-abc"},
			}
			resp, err := dispatcher.CallPostTurnHook(context.Background(), endpoint, req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.Acknowledged != tc.expectedAcknowledged {
				t.Errorf("expected acknowledged %v, got %v", tc.expectedAcknowledged, resp.Acknowledged)
			}
			for k, v := range tc.expectedMetadata {
				if resp.Metadata == nil || resp.Metadata[k] != v {
					t.Errorf("expected metadata %s=%v, got %+v", k, v, resp.Metadata)
				}
			}
			if receivedHeaderEvent != "post_turn" {
				t.Errorf("expected X-Aerial-Event post_turn, got %q", receivedHeaderEvent)
			}
			if receivedHeaderAgent != "aerial-brain/1.0" {
				t.Errorf("expected User-Agent 'aerial-brain/1.0', got %q", receivedHeaderAgent)
			}
			if !strings.HasPrefix(receivedHeaderType, "application/json") {
				t.Errorf("expected Content-Type application/json, got %q", receivedHeaderType)
			}
		})
	}
}

func TestWebhookDispatcher_HTTPStatusAndTimeoutErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		statusCode   int
		responseBody string
		timeoutMs    int
		handlerDelay time.Duration
	}{
		{
			name:         "HTTP 500 error returns error",
			statusCode:   http.StatusInternalServerError,
			responseBody: `{"error": "internal crash"}`,
			timeoutMs:    500,
		},
		{
			name:         "Timeout triggers deadline exceeded",
			handlerDelay: 20 * time.Millisecond,
			timeoutMs:    10,
		},
		{
			name:         "Invalid JSON response returns unmarshal error",
			statusCode:   http.StatusOK,
			responseBody: `{not valid json`,
			timeoutMs:    500,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dispatcher := NewDefaultWebhookDispatcher()

			handlerDone := make(chan struct{})
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.handlerDelay > 0 {
					select {
					case <-r.Context().Done():
					case <-handlerDone:
					case <-time.After(tc.handlerDelay):
					}
				}
				statusCode := tc.statusCode
				if statusCode == 0 {
					statusCode = http.StatusOK
				}
				w.WriteHeader(statusCode)
				if tc.responseBody != "" {
					_, _ = w.Write([]byte(tc.responseBody))
				}
			}))
			defer func() {
				close(handlerDone)
				ts.Close()
			}()

			endpoint := &config.WebhookEndpoint{URL: ts.URL, TimeoutMs: tc.timeoutMs}
			resp, err := dispatcher.CallWakeHook(context.Background(), endpoint, &WakeRequest{ChannelID: "c1"})
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if resp != nil {
				t.Errorf("expected nil response on error, got %+v", resp)
			}
		})
	}
}

func TestWebhookDispatcher_ValidationAndGuardBranches(t *testing.T) {
	t.Parallel()
	dispatcher := NewDefaultWebhookDispatcher()

	if _, err := dispatcher.CallWakeHook(context.Background(), nil, &WakeRequest{}); err == nil {
		t.Errorf("expected error for nil endpoint")
	}
	if _, err := dispatcher.CallWakeHook(context.Background(), &config.WebhookEndpoint{URL: ""}, &WakeRequest{}); err == nil {
		t.Errorf("expected error for empty endpoint url")
	}

	ep := &config.WebhookEndpoint{URL: "http://127.0.0.1:9999"}
	if _, err := dispatcher.CallWakeHook(context.Background(), ep, nil); err == nil {
		t.Errorf("expected error for nil WakeRequest")
	}
	if _, err := dispatcher.CallPreTurnHook(context.Background(), ep, nil); err == nil {
		t.Errorf("expected error for nil PreTurnRequest")
	}
	if _, err := dispatcher.CallPostTurnHook(context.Background(), ep, nil); err == nil {
		t.Errorf("expected error for nil PostTurnRequest")
	}

	var nilDispatcher *DefaultWebhookDispatcher
	if _, err := nilDispatcher.CallWakeHook(context.Background(), &config.WebhookEndpoint{URL: "http://127.0.0.1"}, &WakeRequest{}); err == nil {
		t.Errorf("expected error on nil dispatcher")
	}

	customDisp := NewWebhookDispatcherWithClient(nil)
	if customDisp.client == nil {
		t.Errorf("expected non-nil client with NewWebhookDispatcherWithClient(nil)")
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(WakeResponse{Override: "wake"})
	}))
	defer ts.Close()

	dispWithNilClient := &DefaultWebhookDispatcher{client: nil}
	resp, err := dispWithNilClient.CallWakeHook(nil, &config.WebhookEndpoint{URL: ts.URL}, &WakeRequest{ChannelID: "c1"})
	if err != nil || resp.Override != "wake" {
		t.Fatalf("unexpected response with nil client and nil ctx: resp=%+v err=%v", resp, err)
	}

	epInvalid := &config.WebhookEndpoint{URL: "http://invalid-url-that-does-not-exist:9999", TimeoutMs: 5}
	if _, err := dispatcher.CallWakeHook(context.Background(), epInvalid, &WakeRequest{ChannelID: "c1"}); err == nil {
		t.Errorf("expected network error on invalid host")
	}
	if _, err := dispatcher.CallPreTurnHook(context.Background(), epInvalid, &PreTurnRequest{ChannelID: "c1"}); err == nil {
		t.Errorf("expected network error on invalid host for PreTurn")
	}
	if _, err := dispatcher.CallPostTurnHook(context.Background(), epInvalid, &PostTurnRequest{ChannelID: "c1"}); err == nil {
		t.Errorf("expected network error on invalid host for PostTurn")
	}

	if err := dispatcher.dispatch(context.Background(), "on_wake", &config.WebhookEndpoint{URL: "http://example.com"}, nil, nil); err == nil || !strings.Contains(err.Error(), "payload is nil") {
		t.Errorf("expected nil payload error, got %v", err)
	}
	if err := dispatcher.dispatch(context.Background(), "on_wake", &config.WebhookEndpoint{URL: "http://example.com"}, make(chan int), nil); err == nil || !strings.Contains(err.Error(), "failed to marshal") {
		t.Errorf("expected marshal error, got %v", err)
	}
	if err := dispatcher.dispatch(context.Background(), "on_wake", &config.WebhookEndpoint{URL: "http://example.com/\x7fbad"}, "ok", nil); err == nil || !strings.Contains(err.Error(), "failed to create http request") {
		t.Errorf("expected http request creation error, got %v", err)
	}
	clientWithErrBody := &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(errReader{}),
				Header:     make(http.Header),
			}, nil
		}),
	}
	dispWithErrBody := NewWebhookDispatcherWithClient(clientWithErrBody)
	if err := dispWithErrBody.dispatch(context.Background(), "on_wake", &config.WebhookEndpoint{URL: "http://example.com"}, "ok", &WakeResponse{}); err == nil || !strings.Contains(err.Error(), "failed to read webhook response body") {
		t.Errorf("expected body read error, got %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	epCanceled := &config.WebhookEndpoint{URL: ts.URL}
	if _, err := dispatcher.CallWakeHook(ctx, epCanceled, &WakeRequest{ChannelID: "c1"}); err == nil {
		t.Fatalf("expected error on canceled context")
	}
}
