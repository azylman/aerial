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

func TestWebhookDispatcher_Hooks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		hookType             string // "wake", "pre_turn", "post_turn", "metrics"
		serverResponse       any
		serverStatusCode     int
		expectError          bool
		expectedHeaderEvent  string
		expectedOverride     string
		expectedReason       string
		expectedAllow        bool
		expectedAction       string
		expectedRetryAfter   int
		expectedContext      string
		expectedMetaKey      string
		expectedMetaVal      any
		expectedAcknowledged bool
	}{
		{
			name:                "wake override",
			hookType:            "wake",
			serverResponse:      WakeResponse{Override: "wake", Reason: "direct priority"},
			serverStatusCode:    http.StatusOK,
			expectedHeaderEvent: "on_wake",
			expectedOverride:    "wake",
			expectedReason:      "direct priority",
		},
		{
			name:                "drop override",
			hookType:            "wake",
			serverResponse:      WakeResponse{Override: "drop", Reason: "noise"},
			serverStatusCode:    http.StatusOK,
			expectedHeaderEvent: "on_wake",
			expectedOverride:    "drop",
			expectedReason:      "noise",
		},
		{
			name:                "classify override",
			hookType:            "wake",
			serverResponse:      WakeResponse{Override: "classify"},
			serverStatusCode:    http.StatusOK,
			expectedHeaderEvent: "on_wake",
			expectedOverride:    "classify",
		},
		{
			name:     "pre_turn allow with injected context and metadata",
			hookType: "pre_turn",
			serverResponse: PreTurnResponse{
				Allow:           true,
				Action:          "proceed",
				InjectedContext: "system instructions from hook",
				Metadata:        map[string]any{"lease_id": "lease-abc"},
			},
			serverStatusCode:    http.StatusOK,
			expectedHeaderEvent: "pre_turn",
			expectedAllow:       true,
			expectedAction:      "proceed",
			expectedContext:     "system instructions from hook",
			expectedMetaKey:     "lease_id",
			expectedMetaVal:     "lease-abc",
		},
		{
			name:     "pre_turn deny with retry backoff",
			hookType: "pre_turn",
			serverResponse: PreTurnResponse{
				Allow:             false,
				Action:            "retry",
				RetryAfterSeconds: 15,
				Reason:            "rate limited",
			},
			serverStatusCode:    http.StatusOK,
			expectedHeaderEvent: "pre_turn",
			expectedAllow:       false,
			expectedAction:      "retry",
			expectedRetryAfter:  15,
			expectedReason:      "rate limited",
		},
		{
			name:     "pre_turn deny with drop",
			hookType: "pre_turn",
			serverResponse: PreTurnResponse{
				Allow:  false,
				Action: "drop",
				Reason: "moderated content",
			},
			serverStatusCode:    http.StatusOK,
			expectedHeaderEvent: "pre_turn",
			expectedAllow:       false,
			expectedAction:      "drop",
			expectedReason:      "moderated content",
		},
		{
			name:     "post_turn success",
			hookType: "post_turn",
			serverResponse: PostTurnResponse{
				Acknowledged: true,
				Metadata:     map[string]any{"released": true},
			},
			serverStatusCode:     http.StatusOK,
			expectedHeaderEvent:  "post_turn",
			expectedAcknowledged: true,
			expectedMetaKey:      "released",
			expectedMetaVal:      true,
		},
		{
			name:                "metrics recorded on wake success",
			hookType:            "metrics",
			serverResponse:      WakeResponse{Override: "wake"},
			serverStatusCode:    http.StatusOK,
			expectedHeaderEvent: "on_wake",
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

				w.WriteHeader(tc.serverStatusCode)
				_ = json.NewEncoder(w).Encode(tc.serverResponse)
			}))
			defer ts.Close()

			dispatcher := NewDefaultWebhookDispatcher()
			endpoint := &config.WebhookEndpoint{URL: ts.URL, TimeoutMs: 1000}

			switch tc.hookType {
			case "wake":
				req := &WakeRequest{
					ChannelID: "chan-123",
					ThreadID:  "th-456",
					Message:   db.Message{ID: "msg-789", Content: "test content"},
				}
				resp, err := dispatcher.CallWakeHook(context.Background(), endpoint, req)
				if tc.expectError && err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !tc.expectError && err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !tc.expectError {
					if resp.Override != tc.expectedOverride {
						t.Errorf("expected override %q, got %q", tc.expectedOverride, resp.Override)
					}
					if resp.Reason != tc.expectedReason {
						t.Errorf("expected reason %q, got %q", tc.expectedReason, resp.Reason)
					}
					if receivedHeaderEvent != tc.expectedHeaderEvent {
						t.Errorf("expected X-Aerial-Event %q, got %q", tc.expectedHeaderEvent, receivedHeaderEvent)
					}
					if receivedHeaderAgent != "aerial-brain/1.0" {
						t.Errorf("expected User-Agent 'aerial-brain/1.0', got %q", receivedHeaderAgent)
					}
					if !strings.HasPrefix(receivedHeaderType, "application/json") {
						t.Errorf("expected Content-Type application/json, got %q", receivedHeaderType)
					}
				}

			case "pre_turn":
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
				if tc.expectedMetaKey != "" {
					if resp.Metadata == nil || resp.Metadata[tc.expectedMetaKey] != tc.expectedMetaVal {
						t.Errorf("expected metadata key %s=%v, got %+v", tc.expectedMetaKey, tc.expectedMetaVal, resp.Metadata)
					}
				}
				if receivedHeaderEvent != tc.expectedHeaderEvent {
					t.Errorf("expected X-Aerial-Event %q, got %q", tc.expectedHeaderEvent, receivedHeaderEvent)
				}

			case "post_turn":
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
				if tc.expectedMetaKey != "" {
					if resp.Metadata == nil || resp.Metadata[tc.expectedMetaKey] != tc.expectedMetaVal {
						t.Errorf("expected metadata %s=%v, got %+v", tc.expectedMetaKey, tc.expectedMetaVal, resp.Metadata)
					}
				}
				if receivedHeaderEvent != tc.expectedHeaderEvent {
					t.Errorf("expected X-Aerial-Event %q, got %q", tc.expectedHeaderEvent, receivedHeaderEvent)
				}

			case "metrics":
				_, err := dispatcher.CallWakeHook(context.Background(), endpoint, &WakeRequest{ChannelID: "c1"})
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				rec := httptest.NewRecorder()
				metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
				body := rec.Body.String()
				if !strings.Contains(body, `aerial_brain_webhooks_dispatched_total{hook="on_wake",status="success"}`) {
					t.Errorf("expected metrics to contain on_wake success counter, got:\n%s", body)
				}
			}
		})
	}
}

func TestWebhookDispatcher_ErrorsAndEdgeCases(t *testing.T) {
	tests := []struct {
		name                string
		statusCode          int
		responseBody        string
		timeoutMs           int
		handlerDelay        time.Duration
		nilEndpoint         bool
		emptyURL            bool
		nilRequests         bool
		cancelCtx           bool
		nilReceiver         bool
		invalidHost         bool
		dispatchInvalidArgs bool
		errReaderBody       bool
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
		{
			name:        "nil and empty endpoints",
			nilEndpoint: true,
			emptyURL:    true,
		},
		{
			name:        "nil requests",
			nilRequests: true,
		},
		{
			name:      "canceled parent context",
			cancelCtx: true,
		},
		{
			name:        "nil dispatcher receiver or nil client fallback",
			nilReceiver: true,
		},
		{
			name:        "invalid url format",
			invalidHost: true,
			timeoutMs:   5,
		},
		{
			name:                "dispatch internal branch coverage",
			dispatchInvalidArgs: true,
			errReaderBody:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dispatcher := NewDefaultWebhookDispatcher()

			if tc.nilEndpoint && tc.emptyURL {
				if _, err := dispatcher.CallWakeHook(context.Background(), nil, &WakeRequest{}); err == nil {
					t.Errorf("expected error for nil endpoint")
				}
				if _, err := dispatcher.CallWakeHook(context.Background(), &config.WebhookEndpoint{URL: ""}, &WakeRequest{}); err == nil {
					t.Errorf("expected error for empty endpoint url")
				}
				return
			}

			if tc.nilRequests {
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
				return
			}

			if tc.nilReceiver {
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
				return
			}

			if tc.invalidHost {
				ep := &config.WebhookEndpoint{URL: "http://invalid-url-that-does-not-exist:9999", TimeoutMs: tc.timeoutMs}
				if _, err := dispatcher.CallWakeHook(context.Background(), ep, &WakeRequest{ChannelID: "c1"}); err == nil {
					t.Errorf("expected network error on invalid host")
				}
				return
			}

			if tc.dispatchInvalidArgs {
				if err := dispatcher.dispatch(context.Background(), "on_wake", &config.WebhookEndpoint{URL: "http://example.com"}, nil, nil); err == nil || !strings.Contains(err.Error(), "payload is nil") {
					t.Errorf("expected nil payload error, got %v", err)
				}
				if err := dispatcher.dispatch(context.Background(), "on_wake", &config.WebhookEndpoint{URL: "http://example.com"}, make(chan int), nil); err == nil || !strings.Contains(err.Error(), "failed to marshal") {
					t.Errorf("expected marshal error, got %v", err)
				}
				if err := dispatcher.dispatch(context.Background(), "on_wake", &config.WebhookEndpoint{URL: "http://example.com/\x7fbad"}, "ok", nil); err == nil || !strings.Contains(err.Error(), "failed to create http request") {
					t.Errorf("expected http request creation error, got %v", err)
				}
				if tc.errReaderBody {
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
				}
				return
			}

			if tc.cancelCtx {
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
					_ = json.NewEncoder(w).Encode(WakeResponse{Override: "wake"})
				}))
				defer ts.Close()

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				ep := &config.WebhookEndpoint{URL: ts.URL}
				if _, err := dispatcher.CallWakeHook(ctx, ep, &WakeRequest{ChannelID: "c1"}); err == nil {
					t.Fatalf("expected error on canceled context")
				}
				return
			}

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

			if tc.name == "HTTP 500 error returns error" {
				endpoint := &config.WebhookEndpoint{URL: ts.URL, TimeoutMs: tc.timeoutMs, OnTimeoutAction: "drop"}
				resp, err := dispatcher.CallWakeHook(context.Background(), endpoint, &WakeRequest{ChannelID: "c1"})
				if err == nil || !strings.Contains(err.Error(), "500") {
					t.Fatalf("expected error mentioning 500, got: %v", err)
				}
				if resp != nil {
					t.Errorf("expected nil response on error, got %+v", resp)
				}
			} else if tc.name == "Timeout triggers deadline exceeded" {
				endpoint := &config.WebhookEndpoint{URL: ts.URL, TimeoutMs: tc.timeoutMs, OnTimeoutAction: "classify"}
				resp, err := dispatcher.CallPreTurnHook(context.Background(), endpoint, &PreTurnRequest{ChannelID: "c1"})
				if err == nil {
					t.Fatalf("expected timeout error, got nil")
				}
				if resp != nil {
					t.Errorf("expected nil response on timeout, got %+v", resp)
				}
			} else if tc.name == "Invalid JSON response returns unmarshal error" {
				endpoint := &config.WebhookEndpoint{URL: ts.URL, TimeoutMs: tc.timeoutMs}
				resp, err := dispatcher.CallPostTurnHook(context.Background(), endpoint, &PostTurnRequest{ChannelID: "c1"})
				if err == nil {
					t.Fatalf("expected error on invalid JSON, got nil")
				}
				if resp != nil {
					t.Errorf("expected nil response on unmarshal error, got %+v", resp)
				}
			}
		})
	}
}
