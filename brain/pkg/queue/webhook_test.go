package queue

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/runner"
)

func TestWebhookDispatcher_WakeHook(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		serverResponse   WakeResponse
		serverStatusCode int
		expectError      bool
		expectedOverride string
		expectedReason   string
	}{
		{
			name:             "wake override",
			serverResponse:   WakeResponse{Override: "wake", Reason: "direct priority"},
			serverStatusCode: http.StatusOK,
			expectedOverride: "wake",
			expectedReason:   "direct priority",
		},
		{
			name:             "drop override",
			serverResponse:   WakeResponse{Override: "drop", Reason: "noise"},
			serverStatusCode: http.StatusOK,
			expectedOverride: "drop",
			expectedReason:   "noise",
		},
		{
			name:             "classify override",
			serverResponse:   WakeResponse{Override: "classify"},
			serverStatusCode: http.StatusOK,
			expectedOverride: "classify",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var receivedHeaderEvent string
			var receivedHeaderAgent string
			var receivedHeaderType string
			var receivedReq WakeRequest

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedHeaderEvent = r.Header.Get("X-Aerial-Event")
				receivedHeaderAgent = r.Header.Get("User-Agent")
				receivedHeaderType = r.Header.Get("Content-Type")

				body, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(body, &receivedReq)

				w.WriteHeader(tc.serverStatusCode)
				_ = json.NewEncoder(w).Encode(tc.serverResponse)
			}))
			defer ts.Close()

			dispatcher := NewDefaultWebhookDispatcher()
			endpoint := &config.WebhookEndpoint{
				URL:       ts.URL,
				TimeoutMs: 1000,
			}
			req := &WakeRequest{
				ChannelID: "chan-123",
				ThreadID:  "th-456",
				Message: db.Message{
					ID:      "msg-789",
					Content: "test content",
				},
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
				if receivedHeaderEvent != "on_wake" {
					t.Errorf("expected X-Aerial-Event 'on_wake', got %q", receivedHeaderEvent)
				}
				if receivedHeaderAgent != "aerial-brain/1.0" {
					t.Errorf("expected User-Agent 'aerial-brain/1.0', got %q", receivedHeaderAgent)
				}
				if !strings.HasPrefix(receivedHeaderType, "application/json") {
					t.Errorf("expected Content-Type application/json, got %q", receivedHeaderType)
				}
				if receivedReq.ChannelID != "chan-123" || receivedReq.Message.ID != "msg-789" {
					t.Errorf("received request payload mismatch: %+v", receivedReq)
				}
				if receivedReq.Timestamp.IsZero() {
					t.Errorf("expected non-zero timestamp in request payload")
				}
			}
		})
	}
}

func TestWebhookDispatcher_PreTurnHook(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                  string
		serverResponse        PreTurnResponse
		expectedAllow         bool
		expectedAction        string
		expectedRetryAfterSec int
		expectedContext       string
		expectedReason        string
		expectedMetaVal       string
	}{
		{
			name: "allow with injected context and metadata",
			serverResponse: PreTurnResponse{
				Allow:           true,
				Action:          "proceed",
				InjectedContext: "system instructions from hook",
				Metadata:        map[string]any{"lease_id": "lease-abc"},
			},
			expectedAllow:   true,
			expectedAction:  "proceed",
			expectedContext: "system instructions from hook",
			expectedMetaVal: "lease-abc",
		},
		{
			name: "deny with retry backoff",
			serverResponse: PreTurnResponse{
				Allow:             false,
				Action:            "retry",
				RetryAfterSeconds: 15,
				Reason:            "rate limited",
			},
			expectedAllow:         false,
			expectedAction:        "retry",
			expectedRetryAfterSec: 15,
			expectedReason:        "rate limited",
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
			var receivedHeaderEvent string
			var receivedReq PreTurnRequest

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedHeaderEvent = r.Header.Get("X-Aerial-Event")
				body, _ := io.ReadAll(r.Body)
				_ = json.Unmarshal(body, &receivedReq)

				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(tc.serverResponse)
			}))
			defer ts.Close()

			dispatcher := NewDefaultWebhookDispatcher()
			endpoint := &config.WebhookEndpoint{
				URL:       ts.URL,
				TimeoutMs: 1000,
			}
			req := &PreTurnRequest{
				ChannelID:  "chan-pre-1",
				ThreadID:   "th-pre-1",
				BurstCount: 2,
				Messages: []*db.Message{
					{ID: "m-1", Content: "hello"},
					{ID: "m-2", Content: "world"},
				},
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
			if resp.RetryAfterSeconds != tc.expectedRetryAfterSec {
				t.Errorf("expected retry after %d, got %d", tc.expectedRetryAfterSec, resp.RetryAfterSeconds)
			}
			if resp.InjectedContext != tc.expectedContext {
				t.Errorf("expected injected context %q, got %q", tc.expectedContext, resp.InjectedContext)
			}
			if resp.Reason != tc.expectedReason {
				t.Errorf("expected reason %q, got %q", tc.expectedReason, resp.Reason)
			}
			if tc.expectedMetaVal != "" {
				if resp.Metadata == nil || resp.Metadata["lease_id"] != tc.expectedMetaVal {
					t.Errorf("expected metadata key lease_id=%q, got %+v", tc.expectedMetaVal, resp.Metadata)
				}
			}
			if receivedHeaderEvent != "pre_turn" {
				t.Errorf("expected X-Aerial-Event 'pre_turn', got %q", receivedHeaderEvent)
			}
			if receivedReq.ChannelID != "chan-pre-1" || receivedReq.BurstCount != 2 || len(receivedReq.Messages) != 2 {
				t.Errorf("received payload mismatch: %+v", receivedReq)
			}
			if receivedReq.Timestamp.IsZero() {
				t.Errorf("expected auto-populated timestamp")
			}
		})
	}
}

func TestWebhookDispatcher_PostTurnHook(t *testing.T) {
	t.Parallel()
	var receivedHeaderEvent string
	var receivedReq PostTurnRequest

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaderEvent = r.Header.Get("X-Aerial-Event")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &receivedReq)

		resp := PostTurnResponse{
			Acknowledged: true,
			Metadata:     map[string]any{"released": true},
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer ts.Close()

	dispatcher := NewDefaultWebhookDispatcher()
	endpoint := &config.WebhookEndpoint{
		URL:       ts.URL,
		TimeoutMs: 1000,
	}
	req := &PostTurnRequest{
		ChannelID:    "chan-post",
		ThreadID:     "th-post",
		Status:       "success",
		ResponseText: "completed response",
		DurationMs:   1250,
		TokenUsage: runner.TokenUsage{
			InputTokens:  100,
			OutputTokens: 50,
			TotalTokens:  150,
		},
		Metadata: map[string]any{"lease_id": "lease-abc"},
	}

	resp, err := dispatcher.CallPostTurnHook(context.Background(), endpoint, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Acknowledged {
		t.Errorf("expected acknowledged true")
	}
	if resp.Metadata == nil || resp.Metadata["released"] != true {
		t.Errorf("expected released metadata")
	}
	if receivedHeaderEvent != "post_turn" {
		t.Errorf("expected X-Aerial-Event 'post_turn', got %q", receivedHeaderEvent)
	}
	if receivedReq.ChannelID != "chan-post" || receivedReq.Status != "success" || receivedReq.TokenUsage.TotalTokens != 150 {
		t.Errorf("request payload mismatch: %+v", receivedReq)
	}
	if receivedReq.Timestamp.IsZero() {
		t.Errorf("expected auto-populated timestamp")
	}
}

func TestWebhookDispatcher_HTTPErrorAndTimeout(t *testing.T) {
	t.Parallel()
	t.Run("HTTP 500 error returns error", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error": "internal crash"}`))
		}))
		defer ts.Close()

		dispatcher := NewDefaultWebhookDispatcher()
		endpoint := &config.WebhookEndpoint{
			URL:             ts.URL,
			TimeoutMs:       500,
			OnTimeoutAction: "drop",
		}
		resp, err := dispatcher.CallWakeHook(context.Background(), endpoint, &WakeRequest{ChannelID: "c1"})
		if err == nil {
			t.Fatalf("expected error on 500 response, got nil")
		}
		if resp != nil {
			t.Errorf("expected nil response on error, got %+v", resp)
		}
		if !strings.Contains(err.Error(), "500") {
			t.Errorf("expected error string to mention 500, got: %v", err)
		}
	})

	t.Run("Timeout triggers deadline exceeded", func(t *testing.T) {
		var requestReceived atomic.Bool
		handlerDone := make(chan struct{})
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestReceived.Store(true)
			select {
			case <-handlerDone:
			case <-time.After(20 * time.Millisecond):
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer func() {
			close(handlerDone)
			ts.Close()
		}()

		dispatcher := NewDefaultWebhookDispatcher()
		endpoint := &config.WebhookEndpoint{
			URL:             ts.URL,
			TimeoutMs:       10, // fast timeout
			OnTimeoutAction: "classify",
		}
		resp, err := dispatcher.CallPreTurnHook(context.Background(), endpoint, &PreTurnRequest{ChannelID: "c1"})
		if err == nil {
			t.Fatalf("expected timeout error, got nil")
		}
		if resp != nil {
			t.Errorf("expected nil response on timeout, got %+v", resp)
		}
	})

	t.Run("Invalid JSON response returns unmarshal error", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{not valid json`))
		}))
		defer ts.Close()

		dispatcher := NewDefaultWebhookDispatcher()
		endpoint := &config.WebhookEndpoint{
			URL:       ts.URL,
			TimeoutMs: 500,
		}
		resp, err := dispatcher.CallPostTurnHook(context.Background(), endpoint, &PostTurnRequest{ChannelID: "c1"})
		if err == nil {
			t.Fatalf("expected error on invalid JSON, got nil")
		}
		if resp != nil {
			t.Errorf("expected nil response on unmarshal error, got %+v", resp)
		}
	})
}

func TestWebhookDispatcher_EdgeCases(t *testing.T) {
	t.Parallel()
	dispatcher := NewDefaultWebhookDispatcher()

	t.Run("nil endpoint", func(t *testing.T) {
		_, err := dispatcher.CallWakeHook(context.Background(), nil, &WakeRequest{})
		if err == nil {
			t.Errorf("expected error for nil endpoint")
		}
	})

	t.Run("empty URL endpoint", func(t *testing.T) {
		_, err := dispatcher.CallWakeHook(context.Background(), &config.WebhookEndpoint{URL: ""}, &WakeRequest{})
		if err == nil {
			t.Errorf("expected error for empty endpoint url")
		}
	})

	t.Run("nil requests", func(t *testing.T) {
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
	})

	t.Run("canceled parent context", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(WakeResponse{Override: "wake"})
		}))
		defer ts.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // canceled immediately

		ep := &config.WebhookEndpoint{URL: ts.URL}
		_, err := dispatcher.CallWakeHook(ctx, ep, &WakeRequest{ChannelID: "c1"})
		if err == nil {
			t.Fatalf("expected error on canceled context")
		}
	})

	t.Run("nil dispatcher receiver or nil client fallback", func(t *testing.T) {
		var nilDispatcher *DefaultWebhookDispatcher
		_, err := nilDispatcher.CallWakeHook(context.Background(), &config.WebhookEndpoint{URL: "http://127.0.0.1"}, &WakeRequest{})
		if err == nil {
			t.Errorf("expected error on nil dispatcher")
		}

		customDisp := NewWebhookDispatcherWithClient(nil)
		if customDisp.client == nil {
			t.Errorf("expected non-nil client with NewWebhookDispatcherWithClient(nil)")
		}

		// Verify dispatcher with explicit nil client uses http.DefaultClient fallback
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(WakeResponse{Override: "wake"})
		}))
		defer ts.Close()

		dispWithNilClient := &DefaultWebhookDispatcher{client: nil}
		resp, err := dispWithNilClient.CallWakeHook(nil, &config.WebhookEndpoint{URL: ts.URL}, &WakeRequest{ChannelID: "c1"})
		if err != nil {
			t.Fatalf("unexpected error with nil client and nil ctx: %v", err)
		}
		if resp.Override != "wake" {
			t.Errorf("expected override wake, got %q", resp.Override)
		}
	})

	t.Run("invalid url format", func(t *testing.T) {
		ep := &config.WebhookEndpoint{URL: "http://invalid-url-that-does-not-exist:9999", TimeoutMs: 5}
		_, err := dispatcher.CallWakeHook(context.Background(), ep, &WakeRequest{ChannelID: "c1"})
		if err == nil {
			t.Errorf("expected network error on invalid host")
		}
	})

	t.Run("dispatch internal branch coverage", func(t *testing.T) {
		// nil payload
		err := dispatcher.dispatch(context.Background(), "on_wake", &config.WebhookEndpoint{URL: "http://example.com"}, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "payload is nil") {
			t.Errorf("expected nil payload error, got %v", err)
		}

		// unmarshalable payload
		err = dispatcher.dispatch(context.Background(), "on_wake", &config.WebhookEndpoint{URL: "http://example.com"}, make(chan int), nil)
		if err == nil || !strings.Contains(err.Error(), "failed to marshal") {
			t.Errorf("expected marshal error, got %v", err)
		}

		// invalid http request url (control characters)
		err = dispatcher.dispatch(context.Background(), "on_wake", &config.WebhookEndpoint{URL: "http://example.com/\x7fbad"}, "ok", nil)
		if err == nil || !strings.Contains(err.Error(), "failed to create http request") {
			t.Errorf("expected http request creation error, got %v", err)
		}

		// response body read error
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
		err = dispWithErrBody.dispatch(context.Background(), "on_wake", &config.WebhookEndpoint{URL: "http://example.com"}, "ok", &WakeResponse{})
		if err == nil || !strings.Contains(err.Error(), "failed to read webhook response body") {
			t.Errorf("expected body read error, got %v", err)
		}
	})
}

type errReader struct{}

func (errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("simulated read error")
}

func TestWebhookDispatcher_MetricsRecorded(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(WakeResponse{Override: "wake"})
	}))
	defer ts.Close()

	dispatcher := NewDefaultWebhookDispatcher()
	endpoint := &config.WebhookEndpoint{URL: ts.URL}
	_, err := dispatcher.CallWakeHook(context.Background(), endpoint, &WakeRequest{ChannelID: "c1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	handler := metrics.Handler()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	if !strings.Contains(body, `aerial_brain_webhooks_dispatched_total{hook="on_wake",status="success"}`) {
		t.Errorf("expected metrics to contain on_wake success counter, got:\n%s", body)
	}
}


