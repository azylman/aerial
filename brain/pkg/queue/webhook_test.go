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
)

type errReader struct{}

func (errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("simulated read error")
}

func TestWebhookDispatcher_LifecycleHooks(t *testing.T) {
	t.Parallel()

	// 1. OnWake
	wakeTests := []struct {
		name      string
		override  string
		reason    string
		wantEvent string
	}{
		{"wake override", "wake", "priority", "on_wake"},
		{"drop override", "drop", "noise", "on_wake"},
		{"classify override", "classify", "", "on_wake"},
	}

	for _, tc := range wakeTests {
		t.Run("OnWake_"+tc.name, func(t *testing.T) {
			t.Parallel()
			var receivedHeaderEvent, receivedHeaderAgent, receivedHeaderType string
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedHeaderEvent = r.Header.Get("X-Aerial-Event")
				receivedHeaderAgent = r.Header.Get("User-Agent")
				receivedHeaderType = r.Header.Get("Content-Type")
				_ = json.NewEncoder(w).Encode(WakeResponse{Override: tc.override, Reason: tc.reason})
			}))
			defer ts.Close()

			dispatcher := NewDefaultWebhookDispatcher()
			endpoint := &config.WebhookEndpoint{URL: ts.URL, TimeoutMs: 1000}
			req := &WakeRequest{ChannelID: "c1", ThreadID: "t1", Message: db.Message{ID: "m1", Content: "test"}}
			resp, err := dispatcher.CallWakeHook(context.Background(), endpoint, req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.Override != tc.override || resp.Reason != tc.reason {
				t.Errorf("got override=%s reason=%s, want %s %s", resp.Override, resp.Reason, tc.override, tc.reason)
			}
			if receivedHeaderEvent != tc.wantEvent || receivedHeaderAgent != "aerial-brain/1.0" || !strings.HasPrefix(receivedHeaderType, "application/json") {
				t.Errorf("headers mismatch: event=%s agent=%s type=%s", receivedHeaderEvent, receivedHeaderAgent, receivedHeaderType)
			}
		})
	}

	// 2. PreTurn
	preTests := []struct {
		name       string
		allow      bool
		action     string
		retryAfter int
		ctx        string
		reason     string
	}{
		{"allow with context", true, "proceed", 0, "injected context", ""},
		{"deny retry", false, "retry", 15, "", "rate limit"},
		{"deny drop", false, "drop", 0, "", "moderation"},
	}

	for _, tc := range preTests {
		t.Run("PreTurn_"+tc.name, func(t *testing.T) {
			t.Parallel()
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(PreTurnResponse{
					Allow:             tc.allow,
					Action:            tc.action,
					RetryAfterSeconds: tc.retryAfter,
					InjectedContext:   tc.ctx,
					Reason:            tc.reason,
				})
			}))
			defer ts.Close()

			dispatcher := NewDefaultWebhookDispatcher()
			endpoint := &config.WebhookEndpoint{URL: ts.URL, TimeoutMs: 1000}
			req := &PreTurnRequest{ChannelID: "c1", ThreadID: "t1", BurstCount: 1}
			resp, err := dispatcher.CallPreTurnHook(context.Background(), endpoint, req)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.Allow != tc.allow || resp.Action != tc.action || resp.RetryAfterSeconds != tc.retryAfter || resp.InjectedContext != tc.ctx || resp.Reason != tc.reason {
				t.Errorf("got response %+v, mismatch with test case", resp)
			}
		})
	}

	// 3. PostTurn
	t.Run("PostTurn_Success", func(t *testing.T) {
		t.Parallel()
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(PostTurnResponse{Acknowledged: true, Metadata: map[string]any{"released": true}})
		}))
		defer ts.Close()

		dispatcher := NewDefaultWebhookDispatcher()
		endpoint := &config.WebhookEndpoint{URL: ts.URL, TimeoutMs: 1000}
		req := &PostTurnRequest{ChannelID: "c1", ThreadID: "t1", Status: "success", DurationMs: 100}
		resp, err := dispatcher.CallPostTurnHook(context.Background(), endpoint, req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !resp.Acknowledged || resp.Metadata["released"] != true {
			t.Errorf("unexpected post_turn response: %+v", resp)
		}
	})
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
		{name: "HTTP 500 error returns error", statusCode: http.StatusInternalServerError, responseBody: `{"error": "crash"}`, timeoutMs: 500},
		{name: "Timeout triggers deadline exceeded", handlerDelay: 20 * time.Millisecond, timeoutMs: 10},
		{name: "Invalid JSON response returns unmarshal error", statusCode: http.StatusOK, responseBody: `{invalid json`, timeoutMs: 500},
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
	if _, err := nilDispatcher.CallWakeHook(context.Background(), ep, &WakeRequest{}); err == nil {
		t.Errorf("expected error on nil dispatcher")
	}

	customDisp := NewWebhookDispatcherWithClient(nil)
	if customDisp.client == nil {
		t.Errorf("expected non-nil client with NewWebhookDispatcherWithClient(nil)")
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	if err := dispatcher.dispatch(context.Background(), "on_wake", ep, nil, nil); err == nil || !strings.Contains(err.Error(), "payload is nil") {
		t.Errorf("expected nil payload error, got %v", err)
	}
	if err := dispatcher.dispatch(context.Background(), "on_wake", ep, make(chan int), nil); err == nil || !strings.Contains(err.Error(), "failed to marshal") {
		t.Errorf("expected marshal error, got %v", err)
	}
	if err := dispatcher.dispatch(context.Background(), "on_wake", &config.WebhookEndpoint{URL: "http://example.com/\x7fbad"}, "ok", nil); err == nil || !strings.Contains(err.Error(), "failed to create http request") {
		t.Errorf("expected http request creation error, got %v", err)
	}

	clientWithErrBody := &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
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
	if _, err := dispatcher.CallWakeHook(ctx, &config.WebhookEndpoint{URL: ts.URL}, &WakeRequest{ChannelID: "c1"}); err == nil {
		t.Fatalf("expected error on canceled context")
	}
}
