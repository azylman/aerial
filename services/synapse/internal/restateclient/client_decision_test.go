package restateclient

// InvokePREvent tests: the response-returning ingress path the authoritative
// deploy decision uses. Transport failures and undecodable bodies surface as
// errors so the caller retries instead of losing the signal.

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestInvokePREvent(t *testing.T) {
	t.Parallel()
	const decisionBody = `{"disposition":"apply","should_publish":true,"deduped":false,"effect_key":"azylman/aerial#7@sha123:gen0:deploy_dispatch:job=brain"}`
	tests := []struct {
		name              string
		handler           string
		repo              string
		prNumber          int
		nilOut            bool
		unconfigured      bool
		status            int
		body              string
		wantErrSub        string
		wantRequests      int64
		wantShouldPublish bool
		wantDeduped       bool
	}{
		{name: "decision round-trips", handler: "HandleDeployDispatched", repo: "azylman/aerial", prNumber: 7, status: 202, body: decisionBody, wantRequests: 1, wantShouldPublish: true},
		{name: "deduped decision round-trips", handler: "HandleDeployDispatched", repo: "azylman/aerial", prNumber: 7, status: 202, body: `{"disposition":"apply","should_publish":false,"deduped":true,"effect_key":"k"}`, wantRequests: 1, wantDeduped: true},
		{name: "unknown handler rejected without request", handler: "HandleNope", repo: "azylman/aerial", prNumber: 7, status: 202, body: decisionBody, wantErrSub: "unknown pr handler", wantRequests: 0},
		{name: "unconfigured client fails closed", handler: "HandleDeployDispatched", repo: "azylman/aerial", prNumber: 7, unconfigured: true, wantErrSub: "not configured", wantRequests: 0},
		{name: "ingress 500 surfaces", handler: "HandleDeployDispatched", repo: "azylman/aerial", prNumber: 7, status: 500, body: "boom", wantErrSub: "status 500", wantRequests: 1},
		{name: "malformed json surfaces decode error", handler: "HandleDeployDispatched", repo: "azylman/aerial", prNumber: 7, status: 202, body: "not-json{", wantErrSub: "decode restate response", wantRequests: 1},
		{name: "missing pr number rejected", handler: "HandleDeployDispatched", repo: "azylman/aerial", prNumber: 0, status: 202, body: decisionBody, wantErrSub: "pr_number is required", wantRequests: 0},
		{name: "empty repo rejected", handler: "HandleDeployDispatched", repo: "", prNumber: 7, status: 202, body: decisionBody, wantErrSub: "repo is required", wantRequests: 0},
		{name: "nil output rejected", handler: "HandleDeployDispatched", repo: "azylman/aerial", prNumber: 7, nilOut: true, status: 202, body: decisionBody, wantErrSub: "output is required", wantRequests: 0},
		{name: "ack handler allowed", handler: "HandleDeploySuccessPublished", repo: "azylman/aerial", prNumber: 7, status: 202, body: `{"disposition":"apply","effect_key":"k","recorded":true,"deduped":false}`, wantRequests: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int64
			baseURL := ""
			if !tc.unconfigured {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
				}))
				t.Cleanup(srv.Close)
				baseURL = srv.URL
			}
			c := NewClient(baseURL, "", "", nil)
			var got DeployDispatchDecision
			var out any = &got
			if tc.nilOut {
				out = nil
			}
			err := c.InvokePREvent(t.Context(), tc.handler, PREvent{Repo: tc.repo, PRNumber: tc.prNumber, HeadSHA: "sha123", JobName: "brain"}, out)
			if tc.wantErrSub == "" {
				if err != nil {
					t.Fatalf("InvokePREvent() = %v, want nil", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
				t.Fatalf("InvokePREvent() = %v, want error containing %q", err, tc.wantErrSub)
			}
			if n := calls.Load(); n != tc.wantRequests {
				t.Fatalf("requests = %d, want %d", n, tc.wantRequests)
			}
			if tc.wantErrSub == "" && tc.handler == "HandleDeployDispatched" {
				if got.ShouldPublish != tc.wantShouldPublish || got.Deduped != tc.wantDeduped {
					t.Fatalf("ShouldPublish/Deduped = %v/%v, want %v/%v", got.ShouldPublish, got.Deduped, tc.wantShouldPublish, tc.wantDeduped)
				}
			}
		})
	}
}
