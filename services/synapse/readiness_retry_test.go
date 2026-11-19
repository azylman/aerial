package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type readinessRetryRegistry struct {
	*mockPRRegistry

	accepted bool
	err      error
}

func (r *readinessRetryRegistry) RetryPRReadiness(_ context.Context, _, _ string, _ int) (map[string]kustomizationRequest, error) {
	if r.accepted {
		return map[string]kustomizationRequest{"config": {Namespace: "flux-system", RequestedAt: "2026-01-02T03:04:05Z"}}, r.err
	}
	return nil, r.err
}

func TestRetryPRReadiness(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, body        string
		accepted          bool
		dbErr, errorPatch error
		want              int
	}{
		{name: "original request retried", body: `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc"}`, accepted: true, want: 202},
		{name: "stale identity rejected", body: `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"old"}`, want: 409},
		{name: "missing identity", body: `{"repo":"owner/config","pr_number":42}`, want: 400},
		{name: "invalid number", body: `{"repo":"owner/config","pr_number":0,"expected_merge_sha":"abc"}`, want: 400},
		{name: "malformed", body: `{`, want: 400},
		{name: "no recorded gate", body: `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc"}`, want: 409},
		{name: "database failure", body: `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc"}`, dbErr: errors.New("offline"), want: 503},
		{name: "patch failure retains durable request", body: `{"repo":"owner/config","pr_number":42,"expected_merge_sha":"abc"}`, accepted: true, errorPatch: errors.New("offline"), want: 503},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := &readinessRetryRegistry{mockPRRegistry: &mockPRRegistry{}, accepted: tc.accepted, err: tc.dbErr}
			disp := &mockOutboundDispatcher{reconcileErrs: map[string]error{"config": tc.errorPatch}}
			bus := &mockEventBus{}
			srv := &RouterServer{registry: reg, dispatcher: disp, bus: bus}
			w := httptest.NewRecorder()
			srv.Routes().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/internal/pr/retry-readiness", strings.NewReader(tc.body)))
			if w.Code != tc.want {
				t.Fatalf("status %d body %s, want %d", w.Code, w.Body.String(), tc.want)
			}
			if len(bus.publishedPREvents) != 0 {
				t.Fatal("retry emitted lifecycle event before readiness observation")
			}
			if tc.accepted && (len(disp.reconcileRequestedAts) != 1 || disp.reconcileRequestedAts[0] != "2026-01-02T03:04:05Z") {
				t.Fatal("original request identity changed")
			}
		})
	}
}
