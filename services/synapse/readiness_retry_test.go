package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
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

func TestPostgresPRRegistry_RetryReadiness(t *testing.T) {
	t.Parallel()
	if *registryTestDSN == "" {
		t.Skip("requires disposable PostgreSQL fixture via -registry-test-dsn")
	}
	tests := []struct {
		name, status, merge string
		pending             bool
		want                int
	}{
		{"deployed retries original gate", "deployed", "abc", false, 1},
		{"failed retries original gate", "deploy_failed", "abc", false, 1},
		{"pending retains partial progress", "deploying", "abc", true, 1},
		{"stale merge rejected", "deployed", "old", true, 0},
		{"closed rejected", "closed", "abc", true, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			conn := newRecoveryFixture(t, tc.status, `{"kustomization_requests":{"config":{"namespace":"flux-system","requested_at":"original"}},"deployed_kustomizations":["config"],"jobs":["image"],"ci_checks":{"test":"passed"}}`)
			if _, err := conn.Exec(ctx, `UPDATE pr_registry SET merge_sha='abc'`); err != nil {
				t.Fatal(err)
			}
			requests, err := NewPostgresPRRegistry(conn).RetryPRReadiness(ctx, "example/project", tc.merge, 42)
			if err != nil || len(requests) != tc.want {
				t.Fatalf("requests %v err %v", requests, err)
			}
			if tc.want > 0 && requests["config"].RequestedAt != "original" {
				t.Fatal("request identity replaced")
			}
			var status string
			var body []byte
			if err := conn.QueryRow(ctx, `SELECT status,metadata FROM pr_registry`).Scan(&status, &body); err != nil {
				t.Fatal(err)
			}
			var metadata map[string]any
			if err := json.Unmarshal(body, &metadata); err != nil {
				t.Fatal(err)
			}
			_, retained := metadata["deployed_kustomizations"]
			if retained != tc.pending || !reflect.DeepEqual(metadata["jobs"], []any{"image"}) {
				t.Fatalf("metadata %s", body)
			}
			if tc.want > 0 && status != "deploying" || tc.want == 0 && status != tc.status {
				t.Fatalf("status %s", status)
			}
		})
	}
}
