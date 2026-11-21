package router

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

func TestHandleRetryPRReadinessRestateOwned(t *testing.T) {
	t.Parallel()
	const (
		validReq = `{"repo":"azylman/aerial","pr_number":42,"expected_merge_sha":"m1"}`
		authOK   = `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":true,"user":{"username":"system:serviceaccount:default:eri-aerial"}}}`
		callPath = "/PRObject/azylman%2Faerial%2342/RetryReadiness"
	)
	tests := []struct {
		name        string
		k8s         bool
		token       string
		noURL       bool
		body        string
		callCode    int
		callBody    string
		wantStatus  int
		wantRespSub string
		wantCallSub string
		wantCalls   int
	}{
		{
			name: "accepted gate is pending", body: validReq, callCode: 200, callBody: `{"status":202,"kustomization_requests":{"core":{"namespace":"flux-system","requested_at":"t1"}}}`,
			wantStatus: http.StatusAccepted, wantRespSub: `"readiness_pending"`, wantCallSub: `{"merge_sha":"m1","effects_owner":"restate"}`, wantCalls: 1,
		},
		{name: "no matching gate is a conflict", body: validReq, callCode: 200, callBody: `{"status":409}`, wantStatus: http.StatusConflict, wantRespSub: "no matching merged rollout gate", wantCalls: 1},
		{name: "restate owner fence is a conflict", body: validReq, callCode: 409, callBody: "owner", wantStatus: http.StatusConflict, wantCalls: 1},
		{name: "restate outage is unavailable", body: validReq, callCode: 500, callBody: "boom", wantStatus: http.StatusServiceUnavailable, wantCalls: 1},
		{name: "no restate url is unavailable", noURL: true, body: validReq, wantStatus: http.StatusServiceUnavailable, wantRespSub: "readiness retry unavailable"},
		{name: "missing merge sha is rejected locally", body: `{"repo":"azylman/aerial","pr_number":42}`, wantStatus: http.StatusBadRequest},
		{
			name: "k8s caller fences the owner inside restate", k8s: true, token: "valid-token", body: validReq, callCode: 200, callBody: `{"status":202}`,
			wantStatus: http.StatusAccepted, wantCallSub: `"agent":"aerial","effects_owner":"restate","enforce_owner":true,"expected_owner":"aerial"`, wantCalls: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress := &ownedIngress{sendCode: http.StatusAccepted, callCode: tc.callCode, callBody: tc.callBody}
			cfg := Config{}
			if !tc.noURL {
				cfg.RestateURL = ingress.serve(t)
			}
			if tc.k8s {
				cfg.K8sAuthEnabled = true
				cfg.K8sAuthConfig = newTestK8sAuthConfig(t, &staticAuthRoundTripper{authStatus: http.StatusOK, authBody: authOK, sarRules: []sarRule{{Resource: "prs", Verb: "create", Name: "aerial"}}})
			}
			var db atomic.Int64
			srv := NewRouterServer(fixedConfig(cfg), nil)
			srv.SetRegistry(NewPostgresPRRegistry(countingPool(&db)))
			srv.SetDispatcher(&mockOutboundDispatcher{})
			req := httptest.NewRequest(http.MethodPost, "/internal/pr/retry-readiness", strings.NewReader(tc.body)).WithContext(withEffectsOwner(context.Background(), restateclient.EffectsOwnerRestate))
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			rec := httptest.NewRecorder()
			srv.handleRetryPRReadiness(rec, req)
			if rec.Code != tc.wantStatus || !strings.Contains(rec.Body.String(), tc.wantRespSub) {
				t.Fatalf("response = %d %s, want %d containing %s", rec.Code, rec.Body.String(), tc.wantStatus, tc.wantRespSub)
			}
			calls := ingress.calls()
			if len(calls) != tc.wantCalls {
				t.Fatalf("readiness calls = %v, want %d", calls, tc.wantCalls)
			}
			if tc.wantCalls > 0 && (calls[0] != callPath || !strings.Contains(ingress.callBodies()[0], tc.wantCallSub)) {
				t.Errorf("readiness call = %s %s, want %s containing %s", calls[0], ingress.callBodies()[0], callPath, tc.wantCallSub)
			}
			if got := db.Load(); got != 0 {
				t.Errorf("pr_registry calls = %d, want 0", got)
			}
		})
	}
}

func TestJournalImagePromotion(t *testing.T) {
	t.Parallel()
	const path = "/RepoObject/azylman%2Faerial/HandleImagePromotion/send"
	tests := []struct {
		name        string
		failHandler string
		wantBodySub string
	}{
		{name: "legacy promotion journals images and requests", wantBodySub: `"images":["ghcr.io/azylman/eri:s1"],"kustomization_requests":{"core":{"namespace":"flux-system","requested_at":"t1"}}`},
		{name: "journal failure is only logged", failHandler: "HandleImagePromotion", wantBodySub: `"effects_owner":"legacy"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress, srv := newGitHubIngress(t, http.StatusAccepted, tc.failHandler)
			router := newTestRouter(Config{RestateAuthoritative: true, RestateURL: srv.URL, DefaultOwner: true}, nil, &mockPRRegistry{}, &mockOutboundDispatcher{}, &mockEventBus{})
			router.journalImagePromotion(context.Background(), restateclient.RepoEvent{
				Repo: "azylman/aerial", HeadSHA: "s1", Images: []string{"ghcr.io/azylman/eri:s1"},
				KustomizationRequests: map[string]kustomizationRequest{"core": {Namespace: "flux-system", RequestedAt: "t1"}},
			})
			if got := ingress.Paths(); len(got) != 1 || got[0] != path {
				t.Fatalf("journal paths = %v, want [%s]", got, path)
			}
			if body := string(ingress.Body(0)); !strings.Contains(body, tc.wantBodySub) {
				t.Errorf("journal body = %s, want it to contain %s", body, tc.wantBodySub)
			}
		})
	}
}
