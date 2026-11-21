package router

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

func TestShaLookupDivergence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		legacyPR int
		restate  restateclient.ResolveSHAResponse
		want     string
	}{
		{name: "agreement is clean", legacyPR: 42, restate: restateclient.ResolveSHAResponse{PRNumber: 42, Found: true}, want: ""},
		{name: "different pr diverges", legacyPR: 42, restate: restateclient.ResolveSHAResponse{PRNumber: 7, Found: true}, want: "sha_pr"},
		{name: "unindexed sha is missing", legacyPR: 42, want: "sha_pr_missing"},
		{name: "unresolved legacy says nothing", restate: restateclient.ResolveSHAResponse{PRNumber: 7, Found: true}, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := shaLookupDivergence(tc.legacyPR, tc.restate); got != tc.want {
				t.Errorf("shaLookupDivergence() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestOwnerLookupDivergence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		legacy  string
		restate restateclient.BuildLifecycle
		want    string
	}{
		{name: "agreement ignores case and space", legacy: " Aerial ", restate: restateclient.BuildLifecycle{Agent: "aerial"}, want: ""},
		{name: "different owner diverges", legacy: "aerial", restate: restateclient.BuildLifecycle{Agent: "bot-b"}, want: "owner"},
		{name: "unclaimed restate owner is missing", legacy: "aerial", want: "owner_missing"},
		{name: "unknown legacy owner says nothing", restate: restateclient.BuildLifecycle{Agent: "bot-b"}, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ownerLookupDivergence(tc.legacy, tc.restate); got != tc.want {
				t.Errorf("ownerLookupDivergence() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestShadowCompareLookups drives the read-only shadow comparisons against a
// hermetic ingress. Rows run serially because they share the global
// divergence counters.
func TestShadowCompareLookups(t *testing.T) {
	tests := []struct {
		name      string
		owner     string
		noURL     bool
		sha       bool
		legacyPR  int
		legacyOwn string
		callCode  int
		callBody  string
		kind      string
		wantDelta float64
		wantCalls int
	}{
		{name: "sha agreement records nothing", sha: true, legacyPR: 42, callCode: 200, callBody: `{"pr_number":42,"found":true}`, kind: "sha_pr", wantCalls: 1},
		{name: "sha mismatch is counted", sha: true, legacyPR: 42, callCode: 200, callBody: `{"pr_number":7,"found":true}`, kind: "sha_pr", wantDelta: 1, wantCalls: 1},
		{name: "unindexed sha is counted missing", sha: true, legacyPR: 42, callCode: 200, callBody: `{"found":false}`, kind: "sha_pr_missing", wantDelta: 1, wantCalls: 1},
		{name: "sha lookup failure fails closed", sha: true, legacyPR: 42, callCode: 500, callBody: "boom", kind: "sha_pr_missing", wantCalls: 1},
		{name: "restate-owned events never compare", owner: restateclient.EffectsOwnerRestate, sha: true, legacyPR: 42, kind: "sha_pr"},
		{name: "disabled restate never compares", noURL: true, sha: true, legacyPR: 42, kind: "sha_pr"},
		{name: "owner agreement records nothing", legacyOwn: "aerial", callCode: 200, callBody: `{"found":true,"agent":"aerial"}`, kind: "owner", wantCalls: 1},
		{name: "owner mismatch is counted", legacyOwn: "aerial", callCode: 200, callBody: `{"found":true,"agent":"bot-b"}`, kind: "owner", wantDelta: 1, wantCalls: 1},
		{name: "unclaimed owner is counted missing", legacyOwn: "aerial", callCode: 200, callBody: `{"found":false}`, kind: "owner_missing", wantDelta: 1, wantCalls: 1},
		{name: "owner lookup failure fails closed", legacyOwn: "aerial", callCode: 500, callBody: "boom", kind: "owner_missing", wantCalls: 1},
		{name: "blank legacy owner never compares", kind: "owner"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ingress := &ownedIngress{callCode: tc.callCode, callBody: tc.callBody}
			cfg := Config{}
			if !tc.noURL {
				cfg.RestateURL = ingress.serve(t)
			}
			srv := NewRouterServer(fixedConfig(cfg), nil)
			ctx := context.Background()
			if tc.owner != "" {
				ctx = withEffectsOwner(ctx, tc.owner)
			}
			counter := metrics.RestateLookupDivergenceTotal.WithLabelValues(tc.kind)
			before := testutil.ToFloat64(counter)
			if tc.sha {
				srv.shadowCompareSHALookup(ctx, "azylman/aerial", ownedHead, tc.legacyPR)
			} else {
				srv.shadowCompareOwnerLookup(ctx, "azylman/aerial", 42, tc.legacyOwn)
			}
			if got := testutil.ToFloat64(counter) - before; got != tc.wantDelta {
				t.Errorf("%s divergence delta = %v, want %v", tc.kind, got, tc.wantDelta)
			}
			if got := len(ingress.calls()); got != tc.wantCalls {
				t.Errorf("restate lookups = %d, want %d", got, tc.wantCalls)
			}
		})
	}
}
