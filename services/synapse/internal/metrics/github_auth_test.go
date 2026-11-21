package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestSetGitHubCredentialConfigured(t *testing.T) {
	tests := []struct {
		name       string
		credential string
		wantApp    float64
		wantPAT    float64
	}{
		{name: "app", credential: "app", wantApp: 1, wantPAT: 0},
		{name: "pat", credential: "pat", wantApp: 0, wantPAT: 1},
		{name: "upper PAT", credential: " PAT ", wantApp: 0, wantPAT: 1},
		{name: "unset is app", credential: "", wantApp: 1, wantPAT: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			SetGitHubCredentialConfigured(tc.credential)
			if got := testutil.ToFloat64(GitHubCredentialConfigured.WithLabelValues("app")); got != tc.wantApp {
				t.Errorf("app = %v, want %v", got, tc.wantApp)
			}
			if got := testutil.ToFloat64(GitHubCredentialConfigured.WithLabelValues("pat")); got != tc.wantPAT {
				t.Errorf("pat = %v, want %v", got, tc.wantPAT)
			}
		})
	}
}

func TestGitHubAuthMetrics(t *testing.T) {
	tests := []struct {
		name          string
		source        string
		remaining     float64
		mintResult    string
		wantRequests  float64
		wantRemaining float64
		wantMints     float64
	}{
		{name: "metrics-test-app", source: "metrics-test-app", remaining: 4999, mintResult: "metrics-test-ok", wantRequests: 1, wantRemaining: 4999, wantMints: 1},
		{name: "metrics-test-pat", source: "metrics-test-pat", remaining: 12, mintResult: "metrics-test-error", wantRequests: 1, wantRemaining: 12, wantMints: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := GitHubAuthMetrics{}
			m.IncRequest(tc.source)
			m.SetRateLimitRemaining(tc.source, tc.remaining)
			m.IncTokenMint(tc.mintResult)
			if got := testutil.ToFloat64(GitHubRequestsBySource.WithLabelValues(tc.source)); got != tc.wantRequests {
				t.Errorf("requests = %v, want %v", got, tc.wantRequests)
			}
			if got := testutil.ToFloat64(GitHubRateLimitRemaining.WithLabelValues(tc.source)); got != tc.wantRemaining {
				t.Errorf("remaining = %v, want %v", got, tc.wantRemaining)
			}
			if got := testutil.ToFloat64(GitHubTokenMints.WithLabelValues(tc.mintResult)); got != tc.wantMints {
				t.Errorf("mints = %v, want %v", got, tc.wantMints)
			}
		})
	}
}
