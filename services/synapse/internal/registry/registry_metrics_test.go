package registry

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/azylman/aerial/services/synapse/internal/metrics"
)

// TestPostgresPRRegistry_CountsCalls pins synapse_registry_calls_total: every
// registry method counts itself even when no pool is wired, so the soak can
// prove restate-owned events made zero registry calls.
func TestPostgresPRRegistry_CountsCalls(t *testing.T) {
	t.Parallel()
	reg := NewPostgresPRRegistry(nil)
	_, _ = reg.UpdatePRSync(t.Context(), "aerial", 1, "abc")
	_, _ = reg.HasExpectedDeployJobs(t.Context(), "aerial", 1)
	_, _, _ = reg.LoadRawPRMetadata(t.Context(), "aerial", 1)
	_ = reg.RecordKustomizationRequests(t.Context(), "aerial", 1, nil)
	tests := []struct {
		name   string
		method string
	}{
		{name: "sync counts", method: "UpdatePRSync"},
		{name: "expected jobs count", method: "HasExpectedDeployJobs"},
		{name: "raw metadata counts", method: "LoadRawPRMetadata"},
		{name: "kustomization requests count", method: "RecordKustomizationRequests"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := testutil.ToFloat64(metrics.RegistryCallsTotal.WithLabelValues(tc.method)); got < 1 {
				t.Fatalf("%s calls = %v, want at least 1", tc.method, got)
			}
		})
	}
}
