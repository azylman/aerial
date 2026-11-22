package build

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildObject_ServiceName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		bo   *BuildObject
		want string
	}{
		{"nil receiver", nil, "Build"},
		{"empty agent", NewBuildObjectWithAgent("", "", nil), "Build"},
		{"primary agent", NewBuildObjectWithAgent("", "aerial", nil), "Build"},
		{"sibling agent", NewBuildObjectWithAgent("", "agent-alpha", nil), "Build"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.bo.ServiceName())
		})
	}
}

// restateObjectRuntime.serviceName is a separate function (the per-invocation
// Restate service override), so it keeps its own table.
func TestRestateObjectRuntime_ServiceName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		buildService string
		want         string
	}{
		{"empty defaults to legacy", "", "Build"},
		{"blank defaults to legacy", "  ", "Build"},
		{"custom service returned", "Build-agent-beta", "Build-agent-beta"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, restateObjectRuntime{buildService: tc.buildService}.serviceName())
		})
	}
}
