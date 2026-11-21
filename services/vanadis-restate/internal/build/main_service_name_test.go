package build

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildObject_ServiceName_NilReceiver(t *testing.T) {
	t.Parallel()
	var bo *BuildObject
	assert.Equal(t, "Build", bo.ServiceName())
}

func TestRestateObjectRuntime_ServiceName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		buildService string
		want         string
	}{
		{name: "empty defaults to legacy", buildService: "", want: "Build"},
		{name: "custom service returned", buildService: "Build-agent-beta", want: "Build-agent-beta"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, restateObjectRuntime{buildService: tc.buildService}.serviceName())
		})
	}
}
