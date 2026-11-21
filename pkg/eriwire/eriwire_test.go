package eriwire

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolveDispatchSessionKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		req  BrainDispatchRequest
		want string
	}{
		{"session wins", BrainDispatchRequest{SessionID: " sess-abc ", VanadisChunkID: "c-1", ID: "d-1"}, "sess-abc"},
		{"chunk", BrainDispatchRequest{VanadisChunkID: "chunk-xyz", ID: "d-2"}, "vanadis-chunk-xyz"},
		{"id", BrainDispatchRequest{ID: " d-3 "}, "dispatch-d-3"},
		{"empty", BrainDispatchRequest{}, "dispatch-"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ResolveDispatchSessionKey(tc.req))
		})
	}
}
