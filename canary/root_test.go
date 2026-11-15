package canary

import (
	"testing"
)

func TestRoot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{
			name: "returns canary-root identifier",
			want: "canary-root",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := Root(); got != tc.want {
				t.Fatalf("Root() = %q, want %q", got, tc.want)
			}
		})
	}
}
