package pubsub

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestMergeStreamLimits(t *testing.T) {
	t.Parallel()

	hour72 := 72 * time.Hour
	tests := []struct {
		name        string
		desiredAge  time.Duration
		existingAge time.Duration
		wantAge     time.Duration
		desiredMB   int64
		existingMB  int64
		wantMB      int64
	}{
		{"raise takes desired", hour72, time.Hour, hour72, 128 * 1024 * 1024, 64 * 1024 * 1024, 128 * 1024 * 1024},
		{"lower keeps existing", time.Hour, hour72, hour72, 64 * 1024 * 1024, 128 * 1024 * 1024, 128 * 1024 * 1024},
		{"unchanged stays", hour72, hour72, hour72, 128 * 1024 * 1024, 128 * 1024 * 1024, 128 * 1024 * 1024},
		{"existing unlimited age wins", hour72, 0, 0, 128 * 1024 * 1024, 128 * 1024 * 1024, 128 * 1024 * 1024},
		{"desired unlimited age wins", 0, hour72, 0, 128 * 1024 * 1024, 128 * 1024 * 1024, 128 * 1024 * 1024},
		{"existing unlimited bytes wins", hour72, hour72, hour72, 128 * 1024 * 1024, -1, -1},
		{"desired unlimited bytes wins", hour72, hour72, hour72, -1, 128 * 1024 * 1024, -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			desired := &nats.StreamConfig{Name: "DESIRED", MaxAge: tc.desiredAge, MaxBytes: tc.desiredMB}
			existing := &nats.StreamConfig{Name: "EXISTING", MaxAge: tc.existingAge, MaxBytes: tc.existingMB}
			got := mergeStreamLimits(desired, existing)
			if got.MaxAge != tc.wantAge {
				t.Errorf("MaxAge = %v, want %v", got.MaxAge, tc.wantAge)
			}
			if got.MaxBytes != tc.wantMB {
				t.Errorf("MaxBytes = %d, want %d", got.MaxBytes, tc.wantMB)
			}
			if got.Name != "DESIRED" {
				t.Errorf("Name = %q, want desired config identity", got.Name)
			}
		})
	}
}
