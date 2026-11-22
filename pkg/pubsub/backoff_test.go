package pubsub

import (
	"testing"
	"time"
)

func TestNakBackoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		numDelivered uint64
		maxDelay     time.Duration
		want         time.Duration
	}{
		{name: "zero deliveries", numDelivered: 0, maxDelay: 16 * time.Second, want: time.Second},
		{name: "first delivery", numDelivered: 1, maxDelay: 16 * time.Second, want: time.Second},
		{name: "second delivery doubles", numDelivered: 2, maxDelay: 16 * time.Second, want: 2 * time.Second},
		{name: "fifth delivery reaches cap", numDelivered: 5, maxDelay: 16 * time.Second, want: 16 * time.Second},
		{name: "sixth delivery clamped", numDelivered: 6, maxDelay: 16 * time.Second, want: 16 * time.Second},
		{name: "huge count clamped without overflow", numDelivered: 1 << 40, maxDelay: time.Hour, want: time.Hour},
		{name: "huge count under huge cap uses 2^30s", numDelivered: 1 << 40, maxDelay: 1 << 62, want: (1 << 30) * time.Second},
		{name: "synapse webhook forty-fifth delivery", numDelivered: 45, maxDelay: 16 * time.Second, want: 16 * time.Second},
		{name: "synapse restate first delivery", numDelivered: 1, maxDelay: time.Minute, want: time.Second},
		{name: "synapse restate fifth delivery", numDelivered: 5, maxDelay: time.Minute, want: 16 * time.Second},
		{name: "synapse restate sixth delivery", numDelivered: 6, maxDelay: time.Minute, want: 32 * time.Second},
		{name: "synapse restate seventh delivery caps", numDelivered: 7, maxDelay: time.Minute, want: time.Minute},
		{name: "synapse restate forty-fifth delivery capped", numDelivered: 45, maxDelay: time.Minute, want: time.Minute},
		{name: "cap below one second", numDelivered: 1, maxDelay: 500 * time.Millisecond, want: 500 * time.Millisecond},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := NakBackoff(tc.numDelivered, tc.maxDelay); got != tc.want {
				t.Errorf("NakBackoff(%d, %v) = %v, want %v", tc.numDelivered, tc.maxDelay, got, tc.want)
			}
		})
	}
}
