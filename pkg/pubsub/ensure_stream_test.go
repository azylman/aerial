package pubsub

import (
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestJetStreamBus_EnsureStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		existing        *nats.StreamConfig
		streamInfoErr   error
		addStreamErr    error
		updateStreamErr error
		desired         nats.StreamConfig
		wantErr         string
		wantMaxAge      time.Duration
		wantMaxBytes    int64
	}{
		{
			name:         "creates missing stream",
			desired:      nats.StreamConfig{Name: "S", Subjects: []string{"s.>"}, MaxAge: time.Hour, MaxBytes: 1024},
			wantMaxAge:   time.Hour,
			wantMaxBytes: 1024,
		},
		{
			name:         "update preserves higher existing limits",
			existing:     &nats.StreamConfig{Name: "S", MaxAge: 48 * time.Hour, MaxBytes: 4096},
			desired:      nats.StreamConfig{Name: "S", Subjects: []string{"s.>"}, MaxAge: time.Hour, MaxBytes: 1024},
			wantMaxAge:   48 * time.Hour,
			wantMaxBytes: 4096,
		},
		{
			name:         "update raises lower existing limits",
			existing:     &nats.StreamConfig{Name: "S", MaxAge: time.Minute, MaxBytes: 10},
			desired:      nats.StreamConfig{Name: "S", MaxAge: time.Hour, MaxBytes: 1024},
			wantMaxAge:   time.Hour,
			wantMaxBytes: 1024,
		},
		{
			name:          "stream info error is returned",
			streamInfoErr: errors.New("stream info failed"),
			desired:       nats.StreamConfig{Name: "S"},
			wantErr:       "stream info failed",
		},
		{
			name:         "add stream error is returned",
			addStreamErr: errors.New("add stream failed"),
			desired:      nats.StreamConfig{Name: "S"},
			wantErr:      "add stream failed",
		},
		{
			name:            "update stream error is returned",
			existing:        &nats.StreamConfig{Name: "S"},
			updateStreamErr: errors.New("update stream failed"),
			desired:         nats.StreamConfig{Name: "S"},
			wantErr:         "update stream failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mjs := &mockJetStream{
				existingStreams: make(map[string]*nats.StreamInfo),
				streamInfoErr:   tc.streamInfoErr,
				addStreamErr:    tc.addStreamErr,
				updateStreamErr: tc.updateStreamErr,
			}
			if tc.existing != nil {
				mjs.existingStreams[tc.existing.Name] = &nats.StreamInfo{Config: *tc.existing}
			}
			bus := newJetStreamBusWithContext(nil, mjs)

			desired := tc.desired
			err := bus.EnsureStream(&desired)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("EnsureStream error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("EnsureStream unexpected error: %v", err)
			}
			got := mjs.existingStreams[tc.desired.Name].Config
			if got.MaxAge != tc.wantMaxAge || got.MaxBytes != tc.wantMaxBytes {
				t.Errorf("stream limits = (%v, %d), want (%v, %d)", got.MaxAge, got.MaxBytes, tc.wantMaxAge, tc.wantMaxBytes)
			}
		})
	}
}
