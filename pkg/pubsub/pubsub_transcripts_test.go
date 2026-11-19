package pubsub

import (
	"context"
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

func TestEnsureStreamNeverLowersLimits(t *testing.T) {
	t.Parallel()

	hour72 := 72 * time.Hour
	tests := []struct {
		name      string
		stream    string
		subjects  []string
		seedAge   time.Duration
		seedBytes int64
		wantAge   time.Duration
		wantBytes int64
	}{
		{"coalesced below floor raised", StreamAerialCoalesced, []string{"aerial.coalesced.>"}, time.Hour, 64 * 1024 * 1024, hour72, 64 * 1024 * 1024},
		{"coalesced larger age kept", StreamAerialCoalesced, []string{"aerial.coalesced.>"}, 200 * time.Hour, 64 * 1024 * 1024, 200 * time.Hour, 64 * 1024 * 1024},
		{"coalesced larger bytes kept", StreamAerialCoalesced, []string{"aerial.coalesced.>"}, hour72, 256 * 1024 * 1024, hour72, 256 * 1024 * 1024},
		{"transcripts below floor raised", StreamAerialTranscripts, []string{"aerial.transcripts.>"}, time.Hour, 1024 * 1024, hour72, 128 * 1024 * 1024},
		{"transcripts unlimited age kept", StreamAerialTranscripts, []string{"aerial.transcripts.>"}, 0, 128 * 1024 * 1024, 0, 128 * 1024 * 1024},
		{"transcripts unlimited bytes kept", StreamAerialTranscripts, []string{"aerial.transcripts.>"}, hour72, -1, hour72, -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mjs := &mockJetStream{existingStreams: map[string]*nats.StreamInfo{
				tc.stream: {Config: nats.StreamConfig{
					Name:     tc.stream,
					Subjects: tc.subjects,
					Storage:  nats.FileStorage,
					MaxAge:   tc.seedAge,
					MaxBytes: tc.seedBytes,
				}},
			}}
			bus := newJetStreamBusWithContext(nil, mjs)
			if err := bus.EnsureCoreStreams(context.Background()); err != nil {
				t.Fatalf("EnsureCoreStreams failed: %v", err)
			}
			got := mjs.existingStreams[tc.stream].Config
			if got.MaxAge != tc.wantAge {
				t.Errorf("%s MaxAge = %v, want %v", tc.stream, got.MaxAge, tc.wantAge)
			}
			if got.MaxBytes != tc.wantBytes {
				t.Errorf("%s MaxBytes = %d, want %d", tc.stream, got.MaxBytes, tc.wantBytes)
			}
		})
	}
}
