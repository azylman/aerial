package pubsub

import (
	"context"
	"errors"
	"maps"
	"reflect"
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

// coreStream is the part of a stream config TestJetStreamBus_EnsureCoreStreams checks.
type coreStream struct {
	subjects []string
	maxAge   time.Duration
	maxBytes int64
}

// coreStreamsWith returns the four default core streams with overrides replacing entries.
func coreStreamsWith(overrides map[string]coreStream) map[string]coreStream {
	streams := map[string]coreStream{
		"AERIAL_EVENTS":      {[]string{"aerial.dispatch.>", "aerial.events.>", "aerial.turn.>", "aerial.pr.>"}, 72 * time.Hour, 256 << 20},
		"AERIAL_JOBS":        {[]string{"aerial.jobs.>"}, 72 * time.Hour, 256 << 20},
		"AERIAL_COALESCED":   {[]string{"aerial.coalesced.>"}, 72 * time.Hour, 64 << 20},
		"AERIAL_TRANSCRIPTS": {[]string{"aerial.transcripts.>"}, 72 * time.Hour, 128 << 20},
	}
	maps.Copy(streams, overrides)
	return streams
}

func TestJetStreamBus_EnsureCoreStreams(t *testing.T) {
	t.Parallel()

	coalesced, transcripts := []string{"aerial.coalesced.>"}, []string{"aerial.transcripts.>"}
	tests := []struct {
		name          string
		seed          map[string]coreStream
		streamInfoErr error
		addStreamErr  error
		want          map[string]coreStream
		wantErr       string
	}{
		{name: "empty broker gets the four core streams", want: coreStreamsWith(nil)},
		{name: "existing core streams are updated in place", seed: coreStreamsWith(nil), want: coreStreamsWith(nil)},
		{
			name: "coalesced below the floor is raised", seed: map[string]coreStream{"AERIAL_COALESCED": {coalesced, time.Hour, 64 << 20}},
			want: coreStreamsWith(map[string]coreStream{"AERIAL_COALESCED": {coalesced, 72 * time.Hour, 64 << 20}}),
		},
		{
			name: "coalesced larger age is kept", seed: map[string]coreStream{"AERIAL_COALESCED": {coalesced, 200 * time.Hour, 64 << 20}},
			want: coreStreamsWith(map[string]coreStream{"AERIAL_COALESCED": {coalesced, 200 * time.Hour, 64 << 20}}),
		},
		{
			name: "coalesced larger bytes are kept", seed: map[string]coreStream{"AERIAL_COALESCED": {coalesced, 72 * time.Hour, 256 << 20}},
			want: coreStreamsWith(map[string]coreStream{"AERIAL_COALESCED": {coalesced, 72 * time.Hour, 256 << 20}}),
		},
		{
			name: "transcripts below the floor are raised", seed: map[string]coreStream{"AERIAL_TRANSCRIPTS": {transcripts, time.Hour, 1 << 20}},
			want: coreStreamsWith(nil),
		},
		{
			name: "transcripts unlimited age is kept", seed: map[string]coreStream{"AERIAL_TRANSCRIPTS": {transcripts, 0, 128 << 20}},
			want: coreStreamsWith(map[string]coreStream{"AERIAL_TRANSCRIPTS": {transcripts, 0, 128 << 20}}),
		},
		{
			name: "transcripts unlimited bytes are kept", seed: map[string]coreStream{"AERIAL_TRANSCRIPTS": {transcripts, 72 * time.Hour, -1}},
			want: coreStreamsWith(map[string]coreStream{"AERIAL_TRANSCRIPTS": {transcripts, 72 * time.Hour, -1}}),
		},
		{
			name: "stream info error names the stream", streamInfoErr: errors.New("stream info failed"),
			want: map[string]coreStream{}, wantErr: "ensure stream AERIAL_EVENTS: stream info failed",
		},
		{
			name: "add stream error names the stream", addStreamErr: errors.New("add stream failed"),
			want: map[string]coreStream{}, wantErr: "ensure stream AERIAL_EVENTS: add stream failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mjs := &mockJetStream{existingStreams: map[string]*nats.StreamInfo{}, streamInfoErr: tc.streamInfoErr, addStreamErr: tc.addStreamErr}
			for name, s := range tc.seed {
				cfg := nats.StreamConfig{Name: name, Subjects: s.subjects, Storage: nats.FileStorage, MaxAge: s.maxAge, MaxBytes: s.maxBytes}
				mjs.existingStreams[name] = &nats.StreamInfo{Config: cfg}
			}
			err := newJetStreamBusWithContext(nil, mjs).EnsureCoreStreams(context.Background())
			gotErr := ""
			if err != nil {
				gotErr = err.Error()
			}
			if gotErr != tc.wantErr {
				t.Errorf("EnsureCoreStreams() err = %q, want %q", gotErr, tc.wantErr)
			}
			got := map[string]coreStream{}
			for name, info := range mjs.existingStreams {
				got[name] = coreStream{info.Config.Subjects, info.Config.MaxAge, info.Config.MaxBytes}
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("streams = %+v, want %+v", got, tc.want)
			}
		})
	}
}
