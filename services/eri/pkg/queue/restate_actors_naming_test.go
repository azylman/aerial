package queue

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

func TestServiceNaming(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		agent         string
		wantNormalize string
		wantThreadSvc string
		wantDispSvc   string
		wantVoiceSvc  string
	}{
		{
			name:          "empty agent defaults to aerial canonical names",
			agent:         "",
			wantNormalize: "",
			wantThreadSvc: "ThreadSession",
			wantDispSvc:   "DispatchSession",
			wantVoiceSvc:  "VoiceSession",
		},
		{
			name:          "aerial bot id defaults to aerial canonical names",
			agent:         "aerial",
			wantNormalize: "aerial",
			wantThreadSvc: "ThreadSession",
			wantDispSvc:   "DispatchSession",
			wantVoiceSvc:  "VoiceSession",
		},
		{
			name:          "aerial bot id with spaces and caps",
			agent:         "  AERIAL  ",
			wantNormalize: "aerial",
			wantThreadSvc: "ThreadSession",
			wantDispSvc:   "DispatchSession",
			wantVoiceSvc:  "VoiceSession",
		},
		{
			name:          "miorine persona namespaced",
			agent:         "miorine",
			wantNormalize: "miorine",
			wantThreadSvc: "ThreadSession-miorine",
			wantDispSvc:   "DispatchSession-miorine",
			wantVoiceSvc:  "VoiceSession-miorine",
		},
		{
			name:          "suletta persona namespaced with whitespace",
			agent:         " Suletta ",
			wantNormalize: "suletta",
			wantThreadSvc: "ThreadSession-suletta",
			wantDispSvc:   "DispatchSession-suletta",
			wantVoiceSvc:  "VoiceSession-suletta",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.wantNormalize, NormalizeAgentName(tc.agent))
			assert.Equal(t, tc.wantThreadSvc, ThreadSessionServiceName(tc.agent))
			assert.Equal(t, tc.wantDispSvc, DispatchSessionServiceName(tc.agent))
			assert.Equal(t, tc.wantVoiceSvc, VoiceSessionServiceName(tc.agent))

			thObj := NewThreadSessionObject(nil, tc.agent)
			dispObj := NewDispatchSessionObject(nil, tc.agent)
			voiceObj := NewVoiceSessionObject(nil, tc.agent)

			assert.Equal(t, tc.wantThreadSvc, thObj.ServiceName())
			assert.Equal(t, tc.wantDispSvc, dispObj.ServiceName())
			assert.Equal(t, tc.wantVoiceSvc, voiceObj.ServiceName())
		})
	}

	assert.Equal(t, "ThreadSession", (*ThreadSessionObject)(nil).ServiceName())
	assert.Equal(t, "DispatchSession", (*DispatchSessionObject)(nil).ServiceName())
	assert.Equal(t, "VoiceSession", (*VoiceSessionObject)(nil).ServiceName())
}

func TestResolveDispatchSessionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		req        BrainDispatchRequest
		wantPrefix string
		wantExact  string
	}{
		{
			name:      "session_id takes priority",
			req:       BrainDispatchRequest{SessionID: "sess-abc", VanadisChunkID: "c-1", ID: "d-1"},
			wantExact: "sess-abc",
		},
		{
			name:      "vanadis_chunk_id used when session_id empty",
			req:       BrainDispatchRequest{VanadisChunkID: "chunk-xyz", ID: "d-2"},
			wantExact: "vanadis-chunk-xyz",
		},
		{
			name:      "dispatch id used when others empty",
			req:       BrainDispatchRequest{ID: "d-3"},
			wantExact: "dispatch-d-3",
		},
		{
			name:       "generates dispatch uuid when all empty",
			req:        BrainDispatchRequest{},
			wantPrefix: "dispatch-",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ResolveDispatchSessionKey(tc.req)
			if tc.wantExact != "" {
				assert.Equal(t, tc.wantExact, got)
			}
			if tc.wantPrefix != "" {
				assert.True(t, strings.HasPrefix(got, tc.wantPrefix))
			}
		})
	}
}

func TestResolveActorPromptEffort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		model  string
		effort string
		want   string
	}{
		{name: "explicit effort takes priority", model: "gemini-flash", effort: config.EffortHigh, want: config.EffortHigh},
		{name: "medium model sets medium effort", model: "gemini-2.5-flash-medium", effort: "", want: config.EffortMedium},
		{name: "flash model sets low effort", model: "gemini-2.5-flash", effort: "", want: config.EffortLow},
		{name: "other model returns empty", model: "custom-model", effort: "", want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := resolveActorPromptEffort(tc.model, tc.effort)
			assert.Equal(t, tc.want, got)
		})
	}
}
