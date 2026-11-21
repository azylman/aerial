package queue

import (
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
			name:          "agent-alpha persona namespaced",
			agent:         "agent-alpha",
			wantNormalize: "agent-alpha",
			wantThreadSvc: "ThreadSession-agent-alpha",
			wantDispSvc:   "DispatchSession-agent-alpha",
			wantVoiceSvc:  "VoiceSession-agent-alpha",
		},
		{
			name:          "agent-beta persona namespaced with whitespace",
			agent:         " Agent-Beta ",
			wantNormalize: "agent-beta",
			wantThreadSvc: "ThreadSession-agent-beta",
			wantDispSvc:   "DispatchSession-agent-beta",
			wantVoiceSvc:  "VoiceSession-agent-beta",
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
