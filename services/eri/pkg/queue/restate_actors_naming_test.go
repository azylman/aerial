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
		wantThreadSvc string
		wantDispSvc   string
		wantVoiceSvc  string
		wantMgrSvc    string
	}{
		{name: "empty agent uses canonical names", agent: "", wantThreadSvc: "ThreadSession", wantDispSvc: "DispatchSession", wantVoiceSvc: "VoiceSession", wantMgrSvc: "SessionManager"},
		{name: "aerial uses canonical names", agent: "aerial", wantThreadSvc: "ThreadSession", wantDispSvc: "DispatchSession", wantVoiceSvc: "VoiceSession", wantMgrSvc: "SessionManager"},
		{name: "aerial with spaces and caps", agent: "  AERIAL  ", wantThreadSvc: "ThreadSession", wantDispSvc: "DispatchSession", wantVoiceSvc: "VoiceSession", wantMgrSvc: "SessionManager"},
		{name: "agent-alpha namespaced", agent: "agent-alpha", wantThreadSvc: "ThreadSession-agent-alpha", wantDispSvc: "DispatchSession-agent-alpha", wantVoiceSvc: "VoiceSession-agent-alpha", wantMgrSvc: "SessionManager-agent-alpha"},
		{name: "agent-beta namespaced with whitespace", agent: " Agent-Beta ", wantThreadSvc: "ThreadSession-agent-beta", wantDispSvc: "DispatchSession-agent-beta", wantVoiceSvc: "VoiceSession-agent-beta", wantMgrSvc: "SessionManager-agent-beta"},
		{name: "unsafe characters route to canonical names", agent: "Foo Bar", wantThreadSvc: "ThreadSession", wantDispSvc: "DispatchSession", wantVoiceSvc: "VoiceSession", wantMgrSvc: "SessionManager"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.wantThreadSvc, ThreadSessionServiceName(tc.agent))
			assert.Equal(t, tc.wantDispSvc, DispatchSessionServiceName(tc.agent))
			assert.Equal(t, tc.wantVoiceSvc, VoiceSessionServiceName(tc.agent))
			assert.Equal(t, tc.wantMgrSvc, SessionManagerServiceName(tc.agent))

			assert.Equal(t, tc.wantThreadSvc, NewThreadSessionObject(nil, tc.agent).ServiceName())
			assert.Equal(t, tc.wantDispSvc, NewDispatchSessionObject(nil, tc.agent).ServiceName())
			assert.Equal(t, tc.wantVoiceSvc, NewVoiceSessionObject(nil, tc.agent).ServiceName())
		})
	}

	assert.Equal(t, "ThreadSession", (*ThreadSessionObject)(nil).ServiceName())
	assert.Equal(t, "DispatchSession", (*DispatchSessionObject)(nil).ServiceName())
	assert.Equal(t, "VoiceSession", (*VoiceSessionObject)(nil).ServiceName())
}

func TestResolvePromptEffort(t *testing.T) {
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
			got := ResolvePromptEffort(tc.model, tc.effort)
			assert.Equal(t, tc.want, got)
		})
	}
}
