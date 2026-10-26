package main

import (
	"testing"
)

type amdAudioFrame struct {
	rms    float64
	ms     int
	repeat int
}

func TestAMDGreetingController_InboundFrames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		threshold    float64
		frames       []amdAudioFrame
		wantAction   AMDAction
		wantReleased bool
		wantSubNone  bool
	}{
		{
			name:      "human greeting cadence (300ms speech + 600ms silence) releases immediately",
			threshold: 0.015,
			frames: []amdAudioFrame{
				{rms: 0.05, ms: 20, repeat: 15},  // 300ms speech
				{rms: 0.001, ms: 20, repeat: 30}, // 600ms silence
			},
			wantAction:   AMDActionReleaseGreeting,
			wantReleased: true,
			wantSubNone:  true,
		},
		{
			name:      "2500ms timeout releases without continuous machine speech",
			threshold: 0.015,
			frames: []amdAudioFrame{
				{rms: 0.001, ms: 20, repeat: 125}, // 2500ms quiet
			},
			wantAction:   AMDActionReleaseGreeting,
			wantReleased: true,
		},
		{
			name:      "continuous loud speech over 2000ms reaching 2500ms does not release on timeout",
			threshold: 0.015,
			frames: []amdAudioFrame{
				{rms: 0.08, ms: 20, repeat: 125}, // 2500ms loud speech
			},
			wantAction:   AMDActionNone,
			wantReleased: false,
		},
		{
			name:      "inbound frame with cumulative timestamp input",
			threshold: 0.015,
			frames: []amdAudioFrame{
				{rms: 0.05, ms: 300, repeat: 1},  // cumulative 300ms
				{rms: 0.001, ms: 900, repeat: 1}, // cumulative 900ms (600ms silence)
			},
			wantAction:   AMDActionReleaseGreeting,
			wantReleased: true,
		},
		{
			name:      "zero threshold defaults to standard threshold",
			threshold: 0,
			frames: []amdAudioFrame{
				{rms: 0.001, ms: 20, repeat: 1},
			},
			wantAction:   AMDActionNone,
			wantReleased: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := NewAMDGreetingController(tc.threshold)
			var lastAction AMDAction

			for _, f := range tc.frames {
				for r := 0; r < f.repeat; r++ {
					lastAction = ctrl.OnInboundFrame(f.rms, f.ms)
				}
			}

			if lastAction != tc.wantAction {
				t.Fatalf("lastAction = %v, want %v", lastAction, tc.wantAction)
			}
			if ctrl.IsReleased() != tc.wantReleased {
				t.Fatalf("IsReleased() = %v, want %v", ctrl.IsReleased(), tc.wantReleased)
			}
			if tc.wantSubNone {
				sub := ctrl.OnInboundFrame(0.001, 20)
				if sub != AMDActionNone {
					t.Fatalf("subsequent action = %v, want AMDActionNone", sub)
				}
			}
		})
	}
}

func TestAMDGreetingController_Webhooks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		events       []string
		wantActions  []AMDAction
		wantState    AMDState
		wantReleased bool
	}{
		{
			name:         "webhook human releases greeting and repeated call returns none",
			events:       []string{"human", "human"},
			wantActions:  []AMDAction{AMDActionReleaseGreeting, AMDActionNone},
			wantState:    AMDStateReleased,
			wantReleased: true,
		},
		{
			name:         "webhook unknown releases greeting while holding",
			events:       []string{"unknown"},
			wantActions:  []AMDAction{AMDActionReleaseGreeting},
			wantState:    AMDStateReleased,
			wantReleased: true,
		},
		{
			name:         "webhook machine_start switches to voicemail even after release",
			events:       []string{"unknown", "machine_start"},
			wantActions:  []AMDAction{AMDActionReleaseGreeting, AMDActionSwitchToVoicemail},
			wantState:    AMDStateMachineDetected,
			wantReleased: true,
		},
		{
			name:         "webhook machine_end_beep speaks voicemail and hangs up",
			events:       []string{"machine_start", "machine_end_beep"},
			wantActions:  []AMDAction{AMDActionSwitchToVoicemail, AMDActionSpeakVoicemailAndHangup},
			wantState:    AMDStateVoicemailReady,
			wantReleased: true,
		},
		{
			name:         "webhook machine_end_silence speaks voicemail and hangs up",
			events:       []string{"machine_end_silence"},
			wantActions:  []AMDAction{AMDActionSpeakVoicemailAndHangup},
			wantState:    AMDStateVoicemailReady,
			wantReleased: true,
		},
		{
			name:         "webhook unrecognized returns none",
			events:       []string{"unrecognized_event_type"},
			wantActions:  []AMDAction{AMDActionNone},
			wantState:    AMDStateHolding,
			wantReleased: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := NewAMDGreetingController(0.015)

			for i, evt := range tc.events {
				act := ctrl.OnAMDWebhook(evt)
				if i < len(tc.wantActions) && act != tc.wantActions[i] {
					t.Fatalf("event %d (%s) act = %v, want %v", i, evt, act, tc.wantActions[i])
				}
			}

			if ctrl.State() != tc.wantState {
				t.Fatalf("State() = %v, want %v", ctrl.State(), tc.wantState)
			}
			if ctrl.IsReleased() != tc.wantReleased {
				t.Fatalf("IsReleased() = %v, want %v", ctrl.IsReleased(), tc.wantReleased)
			}
		})
	}
}
