package audio

import (
	"bytes"
	"math"
	"testing"
)

type muLawTestCase struct {
	name     string
	input    []byte
	wantPCM  []int16
	reencode []byte
}

func TestMuLawCodec(t *testing.T) {
	t.Parallel()
	tests := []muLawTestCase{
		{
			name:     "empty slice",
			input:    []byte{},
			wantPCM:  []int16{},
			reencode: []byte{},
		},
		{
			name:     "silence byte 0xFF",
			input:    []byte{0xFF},
			wantPCM:  []int16{0},
			reencode: []byte{0xFF},
		},
		{
			name:     "alternate silence byte 0x7F reencodes to canonical 0xFF",
			input:    []byte{0x7F},
			wantPCM:  []int16{0},
			reencode: []byte{0xFF},
		},
		{
			name:     "maximum positive peak byte 0x80",
			input:    []byte{0x80},
			wantPCM:  []int16{32124},
			reencode: []byte{0x80},
		},
		{
			name:     "maximum negative peak byte 0x00",
			input:    []byte{0x00},
			wantPCM:  []int16{-32124},
			reencode: []byte{0x00},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runMuLawCodecCase(t, tc)
		})
	}

	t.Run("clipping extreme pcm values", func(t *testing.T) {
		t.Parallel()
		out := PCM16ToMuLaw([]int16{32767, -32768})
		if len(out) != 2 {
			t.Fatalf("len(out) = %d, want 2", len(out))
		}
		if out[0] != 0x80 || out[1] != 0x00 {
			t.Errorf("PCM16ToMuLaw(clipped) = [%02X, %02X], want [80, 00]", out[0], out[1])
		}
	})

	t.Run("all 256 bytes roundtrip", func(t *testing.T) {
		t.Parallel()
		testMuLawRoundtrip(t)
	})
}

func runMuLawCodecCase(t *testing.T, tc muLawTestCase) {
	t.Helper()
	pcm := MuLawToPCM16(tc.input)
	if len(pcm) != len(tc.wantPCM) {
		t.Fatalf("len(pcm) = %d, want %d", len(pcm), len(tc.wantPCM))
	}
	for i := range pcm {
		if pcm[i] != tc.wantPCM[i] {
			t.Fatalf("pcm[%d] = %d, want %d", i, pcm[i], tc.wantPCM[i])
		}
	}

	re := PCM16ToMuLaw(pcm)
	if len(re) != len(tc.reencode) {
		t.Fatalf("len(re) = %d, want %d", len(re), len(tc.reencode))
	}
	for i := range re {
		if re[i] != tc.reencode[i] {
			t.Fatalf("re[%d] = 0x%02X, want 0x%02X", i, re[i], tc.reencode[i])
		}
	}
}

func testMuLawRoundtrip(t *testing.T) {
	t.Helper()
	for b := range 256 {
		in := byte(b)
		pcm := MuLawToPCM16([]byte{in})
		if len(pcm) != 1 {
			t.Fatalf("unexpected pcm len: %d", len(pcm))
		}
		out := PCM16ToMuLaw(pcm)
		if len(out) != 1 {
			t.Fatalf("unexpected out len: %d", len(out))
		}
		if in == 0x7F {
			if out[0] != 0xFF {
				t.Errorf("roundtrip for 0x7F = 0x%02X, want 0xFF", out[0])
			}
		} else if out[0] != in {
			t.Errorf("roundtrip mismatch for 0x%02X: got 0x%02X (pcm: %d)", in, out[0], pcm[0])
		}
	}
}

func TestResampleAudio(t *testing.T) {
	t.Parallel()
	t.Run("Resample8kTo16k", testResample8kTo16k)
	t.Run("Resample24kTo8k", testResample24kTo8k)
}

func testResample8kTo16k(t *testing.T) {
	t.Parallel()
	if got := Resample8kTo16k(nil); len(got) != 0 {
		t.Fatalf("empty len = %d, want 0", len(got))
	}
	if got := Resample8kTo16k([]int16{100}); len(got) != 2 {
		t.Fatalf("single sample len = %d, want 2", len(got))
	}
	if got := Resample8kTo16k(make([]int16, 160)); len(got) != 320 {
		t.Fatalf("20ms frame len = %d, want 320", len(got))
	}

	input := []int16{1000, 2000, 3000}
	got := Resample8kTo16k(input)
	want := []int16{1000, 1500, 2000, 2500, 3000, 3000}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("got[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func testResample24kTo8k(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   []int16
		wantLen int
	}{
		{name: "empty", input: nil, wantLen: 0},
		{name: "less than 3 samples", input: []int16{100, 200}, wantLen: 0},
		{name: "standard 20ms frame (480 samples)", input: make([]int16, 480), wantLen: 160},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := Resample24kTo8k(tc.input)
			if len(got) != tc.wantLen {
				t.Fatalf("Resample24kTo8k() len = %d, want %d", len(got), tc.wantLen)
			}
		})
	}

	t.Run("downsample average values", func(t *testing.T) {
		t.Parallel()
		input := []int16{100, 200, 300, 400, 500, 600, 700, 800, 900}
		got := Resample24kTo8k(input)
		want := []int16{200, 500, 800}
		if len(got) != len(want) {
			t.Fatalf("len = %d, want %d", len(got), len(want))
		}
		for i := range got {
			if got[i] != want[i] {
				t.Errorf("got[%d] = %d, want %d", i, got[i], want[i])
			}
		}
	})
}

func TestPCM16LEByteConversions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		samples []int16
	}{
		{name: "empty", samples: []int16{}},
		{name: "single positive", samples: []int16{42}},
		{name: "negative and peak values", samples: []int16{-32124, 0, 32124, -1, 1}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := PCM16ToLEBytes(tc.samples)
			if len(b) != len(tc.samples)*2 {
				t.Fatalf("len(b) = %d, want %d", len(b), len(tc.samples)*2)
			}
			roundtrip := LEBytesToPCM16(b)
			if len(roundtrip) != len(tc.samples) {
				t.Fatalf("len(roundtrip) = %d, want %d", len(roundtrip), len(tc.samples))
			}
			for i := range roundtrip {
				if roundtrip[i] != tc.samples[i] {
					t.Fatalf("roundtrip[%d] = %d, want %d", i, roundtrip[i], tc.samples[i])
				}
			}
		})
	}

	t.Run("odd byte count truncated", func(t *testing.T) {
		t.Parallel()
		oddBytes := []byte{0x01, 0x02, 0x03}
		res := LEBytesToPCM16(oddBytes)
		if len(res) != 1 {
			t.Fatalf("len(res) = %d, want 1", len(res))
		}
	})
}

func TestComputeRMS(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		samples []int16
		wantRMS float64
	}{
		{name: "empty", samples: nil, wantRMS: 0.0},
		{name: "all zero", samples: []int16{0, 0, 0, 0}, wantRMS: 0.0},
		{name: "constant positive", samples: []int16{100, 100, 100, 100}, wantRMS: 100.0},
		{name: "square wave +100 and -100", samples: []int16{100, -100, 100, -100}, wantRMS: 100.0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ComputeRMS(tc.samples)
			if math.Abs(got-tc.wantRMS) > 1e-6 {
				t.Fatalf("ComputeRMS() = %f, want %f", got, tc.wantRMS)
			}
		})
	}
}

func TestEncodePCM16WAV(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		samples    []int16
		sampleRate int
		wantLen    int
	}{
		{name: "empty samples", samples: nil, sampleRate: 8000, wantLen: 0},
		{name: "default sample rate when non-positive", samples: []int16{100, -100}, sampleRate: 0, wantLen: 48},
		{name: "explicit 8kHz mono frame", samples: []int16{1000, -1000, 0}, sampleRate: 8000, wantLen: 50},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wav := EncodePCM16WAV(tc.samples, tc.sampleRate)
			if len(wav) != tc.wantLen {
				t.Fatalf("len(EncodePCM16WAV()) = %d, want %d", len(wav), tc.wantLen)
			}
			if tc.wantLen > 0 && (string(wav[0:4]) != "RIFF" || string(wav[8:12]) != "WAVE" || string(wav[36:40]) != "data") {
				t.Fatalf("invalid WAV header markers: %q / %q / %q", wav[0:4], wav[8:12], wav[36:40])
			}
		})
	}

	if got := ClampInt16(40000); got != 32767 {
		t.Fatalf("ClampInt16(40000) = %d, want 32767", got)
	}
	if got := ClampInt16(-40000); got != -32768 {
		t.Fatalf("ClampInt16(-40000) = %d, want -32768", got)
	}
}

type dtmfTestCase struct {
	name        string
	sequence    string
	wantLen     int
	wantErr     bool
	checkTone   bool
	checkSilent bool
}

func TestSynthesizeDTMFMuLaw(t *testing.T) {
	t.Parallel()
	tests := []dtmfTestCase{
		{
			name:     "empty sequence",
			sequence: "",
			wantLen:  0,
			wantErr:  false,
		},
		{
			name:        "single digit 1",
			sequence:    "1",
			wantLen:     1760,
			wantErr:     false,
			checkTone:   true,
			checkSilent: true,
		},
		{
			name:        "short pause w",
			sequence:    "w",
			wantLen:     4000,
			wantErr:     false,
			checkSilent: true,
		},
		{
			name:        "long pause W",
			sequence:    "W",
			wantLen:     8000,
			wantErr:     false,
			checkSilent: true,
		},
		{
			name:      "mixed sequence 12*#w9",
			sequence:  "12*#w9",
			wantLen:   12800,
			wantErr:   false,
			checkTone: true,
		},
		{
			name:     "lowercase letters a-d allowed",
			sequence: "abcd",
			wantLen:  4 * 1760,
			wantErr:  false,
		},
		{
			name:     "invalid character Z",
			sequence: "Z",
			wantErr:  true,
		},
		{
			name:     "invalid character inside sequence",
			sequence: "123?456",
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSynthesizeDTMFMuLawCase(t, tc)
		})
	}

	t.Run("lowercase matches uppercase output", func(t *testing.T) {
		t.Parallel()
		upper, err1 := SynthesizeDTMFMuLaw("ABCD")
		lower, err2 := SynthesizeDTMFMuLaw("abcd")
		if err1 != nil || err2 != nil {
			t.Fatalf("unexpected errors: err1=%v, err2=%v", err1, err2)
		}
		if !bytes.Equal(upper, lower) {
			t.Errorf("expected upper and lower DTMF synthesis to produce identical audio")
		}
	})
}

func runSynthesizeDTMFMuLawCase(t *testing.T, tc dtmfTestCase) {
	t.Helper()
	got, err := SynthesizeDTMFMuLaw(tc.sequence)
	if (err != nil) != tc.wantErr {
		t.Fatalf("SynthesizeDTMFMuLaw(%q) error = %v, wantErr = %v", tc.sequence, err, tc.wantErr)
	}
	if tc.wantErr {
		return
	}
	if len(got) != tc.wantLen {
		t.Fatalf("SynthesizeDTMFMuLaw(%q) len = %d, want %d", tc.sequence, len(got), tc.wantLen)
	}

	if tc.checkTone && len(got) >= 1280 {
		tonePCM := MuLawToPCM16(got[:1280])
		rms := ComputeRMS(tonePCM)
		if rms < 1000.0 {
			t.Errorf("tone RMS = %f, expected non-silent dual-tone energy (>1000)", rms)
		}
	}

	if tc.checkSilent {
		checkDTMFSilence(t, tc.sequence, got)
	}
}

func checkDTMFSilence(t *testing.T, seq string, got []byte) {
	t.Helper()
	if seq == "w" || seq == "W" {
		assertSilenceBytes(t, got, 0, len(got), "pause byte")
		return
	}
	if len(got) >= 1760 {
		assertSilenceBytes(t, got, 1280, 1760, "inter-digit silence byte")
	}
}

func assertSilenceBytes(t *testing.T, got []byte, start, end int, desc string) {
	t.Helper()
	for i := start; i < end; i++ {
		if got[i] != 0xFF {
			t.Fatalf("%s at %d = 0x%02X, want 0xFF", desc, i, got[i])
		}
	}
}

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
				{rms: 0.05, ms: 20, repeat: 15},
				{rms: 0.001, ms: 20, repeat: 30},
			},
			wantAction:   AMDActionReleaseGreeting,
			wantReleased: true,
			wantSubNone:  true,
		},
		{
			name:      "2500ms timeout releases without continuous machine speech",
			threshold: 0.015,
			frames: []amdAudioFrame{
				{rms: 0.001, ms: 20, repeat: 125},
			},
			wantAction:   AMDActionReleaseGreeting,
			wantReleased: true,
		},
		{
			name:      "continuous loud speech over 2000ms reaching 2500ms does not release on timeout",
			threshold: 0.015,
			frames: []amdAudioFrame{
				{rms: 0.08, ms: 20, repeat: 125},
			},
			wantAction:   AMDActionNone,
			wantReleased: false,
		},
		{
			name:      "inbound frame with cumulative timestamp input",
			threshold: 0.015,
			frames: []amdAudioFrame{
				{rms: 0.05, ms: 300, repeat: 1},
				{rms: 0.001, ms: 900, repeat: 1},
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
				for range f.repeat {
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
		name               string
		conversationActive bool
		events             []string
		wantActions        []AMDAction
		wantState          AMDState
		wantReleased       bool
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
			name:         "webhook machine_end_other after release is ignored",
			events:       []string{"human", "machine_end_other"},
			wantActions:  []AMDAction{AMDActionReleaseGreeting, AMDActionNone},
			wantState:    AMDStateReleased,
			wantReleased: true,
		},
		{
			name:               "late machine webhook during active conversation is ignored",
			conversationActive: true,
			events:             []string{"machine_end_beep", "machine_start"},
			wantActions:        []AMDAction{AMDActionNone, AMDActionNone},
			wantState:          AMDStateHolding,
			wantReleased:       false,
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
			if tc.conversationActive {
				ctrl.MarkConversationActive()
			}

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
