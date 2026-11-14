package main

import (
	"bytes"
	"testing"
)

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
			wantLen:     1760, // 1280 tone + 480 silence
			wantErr:     false,
			checkTone:   true,
			checkSilent: true,
		},
		{
			name:        "short pause w",
			sequence:    "w",
			wantLen:     4000, // 500ms silence = 4000 bytes
			wantErr:     false,
			checkSilent: true,
		},
		{
			name:        "long pause W",
			sequence:    "W",
			wantLen:     8000, // 1000ms silence = 8000 bytes
			wantErr:     false,
			checkSilent: true,
		},
		{
			name:      "mixed sequence 12*#w9",
			sequence:  "12*#w9",
			wantLen:   12800, // 5 * 1760 (8800) + 4000 = 12800 bytes
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
