package main

import (
	"bytes"
	"testing"
)

func TestSynthesizeDTMFMuLaw(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		sequence    string
		wantLen     int
		wantErr     bool
		checkTone   bool
		checkSilent bool
	}{
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
				if tc.sequence == "w" || tc.sequence == "W" {
					for i, b := range got {
						if b != 0xFF {
							t.Fatalf("pause byte at %d = 0x%02X, want silence 0xFF", i, b)
						}
					}
				} else if len(got) >= 1760 {
					for i := 1280; i < 1760; i++ {
						if got[i] != 0xFF {
							t.Fatalf("inter-digit silence byte at %d = 0x%02X, want 0xFF", i, got[i])
						}
					}
				}
			}
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
