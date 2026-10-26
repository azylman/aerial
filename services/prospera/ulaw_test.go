package main

import (
	"math"
	"testing"
)

func TestMuLawCodec(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		input    []byte
		wantPCM  []int16
		reencode []byte
	}{
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
		for b := 0; b < 256; b++ {
			in := byte(b)
			pcm := MuLawToPCM16([]byte{in})
			if len(pcm) != 1 {
				t.Fatalf("unexpected pcm len: %d", len(pcm))
			}
			out := PCM16ToMuLaw(pcm)
			if len(out) != 1 {
				t.Fatalf("unexpected out len: %d", len(out))
			}
			// Both 0xFF and 0x7F encode silence (0)
			if in == 0x7F {
				if out[0] != 0xFF {
					t.Errorf("roundtrip for 0x7F = 0x%02X, want 0xFF", out[0])
				}
			} else if out[0] != in {
				t.Errorf("roundtrip mismatch for 0x%02X: got 0x%02X (pcm: %d)", in, out[0], pcm[0])
			}
		}
	})
}

func TestResample8kTo16k(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		input   []int16
		wantLen int
	}{
		{name: "empty", input: nil, wantLen: 0},
		{name: "single sample", input: []int16{100}, wantLen: 2},
		{name: "standard 20ms frame (160 samples)", input: make([]int16, 160), wantLen: 320},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := Resample8kTo16k(tc.input)
			if len(got) != tc.wantLen {
				t.Fatalf("Resample8kTo16k() len = %d, want %d", len(got), tc.wantLen)
			}
		})
	}

	t.Run("linear interpolation values", func(t *testing.T) {
		t.Parallel()
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
	})
}

func TestResample24kTo8k(t *testing.T) {
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
		input := []int16{100, 200, 300, 400, 500, 600}
		got := Resample24kTo8k(input)
		want := []int16{200, 500}
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
