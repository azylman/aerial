package main

import (
	"fmt"
	"math"
	"strings"
)

// dtmfFrequencies maps valid DTMF characters to their low and high group frequencies (Hz).
var dtmfFrequencies = map[rune][2]float64{
	'1': {697, 1209},
	'2': {697, 1336},
	'3': {697, 1477},
	'A': {697, 1633},
	'4': {770, 1209},
	'5': {770, 1336},
	'6': {770, 1477},
	'B': {770, 1633},
	'7': {852, 1209},
	'8': {852, 1336},
	'9': {852, 1477},
	'C': {852, 1633},
	'*': {941, 1209},
	'0': {941, 1336},
	'#': {941, 1477},
	'D': {941, 1633},
}

// SynthesizeDTMFMuLaw generates 8kHz ITU-T G.711 mu-law audio for a DTMF sequence.
// Digits (0-9, *, #, A-D) produce 160ms dual tone followed by 60ms silence.
// 'w' produces 500ms silence.
// 'W' produces 1000ms silence.
func SynthesizeDTMFMuLaw(sequence string) ([]byte, error) {
	if sequence == "" {
		return []byte{}, nil
	}

	const sampleRate = 8000
	const toneSamples = 1280      // 160ms * 8000
	const silenceSamples = 480    // 60ms * 8000
	const pauseWSamples = 4000    // 500ms * 8000
	const pauseBigWSamples = 8000 // 1000ms * 8000
	const amplitude = 16000.0

	var out []byte

	for _, ch := range sequence {
		switch ch {
		case 'w':
			silence := make([]byte, pauseWSamples)
			for i := range silence {
				silence[i] = 0xFF
			}
			out = append(out, silence...)
		case 'W':
			silence := make([]byte, pauseBigWSamples)
			for i := range silence {
				silence[i] = 0xFF
			}
			out = append(out, silence...)
		default:
			normalized := ch
			if ch >= 'a' && ch <= 'd' {
				normalized = rune(strings.ToUpper(string(ch))[0])
			}

			freqs, ok := dtmfFrequencies[normalized]
			if !ok {
				return nil, fmt.Errorf("unsupported DTMF character: %q", ch)
			}

			tonePCM := make([]int16, toneSamples)
			twoPi := 2.0 * math.Pi
			fLow := freqs[0]
			fHigh := freqs[1]

			for i := 0; i < toneSamples; i++ {
				t := float64(i) / float64(sampleRate)
				sLow := math.Sin(twoPi * fLow * t)
				sHigh := math.Sin(twoPi * fHigh * t)
				combined := 0.5 * (sLow + sHigh) * amplitude
				tonePCM[i] = int16(combined)
			}

			toneMuLaw := PCM16ToMuLaw(tonePCM)
			out = append(out, toneMuLaw...)

			silence := make([]byte, silenceSamples)
			for i := range silence {
				silence[i] = 0xFF
			}
			out = append(out, silence...)
		}
	}

	return out, nil
}
