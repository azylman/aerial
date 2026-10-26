package main

import (
	"encoding/binary"
	"math"
)

// muLawToPCMTable maps an 8-bit ITU-T G.711 mu-law byte to a 16-bit linear PCM sample.
var muLawToPCMTable [256]int16

func init() {
	for i := 0; i < 256; i++ {
		inv := ^byte(i)
		sign := inv & 0x80
		exponent := (inv >> 4) & 0x07
		mantissa := inv & 0x0F
		sample := int16(((int(mantissa)<<3)+0x84)<<int(exponent)) - 0x84
		if sign != 0 {
			sample = -sample
		}
		muLawToPCMTable[i] = sample
	}
}

// MuLawToPCM16 converts G.711 mu-law audio bytes into 16-bit linear PCM samples.
func MuLawToPCM16(ulaw []byte) []int16 {
	if len(ulaw) == 0 {
		return []int16{}
	}
	pcm := make([]int16, len(ulaw))
	for i, b := range ulaw {
		pcm[i] = muLawToPCMTable[b]
	}
	return pcm
}

// PCM16ToMuLaw converts 16-bit linear PCM samples into G.711 mu-law audio bytes.
func PCM16ToMuLaw(pcm []int16) []byte {
	if len(pcm) == 0 {
		return []byte{}
	}
	out := make([]byte, len(pcm))
	const bias = 0x84
	const clip = 32635

	for i, sample := range pcm {
		var sign byte
		s := int(sample)
		if s < 0 {
			sign = 0x80
			s = -s
		}
		if s > clip {
			s = clip
		}
		s += bias
		exponent := 7
		for expMask := 0x4000; (s&expMask) == 0 && exponent > 0; expMask >>= 1 {
			exponent--
		}
		mantissa := (s >> (exponent + 3)) & 0x0F
		ulaw := sign | byte(exponent<<4) | byte(mantissa)
		out[i] = ^ulaw
	}
	return out
}

// Resample8kTo16k upsamples linear PCM audio from 8kHz to 16kHz via linear interpolation.
func Resample8kTo16k(in []int16) []int16 {
	if len(in) == 0 {
		return []int16{}
	}
	out := make([]int16, len(in)*2)
	for i := 0; i < len(in); i++ {
		out[2*i] = in[i]
		if i+1 < len(in) {
			out[2*i+1] = int16((int32(in[i]) + int32(in[i+1])) / 2)
		} else {
			out[2*i+1] = in[i]
		}
	}
	return out
}

// Resample24kTo8k downsamples linear PCM audio from 24kHz to 8kHz by averaging 3-sample blocks.
func Resample24kTo8k(in []int16) []int16 {
	n := len(in) / 3
	if n == 0 {
		return []int16{}
	}
	out := make([]int16, n)
	for i := 0; i < n; i++ {
		avg := (int32(in[3*i]) + int32(in[3*i+1]) + int32(in[3*i+2])) / 3
		out[i] = int16(avg)
	}
	return out
}

// PCM16ToLEBytes converts a slice of int16 PCM samples to little-endian bytes.
func PCM16ToLEBytes(samples []int16) []byte {
	out := make([]byte, len(samples)*2)
	for i, s := range samples {
		binary.LittleEndian.PutUint16(out[2*i:], uint16(s))
	}
	return out
}

// LEBytesToPCM16 converts little-endian bytes to a slice of int16 PCM samples.
func LEBytesToPCM16(b []byte) []int16 {
	n := len(b) / 2
	out := make([]int16, n)
	for i := 0; i < n; i++ {
		out[i] = int16(binary.LittleEndian.Uint16(b[2*i:]))
	}
	return out
}

// ComputeRMS computes the root-mean-square amplitude of 16-bit PCM audio samples.
func ComputeRMS(samples []int16) float64 {
	if len(samples) == 0 {
		return 0.0
	}
	var sum float64
	for _, s := range samples {
		v := float64(s)
		sum += v * v
	}
	return math.Sqrt(sum / float64(len(samples)))
}
