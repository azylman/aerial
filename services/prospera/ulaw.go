package main

import (
	"encoding/binary"
	"math"
)

// muLawToPCMTable maps an 8-bit ITU-T G.711 mu-law byte to a 16-bit linear PCM sample.
var muLawToPCMTable [256]int16

func init() {
	for i := range 256 {
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
	for i := range in {
		out[2*i] = in[i]
		if i+1 < len(in) {
			out[2*i+1] = int16((int32(in[i]) + int32(in[i+1])) / 2)
		} else {
			out[2*i+1] = in[i]
		}
	}
	return out
}

// Resample24kTo8k downsamples linear PCM audio from 24kHz to 8kHz using a 7-tap
// low-pass anti-aliasing FIR filter ([1, 2, 3, 4, 3, 2, 1] / 16) centered on each
// 3-sample block to suppress >4kHz vocal harmonics and sibilant aliasing before G.711 mu-law encoding.
func Resample24kTo8k(in []int16) []int16 {
	n := len(in) / 3
	if n == 0 {
		return []int16{}
	}
	out := make([]int16, n)
	for i := range n {
		c := 3*i + 1
		if c >= 3 && c+3 < len(in) {
			filtered := int32(in[c-3]) +
				2*int32(in[c-2]) +
				3*int32(in[c-1]) +
				4*int32(in[c]) +
				3*int32(in[c+1]) +
				2*int32(in[c+2]) +
				int32(in[c+3])
			out[i] = int16(filtered / 16)
		} else {
			avg := (int32(in[3*i]) + int32(in[3*i+1]) + int32(in[3*i+2])) / 3
			out[i] = int16(avg)
		}
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
	for i := range n {
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

// EncodePCM16WAV encodes mono 16-bit signed linear PCM samples into a standard RIFF/WAVE payload.
func EncodePCM16WAV(samples []int16, sampleRate int) []byte {
	if len(samples) == 0 {
		return nil
	}
	if sampleRate <= 0 {
		sampleRate = 8000
	}
	dataSize := len(samples) * 2
	out := make([]byte, 44+dataSize)
	copy(out[0:4], "RIFF")
	binary.LittleEndian.PutUint32(out[4:8], uint32(36+dataSize))
	copy(out[8:12], "WAVE")
	copy(out[12:16], "fmt ")
	binary.LittleEndian.PutUint32(out[16:20], 16)
	binary.LittleEndian.PutUint16(out[20:22], 1)
	binary.LittleEndian.PutUint16(out[22:24], 1)
	binary.LittleEndian.PutUint32(out[24:28], uint32(sampleRate))
	binary.LittleEndian.PutUint32(out[28:32], uint32(sampleRate*2))
	binary.LittleEndian.PutUint16(out[32:34], 2)
	binary.LittleEndian.PutUint16(out[34:36], 16)
	copy(out[36:40], "data")
	binary.LittleEndian.PutUint32(out[40:44], uint32(dataSize))
	for i, s := range samples {
		binary.LittleEndian.PutUint16(out[44+2*i:], uint16(s))
	}
	return out
}

func clampInt16(v int32) int16 {
	if v > 32767 {
		return 32767
	}
	if v < -32768 {
		return -32768
	}
	return int16(v)
}
