package main

import "github.com/azylman/aerial/services/prospera/internal/audio"

var (
	MuLawToPCM16    = audio.MuLawToPCM16
	PCM16ToMuLaw    = audio.PCM16ToMuLaw
	Resample8kTo16k = audio.Resample8kTo16k
	Resample24kTo8k = audio.Resample24kTo8k
	PCM16ToLEBytes  = audio.PCM16ToLEBytes
	LEBytesToPCM16  = audio.LEBytesToPCM16
	ComputeRMS      = audio.ComputeRMS
	EncodePCM16WAV  = audio.EncodePCM16WAV
	ClampInt16      = audio.ClampInt16
	clampInt16      = audio.ClampInt16
)
