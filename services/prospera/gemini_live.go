package main

import (
	"github.com/azylman/aerial/services/prospera/internal/gemini"
)

type ContextConsultClient = gemini.ContextConsultClient

type ConsultOption = gemini.ConsultOption

type HTTPContextConsultClient = gemini.HTTPContextConsultClient

type GeminiSetupMessage = gemini.GeminiSetupMessage

type GeminiSetup = gemini.GeminiSetup

type GeminiGenerationConfig = gemini.GeminiGenerationConfig

type GeminiSpeechConfig = gemini.GeminiSpeechConfig

type GeminiVoiceConfig = gemini.GeminiVoiceConfig

type GeminiPrebuiltVoice = gemini.GeminiPrebuiltVoice

type GeminiContent = gemini.GeminiContent

type GeminiInlineData = gemini.GeminiInlineData

type GeminiPart = gemini.GeminiPart

type GeminiTool = gemini.GeminiTool

type GeminiFunctionDecl = gemini.GeminiFunctionDecl

type GeminiRealtimeInput = gemini.GeminiRealtimeInput

type GeminiRealtimeMedia = gemini.GeminiRealtimeMedia

type GeminiMediaChunk = gemini.GeminiMediaChunk

type GeminiClientContentMessage = gemini.GeminiClientContentMessage

type GeminiClientContent = gemini.GeminiClientContent

type GeminiTurn = gemini.GeminiTurn

type GeminiToolResponseMessage = gemini.GeminiToolResponseMessage

type GeminiToolResponses = gemini.GeminiToolResponses

type GeminiFunctionResponse = gemini.GeminiFunctionResponse

type GeminiServerMessage = gemini.GeminiServerMessage

type GeminiTranscription = gemini.GeminiTranscription

type GeminiServerContent = gemini.GeminiServerContent

type GeminiModelTurn = gemini.GeminiModelTurn

type GeminiToolCall = gemini.GeminiToolCall

type GeminiFunctionCall = gemini.GeminiFunctionCall

var NewHTTPContextConsultClient = gemini.NewHTTPContextConsultClient

var BuildGeminiSetupMessage = gemini.BuildGeminiSetupMessage

var BuildGeminiAudioChunk = gemini.BuildGeminiAudioChunk

var BuildGeminiClientContent = gemini.BuildGeminiClientContent

var BuildGeminiToolResponse = gemini.BuildGeminiToolResponse

var DialGeminiLive = gemini.DialGeminiLive
