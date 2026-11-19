package gemini

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	schemaKeyType = "type"
)

const (
	geminiTypeObject     = "OBJECT"
	geminiTypeString     = "STRING"
	schemaKeyProperties  = "properties"
	schemaKeyDescription = "description"
	schemaKeyRequired    = "required"
)

// ArgDigits is the tool argument name for DTMF digit sequences.
const ArgDigits = "digits"

// ArgQuery is the tool argument name for calendar/memory search queries.
const ArgQuery = "query"

// EndCallToolName is the Gemini function declaration name that ends the call.
const EndCallToolName = "end_call"

// Gemini Live WebSocket message structures

// GeminiSetupMessage represents the initialization handshake payload sent to Gemini Live.
type GeminiSetupMessage struct {
	Setup GeminiSetup `json:"setup"`
}

// GeminiSetup specifies model, generation config, system instructions, and tool declarations.
type GeminiSetup struct {
	Model                    string                 `json:"model"`
	GenerationConfig         GeminiGenerationConfig `json:"generationConfig,omitempty"`
	InputAudioTranscription  map[string]any         `json:"inputAudioTranscription,omitempty"`
	OutputAudioTranscription map[string]any         `json:"outputAudioTranscription,omitempty"`
	SystemInstruction        *GeminiContent         `json:"systemInstruction,omitempty"`
	Tools                    []GeminiTool           `json:"tools,omitempty"`
}

// GeminiGenerationConfig controls response modalities and speech synthesis.
type GeminiGenerationConfig struct {
	ResponseModalities    []string            `json:"responseModalities,omitempty"`
	EnableAffectiveDialog bool                `json:"enableAffectiveDialog,omitempty"`
	SpeechConfig          *GeminiSpeechConfig `json:"speechConfig,omitempty"`
}

// GeminiSpeechConfig defines voice personality and parameters.
type GeminiSpeechConfig struct {
	VoiceConfig  GeminiVoiceConfig `json:"voiceConfig"`
	LanguageCode string            `json:"languageCode,omitempty"`
}

// GeminiVoiceConfig encapsulates prebuilt voice choices.
type GeminiVoiceConfig struct {
	PrebuiltVoiceConfig GeminiPrebuiltVoice `json:"prebuiltVoiceConfig"`
}

// GeminiPrebuiltVoice specifies the prebuilt voice name.
type GeminiPrebuiltVoice struct {
	VoiceName string `json:"voiceName"`
}

// GeminiContent represents structured text or audio turns.
type GeminiContent struct {
	Parts []GeminiPart `json:"parts"`
}

// GeminiInlineData holds nested inline binary audio or media data in a GeminiPart.
type GeminiInlineData struct {
	MimeType string `json:"mimeType,omitempty"`
	Data     string `json:"data,omitempty"`
}

// GeminiPart represents a single content part (text or audio).
type GeminiPart struct {
	Text       string            `json:"text,omitempty"`
	Thought    bool              `json:"thought,omitempty"`
	InlineData *GeminiInlineData `json:"inlineData,omitempty"`
	MimeType   string            `json:"mimeType,omitempty"`
	Data       string            `json:"data,omitempty"`
}

// GeminiTool defines function declarations exposed to Gemini Live.
type GeminiTool struct {
	FunctionDeclarations []GeminiFunctionDecl `json:"functionDeclarations"`
}

// GeminiFunctionDecl defines an individual mid-call callable tool.
type GeminiFunctionDecl struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

// GeminiRealtimeInput represents streaming audio chunks sent from client to Gemini.
type GeminiRealtimeInput struct {
	RealtimeInput GeminiRealtimeMedia `json:"realtimeInput"`
}

// GeminiRealtimeMedia wraps streaming audio or media chunks.
type GeminiRealtimeMedia struct {
	Audio       *GeminiMediaChunk  `json:"audio,omitempty"`
	MediaChunks []GeminiMediaChunk `json:"mediaChunks,omitempty"`
}

// GeminiMediaChunk contains base64 encoded audio.
type GeminiMediaChunk struct {
	MimeType string `json:"mimeType"`
	Data     string `json:"data"`
}

// GeminiClientContentMessage represents mid-call client turns (steering/guidance).
type GeminiClientContentMessage struct {
	ClientContent GeminiClientContent `json:"clientContent"`
}

// GeminiClientContent wraps turns.
type GeminiClientContent struct {
	Turns        []GeminiTurn `json:"turns"`
	TurnComplete bool         `json:"turnComplete"`
}

// GeminiTurn defines an individual role and parts.
type GeminiTurn struct {
	Role  string       `json:"role"`
	Parts []GeminiPart `json:"parts"`
}

// GeminiToolResponseMessage transmits execution output for a tool call back to Gemini.
type GeminiToolResponseMessage struct {
	ToolResponse GeminiToolResponses `json:"toolResponse"`
}

// GeminiToolResponses wraps list of function responses.
type GeminiToolResponses struct {
	FunctionResponses []GeminiFunctionResponse `json:"functionResponses"`
}

// GeminiFunctionResponse is a single tool execution result.
type GeminiFunctionResponse struct {
	ID       string         `json:"id"`
	Response map[string]any `json:"response"`
}

// GeminiServerMessage represents messages received from Gemini Live.
type GeminiServerMessage struct {
	SetupComplete *struct{}            `json:"setupComplete,omitempty"`
	ServerContent *GeminiServerContent `json:"serverContent,omitempty"`
	ToolCall      *GeminiToolCall      `json:"toolCall,omitempty"`
}

// GeminiTranscription holds streaming input or output audio transcription text.
type GeminiTranscription struct {
	Text string `json:"text,omitempty"`
}

// GeminiServerContent represents generated audio or text parts.
type GeminiServerContent struct {
	ModelTurn           *GeminiModelTurn     `json:"modelTurn,omitempty"`
	InputTranscription  *GeminiTranscription `json:"inputTranscription,omitempty"`
	OutputTranscription *GeminiTranscription `json:"outputTranscription,omitempty"`
	GenerationComplete  bool                 `json:"generationComplete,omitempty"`
	TurnComplete        bool                 `json:"turnComplete,omitempty"`
	Interrupted         bool                 `json:"interrupted,omitempty"`
}

// GeminiModelTurn holds the parts generated in a turn.
type GeminiModelTurn struct {
	Parts []GeminiPart `json:"parts"`
}

// GeminiToolCall represents one or more tool calls initiated by Gemini.
type GeminiToolCall struct {
	FunctionCalls []GeminiFunctionCall `json:"functionCalls"`
}

// GeminiFunctionCall describes the specific tool to execute.
type GeminiFunctionCall struct {
	ID   string         `json:"id"`
	Name string         `json:"name"`
	Args map[string]any `json:"args"`
}

// BuildGeminiSetupMessage constructs the handshake setup payload with a configurable voice (defaulting to Kore), language code (defaulting to en-US), and the 4 bounded tools.
func BuildGeminiSetupMessage(model, systemPrompt string, opts ...string) GeminiSetupMessage {
	if !strings.HasPrefix(model, "models/") {
		model = "models/" + model
	}
	voiceName := "Kore"
	if len(opts) > 0 && strings.TrimSpace(opts[0]) != "" {
		voiceName = strings.TrimSpace(opts[0])
	}
	languageCode := "en-US"
	if len(opts) > 1 && strings.TrimSpace(opts[1]) != "" {
		languageCode = strings.TrimSpace(opts[1])
	}

	tools := []GeminiTool{
		{
			FunctionDeclarations: []GeminiFunctionDecl{
				{
					Name:        "send_dtmf",
					Description: "Send DTMF touch-tones during the call for interactive phone menus (IVR)",
					Parameters: map[string]any{
						schemaKeyType: geminiTypeObject,
						schemaKeyProperties: map[string]any{
							ArgDigits: map[string]any{
								schemaKeyType:        geminiTypeString,
								schemaKeyDescription: "Sequence of DTMF digits 0-9, *, #, and w for 500ms pause",
							},
						},
						schemaKeyRequired: []string{ArgDigits},
					},
				},
				{
					Name:        "consult_calendar",
					Description: "Consult the owner's calendar for events, commitments, or schedule availability when the brief allows date/time flexibility or requires checking whether a proposed slot is free. Do not call if the task is strictly limited to a fixed date/time and alternative dates are not permitted.",
					Parameters: map[string]any{
						schemaKeyType: geminiTypeObject,
						schemaKeyProperties: map[string]any{
							ArgQuery: map[string]any{
								schemaKeyType:        geminiTypeString,
								schemaKeyDescription: "Keywords or topic to search on calendar",
							},
							"start_iso": map[string]any{
								schemaKeyType:        geminiTypeString,
								schemaKeyDescription: "Optional ISO 8601 start timestamp filter",
							},
							"end_iso": map[string]any{
								schemaKeyType:        geminiTypeString,
								schemaKeyDescription: "Optional ISO 8601 end timestamp filter",
							},
						},
						schemaKeyRequired: []string{ArgQuery},
					},
				},
				{
					Name:        "consult_memory",
					Description: "Consult the owner's long-term memory for dietary restrictions, allergies, preferences, family/household details, or addresses. MUST be called whenever asked about any detail not explicitly provided in the call brief—never assume 'none' without checking.",
					Parameters: map[string]any{
						schemaKeyType: geminiTypeObject,
						schemaKeyProperties: map[string]any{
							ArgQuery: map[string]any{
								schemaKeyType:        geminiTypeString,
								schemaKeyDescription: "Subject or question to look up in memory",
							},
						},
						schemaKeyRequired: []string{ArgQuery},
					},
				},
				{
					Name:        EndCallToolName,
					Description: "Politely hang up and conclude the phone call once the errand or conversation is done",
					Parameters: map[string]any{
						schemaKeyType: geminiTypeObject,
						schemaKeyProperties: map[string]any{
							"reason": map[string]any{
								schemaKeyType:        geminiTypeString,
								schemaKeyDescription: "Short explanation of why the call concluded",
							},
						},
					},
				},
			},
		},
	}

	return GeminiSetupMessage{
		Setup: GeminiSetup{
			Model: model,
			GenerationConfig: GeminiGenerationConfig{
				ResponseModalities:    []string{"AUDIO"},
				EnableAffectiveDialog: strings.Contains(model, "native-audio"),
				SpeechConfig: &GeminiSpeechConfig{
					VoiceConfig: GeminiVoiceConfig{
						PrebuiltVoiceConfig: GeminiPrebuiltVoice{
							VoiceName: voiceName,
						},
					},
					LanguageCode: languageCode,
				},
			},
			InputAudioTranscription:  map[string]any{},
			OutputAudioTranscription: map[string]any{},
			SystemInstruction: &GeminiContent{
				Parts: []GeminiPart{{Text: systemPrompt}},
			},
			Tools: tools,
		},
	}
}

// BuildGeminiAudioChunk wraps 16kHz PCM audio bytes in a realtimeInput message.
func BuildGeminiAudioChunk(pcm16LEBytes []byte) GeminiRealtimeInput {
	return GeminiRealtimeInput{
		RealtimeInput: GeminiRealtimeMedia{
			Audio: &GeminiMediaChunk{
				MimeType: "audio/pcm;rate=16000",
				Data:     base64.StdEncoding.EncodeToString(pcm16LEBytes),
			},
		},
	}
}

// BuildGeminiClientContent constructs a mid-call steer or instruction turn.
func BuildGeminiClientContent(text, role string) GeminiClientContentMessage {
	if role == "" {
		role = "user"
	}
	return GeminiClientContentMessage{
		ClientContent: GeminiClientContent{
			Turns: []GeminiTurn{
				{
					Role: role,
					Parts: []GeminiPart{
						{Text: text},
					},
				},
			},
			TurnComplete: true,
		},
	}
}

// BuildGeminiToolResponse wraps the output of a function call in a toolResponse message.
func BuildGeminiToolResponse(callID string, output map[string]any) GeminiToolResponseMessage {
	return GeminiToolResponseMessage{
		ToolResponse: GeminiToolResponses{
			FunctionResponses: []GeminiFunctionResponse{
				{
					ID:       callID,
					Response: output,
				},
			},
		},
	}
}

// DialGeminiLive connects to the Gemini Live WebSocket endpoint.
func DialGeminiLive(ctx context.Context, endpointURL, apiKey string) (*websocket.Conn, error) {
	if endpointURL == "" {
		endpointURL = "wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1alpha.GenerativeService.BidiGenerateContent?key=" + apiKey
	} else if apiKey != "" && !strings.Contains(endpointURL, "key=") {
		sep := "?"
		if strings.Contains(endpointURL, "?") {
			sep = "&"
		}
		endpointURL = endpointURL + sep + "key=" + apiKey
	}

	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second

	conn, resp, err := dialer.DialContext(ctx, endpointURL, nil)
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("gemini live dial status %d: %w", resp.StatusCode, err)
		}
		return nil, fmt.Errorf("gemini live dial: %w", err)
	}
	return conn, nil
}
