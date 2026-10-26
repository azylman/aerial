package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// ContextConsultClient queries external context (calendar, memory) mid-call.
type ContextConsultClient interface {
	ConsultCalendar(ctx context.Context, query, startISO, endISO string) (string, error)
	ConsultMemory(ctx context.Context, query string) (string, error)
}

// ConsultOption configures HTTPContextConsultClient.
type ConsultOption func(*HTTPContextConsultClient)

// HTTPContextConsultClient implements ContextConsultClient over HTTP.
type HTTPContextConsultClient struct {
	googleMCPURL string
	covenURL     string
	httpClient   *http.Client
}

// NewHTTPContextConsultClient initializes an HTTPContextConsultClient.
func NewHTTPContextConsultClient(googleMCPURL, covenURL string, opts ...ConsultOption) *HTTPContextConsultClient {
	c := &HTTPContextConsultClient{
		googleMCPURL: strings.TrimRight(googleMCPURL, "/"),
		covenURL:     strings.TrimRight(covenURL, "/"),
		httpClient:   &http.Client{Timeout: 10 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// ConsultCalendar calls google-mcp to query calendar events or availability.
func (c *HTTPContextConsultClient) ConsultCalendar(ctx context.Context, query, startISO, endISO string) (string, error) {
	if c.googleMCPURL == "" {
		return "Calendar consultation unavailable (no Google MCP configured)", nil
	}

	payload := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]any{
			"name": "get_events",
			"arguments": map[string]any{
				"query":    query,
				"time_min": startISO,
				"time_max": endISO,
			},
		},
	}

	bodyBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal calendar request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.googleMCPURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("calendar consult request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("calendar consult returned status %d", resp.StatusCode)
	}

	var rpcResp struct {
		Result struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		return "", fmt.Errorf("decode calendar rpc response: %w", err)
	}

	if rpcResp.Error != nil {
		return "", fmt.Errorf("calendar mcp error: %s", rpcResp.Error.Message)
	}

	var sb strings.Builder
	for _, item := range rpcResp.Result.Content {
		if item.Text != "" {
			if sb.Len() > 0 {
				sb.WriteString("\n")
			}
			sb.WriteString(item.Text)
		}
	}
	if sb.Len() == 0 {
		return "No matching calendar events found.", nil
	}
	return sb.String(), nil
}

// ConsultMemory queries Coven context/memory for background facts.
func (c *HTTPContextConsultClient) ConsultMemory(ctx context.Context, query string) (string, error) {
	if c.covenURL == "" {
		return "Memory consultation unavailable (no Coven configured)", nil
	}

	endpoint := fmt.Sprintf("%s/context?q=%s", c.covenURL, url.QueryEscape(query))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "text/plain")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("coven consult request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("coven consult returned status %d", resp.StatusCode)
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return "", fmt.Errorf("read coven response: %w", err)
	}

	text := strings.TrimSpace(string(raw))
	if text == "" {
		return "No relevant memory context found.", nil
	}
	return text, nil
}

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
	ResponseModalities []string            `json:"responseModalities,omitempty"`
	SpeechConfig       *GeminiSpeechConfig `json:"speechConfig,omitempty"`
}

// GeminiSpeechConfig defines voice personality and parameters.
type GeminiSpeechConfig struct {
	VoiceConfig GeminiVoiceConfig `json:"voiceConfig"`
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

// BuildGeminiSetupMessage constructs the handshake setup payload with the 4 bounded tools.
func BuildGeminiSetupMessage(model, systemPrompt string) GeminiSetupMessage {
	if !strings.HasPrefix(model, "models/") {
		model = "models/" + model
	}

	tools := []GeminiTool{
		{
			FunctionDeclarations: []GeminiFunctionDecl{
				{
					Name:        "send_dtmf",
					Description: "Send DTMF touch-tones during the call for interactive phone menus (IVR)",
					Parameters: map[string]any{
						"type": "OBJECT",
						"properties": map[string]any{
							"digits": map[string]any{
								"type":        "STRING",
								"description": "Sequence of DTMF digits 0-9, *, #, and w for 500ms pause",
							},
						},
						"required": []string{"digits"},
					},
				},
				{
					Name:        "consult_calendar",
					Description: "Consult the owner's calendar for events, commitments, or schedule availability",
					Parameters: map[string]any{
						"type": "OBJECT",
						"properties": map[string]any{
							"query": map[string]any{
								"type":        "STRING",
								"description": "Keywords or topic to search on calendar",
							},
							"start_iso": map[string]any{
								"type":        "STRING",
								"description": "Optional ISO 8601 start timestamp filter",
							},
							"end_iso": map[string]any{
								"type":        "STRING",
								"description": "Optional ISO 8601 end timestamp filter",
							},
						},
						"required": []string{"query"},
					},
				},
				{
					Name:        "consult_memory",
					Description: "Consult the owner's long-term memory for background facts, addresses, or preferences",
					Parameters: map[string]any{
						"type": "OBJECT",
						"properties": map[string]any{
							"query": map[string]any{
								"type":        "STRING",
								"description": "Subject or question to look up in memory",
							},
						},
						"required": []string{"query"},
					},
				},
				{
					Name:        "end_call",
					Description: "Politely hang up and conclude the phone call once the errand or conversation is done",
					Parameters: map[string]any{
						"type": "OBJECT",
						"properties": map[string]any{
							"reason": map[string]any{
								"type":        "STRING",
								"description": "Short explanation of why the call concluded",
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
				ResponseModalities: []string{"AUDIO"},
				SpeechConfig: &GeminiSpeechConfig{
					VoiceConfig: GeminiVoiceConfig{
						PrebuiltVoiceConfig: GeminiPrebuiltVoice{
							VoiceName: "Aoede",
						},
					},
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
