package delivery

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/azylman/aerial/services/prospera/internal/config"
)

func resolveRESTModel(cfg *config.Config) string {
	if cfg != nil && cfg.GeminiFallbackModel != "" && !strings.Contains(cfg.GeminiFallbackModel, "live") {
		return cfg.GeminiFallbackModel
	}
	return "gemini-2.5-flash"
}

func (r *Reporter) generateGeminiSummary(ctx context.Context, cfg *config.Config, prompt string) (string, error) {
	if cfg == nil || strings.TrimSpace(cfg.GeminiAPIKey) == "" {
		return "", fmt.Errorf("reporter: gemini api key not configured")
	}

	restModel := resolveRESTModel(cfg)

	reqBody := map[string]any{
		"contents": []map[string]any{
			{
				"parts": []map[string]any{
					{"text": prompt},
				},
			},
		},
		"tools": []any{},
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("reporter: marshal gemini request: %w", err)
	}

	baseURL := "https://generativelanguage.googleapis.com"
	if r.geminiBaseURL != "" {
		baseURL = r.geminiBaseURL
	}
	url := fmt.Sprintf("%s/v1beta/models/%s:generateContent?key=%s", strings.TrimRight(baseURL, "/"), restModel, cfg.GeminiAPIKey)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("reporter: new gemini request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := r.externalClient.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("reporter: do gemini request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 512))
		if readErr != nil {
			return "", fmt.Errorf("reporter: gemini returned %d", resp.StatusCode)
		}
		return "", fmt.Errorf("reporter: gemini returned %d: %s", resp.StatusCode, string(body))
	}

	var res struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", fmt.Errorf("reporter: decode gemini response: %w", err)
	}

	if len(res.Candidates) > 0 && len(res.Candidates[0].Content.Parts) > 0 {
		return res.Candidates[0].Content.Parts[0].Text, nil
	}

	return "", fmt.Errorf("reporter: no text in gemini candidate")
}

func (r *Reporter) transcribeAudio(ctx context.Context, cfg *config.Config, audioBytes []byte, contentType string) (string, error) {
	// First attempt: Whisper STT endpoint
	if cfg != nil && strings.TrimSpace(cfg.WhisperURL) != "" {
		text, err := r.transcribeViaWhisper(ctx, cfg, audioBytes)
		if err == nil && strings.TrimSpace(text) != "" {
			return text, nil
		}
	}

	// Fallback attempt: Gemini Flash inline audio transcription
	if cfg != nil && strings.TrimSpace(cfg.GeminiAPIKey) != "" {
		text, err := r.transcribeViaGeminiFlash(ctx, cfg, audioBytes, contentType)
		if err == nil && strings.TrimSpace(text) != "" {
			return text, nil
		}
	}

	return "", fmt.Errorf("reporter: all transcription backends failed")
}

func (r *Reporter) transcribeViaWhisper(ctx context.Context, cfg *config.Config, audioBytes []byte) (string, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	part, err := writer.CreateFormFile("file", "voicemail.wav")
	if err != nil {
		return "", err
	}
	if _, err = part.Write(audioBytes); err != nil {
		return "", err
	}
	if err = writer.WriteField("model", "whisper-1"); err != nil {
		return "", err
	}
	if err = writer.Close(); err != nil {
		return "", err
	}

	endpoint := strings.TrimRight(cfg.WhisperURL, "/") + "/v1/audio/transcriptions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := r.externalClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("whisper returned status %d", resp.StatusCode)
	}

	var res struct {
		Text string `json:"text"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", err
	}
	return res.Text, nil
}

func (r *Reporter) transcribeViaGeminiFlash(ctx context.Context, cfg *config.Config, audioBytes []byte, contentType string) (string, error) {
	restModel := resolveRESTModel(cfg)

	b64Data := base64.StdEncoding.EncodeToString(audioBytes)
	reqBody := map[string]any{
		"contents": []map[string]any{
			{
				"parts": []map[string]any{
					{
						"inlineData": map[string]string{
							"mimeType": contentType,
							"data":     b64Data,
						},
					},
					{
						"text": "Please transcribe this voicemail audio recording verbatim. Output only the transcript.",
					},
				},
			},
		},
		"tools": []any{},
	}

	data, err := json.Marshal(reqBody)
	if err != nil {
		return "", err
	}

	baseURL := "https://generativelanguage.googleapis.com"
	if r.geminiBaseURL != "" {
		baseURL = r.geminiBaseURL
	}
	url := fmt.Sprintf("%s/v1beta/models/%s:generateContent?key=%s", strings.TrimRight(baseURL, "/"), restModel, cfg.GeminiAPIKey)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.externalClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("gemini flash audio returned %d", resp.StatusCode)
	}

	var res struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", err
	}

	if len(res.Candidates) > 0 && len(res.Candidates[0].Content.Parts) > 0 {
		return res.Candidates[0].Content.Parts[0].Text, nil
	}

	return "", fmt.Errorf("gemini returned no transcription text")
}
