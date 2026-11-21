// Package enginefmt holds the engine JSONL wire shapes and text extraction
// shared by eri's session and transcript packages.
package enginefmt

import (
	"encoding/json"
	"strings"
)

// parseCodexText extracts text from a codex content value. ok is false when
// the content is neither a JSON string nor a JSON array of blocks.
func parseCodexText(content json.RawMessage) (string, bool) {
	var text string
	if err := json.Unmarshal(content, &text); err == nil {
		return text, true
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(content, &blocks); err != nil {
		return "", false
	}
	var parts []string
	for _, b := range blocks {
		switch b.Type {
		case "text", "input_text", "output_text":
			if strings.TrimSpace(b.Text) != "" {
				parts = append(parts, b.Text)
			}
		}
	}
	return strings.Join(parts, "\n"), true
}

// ExtractCodexText returns textFallback when set, else the text of content.
// Unknown content shapes yield "".
func ExtractCodexText(content json.RawMessage, textFallback string) string {
	if textFallback != "" {
		return textFallback
	}
	if len(content) == 0 {
		return ""
	}
	text, _ := parseCodexText(content)
	return text
}

// ExtractCodexTextOrRaw is ExtractCodexText but returns the raw content JSON
// for unknown shapes.
func ExtractCodexTextOrRaw(content json.RawMessage, textFallback string) string {
	if textFallback != "" {
		return textFallback
	}
	if len(content) == 0 {
		return ""
	}
	if text, ok := parseCodexText(content); ok {
		return text
	}
	return string(content)
}

// MusePayload is the payload of a muse JSONL transcript line.
type MusePayload struct {
	Kind  string    `json:"kind"`
	RunID string    `json:"run_id"`
	Event MuseEvent `json:"event"`
}

// MuseEvent is a muse run event.
type MuseEvent struct {
	Kind      string           `json:"kind"`
	Prompt    string           `json:"prompt,omitempty"`
	Text      string           `json:"text,omitempty"`
	Terminal  string           `json:"terminal,omitempty"`
	Reason    string           `json:"reason,omitempty"`
	ToolCalls []MuseToolCall   `json:"tool_calls,omitempty"`
	Results   []MuseToolResult `json:"results,omitempty"`
}

// MuseToolCall is one tool call inside a muse event.
type MuseToolCall struct {
	ID     string          `json:"id,omitempty"`
	CallID string          `json:"call_id,omitempty"`
	Name   string          `json:"name"`
	Args   json.RawMessage `json:"args,omitempty"`
}

// MuseToolResult is one tool result inside a muse event.
type MuseToolResult struct {
	ToolCallID string `json:"tool_call_id,omitempty"`
	CallID     string `json:"call_id,omitempty"`
	Text       string `json:"text,omitempty"`
	Output     string `json:"output,omitempty"`
	IsError    bool   `json:"is_error,omitempty"`
}
